"""Shared WebSocket ingest engine runtime."""

from __future__ import annotations

import signal
import socket
import threading
import time
from typing import Any, Optional

from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

from deepwater.platform import Platform


def _now_us() -> int:
    return time.time_ns() // 1_000


def _backoff():
    b = 0.5
    while True:
        yield b
        b = min(b * 2.0, 15.0)


class SharedWsIngestEngine:
    def __init__(self, *, connector: Any, platform_base_path: str, logger) -> None:
        self.connector = connector
        self.log = logger
        self.uri = connector.uri

        self._should_run = False
        self._ws = None
        self.product_ids: set[str] = set()
        self.io_thread: Optional[threading.Thread] = None

        self.platform = Platform(base_path=platform_base_path)
        self.family_writers: dict[str, dict[str, Any]] = {}
        self.writers_by_product: dict[str, dict[str, Any]] = {}
        self.trade_writers: dict[str, Any] = self.family_writers.setdefault("trades", {})
        self.book_writers: dict[str, Any] = self.family_writers.setdefault("l2", {})

        self._roll_lock = threading.Lock()
        self._roll_requests: list[tuple[Optional[str], threading.Event, dict]] = []

        self._hb_last = time.monotonic()

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def stop(self) -> None:
        self._should_run = False
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception as e:
                self.log.warning("WS close error: %s", e)
            self._ws = None
        if self.io_thread and self.io_thread.is_alive():
            self.io_thread.join(timeout=2.0)
        self.platform.close()
        for fam_map in self.family_writers.values():
            fam_map.clear()
        self.writers_by_product.clear()

    def subscribe(self, product_id: str) -> None:
        if not product_id:
            return
        pid = product_id.upper()
        specs = self._feed_specs_for_product(pid)
        product_writers: dict[str, Any] = {}
        for family, spec in specs.items():
            self.platform.create_feed(spec)
            writer = self.platform.create_writer(spec["feed_name"])
            self.family_writers.setdefault(family, {})[pid] = writer
            product_writers[family] = writer
        self.writers_by_product[pid] = product_writers
        self.product_ids.add(pid)
        self._send_subscribe((pid,))

    def unsubscribe(self, product_id: str) -> None:
        if not product_id:
            return
        pid = product_id.upper()
        self.product_ids.discard(pid)
        writers_to_close: list[Any] = []
        for fam_map in self.family_writers.values():
            writer = fam_map.pop(pid, None)
            if writer is not None:
                writers_to_close.append(writer)
        self.writers_by_product.pop(pid, None)
        closed_ids: set[int] = set()
        for writer in writers_to_close:
            wid = id(writer)
            if wid in closed_ids:
                continue
            closed_ids.add(wid)
            try:
                writer.close()
            except Exception as e:
                self.log.warning("Error closing writer for %s: %s", pid, e)
        self._send_unsubscribe((pid,))

    def request_segment_roll(self, product_id: Optional[str] = None, timeout_s: float = 3.0) -> dict:
        target = product_id.upper() if product_id else None
        done = threading.Event()
        result: dict[str, Any] = {}
        with self._roll_lock:
            self._roll_requests.append((target, done, result))
        if not (self.io_thread and self.io_thread.is_alive()):
            self._process_roll_requests()
        if not done.wait(timeout=max(0.1, float(timeout_s))):
            return {"status": "timeout", "target": target or "ALL"}
        return result

    def _process_roll_requests(self) -> None:
        with self._roll_lock:
            pending = self._roll_requests
            self._roll_requests = []
        for target, done, result in pending:
            try:
                out = self._roll_segments(target)
            except Exception as e:
                out = {"status": "error", "target": target or "ALL", "error": str(e)}
            result.update(out)
            done.set()

    @staticmethod
    def _mark_writer_boundary(writer: Any, reason: str) -> bool:
        mark = getattr(writer, "mark_segment_boundary", None)
        if callable(mark):
            return bool(mark(reason))
        store = getattr(writer, "segment_store", None)
        if store is not None:
            return bool(store.close_open_segment(reason))
        return False

    def _roll_segments(self, target: Optional[str]) -> dict:
        known = set(self.writers_by_product.keys())
        if not known:
            for fam_map in self.family_writers.values():
                known.update(fam_map.keys())
        products = sorted(known) if target is None else [target]

        trades_closed = 0
        l2_closed = 0
        other_closed = 0
        missing: list[str] = []
        for pid in products:
            family_map = self.writers_by_product.get(pid)
            if not family_map:
                missing.append(pid)
                continue
            for family, writer in family_map.items():
                if not self._mark_writer_boundary(writer, "manual_roll"):
                    continue
                if family == "trades":
                    trades_closed += 1
                elif family == "l2":
                    l2_closed += 1
                else:
                    other_closed += 1

        return {
            "status": "ok",
            "target": target or "ALL",
            "products": products,
            "trades_closed": trades_closed,
            "l2_closed": l2_closed,
            "other_closed": other_closed,
            "missing": missing,
        }

    def _mark_disconnect_boundaries(self, reason: str) -> None:
        for fam_map in self.family_writers.values():
            for writer in fam_map.values():
                try:
                    self._mark_writer_boundary(writer, reason)
                except Exception:
                    pass

    def _feed_specs_for_product(self, pid: str) -> dict[str, dict]:
        feed_specs = getattr(self.connector, "feed_specs", None)
        if callable(feed_specs):
            specs = feed_specs(pid)
            if isinstance(specs, dict) and specs:
                return specs
        return {"trades": self.connector.trades_spec(pid), "l2": self.connector.l2_spec(pid)}

    def _connect(self) -> None:
        self._ws = create_connection(
            self.uri,
            timeout=5,
            enable_multithread=True,
            sockopt=[
                (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 21),
            ],
            skip_utf8_validation=True,
        )
        self._ws.settimeout(2.0)
        self._hb_last = time.monotonic()
        self.connector.on_connect(self)

    def _send_subscribe(self, product_ids) -> None:
        if self._ws is None:
            return
        try:
            self.connector.send_subscribe(self, tuple(product_ids or ()))
        except Exception as e:
            self.log.warning("subscribe error: %s", e, exc_info=True)

    def _send_unsubscribe(self, targets) -> None:
        if self._ws is None:
            return
        try:
            self.connector.send_unsubscribe(self, tuple(targets or ()))
        except Exception as e:
            self.log.warning("unsubscribe error: %s", e, exc_info=True)

    def _io_loop(self) -> None:
        now_us = _now_us
        self._should_run = True
        for delay in _backoff():
            if not self._should_run:
                return
            try:
                self._connect()
                self.log.info("WS connected %s", self.uri)
                self._send_subscribe(self.product_ids)
                while self._should_run:
                    self._process_roll_requests()
                    try:
                        raw = self._ws.recv()
                        if not raw:
                            raise WebSocketConnectionClosedException("recv returned None/empty")
                        recv_us = now_us()
                        self._hb_last = time.monotonic()
                    except WebSocketTimeoutException:
                        self.connector.on_timeout(self)
                        continue
                    except WebSocketConnectionClosedException:
                        raise
                    except Exception as e:
                        raise WebSocketConnectionClosedException(f"WS recv error: {e!s}")

                    try:
                        self.connector.handle_raw(self, raw, recv_us, now_us)
                    except Exception as e:
                        self.log.warning("message processing error: %s", e, exc_info=True)
                return
            except WebSocketConnectionClosedException as e:
                self.log.warning("WS closed: %s", e)
                self._mark_disconnect_boundaries("ws_disconnect")
            except Exception as e:
                self.log.error("WS ERROR: %s", e, exc_info=True)
                self._mark_disconnect_boundaries("ws_error")
            finally:
                if self._ws is not None:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            if not self._should_run:
                return
            self.log.info("Reconnecting in %.2fs", delay)
            time.sleep(delay)

    def _handle_signal(self, _signum, _frame):
        try:
            self.stop()
        finally:
            raise SystemExit(0)

    def status_snapshot(self) -> dict:
        now = time.monotonic()
        out = {
            "running": self._should_run,
            "connected": self._ws is not None,
            "subs": sorted(self.product_ids),
            "hb_age": max(0.0, now - self._hb_last) if self._hb_last else None,
        }
        out.update(self.connector.extra_status(self))
        return out
