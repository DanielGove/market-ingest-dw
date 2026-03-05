"""Shared WebSocket ingest engine runtime."""

from __future__ import annotations

import signal
import socket
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from logging import Logger
from typing import Any

from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException, create_connection

from deepwater.platform import Platform


def _now_us() -> int:
    return time.time_ns() // 1_000


def _backoff() -> Iterator[float]:
    b = 0.5
    while True:
        yield b
        b = min(b * 2.0, 15.0)


class SharedWsIngestEngine:
    """Generic websocket ingest runtime.

    Responsibilities:
    - maintain websocket lifecycle (connect/reconnect/read loop)
    - manage product subscriptions and per-family writers

    Non-responsibilities:
    - venue protocol details (connector-owned)
    - feed schema choices (connector-owned via feed_specs)
    """

    def __init__(self, *, connector: Any, platform_base_path: str, logger: Logger) -> None:
        self.connector = connector
        self.log = logger
        self.uri = connector.uri

        self._should_run = False
        self._ws = None
        self.product_ids: set[str] = set()
        self.io_thread: threading.Thread | None = None

        self.platform = Platform(base_path=platform_base_path)
        self.family_writers: dict[str, dict[str, Any]] = {}
        self.writers_by_product: dict[str, dict[str, Any]] = {}

        self._hb_last = time.monotonic()

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def stop(self) -> None:
        """Stop websocket I/O, close writers, and release platform resources."""

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
        """Subscribe one product and initialize writers for all declared families."""

        if not product_id:
            return
        pid = product_id.upper()
        specs = self._feed_specs_for_product(pid)
        platform = self.platform
        family_writers = self.family_writers
        product_writers: dict[str, Any] = {}
        for family, spec in specs.items():
            platform.create_feed(spec)
            writer = platform.create_writer(spec["feed_name"])
            family_writers.setdefault(family, {})[pid] = writer
            product_writers[family] = writer
        self.writers_by_product[pid] = product_writers
        self.product_ids.add(pid)
        self._send_subscribe((pid,))

    def unsubscribe(self, product_id: str) -> None:
        """Unsubscribe one product and close its associated family writers."""

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

    @staticmethod
    def _mark_writer_boundary(writer: Any, reason: str) -> bool:
        """Mark a writer segment boundary using the writer-native API."""
        mark = getattr(writer, "mark_segment_boundary", None)
        if not callable(mark):
            raise TypeError("writer missing mark_segment_boundary(reason)")
        return bool(mark(reason))

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
        raise TypeError("connector must implement feed_specs(pid) returning a non-empty mapping")

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

    def _send_subscribe(self, product_ids: Sequence[str]) -> None:
        if self._ws is None:
            return
        try:
            self.connector.send_subscribe(self, tuple(product_ids or ()))
        except Exception as e:
            self.log.warning("subscribe error: %s", e, exc_info=True)

    def _send_unsubscribe(self, targets: Sequence[str]) -> None:
        if self._ws is None:
            return
        try:
            self.connector.send_unsubscribe(self, tuple(targets or ()))
        except Exception as e:
            self.log.warning("unsubscribe error: %s", e, exc_info=True)

    def _io_loop(self) -> None:
        now_us = _now_us
        monotonic = time.monotonic
        connector = self.connector
        self._should_run = True
        for delay in _backoff():
            if not self._should_run:
                return
            try:
                self._connect()
                self.log.info("WS connected %s", self.uri)
                self._send_subscribe(self.product_ids)

                ws = self._ws
                if ws is None:
                    raise WebSocketConnectionClosedException("WS missing after connect")
                recv = ws.recv
                on_timeout = connector.on_timeout
                handle_raw = connector.handle_raw

                while self._should_run:
                    try:
                        raw = recv()
                        if not raw:
                            raise WebSocketConnectionClosedException("recv returned None/empty")
                        recv_us = now_us()
                        self._hb_last = monotonic()
                    except WebSocketTimeoutException:
                        on_timeout(self)
                        continue
                    except WebSocketConnectionClosedException:
                        raise
                    except Exception as e:
                        raise WebSocketConnectionClosedException(f"WS recv error: {e!s}")

                    try:
                        handle_raw(self, raw, recv_us, now_us)
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

    def _handle_signal(self, _signum: int, _frame: Any) -> None:
        try:
            self.stop()
        finally:
            raise SystemExit(0)

    def status_snapshot(self) -> dict[str, Any]:
        """Return control-plane status payload for STATUS command."""

        now = time.monotonic()
        out = {
            "running": self._should_run,
            "connected": self._ws is not None,
            "venue": str(getattr(self.connector, "venue", "") or "unknown"),
            "subs": sorted(self.product_ids),
            "hb_age": max(0.0, now - self._hb_last) if self._hb_last else None,
        }
        out.update(self.connector.extra_status(self))
        return out
