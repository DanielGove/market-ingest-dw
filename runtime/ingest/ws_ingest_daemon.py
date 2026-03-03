"""Shared websocket ingest daemon runtime."""

from __future__ import annotations

import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable

from deepwater.platform import Platform


class WebSocketIngestDaemonRuntime:
    def __init__(
        self,
        *,
        products: list[str],
        base_path: str,
        control_sock: str,
        engine_factory: Callable[[], Any],
        log,
    ):
        self.products = [p.upper() for p in products]
        self.base_path = base_path
        self.control_sock = control_sock
        self.engine_factory = engine_factory
        self.log = log

        self.engine = None
        self.running = False
        self._ctl_thread: threading.Thread | None = None

    def start(self):
        self.log.info("Starting WebSocket ingest daemon for %s", self.products)
        self.log.info("Base path: %s", self.base_path)

        self.engine = self.engine_factory()

        try:
            self.engine.platform.close()
        except Exception as e:
            self.log.warning("Error closing default platform: %s", e)

        self.engine.trade_writers.clear()
        self.engine.book_writers.clear()
        self.engine.product_ids.clear()
        self.engine.platform = Platform(base_path=self.base_path)

        for product in self.products:
            self.log.info("Subscribing to %s...", product)
            self.engine.subscribe(product)

        self.engine._should_run = True
        self.engine.io_thread = threading.Thread(target=self.engine._io_loop, name="ws-io", daemon=True)
        self.engine.io_thread.start()

        self.running = True
        self.log.info("WebSocket engine started successfully")
        self._start_control()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.log.info("Received interrupt signal")
        finally:
            self.stop()

    def stop(self):
        if self.engine and self.running:
            self.log.info("Stopping WebSocket engine...")
            self.running = False
            self._stop_control()
            self.engine.stop()
            self.log.info("Engine stopped")

    def _start_control(self):
        sock_path = Path(self.control_sock)
        sock_path.parent.mkdir(parents=True, exist_ok=True)
        if sock_path.exists():
            try:
                sock_path.unlink()
            except Exception:
                pass

        def loop():
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(str(sock_path))
            srv.listen(1)
            srv.settimeout(1.0)
            self.log.info("Control socket listening at %s", sock_path)
            while self.running:
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        self.log.warning("Control accept error: %s", e)
                    break
                with conn:
                    try:
                        data = conn.recv(1024).decode("ascii", "ignore").strip()
                        if not data:
                            continue
                        parts = data.split()
                        cmd = parts[0].upper()
                        if cmd == "SUB" and len(parts) == 2:
                            self.engine.subscribe(parts[1].upper())
                            conn.sendall(b"OK\n")
                        elif cmd == "UNSUB" and len(parts) == 2:
                            self.engine.unsubscribe(parts[1].upper())
                            conn.sendall(b"OK\n")
                        elif cmd == "LIST":
                            subs = sorted(self.engine.product_ids)
                            conn.sendall((", ".join(subs) + "\n").encode("ascii"))
                        elif cmd == "ROLL" and len(parts) in (1, 2):
                            target = parts[1].upper() if len(parts) == 2 and parts[1].upper() != "ALL" else None
                            result = self.engine.request_segment_roll(target, timeout_s=3.0)
                            if result.get("status") == "ok":
                                conn.sendall(
                                    (
                                        "OK target={target} trades_closed={trades_closed} l2_closed={l2_closed} "
                                        "missing={missing}\n"
                                    ).format(
                                        target=result.get("target", "ALL"),
                                        trades_closed=result.get("trades_closed", 0),
                                        l2_closed=result.get("l2_closed", 0),
                                        missing=",".join(result.get("missing", [])),
                                    ).encode("ascii", "ignore")
                                )
                            else:
                                conn.sendall(
                                    ("ERR status={status} target={target}\n").format(
                                        status=result.get("status", "error"),
                                        target=result.get("target", "ALL"),
                                    ).encode("ascii", "ignore")
                                )
                        else:
                            conn.sendall(b"ERR unknown\n")
                    except Exception as e:
                        self.log.warning("Control cmd error: %s", e, exc_info=True)
            try:
                srv.close()
            except Exception:
                pass
            try:
                sock_path.unlink()
            except Exception:
                pass

        self._ctl_thread = threading.Thread(target=loop, name="ingest-ctl", daemon=True)
        self._ctl_thread.start()

    def _stop_control(self):
        self.running = False
        if self._ctl_thread and self._ctl_thread.is_alive():
            self._ctl_thread.join(timeout=2.0)
