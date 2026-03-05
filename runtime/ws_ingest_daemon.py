"""Shared websocket ingest daemon runtime."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import socket
import sys
import threading
import time
from logging import Logger
from pathlib import Path
from typing import Any, Callable

from deepwater.platform import Platform


class WebSocketIngestDaemonRuntime:
    """Process-level runtime that manages engine lifecycle and control socket."""

    def __init__(
        self,
        *,
        products: list[str],
        base_path: str,
        control_sock: str,
        engine_factory: Callable[[], Any],
        log: Logger,
    ) -> None:
        self.products = [p.upper() for p in products]
        self.base_path = base_path
        self.control_sock = control_sock
        self.engine_factory = engine_factory
        self.log = log

        self.engine: Any | None = None
        self.running = False
        self._ctl_thread: threading.Thread | None = None

    def start(self) -> None:
        """Initialize engine state, start I/O thread, and serve control commands."""

        self.log.info("Starting WebSocket ingest daemon for %s", self.products)
        self.log.info("Base path: %s", self.base_path)

        self.engine = self.engine_factory()

        try:
            self.engine.platform.close()
        except Exception as e:
            self.log.warning("Error closing default platform: %s", e)

        self.engine.family_writers.clear()
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

    def stop(self) -> None:
        """Stop control loop and engine cleanly if running."""

        if self.engine and self.running:
            self.log.info("Stopping WebSocket engine...")
            self.running = False
            self._stop_control()
            self.engine.stop()
            self.log.info("Engine stopped")

    def _start_control(self) -> None:
        self._ctl_thread = threading.Thread(target=self._serve_control, name="ingest-ctl", daemon=True)
        self._ctl_thread.start()

    def _stop_control(self) -> None:
        self.running = False
        if self._ctl_thread and self._ctl_thread.is_alive():
            self._ctl_thread.join(timeout=2.0)

    def _serve_control(self) -> None:
        sock_path = Path(self.control_sock)
        sock_path.parent.mkdir(parents=True, exist_ok=True)
        if sock_path.exists():
            try:
                sock_path.unlink()
            except Exception:
                pass

        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
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
                self._handle_control_connection(conn)
        finally:
            try:
                srv.close()
            except Exception:
                pass
            try:
                sock_path.unlink()
            except Exception:
                pass

    def _handle_control_connection(self, conn: socket.socket) -> None:
        with conn:
            try:
                data = conn.recv(1024).decode("ascii", "ignore").strip()
                if not data:
                    return
                response = self._handle_control_command(data)
                conn.sendall((response + "\n").encode("ascii", "ignore"))
            except Exception as e:
                self.log.warning("Control cmd error: %s", e, exc_info=True)

    def _handle_control_command(self, command: str) -> str:
        """Handle single-line control command and return a line-safe response."""

        parts = command.split()
        cmd = parts[0].upper()
        if cmd == "SUB" and len(parts) == 2:
            self.engine.subscribe(parts[1].upper())
            return "OK"
        if cmd == "UNSUB" and len(parts) == 2:
            self.engine.unsubscribe(parts[1].upper())
            return "OK"
        if cmd == "LIST":
            return ", ".join(sorted(self.engine.product_ids))
        if cmd == "STATUS":
            return json.dumps(self.engine.status_snapshot(), sort_keys=True)
        return "ERR unknown"


def _select_venue(argv: list[str]) -> tuple[str, list[str]]:
    from runtime.venue import venue_key_from_env

    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--venue")
    args, rest = ap.parse_known_args(argv)
    if args.venue:
        venue_key = args.venue.strip().lower()
    else:
        venue_key = venue_key_from_env(default=None)
    return venue_key, rest


def main() -> None:
    from runtime.engine_factory import create_configured_ws_engine
    from runtime.venue import make_venue, resolve_base_path

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("ws_ingest_daemon")
    ws_log = logging.getLogger("dw.ws")

    venue_key, argv = _select_venue(sys.argv[1:])
    venue = make_venue(venue_key)

    root_dir = Path(__file__).resolve().parent.parent
    default_base_path = resolve_base_path(venue, root_dir)

    parser = argparse.ArgumentParser(description=venue.ingest_description)
    parser.add_argument(
        "--products",
        type=str,
        default=venue.default_products,
        help="Comma-separated list of products (e.g., XRP-USD,BTC-USD)",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=default_base_path,
        help="Base path for data storage",
    )
    parser.add_argument(
        "--control-sock",
        type=str,
        default=str(root_dir / "ops" / "pids" / "ingest.sock"),
        help="Unix domain socket path for control commands",
    )
    parser.add_argument(
        "--uri",
        type=str,
        default=venue.websocket_uri,
        help="WebSocket endpoint URI",
    )
    parser.add_argument(
        "--book-depth",
        type=int,
        default=1000,
        help="Kraken book depth (ignored by non-Kraken venues)",
    )
    args = parser.parse_args(argv)

    products = [p.strip().upper() for p in args.products.split(",") if p.strip()]

    def _engine_factory():
        kwargs = {
            "venue_key": venue_key,
            "logger": ws_log,
            "uri": args.uri,
        }
        if venue_key == "kraken":
            kwargs["book_depth"] = int(max(10, args.book_depth))
        return create_configured_ws_engine(**kwargs)

    daemon = WebSocketIngestDaemonRuntime(
        products=products,
        base_path=args.base_path,
        control_sock=args.control_sock,
        engine_factory=_engine_factory,
        log=log,
    )

    def signal_handler(signum: int, _frame: Any) -> None:
        log.info("Received signal %s", signum)
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    daemon.start()


if __name__ == "__main__":
    main()
