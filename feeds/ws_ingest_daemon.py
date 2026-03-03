#!/usr/bin/env python3
"""Unified WebSocket ingest daemon wrapper (venue-selected via env/--venue)."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path

from runtime.ingest.market_engine import ConfiguredMarketDataEngine
from runtime.ingest.ws_ingest_daemon import WebSocketIngestDaemonRuntime
from runtime.venues import get_venue_defaults, resolve_base_path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ws_ingest_daemon")
ws_log = logging.getLogger("dw.ws")


def _select_venue(argv: list[str]) -> tuple[str, list[str]]:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--venue")
    args, rest = ap.parse_known_args(argv)
    venue_key = args.venue or os.environ.get("DW_VENUE_KEY") or os.environ.get("DW_VENUE") or "coinbase"
    return venue_key, rest


def main() -> None:
    venue_key, argv = _select_venue(sys.argv[1:])
    venue = get_venue_defaults(venue_key)

    root_dir = Path(__file__).resolve().parent.parent
    default_base_path = resolve_base_path(venue, root_dir)

    parser = argparse.ArgumentParser(description=venue.ingest_description)
    parser.add_argument(
        "--products",
        type=str,
        default=venue.default_products_csv,
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
        default=str(Path(__file__).resolve().parent / "pids" / "ingest.sock"),
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
        default=venue.kraken_book_depth_default or 1000,
        help="Kraken book depth (ignored by non-Kraken venues)",
    )
    args = parser.parse_args(argv)

    products = [p.strip().upper() for p in args.products.split(",") if p.strip()]

    def _engine_factory() -> ConfiguredMarketDataEngine:
        kwargs = {
            "venue_key": venue_key,
            "logger": ws_log,
            "uri": args.uri,
        }
        if venue_key == "kraken":
            kwargs["book_depth"] = int(max(10, args.book_depth))
        return ConfiguredMarketDataEngine(**kwargs)

    daemon = WebSocketIngestDaemonRuntime(
        products=products,
        base_path=args.base_path,
        control_sock=args.control_sock,
        engine_factory=_engine_factory,
        log=log,
    )

    def signal_handler(signum, _frame):
        log.info("Received signal %s", signum)
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    daemon.start()


if __name__ == "__main__":
    main()
