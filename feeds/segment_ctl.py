#!/usr/bin/env python3
"""
Manual segment control for live feed daemons.

Examples:
  ./feeds/segment_ctl.py roll
  ./feeds/segment_ctl.py roll --product BTC-USD
  ./feeds/segment_ctl.py roll --service orderbook
"""
from __future__ import annotations

import argparse
import socket
from pathlib import Path


def _send(sock_path: Path, cmd: str, timeout: float = 2.0) -> str:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect(str(sock_path))
    s.sendall((cmd + "\n").encode("ascii"))
    data = s.recv(4096).decode("ascii", "ignore").strip()
    s.close()
    return data


def _discover_orderbook_socks(pid_dir: Path) -> list[Path]:
    workers = sorted(pid_dir.glob("orderbook_*.sock"))
    if workers:
        return workers
    single = pid_dir / "orderbook.sock"
    return [single] if single.exists() else []


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    pid_dir = script_dir / "pids"

    ap = argparse.ArgumentParser(description="Manual segment control for ingest/orderbook daemons")
    ap.add_argument("action", choices=["roll"], help="Action")
    ap.add_argument("--product", help="Optional product symbol, e.g. BTC-USD")
    ap.add_argument("--service", choices=["all", "ingest", "orderbook"], default="all")
    ap.add_argument("--ingest-sock", default=str(pid_dir / "ingest.sock"))
    ap.add_argument(
        "--orderbook-socks",
        help="Comma-separated override socket list (default: auto-discover orderbook_*.sock)",
    )
    args = ap.parse_args()

    cmd = f"ROLL {args.product.upper()}" if args.product else "ROLL"
    results: list[tuple[str, str]] = []

    if args.service in ("all", "ingest"):
        ingest_sock = Path(args.ingest_sock)
        if ingest_sock.exists():
            try:
                results.append((str(ingest_sock), _send(ingest_sock, cmd)))
            except Exception as e:
                results.append((str(ingest_sock), f"ERR {e}"))
        else:
            results.append((str(ingest_sock), "ERR missing socket"))

    if args.service in ("all", "orderbook"):
        if args.orderbook_socks:
            sockets = [Path(x.strip()) for x in args.orderbook_socks.split(",") if x.strip()]
        else:
            sockets = _discover_orderbook_socks(pid_dir)

        if not sockets:
            results.append(("orderbook", "ERR no sockets found"))
        for sp in sockets:
            try:
                results.append((str(sp), _send(sp, cmd)))
            except Exception as e:
                results.append((str(sp), f"ERR {e}"))

    for target, resp in results:
        print(f"{target}: {resp}")


if __name__ == "__main__":
    main()
