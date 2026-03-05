#!/usr/bin/env python3
"""
CLI helper to control ingest daemon via its Unix socket.
Usage examples:
    ./ops/ingest_ctl sub BTC-USD
    ./ops/ingest_ctl unsub BTC-USD
    ./ops/ingest_ctl list
    ./ops/ingest_ctl status
"""

from __future__ import annotations

import argparse
import os
import socket
from pathlib import Path


def _default_sock(root_dir: Path, instance: str) -> Path:
    return root_dir / "ops" / "pids" / f"ingest.{instance}.sock"


def send(cmd: str, sock_path: str) -> str:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall((cmd + "\n").encode("ascii"))
    data = s.recv(1024).decode("ascii", "ignore").strip()
    s.close()
    return data


def main() -> None:
    root_dir = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description="Control ingest daemon")
    ap.add_argument("action", choices=["sub", "unsub", "list", "status"], help="Command")
    ap.add_argument("product", nargs="?", help="Product ID, e.g., BTC-USD")
    ap.add_argument(
        "--instance",
        default=None,
        help="Instance name (default: DW_INSTANCE or DW_VENUE or 'default')",
    )
    ap.add_argument("--sock", default=None, help="Ingest control socket path override")
    args = ap.parse_args()

    default_instance = (
        os.environ.get("DW_INSTANCE")
        or os.environ.get("DW_VENUE")
        or "default"
    )
    instance = (args.instance or default_instance).strip() or "default"
    sock_path = args.sock or str(_default_sock(root_dir, instance))

    if args.action in ("sub", "unsub") and not args.product:
        ap.error("product is required for sub/unsub")

    if args.action == "list":
        cmd = "LIST"
    elif args.action == "status":
        cmd = "STATUS"
    elif args.action == "sub":
        cmd = f"SUB {args.product.upper()}"
    else:
        cmd = f"UNSUB {args.product.upper()}"

    print(send(cmd, sock_path))


if __name__ == "__main__":
    main()