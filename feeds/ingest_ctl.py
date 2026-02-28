#!/usr/bin/env python3
"""
CLI helper to control ingest daemon via its Unix socket.
Usage examples:
  ./feeds/ingest_ctl.py sub BTC-USD
  ./feeds/ingest_ctl.py unsub BTC-USD
  ./feeds/ingest_ctl.py list
  ./feeds/ingest_ctl.py roll
  ./feeds/ingest_ctl.py roll BTC-USD
"""
import argparse
import socket
import sys
from pathlib import Path


def send(cmd: str, sock_path: str) -> str:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall((cmd + "\n").encode("ascii"))
    data = s.recv(1024).decode("ascii", "ignore").strip()
    s.close()
    return data


def main():
    script_dir = Path(__file__).resolve().parent
    default_sock = script_dir / "pids" / "ingest.sock"
    ap = argparse.ArgumentParser(description="Control ingest daemon")
    ap.add_argument("action", choices=["sub", "unsub", "list", "roll"], help="Command")
    ap.add_argument("product", nargs="?", help="Product ID, e.g., BTC-USD")
    ap.add_argument("--sock", default=str(default_sock), help="Ingest control socket path")
    args = ap.parse_args()

    if args.action in ("sub", "unsub") and not args.product:
        ap.error("product is required for sub/unsub")

    if args.action == "list":
        cmd = "LIST"
    elif args.action == "sub":
        cmd = f"SUB {args.product.upper()}"
    elif args.action == "roll":
        cmd = f"ROLL {args.product.upper()}" if args.product else "ROLL"
    else:
        cmd = f"UNSUB {args.product.upper()}"

    resp = send(cmd, args.sock)
    print(resp)


if __name__ == "__main__":
    main()
