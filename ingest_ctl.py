#!/usr/bin/env python3
"""
CLI helper to control ingest daemon via its Unix socket.
Usage examples:
  ./ingest_ctl.py sub BTC-USD
  ./ingest_ctl.py unsub BTC-USD
  ./ingest_ctl.py list
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
    ap = argparse.ArgumentParser(description="Control ingest daemon")
    ap.add_argument("action", choices=["sub", "unsub", "list"], help="Command")
    ap.add_argument("product", nargs="?", help="Product ID, e.g., BTC-USD")
    ap.add_argument("--sock", default="pids/ingest.sock", help="Ingest control socket path")
    args = ap.parse_args()

    if args.action in ("sub", "unsub") and not args.product:
        ap.error("product is required for sub/unsub")

    cmd = {
        "sub": f"SUB {args.product.upper()}",
        "unsub": f"UNSUB {args.product.upper()}",
        "list": "LIST",  # not implemented in daemon, will return ERR; kept for symmetry
    }[args.action]

    resp = send(cmd, args.sock)
    print(resp)


if __name__ == "__main__":
    main()
