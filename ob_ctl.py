#!/usr/bin/env python3
"""
CLI helper to control orderbook daemon.
Usage:
  ./ob_ctl.py add BTC-USD 200 200
  ./ob_ctl.py remove BTC-USD
  ./ob_ctl.py list
"""
import argparse
import socket


def send(cmd: str, sock_path: str) -> str:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall((cmd + "\n").encode("ascii"))
    data = s.recv(1024).decode("ascii", "ignore").strip()
    s.close()
    return data


def main():
    ap = argparse.ArgumentParser(description="Control orderbook daemon")
    sub = ap.add_subparsers(dest="action", required=True)

    p_add = sub.add_parser("add", help="Add product depth period")
    p_add.add_argument("product")
    p_add.add_argument("depth", type=int)
    p_add.add_argument("period", type=int)

    p_rm = sub.add_parser("remove", help="Remove product")
    p_rm.add_argument("product")

    sub.add_parser("list", help="List products")

    ap.add_argument("--sock", default="pids/orderbook.sock", help="Orderbook control socket path")
    args = ap.parse_args()

    if args.action == "add":
        cmd = f"ADD {args.product.upper()} {args.depth} {args.period}"
    elif args.action == "remove":
        cmd = f"REMOVE {args.product.upper()}"
    else:
        cmd = "LIST"

    resp = send(cmd, args.sock)
    print(resp)


if __name__ == "__main__":
    main()
