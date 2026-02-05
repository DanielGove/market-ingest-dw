#!/usr/bin/env python3
"""
CLI helper to control a specific orderbook daemon.
Usage:
  ./ob_ctl.py 200-50 add BTC-USD
  ./ob_ctl.py 200-50 remove BTC-USD
  ./ob_ctl.py 200-50 list
By default targets socket pids/orderbook_200_50.sock (override with --sock).
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


def parse_target(target: str) -> str:
    target = target.replace(":", "-")
    parts = target.split("-")
    if len(parts) != 2:
        raise ValueError("target must look like depth-period, e.g., 200-50")
    depth, period = parts
    return f"pids/orderbook_{depth}_{period}.sock"


def main():
    ap = argparse.ArgumentParser(description="Control orderbook daemon")
    ap.add_argument("target", help="Depth-period selector, e.g., 200-50")
    sub = ap.add_subparsers(dest="action", required=True)

    p_add = sub.add_parser("add", help="Add product")
    p_add.add_argument("product")

    p_rm = sub.add_parser("remove", help="Remove product")
    p_rm.add_argument("product")

    sub.add_parser("list", help="List products")

    ap.add_argument("--sock", help="Override socket path (otherwise derived from target)")
    args = ap.parse_args()

    sock_path = args.sock or parse_target(args.target)

    if args.action == "add":
        cmd = f"ADD {args.product.upper()} 0 0"  # depth/period unused by daemon; provided in target daemon config
    elif args.action == "remove":
        cmd = f"REMOVE {args.product.upper()}"
    else:
        cmd = "LIST"

    resp = send(cmd, sock_path)
    print(resp)


if __name__ == "__main__":
    main()
