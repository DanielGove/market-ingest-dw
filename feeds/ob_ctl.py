#!/usr/bin/env python3
"""
CLI helper to control a specific orderbook daemon.
Usage:
  ./feeds/ob_ctl.py add BTC-USD
  ./feeds/ob_ctl.py remove BTC-USD
  ./feeds/ob_ctl.py list
  ./feeds/ob_ctl.py roll
  ./feeds/ob_ctl.py roll BTC-USD
  ./feeds/ob_ctl.py 200-50 list
By default targets socket pids/orderbook.sock (override with --sock).
If that socket is absent and worker sockets (orderbook_*.sock) exist,
`list` and `roll` fan out across workers.
"""
import argparse
import socket
from pathlib import Path


def send(cmd: str, sock_path: str) -> str:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(sock_path)
    s.sendall((cmd + "\n").encode("ascii"))
    data = s.recv(1024).decode("ascii", "ignore").strip()
    s.close()
    return data


def parse_target(target: str, base_dir: Path) -> str:
    target = target.replace(":", "-")
    parts = target.split("-")
    if len(parts) != 2:
        raise ValueError("target must look like depth-period, e.g., 200-50")
    depth, period = parts
    return str(base_dir / f"orderbook_{depth}_{period}.sock")


def worker_sockets(base_dir: Path) -> list[str]:
    return sorted(str(p) for p in base_dir.glob("orderbook_*.sock"))


def fanout(cmd: str, sock_paths: list[str]) -> list[str]:
    out: list[str] = []
    for path in sock_paths:
        try:
            resp = send(cmd, path)
            if resp:
                out.append(resp)
        except Exception:
            continue
    return out


def main():
    ap = argparse.ArgumentParser(description="Control orderbook daemon")
    ap.add_argument("target", nargs="?", default="", help="Optional depth-period selector, e.g., 200-50")
    sub = ap.add_subparsers(dest="action", required=True)

    p_add = sub.add_parser("add", help="Add product")
    p_add.add_argument("product")

    p_rm = sub.add_parser("remove", help="Remove product")
    p_rm.add_argument("product")

    sub.add_parser("list", help="List products")
    p_roll = sub.add_parser("roll", help="Close current segment(s)")
    p_roll.add_argument("product", nargs="?", help="Optional product; omit for all products on target socket")

    ap.add_argument("--sock", help="Override socket path (otherwise derived from target)")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    pid_dir = script_dir / "pids"

    if args.action == "add":
        cmd = f"ADD {args.product.upper()} 0 0"  # depth/period unused by daemon; provided in target daemon config
    elif args.action == "remove":
        cmd = f"REMOVE {args.product.upper()}"
    elif args.action == "roll":
        cmd = f"ROLL {args.product.upper()}" if args.product else "ROLL"
    else:
        cmd = "LIST"

    sock_path = args.sock or (parse_target(args.target, pid_dir) if args.target else str(pid_dir / "orderbook.sock"))

    if not args.sock and not args.target and not Path(sock_path).exists():
        worker_paths = worker_sockets(pid_dir)
        if not worker_paths:
            raise FileNotFoundError(f"no orderbook control sockets found in {pid_dir}")
        if args.action in ("add", "remove"):
            raise RuntimeError("add/remove require --sock or <depth-period> target when using worker sockets")
        responses = fanout(cmd, worker_paths)
        if args.action == "list":
            products: set[str] = set()
            for resp in responses:
                for part in resp.split(","):
                    p = part.strip()
                    if p:
                        products.add(p)
            print(",".join(sorted(products)))
        else:
            for resp in responses:
                print(resp)
        return

    resp = send(cmd, sock_path)
    print(resp)


if __name__ == "__main__":
    main()
