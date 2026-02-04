#!/usr/bin/env python3
import sys
import socket
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        print("Usage: ob_ctl.py <ADD|REMOVE> <PRODUCT> [DEPTH PERIOD_MS] [socket_path]", file=sys.stderr)
        sys.exit(1)
    cmd = sys.argv[1].upper()
    product = sys.argv[2].upper()
    depth = None
    period = None
    sock_path = Path("pids/orderbook.sock")
    # ADD requires depth/period
    argi = 3
    if cmd == "ADD":
        if len(sys.argv) < 5:
            print("Usage: ob_ctl.py ADD <PRODUCT> <DEPTH> <PERIOD_MS> [socket_path]", file=sys.stderr)
            sys.exit(1)
        depth = int(sys.argv[3])
        period = int(sys.argv[4])
        argi = 5
    if len(sys.argv) > argi:
        sock_path = Path(sys.argv[argi])
    if cmd == "ADD":
        payload = f"{cmd} {product} {depth} {period}".encode()
    else:
        payload = f"{cmd} {product}".encode()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(sock_path))
        s.sendall(payload)
        resp = s.recv(1024)
    sys.stdout.write(resp.decode().strip() + "\n")

if __name__ == "__main__":
    main()
