#!/usr/bin/env python3
import sys
import socket
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        print("Usage: ingest_ctl.py <SUB|UNSUB> <PRODUCT> [socket_path]", file=sys.stderr)
        sys.exit(1)
    cmd, product = sys.argv[1], sys.argv[2]
    sock_path = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("pids/ingest.sock")
    payload = f"{cmd.upper()} {product.upper()}".encode()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(str(sock_path))
        s.sendall(payload)
        resp = s.recv(1024)
    sys.stdout.write(resp.decode().strip() + "\n")

if __name__ == "__main__":
    main()
