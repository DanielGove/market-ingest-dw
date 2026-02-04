#!/usr/bin/env bash
# One-time setup for coinbase-dw on a fresh clone.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

echo "==> Ensuring virtualenv"
if [ ! -d venv ]; then
  python3 -m venv venv
fi

echo "==> Installing requirements"
./venv/bin/pip install --upgrade pip >/dev/null
./venv/bin/pip install -r requirements.txt

echo "==> Marking helper scripts executable"
chmod +x run.sh stop.sh ingest_ctl.py ob_ctl.py watch_ob.py

echo "==> Done. Start pipeline with:"
echo "    ./run.sh"
echo "  Control sockets (after run.sh):"
echo "    ./ingest_ctl.py SUB BTC-USD"
echo "    ./ob_ctl.py ADD BTC-USD 200 200"
