#!/usr/bin/env bash
set -e

BASE="/data/coinbase-test"
PRODS="BTC-USD ETH-USD SOL-USD XRP-USD DOGE-USD MON-USD ADA-USD AAVE-USD SUI-USD LINK-USD HBAR-USD"
BOOK_DEPTH=500
INTERVAL=1.0
STAMP=$(date +"%Y%m%d_%H%M%S")

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
source venv/bin/activate

nohup taskset -c 0 python ingest.py --base-path "$BASE" --products $PRODS > "logs/ingest_${STAMP}.log" 2>&1 &
echo $! > pids/ingest.pid
nohup taskset -c 1 python snapshots.py --base-path "$BASE" --products $PRODS --depth $BOOK_DEPTH --interval $INTERVAL > "logs/snapshots_${STAMP}.log" 2>&1 &
echo $! > pids/snapshots.pid

echo "Ingest PID: $(cat pids/ingest.pid)"
echo "Snapshots PID: $(cat pids/snapshots.pid)"
