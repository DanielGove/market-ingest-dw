#!/bin/bash
# Start a single orderbook daemon with its own socket/log/pid.
# Usage: ./start_ob.sh <depth> <period_ms> <products> [base_path]
# Example: ./start_ob.sh 200 50 "BTC-USD,XRP-USD,ETH-USD" data/coinbase-main

set -e

DEPTH="$1"
PERIOD="$2"
PRODUCTS="$3"
BASE_PATH="${4:-data/coinbase-main}"
VENV="./venv/bin/python"

if [ -z "$DEPTH" ] || [ -z "$PERIOD" ] || [ -z "$PRODUCTS" ]; then
  echo "Usage: $0 <depth> <period_ms> <products> [base_path]"
  exit 1
fi

mkdir -p logs pids

SOCK="pids/orderbook_${DEPTH}_${PERIOD}.sock"
LOG="logs/orderbook_${DEPTH}_${PERIOD}.log"
PIDFILE="pids/orderbook_${DEPTH}_${PERIOD}.pid"

echo "▶️  Starting orderbook daemon OB${DEPTH}${PERIOD} for ${PRODUCTS}..."
nohup $VENV orderbook_daemon.py \
  --products "$PRODUCTS" \
  --base-path "$BASE_PATH" \
  --depth "$DEPTH" \
  --period "$PERIOD" \
  --control-sock "$SOCK" \
  > "$LOG" 2>&1 &

echo $! > "$PIDFILE"
echo "   ✅ PID: $(cat $PIDFILE)  sock: $SOCK  log: $LOG"
