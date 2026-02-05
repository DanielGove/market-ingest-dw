#!/bin/bash
# Start Coinbase data pipeline: L2 ingest + orderbook construction

set -e

# Configuration (no defaults; must be provided)
PRODUCTS="${PRODUCTS}"
BASE_PATH="${BASE_PATH}"
OB_DEPTH="${OB_DEPTH}"
OB_PERIOD="${OB_PERIOD}"
VENV="./venv/bin/python"
INGEST_SOCK="pids/ingest.sock"
ORDERBOOK_SOCK="pids/orderbook.sock"

mkdir -p logs pids

if [ -z "$PRODUCTS" ] || [ -z "$BASE_PATH" ] || [ -z "$OB_DEPTH" ] || [ -z "$OB_PERIOD" ]; then
    echo "Usage: set PRODUCTS, BASE_PATH, OB_DEPTH, OB_PERIOD env vars."
    echo "Example: PRODUCTS=\"BTC-USD,XRP-USD\" BASE_PATH=data/coinbase-main OB_DEPTH=200 OB_PERIOD=50 ./run.sh"
    exit 1
fi

echo "🚀 Starting Coinbase data pipeline"
echo "📁 Base path: $BASE_PATH"
echo "📊 Products: $PRODUCTS"
FREQ_HZ=$(echo "scale=1; 1000 / $OB_PERIOD" | bc)
echo "📈 Orderbook: OB${OB_DEPTH}${OB_PERIOD} @ ${OB_PERIOD}ms (${FREQ_HZ}Hz)"
echo ""

# Start L2 WebSocket ingest
echo "▶️  Starting L2 ingest..."
nohup $VENV ws_ingest_daemon.py \
    --products "$PRODUCTS" \
    --base-path "$BASE_PATH" \
    --control-sock "$INGEST_SOCK" \
    > logs/ingest.log 2>&1 &
echo $! > pids/ingest.pid
echo "   ✅ L2 ingest started (PID: $(cat pids/ingest.pid))"

# Wait for feeds to be created
echo "⏳ Waiting 1s for L2 feeds..."
sleep 1

if ! kill -0 $(cat pids/ingest.pid) 2>/dev/null; then
    echo "❌ L2 ingest failed! Check logs/ingest.log"
    exit 1
fi

# Start orderbook daemon
echo "▶️  Starting orderbook daemon..."
nohup $VENV orderbook_daemon.py \
    --products "$PRODUCTS" \
    --base-path "$BASE_PATH" \
    --depth "$OB_DEPTH" \
    --period "$OB_PERIOD" \
    --control-sock "$ORDERBOOK_SOCK" \
    > logs/orderbook.log 2>&1 &
echo $! > pids/orderbook.pid
echo "   ✅ Orderbook daemon started (PID: $(cat pids/orderbook.pid))"

echo ""
echo "✅ Pipeline running!"
echo ""
echo "📊 Monitor:"
echo "   tail -f logs/ingest.log"
echo "   tail -f logs/orderbook.log"
echo ""
echo "🛑 Stop:"
echo "   ./stop.sh"
