#!/bin/bash
# Start Coinbase data pipeline: L2 ingest + orderbook construction

set -e

# Configuration (no defaults; must be provided)
PRODUCTS="${PRODUCTS}"
BASE_PATH="${BASE_PATH}"
# ORDERBOOKS format: "depth:period,depth:period" e.g. "200:50,200:200"
ORDERBOOKS="${ORDERBOOKS}"
VENV="./venv/bin/python"
INGEST_SOCK="pids/ingest.sock"

mkdir -p logs pids

if [ -z "$PRODUCTS" ] || [ -z "$BASE_PATH" ] || [ -z "$ORDERBOOKS" ]; then
    echo "Usage: set PRODUCTS, BASE_PATH, ORDERBOOKS env vars."
    echo "ORDERBOOKS format: \"depth:period,depth:period\""
    echo "Example: PRODUCTS=\"BTC-USD,XRP-USD\" BASE_PATH=data/coinbase-main ORDERBOOKS=\"200:50,200:200\" ./run.sh"
    exit 1
fi

echo "🚀 Starting Coinbase data pipeline"
echo "📁 Base path: $BASE_PATH"
echo "📊 Products: $PRODUCTS"
echo "📈 Orderbooks: $ORDERBOOKS"
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

IFS=',' read -ra OB_CFG <<< "$ORDERBOOKS"
ob_index=0
for cfg in "${OB_CFG[@]}"; do
    depth=${cfg%%:*}
    period=${cfg##*:}
    FREQ_HZ=$(echo "scale=1; 1000 / $period" | bc)
    echo "▶️  Starting orderbook daemon OB${depth}${period} @ ${period}ms (${FREQ_HZ}Hz)..."
    nohup $VENV orderbook_daemon.py \
        --products "$PRODUCTS" \
        --base-path "$BASE_PATH" \
        --depth "$depth" \
        --period "$period" \
        --control-sock "pids/orderbook_${depth}_${period}.sock" \
        > "logs/orderbook_${depth}_${period}.log" 2>&1 &
    echo $! > "pids/orderbook_${depth}_${period}.pid"
    echo "   ✅ Orderbook daemon started (PID: $(cat pids/orderbook_${depth}_${period}.pid))"
    ob_index=$((ob_index+1))
done

echo ""
echo "✅ Pipeline running!"
echo ""
echo "📊 Monitor:"
echo "   tail -f logs/ingest.log"
echo "   tail -f logs/orderbook_*.log"
echo ""
echo "🛑 Stop:"
echo "   ./stop.sh"
