#!/bin/bash
# Stop all pipeline processes

echo "🛑 Stopping Coinbase data pipeline..."

for pidfile in pids/*.pid; do
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        name=$(basename "$pidfile" .pid)
        
        if kill -0 "$pid" 2>/dev/null; then
            echo "   Stopping $name (PID: $pid)..."
            kill "$pid"
            sleep 1
            
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                echo "   Force stopping $name..."
                kill -9 "$pid"
            fi
        fi
        
        rm "$pidfile"
    fi
done

echo "✅ Pipeline stopped"
