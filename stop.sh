#!/usr/bin/env bash
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

kill_if_exists() {
  if [ -f "$1" ]; then
    PID=$(cat "$1")
    if kill -0 "$PID" 2>/dev/null; then
      kill "$PID"
      echo "Stopped $1 (PID $PID)"
    else
      echo "No running process for $1 (PID $PID stale)"
    fi
    rm -f "$1"
  else
    echo "No pid file $1"
  fi
}

kill_if_exists pids/ingest.pid
kill_if_exists pids/snapshots.pid
