#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COINBASE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BSC_ROOT="${BSC_ROOT:-$(cd "$COINBASE_ROOT/.." && pwd)/bsc-dw}"

COINBASE_BASE_PATH="${COINBASE_BASE_PATH:-/deepwater/data/coinbase-advanced}"
BSC_BASE_PATH="${BSC_BASE_PATH:-/deepwater/data/bsc-ps}"

is_alive() {
  local pidfile="$1"
  if [ ! -f "$pidfile" ]; then
    return 1
  fi
  local pid
  pid="$(cat "$pidfile")"
  kill -0 "$pid" 2>/dev/null
}

mkdir -p "$COINBASE_ROOT/feeds/logs" "$COINBASE_ROOT/feeds/pids"
(
  cd "$COINBASE_ROOT"
  if [ -x "./feeds/ctl" ]; then
    BASE_PATH="$COINBASE_BASE_PATH" ./feeds/ctl ensure --aggressive || true
  elif ! is_alive "$COINBASE_ROOT/feeds/pids/supervise.pid"; then
    BASE_PATH="$COINBASE_BASE_PATH" ./feeds/start_24x7 || true
  fi
) || true

if [ -d "$BSC_ROOT" ]; then
  mkdir -p "$BSC_ROOT/feeds/logs" "$BSC_ROOT/feeds/pids"
  (
    cd "$BSC_ROOT"
    if [ -x "./feeds/ctl" ]; then
      BASE_PATH="$BSC_BASE_PATH" ./feeds/ctl ensure --aggressive || true
    elif ! is_alive "$BSC_ROOT/feeds/pids/supervise.pid"; then
      BASE_PATH="$BSC_BASE_PATH" ./feeds/start_24x7 || true
    fi
  ) || true
fi
