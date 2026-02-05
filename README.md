# Coinbase DW Pipeline

High‑level: one ingest daemon streams Coinbase WS L2 into Deepwater feeds, and one orderbook daemon builds OB feeds at a chosen depth/period for all configured products (default flow). If you need more OB processes, use `start_ob.sh` manually.

## Quick start
```bash
# 1) set envs explicitly (no defaults)
PRODUCTS="XRP-USD,BTC-USD,ETH-USD,SOL-USD" \
BASE_PATH=data/coinbase-main \
OB_DEPTH=200 \
OB_PERIOD=50 \
./run.sh
```
- `PRODUCTS`: comma list used by ingest and the single orderbook daemon.
- `BASE_PATH`: Deepwater data directory.
- `OB_DEPTH` / `OB_PERIOD`: depth and period (ms) for the orderbook daemon launched by run.sh.

Stop everything:
```bash
./stop.sh
```

## Control plane (Unix sockets)
Sockets live in `pids/`:
- `pids/ingest.sock` — ingest control (SUB/UNSUB/LIST).
- `pids/orderbook.sock` — single orderbook daemon started by run.sh.

CLI helpers (executable):
```bash
./ingest_ctl.py sub BTC-USD
./ingest_ctl.py unsub BTC-USD
./ingest_ctl.py list

./ob_ctl.py 200-50 add BTC-USD   # if you start extra daemons via start_ob.sh
./ob_ctl.py 200-50 list
```

## Watching orderbooks
```
./venv/bin/python watch_ob.py --product BTC-USD --depth 200 --period 50 --base-path data/coinbase-main
```
Match `depth`/`period` to the orderbook daemon you started (e.g., 200/50 or 200/200).

Quick sanity for multiple feeds:
```
./venv/bin/python tools/ob_peek.py \
  --products BTC-USD,XRP-USD,ETH-USD,SOL-USD \
  --depth 200 --period 50 --seconds 3
```

## Logs and PIDs
- Ingest: `logs/ingest.log`, `pids/ingest.pid`
- Orderbooks: `logs/orderbook_<depth>_<period>.log`, `pids/orderbook_<depth>_<period>.pid`

## Data reset
```
mkdir -p data/coinbase-main && find data/coinbase-main -mindepth 1 -delete
```

## Scaling / many books
- Ingest stays single-process; it handles all `PRODUCTS` and fans out to readers.
- Orderbooks scale horizontally if you manually start more via `start_ob.sh`; otherwise run.sh launches one OB daemon.
- All scripts accept a configurable base path (`BASE_PATH` env for run.sh, `--base-path` for watchers/start_ob optional arg).

## Notes
- Orderbooks trust L2: no price bounds; levels cleared only on qty=0 and L2 snapshot records.
- Each orderbook daemon runs a thread per product; playback=True to rebuild from history on start.
- Control sockets are plain text; each command is one line.
