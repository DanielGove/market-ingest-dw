# Coinbase DW Pipeline

High‑level: one ingest daemon streams Coinbase WS L2 into Deepwater feeds, and N orderbook daemons each build OB feeds at a chosen depth/period for all configured products.

## Quick start
```bash
# 1) set envs explicitly (no defaults)
PRODUCTS="XRP-USD,BTC-USD,ETH-USD,SOL-USD" \
BASE_PATH=data/coinbase-main \
ORDERBOOKS="200:50,200:200" \
./run.sh
```
- `PRODUCTS`: comma list used by ingest and all orderbook daemons.
- `BASE_PATH`: Deepwater data directory.
- `ORDERBOOKS`: comma list of `depth:period_ms` pairs; one orderbook daemon is spawned per pair.

Stop everything:
```bash
./stop.sh
```

## Control plane (Unix sockets)
Two sockets live in `pids/`:
- `pids/ingest.sock` — subscribe/unsubscribe products for L2 ingest.
- `pids/orderbook_<depth>_<period>.sock` — control each orderbook daemon (add/remove/list products for that depth/period).

CLI helpers (executable):
```bash
./ingest_ctl.py sub BTC-USD
./ingest_ctl.py unsub BTC-USD

./ob_ctl.py --sock pids/orderbook_200_50.sock add BTC-USD 200 50
./ob_ctl.py --sock pids/orderbook_200_50.sock remove BTC-USD
./ob_ctl.py --sock pids/orderbook_200_50.sock list
```

## Watching orderbooks
```
./venv/bin/python watch_ob.py --product BTC-USD --depth 200 --period 50
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

## Notes
- Orderbooks trust L2: no price bounds; levels cleared only on qty=0 and L2 snapshot records.
- Each orderbook daemon runs a thread per product; playback=True to rebuild from history on start.
- Control sockets are plain text; each command is one line.
