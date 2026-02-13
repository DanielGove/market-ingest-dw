# Coinbase DW Pipeline

High‑level: one ingest daemon streams Coinbase WS L2 into Deepwater feeds, and one orderbook daemon builds OB feeds at a chosen depth/period for all configured products (default flow). If you need more OB processes, use `feeds/start_ob` manually.
Runtime daemons, control CLIs, and orchestration wrappers (`run`, `stop`) live in `feeds/`.

## Quick start
Defaults are baked into `feeds/run` so you can just:
```bash
./feeds/run
```
Defaults: PRODUCTS=`BTC-USD,ETH-USD,SOL-USD,XRP-USD`, BASE_PATH=`data/coinbase-main`, OB_DEPTH=`200`, OB_PERIOD=`50`.
Override via env vars, e.g.:
```bash
PRODUCTS="XRP-USD" OB_DEPTH=200 OB_PERIOD=200 ./feeds/run
```
- `PRODUCTS`: comma list used by ingest and the single orderbook daemon.
- `BASE_PATH`: Deepwater data directory.
- `OB_DEPTH` / `OB_PERIOD`: depth and period (ms) for the orderbook daemon launched by `feeds/run`.

Stop everything:
```bash
./feeds/stop
```

## Control plane (Unix sockets)
Sockets live in `pids/`:
- `feeds/pids/ingest.sock` — ingest control (SUB/UNSUB/LIST).
- `feeds/pids/orderbook.sock` — single orderbook daemon started by `feeds/run`.

CLI helpers (executable):
```bash
./feeds/ingest_ctl.py sub BTC-USD
./feeds/ingest_ctl.py unsub BTC-USD
./feeds/ingest_ctl.py list

./feeds/ob_ctl.py 200-50 add BTC-USD   # if you start extra daemons via `feeds/start_ob`
./feeds/ob_ctl.py 200-50 list
```

## Watching orderbooks
```
./venv/bin/python feeds/watch_ob.py --product BTC-USD --depth 200 --period 50 --base-path data/coinbase-main
```
Match `depth`/`period` to the orderbook daemon you started (e.g., 200/50 or 200/200).

Quick sanity for multiple feeds:
```
./venv/bin/python tools/ob_peek.py \
  --products BTC-USD,XRP-USD,ETH-USD,SOL-USD \
  --depth 200 --period 50 --seconds 3
```

## Latency probe
Estimate ingest latency (recv_us - ev_us) and optional network ping:
```
./venv/bin/python tools/latency_probe.py \
  --base-path data/coinbase-main \
  --product BTC-USD \
  --seconds 30 \
  --ping
```

## Backtest harness (separate from data plane)
Located in `backtest/`:
- `backtest/engine.py` — closed-loop runtime: strategy splice, exchange splice, intent/execution feeds, status callbacks.
- `backtest/ping_pong.py` — symmetric maker: buy below mid, sell above mid on every snapshot.
- `backtest/taker_pulse.py` — taker validation strategy.

Operator docs:
- `docs/backtest/README.md` (entry index)
- `docs/backtest/quickstart.md`
- `docs/backtest/system_map.md`
- `docs/backtest/phase3b_signoff.md`
- `docs/backtest/resume.md`

Canonical command surface:
```bash
./tools/run_backtest engine
./tools/run_backtest determinism
./tools/run_backtest all
```

Example closed-loop replay:
```bash
./venv/bin/python backtest/engine.py \
  --base-path data/coinbase-main \
  --intent-base-path data/strategy-intents \
  --execution-base-path data/strategy-intents \
  --product BTC-USD \
  --depth 200 --period 50 \
  --seconds 120 \
  --strategy ping_pong \
  --run-id RUN1 \
  --latency-ms 0.5 \
  --reverse-latency-ms 0.5 \
  --fee-bps 1.0 \
  --manifest-path /tmp/engine-run.json

# Determinism check (runs engine twice on one frozen event slice)
./venv/bin/python tools/replay_determinism_check.py \
  --base-path data/coinbase-main \
  --product BTC-USD \
  --depth 200 --period 50 \
  --seconds 30 \
  --strategy ping_pong
```

## Logs and PIDs
- Ingest: `feeds/logs/ingest.log`, `feeds/pids/ingest.pid`
- Orderbooks: `feeds/logs/orderbook_<depth>_<period>.log`, `feeds/pids/orderbook_<depth>_<period>.pid`

## Data reset
```
mkdir -p data/coinbase-main && find data/coinbase-main -mindepth 1 -delete
```

## Scaling / many books
- Ingest stays single-process; it handles all `PRODUCTS` and fans out to readers.
- Orderbooks scale horizontally if you manually start more via `feeds/start_ob`; otherwise `feeds/run` launches one OB daemon.
- All scripts accept a configurable base path (`BASE_PATH` env for `feeds/run`, `--base-path` for watchers/`feeds/start_ob` optional arg).

## Notes
- Orderbooks trust L2: no price bounds; levels cleared only on qty=0 and L2 snapshot records.
- Each orderbook daemon runs a thread per product; playback=True to rebuild from history on start.
- Control sockets are plain text; each command is one line.
