# Backtest Quickstart

This guide is the minimum path to run and verify the current backtest stack.

## 1. Preconditions

- Python environment exists (`./venv/bin/python` or `python3`)
- market data exists under `data/coinbase-main`
- product/depth/period window has actual records

## 2. One-command run

Run closed-loop engine and determinism check:

```bash
./tools/run_backtest all
```

Defaults:

- `PRODUCT=BTC-USD`
- `DEPTH=200`
- `PERIOD=50`
- `SECONDS=60`
- `STRATEGY=ping_pong`
- `RUN_ID=RUN1`

Override example:

```bash
PRODUCT=ETH-USD SECONDS=120 STRATEGY=ping_pong RUN_ID=ETH_RUN ./tools/run_backtest all
```

## 3. Where outputs land

- Engine manifest: `out/backtest/<RUN_ID>/engine-manifest.json`
- Intent feed data: `data/strategy-intents`
- Execution feed data: `data/strategy-intents` (default path)

## 4. Individual mode commands

```bash
./tools/run_backtest engine
./tools/run_backtest determinism
```

Use extra args for the underlying script when needed:

```bash
./tools/run_backtest engine --strict-contracts
```

## 5. Expected success signals

- engine prints `Engine done. events=... snapshots=... trades=... intents=...`
- determinism prints `PASS: deterministic replay checks matched`
- outputs exist in `out/backtest/<RUN_ID>/`

## 6. Fast troubleshooting

- no events: increase `SECONDS`, check `PRODUCT`, `DEPTH`, `PERIOD`
- no snapshots: verify orderbook feed exists for selected depth/period
- no fills: try taker-focused strategy or larger window
