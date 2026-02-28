# Coinbase DW Backtest

Deepwater-native backtesting harness with a single-threaded, causal scheduler.

## Files

- `backtest/engine.py`: harness runtime and scheduler
- `backtest/exchange.py`: incremental exchange simulator
- `backtest/strategy.py`: strategy contract and intent normalization
- `backtest/feeds.py`: Deepwater feed schemas and writer helpers
- `backtest/ping_pong.py`: reference maker-style strategy
- `backtest/taker_pulse.py`: reference taker-validation strategy

## Strategy Contract

Strategies define:

- `subscriptions() -> list[Subscription]`
  - Declares all input feeds and callback methods.
  - `on_status` subscriptions point to execution status input feed(s).
- `outputs() -> list[OutputFeed]`
  - Declares only strategy-produced outputs.
  - Current harness expects exactly one output: `role="intent"`.
- Callback methods consume raw tuples only:
  - `on_snapshot(record)`
  - `on_trade(record)`
  - `on_status(record)`

Strategies return lists of `OrderIntent` (or intent-like dicts) from callbacks that produce orders.

Low-boilerplate option:

- Subclass `backtest.strategy.EventDrivenStrategy`
- Override only callback methods (`on_snapshot`, `on_trade`, `on_status`)
- Defaults use current feed profile:
  - market base path: `/deepwater/data/coinbase-advanced` when available
  - orderbook: `OB256100-<PRODUCT>`
  - trades: `CB-TRADES-<PRODUCT>`

## Runtime Model

Two time axes:

- Exchange axis: market `event_time`
- Strategy axis: local `processed_time`

Engine builds two priority queues:

- `trade_heap`: exchange trade events keyed by `event_time`
- `strategy_heap`: strategy-deliverable records keyed by `processed_time`
  - market records
  - execution status records

Market format assumptions are intentionally lightweight:

- Processed-time field aliases supported: `processed_time`, `processed_us`
- Event-time field aliases supported: `event_time`, `snapshot_time`, `snapshot_us`, `event_us`
- Trade field aliases supported: `side|aggressor_side|taker_side`, `price|trade_price|px`, `size|qty|amount|trade_size`

## Timeline Invariant

Backtest correctness rule:

- Before exchange processes a trade at `event_time = E`, strategy may only process records with `processed_time <= E`.

This is enforced in the scheduler loop in `backtest/engine.py`.

## Exchange Simulation

`ExchangeSimulator` is incremental:

- Receives intents with arrival time = `intent.processed_time + forward_latency`
- Processes trade events on `event_time`
- Matches against working orders
- Emits execution records with:
  - `event_time` = fill event time
  - `processed_time` = `event_time + reverse_latency`

Execution records are written to Deepwater and also routed back into `strategy_heap` for `on_status`.

## Initialization Sequence

1. Load strategy and validate `subscriptions` / `outputs`
2. Resolve output paths and product ID from strategy output config
3. Resolve execution status feed from `on_status` subscription
4. Resolve replay window from market anchor feed on `processed_time`
5. Initialize exchange simulator (special harness opinion)
6. Initialize intent writer
7. Build lane readers and prime scheduler heaps

## Run

```bash
./tools/run_backtest engine
./tools/run_backtest determinism
./tools/run_backtest all
```

Example:

```bash
WINDOW_SECONDS=60 STRATEGY=backtest.taker_pulse:TakerPulse RUN_ID=RUN1 ./tools/run_backtest all
```

Use a frozen dataset segment ID (repeatable window selection):

```bash
./tools/backtest_segments create --segment-id BTCUSD-A --strategy ping_pong --seconds 300 --description "5m freeze"
SEGMENT_ID=BTCUSD-A STRATEGY=ping_pong RUN_ID=RUN_SEG_A ./tools/run_backtest all
./tools/backtest_segments get --segment-id BTCUSD-A
./tools/backtest_segments list
```

## 24/7 Ops

Pipeline control:

```bash
./feeds/ctl status --json
./feeds/ctl reconcile --aggressive --json
./feeds/ctl ensure --aggressive --json
./feeds/ctl ensure --aggressive --workers 2 --pin-cores 14,15 --ob-depth 256 --ob-period 100 --json
```

Legacy/manual controls:

```bash
./feeds/run
./feeds/status
./feeds/stop            # default: stop orderbook only (ingest-safe)
./feeds/stop all        # full pipeline stop
./feeds/restart_orderbook
```

`feeds/ctl ensure` is idempotent and safe for cron/agents. It converges ingest/orderbook/supervise to target state, and when ingest is already up it uses orderbook-only restart paths.

Orderbook-only iteration (keeps trades/L2 ingest up):

```bash
./feeds/restart_orderbook
./feeds/stop orderbook
./feeds/start_orderbook
```

Orderbook performance knobs:

```bash
OB_WORKERS=4 OB_MAX_LEVELS=8192 ./feeds/restart_orderbook
```

Shadow/canary orderbook (parallel OB feed namespace):

```bash
OB_PREFIX=OBC ./feeds/start_ob 200 50 "BTC-USD,ETH-USD,SOL-USD"
```

`feeds/run` and `tools/feed_health` auto-default to `/deepwater/data/coinbase-advanced` when `/deepwater/data` exists.

Continuous supervision:

```bash
./feeds/start_24x7
tail -f feeds/logs/supervise.log
```

Install boot + periodic watchdog (every 5 minutes):

```bash
./tools/install_daily_feeds_cron.sh
```

Feed health (throughput + latency):

```bash
./tools/feed_health --window 60 --once --hide-idle
./tools/feed_health --window 60 --interval 60 --hide-idle
```

Backtest preflight:

```bash
./tools/backtest_ready --strategy ping_pong --window 300
```
