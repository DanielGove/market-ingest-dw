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

## Runtime Model

Two time axes:

- Exchange axis: market `event_time`
- Strategy axis: local `processed_time`

Engine builds two priority queues:

- `trade_heap`: exchange trade events keyed by `event_time`
- `strategy_heap`: strategy-deliverable records keyed by `processed_time`
  - market records
  - execution status records

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
