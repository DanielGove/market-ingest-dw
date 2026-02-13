# Backtest System Map

This map reflects the current minimal runtime.

## Core runtime

- `backtest/engine.py`
  - Closed-loop scheduler.
  - Strategy splice on `processed_time` (OB + trades).
  - Exchange splice with intent arrival latency + trade `event_time`.
  - Execution feed publish (`event_time`, synthetic `processed_time`).
  - Execution status delivery back to strategy on `processed_time`.

- `backtest/state_tracker.py`
  - Position/PnL updates from fill statuses.

## Boundaries and feed contracts

- `backtest/interfaces.py`
  - Strategy protocol and `OrderIntent`.

- `backtest/strategy_boundary.py`
  - Runtime normalization/validation for strategy outputs.

- `backtest/intent_feed.py`
  - Intent feed schema/writer helper.

- `backtest/execution_feed.py`
  - Execution feed schema/writer helper.

- `backtest/contracts.py`
  - Event contract constants/validators.

## Strategies

- `backtest/ping_pong.py`
  - Symmetric maker example.

- `backtest/taker_pulse.py`
  - Taker validation strategy.

## Verification

- `tools/replay_determinism_check.py`
  - Frozen-window replay determinism check.
