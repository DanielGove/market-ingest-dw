# Deepwater Strategy Backtesting Development Plan

## Purpose
This document defines the architecture and execution plan for a strategy backtesting system built on Deepwater feeds.

The objective is not a "trading bot." The objective is a truthful experimentation environment where:

- strategies are deterministic and isolated
- decisions are first-class artifacts
- executions are modeled as consequences
- backtests reject weak ideas quickly
- live and simulated operation share one strategy interface

## Core Principle
> Strategies do not simulate the exchange.  
> Strategies express intent.  
> The exchange (real or simulated) determines outcomes.

This separation is non-negotiable.

## System Architecture
```text
Market Feeds (Deepwater, live or recorded)
            |
            v
      Strategy Runtime
  - reads market + execution events
  - writes intent events
            |
            v
 Exchange Adapter / Simulator
  - applies latency, fill logic, fees
  - writes execution events
            |
            v
      Strategy Runtime
  - reacts to ACK/FILL/CANCEL/REJECT
```

This event loop is the same in live, paper, and backtest modes. Only the exchange implementation changes.

## First-Class Feeds
### 1. Market Feed (Truth)
Owned by ingestion pipeline. Contains L2 snapshots/deltas and trade context with source timestamps.

Rules:

- never mutated by strategy code
- replayed exactly for backtests
- treated as immutable ground truth

### 2. Intent Feed (Decisions)
Owned by strategy runtime. Represents what the strategy requests.

Rules:

- deterministic for a fixed event stream
- timestamped at decision time
- no direct fill assumptions
- replayable for repeated evaluation

Current feed pattern:

- `STRAT-ORDERS-<STRATEGY_NAME>`

Current schema in code (`backtest/intent_feed.py`):

- `event_time` (`uint64`)
- `processed_time` (`uint64`)
- `product_id` (`bytes16`)
- `side` (`char`) (`B`/`S`)
- `type` (`char`) (`L`/`M`)
- `price` (`float64`)
- `size` (`float64`)
- `tif` (`bytes8`) (`GTC`/`IOC`/`FOK`/`PO`)
- `client_tag` (`bytes8`)

### 3. Execution Feed (Consequences)
Owned by real exchange adapter or simulation engine. Represents ACK/FILL/CANCEL/REJECT outcomes with timing and costs.

Rules:

- strategies update state only from this feed
- fees and slippage are encoded here
- identical contract for live and backtest

## Canonical Event Schema
This section freezes minimum required fields and semantics so all components can evolve without breaking contracts.

### Global Schema Rules
- each event has a globally unique `event_id` and a `schema_version`
- all timestamps are UTC microseconds as `uint64`
- `product_id` uses a canonical uppercase symbol format (for example, `BTC-USD`)
- enums are serialized as stable short codes, not free-form strings
- additive-only schema evolution within a major version

### Market Event Contract (minimum)
- `event_id`
- `schema_version`
- `event_time` (exchange event time)
- `processed_time` (local processing time)
- `product_id`
- `source` (feed/source identifier)
- `event_type` (`L2_SNAPSHOT`, `L2_DELTA`, `TRADE`)
- payload fields specific to `event_type`

### Intent Event Contract (minimum)
- `event_id`
- `schema_version`
- `event_time` (snapshot/event that triggered decision)
- `processed_time` (strategy decision time)
- `strategy_id`
- `run_id`
- `product_id`
- `client_order_id` (strategy-generated, idempotency key)
- `action` (`PLACE`, `CANCEL`, `REPLACE`)
- `side` (`B`, `S`) when applicable
- `order_type` (`L`, `M`) when applicable
- `price` (nullable for market)
- `size`
- `tif` (`GTC`, `IOC`, `FOK`, `PO`)
- `client_tag` (optional)

### Execution Event Contract (minimum)
- `event_id`
- `schema_version`
- `event_time` (exchange/sim event time)
- `processed_time` (adapter processing time)
- `strategy_id`
- `run_id`
- `product_id`
- `client_order_id`
- `exchange_order_id` (nullable until ACK)
- `status` (`ACK`, `REJECT`, `PARTIAL_FILL`, `FILL`, `CANCEL`)
- `filled_size_delta`
- `fill_price` (nullable unless fill)
- `cum_filled_size`
- `remaining_size`
- `fee`
- `liquidity_flag` (`M`, `T`, `U`)
- `reason_code` (required for rejects/cancels)

### Idempotency and Ordering Rules
- `(run_id, strategy_id, client_order_id)` uniquely identifies a logical order lifecycle
- duplicate events with the same `event_id` are ignored by consumers
- execution status transitions must be monotonic for each `client_order_id`
- all components must reject unknown required fields for the active `schema_version`

## Strategy Contract
Strategies are pure event-driven state machines.

Allowed:

- consume market/execution events
- emit order intents
- update internal state from execution outcomes

Not allowed:

- sleep/poll loops
- wall-clock dependencies
- direct exchange simulation inside strategy code
- side effects outside declared outputs

Interface in current code (`backtest/interfaces.py`):

```python
class Strategy(Protocol):
    def on_snapshot(self, snapshot: tuple, meta: dict) -> List[OrderIntent]:
        ...

    def on_status(self, status: dict) -> None:
        ...
```

## Backtesting Philosophy
### What backtesting can do

- simulate visible order book behavior from recorded data
- apply conservative latency assumptions
- estimate fill outcomes under explicit uncertainty models
- apply fees/slippage consistently
- provide reproducible metrics

### What backtesting cannot do (with L2 only)

- recover exact queue position
- observe hidden liquidity
- infer counterfactual market impact precisely
- predict all participant reactions

These are hard data constraints, not tooling failures.

## Fill and Latency Modeling
### Queue modeling policy
Queue position is unidentifiable with L2-only data, so it is modeled as uncertainty with three scenarios:

- pessimistic: default decision gate for go/no-go
- neutral: proportional assumptions for ranking variants
- optimistic: diagnostic only

A strategy is considered viable only if it survives pessimistic assumptions.

### Latency policy
Latency is part of the environment, not noise.

Minimum modeled components:

- strategy-to-exchange delay
- exchange-to-strategy delay
- jitter and tail events

Queue placement is determined by arrival time, not decision timestamp.

Timestamp pipeline (required):

- strategy receives market data at local runtime processing time
- `intent.event_time` = triggering market `event_time`
- `intent.processed_time` = local strategy decision time
- `exchange_arrival_time` = `intent.processed_time` + forward latency (+ jitter)
- `execution.event_time` = exchange/simulator outcome time (ACK/FILL/CANCEL/REJECT)
- `execution.processed_time` = `execution.event_time` + reverse latency (+ jitter)

Matching logic uses exchange-time causality (`event_time`). Delivery delay to strategy is represented by `processed_time`.

## Simulation Semantics
Backtests run as discrete-event simulation.

Rules:

- simulation clock jumps to next event timestamp
- all feeds are merged deterministically
- ordering ties are resolved with fixed rules
- no real-time waiting or sleep-driven loops

Benefits:

- high replay speed
- reproducibility
- deterministic debugging

## Determinism Guarantees
Determinism is a hard requirement, not a best effort.

### Deterministic Execution Rules
- fixed random seed is required for every run, including model sampling paths
- no wall-clock reads in strategy or simulator logic
- no nondeterministic iteration over maps/sets in event-critical paths
- all event merges use a stable tie-break order
- floating-point behavior is standardized and documented per metric path

### Canonical Event Merge Order
When timestamps are equal, apply this order:

1. market events
2. execution events
3. strategy-generated intents scheduled at that same simulation timestamp

Tie-break within each class:

1. `event_time`
2. source sequence number if present
3. `event_id` lexical order

### Replay Invariants
- same dataset slice + same execution stream + same config + same code commit = identical intent stream
- same intent stream + same simulator config = identical execution stream
- same execution stream = identical metrics report

### Reproducibility Artifacts
Every run must persist:

- `run_id`
- code commit hash
- schema versions in use
- dataset identifiers and time window
- full simulator and strategy config
- random seed
- output checksums for intent stream, execution stream, and metrics report

### Determinism CI Tests
- replay test: run identical config twice and assert identical checksums
- perturbation test: change only seed and verify expected checksum change
- contract test: reject events that violate ordering or schema version rules

## Metrics and Quality Gates
Required output metrics per run:

- net PnL and fee-adjusted PnL
- max drawdown
- inventory distribution and peak exposure
- fill rate and adverse selection rate
- latency sensitivity and fill-model sensitivity

Promotion gates:

- must pass pessimistic fill model thresholds
- must remain stable across latency stress tests
- must not rely on impossible execution assumptions

## Development Phases
### Phase 0: Freeze Contracts
Goal: prevent architectural drift.

Deliverables:

- final feed schemas
- canonical timestamp definitions
- strategy and execution event interfaces
- versioned contract module in code
- documented exchange field mapping (Coinbase Advanced Trade)

Exit criteria:

- interface docs approved
- schema versioning plan defined
- execution lifecycle transitions are validated
- deterministic merge ordering is specified and testable

Current implementation artifacts:

- `backtest/contracts.py`
- `backtest/intent_feed.py`
- `backtest/execution_feed.py`
- `docs/phase0_contracts.md`

### Phase 1: Deterministic Strategy Harness
Goal: isolate and replay strategy behavior.

Deliverables:

- strategy runner over historical market feed
- intent feed writer with deterministic output
- snapshot/status callback contract enforced
- runtime environment boundary that owns feed I/O
- strategy output normalization and validation at runtime boundary

Exit criteria:

- same input events produce byte-for-byte equivalent intent stream
- strategy code has no direct Deepwater reader/writer dependency

Current implementation artifacts:

- `backtest/environment.py`
- `backtest/strategy_boundary.py`
- `backtest/engine.py` (`--strict-contracts` boundary enforcement)

### Phase 2: One-Way Evaluator
Goal: reject weak ideas early.

Deliverables:

- evaluator that consumes market + intent feeds
- conservative fill simulation
- metrics report generation
- optional machine-readable report/log outputs for reproducible analysis

Exit criteria:

- baseline metrics generated for at least one reference strategy

Current implementation artifacts:

- `backtest/intent_eval.py`
- `backtest/contracts.py` (intent/execution validation reused by evaluator)

### Phase 3: Closed-Loop Exchange Simulation
Goal: behavioral fidelity.

Deliverables:

- simulated exchange emitting execution events
- strategy consuming execution status in-loop
- deterministic scheduler for merged feeds

Exit criteria:

- strategy code path is identical between live and backtest modes

Current implementation artifacts:

- `backtest/engine.py` (causal merged-event scheduler loop)
- `backtest/market_stream.py` (deterministic merge of L2, trades, OB snapshots)
- `backtest/sim_exchange.py` (pending/working order state machine + execution statuses)
- `backtest/execution_feed.py` (execution feed emission surface)
- `tools/replay_determinism_check.py` (frozen-slice replay determinism check)

Phase 3B hardening (implemented):

- execution lifecycle transition validation on emit (`ACK/FILL/CANCEL/REJECT` monotonicity)
- reverse-latency status delivery path (`execution.processed_time = event_time + reverse_latency`)
- run manifest output (window bounds, config, checksums, status counts)
- deterministic checksums for market stream, intent feed, and execution feed

### Phase 4: Paper Trading Calibration
Goal: align model assumptions to reality.

Deliverables:

- live intent capture
- observed fill/latency comparison reports
- calibrated pessimistic/neutral parameters

Exit criteria:

- model error bands tracked and improving

Current implementation artifacts (baseline):

- `backtest/phase4_calibration.py` (observed-vs-sim comparison + parameter search)
- `tools/run_backtest` (`calibration` mode)
- `docs/backtest/phase4_calibration.md`

### Phase 5: Research Automation (Optional)
Goal: automate research, not execution authority.

Deliverables:

- batch backtest orchestration
- variant generation and ranking
- adversarial scenario sweeps

Guardrails:

- no bypass of pessimistic gates
- no bypass of risk controls
- human review required for promotion

## Current Repo Mapping
Implemented components:

- strategy engine and intent writing: `backtest/engine.py`
- intent feed schema/helper: `backtest/intent_feed.py`
- offline intent evaluator: `backtest/intent_eval.py`
- backtest router and status emission: `backtest/router_backtest.py`
- order state and PnL tracker: `backtest/state_tracker.py`
- reference strategies: `backtest/ping_pong.py`, `backtest/sample_strat.py`
- operator/discovery docs: `docs/backtest/README.md`, `docs/backtest/quickstart.md`, `docs/backtest/system_map.md`

Known gaps for state-of-the-art target:

- formal execution feed contract versioning
- richer queue-position uncertainty models
- deterministic multi-feed scheduler with tie-breaking spec
- standardized metrics output format and run manifest
- regression test suite for replay determinism

## Guiding Sentence
This system is designed to prevent self-deception: decisions are explicit, assumptions are testable, and outcomes are consequences.
