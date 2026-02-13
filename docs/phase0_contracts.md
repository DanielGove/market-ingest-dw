# Phase 0 Contracts (Frozen)

This document freezes interfaces before scheduler and simulator work.

## Scope
Phase 0 includes:

- canonical timestamp semantics
- versioned event contracts
- deterministic merge and lifecycle rules
- Coinbase field mapping for execution events

Phase 0 excludes fill modeling, queue modeling, and exchange simulation behavior.

## Canonical Timestamps
All timestamps are UTC microseconds (`uint64`).

- `event_time`: source event time (exchange, feed, or simulator).
- `processed_time`: local processing time when event is emitted into the runtime.

Rules:

- no wall-clock reads inside strategy code
- strategy decisions inherit triggering event context via `event_time`
- exchange outcomes use exchange/simulator `event_time`

Latency timestamp equations:

- `exchange_arrival_time = intent.processed_time + forward_latency_us (+ jitter_us)`
- `execution.processed_time = execution.event_time + reverse_latency_us (+ jitter_us)`

Contract rule:

- exchange matching/fill causality is anchored to `event_time`
- strategy delivery delay is represented by `processed_time`

## Event Contracts
The code contract is defined in `backtest/contracts.py`.

Required event families:

- `MarketEvent`: read-only market truth
- `IntentEvent`: strategy requests
- `ExecutionEvent`: exchange/simulator outcomes

Execution status lifecycle (per `client_order_id`):

- allowed starts: `ACK`, `REJECT`, `PARTIAL_FILL`, `FILL`, `CANCEL`
- terminal: `REJECT`, `FILL`, `CANCEL`
- terminal states may not transition further

## Deterministic Merge Order
When timestamps are equal, scheduler order is:

1. market events
2. execution events
3. strategy intents scheduled at that same timestamp

Tie-break within a class:

1. `event_time`
2. source sequence number
3. lexical `event_id`

## Coinbase Mapping (Advanced Trade)
Initial execution contract mapping should follow Coinbase Advanced Trade fields.

Primary references:

- List Orders endpoint: `https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/orders/list-orders`
- List Fills endpoint: `https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/orders/list-fills`
- WebSocket channels reference: `https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels`

Mapping guidance:

- `client_order_id` <- Coinbase client order ID
- `exchange_order_id` <- Coinbase order ID
- `status` <- normalized order status updates (`ACK`, `REJECT`, `PARTIAL_FILL`, `FILL`, `CANCEL`)
- `filled_size_delta` <- per-update fill amount (derive from cumulative when needed)
- `cum_filled_size` <- Coinbase cumulative filled size (`filled_size` / equivalent)
- `fill_price` <- average/per-fill execution price based on event source
- `fee` <- fee from fills where available
- `reason_code` <- cancel/reject reason when provided

Normalization rule:

- keep raw Coinbase payload alongside normalized event during adapter integration
- normalized event is the strategy/backtest contract; raw payload is audit data

## Feed Specs
- intent feed helper: `backtest/intent_feed.py`
- execution feed helper: `backtest/execution_feed.py`

Execution feed name convention:

- `STRAT-EXEC-<STRATEGY_NAME>`

## Phase 0 Acceptance Criteria
Phase 0 is complete when:

- contracts are versioned and importable from code
- execution lifecycle transition validator exists
- intent and execution feed specs exist
- deterministic merge ordering rule is documented and testable
- Coinbase mapping is documented for adapter implementation
