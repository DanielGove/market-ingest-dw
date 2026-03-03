# Venue Connector Spec (Coinbase)

This document is the source of truth for the live Coinbase ingest and orderbook connector in this repo.

## Scope

This connector runs two live services:

1. Ingest daemon (`ws_ingest_daemon.py`) for trades and L2 updates.
2. Orderbook daemon (`orderbook_daemon.py`) for derived fixed-depth snapshots.

All writes target Deepwater under `BASE_PATH` (defaults to `/deepwater/data/coinbase-advanced` when available).

## Process Model

Primary process scripts:

1. `./feeds/start_ingest`
2. `./feeds/start_orderbook`
3. `./feeds/supervise`
4. `./feeds/ctl` for idempotent control/reconcile/ensure

Runtime topology:

1. One ingest process with one websocket IO loop.
2. Orderbook manager with N workers (`OB_WORKERS`), each worker handling a product subset.
3. Supervisor that only restarts missing/down services.

## Feed Names

For each product `<PRODUCT>`:

1. Trades feed: `CB-TRADES-<PRODUCT>`
2. L2 feed: `CB-L2-<PRODUCT>`
3. Orderbook feed: `OB<DEPTH><PERIOD>-<PRODUCT>`

Default OB feed profile:

1. `DEPTH=256`
2. `PERIOD=100` ms
3. Feed name examples: `OB256100-BTC-USD`, `OB256100-ETH-USDT`

## Schemas

### Trades (`CB-TRADES-*`)

Record fields in write order:

1. `event_time` (`uint64`)
2. `received_time` (`uint64`)
3. `processed_time` (`uint64`)
4. `packet_sent` (`uint64`)
5. `trade_id` (`uint64`)
6. `type` (`char`, `'T'`)
7. `side` (`char`, buy/sell from exchange)
8. `_` (`_6` padding)
9. `price` (`float64`)
10. `size` (`float64`)

Clock level is `3`.

### L2 (`CB-L2-*`)

Record fields in write order:

1. `event_time` (`uint64`)
2. `received_time` (`uint64`)
3. `processed_time` (`uint64`)
4. `packet_sent` (`uint64`)
5. `type` (`char`, snapshot/update type from exchange)
6. `side` (`char`, bid/ask)
7. `_` (`_14` padding)
8. `price` (`float64`)
9. `qty` (`float64`)

Clock level is `3`.
`index_playback=True` is enabled.

### Orderbook (`OB*`)

Record header:

1. `snapshot_time` (`uint64`) from latest incorporated L2 `event_time`
2. `processed_time` (`uint64`) local emit timestamp
3. `type` (`char`, `'S'`)
4. `_` (`_7` padding)

Then:

1. `depth` pairs of bid `(price, qty)` as `float64`
2. `depth` pairs of ask `(price, qty)` as `float64`

Clock level is `2`.

## Time Semantics

Definitions:

1. `event_time`: exchange event timestamp from Coinbase payload.
2. `packet_sent`: Coinbase packet timestamp on websocket message.
3. `received_time`: local time immediately after `recv`.
4. `processed_time`: local time at write/emission.

Important metric interpretation:

1. `processed_time - event_time` includes exchange-side event stamping delay.
2. `processed_time - received_time` is connector processing overhead and should stay near microseconds to low milliseconds.
3. Trades can show materially higher `processed_time - event_time` than L2 even when connector hot path is healthy.

## Orderbook Emission Behavior

Current behavior is wall-clock periodic emission:

1. Emit every `OB_PERIOD` ms while book is valid.
2. `snapshot_time` is always the latest incorporated L2 `event_time`.
3. If no new L2 arrives, snapshots may repeat the same `snapshot_time` on successive emits.

This preserves a fixed reaction-time cadence while keeping exchange-time traceability.

## Segmentation Semantics

Segment lifecycle:

1. Segment opens on first write.
2. Segment closes on writer close (`writer_close`) or explicit boundary control (for example `ws_disconnect`).
3. If process dies mid-segment, next writer startup recovers it as `crash_closed` (`writer_recovery`).

Status filter semantics:

1. `usable` is a filter alias, not a literal status.
2. `usable` includes `closed` and `crash_closed`.

Control operations:

1. Ingest segment roll: `./feeds/ingest_ctl.py roll [PRODUCT]`
2. Orderbook segment roll: `./feeds/ob_ctl.py roll [PRODUCT]`
3. Unified helper: `./feeds/segment_ctl.py roll [--service ingest|orderbook|all] [--product PRODUCT]`

## Control Plane

Idempotent high-level control:

1. `./feeds/ctl status --json`
2. `./feeds/ctl ensure --aggressive --json`
3. `./feeds/ctl reconcile --aggressive --json`

Low-level socket controls:

1. Ingest: `SUB`, `UNSUB`, `LIST`, `ROLL`
2. Orderbook worker: `ADD`, `REMOVE`, `LIST`, `ROLL`

## Failure and Recovery Model

Expected behaviors:

1. WS disconnect should trigger segment boundary close with reason `ws_disconnect` where writer is healthy.
2. Supervisor restarts down services and logs restart events in `feeds/logs/supervise.log`.
3. Orderbook-only iteration should not interrupt ingest:
   1. `./feeds/restart_orderbook`
   2. `./feeds/stop orderbook`
   3. `./feeds/start_orderbook`

Hard failure caveat:

1. If writer state is already corrupted/released during crash paths, boundary close may fail and recovery will rely on next startup (`crash_closed`).

## Operational Observability

Primary logs:

1. `feeds/logs/ingest.log`
2. `feeds/logs/orderbook.log`
3. `feeds/logs/supervise.log`

Primary health/metadata tools:

1. `./tools/feed_health --window 60 --once --json`
2. `./venv/bin/deepwater-health --base-path "$BASE_PATH" --check-feeds --max-age-seconds 300`
3. `./venv/bin/deepwater-segments --base-path "$BASE_PATH" --feed <FEED> --status all --json`
4. `./venv/bin/deepwater-datasets --base-path "$BASE_PATH" --feeds <F1,F2,...> --json`

## Deployment Notes

Process code changes are only active after service restart:

1. Editing `feeds/*.py` or Deepwater package code does not affect already-running daemons.
2. Apply controlled restart windows for deploys.

## Recommended 48h Validation Gate

Before promoting for trading use, run at least one continuous 48h soak and verify:

1. No unhandled tracebacks in ingest/orderbook logs.
2. No prolonged stale-feed windows in `feed_health`.
3. Segment boundaries are present for disconnect/restart events.
4. `deepwater-health` remains healthy.
5. Dataset windows from `deepwater-datasets` are consistent with expected uptime.
