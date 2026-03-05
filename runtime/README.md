# Runtime Architecture

This folder contains the shared, venue-agnostic ingest runtime used by wrappers in `ops/`.

## Layout

- `runtime/ws_engine.py`
  - Core websocket lifecycle: connect/reconnect, subscribe/unsubscribe, parse dispatch, boundary handling.
- `runtime/engine_factory.py`
  - Connector wiring layer that builds an engine from connector-owned profiles.
- `runtime/ws_ingest_daemon.py`
  - Process runtime: product bootstrapping + control socket (`SUB`, `UNSUB`, `LIST`, `STATUS`).
- `runtime/venue.py`
  - Runtime-owned venue/profile constructor helpers.

## Data Flow

1. Operator script (`ops/internal/ws_ingest_daemon`) selects venue from `--venue` or `DW_VENUE`.
2. Venue config is constructed from connector profile metadata in `runtime/venue.py`.
3. `create_configured_ws_engine()` resolves connector package dynamically and creates a connector instance.
4. `SharedWsIngestEngine` runs the websocket loop and writes records using connector feed specs.

## Control + Segment Boundaries

- Ingest control socket accepts: `SUB`, `UNSUB`, `LIST`, `STATUS`.
- `STATUS` returns JSON runtime state (`running`, `connected`, `subs`, heartbeat age, connector extras).
- Segment boundaries are automatic and writer-driven.
- Runtime marks disconnect boundaries with reasons like `ws_disconnect` / `ws_error`.

## Anti-Monolith Boundaries

- `runtime/ws_engine.py`: generic lifecycle + writer orchestration only.
- `connectors/*`: venue protocol, subscriptions, parsing, and connector-owned profiles.
- `ops/*`: operator UX and observability for ingest runtime only.

If code crosses these boundaries, treat that as architecture drift.

## Add a New Venue

1. Create connector module under `connectors/<venue>/connector.py` implementing `WsConnector`.
2. Add connector profile metadata (`CONNECTOR_PROFILE`) in the connector package `__init__.py`.
3. Ensure package naming matches discovery convention (`<venue_key>` or `<venue_key>_*`).
4. Run with `./ops/deploy <venue> --status`.

## Notes

- Shared runtime should stay venue-agnostic; venue logic belongs in connectors.
- Feed family expansion beyond `trades`/`l2` should be implemented via connector `feed_specs()`.
