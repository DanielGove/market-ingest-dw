# Runtime Architecture

This folder contains the shared, venue-agnostic ingest runtime used by wrappers in `feeds/`.

## Layout

- `runtime/venues.py`
  - Central defaults per venue (URI, product defaults, feed prefixes, path defaults).
- `runtime/ingest/ws_engine.py`
  - Core websocket lifecycle: connect/reconnect, subscribe/unsubscribe, parse dispatch, segment roll.
- `runtime/ingest/market_engine.py`
  - Connector/venue wiring layer that builds a configured engine from `connectors/registry.py`.
- `runtime/ingest/ws_ingest_daemon.py`
  - Process runtime: product bootstrapping + control socket (`SUB`, `UNSUB`, `LIST`, `ROLL`).

## Data Flow

1. Wrapper (`feeds/ws_ingest_daemon.py`) selects venue from `--venue` or `DW_VENUE`.
2. Venue defaults are loaded from `runtime/venues.py`.
3. `ConfiguredMarketDataEngine` resolves connector registration and creates a connector instance.
4. `SharedWsIngestEngine` runs the websocket loop and writes records using connector feed specs.
5. Orderbook daemon (`feeds/orderbook_daemon.py`) consumes venue-specific L2 prefixes.

## Add a New Venue

1. Create connector module under `connectors/<venue>/connector.py` implementing `WsConnector`.
2. Add venue defaults in `runtime/venues.py`.
3. Register the connector in `connectors/registry.py`.
4. Run with `DW_VENUE=<venue> ./feeds/run`.

## Notes

- Shared runtime should stay venue-agnostic; venue logic belongs in connectors.
- Feed family expansion beyond `trades`/`l2` should be implemented via connector `feed_specs()`.
