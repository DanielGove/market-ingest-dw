# Connector Interface

`connectors/base.py` defines the target adapter contract for venue modules.
Current connector modules:
- `connectors/coinbase_spot/connector.py`
- `connectors/kraken_spot/connector.py`
- `connectors/hyperliquid_perp/connector.py`

Goals:
- keep shared ingest runtime venue-agnostic
- keep venue-specific transport/subscription/parsing logic connector-owned
- support event families beyond trades/l2

Current runtime behavior is unchanged; these adapters preserve Coinbase/Kraken
behavior while reducing venue-specific runtime duplication.

`SharedWsIngestEngine` supports:
- generic mode via `feed_specs(pid)` for arbitrary families

Venue wrapper construction is centralized in:
- `runtime/engine_factory.py`

Connector discovery/loading is centralized in:
- `connectors/loader.py`

## Minimal Add-Venue Workflow

To add a new venue without touching shared runtime loops:

1. Add connector module under `connectors/<venue_name>/connector.py` implementing `WsConnector` from `connectors/base.py`.
2. Define feed specs via `feed_specs(pid)`.
3. Define `CONNECTOR_PROFILE` in the connector package `__init__.py` with one canonical `base_path`.
4. Name the package by venue convention and include `connector.py`.

Required `CONNECTOR_PROFILE` keys:
- `key`
- `label`
- `ingest_description`
- `websocket_uri`
- `base_path`
- `default_products`
- `families`

Discovery convention:
- package under `connectors/` must be either `<venue_key>` or `<venue_key>_*`
- package must contain `connector.py`
- `connector.py` must contain exactly one effective `*Connector` class for the venue

Required connector methods are intentionally small:
- `feed_specs(pid)`
- `send_subscribe(engine, product_ids)`
- `handle_raw(engine, raw, recv_us, now_us)`

Everything else (`on_connect`, `send_unsubscribe`, `on_timeout`, `extra_status`) is runtime integration polish.

Shared runtime remains unchanged:
- `runtime/ws_engine.py`
- `runtime/ws_ingest_daemon.py`

## Non-Trades/L2 Families (Pools, RPC, Funding)

If a venue needs feeds beyond `trades`/`l2`, implement `feed_specs(pid)` in the
connector and return additional families, for example:

- `pool_swaps`
- `pool_liquidity`
- `rpc_blocks`

The shared engine will create writers for each returned family automatically.
No shared runtime loop changes are required.

For ops visibility, include extra prefixes when checking health:

- `./ops/feed_health --window 60 --once`
