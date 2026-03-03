# Connector Interface

`connectors/base.py` defines the target adapter contract for venue modules.
Current connector modules:
- `connectors/coinbase_spot/connector.py`
- `connectors/kraken_spot/connector.py`
- `connectors/hyperliquid_perp/connector.py`

Goals:
- keep shared ingest runtime venue-agnostic
- keep venue-specific transport/subscription/parsing logic connector-owned
- support event families beyond trades/l2/orderbook

Current runtime behavior is unchanged; these adapters preserve Coinbase/Kraken
behavior while reducing venue-specific runtime duplication.

`SharedWsIngestEngine` supports:
- compatibility mode via `trades_spec` + `l2_spec`
- generic mode via optional `feed_specs(pid)` for arbitrary families

Venue wrapper construction is centralized in:
- `runtime/ingest/market_engine.py`

Connector registration is centralized in:
- `connectors/registry.py`

## Minimal Add-Venue Workflow

To add a new venue without touching shared runtime loops:

1. Add connector module under `connectors/<venue_name>/connector.py` implementing `WsConnector` from `connectors/base.py`.
2. Define feed specs (`trades_spec`, `l2_spec`, or `feed_specs`).
3. Add venue defaults in `runtime/venues.py`.
4. Register the connector in `connectors/registry.py` (single registration entry).

Shared runtime remains unchanged:
- `runtime/ingest/ws_engine.py`
- `runtime/ingest/ws_ingest_daemon.py`

## Non-Trades/L2 Families (Pools, RPC, Funding)

If a venue needs feeds beyond `trades`/`l2`, implement `feed_specs(pid)` in the
connector and return additional families, for example:

- `pool_swaps`
- `pool_liquidity`
- `rpc_blocks`

The shared engine will create writers for each returned family automatically.
No shared runtime loop changes are required.

For ops visibility, include extra prefixes when checking health:

- `./tools/feed_health --extra-prefixes HL-POOL,HL-RPC --window 60 --once`
