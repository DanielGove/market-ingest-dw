# Connector Interface

`connectors/base.py` defines the target adapter contract for venue modules.
Current connector modules:
- `connectors/coinbase_spot/connector.py`
- `connectors/kraken_spot/connector.py`
- `connectors/binance_spot/connector.py`
- `connectors/hyperliquid_perp/connector.py` — perp DEX (trades, l2, perp_ctx, funding, open_interest)
- `connectors/dydx_perp/connector.py` — dYdX v4 perp DEX (trades, l2, perp_ctx)
- `connectors/univ3_dex/connector.py` — Uniswap V3 via The Graph (pool_swaps, pool_liquidity)
- `connectors/univ2_dex/connector.py` — Uniswap V2 via The Graph (pool_swaps, pool_liquidity)
- `connectors/pancake_dex/connector.py` — PancakeSwap V3/V2 via The Graph (pool_swaps, pool_liquidity)
- `connectors/stablecoin_monitor/connector.py` — stablecoin transfers + supply via The Graph
- `connectors/bridge_monitor/connector.py` — cross-chain bridge transfers via The Graph

Shared protocol helpers:
- `connectors/thegraph_ws.py` — `TheGraphWsConnector` base class for graphql-transport-ws protocol

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

## Non-Trades/L2 Families (Pools, Stablecoins, Bridges, Funding)

If a venue needs feeds beyond `trades`/`l2`, implement `feed_specs(pid)` in the
connector and return additional families.  Currently active families:

- `pool_swaps` — AMM swap events (Uniswap V3/V2, PancakeSwap)
- `pool_liquidity` — mint/burn liquidity events (Uniswap V3/V2, PancakeSwap)
- `stablecoin_transfers` — on-chain stablecoin transfer events (mint M, burn B, transfer T)
- `stablecoin_supply` — per-token total/circulating supply snapshots
- `bridge_transfers` — cross-chain bridge transfer events (deposit D, withdrawal W)
- `perp_ctx` — perpetual derivatives context snapshot (mark price, oracle, funding, OI, premium)
- `funding` — lightweight per-product funding rate feed (Hyperliquid)
- `open_interest` — lightweight per-product open interest feed (Hyperliquid)

The shared engine will create writers for each returned family automatically.
No shared runtime loop changes are required.

For ops visibility, include extra prefixes when checking health:

- `./ops/feed_health --window 60 --once`

## TheGraph WebSocket Protocol

All DEX, stablecoin, and bridge connectors inherit from `TheGraphWsConnector`
in `connectors/thegraph_ws.py`.  This base class handles the full
`graphql-transport-ws` protocol state machine:

1. `on_connect` → sends `connection_init`
2. `handle_raw` → dispatches `connection_ack`, `ping`→`pong`, `next`→`_handle_data`, `error`→log
3. `send_subscribe` → sends `subscribe` with connector-provided query + variables
4. `send_unsubscribe` → sends `complete` for all active subscriptions
5. `on_timeout` → sends protocol-level `ping`; raises on heartbeat timeout

Subclasses implement:
- `_build_subscription(product_ids)` → `(graphql_query, variables_dict)`
- `_handle_data(engine, data, recv_us, now_us)` → write to family writers

## Recommended Expansion Order

To maximize coverage quickly without redesigning the shared runtime:

1. Add bridge + stablecoin connectors first ✅ *done*
   - families: `bridge_transfers`, `stablecoin_transfers`, `stablecoin_supply`
2. Add one concentrated-liquidity AMM path next ✅ *done*
   - start with `pool_swaps`, `pool_liquidity`
   - prefer a single high-value deployment target first (Uniswap v3 on Base)
3. Extend existing perp connectors with more context ✅ *done*
   - families: `funding`, `open_interest`, `perp_ctx`

If self-hosted RPC is unavailable, build connectors around managed websocket/indexing
providers that can emit decoded logs or event streams. Keep provider-specific transport
and message translation inside the connector; the shared runtime does not need to know
whether the upstream source is a CEX websocket, DEX indexer stream, or bridge event feed.
