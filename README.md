# Market Data Ingestion Runtime

Lean multi-venue websocket ingestion runtime built on Deepwater.

## Scope

- Shared websocket ingest runtime
- Venue connectors (Coinbase / Kraken / Binance / Hyperliquid Perp / dYdX Perp)
- AMM pool flow connectors (Uniswap V3, Uniswap V2, PancakeSwap) via The Graph
- Stablecoin and bridge monitoring connectors via The Graph
- Runtime data plane for 24x7 websocket ingest + feed writes
- Control sockets + operational run/status/stop tooling
- Feed health monitoring

This repo is intentionally ingestion-only.

## Architecture

- `runtime/`
  - `runtime/ws_engine.py`: websocket lifecycle/reconnect loop
  - `runtime/engine_factory.py`: connector + runtime wiring
  - `runtime/ws_ingest_daemon.py`: daemon wrapper + control socket
  - `runtime/venue.py`: runtime-side venue/profile constructor helpers
- `connectors/`
  - venue adapters and connector-owned profiles (dynamic discovery)
  - `connectors/thegraph_ws.py`: shared graphql-transport-ws base for The Graph connectors
- `ops/`
  - operator scripts, daemons, and control CLIs
  - `ops/internal/venue_profile`: emits venue defaults for shell tooling
  - `ops/feed_health`: operational ingest health helper

## Quick Start

```bash
./setup_env
./ops/deploy coinbase --status --health
```

Other CEX venues:

```bash
./ops/deploy kraken --status --health
./ops/deploy binance --status --health
./ops/deploy hyperliquid --status --health
./ops/deploy dydx --status --health
```

DEX venues (URI configures target chain):

```bash
# Uniswap V3 – Ethereum mainnet (The Graph; API key required for production)
./ops/deploy univ3 \
  --uri wss://gateway.thegraph.com/api/<API_KEY>/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV \
  --products WETH-USDC,WBTC-WETH,WETH-USDT,WETH-DAI \
  --instance univ3-eth

# Uniswap V3 – Base (same connector, different chain URI)
./ops/deploy univ3 \
  --uri wss://gateway.thegraph.com/api/<API_KEY>/subgraphs/id/GqzP4Xaehti8KSfQmv3ZctFSjnSUYZ4En5NRsiTbvZpz \
  --products WETH-USDC,WETH-USDT \
  --instance univ3-base

# Uniswap V2 – Ethereum
./ops/deploy univ2 \
  --uri wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2 \
  --instance univ2-eth

# PancakeSwap V3 – BNB Chain
./ops/deploy pancake \
  --uri wss://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-bnb \
  --products WBNB-USDT,WBNB-USDC,ETH-WBNB,BTCB-WBNB \
  --instance pancake-v3-bsc

# PancakeSwap V2 – BNB Chain
./ops/deploy pancake \
  --uri wss://api.thegraph.com/subgraphs/name/pancakeswap/exchange \
  --products WBNB-USDT,CAKE-WBNB \
  --instance pancake-v2-bsc

# Stablecoin monitor – Ethereum
./ops/deploy stablecoin \
  --uri wss://api.thegraph.com/subgraphs/name/messari/ethereum-stablecoins \
  --products USDT,USDC,DAI,FRAX \
  --instance stablecoin-eth

# Bridge monitor – Stargate Ethereum
./ops/deploy bridge \
  --uri wss://api.thegraph.com/subgraphs/name/stargate-finance/mainnet-eth \
  --products USDC,USDT,ETH \
  --instance bridge-stargate-eth
```

Minimal operator flow (new host/region):

```bash
git clone <repo>
cd market-ingest-dw
./setup_env
./ops/deploy kraken --products BTC-USD --uri wss://ws.kraken.com/v2 --status
```

## Venue Selection

```bash
./ops/deploy kraken --status --health
DW_VENUE=kraken ./ops/status
```

Notes:
- `--instance` identifies the daemon instance token.
- `--venue`/`DW_VENUE_KEY` identifies connector platform.
- Health/status commands now fail fast when venue cannot be resolved.

## Data Portfolio Overview

### CEX Perpetuals

| Venue | Key | Feed Families | Endpoint |
|-------|-----|--------------|----------|
| Hyperliquid | `hyperliquid` | `trades`, `l2`, `perp_ctx`, `funding`, `open_interest` | `wss://api.hyperliquid.xyz/ws` |
| dYdX v4 | `dydx` | `trades`, `l2`, `perp_ctx` | `wss://indexer.dydx.trade/v4/ws/v4` |

### DEX AMM (via The Graph)

| Venue | Key | Feed Families | Default URI / Subgraph |
|-------|-----|--------------|----------------------|
| Uniswap V3 | `univ3` | `pool_swaps`, `pool_liquidity` | Configurable per chain (Ethereum, Base, Arbitrum, Optimism) |
| Uniswap V2 | `univ2` | `pool_swaps`, `pool_liquidity` | `uniswap/uniswap-v2` |
| PancakeSwap | `pancake` | `pool_swaps`, `pool_liquidity` | `pancakeswap/exchange-v3-bnb` (V3 default; V2 via URI override) |

### Stablecoin + Bridge (via The Graph)

| Venue | Key | Feed Families | Notes |
|-------|-----|--------------|-------|
| Stablecoin Monitor | `stablecoin` | `stablecoin_transfers`, `stablecoin_supply` | Multi-chain via URI; USDT, USDC, DAI, FRAX, LUSD defaults |
| Bridge Monitor | `bridge` | `bridge_transfers` | Covers Stargate, Hop, Across; direction D=deposit W=withdrawal |

### CEX Spot

| Venue | Key | Feed Families |
|-------|-----|--------------|
| Coinbase | `coinbase` | `trades`, `l2` |
| Kraken | `kraken` | `trades`, `l2` |
| Binance | `binance` | `trades`, `l2` |

### Hyperliquid Perpetuals

The `hyperliquid` connector ingests trades, L2 book snapshots, and full
perpetual context from the Hyperliquid perp DEX WebSocket API
(`wss://api.hyperliquid.xyz/ws`).

**Feed families:**
- `trades` — per-trade events
- `l2` — L2 book snapshots
- `perp_ctx` — full context snapshot (funding, OI, mark/oracle/mid prices, premium)
- `funding` — lightweight funding-rate-only feed (derived from `activeAssetCtx`)
- `open_interest` — lightweight OI-only feed (derived from `activeAssetCtx`)

**Default product set** — main crypto perps by open interest / volume plus
non-crypto perpetuals using Hyperliquid's `@<ticker>` index notation:

| Category | Products |
|----------|----------|
| Crypto perps | BTC-USD, ETH-USD, SOL-USD, XRP-USD, BNB-USD, AVAX-USD, DOGE-USD, LINK-USD, ARB-USD, OP-USD, MATIC-USD, SUI-USD, APT-USD, INJ-USD, TIA-USD, WIF-USD |
| S&P 500 index perp | @SPX-USD |
| Gold futures perp | @GOLD-USD |
| Silver futures perp | @SILVER-USD |

> The `@`-prefixed tickers map directly to Hyperliquid's index perpetual coin
> names.  If a particular instrument is not yet live on Hyperliquid the
> subscription is silently skipped; no runtime error is raised.

Override the product list at deploy time:

```bash
./ops/deploy hyperliquid --products "BTC-USD,ETH-USD,@GOLD-USD,@SPX-USD"
```

### dYdX Perpetuals

The `dydx` connector ingests trades, orderbook snapshots, and perpetual
context from the public dYdX v4 indexer WebSocket (`wss://indexer.dydx.trade/v4/ws/v4`).
No API key is required.

```bash
./ops/deploy dydx --products "BTC-USD,ETH-USD,SOL-USD" --status
```

### DEX AMM Connectors (The Graph)

All three DEX connectors share the `graphql-transport-ws` protocol via the
shared `connectors/thegraph_ws.py` base.  The connector handles the
`connection_init` / `connection_ack` / `ping` / `pong` / `subscribe` /
`next` / `complete` protocol states.  Each `next` notification contains
events from the latest indexed block; the connector deduplicates by block
number.

Products are specified as `TOKEN0-TOKEN1` pair names (e.g. `WETH-USDC`).
The GraphQL subscription filters by token symbols.

**Chain coverage via URI override:**

| Chain | univ3 Subgraph ID |
|-------|------------------|
| Ethereum mainnet | `5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV` |
| Base | `GqzP4Xaehti8KSfQmv3ZctFSjnSUYZ4En5NRsiTbvZpz` |
| Arbitrum One | `FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM` |
| Optimism | `Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj` |

URI pattern: `wss://gateway.thegraph.com/api/<API_KEY>/subgraphs/id/<SUBGRAPH_ID>`

### Stablecoin + Bridge Monitoring

Both connectors use The Graph via `graphql-transport-ws`.  Products are
asset symbols for stablecoin (e.g. `USDT,USDC,DAI`) and for bridge
(e.g. `USDC,USDT,ETH`).

**Supported stablecoin subgraphs (Messari):**

| Chain | URI path suffix |
|-------|----------------|
| Ethereum | `messari/ethereum-stablecoins` |
| Arbitrum | `messari/arbitrum-stablecoins` |
| Base | `messari/base-stablecoins` |
| Optimism | `messari/optimism-stablecoins` |
| BNB Chain | `messari/bsc-stablecoins` |

**Supported bridge subgraphs:**

| Bridge | Chain | URI path suffix |
|--------|-------|----------------|
| Stargate | Ethereum | `stargate-finance/mainnet-eth` |
| Stargate | Arbitrum | `stargate-finance/arbitrum` |
| Stargate | Base | `stargate-finance/base` |
| Hop | Ethereum | `hop-protocol/hop-mainnet` |
| Hop | Arbitrum | `hop-protocol/hop-arbitrum` |

## Control Plane

```bash
./ops/ingest_ctl list
./ops/ingest_ctl status
./ops/discover --all-instances
```

Machine-readable discovery (for feature pipelines):

```bash
./ops/discover --all-instances --include-schemas > runtime_discovery.json
```

## Health + Ops

```bash
./ops/feed_health --window 60 --once --hide-idle
./ops/segment_health --all-instances
./ops/stop ingest
```

## Deployment Readiness

The current runtime is in a good place to run continuously **without a shared-runtime
redesign** if these operating conditions hold:

- `./ops/status --all-instances` shows healthy websocket connectivity and expected subscriptions
- `./ops/feed_health --window 60 --once --json` shows steady row production with low lag
- `./ops/segment_health --all-instances` shows clean boundaries without repeated churn
- `./ops/discover --all-instances --include-schemas` remains stable enough for downstream feature jobs

Decision rule:

- **Go ahead with features / new connectors** when the shared runtime is stable and the next source can be modeled as connector-owned families
- **Redesign first** only if a new source requires shared runtime behavior that is not stream-like (for example: long-running historical backfills, cross-connector joins inside ingest, or a scheduler that depends on global state)

For AMMs, bridges, and stablecoin monitoring, the current connector boundary is still the right one: keep venue/provider transport and parsing inside `connectors/*`, and let the shared runtime keep doing lifecycle, health, and writes.

## Aggressive Data Portfolio Priorities

If the objective is answering **"where is the money coming from, where is it going?"** as fast as possible, prioritize the next feeds in this order:

1. **Stablecoin + bridge flows first** ✅ *now implemented*
   - Capture transfers, mint/burn events, and bridge ingress/egress for major stablecoins
   - Start with the highest-importance rails: Ethereum, Base, Arbitrum, Optimism, BNB Chain
   - Deploy: `stablecoin` (5 chains via URI) + `bridge` (Stargate/Hop via URI)
   - Families: `stablecoin_transfers`, `stablecoin_supply`, `bridge_transfers`
2. **AMM pool flow next** ✅ *now implemented*
   - Start with **Uniswap v3 on Base**, then **Uniswap v3 on Ethereum**, then **PancakeSwap on BNB Chain**
   - Add v2-style pools: `univ2` for Ethereum V2 pools
   - Families: `pool_swaps`, `pool_liquidity`
3. **Perp context expansion** ✅ *now implemented*
   - Hyperliquid extended with `funding` + `open_interest` lightweight feeds
   - dYdX v4 added as second perp DEX venue
   - Families: `funding`, `open_interest`, `perp_ctx`

Practical sourcing guidance when self-hosting RPC is off the table:

- prefer managed websocket/indexing providers over self-run RPC
- prefer provider streams that already decode logs/events for swaps, liquidity changes, and bridge activity
- treat raw RPC polling as a fallback, not the default ingestion path
- The Graph subscriptions require an API key for the decentralised network gateway; the legacy hosted service endpoints work without keys for dev/stress-testing

This sequence gets the biggest macro flow coverage quickly: centralized price discovery, perp leverage, AMM routing, and the cross-chain stablecoin/bridge balance sheet.
