# Market Data Ingestion Runtime

Lean multi-venue websocket ingestion runtime built on Deepwater.

## Scope

- Shared websocket ingest runtime
- Venue connectors (Coinbase/Kraken/Hyperliquid Perp)
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
- `ops/`
  - operator scripts, daemons, and control CLIs
  - `ops/internal/venue_profile`: emits venue defaults for shell tooling
  - `ops/feed_health`: operational ingest health helper

## Quick Start

```bash
./setup_env
./ops/deploy coinbase --status --health
```

Other venues:

```bash
./ops/deploy kraken --status --health
./ops/deploy hyperliquid --status --health
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

### Hyperliquid Perpetuals

The `hyperliquid` connector ingests trades and L2 book snapshots from the
Hyperliquid perp DEX WebSocket API (`wss://api.hyperliquid.xyz/ws`).

It also records derivatives context from the public `activeAssetCtx` stream into
dedicated `perp_ctx`, `funding`, and `open_interest` feed families so feature
work can start from normalized time series instead of re-parsing raw snapshots.

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

1. **Stablecoin + bridge flows first**
   - Capture transfers, mint/burn events, and bridge ingress/egress for major stablecoins
   - Start with the highest-importance rails: Ethereum, Base, Arbitrum, Optimism, BNB Chain
   - Recommended families: `stablecoin_transfers`, `stablecoin_supply`, `bridge_transfers`
2. **AMM pool flow next**
   - Start with **Uniswap v3 on Base**, then **Uniswap v3 on Ethereum**, then **PancakeSwap on BNB Chain**
   - Add v2-style pools only after the first concentrated-liquidity path is working cleanly
   - Recommended families: `pool_swaps`, `pool_liquidity`, `pool_ticks`
3. **Perp context expansion**
   - Extend perp venues with `funding`, `open_interest`, `liquidations`, and venue-specific context feeds

Practical sourcing guidance when self-hosting RPC is off the table:

- prefer managed websocket/indexing providers over self-run RPC
- prefer provider streams that already decode logs/events for swaps, liquidity changes, and bridge activity
- treat raw RPC polling as a fallback, not the default ingestion path

This sequence gets the biggest macro flow coverage quickly: centralized price discovery, perp leverage, AMM routing, and the cross-chain stablecoin/bridge balance sheet.
