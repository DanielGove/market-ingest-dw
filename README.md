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
