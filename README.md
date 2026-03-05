# Market Data Ingestion Runtime

Lean multi-venue websocket ingestion runtime built on Deepwater.

## Scope

- Shared websocket ingest runtime
- Venue connectors (Coinbase/Kraken/Hyperliquid)
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
