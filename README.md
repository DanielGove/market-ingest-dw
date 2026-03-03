# Market Data Ingestion Runtime

Lean multi-venue ingestion and orderbook-derivation runtime built on Deepwater.

## Scope

- Shared websocket ingest runtime
- Venue connectors (Coinbase/Kraken/Hyperliquid)
- Derived orderbook construction from L2 feeds
- Control sockets + operational run/status/stop tooling
- Feed health monitoring

This repo is intentionally ingestion-only.

## Architecture

- `runtime/`
  - `runtime/ingest/ws_engine.py`: websocket lifecycle/reconnect loop
  - `runtime/ingest/market_engine.py`: venue + connector wiring
  - `runtime/ingest/ws_ingest_daemon.py`: daemon wrapper + control socket
  - `runtime/venues.py`: venue defaults/path resolution
- `connectors/`
  - venue adapters and registry
- `feeds/`
  - operator scripts, daemons, and control CLIs
- `tools/`
  - operational helpers (health/cron/ensure)

## Quick Start

```bash
./setup_env
./feeds/run
./feeds/status
```

## Venue Selection

```bash
./feeds/venue kraken run
./feeds/venue kraken status
```

or

```bash
DW_VENUE=kraken ./feeds/run
DW_VENUE=kraken ./feeds/status
```

## Control Plane

```bash
./feeds/ingest_ctl.py list
./feeds/ob_ctl.py list
./feeds/segment_ctl.py roll --service all
```

## Health + Ops

```bash
./tools/feed_health --window 60 --once --hide-idle
./feeds/restart_orderbook
./feeds/start_24x7
./feeds/stop all
```
