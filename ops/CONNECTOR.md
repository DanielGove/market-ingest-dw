# Ingest Connector Spec

This repository runs an ingest-only runtime.

## Scope

1. One websocket ingest daemon (`ops/internal/ws_ingest_daemon`)
2. Connector-owned protocol parsing and feed schemas
3. Ingest control socket commands: `SUB`, `UNSUB`, `LIST`, `STATUS`

## Feed Creation Model

1. Feeds are created on subscription, not by separate provisioning commands.
2. Runtime calls connector `feed_specs(product)` for each subscribed product.
3. Runtime creates feeds/writers from those specs and writes records.

## Segment Boundaries

1. Boundaries are automatic and writer-driven.
2. Runtime marks boundaries on disconnect/error (`ws_disconnect`, `ws_error`).
3. Manual roll commands are unsupported.

## Operator Commands

1. `./ops/deploy <venue> --instance <name> --status --health`
2. `./ops/ingest_ctl sub BTC-USD --instance <name>`
3. `./ops/ingest_ctl list --instance <name>`
4. `./ops/status --instance <name>` or `./ops/status --all-instances`
5. `./ops/feed_health --instance <name> --window 60 --once --hide-idle`
6. `./ops/segment_health --instance <name>` or `./ops/segment_health --all-instances`
7. `./ops/discover --all-instances --include-schemas`

## Observability

1. Logs: `ops/logs/ingest.<instance>.log`, `ops/logs/supervise.<instance>.log`
2. Control state: `./ops/ctl status --instance <name> --json`
3. Health scan: `./ops/feed_health --instance <name> --window 60 --once --json`
4. Discovery payload: `./ops/discover --all-instances --include-schemas`
