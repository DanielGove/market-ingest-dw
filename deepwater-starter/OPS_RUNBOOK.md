# Deepwater Ops Runbook

Use this file during incidents and regular operations.

## Required Variable

```bash
export DEEPWATER_BASE=./data
```

## Health Checks

```bash
# Full health signal for monitoring
deepwater-health --base-path "$DEEPWATER_BASE" --check-feeds --max-age-seconds 300
```

Exit codes:
- `0`: healthy
- `1`: unhealthy
- `2`: checker error

## Storage Cleanup

```bash
# Preview
deepwater-cleanup --base-path "$DEEPWATER_BASE" --dry-run

# Execute
deepwater-cleanup --base-path "$DEEPWATER_BASE"
```

## Common Incident Playbooks

### A feed schema is broken during development

```bash
deepwater-delete-feed --base-path "$DEEPWATER_BASE" --feed trades
deepwater-create-feed --base-path "$DEEPWATER_BASE" --config ./configs/trades.json
```

### Multiple feeds need reset from known configs

```bash
deepwater-delete-feed --base-path "$DEEPWATER_BASE" --config-dir ./configs
deepwater-create-feed --base-path "$DEEPWATER_BASE" --config-dir ./configs
```

### Writer appears stuck

1) Run health check command above.
2) Validate process status in your supervisor/systemd.
3) If safe and in dev/test, perform feed reset using commands above.
4) Re-run integration script: `python ./apps/quickstart_app.py`.
