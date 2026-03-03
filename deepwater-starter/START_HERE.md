# Deepwater: Start Here (2 Minutes)

This folder is designed for agents and humans to get productive immediately.

## Fast Path

1) Pick your data path:

```bash
export DEEPWATER_BASE=./data
```

2) Create feeds from config files:

```bash
deepwater-create-feed --base-path "$DEEPWATER_BASE" --config-dir ./configs
```

3) Inspect available feeds + schema metadata:

```bash
deepwater-feeds --base-path "$DEEPWATER_BASE"
deepwater-feeds --base-path "$DEEPWATER_BASE" --feed trades
```

4) Run the integration example:

```bash
python ./apps/quickstart_app.py
```

5) Check health:

```bash
deepwater-health --base-path "$DEEPWATER_BASE" --check-feeds --max-age-seconds 300
```

6) See usable segment windows:

```bash
deepwater-segments --base-path "$DEEPWATER_BASE" --feed trades --status usable --suggest-range
```

7) Compute common contiguous windows across your strategy feed set:

```bash
deepwater-datasets --base-path "$DEEPWATER_BASE"   --feeds cb_btcusd,cb_ethusd,cb_solusd,cb_xrpusd,kr_btcusd,kr_ethusd,kr_solusd,kr_xrpusd   --json
```

Two-base-path example:

```bash
deepwater-datasets   --source A=./data_us   --source B=./data_de   --feed-ref A:cb_btcusd --feed-ref A:cb_ethusd --feed-ref A:cb_solusd --feed-ref A:cb_xrpusd   --feed-ref B:kr_btcusd --feed-ref B:kr_ethusd --feed-ref B:kr_solusd --feed-ref B:kr_xrpusd   --json
```

## If You Need a Hard Reset

```bash
deepwater-delete-feed --base-path "$DEEPWATER_BASE" --config-dir ./configs
deepwater-create-feed --base-path "$DEEPWATER_BASE" --config-dir ./configs
```

Next: read `OPS_RUNBOOK.md` for incident procedures and `GUIDE.md` for full details.
