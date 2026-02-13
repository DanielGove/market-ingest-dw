# Phase 3B Sign-Off Checklist

Use this checklist before starting Phase 4 calibration.

## Determinism

1. Run:
```bash
./tools/run_backtest determinism
```
2. Confirm output includes:
`PASS: deterministic replay checks matched`
3. Confirm checksums match between both runs:
- stream checksum
- intent checksum
- execution checksum

## Causality and timestamp discipline

1. Confirm engine manifest is produced:
`out/backtest/<RUN_ID>/engine-manifest.json`
2. Confirm no time reversal in events:
- scheduler input is monotonic by merged event ordering
- execution `processed_time >= event_time`
3. Confirm reverse latency path is active when configured:
- `processed_time - event_time == reverse_latency_us` for emitted statuses

## Lifecycle validation

1. Confirm no invalid status transition exceptions during run.
2. Confirm lifecycle only uses allowed transitions per `backtest/contracts.py`.

## Accounting sanity

1. Confirm at least one scenario with fills.
2. Confirm `position` and `pnl` in manifest are finite values.
3. Confirm no unexpected negative remaining size in execution events.

## Known current limits

- partial-fill richness is limited
- cancel/replace race modeling is simplified
- no queue-position truth with L2-only data

These are acceptable at Phase 3B if documented and understood before Phase 4.
