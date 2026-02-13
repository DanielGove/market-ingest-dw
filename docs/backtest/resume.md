# Resume Guide

Use this when restarting work after a break.

## 5-minute restart

1. Check repo state:
```bash
git status --short
```
2. Run a baseline backtest:
```bash
./tools/run_backtest all
```
3. Open produced artifacts:
- `out/backtest/<RUN_ID>/engine-manifest.json`
4. Confirm determinism pass is still green.

## Current development checkpoint

- Architecture phase: closed-loop + determinism complete
- Main open area: exchange model fidelity and strategy development

## Next-session TODO template

Copy and update this section at the top of your session notes.

```text
Date:
Goal:
Window/Product:
Strategy:
Hypothesis:
Run ID:
Commands executed:
Result summary:
Open questions:
Next action:
```

## Recommended branch discipline

1. One branch per change set.
2. Keep backtest runtime changes separate from docs-only changes.
3. Save manifests/reports by run id so comparisons are reproducible.

## Remaining runtime tasks

1. Improve exchange lifecycle fidelity (cancel/replace/reject paths).
2. Add robust run-scoped intent/execution slicing.
3. Expand deterministic tests for edge/tie timestamp cases.
