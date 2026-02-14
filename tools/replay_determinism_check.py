#!/usr/bin/env python3
"""Determinism check over a frozen processed-time window."""
import argparse
import tempfile
from pathlib import Path
import sys

from deepwater.platform import Platform

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from backtest import engine as bt_engine


def _detect_proc_scale(sample_raw: int) -> int:
    return 1000 if sample_raw > 10**17 else 1


def _freeze_window(strategy_name: str, seconds: int):
    strat = bt_engine.load_strategy(strategy_name)
    subs = list(strat.subscriptions())
    market_sub = next((s for s in subs if s.method != "on_status"), None)
    if market_sub is None:
        raise SystemExit("Strategy has no market subscription")

    reader = Platform(base_path=market_sub.base_path).create_reader(market_sub.feed)
    names = tuple(reader.field_names)
    proc_field = "processed_time" if "processed_time" in names else "processed_us"
    idx_proc = names.index(proc_field)
    recs = reader.latest(seconds=max(1, seconds))
    if not recs:
        raise SystemExit("No records in selected window")

    scale = _detect_proc_scale(int(recs[-1][idx_proc]))
    end_proc_us = int(recs[-1][idx_proc]) // scale
    start_proc_us = end_proc_us - int(seconds * 1_000_000.0)
    return start_proc_us, end_proc_us


def _run_once(args, start_proc_us: int, end_proc_us: int, run_label: str):
    tmp = Path(tempfile.mkdtemp(prefix=f"dw-det-{run_label}-"))
    ns = argparse.Namespace(
        strategy=args.strategy,
        seconds=args.seconds,
        run_id=f"{args.run_id}-{run_label}",
        output_base_path=str(tmp / "io"),
        window_start_processed_us=start_proc_us,
        window_end_processed_us=end_proc_us,
        manifest_path=None,
        strict_contracts=True,
    )
    return bt_engine.run(ns)


def main():
    ap = argparse.ArgumentParser(description="Determinism replay check over frozen event slice.")
    ap.add_argument("--strategy", default="ping_pong")
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--run-id", default="DETCHK")
    args = ap.parse_args()

    start_proc_us, end_proc_us = _freeze_window(args.strategy, args.seconds)
    a = _run_once(args, start_proc_us, end_proc_us, "a")
    b = _run_once(args, start_proc_us, end_proc_us, "b")

    checks = {
        "events": a["events"] == b["events"],
        "snapshot_events": a["snapshot_events"] == b["snapshot_events"],
        "trade_events": a["trade_events"] == b["trade_events"],
        "intent_records": a["intent_feed"]["records"] == b["intent_feed"]["records"],
        "execution_records": a["execution_feed"]["records"] == b["execution_feed"]["records"],
        "status_counts": a["status_counts"] == b["status_counts"],
        "status_callbacks": a["status_callbacks"] == b["status_callbacks"],
        "position": a["position"] == b["position"],
        "pnl": a["pnl"] == b["pnl"],
    }

    print("run_a", a)
    print("run_b", b)
    print("checks", checks)

    if all(checks.values()):
        print("PASS: deterministic replay checks matched")
        return
    raise SystemExit("FAIL: deterministic replay mismatch")


if __name__ == "__main__":
    main()
