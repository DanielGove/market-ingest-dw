#!/usr/bin/env python3
"""Phase 3B determinism check.

Runs the engine twice on the same frozen market event slice and compares
intent/execution checksums, status counts, and PnL/position.
"""
import argparse
import tempfile
from pathlib import Path
import sys

from deepwater.platform import Platform

# ensure project root on path
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from backtest import engine as bt_engine


def _detect_proc_scale(sample_raw: int) -> int:
    return 1000 if sample_raw > 10**17 else 1


def _freeze_window(args):
    plat = Platform(base_path=args.base_path)
    ob_feed = f"OB{args.depth}{args.period}-{args.product.upper()}"
    r = plat.create_reader(ob_feed)
    names = r.field_names
    idx_proc = names.index("processed_time") if "processed_time" in names else names.index("processed_us")
    recs = r.latest(seconds=max(1, args.seconds))
    if not recs:
        raise SystemExit("No OB snapshots in selected window")
    scale = _detect_proc_scale(int(recs[-1][idx_proc]))
    end_proc_us = int(recs[-1][idx_proc]) // scale
    start_proc_us = end_proc_us - int(args.seconds * 1_000_000.0)
    return start_proc_us, end_proc_us


def _run_once(args, window_start_proc_us, window_end_proc_us, run_label):
    tmp = Path(tempfile.mkdtemp(prefix=f"dw-det-{run_label}-"))
    ns = argparse.Namespace(
        base_path=args.base_path,
        intent_base_path=str(tmp / "intents"),
        execution_base_path=str(tmp / "exec"),
        product=args.product,
        depth=args.depth,
        period=args.period,
        seconds=args.seconds,
        strategy=args.strategy,
        run_id=args.run_id,
        latency_ms=args.latency_ms,
        reverse_latency_ms=args.reverse_latency_ms,
        fee_bps=args.fee_bps,
        maker_fee_bps=args.maker_fee_bps,
        decision_latency_us=0,
        playback=False,
        window_start_processed_us=window_start_proc_us,
        window_end_processed_us=window_end_proc_us,
        manifest_path=None,
        strict_contracts=True,
        events_override=None,
    )
    return bt_engine.run(ns)


def main():
    ap = argparse.ArgumentParser(description="Determinism replay check over frozen event slice.")
    ap.add_argument("--base-path", default="data/coinbase-main")
    ap.add_argument("--product", default="BTC-USD")
    ap.add_argument("--depth", type=int, default=200)
    ap.add_argument("--period", type=int, default=50)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--strategy", default="ping_pong")
    ap.add_argument("--run-id", default="DETCHK")
    ap.add_argument("--latency-ms", type=float, default=0.5)
    ap.add_argument("--reverse-latency-ms", type=float, default=0.5)
    ap.add_argument("--fee-bps", type=float, default=1.0)
    ap.add_argument("--maker-fee-bps", type=float, default=0.0)
    args = ap.parse_args()

    window_start_proc_us, window_end_proc_us = _freeze_window(args)
    a = _run_once(args, window_start_proc_us, window_end_proc_us, "a")
    b = _run_once(args, window_start_proc_us, window_end_proc_us, "b")

    checks = {
        "stream_checksum": a["stream_checksum"] == b["stream_checksum"],
        "intent_checksum": a["intent_feed"]["checksum"] == b["intent_feed"]["checksum"],
        "execution_checksum": a["execution_feed"]["checksum"] == b["execution_feed"]["checksum"],
        "status_counts": a["status_counts"] == b["status_counts"],
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
