#!/usr/bin/env python3
"""
Check monotonicity of L2 timestamps: packet_us, recv_us, proc_us, ev_us.
Reports counts of non-monotonic transitions and basic stats per field.

Example:
  ./venv/bin/python tools/l2_monotonicity_check.py \
      --base-path data/coinbase-main \
      --product BTC-USD \
      --seconds 60
"""
import argparse
import statistics
from deepwater.platform import Platform


def monotonic_errors(recs, idx):
    errors = 0
    prev = None
    for r in recs:
        val = r[idx]
        if prev is not None and val < prev:
            errors += 1
        prev = val
    return errors


def describe(vals):
    if not vals:
        return {}
    return {
        "count": len(vals),
        "min": min(vals),
        "max": max(vals),
        "mean": statistics.fmean(vals),
        "p50": statistics.median(vals),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-path", required=True)
    ap.add_argument("--product", default="BTC-USD")
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--max-records", type=int, default=100000)
    args = ap.parse_args()

    plat = Platform(base_path=args.base_path)
    r = plat.create_reader(f"CB-L2-{args.product.upper()}")
    recs = r.latest(seconds=args.seconds)
    if args.max_records and recs:
        recs = recs[-args.max_records:]
    if not recs:
        print("No records")
        return

    names = list(r.field_names)
    # dynamic index mapping to tolerate schema changes
    idx_ev = names.index("event_time")
    idx_packet = names.index("packet_sent")
    idx_recv = names.index("received_time")
    idx_proc = names.index("processed_time")

    print(f"Records analyzed: {len(recs)} over last {args.seconds}s")

    for label, idx in (("ev", idx_ev), ("packet", idx_packet), ("recv", idx_recv), ("proc", idx_proc)):
        vals = [rec[idx] for rec in recs]
        errs = monotonic_errors(recs, idx)
        stats = describe(vals)
        print(f"{label}_us: monotonic breaks={errs} min={stats.get('min')} max={stats.get('max')} mean={stats.get('mean'):.1f} p50={stats.get('p50')}")

    # Cross-field sanity: ev <= packet <= recv <= proc (expected)
    cross_errors = 0
    for rec in recs:
        ev = rec[idx_ev]; packet = rec[idx_packet]; recv = rec[idx_recv]; proc = rec[idx_proc]
        if not (ev <= packet <= recv <= proc):
            cross_errors += 1
    print(f"cross-order violations (ev<=packet<=recv<=proc): {cross_errors}")


if __name__ == "__main__":
    main()
