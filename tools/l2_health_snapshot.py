#!/usr/bin/env python3
"""
Emit a single-line health snapshot for an L2 feed.

Designed to be fast (<150ms) and human-readable, not JSON.
Outputs key=value fields on one line.

Example:
  ./venv/bin/python tools/l2_health_snapshot.py \
      --base-path data/coinbase-main \
      --product BTC-USD \
      --seconds 60
"""
import argparse
import statistics
import time
from deepwater.platform import Platform


FIELDS = {
    "packet": 2,
    "recv": 3,
    "proc": 4,
    "ev": 5,
}


def percentiles(vals):
    if not vals:
        return {}
    # statistics.quantiles needs n>=1; guard above.
    qs = statistics.quantiles(vals, n=100, method="inclusive")
    return {
        "p50": qs[49],
        "p90": qs[89],
        "p99": qs[98],
    }


def describe(vals):
    if not vals:
        return {}
    return {
        "count": len(vals),
        "min": min(vals),
        "max": max(vals),
        "mean": statistics.fmean(vals),
        "p50": statistics.median(vals),
    } | percentiles(vals)


def monotonic_breaks(recs, idx):
    prev = None
    breaks = 0
    for rec in recs:
        v = rec[idx]
        if prev is not None and v < prev:
            breaks += 1
        prev = v
    return breaks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-path", required=True)
    ap.add_argument("--product", required=True)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--max-records", type=int, default=100000)
    args = ap.parse_args()

    start_ns = time.monotonic_ns()
    plat = Platform(base_path=args.base_path)
    reader = plat.create_reader(f"CB-L2-{args.product.upper()}")
    recs = reader.latest(seconds=args.seconds)
    if args.max_records and recs:
        recs = recs[-args.max_records:]

    if not recs:
        print(f"product={args.product} status=no_records window_s={args.seconds}")
        return

    # core stats
    stats = {name: describe([r[idx] for r in recs]) for name, idx in FIELDS.items()}
    # monotonic breaks
    breaks = {name: monotonic_breaks(recs, idx) for name, idx in FIELDS.items()}
    # cross-order violations
    cross_breaks = 0
    for r in recs:
        ev, packet, recv, proc = r[5], r[2], r[3], r[4]
        if not (ev <= packet <= recv <= proc):
            cross_breaks += 1
    # lag (recv - ev)
    lag_vals = [r[3] - r[5] for r in recs]
    lag_stats = describe(lag_vals)

    duration_ms = (time.monotonic_ns() - start_ns) / 1e6

    # human-readable single line
    parts = [
        f"product={args.product}",
        f"window_s={args.seconds}",
        f"samples={len(recs)}",
        f"lag_ms_p50={lag_stats.get('p50', 0)/1000:.1f}",
        f"lag_ms_p90={lag_stats.get('p90', 0)/1000:.1f}",
        f"lag_ms_p99={lag_stats.get('p99', 0)/1000:.1f}",
        f"lag_ms_min={lag_stats.get('min', 0)/1000:.1f}",
        f"lag_ms_max={lag_stats.get('max', 0)/1000:.1f}",
        f"packet_breaks={breaks['packet']}",
        f"recv_breaks={breaks['recv']}",
        f"proc_breaks={breaks['proc']}",
        f"ev_breaks={breaks['ev']}",
        f"cross_breaks={cross_breaks}",
        f"packet_p50={stats['packet'].get('p50')}",
        f"recv_p50={stats['recv'].get('p50')}",
        f"proc_p50={stats['proc'].get('p50')}",
        f"ev_p50={stats['ev'].get('p50')}",
        f"runtime_ms={duration_ms:.1f}",
    ]
    print(" ".join(parts))


if __name__ == "__main__":
    main()
