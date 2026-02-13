#!/usr/bin/env python3
"""
Emit a single-line health snapshot for a trades feed (market_trades).

Human-readable key=value line; intended to run fast (<150ms).
Fields: lag stats (recv-ev), monotonic breaks for packet/recv/proc/ev,
and basic percentiles.
"""
import argparse
import statistics
import time
from deepwater.platform import Platform


def percentiles(vals):
    if not vals:
        return {}
    qs = statistics.quantiles(vals, n=100, method="inclusive")
    return {"p50": qs[49], "p90": qs[89], "p99": qs[98]}


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
    ap.add_argument("--max-records", type=int, default=10000)
    args = ap.parse_args()

    start_ns = time.monotonic_ns()
    plat = Platform(base_path=args.base_path)
    reader = plat.create_reader(f"CB-TRADES-{args.product.upper()}")
    recs = reader.latest(seconds=args.seconds)
    if args.max_records and recs:
        recs = recs[-args.max_records:]
    if not recs:
        print(f"product={args.product} feed=trades status=no_records window_s={args.seconds}")
        return

    names = list(reader.field_names)
    idx_packet = names.index("packet_sent")
    idx_recv = names.index("received_time")
    idx_proc = names.index("processed_time")
    idx_ev = names.index("event_time")

    fields = {
        "packet": idx_packet,
        "recv": idx_recv,
        "proc": idx_proc,
        "ev": idx_ev,
    }

    stats = {name: describe([r[idx] for r in recs]) for name, idx in fields.items()}
    breaks = {name: monotonic_breaks(recs, idx) for name, idx in fields.items()}
    cross_breaks = 0
    for r in recs:
        ev, packet, recv, proc = r[idx_ev], r[idx_packet], r[idx_recv], r[idx_proc]
        if not (ev <= packet <= recv <= proc):
            cross_breaks += 1
    lag_vals = [r[idx_recv] - r[idx_ev] for r in recs]
    lag_stats = describe(lag_vals)
    duration_ms = (time.monotonic_ns() - start_ns) / 1e6

    parts = [
        f"product={args.product}",
        "feed=trades",
        f"window_s={args.seconds}",
        f"samples={len(recs)}",
        f"lag_ms_p50={lag_stats.get('p50',0)/1000:.1f}",
        f"lag_ms_p90={lag_stats.get('p90',0)/1000:.1f}",
        f"lag_ms_p99={lag_stats.get('p99',0)/1000:.1f}",
        f"lag_ms_min={lag_stats.get('min',0)/1000:.1f}",
        f"lag_ms_max={lag_stats.get('max',0)/1000:.1f}",
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
