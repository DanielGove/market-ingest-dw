#!/usr/bin/env python3
"""
Latency probe utilities:
- Estimate WS ingest latency from Deepwater L2 feed (recv_us - ev_us).
- Optional network ping to advanced-trade-ws.coinbase.com (or custom host).

Example:
  ./venv/bin/python tools/latency_probe.py \
      --base-path data/coinbase-main \
      --product BTC-USD \
      --seconds 30

Optional ping:
  ./venv/bin/python tools/latency_probe.py --ping --host advanced-trade-ws.coinbase.com
"""
import argparse
import subprocess
import statistics
from typing import List
from deepwater.platform import Platform


def percentiles(xs: List[float], ps=(50, 90, 95, 99)):
    if not xs:
        return {p: None for p in ps}
    xs = sorted(xs)
    n = len(xs)
    out = {}
    for p in ps:
        k = (p/100) * (n-1)
        lo = int(k)
        hi = min(lo+1, n-1)
        w = k - lo
        out[p] = xs[lo]*(1-w) + xs[hi]*w
    return out


def probe_feed_latency(base_path: str, product: str, seconds: int, max_records: int):
    plat = Platform(base_path=base_path)
    feed = f"CB-L2-{product.upper()}"
    r = plat.create_reader(feed)
    recs = r.latest(seconds=seconds)
    if max_records:
        recs = recs[-max_records:]
    if not recs:
        return None
    # fields: type, side, packet_us, recv_us, proc_us, ev_us, price, qty
    latencies = []
    for rec in recs:
        ev_us = rec[5]
        recv_us = rec[3]
        if ev_us and recv_us:
            latencies.append((recv_us - ev_us)/1000.0)  # to ms
    if not latencies:
        return None
    return {
        "count": len(latencies),
        "mean_ms": statistics.fmean(latencies),
        "median_ms": statistics.median(latencies),
        "p": percentiles(latencies),
        "min_ms": min(latencies),
        "max_ms": max(latencies),
    }


def run_ping(host: str, count: int = 5):
    try:
        out = subprocess.run(
            ["ping", "-c", str(count), host],
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout
        # Parse lines containing "time="
        times = []
        for line in out.splitlines():
            if "time=" in line:
                try:
                    part = line.split("time=")[1].split()[0]
                    times.append(float(part))
                except Exception:
                    pass
        if not times:
            return None
        return {
            "host": host,
            "count": len(times),
            "mean_ms": statistics.fmean(times),
            "median_ms": statistics.median(times),
            "p": percentiles(times),
            "min_ms": min(times),
            "max_ms": max(times),
        }
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-path", required=True)
    ap.add_argument("--product", default="BTC-USD")
    ap.add_argument("--seconds", type=int, default=30, help="Window of L2 data")
    ap.add_argument("--max-records", type=int, default=5000, help="Cap records for speed")
    ap.add_argument("--ping", action="store_true", help="Also run system ping")
    ap.add_argument("--host", default="advanced-trade-ws.coinbase.com")
    ap.add_argument("--ping-count", type=int, default=5)
    args = ap.parse_args()

    res = probe_feed_latency(args.base_path, args.product, args.seconds, args.max_records)
    if res:
        print(f"L2 latency for {args.product.upper()} over {res['count']} recs (ms):")
        print(f"  mean {res['mean_ms']:.3f}  median {res['median_ms']:.3f}"
              f"  p90 {res['p'][90]:.3f}  p99 {res['p'][99]:.3f}"
              f"  min {res['min_ms']:.3f}  max {res['max_ms']:.3f}")
    else:
        print("No L2 data to compute latency.")

    if args.ping:
        pong = run_ping(args.host, args.ping_count)
        if pong:
            print(f"Ping {args.host} (ms): mean {pong['mean_ms']:.2f}"
                  f" median {pong['median_ms']:.2f} p90 {pong['p'][90]:.2f}"
                  f" p99 {pong['p'][99]:.2f} min {pong['min_ms']:.2f} max {pong['max_ms']:.2f}")
        else:
            print("Ping failed or unavailable.")


if __name__ == "__main__":
    main()
