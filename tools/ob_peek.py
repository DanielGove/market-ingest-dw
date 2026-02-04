#!/usr/bin/env python3
"""
Quick sanity printer: show 10s of OB + L2 activity per product.
Helps confirm mid moves and snapshot freshness.
"""
import argparse
import time
from collections import defaultdict
from deepwater.platform import Platform

def summarize_l2(reader, seconds):
    recs = reader.latest(seconds=seconds)
    if not recs:
        return {"count": 0, "best_bid": None, "best_ask": None}
    # apply qty=0 deletes newest->oldest so top-of-book matches the live book
    bids = {}
    asks = {}
    for r in reversed(recs):
        price = r[6]
        qty = r[7] if len(r) <= 8 else r[8]  # some L2 schemas may omit padding
        side = r[1]
        if side == b'b':
            if qty == 0.0:
                bids.pop(price, None)
            else:
                bids[price] = qty
        else:
            if qty == 0.0:
                asks.pop(price, None)
            else:
                asks[price] = qty
    bid_prices = sorted(bids.keys(), reverse=True)
    ask_prices = sorted(asks.keys())
    best_bid = bid_prices[0] if bid_prices else None
    best_ask = ask_prices[0] if ask_prices else None
    return {
        "count": len(recs),
        "best_bid": best_bid,
        "best_ask": best_ask,
    }

def summarize_ob(reader, seconds, depth):
    recs = reader.latest(seconds=seconds)
    if not recs:
        return {"count": 0, "best_bid": None, "best_ask": None, "age_ms": None}
    last = recs[-1]
    snap_ts = last[1]
    now_us = int(time.time() * 1_000_000)
    bids = [last[2 + i * 2] for i in range(depth) if last[2 + i * 2] != 0.0]
    asks = [last[2 + depth * 2 + i * 2] for i in range(depth) if last[2 + depth * 2 + i * 2] != 0.0]
    best_bid = bids[0] if bids else None
    best_ask = asks[0] if asks else None
    return {
        "count": len(recs),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "age_ms": (now_us - snap_ts) / 1000.0 if snap_ts else None,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-path", default="data/coinbase-main")
    ap.add_argument("--products", required=True, help="Comma list, e.g. BTC-USD,XRP-USD")
    ap.add_argument("--depth", type=int, default=200)
    ap.add_argument("--period", type=int, default=50)
    ap.add_argument("--seconds", type=int, default=10)
    args = ap.parse_args()

    platform = Platform(base_path=args.base_path)
    for prod in [p.strip().upper() for p in args.products.split(",")]:
        l2 = platform.create_reader(f"CB-L2-{prod}")
        ob = platform.create_reader(f"OB{args.depth}{args.period}-{prod}")
        l2_info = summarize_l2(l2, args.seconds)
        ob_info = summarize_ob(ob, args.seconds, args.depth)
        print(f"{prod}:")
        print(f"  L2: count={l2_info['count']} bid={l2_info['best_bid']} ask={l2_info['best_ask']}")
        print(f"  OB: count={ob_info['count']} bid={ob_info['best_bid']} ask={ob_info['best_ask']} age_ms={ob_info['age_ms']}")
        if l2_info["best_bid"] and ob_info["best_bid"]:
            mid_l2 = (l2_info["best_bid"] + l2_info["best_ask"]) / 2 if l2_info["best_ask"] else None
            mid_ob = (ob_info["best_bid"] + ob_info["best_ask"]) / 2 if ob_info["best_ask"] else None
            print(f"  Mid diff: {None if mid_l2 is None or mid_ob is None else mid_ob - mid_l2}")
        print("")

if __name__ == "__main__":
    main()
