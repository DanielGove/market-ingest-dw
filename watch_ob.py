#!/usr/bin/env python3
"""
Simple OB200200 stream viewer: prints midprice and spread for each snapshot
"""
import sys
import time
from deepwater import Platform

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Watch orderbook feed")
    parser.add_argument("--base-path", default="data/coinbase-main", help="Deepwater base path")
    parser.add_argument("--product", default="BTC-USD", help="Product, e.g. BTC-USD")
    parser.add_argument("--depth", type=int, default=200, help="OB depth (matches feed)")
    parser.add_argument("--period", type=int, default=200, help="OB period ms (matches feed)")
    args = parser.parse_args()

    feed_name = f"OB{args.depth}{args.period}-{args.product.upper()}"
    
    platform = Platform(base_path=args.base_path)
    reader = platform.create_reader(feed_name)
    
    print(f"Streaming {feed_name}...")
    print("Snapshot_us          Lag(s) | Best Bid | Best Ask | Mid Price | Spread  | Spread %")
    print("-" * 98)
    
    # derive indices from schema to avoid hard-coded offsets
    fields = reader.field_names
    snapshot_idx = fields.index("snapshot_us")
    bid_idx = fields.index("bid_price_0")
    ask_idx = fields.index("ask_price_0")

    stream = reader.stream_live(playback=False)

    for rec in stream:
        snapshot_us = rec[snapshot_idx]
        best_bid_price = rec[bid_idx]
        best_ask_price = rec[ask_idx]
        
        if best_bid_price > 0 and best_ask_price > 0:
            mid_price = (best_bid_price + best_ask_price) / 2
            spread = best_ask_price - best_bid_price
            spread_pct = (spread / mid_price) * 100
            lag_s = max(0.0, (time.time_ns() / 1e9) - (snapshot_us / 1e6))
            
            print(f"{snapshot_us:20d} {lag_s:6.3f} | {best_bid_price:8.4f} | {best_ask_price:8.4f} | "
                  f"{mid_price:9.4f} | {spread:7.4f} | {spread_pct:7.4f}%")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
