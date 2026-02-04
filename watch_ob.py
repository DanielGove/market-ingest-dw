#!/usr/bin/env python3
"""
Simple OB200200 stream viewer: prints midprice and spread for each snapshot
"""
import sys
from deepwater import Platform

def main():
    product = 'XRP-USD'
    base_path = './data/coinbase-main'
    feed_name = f"OB200200-{product}"
    
    platform = Platform(base_path=base_path)
    reader = platform.create_reader(feed_name)
    
    print(f"Streaming {feed_name}...")
    print("Timestamp            | Best Bid | Best Ask | Mid Price | Spread  | Spread %")
    print("-" * 85)
    
    # derive indices from schema to avoid hard-coded offsets
    fields = reader.field_names
    snapshot_idx = fields.index("snapshot_us")
    bid_idx = fields.index("bid_price_0")
    ask_idx = fields.index("ask_price_0")

    stream = reader.stream_live(playback=False)

    while True:
        rec = next(stream)
        snapshot_us = rec[snapshot_idx]
        best_bid_price = rec[bid_idx]
        best_ask_price = rec[ask_idx]
        
        if best_bid_price > 0 and best_ask_price > 0:
            mid_price = (best_bid_price + best_ask_price) / 2
            spread = best_ask_price - best_bid_price
            spread_pct = (spread / mid_price) * 100
            
            print(f"{snapshot_us:20d} | {best_bid_price:8.4f} | {best_ask_price:8.4f} | "
                  f"{mid_price:9.4f} | {spread:7.4f} | {spread_pct:7.4f}%")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
