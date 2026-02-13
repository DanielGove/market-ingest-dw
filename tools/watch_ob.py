#!/usr/bin/env python3
"""
Lightweight live orderbook watcher for Deepwater OB feeds.

Shows best bid/ask, spread, and snapshot timestamp; works with any depth/period.
Designed to be low-overhead and human-readable.
"""
import argparse
from deepwater.platform import Platform


def main():
    ap = argparse.ArgumentParser(description="Watch an orderbook feed (best bid/ask)")
    ap.add_argument("--base-path", default="data/coinbase-main", help="Deepwater base path")
    ap.add_argument("--product", default="BTC-USD", help="Product, e.g., BTC-USD")
    ap.add_argument("--depth", type=int, default=200, help="OB depth (matches feed)")
    ap.add_argument("--period", type=int, default=50, help="OB period ms (matches feed)")
    ap.add_argument("--playback", action="store_true", help="Replay from start instead of live")
    args = ap.parse_args()

    feed_name = f"OB{args.depth}{args.period}-{args.product.upper()}"
    plat = Platform(base_path=args.base_path)
    reader = plat.create_reader(feed_name)
    names = reader.field_names
    idx_snap = names.index("snapshot_time") if "snapshot_time" in names else names.index("snapshot_us")
    idx_bid = names.index("bid_price_0")
    idx_ask = names.index("ask_price_0")

    print(f"Streaming {feed_name} (base={args.base_path})")
    stream = reader.stream(playback=args.playback)
    try:
        for r in stream:
            bid = r[idx_bid]
            ask = r[idx_ask]
            snap = r[idx_snap]
            spread = ask - bid if bid and ask else None
            if spread is not None:
                print(f"{snap}  bid={bid:.6f}  ask={ask:.6f}  spread={spread:.6f}")
            else:
                print(f"{snap}  bid={bid}  ask={ask}  spread=None")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
