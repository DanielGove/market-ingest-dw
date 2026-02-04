#!/usr/bin/env python3
"""
Simplified orderbook daemon.
 - One thread per product (no round-robin starvation)
 - Reads L2 playback=True so we get the initial snapshot
 - Emits snapshots every period_ms when book is valid
"""
import time
import signal
import logging
import argparse
import threading
from pathlib import Path
from collections import defaultdict
from sortedcontainers import SortedDict

from deepwater.platform import Platform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("orderbook")


class OrderBookDaemon:
    def __init__(self, products, base_path, depth, period_ms):
        self.products = [p.upper() for p in products]
        self.base_path = base_path
        self.depth = depth
        self.period_ms = period_ms
        self.running = False

        self.platform = None
        self.readers = {}
        self.writers = {}
        self.field_idx = {}

        self.bids = defaultdict(lambda: SortedDict(lambda x: -x))
        self.asks = defaultdict(SortedDict)
        self.last_l2_ts = {}

        self._threads = {}
        self._stops = {}

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # lifecycle -------------------------------------------------
    def start(self):
        log.info("Starting OB daemon products=%s depth=%d period_ms=%d", self.products, self.depth, self.period_ms)
        self.platform = Platform(base_path=self.base_path)
        self.running = True

        for prod in self.products:
            self._create_product(prod)
            stop_evt = threading.Event()
            self._stops[prod] = stop_evt
            t = threading.Thread(target=self._product_loop, args=(prod, stop_evt), name=f"ob-{prod}", daemon=True)
            self._threads[prod] = t
            t.start()

        try:
            while self.running:
                time.sleep(0.5)
        finally:
            self.stop()

    def stop(self):
        if not self.running:
            return
        self.running = False
        for evt in self._stops.values():
            evt.set()
        for t in self._threads.values():
            if t.is_alive():
                t.join(timeout=1.0)
        log.info("Stopped")

    def _handle_signal(self, signum, _frame):
        log.info("Signal %s, stopping", signum)
        self.stop()

    # per product -----------------------------------------------
    def _create_product(self, product):
        feed_name = f"OB{self.depth}{self.period_ms}-{product}"
        fields = [
            {"name": "type", "type": "char"},
            {"name": "_", "type": "_7"},
            {"name": "snapshot_us", "type": "uint64"},
        ]
        for i in range(self.depth):
            fields += [
                {"name": f"bid_price_{i}", "type": "float64"},
                {"name": f"bid_qty_{i}", "type": "float64"},
            ]
        for i in range(self.depth):
            fields += [
                {"name": f"ask_price_{i}", "type": "float64"},
                {"name": f"ask_qty_{i}", "type": "float64"},
            ]

        spec = {
            "feed_name": feed_name,
            "mode": "UF",
            "fields": fields,
            "ts_col": "snapshot_us",
            "chunk_size_bytes": 256 * 1024 * 1024,
            "persist": True,
            "index_playback": True,
        }
        try:
            self.platform.create_feed(spec)
        except RuntimeError as e:
            if "exists" in str(e) or "locked" in str(e):
                log.info("Feed %s exists, reuse", feed_name)
            else:
                raise

        self.writers[product] = self.platform.create_writer(feed_name)
        l2_name = f"CB-L2-{product}"
        self.readers[product] = self.platform.create_reader(l2_name)
        self.field_idx[product] = {n: i for i, n in enumerate(self.readers[product].field_names)}
        self.last_l2_ts[product] = 0
        self.bids[product].clear()
        self.asks[product].clear()
        log.info("Created product %s feeds (%s)", product, feed_name)

    def _product_loop(self, product, stop_evt):
        reader = self.readers[product]
        writer = self.writers[product]
        idx = self.field_idx[product]
        interval_us = self.period_ms * 1000
        next_snap = 0

        try:
            # Seed from the latest few seconds (no full backlog) to start fresh
            seed = reader.latest(seconds=5)
            if seed:
                seed = sorted(seed, key=lambda r: r[idx["ev_us"]])
                for rec in seed:
                    ev_us = self._process_l2(product, rec, idx)
                    self.last_l2_ts[product] = ev_us
                if self.last_l2_ts[product]:
                    next_snap = self.last_l2_ts[product] + interval_us

            # Live only (no backlog); ingest will get a new snapshot from feed soon
            for rec in reader.stream_live(playback=False):
                if stop_evt.is_set():
                    break
                ev_us = self._process_l2(product, rec, idx)
                if self.last_l2_ts[product] == 0:
                    self.last_l2_ts[product] = ev_us
                if next_snap == 0:
                    next_snap = ev_us + interval_us
                if ev_us >= next_snap and self._emit_if_valid(product, writer, ev_us):
                    next_snap += interval_us
        except Exception as e:
            log.error("product loop error %s: %s", product, e, exc_info=True)

    # processing -------------------------------------------------
    def _process_l2(self, product, rec, idx):
        rec_type = rec[idx["type"]]
        side = rec[idx["side"]]
        ev_us = rec[idx["ev_us"]]
        price = rec[idx["price"]]
        qty = rec[idx["qty"]]

        if rec_type in (b's', b'S'):
            self.bids[product].clear()
            self.asks[product].clear()

        book = self.bids[product] if side in (b'b', b'B') else self.asks[product]
        if qty == 0.0:
            book.pop(price, None)
        else:
            book[price] = qty
        self.last_l2_ts[product] = ev_us
        return ev_us

    def _emit_if_valid(self, product, writer, snap_time_us):
        bids = self.bids[product]
        asks = self.asks[product]
        if not bids or not asks:
            return False

        depth = self.depth
        vals = [b'S', snap_time_us]
        # bids descending
        bcount = 0
        for p, q in bids.items():
            vals.extend([p, q]); bcount += 1
            if bcount >= depth: break
        for _ in range(depth - bcount):
            vals.extend([0.0, 0.0])
        # asks ascending
        acount = 0
        for p, q in asks.items():
            vals.extend([p, q]); acount += 1
            if acount >= depth: break
        for _ in range(depth - acount):
            vals.extend([0.0, 0.0])
        try:
            writer.write_values(*vals)
        except Exception as e:
            log.error("write_values error %s: %s", product, e, exc_info=True)
            return False
        return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--products", required=True, help="Comma-separated, e.g., BTC-USD,XRP-USD")
    ap.add_argument("--base-path", required=True)
    ap.add_argument("--depth", type=int, required=True)
    ap.add_argument("--period", type=int, required=True, help="Snapshot period ms")
    args = ap.parse_args()

    daemon = OrderBookDaemon(
        products=[p.strip() for p in args.products.split(",")],
        base_path=args.base_path,
        depth=args.depth,
        period_ms=args.period,
    )
    daemon.start()


if __name__ == "__main__":
    main()
