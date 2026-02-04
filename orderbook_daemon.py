#!/usr/bin/env python3
"""
Orderbook daemon: constructs OB[depth][frequency] feeds from L2 updates
Example: OB200200 = 200 levels at 5Hz (200ms intervals)
"""
import sys
import time
import signal
import logging
import argparse
from collections import defaultdict
from typing import Dict
from sortedcontainers import SortedDict

from deepwater.platform import Platform

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("orderbook")

class OrderBookDaemon:
    """Maintains orderbook from L2 feed, publishes periodic snapshots"""
    
    def __init__(self, products: list[str], base_path: str, depth: int, period_ms: int):
        """
        Args:
            products: List of trading pairs (e.g., ['XRP-USD'])
            base_path: Deepwater data directory
            depth: Number of bid/ask levels per side (e.g., 200)
            period_ms: Snapshot period in milliseconds (e.g., 50 for 20Hz)
        """
        self.products = [p.upper() for p in products]
        self.base_path = base_path
        self.depth = depth
        self.period_ms = period_ms
        self.interval_us = period_ms * 1000
        self.feed_name_prefix = f"OB{depth}{period_ms}"
        self.running = False
        
        # Orderbook state per product
        self.bids: Dict[str, SortedDict] = defaultdict(lambda: SortedDict(lambda x: -x))
        self.asks: Dict[str, SortedDict] = defaultdict(SortedDict)
        self.last_l2_timestamp: Dict[str, int] = {}
        self.next_snapshot_time: Dict[str, int] = {}
        
        self.platform = None
        self.readers = {}
        self.writers = {}
        self.field_indices = {}
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        log.info(f"Received signal {signum}")
        self.stop()
    
    def stop(self):
        self.running = False
        log.info("Stopped")
    
    def start(self):
        log.info(f"Starting orderbook daemon for {self.products}")
        log.info(f"Base path: {self.base_path}")
        frequency_hz = 1000 / self.period_ms
        log.info(f"Feed: {self.feed_name_prefix}, depth={self.depth}, period={self.period_ms}ms ({frequency_hz:.1f}Hz)")
        
        self.platform = Platform(base_path=self.base_path)
        
        # Create orderbook snapshot feeds
        for product in self.products:
            feed_name = f"{self.feed_name_prefix}-{product}"
            
            # Feed spec: type + timestamp + depth levels of (bid_price, bid_qty, ask_price, ask_qty)
            fields = [
                {"name": "type", "type": "char", "desc": "record type 'S'"},
                {"name": "_", "type": "_7", "desc": "padding"},
                {"name": "snapshot_us", "type": "uint64", "desc": "snapshot timestamp (us)"},
            ]
            
            for i in range(self.depth):
                fields.extend([
                    {"name": f"bid_price_{i}", "type": "float64", "desc": f"bid price level {i}"},
                    {"name": f"bid_qty_{i}", "type": "float64", "desc": f"bid quantity level {i}"},
                ])
            
            for i in range(self.depth):
                fields.extend([
                    {"name": f"ask_price_{i}", "type": "float64", "desc": f"ask price level {i}"},
                    {"name": f"ask_qty_{i}", "type": "float64", "desc": f"ask quantity level {i}"},
                ])
            
            feed_spec = {
                "feed_name": feed_name,
                "mode": "UF",
                "fields": fields,
                "ts_col": "snapshot_us",
                "chunk_size_bytes": 256 * 1024 * 1024,
                "persist": True,
                "index_playback": True
            }
            
            try:
                self.platform.create_feed(feed_spec)
            except RuntimeError as e:
                if "locked" in str(e) or "exists" in str(e):
                    log.info(f"Feed {feed_name} already exists, using existing")
                else:
                    raise
            
            self.writers[product] = self.platform.create_writer(feed_name)
            
            # Create L2 reader
            l2_feed_name = f"CB-L2-{product}"
            self.readers[product] = self.platform.create_reader(l2_feed_name)
            self.field_indices[product] = {name: i for i, name in enumerate(self.readers[product].field_names)}
            
            log.info(f"Created {feed_name} feed and writer for {product}")
        
        self.running = True
        log.info("Starting main loop")
        
        try:
            self._run_loop()
        except KeyboardInterrupt:
            log.info("Received interrupt")
        finally:
            self.stop()
    
    def _run_loop(self):
        """Main loop: consume L2 updates, emit snapshots at period_ms intervals"""
        interval_us = self.interval_us
        iterators = {p: self.readers[p].stream_live(playback=True) for p in self.products}
        
        for p in self.products:
            self.last_l2_timestamp[p] = 0
            self.next_snapshot_time[p] = 0
        
        emit_count = 0
        total_updates = 0
        
        while self.running:
            # Consume available L2 updates
            for product in self.products:
                while True:
                    try:
                        rec = next(iterators[product])
                        l2_timestamp_us = self._process_l2_update(product, rec)
                        total_updates += 1

                        if self.next_snapshot_time[product] == 0:
                            self.next_snapshot_time[product] = l2_timestamp_us + interval_us
                        if self.last_l2_timestamp[product] >= self.next_snapshot_time[product]:
                            break  # Time to emit snapshot
                        
                    except StopIteration:
                        break
                    except Exception as e:
                        log.error(f"Error processing L2 for {product}: {e}", exc_info=True)
                        break
            
            # Emit snapshots when accumulated enough data time
            for product in self.products:
                if self.last_l2_timestamp[product] >= self.next_snapshot_time[product] and self.next_snapshot_time[product] > 0:
                    emit_count += 1
                    snapshot_time_us = self.last_l2_timestamp[product]
                    
                    try:
                        self._emit_snapshot(product, snapshot_time_us)
                        self.next_snapshot_time[product] = self.next_snapshot_time[product] + interval_us
                        
                        if emit_count % 1000 == 0:
                            log.info(f"[{product}] #{emit_count}: {total_updates} updates processed")
                    except Exception as e:
                        log.error(f"Error emitting snapshot for {product}: {e}", exc_info=True)
    
    def _process_l2_update(self, product: str, rec):
        """Process a single L2 update, return event timestamp"""
        indices = self.field_indices[product]
        rec_type = rec[indices['type']]
        ev_us = rec[indices['ev_us']]
        price = rec[indices['price']]
        qty = rec[indices['qty']]
        side = rec[indices['side']]
        
        self.last_l2_timestamp[product] = ev_us
        
        # Update orderbook (side is b'b' for bid or b'o' for offer/ask)
        book = self.bids[product] if side == b'b' else self.asks[product]
        
        if qty == 0.0:
            book.pop(price, None)
        else:
            book[price] = qty
        
        return ev_us
    
    def _emit_snapshot(self, product: str, snapshot_time_us: int):
        """Emit orderbook snapshot with top N levels"""
        values = [b'S', snapshot_time_us]
        
        # Bid levels (descending price)
        bid_count = 0
        for price, qty in self.bids[product].items():
            values.append(price)
            values.append(qty)
            bid_count += 1
            if bid_count >= self.depth:
                break
        
        # Pad remaining bid levels
        for _ in range(self.depth - bid_count):
            values.append(0.0)
            values.append(0.0)
        
        # Ask levels (ascending price)
        ask_count = 0
        for price, qty in self.asks[product].items():
            values.append(price)
            values.append(qty)
            ask_count += 1
            if ask_count >= self.depth:
                break
        
        # Pad remaining ask levels
        for _ in range(self.depth - ask_count):
            values.append(0.0)
            values.append(0.0)
        
        self.writers[product].write_values(*values)

def main():
    parser = argparse.ArgumentParser(description='Orderbook daemon')
    parser.add_argument('--products', required=True, help='Comma-separated products (e.g., XRP-USD,BTC-USD)')
    parser.add_argument('--base-path', required=True, help='Deepwater data directory')
    parser.add_argument('--depth', type=int, required=True, help='Number of levels per side (e.g., 200)')
    parser.add_argument('--period', type=int, required=True, help='Snapshot period in milliseconds (e.g., 50 for 20Hz)')
    
    args = parser.parse_args()
    products = [p.strip() for p in args.products.split(',')]
    
    daemon = OrderBookDaemon(
        products=products,
        base_path=args.base_path,
        depth=args.depth,
        period_ms=args.period
    )
    daemon.start()

if __name__ == "__main__":
    main()
