"""
Orderbook snapshot builder: reads L2 feeds with playback, maintains a local book,
and emits fixed-depth snapshots into ring feeds.

Usage:
  python -m app.snapshots --base-path data/coinbase-test --products BTC-USD ETH-USD --depth 500 --interval 1.0
"""
import argparse
import logging
import signal
import threading
import time
from typing import Dict, List

from deepwater.platform import Platform
from deepwater.utils.timestamps import us_to_iso
def snapshot_spec(pid: str, depth: int, chunk_size_bytes: int = 1_000_000) -> dict:
    fields = [
        {"name": "type", "type": "char"},
        {"name": "_", "type": "_7"},
        {"name": "snapshot_us", "type": "uint64"},
    ]
    for i in range(depth):
        fields.append({"name": f"bid_px_{i:03d}", "type": "float64"})
        fields.append({"name": f"bid_sz_{i:03d}", "type": "float64"})
    for i in range(depth):
        fields.append({"name": f"ask_px_{i:03d}", "type": "float64"})
        fields.append({"name": f"ask_sz_{i:03d}", "type": "float64"})
    return {
        "feed_name": f"CB-L2SNAP-{pid}",
        "mode": "UF",
        "fields": fields,
        "ts_col": "snapshot_us",
        "chunk_size_bytes": chunk_size_bytes,
        "persist": False,
        "index_playback": False,
    }


def parse_args():
    ap = argparse.ArgumentParser(description="Build orderbook snapshots from L2 feeds.")
    ap.add_argument("--base-path", default="/data/coinbase-test", help="Deepwater base path")
    ap.add_argument("--products", nargs="+", required=True, help="Product ids e.g. BTC-USD ETH-USD")
    ap.add_argument("--depth", type=int, default=500, help="Levels per side in snapshot")
    ap.add_argument("--interval", type=float, default=1.0, help="Snapshot interval seconds")
    ap.add_argument("--log-level", default="INFO", help="Logging level")
    return ap.parse_args()


class SnapshotWorker:
    def __init__(self, platform: Platform, product_id: str, depth: int, interval: float, log: logging.Logger):
        self.platform = platform
        self.pid = product_id
        self.depth = depth
        self.interval = interval
        self.log = log
        self.reader = platform.create_reader(f"CB-L2-{product_id}")
        snap_spec = snapshot_spec(product_id, depth)
        platform.create_feed(snap_spec)
        self.writer = platform.create_writer(snap_spec["feed_name"])
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name=f"snap-{product_id}", daemon=True)
        self.fields = [f["name"] for f in self.reader.record_format["fields"] if f.get("name") != "_"]

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=1.0)
        try:
            self.writer.close()
        except Exception:
            pass
        try:
            self.reader.close()
        except Exception:
            pass

    def _loop(self):
        next_emit = time.time() + self.interval
        for rec in self.reader.stream_latest_records(playback=True):
            if self._stop.is_set():
                break
            rec_map = dict(zip(self.fields, rec))
            side = rec_map.get("side")
            price = rec_map.get("price")
            qty = rec_map.get("qty") or rec_map.get("size")
            if side is None or price is None or qty is None:
                continue
            side_b = side.encode("ascii") if isinstance(side, str) else bytes(side[:1]).upper()
            book = self.bids if side_b == b"B" else self.asks
            if qty <= 0:
                book.pop(float(price), None)
            else:
                book[float(price)] = float(qty)

            now = time.time()
            if now >= next_emit:
                vals, snap_ts = self._snapshot()
                self.writer.write_values(*vals)
                self.log.info("%s snapshot ts=%s bids=%s asks=%s",
                              self.pid, us_to_iso(snap_ts),
                              min(len(self.bids), self.depth), min(len(self.asks), self.depth))
                next_emit = now + self.interval

    def _snapshot(self):
        snap_ts = int(time.time() * 1_000_000)
        vals = [b"S", snap_ts]
        top_bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:self.depth]
        for px, sz in top_bids:
            vals.append(px); vals.append(sz)
        vals.extend([0.0, 0.0] * (self.depth - len(top_bids)))
        top_asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:self.depth]
        for px, sz in top_asks:
            vals.append(px); vals.append(sz)
        vals.extend([0.0, 0.0] * (self.depth - len(top_asks)))
        return vals, snap_ts


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("dw.snapshots")
    platform = Platform(base_path=args.base_path)
    workers: List[SnapshotWorker] = []

    for pid in args.products:
        pid = pid.upper()
        workers.append(SnapshotWorker(platform, pid, args.depth, args.interval, log))

    for w in workers:
        w.start()

    stop_evt = threading.Event()

    def _stop(*_):
        stop_evt.set()
        for w in list(workers):
            w.stop()
        platform.close()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    # wait until stopped
    try:
        while not stop_evt.is_set():
            time.sleep(0.5)
    finally:
        _stop()


if __name__ == "__main__":
    main()
