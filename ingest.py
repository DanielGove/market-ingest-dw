"""
Headless Coinbase ingest runner.

Usage:
  python -m app.ingest --base-path data/coinbase-test --products BTC-USD ETH-USD
"""
import argparse
import logging
import signal
from typing import List

from deepwater.platform import Platform
from websocket import create_connection, WebSocketConnectionClosedException
import time
import threading
import orjson
from simdjson import Parser as _JSONParser
from fastnumbers import fast_float
from deepwater.utils.timestamps import parse_us_timestamp

# Reuse the feed specs from the original test client
def trades_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name":"type",       "type":"char"},
            {"name":"side",       "type":"char"},
            {"name":"_",          "type":"_6"},
            {"name":"trade_id",   "type":"uint64"},
            {"name":"packet_us",  "type":"uint64"},
            {"name":"recv_us",    "type":"uint64"},
            {"name":"proc_us",    "type":"uint64"},
            {"name":"ev_us",      "type":"uint64"},
            {"name":"price",      "type":"float64"},
            {"name":"size",       "type":"float64"},
        ],
        "ts_col": "proc_us",
        "chunk_size_bytes": int(0.0625 * 1024 * 1024),
        "persist": True,
    }

def l2_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name":"type",       "type":"char"},
            {"name":"side",       "type":"char"},
            {"name":"_",          "type":"_6"},
            {"name":"packet_us",  "type":"uint64"},
            {"name":"recv_us",    "type":"uint64"},
            {"name":"proc_us",    "type":"uint64"},
            {"name":"ev_us",      "type":"uint64"},
            {"name":"price",      "type":"float64"},
            {"name":"qty",        "type":"float64"},
            {"name":"_",          "type":"_8"},
        ],
        "ts_col": "proc_us",
        "chunk_size_bytes": int(0.0625 * 1024 * 1024),
        "persist": True,
        "index_playback": True,
    }



def parse_args():
    ap = argparse.ArgumentParser(description="Coinbase ingest -> Deepwater feeds (headless).")
    ap.add_argument("--base-path", default="/data/coinbase-test", help="Deepwater base path")
    ap.add_argument("--products", nargs="+", required=True, help="Product ids e.g. BTC-USD ETH-USD")
    ap.add_argument("--log-level", default="INFO", help="Logging level")
    return ap.parse_args()


class IngestEngine:
    def __init__(self, base_path: str, products: List[str]):
        self.base_path = base_path
        self.products = [p.upper() for p in products]
        self.ws_url = "wss://advanced-trade-ws.coinbase.com"
        self.parser = _JSONParser()
        self.platform = Platform(base_path=self.base_path)
        self._ws = None
        self._should_run = False
        self._thread = None
        self.log = logging.getLogger("dw.ingest")
        self.trade_writers = {}
        self.book_writers = {}

    def start(self):
        if self._should_run:
            return
        self._should_run = True
        # ensure feeds
        for pid in self.products:
            self.platform.create_feed(trades_spec(pid))
            self.platform.create_feed(l2_spec(pid))
            self.trade_writers[pid] = self.platform.create_writer(f"CB-TRADES-{pid}")
            self.book_writers[pid] = self.platform.create_writer(f"CB-L2-{pid}")
        self._thread = threading.Thread(target=self._loop, name="cb-ws", daemon=True)
        self._thread.start()
        self.log.info("Ingest started for %s", ", ".join(self.products))

    def stop(self):
        self._should_run = False
        if self._ws:
            try: self._ws.close()
            except Exception: pass
            self._ws = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self.platform.close()

    def _connect(self):
        self._ws = create_connection(self.ws_url, timeout=5)
        subs = [
            {"type":"subscribe","channel":"market_trades","product_ids":self.products},
            {"type":"subscribe","channel":"level2","product_ids":self.products},
            {"type":"subscribe","channel":"heartbeats"},
        ]
        for s in subs:
            self._ws.send(orjson.dumps(s))
        self.log.info("Subscribed to %s", ", ".join(self.products))

    def _loop(self):
        while self._should_run:
            try:
                if self._ws is None:
                    self._connect()
                raw = self._ws.recv()
                try:
                    doc = self.parser.parse(raw).as_dict()
                except ValueError:
                    # Ignore malformed/empty frames (e.g., during shutdown)
                    continue
                if doc.get("channel") == "market_trades":
                    for tr in doc.get("events", []):
                        pid = tr.get("product_id")
                        w = self.trade_writers.get(pid)
                        if w is None:
                            continue
                        packet_us = self._parse_ts(tr.get("time"))
                        recv_us = self._parse_ts(doc.get("timestamp"))
                        proc_us = self._now_us()
                        w.write_values(
                            b'T', tr["taker_side"][0].encode("ascii"),
                            tr.get("trade_id", 0),
                            packet_us, recv_us, proc_us,
                            packet_us,  # ev_us
                            fast_float(tr["price"]),
                            fast_float(tr["size"])
                        )
                elif doc.get("channel") == "level2":
                    packet_us = self._parse_ts(doc.get("timestamp"))
                    recv_us = packet_us
                    for ev in doc.get("events", []):
                        pid = ev.get("product_id")
                        w = self.book_writers.get(pid)
                        if w is None:
                            continue
                        l2_type = ev["type"][0].encode("ascii")
                        idx = True if l2_type == b's' else False
                        proc_us = self._now_us()
                        for u in ev.get("updates", []):
                            w.write_values(
                                l2_type,
                                u["side"][0].encode("ascii"),
                                packet_us, recv_us, proc_us,
                                self._parse_ts(u.get("event_time")),
                                fast_float(u["price_level"]),
                                fast_float(u["new_quantity"]),
                                create_index=idx
                            )
                            idx = False
                elif doc.get("channel") == "heartbeats":
                    pass
            except WebSocketConnectionClosedException:
                self.log.warning("WS closed; reconnecting")
                self._ws = None
                time.sleep(1.0)
            except Exception as e:
                self.log.error("WS error: %s", e, exc_info=True)
                self._ws = None
                time.sleep(1.0)

    @staticmethod
    def _now_us() -> int:
        return time.time_ns() // 1_000

    @staticmethod
    def _parse_ts(val) -> int:
        if val is None:
            return 0
        try:
            b = val.encode("ascii") if isinstance(val, str) else bytes(val)
            return parse_us_timestamp(b)
        except Exception:
            return 0

    # control methods removed for headless static product set


def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")
    engine = IngestEngine(base_path=args.base_path, products=args.products)

    def _stop(*_):
        engine.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    engine.start()
    signal.pause()


if __name__ == "__main__":
    main()
