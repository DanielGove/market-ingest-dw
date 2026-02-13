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
import socket
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
    def __init__(self, products, base_path, depth, period_ms, control_sock: str | None = None):
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
        self._lock = threading.RLock()
        self.control_sock = control_sock or "pids/orderbook.sock"
        self._ctl_thread: threading.Thread | None = None

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # lifecycle -------------------------------------------------
    def start(self):
        log.info("Starting OB daemon products=%s depth=%d period_ms=%d", self.products, self.depth, self.period_ms)
        self.platform = Platform(base_path=self.base_path)
        self.running = True

        self._start_control()

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
        self._stop_control()
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
    def _create_product(self, product, depth=None, period_ms=None):
        with self._lock:
            if product in self.readers:
                return
        depth = depth or self.depth
        period_ms = period_ms or self.period_ms
        feed_name = f"OB{depth}{period_ms}-{product}"
        fields = [
            {"name": "snapshot_time", "type": "uint64"},
            {"name": "processed_time", "type": "uint64"},
            {"name": "type", "type": "char"},
            {"name": "_", "type": "_7"}]
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
            "clock_level": 2,
            "chunk_size_bytes": 256 * 1024 * 1024,
            "persist": True,
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
            # Playback True to rebuild book fully before going live
            for rec in reader.stream(playback=True):
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
        ev_us = rec[idx["event_time"]]
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
        vals = []
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
            writer.write_values(snap_time_us, time.time_ns(), b'S', *vals)
        except Exception as e:
            log.error("write_values error %s: %s", product, e, exc_info=True)
            return False
        return True

    # ------------ control socket -------------
    def _start_control(self):
        sock_path = Path(self.control_sock)
        sock_path.parent.mkdir(parents=True, exist_ok=True)
        if sock_path.exists():
            try:
                sock_path.unlink()
            except Exception:
                pass

        def loop():
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(str(sock_path))
            srv.listen(1)
            srv.settimeout(1.0)
            log.info("Orderbook control socket at %s", sock_path)
            while self.running:
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        log.warning("OB control accept error: %s", e)
                    break
                with conn:
                    try:
                        data = conn.recv(256).decode("ascii", "ignore").strip()
                        if not data:
                            continue
                        parts = data.split()
                        cmd = parts[0].upper()
                        if cmd == "ADD" and len(parts) == 4:
                            product = parts[1].upper()
                            depth = int(parts[2]); period = int(parts[3])
                            self._add_product(product, depth, period)
                            conn.sendall(b"OK\n")
                        elif cmd == "REMOVE" and len(parts) == 2:
                            self._remove_product(parts[1].upper())
                            conn.sendall(b"OK\n")
                        elif cmd == "LIST":
                            with self._lock:
                                prods = ",".join(self.products)
                            conn.sendall(f"{prods}\n".encode())
                        else:
                            conn.sendall(b"ERR unknown\n")
                    except Exception as e:
                        log.warning("OB control cmd error: %s", e, exc_info=True)
            try:
                srv.close()
            except Exception:
                pass
            try:
                sock_path.unlink()
            except Exception:
                pass

        self._ctl_thread = threading.Thread(target=loop, name="ob-ctl", daemon=True)
        self._ctl_thread.start()

    def _stop_control(self):
        # closing socket happens in loop exit
        if self._ctl_thread and self._ctl_thread.is_alive():
            self._ctl_thread.join(timeout=1.0)

    def _add_product(self, product: str, depth: int, period: int):
        with self._lock:
            if product in self.products:
                return
            self.products.append(product)
        self._create_product(product, depth, period)
        stop_evt = threading.Event()
        self._stops[product] = stop_evt
        t = threading.Thread(target=self._product_loop, args=(product, stop_evt), name=f"ob-{product}", daemon=True)
        self._threads[product] = t
        t.start()
        log.info("Added product %s depth=%d period=%d", product, depth, period)

    def _remove_product(self, product: str):
        with self._lock:
            if product not in self.products:
                return
            self.products.remove(product)
        stop_evt = self._stops.pop(product, None)
        if stop_evt:
            stop_evt.set()
        t = self._threads.pop(product, None)
        if t and t.is_alive():
            t.join(timeout=1.0)
        try:
            self.readers.pop(product, None)
            self.writers.pop(product, None)
        except Exception:
            pass
        self.bids.pop(product, None)
        self.asks.pop(product, None)
        self.last_l2_ts.pop(product, None)
        log.info("Removed product %s", product)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--products", required=True, help="Comma-separated, e.g., BTC-USD,XRP-USD")
    ap.add_argument("--base-path", required=True)
    ap.add_argument("--depth", type=int, required=True)
    ap.add_argument("--period", type=int, required=True, help="Snapshot period ms")
    ap.add_argument("--control-sock", type=str, default="pids/orderbook.sock")
    args = ap.parse_args()

    daemon = OrderBookDaemon(
        products=[p.strip() for p in args.products.split(",")],
        base_path=args.base_path,
        depth=args.depth,
        period_ms=args.period,
        control_sock=args.control_sock,
    )
    daemon.start()


if __name__ == "__main__":
    main()
