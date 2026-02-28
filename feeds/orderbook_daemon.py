#!/usr/bin/env python3
"""
Simplified orderbook daemon.
 - One thread per product (no round-robin starvation)
 - Warm-starts from recent L2 rows, then follows live stream
 - Emits snapshots every period_ms when book is valid
"""
import time
import signal
import logging
import argparse
import threading
import socket
import struct
from bisect import bisect_left
from pathlib import Path

from deepwater.platform import Platform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("orderbook")


class OrderBookDaemon:
    _HEADER_STRUCT = struct.Struct("<QQc7x")

    def __init__(
        self,
        products,
        base_path,
        depth,
        period_ms,
        control_sock: str | None = None,
        warmup_seconds: int = 60,
        feed_prefix: str = "OB",
        max_levels: int = 8192,
    ):
        self.products = [p.upper() for p in products]
        self.base_path = base_path
        self.depth = depth
        self.period_ms = period_ms
        self.warmup_seconds = int(max(1, warmup_seconds))
        self.feed_prefix = feed_prefix
        self.max_levels = int(max(self.depth, max_levels))
        self.running = False

        self.platform = None
        self.readers = {}
        self.writers = {}
        self.field_idx = {}
        self.bid_keys = {}
        self.bid_qtys = {}
        self.ask_keys = {}
        self.ask_qtys = {}
        self.book_locks = {}
        self.last_l2_ts = {}
        self.snap_payloads = {}
        self.snap_float_views = {}
        self.snap_bid_views = {}
        self.snap_ask_views = {}

        self._threads = {}
        self._stops = {}
        self._lock = threading.RLock()
        self.control_sock = control_sock or "pids/orderbook.sock"
        self._ctl_thread: threading.Thread | None = None

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # lifecycle -------------------------------------------------
    def start(self):
        log.info(
            "Starting OB daemon products=%s depth=%d period_ms=%d prefix=%s max_levels=%d",
            self.products,
            self.depth,
            self.period_ms,
            self.feed_prefix,
            self.max_levels,
        )
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
        feed_name = f"{self.feed_prefix}{depth}{period_ms}-{product}"
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
        rec_fmt = self.writers[product].record_format["fmt"]
        rec_size = struct.calcsize(rec_fmt)
        payload = bytearray(rec_size)
        float_view = memoryview(payload)[self._HEADER_STRUCT.size:].cast("d")
        expected_floats = self.depth * 4
        if len(float_view) != expected_floats:
            raise RuntimeError(
                f"OB record layout mismatch for {feed_name}: floats={len(float_view)} expected={expected_floats}"
            )
        self.snap_payloads[product] = payload
        self.snap_float_views[product] = float_view
        side_width = self.depth * 2
        self.snap_bid_views[product] = float_view[:side_width]
        self.snap_ask_views[product] = float_view[side_width:]

        l2_name = f"CB-L2-{product}"
        self.readers[product] = self.platform.create_reader(l2_name)
        self.field_idx[product] = {n: i for i, n in enumerate(self.readers[product].field_names)}
        self.last_l2_ts[product] = 0
        self.bid_keys[product] = []
        self.bid_qtys[product] = []
        self.ask_keys[product] = []
        self.ask_qtys[product] = []
        self.book_locks[product] = threading.Lock()
        log.info("Created product %s feeds (%s)", product, feed_name)

    def _product_loop(self, product, stop_evt):
        reader = self.readers[product]
        writer = self.writers[product]
        idx = self.field_idx[product]
        emit_t = threading.Thread(
            target=self._emit_loop,
            args=(product, writer, stop_evt),
            name=f"ob-emit-{product}",
            daemon=True,
        )
        emit_t.start()

        try:
            # Fast warm-start from recent rows: process last snapshot and its deltas,
            # then switch to live stream without replaying the full feed history.
            warm = reader.latest(seconds=self.warmup_seconds)
            if warm:
                start_i = 0
                for i in range(len(warm) - 1, -1, -1):
                    rtype = warm[i][idx["type"]]
                    if rtype in (b's', b'S'):
                        start_i = i
                        break
                for rec in warm[start_i:]:
                    ev_us = self._process_l2(product, rec, idx)
                    if self.last_l2_ts[product] == 0:
                        self.last_l2_ts[product] = ev_us

            for rec in reader.stream(playback=False):
                if stop_evt.is_set():
                    break
                ev_us = self._process_l2(product, rec, idx)
                if self.last_l2_ts[product] == 0:
                    self.last_l2_ts[product] = ev_us
        except Exception as e:
            log.error("product loop error %s: %s", product, e, exc_info=True)
        finally:
            stop_evt.set()
            if emit_t.is_alive():
                emit_t.join(timeout=1.0)

    def _emit_loop(self, product, writer, stop_evt):
        interval_ns = self.period_ms * 1_000_000
        next_emit_ns = time.monotonic_ns() + interval_ns
        while not stop_evt.is_set():
            now_ns = time.monotonic_ns()
            wait_ns = next_emit_ns - now_ns
            if wait_ns > 0:
                if stop_evt.wait(wait_ns / 1_000_000_000):
                    break
                continue
            self._emit_if_valid(product, writer)
            next_emit_ns += interval_ns
            if next_emit_ns < now_ns - interval_ns:
                next_emit_ns = now_ns + interval_ns

    # processing -------------------------------------------------
    def _clear_book(self, product):
        self.bid_keys[product].clear()
        self.bid_qtys[product].clear()
        self.ask_keys[product].clear()
        self.ask_qtys[product].clear()

    def _upsert_side(self, keys, qtys, key, qty):
        idx = bisect_left(keys, key)
        found = idx < len(keys) and keys[idx] == key
        if qty == 0.0:
            if found:
                del keys[idx]
                del qtys[idx]
            return
        if found:
            qtys[idx] = qty
            return
        if len(keys) < self.max_levels:
            keys.insert(idx, key)
            qtys.insert(idx, qty)
            return
        # Keep only top max_levels levels by dropping deeper levels.
        if idx >= self.max_levels:
            return
        keys.insert(idx, key)
        qtys.insert(idx, qty)
        del keys[self.max_levels]
        del qtys[self.max_levels]

    def _process_l2(self, product, rec, idx):
        rec_type = rec[idx["type"]]
        side = rec[idx["side"]]
        ev_us = rec[idx["event_time"]]
        price = rec[idx["price"]]
        qty = rec[idx["qty"]]

        with self.book_locks[product]:
            if rec_type in (b's', b'S'):
                self._clear_book(product)

            if side in (b'b', b'B'):
                # Bids sorted by descending price via ascending negative key.
                self._upsert_side(self.bid_keys[product], self.bid_qtys[product], -price, qty)
            else:
                # Asks sorted ascending by price.
                self._upsert_side(self.ask_keys[product], self.ask_qtys[product], price, qty)
            self.last_l2_ts[product] = ev_us
        return ev_us

    def _emit_if_valid(self, product, writer):
        # Copy only the top depth levels under lock so writes do not block L2 updates.
        with self.book_locks[product]:
            source_event_us = self.last_l2_ts.get(product, 0)
            if source_event_us <= 0:
                return False
            bid_keys = self.bid_keys[product][: self.depth]
            bid_qtys = self.bid_qtys[product][: self.depth]
            ask_keys = self.ask_keys[product][: self.depth]
            ask_qtys = self.ask_qtys[product][: self.depth]

        if not bid_keys or not ask_keys:
            return False

        depth = self.depth
        bid_vals = self.snap_bid_views[product]
        ask_vals = self.snap_ask_views[product]
        # bids descending (stored as ascending negative prices)
        i = 0
        for key, q in zip(bid_keys, bid_qtys):
            bid_vals[i] = -key
            bid_vals[i + 1] = q
            i += 2
            if i >= depth * 2:
                break
        while i < (depth * 2):
            bid_vals[i] = 0.0
            bid_vals[i + 1] = 0.0
            i += 2
        # asks ascending
        i = 0
        for p, q in zip(ask_keys, ask_qtys):
            ask_vals[i] = p
            ask_vals[i + 1] = q
            i += 2
            if i >= depth * 2:
                break
        while i < (depth * 2):
            ask_vals[i] = 0.0
            ask_vals[i + 1] = 0.0
            i += 2
        payload = self.snap_payloads[product]
        try:
            self._HEADER_STRUCT.pack_into(payload, 0, source_event_us, time.time_ns(), b'S')
            writer.write(source_event_us, payload)
        except Exception as e:
            log.error("write error %s: %s", product, e, exc_info=True)
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
        self.bid_keys.pop(product, None)
        self.bid_qtys.pop(product, None)
        self.ask_keys.pop(product, None)
        self.ask_qtys.pop(product, None)
        self.book_locks.pop(product, None)
        self.last_l2_ts.pop(product, None)
        self.snap_payloads.pop(product, None)
        self.snap_bid_views.pop(product, None)
        self.snap_ask_views.pop(product, None)
        view = self.snap_float_views.pop(product, None)
        if view is not None:
            try:
                view.release()
            except Exception:
                pass
        log.info("Removed product %s", product)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--products", required=True, help="Comma-separated, e.g., BTC-USD,XRP-USD")
    ap.add_argument("--base-path", required=True)
    ap.add_argument("--depth", type=int, required=True)
    ap.add_argument("--period", type=int, required=True, help="Snapshot period ms")
    ap.add_argument("--control-sock", type=str, default="pids/orderbook.sock")
    ap.add_argument("--warmup-seconds", type=int, default=60, help="Warm-start window for L2 bootstrap")
    ap.add_argument("--feed-prefix", type=str, default="OB", help="Feed name prefix (default: OB)")
    ap.add_argument("--max-levels", type=int, default=8192, help="Max price levels per side to retain in memory")
    args = ap.parse_args()

    daemon = OrderBookDaemon(
        products=[p.strip() for p in args.products.split(",")],
        base_path=args.base_path,
        depth=args.depth,
        period_ms=args.period,
        control_sock=args.control_sock,
        warmup_seconds=args.warmup_seconds,
        feed_prefix=args.feed_prefix,
        max_levels=args.max_levels,
    )
    daemon.start()


if __name__ == "__main__":
    main()
