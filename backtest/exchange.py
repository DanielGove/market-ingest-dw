"""Incremental exchange simulation runtime."""
import heapq
import itertools
from typing import Any, Dict, List, Optional, Tuple

from deepwater.platform import Platform

from backtest.feeds import get_execution_writer_named


class _OrderStateTracker:
    def __init__(self):
        self.position = 0.0
        self.pnl = 0.0

    def apply_fill(self, side: str, size: float, price: float, fee: float) -> None:
        sign = 1.0 if side == "buy" else -1.0
        self.position += sign * size
        self.pnl -= sign * size * price
        self.pnl -= fee


def _fixed_bytes(value: str, size: int) -> bytes:
    return value.encode("utf-8").ljust(size, b"\0")


def _decode_token(value: Any) -> str:
    if isinstance(value, bytes):
        return value.split(b"\0", 1)[0].decode("utf-8", errors="ignore").strip()
    return str(value).strip()


def normalize_trade_side(raw: Any) -> str:
    tok = _decode_token(raw).upper()
    if tok in ("B", "BUY"):
        return "buy"
    if tok in ("S", "SELL"):
        return "sell"
    return tok.lower()


class ExchangeSimulator:
    def __init__(
        self,
        *,
        execution_base_path: str,
        execution_feed: str,
        strategy_key: str,
        product_id: str,
        run_id: str,
        latency_ms: float,
        reverse_latency_ms: float,
        taker_fee_bps: float,
        maker_fee_bps: float,
    ):
        self.product_id = product_id.upper()
        self.strategy_id = strategy_key.upper()[:16]
        self.run_id_token = str(run_id).upper()[:16]
        self.forward_latency_us = int(latency_ms * 1_000.0)
        self.reverse_latency_us = int(reverse_latency_ms * 1_000.0)
        self.taker_fee_rate = float(taker_fee_bps) / 10_000.0
        self.maker_fee_rate = float(maker_fee_bps) / 10_000.0

        plat = Platform(base_path=execution_base_path)
        self.execution_writer = get_execution_writer_named(plat, execution_feed)
        self.execution_feed_name = execution_feed
        self.execution_base_path = execution_base_path

        self.tracker = _OrderStateTracker()
        self.status_counts: Dict[str, int] = {}
        self.execution_records = 0
        self.execution_min_proc_us: Optional[int] = None
        self.execution_max_proc_us: Optional[int] = None

        self.order_seq = 0
        self.pending_seq = itertools.count(1)
        self.pending_intents: List[Tuple[int, int, Dict[str, Any]]] = []  # (arrival_us, seq, intent)
        self.working_orders: Dict[str, Dict[str, Any]] = {}

    def close(self) -> None:
        self.execution_writer.close()

    def summary(self) -> Dict[str, Any]:
        return {
            "position": self.tracker.position,
            "pnl": self.tracker.pnl,
            "status_counts": self.status_counts,
            "execution_feed": {"records": self.execution_records},
            "execution_window": {
                "start_processed_us": self.execution_min_proc_us,
                "end_processed_us": self.execution_max_proc_us,
            },
        }

    def submit_intent(
        self,
        *,
        event_us: int,
        processed_us: int,
        product_id: str,
        side: str,
        order_type: str,
        price: float,
        size: float,
        tif: str,
        client_tag: str,
    ) -> None:
        if product_id.upper() != self.product_id:
            return
        if size <= 0 or order_type not in ("L", "M") or side not in ("B", "S"):
            return
        intent = {
            "arrival_us": int(processed_us + self.forward_latency_us),
            "event_us": int(event_us),
            "processed_us": int(processed_us),
            "side": side,
            "type": order_type,
            "price": float(price),
            "size": float(size),
            "tif": tif,
            "client_tag": client_tag,
        }
        heapq.heappush(self.pending_intents, (intent["arrival_us"], next(self.pending_seq), intent))

    def _activate_intents(self, event_us: int) -> None:
        while self.pending_intents and self.pending_intents[0][0] <= event_us:
            _, _, intent = heapq.heappop(self.pending_intents)
            self.order_seq += 1
            client_id = intent["client_tag"] or f"C{self.order_seq:07d}"
            self.working_orders[client_id] = {
                "seq": self.order_seq,
                "client_id": client_id,
                "side": intent["side"],
                "type": intent["type"],
                "price": intent["price"],
                "size": intent["size"],
                "remaining": intent["size"],
            }

    def _emit_execution(
        self,
        *,
        order: Dict[str, Any],
        fill_size: float,
        event_us: int,
        fill_price: float,
        liquidity_flag: str,
    ) -> Tuple[int, int, tuple]:
        status = "FILL" if order["remaining"] <= 1e-12 else "PARTIAL_FILL"
        self.status_counts[status] = self.status_counts.get(status, 0) + 1

        fee_rate = self.maker_fee_rate if liquidity_flag == "M" else self.taker_fee_rate
        fee = abs(fill_price * fill_size) * fee_rate
        cum_filled = order["size"] - order["remaining"]
        proc_us = int(event_us + self.reverse_latency_us)

        self.execution_records += 1
        event_id = f"E{self.execution_records:015d}"
        exchange_order_id = f"X{int(order['seq']):015d}"

        if self.execution_min_proc_us is None or proc_us < self.execution_min_proc_us:
            self.execution_min_proc_us = proc_us
        if self.execution_max_proc_us is None or proc_us > self.execution_max_proc_us:
            self.execution_max_proc_us = proc_us

        record = (
            int(event_us),
            int(proc_us),
            _fixed_bytes(event_id, 16),
            1,
            _fixed_bytes(self.product_id, 16),
            _fixed_bytes(self.strategy_id, 16),
            _fixed_bytes(self.run_id_token, 16),
            _fixed_bytes(order["client_id"], 16),
            _fixed_bytes(exchange_order_id, 16),
            _fixed_bytes(status, 16),
            order["side"].encode("utf-8"),
            float(fill_price),
            float(fill_size),
            float(cum_filled),
            float(order["remaining"]),
            float(fee),
            liquidity_flag.encode("utf-8"),
            _fixed_bytes("", 16),
        )
        self.execution_writer.write_values(*record)
        self.tracker.apply_fill(
            "buy" if order["side"] == "B" else "sell",
            float(fill_size),
            float(fill_price),
            float(fee),
        )
        return proc_us, int(event_us), record

    def process_trade(self, *, event_us: int, side: str, price: float, size: float) -> List[Tuple[int, int, tuple]]:
        """Process a single trade event; returns execution records with processed/event time."""
        if side not in ("buy", "sell") or size <= 0:
            return []

        self._activate_intents(event_us)
        if not self.working_orders:
            return []

        remaining_trade = float(size)
        out: List[Tuple[int, int, tuple]] = []
        orders = list(self.working_orders.values())

        if side == "buy":
            candidates = [
                o
                for o in orders
                if o["side"] == "S" and o["remaining"] > 0 and (o["type"] == "M" or o["price"] <= price)
            ]
            candidates.sort(key=lambda o: (0 if o["type"] == "M" else 1, o["price"], o["seq"]))
        else:
            candidates = [
                o
                for o in orders
                if o["side"] == "B" and o["remaining"] > 0 and (o["type"] == "M" or o["price"] >= price)
            ]
            candidates.sort(key=lambda o: (0 if o["type"] == "M" else 1, -o["price"], o["seq"]))

        for order in candidates:
            if remaining_trade <= 0:
                break
            fill_size = min(order["remaining"], remaining_trade)
            if fill_size <= 0:
                continue

            order["remaining"] -= fill_size
            remaining_trade -= fill_size
            fill_price = price if order["type"] == "M" else order["price"]
            liquidity_flag = "T" if order["type"] == "M" else "M"
            out.append(
                self._emit_execution(
                    order=order,
                    fill_size=fill_size,
                    event_us=event_us,
                    fill_price=fill_price,
                    liquidity_flag=liquidity_flag,
                )
            )
            if order["remaining"] <= 1e-12:
                self.working_orders.pop(order["client_id"], None)

        return out
