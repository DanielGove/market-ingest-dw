import itertools
from typing import Dict, List
from backtest.interfaces import OrderIntent


class OrderStateTracker:
    """
    Tracks working orders by client_id and basic PnL/inventory.
    """
    def __init__(self):
        self._id_gen = itertools.count(1)
        self.open_orders: Dict[str, OrderIntent] = {}
        self.position = 0.0
        self.pnl = 0.0

    def assign_ids(self, intents):
        for it in intents:
            if it.client_id is None:
                it.client_id = f"cli-{next(self._id_gen)}"
        return intents

    def replace_all(self, new_intents: List[OrderIntent]):
        """
        Simple converge: cancel all existing, replace with new.
        """
        self.open_orders = {it.client_id: it for it in new_intents}

    def apply_fill(self, status: dict):
        if status.get("type") != "fill":
            return
        side = status["side"]
        size = status["size"]
        price = status["price"]
        fee = status.get("fee", 0.0)
        sign = 1.0 if side == "buy" else -1.0
        self.position += sign * size
        self.pnl -= sign * size * price
        self.pnl -= fee
