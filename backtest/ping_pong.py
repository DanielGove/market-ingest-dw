from backtest.interfaces import OrderIntent
from backtest.interfaces import Strategy


class PingPong(Strategy):
    """
    Simple symmetric maker: place one buy below mid and one sell above mid each snapshot.
    Defaults to post-only limits.
    """

    def __init__(self, offset_bp: float = 5.0, size: float = 1.0):
        self.offset = offset_bp / 10_000.0
        self.size = size

    def on_snapshot(self, snapshot, meta):
        mid = meta.get("mid")
        if mid is None:
            return []
        buy_px = mid - self.offset
        sell_px = mid + self.offset
        return [
            OrderIntent(side="buy", price=buy_px, size=self.size, tif="PO", order_type="limit"),
            OrderIntent(side="sell", price=sell_px, size=self.size, tif="PO", order_type="limit"),
        ]
