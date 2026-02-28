from backtest.strategy import EventDrivenStrategy, OrderIntent


class PingPong(EventDrivenStrategy):
    """
    Simple symmetric maker: place one buy below mid and one sell above mid each snapshot.
    Defaults to post-only limits.
    """

    def __init__(self, offset_bp: float = 5.0, size: float = 1.0):
        super().__init__(
            product="BTC-USD",
            include_trades=True,
            include_status=True,
        )
        self.offset = offset_bp / 10_000.0
        self.size = size
        self._bid_idx = 3
        self._ask_idx = 403

    def on_snapshot(self, snapshot):
        if len(snapshot) <= self._ask_idx:
            return []
        bid = float(snapshot[self._bid_idx])
        ask = float(snapshot[self._ask_idx])
        mid = (bid + ask) * 0.5
        buy_px = mid - self.offset
        sell_px = mid + self.offset
        return [
            OrderIntent(side="buy", price=buy_px, size=self.size, tif="PO", order_type="limit"),
            OrderIntent(side="sell", price=sell_px, size=self.size, tif="PO", order_type="limit"),
        ]

    def on_trade(self, trade):
        return []

    def on_status(self, record):
        return []
