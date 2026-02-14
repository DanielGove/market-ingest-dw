from backtest.feeds import execution_feed_name
from backtest.strategy import OrderIntent, OutputFeed, Strategy, Subscription


class PingPong(Strategy):
    """
    Simple symmetric maker: place one buy below mid and one sell above mid each snapshot.
    Defaults to post-only limits.
    """

    def __init__(self, offset_bp: float = 5.0, size: float = 1.0):
        self.offset = offset_bp / 10_000.0
        self.size = size
        self.product = "BTC-USD"
        self.depth = 200
        self.period = 50
        self.market_base_path = "data/coinbase-main"
        self.io_base_path = "data/strategy-intents"
        self._bid_idx = 3
        self._ask_idx = 403

    def subscriptions(self):
        return [
            Subscription(
                feed=f"OB{self.depth}{self.period}-{self.product}",
                base_path=self.market_base_path,
                method="on_snapshot",
            ),
            Subscription(
                feed=f"CB-TRADES-{self.product}",
                base_path=self.market_base_path,
                method="on_trade",
            ),
            Subscription(
                feed=execution_feed_name(self.__class__.__name__),
                base_path=self.io_base_path,
                method="on_status",
            ),
        ]

    def outputs(self):
        return [
            OutputFeed(
                role="intent",
                feed=f"STRAT-ORDERS-{self.__class__.__name__.upper()}",
                base_path=self.io_base_path,
                product_id=self.product,
            ),
        ]

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
