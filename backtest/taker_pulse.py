from backtest.feeds import execution_feed_name
from backtest.strategy import OrderIntent, OutputFeed, Subscription


class TakerPulse:
    """Validation strategy: submit one IOC market order each snapshot.

    This is not a production strategy. It exists to prove the closed-loop
    runtime emits execution fills when orders are intentionally taker.
    """

    def __init__(self, size: float = 0.001):
        self.size = size
        self._n = 0
        self.product = "BTC-USD"
        self.depth = 200
        self.period = 50
        self.market_base_path = "data/coinbase-main"
        self.io_base_path = "data/strategy-intents"

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
        side = "buy" if (self._n % 2 == 0) else "sell"
        self._n += 1
        return [
            OrderIntent(
                side=side,
                price=0.0,          # market orders ignore limit price in sim
                size=self.size,
                tif="IOC",
                order_type="market",
                client_tag="TAKER",
            )
        ]

    def on_trade(self, trade):
        return []

    def on_status(self, record):
        return []
