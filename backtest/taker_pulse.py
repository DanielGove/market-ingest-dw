from backtest.interfaces import OrderIntent


class TakerPulse:
    """Validation strategy: submit one IOC market order each snapshot.

    This is not a production strategy. It exists to prove the closed-loop
    runtime emits execution fills when orders are intentionally taker.
    """

    def __init__(self, size: float = 0.001):
        self.size = size
        self._n = 0

    def on_snapshot(self, snapshot, meta):
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
