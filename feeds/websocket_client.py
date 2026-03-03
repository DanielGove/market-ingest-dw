"""Coinbase websocket engine compatibility facade.

Venue parsing/subscription logic is connector-owned in `connectors.coinbase_spot`
and shared lifecycle is in `runtime.ingest.ws_engine`.
"""

from __future__ import annotations

import logging

from runtime.ingest.market_engine import ConfiguredMarketDataEngine, venue_l2_spec, venue_trades_spec
from runtime.venues import get_venue_defaults

log = logging.getLogger("dw.ws")
if not log.handlers:
    log.addHandler(logging.NullHandler())

VENUE = get_venue_defaults("coinbase")
trades_spec = venue_trades_spec("coinbase")
l2_spec = venue_l2_spec("coinbase")


class MarketDataEngine(ConfiguredMarketDataEngine):
    """Backward-compatible Coinbase market data engine."""

    def __init__(
        self,
        uri: str = VENUE.websocket_uri,
        sample_size: int = 16,
    ) -> None:
        super().__init__(
            venue_key="coinbase",
            logger=log,
            uri=uri,
            sample_size=sample_size,
        )


__all__ = ["MarketDataEngine", "trades_spec", "l2_spec"]
