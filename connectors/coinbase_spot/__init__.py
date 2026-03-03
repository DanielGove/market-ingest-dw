"""Coinbase spot connector package."""

from .connector import CoinbaseSpotConnector, l2_spec, trades_spec

__all__ = ["CoinbaseSpotConnector", "trades_spec", "l2_spec"]
