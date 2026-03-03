"""Kraken spot connector package."""

from .connector import KrakenSpotConnector, l2_spec, trades_spec

__all__ = ["KrakenSpotConnector", "trades_spec", "l2_spec"]
