"""Connector registry for venue to adapter wiring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from connectors.coinbase_spot import CoinbaseSpotConnector
from connectors.coinbase_spot import l2_spec as _cb_l2_spec
from connectors.coinbase_spot import trades_spec as _cb_trades_spec
from connectors.kraken_spot import KrakenSpotConnector
from connectors.kraken_spot import l2_spec as _kr_l2_spec
from connectors.kraken_spot import trades_spec as _kr_trades_spec
from runtime.venues import VenueDefaults


BuildConnector = Callable[[VenueDefaults, dict[str, Any]], tuple[Any, dict[str, Any]]]


@dataclass(frozen=True)
class VenueConnectorRegistration:
    trades_spec: Callable[[str], dict]
    l2_spec: Callable[[str], dict]
    build_connector: BuildConnector


def _build_coinbase(venue: VenueDefaults, kwargs: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    uri = kwargs.pop("uri", venue.websocket_uri)
    sample_size = int(kwargs.pop("sample_size", 16))
    connector = CoinbaseSpotConnector(uri=uri, sample_size=sample_size)
    return connector, {"channels": ["market_trades", "level2"], "sample_size": sample_size}


def _build_kraken(venue: VenueDefaults, kwargs: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    uri = kwargs.pop("uri", venue.websocket_uri)
    depth_default = venue.kraken_book_depth_default or 1000
    book_depth = int(max(10, kwargs.pop("book_depth", depth_default)))
    connector = KrakenSpotConnector(uri=uri, book_depth=book_depth)
    return connector, {"book_depth": book_depth, "uri": connector.uri}


_REGISTRY = {
    "coinbase": VenueConnectorRegistration(
        trades_spec=_cb_trades_spec,
        l2_spec=_cb_l2_spec,
        build_connector=_build_coinbase,
    ),
    "kraken": VenueConnectorRegistration(
        trades_spec=_kr_trades_spec,
        l2_spec=_kr_l2_spec,
        build_connector=_build_kraken,
    ),
}


def get_venue_registration(venue_key: str) -> VenueConnectorRegistration:
    try:
        return _REGISTRY[venue_key]
    except KeyError as e:
        raise ValueError(f"unsupported venue key: {venue_key}") from e
