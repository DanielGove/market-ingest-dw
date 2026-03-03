"""Central venue defaults for app/runtime wrappers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class VenueDefaults:
    key: str
    label: str
    ingest_description: str
    websocket_uri: str
    test_base_path: str
    local_base_path: str
    deepwater_base_path: str
    default_products_csv: str
    trades_prefix: str
    l2_prefix: str
    default_ob_prefix: str
    default_l2_feed_prefix: str
    extra_feed_prefixes_csv: str
    orderbook_supports_l2_prefix: bool
    supervise_write_pidfile: bool
    ctl_connect_timeout_s: float
    kraken_book_depth_default: int | None = None


_COINBASE = VenueDefaults(
    key="coinbase",
    label="coinbase",
    ingest_description="WebSocket ingest daemon",
    websocket_uri="wss://advanced-trade-ws.coinbase.com",
    test_base_path="data/coinbase-test",
    local_base_path="data/coinbase-main",
    deepwater_base_path="/deepwater/data/coinbase-advanced",
    default_products_csv="BTC-USD,BTC-USDT,ETH-USD,ETH-USDT,SOL-USD,SOL-USDT,USDT-USD,USDT-USDC,XRP-USD,XRP-USDT",
    trades_prefix="CB-TRADES",
    l2_prefix="CB-L2",
    default_ob_prefix="OB",
    default_l2_feed_prefix="",
    extra_feed_prefixes_csv="",
    orderbook_supports_l2_prefix=False,
    supervise_write_pidfile=False,
    ctl_connect_timeout_s=0.25,
)

_KRAKEN = VenueDefaults(
    key="kraken",
    label="kraken",
    ingest_description="Kraken WebSocket ingest daemon",
    websocket_uri="wss://ws.kraken.com/v2",
    test_base_path="data/kraken-test",
    local_base_path="data/kraken-main",
    deepwater_base_path="/deepwater/data/kraken-spot",
    default_products_csv="BTC-USD,BTC-USDT,ETH-USD,ETH-USDT,SOL-USD,SOL-USDT,USDT-USD,XRP-USD,XRP-USDT",
    trades_prefix="KR-TRADES",
    l2_prefix="KR-L2",
    default_ob_prefix="KROB",
    default_l2_feed_prefix="KR-L2",
    extra_feed_prefixes_csv="",
    orderbook_supports_l2_prefix=True,
    supervise_write_pidfile=True,
    ctl_connect_timeout_s=1.0,
    kraken_book_depth_default=1000,
)

_HYPERLIQUID = VenueDefaults(
    key="hyperliquid",
    label="hyperliquid",
    ingest_description="Hyperliquid WebSocket ingest daemon",
    websocket_uri="wss://api.hyperliquid.xyz/ws",
    test_base_path="data/hyperliquid-test",
    local_base_path="data/hyperliquid-main",
    deepwater_base_path="/deepwater/data/hyperliquid-main",
    default_products_csv="BTC-USD,ETH-USD,SOL-USD,XRP-USD",
    trades_prefix="HL-TRADES",
    l2_prefix="HL-L2",
    default_ob_prefix="HLOB",
    default_l2_feed_prefix="HL-L2",
    extra_feed_prefixes_csv="",
    orderbook_supports_l2_prefix=True,
    supervise_write_pidfile=True,
    ctl_connect_timeout_s=1.0,
)

_VENUES: dict[str, VenueDefaults] = {
    _COINBASE.key: _COINBASE,
    _KRAKEN.key: _KRAKEN,
    _HYPERLIQUID.key: _HYPERLIQUID,
}


def get_venue_defaults(key: str) -> VenueDefaults:
    try:
        return _VENUES[key]
    except KeyError as e:
        raise ValueError(f"unknown venue key: {key}") from e


def resolve_base_path(venue: VenueDefaults) -> str:
    if Path("/deepwater/data").exists():
        return venue.deepwater_base_path
    return venue.local_base_path
