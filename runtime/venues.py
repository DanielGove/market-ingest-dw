"""Venue defaults and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class VenueDefaults:
    key: str
    label: str
    ingest_description: str
    websocket_uri: str
    local_base_path: str
    deepwater_base_path: str
    default_products_csv: str
    trades_prefix: str
    l2_prefix: str
    default_ob_prefix: str
    default_l2_feed_prefix: str
    kraken_book_depth_default: int | None = None


_COINBASE = VenueDefaults(
    key="coinbase",
    label="coinbase",
    ingest_description="Coinbase WebSocket ingest daemon",
    websocket_uri="wss://advanced-trade-ws.coinbase.com",
    local_base_path="data/coinbase-main",
    deepwater_base_path="/deepwater/data/coinbase-advanced",
    default_products_csv="BTC-USD,BTC-USDT,ETH-USD,ETH-USDT,SOL-USD,SOL-USDT,USDT-USD,USDT-USDC,XRP-USD,XRP-USDT",
    trades_prefix="CB-TRADES",
    l2_prefix="CB-L2",
    default_ob_prefix="OB",
    default_l2_feed_prefix="CB-L2",
)

_KRAKEN = VenueDefaults(
    key="kraken",
    label="kraken",
    ingest_description="Kraken WebSocket ingest daemon",
    websocket_uri="wss://ws.kraken.com/v2",
    local_base_path="data/kraken-main",
    deepwater_base_path="/deepwater/data/kraken-spot",
    default_products_csv="BTC-USD,BTC-USDT,ETH-USD,ETH-USDT,SOL-USD,SOL-USDT,USDT-USD,XRP-USD,XRP-USDT",
    trades_prefix="KR-TRADES",
    l2_prefix="KR-L2",
    default_ob_prefix="KROB",
    default_l2_feed_prefix="KR-L2",
    kraken_book_depth_default=1000,
)

_HYPERLIQUID = VenueDefaults(
    key="hyperliquid",
    label="hyperliquid",
    ingest_description="Hyperliquid WebSocket ingest daemon",
    websocket_uri="wss://api.hyperliquid.xyz/ws",
    local_base_path="data/hyperliquid-main",
    deepwater_base_path="/deepwater/data/hyperliquid-main",
    default_products_csv="BTC-USD,ETH-USD,SOL-USD,XRP-USD",
    trades_prefix="HL-TRADES",
    l2_prefix="HL-L2",
    default_ob_prefix="HLOB",
    default_l2_feed_prefix="HL-L2",
)

_VENUES = {
    _COINBASE.key: _COINBASE,
    _KRAKEN.key: _KRAKEN,
    _HYPERLIQUID.key: _HYPERLIQUID,
}


def get_venue_defaults(venue_key: str) -> VenueDefaults:
    try:
        return _VENUES[venue_key]
    except KeyError as e:
        raise ValueError(f"unsupported venue key: {venue_key}") from e


def resolve_base_path(venue: VenueDefaults, root_dir: Path) -> str:
    if Path("/deepwater/data").exists():
        return venue.deepwater_base_path
    return str((root_dir / venue.local_base_path).resolve())
