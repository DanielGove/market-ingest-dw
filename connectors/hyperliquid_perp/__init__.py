"""Hyperliquid perpetual connector package."""

# Main crypto perpetuals: top pairs by open interest / volume on Hyperliquid.
# TradFi perpetuals: Hyperliquid indexes non-crypto instruments using the "@"
# prefix notation (e.g. @SPX, @GOLD, @SILVER).  Include these in the default
# set so they are ingested automatically when Hyperliquid makes them available;
# the connector handles missing/unknown subscriptions gracefully.
_CRYPTO_PERPS = (
    "BTC-USD,ETH-USD,SOL-USD,XRP-USD,"
    "BNB-USD,AVAX-USD,DOGE-USD,LINK-USD,"
    "ARB-USD,OP-USD,MATIC-USD,SUI-USD,"
    "APT-USD,INJ-USD,TIA-USD,WIF-USD"
)

# Non-crypto perpetuals using Hyperliquid's "@<ticker>" coin convention.
# @SPX  = S&P 500 index perpetual
# @GOLD = Gold futures perpetual
# @SILVER = Silver futures perpetual
_TRADFI_PERPS = "@SPX-USD,@GOLD-USD,@SILVER-USD"

CONNECTOR_PROFILE = {
	"key": "hyperliquid",
	"label": "Hyperliquid Perp",
	"ingest_description": "Hyperliquid perpetuals WebSocket ingest daemon",
	"websocket_uri": "wss://api.hyperliquid.xyz/ws",
	"base_path": "/deepwater/data/hyperliquid",
	"default_products": f"{_CRYPTO_PERPS},{_TRADFI_PERPS}",
	"families": ("trades", "l2", "perp_ctx", "funding", "open_interest"),
}

__all__ = ["CONNECTOR_PROFILE"]
