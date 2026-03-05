"""Hyperliquid perpetual connector package."""

CONNECTOR_PROFILE = {
	"key": "hyperliquid",
	"label": "hyperliquid",
	"ingest_description": "Hyperliquid WebSocket ingest daemon",
	"websocket_uri": "wss://api.hyperliquid.xyz/ws",
	"base_path": "/deepwater/data/hyperliquid",
	"default_products": "BTC-USD,ETH-USD,SOL-USD,XRP-USD",
	"families": ("trades", "l2"),
}

__all__ = ["CONNECTOR_PROFILE"]