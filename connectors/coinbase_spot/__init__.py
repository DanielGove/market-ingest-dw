"""Coinbase spot connector package."""

CONNECTOR_PROFILE = {
	"key": "coinbase",
	"label": "coinbase",
	"ingest_description": "Coinbase WebSocket ingest daemon",
	"websocket_uri": "wss://advanced-trade-ws.coinbase.com",
	"base_path": "/deepwater/data/coinbase-advanced",
	"default_products": (
		"BTC-USD,BTC-USDT,ETH-USD,ETH-USDT,SOL-USD,SOL-USDT,"
		"USDT-USD,USDT-USDC,XRP-USD,XRP-USDT"
	),
	"families": ("trades", "l2"),
}

__all__ = ["CONNECTOR_PROFILE"]
