"""Binance spot connector package."""

CONNECTOR_PROFILE = {
	"key": "binance",
	"label": "binance",
	"ingest_description": "Binance Spot WebSocket ingest daemon",
	"websocket_uri": "wss://stream.binance.com:9443/ws",
	"base_path": "/deepwater/data/binance-spot",
	"default_products": (
		"BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,BNB-USDT,"
		"ADA-USDT,DOGE-USDT,LINK-USDT"
	),
	"families": ("trades", "l2"),
}

__all__ = ["CONNECTOR_PROFILE"]
