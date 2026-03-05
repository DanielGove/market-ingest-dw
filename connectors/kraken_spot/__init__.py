"""Kraken spot connector package."""

CONNECTOR_PROFILE = {
	"key": "kraken",
	"label": "kraken",
	"ingest_description": "Kraken WebSocket ingest daemon",
	"websocket_uri": "wss://ws.kraken.com/v2",
	"base_path": "/deepwater/data/kraken",
	"default_products": (
		"BTC-USD,BTC-USDT,ETH-USD,ETH-USDT,SOL-USD,SOL-USDT,USDT-USD,XRP-USD,XRP-USDT"
	),
	"families": ("trades", "l2"),
}

__all__ = ["CONNECTOR_PROFILE"]
