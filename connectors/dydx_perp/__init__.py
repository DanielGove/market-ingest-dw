"""dYdX v4 perpetual connector package."""

CONNECTOR_PROFILE = {
	"key": "dydx",
	"label": "dYdX Perp",
	"ingest_description": "dYdX v4 perpetuals WebSocket ingest daemon",
	"websocket_uri": "wss://indexer.dydx.trade/v4/ws/v4",
	"base_path": "/deepwater/data/dydx",
	"default_products": (
		"BTC-USD,ETH-USD,SOL-USD,DOGE-USD,AVAX-USD,"
		"LTC-USD,LINK-USD,ATOM-USD,CRV-USD,SUSHI-USD,"
		"AAVE-USD,SNX-USD,UNI-USD,XLM-USD,FIL-USD"
	),
	"families": ("trades", "l2", "perp_ctx"),
}

__all__ = ["CONNECTOR_PROFILE"]
