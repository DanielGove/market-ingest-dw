"""Uniswap V2 connector package.

Connects to The Graph's Uniswap V2 subgraph (Ethereum mainnet) via
graphql-transport-ws.  Override ``--uri`` for other V2 forks.

Default subgraph ID (The Graph hosted service legacy):
  uniswap/uniswap-v2

Operator example:
  ./ops/deploy univ2 \\
    --uri wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2 \\
    --products WETH-USDC,WBTC-WETH,WETH-USDT,UNI-WETH \\
    --instance univ2-eth
"""

# Top Uniswap V2 Ethereum pairs by liquidity.
_DEFAULT_PAIRS = (
    "WETH-USDC,WBTC-WETH,WETH-USDT,UNI-WETH,"
    "WETH-DAI,WBTC-USDC,COMP-WETH,LINK-WETH,"
    "MKR-WETH,AAVE-WETH,SNX-WETH,YFI-WETH"
)

CONNECTOR_PROFILE = {
    "key": "univ2",
    "label": "Uniswap V2",
    "ingest_description": "Uniswap V2 pair swap and liquidity WebSocket ingest daemon (via The Graph)",
    "websocket_uri": "wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2",
    "base_path": "/deepwater/data/univ2",
    "default_products": _DEFAULT_PAIRS,
    "families": ("pool_swaps", "pool_liquidity"),
}

__all__ = ["CONNECTOR_PROFILE"]
