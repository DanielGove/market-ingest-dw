"""PancakeSwap connector package.

Covers both PancakeSwap V3 (concentrated liquidity) and V2 (constant-product)
pools on BNB Chain via The Graph's graphql-transport-ws protocol.  By
default connects to the V3 subgraph; override ``--uri`` to target V2 or
other chains.

Subgraph IDs (The Graph hosted service):
  PancakeSwap V3 BNB  : pancakeswap/exchange-v3-bnb
  PancakeSwap V2 BNB  : pancakeswap/exchange
  PancakeSwap V3 Ethereum : pancakeswap/exchange-v3-eth

Operator examples:
  # V3 (default)
  ./ops/deploy pancake \\
    --uri wss://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-bnb \\
    --products WBNB-USDT,WBNB-USDC,ETH-WBNB,BTCB-WBNB,CAKE-WBNB \\
    --instance pancake-v3-bsc

  # V2
  ./ops/deploy pancake \\
    --uri wss://api.thegraph.com/subgraphs/name/pancakeswap/exchange \\
    --products WBNB-USDT,WBNB-USDC,CAKE-WBNB,ETH-WBNB \\
    --instance pancake-v2-bsc
"""

# Top PancakeSwap BNB Chain pools by TVL.
_DEFAULT_POOLS = (
    "WBNB-USDT,WBNB-USDC,ETH-WBNB,BTCB-WBNB,"
    "CAKE-WBNB,WBNB-BUSD,USDT-USDC,WBNB-ADA,"
    "MATIC-WBNB,DOT-WBNB,LINK-WBNB,UNI-WBNB"
)

CONNECTOR_PROFILE = {
    "key": "pancake",
    "label": "PancakeSwap",
    "ingest_description": "PancakeSwap V3/V2 pool swap and liquidity WebSocket ingest daemon (via The Graph)",
    "websocket_uri": "wss://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-bnb",
    "base_path": "/deepwater/data/pancake",
    "default_products": _DEFAULT_POOLS,
    "families": ("pool_swaps", "pool_liquidity"),
}

__all__ = ["CONNECTOR_PROFILE"]
