"""Uniswap V3 connector package.

Connects to The Graph's Uniswap V3 subgraph via the graphql-transport-ws
protocol.  The default URI targets the Ethereum mainnet subgraph hosted on
the decentralised network.  Override ``--uri`` at deploy time to target
Base, Arbitrum, or Optimism subgraph deployments.

Default subgraph IDs (The Graph decentralised network):
  Ethereum mainnet : 5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV
  Base             : GqzP4Xaehti8KSfQmv3ZctFSjnSUYZ4En5NRsiTbvZpz
  Arbitrum One     : FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM
  Optimism         : Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj

URI format:
  wss://gateway.thegraph.com/api/<API_KEY>/subgraphs/id/<SUBGRAPH_ID>

Operator example:
  ./ops/deploy univ3 \\
    --uri wss://gateway.thegraph.com/api/MY_KEY/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV \\
    --products WETH-USDC,WBTC-WETH,WETH-USDT,WETH-DAI \\
    --instance univ3-eth
"""

# Top Uniswap V3 Ethereum pools by TVL (TOKEN0-TOKEN1 format).
_DEFAULT_POOLS = (
    "WETH-USDC,WBTC-WETH,WETH-USDT,WETH-DAI,"
    "USDC-USDT,WBTC-USDC,WETH-WBTC,UNI-WETH,"
    "MATIC-WETH,LINK-WETH,ARB-WETH,OP-WETH"
)

CONNECTOR_PROFILE = {
    "key": "univ3",
    "label": "Uniswap V3",
    "ingest_description": "Uniswap V3 pool swap and liquidity WebSocket ingest daemon (via The Graph)",
    "websocket_uri": "wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    "base_path": "/deepwater/data/univ3",
    "default_products": _DEFAULT_POOLS,
    "families": ("pool_swaps", "pool_liquidity"),
}

__all__ = ["CONNECTOR_PROFILE"]
