"""Bridge monitoring connector package.

Ingests cross-chain bridge transfer events (deposits and withdrawals) via
The Graph's graphql-transport-ws protocol.  Covers the major bridge rails
used for stablecoin and ETH cross-chain flows.

Supported bridge subgraphs (The Graph hosted service):
  Stargate (Ethereum)  : stargate-finance/mainnet-eth
  Stargate (Arbitrum)  : stargate-finance/arbitrum
  Stargate (Base)      : stargate-finance/base
  Stargate (Optimism)  : stargate-finance/optimism
  Stargate (BNB)       : stargate-finance/bsc
  Hop Protocol (ETH)   : hop-protocol/hop-mainnet
  Hop Protocol (ARB)   : hop-protocol/hop-arbitrum
  Across (ETH)         : across-protocol/across-v2

Operator examples:
  ./ops/deploy bridge \\
    --uri wss://api.thegraph.com/subgraphs/name/stargate-finance/mainnet-eth \\
    --products USDC,USDT,ETH \\
    --instance bridge-stargate-eth

  ./ops/deploy bridge \\
    --uri wss://api.thegraph.com/subgraphs/name/hop-protocol/hop-arbitrum \\
    --products USDC,USDT,ETH,WBTC \\
    --instance bridge-hop-arb
"""

CONNECTOR_PROFILE = {
    "key": "bridge",
    "label": "Bridge Monitor",
    "ingest_description": "Cross-chain bridge transfer monitoring daemon (via The Graph)",
    "websocket_uri": "wss://api.thegraph.com/subgraphs/name/stargate-finance/mainnet-eth",
    "base_path": "/deepwater/data/bridge",
    "default_products": "USDC,USDT,ETH,WBTC,DAI",
    "families": ("bridge_transfers",),
}

__all__ = ["CONNECTOR_PROFILE"]
