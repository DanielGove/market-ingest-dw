"""Stablecoin monitoring connector package.

Ingests on-chain stablecoin transfer and supply events via The Graph's
graphql-transport-ws protocol.  Default subgraph targets Ethereum mainnet
stablecoin transfers (USDT, USDC, DAI, FRAX).  Override ``--uri`` to target
Base, Arbitrum, Optimism, or BNB Chain subgraph deployments.

Recommended subgraphs:
  Ethereum mainnet : messari/ethereum-stablecoins
  Arbitrum         : messari/arbitrum-stablecoins
  Base             : messari/base-stablecoins
  Optimism         : messari/optimism-stablecoins
  BNB Chain        : messari/bsc-stablecoins

Operator examples:
  ./ops/deploy stablecoin \\
    --uri wss://api.thegraph.com/subgraphs/name/messari/ethereum-stablecoins \\
    --products USDT,USDC,DAI,FRAX,LUSD \\
    --instance stablecoin-eth

  ./ops/deploy stablecoin \\
    --uri wss://api.thegraph.com/subgraphs/name/messari/arbitrum-stablecoins \\
    --products USDT,USDC,DAI \\
    --instance stablecoin-arb
"""

CONNECTOR_PROFILE = {
    "key": "stablecoin",
    "label": "Stablecoin Monitor",
    "ingest_description": "Multi-chain stablecoin transfer and supply monitoring daemon (via The Graph)",
    "websocket_uri": "wss://api.thegraph.com/subgraphs/name/messari/ethereum-stablecoins",
    "base_path": "/deepwater/data/stablecoin",
    "default_products": "USDT,USDC,DAI,FRAX,LUSD,BUSD,TUSD,USDP",
    "families": ("stablecoin_transfers", "stablecoin_supply"),
}

__all__ = ["CONNECTOR_PROFILE"]
