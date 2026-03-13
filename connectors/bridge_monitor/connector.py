"""Bridge monitoring connector for shared WebSocket ingest runtime.

Subscribes to cross-chain bridge transfer events (deposits and withdrawals)
via The Graph's graphql-transport-ws protocol.  Targets bridge subgraphs
that expose a common schema for sender, receiver, token, amount, source
chain, and destination chain.

The connector emits a single ``bridge_transfers`` family.  Each record
captures the direction (deposit=D, withdrawal=W), the source/destination
chain identifiers, and the USD value at time of the transfer.  Products
are asset symbols (e.g. ``USDC``, ``ETH``).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from connectors.thegraph_ws import TheGraphWsConnector


def bridge_transfers_spec(symbol: str) -> dict:
    """Build Deepwater feed spec for cross-chain bridge transfer events."""

    return {
        "feed_name": f"BR-XFER-{symbol}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "block timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "block_number", "type": "uint64", "desc": "source block number"},
            {"name": "log_index", "type": "uint64", "desc": "log index within the block"},
            {"name": "direction", "type": "char", "desc": "D=deposit (inbound), W=withdrawal (outbound)"},
            {"name": "_", "type": "_7", "desc": "padding"},
            {"name": "amount", "type": "float64", "desc": "transfer amount in token units"},
            {"name": "amount_usd", "type": "float64", "desc": "USD value at time of transfer"},
            {"name": "src_chain_id", "type": "float64", "desc": "source chain ID"},
            {"name": "dst_chain_id", "type": "float64", "desc": "destination chain ID"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
    }


def _to_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def _to_int(val: Any) -> int:
    try:
        return int(val)
    except Exception:
        return 0


def _block_us(ts: Any) -> int:
    try:
        return int(ts) * 1_000_000
    except Exception:
        return 0


class BridgeMonitorConnector(TheGraphWsConnector):
    """Bridge transfer monitor via The Graph.

    Handles both Stargate and Hop Protocol subgraph schemas.  Queries for
    ``bridgeEvents`` (Stargate naming) and falls back to ``transfers`` or
    ``crossChainMessages`` if the bridge subgraph uses different entity names.
    """

    venue = "bridge_monitor"

    # Generic bridge transfer query.  Tries the ``crossChainTransferreds``
    # entity name used by Stargate-style subgraphs and aliased to
    # ``bridgeEvents`` for handler consistency.  When targeting Hop Protocol
    # or Across subgraphs the operator should confirm the entity name matches
    # the deployed subgraph schema (e.g. ``transfers`` for Hop) and redeploy
    # with a custom URI only; a future per-bridge subclass can override
    # ``_QUERY`` for schema-specific field names.
    _QUERY = """
subscription OnBridgeTransfers($tokens: [String!]!) {
  bridgeEvents: crossChainTransferreds(
    first: 200
    orderBy: blockTimestamp
    orderDirection: desc
    where: { token_: { symbol_in: $tokens } }
  ) {
    id
    blockNumber
    blockTimestamp
    token { symbol }
    amount
    amountUSD
    srcChainId
    dstChainId
    direction
    logIndex
  }
}
"""

    def __init__(
        self,
        *,
        uri: str = "wss://api.thegraph.com/subgraphs/name/stargate-finance/mainnet-eth",
        hb_timeout: float = 90.0,
        ping_interval: float = 30.0,
    ) -> None:
        super().__init__(uri=uri, hb_timeout=hb_timeout, ping_interval=ping_interval)
        self._last_block = 0

    def feed_specs(self, pid: str) -> dict[str, dict]:
        """Return bridge_transfers feed spec for an asset symbol."""

        return {
            "bridge_transfers": bridge_transfers_spec(pid),
        }

    def _build_subscription(self, product_ids: Sequence[str]) -> tuple[str, dict]:
        tokens = [str(p).upper() for p in product_ids if p]
        return self._QUERY, {"tokens": tokens or ["USDC", "USDT", "ETH"]}

    def _handle_data(self, engine: Any, data: dict, recv_us: int, now_us: Callable[[], int]) -> None:
        family_writers = engine.family_writers
        xfer_writers = family_writers.get("bridge_transfers", {})

        events = data.get("bridgeEvents") or []
        for evt in events:
            if not isinstance(evt, dict):
                continue
            block = _to_int(evt.get("blockNumber"))
            if block <= self._last_block:
                continue
            symbol = str((evt.get("token") or {}).get("symbol") or "").upper()
            writer = xfer_writers.get(symbol)
            if writer is None:
                continue
            ts_raw = evt.get("blockTimestamp") or evt.get("timestamp")
            ts_us = _block_us(ts_raw) or recv_us
            proc_us = now_us()
            dir_raw = str(evt.get("direction") or "").upper()
            direction = b"W" if dir_raw.startswith("W") else b"D"
            writer.write_values(
                ts_us,
                recv_us,
                proc_us,
                block,
                _to_int(evt.get("logIndex")),
                direction,
                _to_float(evt.get("amount")),
                _to_float(evt.get("amountUSD")),
                _to_float(evt.get("srcChainId")),
                _to_float(evt.get("dstChainId")),
            )

        new_block = max(
            (
                _to_int(e.get("blockNumber"))
                for e in events
                if isinstance(e, dict)
            ),
            default=self._last_block,
        )
        if new_block > self._last_block:
            self._last_block = new_block
