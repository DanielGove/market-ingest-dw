"""Stablecoin monitoring connector for shared WebSocket ingest runtime.

Subscribes to stablecoin transfer events and supply snapshots via The
Graph's graphql-transport-ws protocol.  Two feed families are emitted:

- ``stablecoin_transfers``: individual large transfer events (sender, receiver,
  amount, block).  Mint and burn events (transfers from/to the zero address)
  are flagged in the ``direction`` field.

- ``stablecoin_supply``: per-token total supply snapshots emitted whenever the
  supply changes.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from connectors.thegraph_ws import TheGraphWsConnector

# Zero address used by stablecoin contracts for mint (from=zero) and burn (to=zero).
_ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def stablecoin_transfers_spec(symbol: str) -> dict:
    """Build Deepwater feed spec for stablecoin transfer events."""

    return {
        "feed_name": f"SC-XFER-{symbol}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "block timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "block_number", "type": "uint64", "desc": "block number"},
            {"name": "log_index", "type": "uint64", "desc": "log index within the block"},
            {"name": "direction", "type": "char", "desc": "T=transfer, M=mint (from zero), B=burn (to zero)"},
            {"name": "_", "type": "_7", "desc": "padding"},
            {"name": "amount", "type": "float64", "desc": "transfer amount in token units"},
            {"name": "amount_usd", "type": "float64", "desc": "USD value at time of transfer"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
    }


def stablecoin_supply_spec(symbol: str) -> dict:
    """Build Deepwater feed spec for stablecoin total supply snapshots."""

    return {
        "feed_name": f"SC-SUPPLY-{symbol}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "block timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "block_number", "type": "uint64", "desc": "block number"},
            {"name": "total_supply", "type": "float64", "desc": "total outstanding token supply"},
            {"name": "circulating_supply", "type": "float64", "desc": "circulating supply (may equal total_supply if unavailable)"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
        "index_playback": True,
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


class StablecoinMonitorConnector(TheGraphWsConnector):
    """Stablecoin transfer and supply monitor via The Graph."""

    venue = "stablecoin_monitor"

    # Messari stablecoin subgraph query.  The ``tokens`` entity provides
    # current supply; ``transfers`` provides individual transfer events.
    _QUERY = """
subscription OnStablecoinEvents($symbols: [String!]!) {
  tokens(
    where: { symbol_in: $symbols }
  ) {
    id
    symbol
    lastPriceBlockNumber
    totalSupply
    circulatingSupply
  }
  transfers(
    first: 200
    orderBy: blockNumber
    orderDirection: desc
    where: { token_: { symbol_in: $symbols } }
  ) {
    id
    blockNumber
    timestamp
    token { symbol }
    from
    to
    amount
    amountUSD
    logIndex
  }
}
"""

    def __init__(
        self,
        *,
        uri: str = "wss://api.thegraph.com/subgraphs/name/messari/ethereum-stablecoins",
        hb_timeout: float = 90.0,
        ping_interval: float = 30.0,
    ) -> None:
        super().__init__(uri=uri, hb_timeout=hb_timeout, ping_interval=ping_interval)
        self._last_xfer_block = 0
        self._last_supply_block: dict[str, int] = {}

    def feed_specs(self, pid: str) -> dict[str, dict]:
        """Return stablecoin_transfers and stablecoin_supply feed specs.

        ``pid`` is a stablecoin symbol such as ``USDT`` or ``USDC``.
        """

        return {
            "stablecoin_transfers": stablecoin_transfers_spec(pid),
            "stablecoin_supply": stablecoin_supply_spec(pid),
        }

    def _build_subscription(self, product_ids: Sequence[str]) -> tuple[str, dict]:
        symbols = [str(p).upper() for p in product_ids if p]
        return self._QUERY, {"symbols": symbols or ["USDT", "USDC"]}

    def _handle_data(self, engine: Any, data: dict, recv_us: int, now_us: Callable[[], int]) -> None:
        family_writers = engine.family_writers
        xfer_writers = family_writers.get("stablecoin_transfers", {})
        supply_writers = family_writers.get("stablecoin_supply", {})

        # ---- supply snapshots -------------------------------------------
        for token in data.get("tokens") or []:
            if not isinstance(token, dict):
                continue
            symbol = str(token.get("symbol") or "").upper()
            writer = supply_writers.get(symbol)
            if writer is None:
                continue
            block = _to_int(token.get("lastPriceBlockNumber"))
            last = self._last_supply_block.get(symbol, 0)
            if block <= last:
                continue
            proc_us = now_us()
            total = _to_float(token.get("totalSupply"))
            circ = _to_float(token.get("circulatingSupply")) or total
            writer.write_values(
                recv_us,
                recv_us,
                proc_us,
                block,
                total,
                circ,
                create_index=True,
            )
            self._last_supply_block[symbol] = block

        # ---- transfer events --------------------------------------------
        for xfer in data.get("transfers") or []:
            if not isinstance(xfer, dict):
                continue
            block = _to_int(xfer.get("blockNumber"))
            if block <= self._last_xfer_block:
                continue
            symbol = str((xfer.get("token") or {}).get("symbol") or "").upper()
            writer = xfer_writers.get(symbol)
            if writer is None:
                continue
            ts_us = _block_us(xfer.get("timestamp")) or recv_us
            proc_us = now_us()
            from_addr = str(xfer.get("from") or "").lower()
            to_addr = str(xfer.get("to") or "").lower()
            if from_addr == _ZERO_ADDR:
                direction = b"M"  # mint
            elif to_addr == _ZERO_ADDR:
                direction = b"B"  # burn
            else:
                direction = b"T"  # transfer
            writer.write_values(
                ts_us,
                recv_us,
                proc_us,
                block,
                _to_int(xfer.get("logIndex")),
                direction,
                _to_float(xfer.get("amount")),
                _to_float(xfer.get("amountUSD")),
            )

        new_xfer_block = max(
            (
                _to_int(x.get("blockNumber"))
                for x in (data.get("transfers") or [])
                if isinstance(x, dict)
            ),
            default=self._last_xfer_block,
        )
        if new_xfer_block > self._last_xfer_block:
            self._last_xfer_block = new_xfer_block
