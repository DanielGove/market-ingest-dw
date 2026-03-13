"""Uniswap V3 connector for shared WebSocket ingest runtime.

Subscribes to swap and mint/burn events for requested token-pair pools via
The Graph's graphql-transport-ws protocol.  Each GraphQL subscription
notification delivers the most recent events from the latest indexed block;
the connector deduplicates by block number so each swap or liquidity event
is written exactly once.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from connectors.thegraph_ws import TheGraphWsConnector


def pool_swaps_spec(pid: str) -> dict:
    """Build Deepwater feed spec for Uniswap V3 swap events."""

    return {
        "feed_name": f"UV3-SWAPS-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "block timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "block_number", "type": "uint64", "desc": "block number containing this swap"},
            {"name": "log_index", "type": "uint64", "desc": "log index within the block"},
            {"name": "side", "type": "char", "desc": "B=buy token0,S=sell token0"},
            {"name": "_", "type": "_7", "desc": "padding"},
            {"name": "amount0", "type": "float64", "desc": "signed amount of token0 (positive=in, negative=out)"},
            {"name": "amount1", "type": "float64", "desc": "signed amount of token1"},
            {"name": "amount_usd", "type": "float64", "desc": "USD value of the swap"},
            {"name": "sqrt_price_x96", "type": "float64", "desc": "sqrtPriceX96 after swap (V3 price encoding)"},
            {"name": "tick", "type": "float64", "desc": "current tick after swap"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
    }


def pool_liquidity_spec(pid: str) -> dict:
    """Build Deepwater feed spec for Uniswap V3 mint/burn liquidity events."""

    return {
        "feed_name": f"UV3-LIQ-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "block timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "block_number", "type": "uint64", "desc": "block number"},
            {"name": "log_index", "type": "uint64", "desc": "log index within the block"},
            {"name": "event_type", "type": "char", "desc": "M=mint (add), B=burn (remove)"},
            {"name": "_", "type": "_7", "desc": "padding"},
            {"name": "amount", "type": "float64", "desc": "liquidity delta"},
            {"name": "amount0", "type": "float64", "desc": "token0 deposited/withdrawn"},
            {"name": "amount1", "type": "float64", "desc": "token1 deposited/withdrawn"},
            {"name": "tick_lower", "type": "float64", "desc": "lower tick of the position"},
            {"name": "tick_upper", "type": "float64", "desc": "upper tick of the position"},
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


def _pair_tokens(pid: str) -> tuple[str, str]:
    """Split a TOKEN0-TOKEN1 product id into its two tokens."""
    parts = str(pid or "").upper().split("-", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""


class UniswapV3Connector(TheGraphWsConnector):
    """Uniswap V3 connector: pool swaps and liquidity events via The Graph."""

    venue = "univ3_dex"

    # GraphQL subscription that fetches recent swaps and mints/burns.
    # Variables: $token0s, $token1s — lists of token symbols for the requested pairs.
    _SWAP_QUERY = """
subscription OnUniV3Events($token0s: [String!]!, $token1s: [String!]!) {
  swaps(
    first: 100
    orderBy: timestamp
    orderDirection: desc
    where: {
      token0_: { symbol_in: $token0s }
      token1_: { symbol_in: $token1s }
    }
  ) {
    id
    timestamp
    pool { id token0 { symbol } token1 { symbol } }
    amount0
    amount1
    amountUSD
    sqrtPriceX96
    tick
    logIndex
    transaction { blockNumber }
  }
  mints(
    first: 50
    orderBy: timestamp
    orderDirection: desc
    where: {
      pool_: {
        token0_: { symbol_in: $token0s }
        token1_: { symbol_in: $token1s }
      }
    }
  ) {
    id
    timestamp
    pool { id token0 { symbol } token1 { symbol } }
    amount
    amount0
    amount1
    tickLower
    tickUpper
    logIndex
    transaction { blockNumber }
  }
  burns(
    first: 50
    orderBy: timestamp
    orderDirection: desc
    where: {
      pool_: {
        token0_: { symbol_in: $token0s }
        token1_: { symbol_in: $token1s }
      }
    }
  ) {
    id
    timestamp
    pool { id token0 { symbol } token1 { symbol } }
    amount
    amount0
    amount1
    tickLower
    tickUpper
    logIndex
    transaction { blockNumber }
  }
}
"""

    def __init__(
        self,
        *,
        uri: str = "wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
        hb_timeout: float = 90.0,
        ping_interval: float = 30.0,
    ) -> None:
        super().__init__(uri=uri, hb_timeout=hb_timeout, ping_interval=ping_interval)
        self._product_ids: list[str] = []
        # Track last-seen block per event type to avoid duplicate writes.
        self._last_swap_block = 0
        self._last_liq_block = 0

    def feed_specs(self, pid: str) -> dict[str, dict]:
        """Return pool_swaps and pool_liquidity feed specs for a pool pair."""

        return {
            "pool_swaps": pool_swaps_spec(pid),
            "pool_liquidity": pool_liquidity_spec(pid),
        }

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None:
        """Store product list and send GraphQL subscription."""

        self._product_ids = list(product_ids)
        super().send_subscribe(engine, product_ids)

    def _build_subscription(self, product_ids: Sequence[str]) -> tuple[str, dict]:
        """Build a Uniswap V3 swap/liquidity subscription covering all requested pairs."""

        token0s: set[str] = set()
        token1s: set[str] = set()
        for pid in product_ids:
            t0, t1 = _pair_tokens(pid)
            if t0:
                token0s.add(t0)
            if t1:
                token1s.add(t1)
        return self._SWAP_QUERY, {
            "token0s": sorted(token0s) or ["WETH"],
            "token1s": sorted(token1s) or ["USDC"],
        }

    def _handle_data(self, engine: Any, data: dict, recv_us: int, now_us: Callable[[], int]) -> None:
        """Write swap and liquidity events to feed writers, deduplicating by block."""

        family_writers = engine.family_writers
        swap_writers = family_writers.get("pool_swaps", {})
        liq_writers = family_writers.get("pool_liquidity", {})

        # ---- swaps -------------------------------------------------------
        for swap in data.get("swaps") or []:
            if not isinstance(swap, dict):
                continue
            block = _to_int((swap.get("transaction") or {}).get("blockNumber"))
            if block <= self._last_swap_block:
                continue
            pool = swap.get("pool") or {}
            t0 = str((pool.get("token0") or {}).get("symbol") or "").upper()
            t1 = str((pool.get("token1") or {}).get("symbol") or "").upper()
            pid = f"{t0}-{t1}" if t0 and t1 else ""
            writer = swap_writers.get(pid)
            if writer is None:
                continue
            ts_us = _block_us(swap.get("timestamp")) or recv_us
            proc_us = now_us()
            amt0 = _to_float(swap.get("amount0"))
            side = b"B" if amt0 >= 0 else b"S"
            writer.write_values(
                ts_us,
                recv_us,
                proc_us,
                block,
                _to_int(swap.get("logIndex")),
                side,
                amt0,
                _to_float(swap.get("amount1")),
                _to_float(swap.get("amountUSD")),
                _to_float(swap.get("sqrtPriceX96")),
                _to_float(swap.get("tick")),
            )

        new_swap_block = max(
            (
                _to_int((s.get("transaction") or {}).get("blockNumber"))
                for s in (data.get("swaps") or [])
                if isinstance(s, dict)
            ),
            default=self._last_swap_block,
        )
        if new_swap_block > self._last_swap_block:
            self._last_swap_block = new_swap_block

        # ---- mints and burns (pool_liquidity) ----------------------------
        for evts, ev_type_byte in (
            (data.get("mints") or [], b"M"),
            (data.get("burns") or [], b"B"),
        ):
            for evt in evts:
                if not isinstance(evt, dict):
                    continue
                block = _to_int((evt.get("transaction") or {}).get("blockNumber"))
                if block <= self._last_liq_block:
                    continue
                pool = evt.get("pool") or {}
                t0 = str((pool.get("token0") or {}).get("symbol") or "").upper()
                t1 = str((pool.get("token1") or {}).get("symbol") or "").upper()
                pid = f"{t0}-{t1}" if t0 and t1 else ""
                writer = liq_writers.get(pid)
                if writer is None:
                    continue
                ts_us = _block_us(evt.get("timestamp")) or recv_us
                proc_us = now_us()
                writer.write_values(
                    ts_us,
                    recv_us,
                    proc_us,
                    block,
                    _to_int(evt.get("logIndex")),
                    ev_type_byte,
                    _to_float(evt.get("amount")),
                    _to_float(evt.get("amount0")),
                    _to_float(evt.get("amount1")),
                    _to_float(evt.get("tickLower")),
                    _to_float(evt.get("tickUpper")),
                    create_index=True,
                )

        new_liq_block = max(
            (
                _to_int((e.get("transaction") or {}).get("blockNumber"))
                for e in (data.get("mints") or []) + (data.get("burns") or [])
                if isinstance(e, dict)
            ),
            default=self._last_liq_block,
        )
        if new_liq_block > self._last_liq_block:
            self._last_liq_block = new_liq_block
