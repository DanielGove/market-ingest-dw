"""dYdX v4 perpetual connector for shared WebSocket ingest runtime.

Connects to the public dYdX v4 indexer WebSocket endpoint.  No API key is
required.  The dYdX v4 indexer uses a simple JSON-over-WebSocket protocol
with typed ``subscribe`` / ``channel_data`` / ``channel_batch_data`` messages.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any

import orjson
from simdjson import Parser as _JSONParser
from websocket import WebSocketConnectionClosedException


def trades_spec(pid: str) -> dict:
    """Build Deepwater feed spec for dYdX v4 trades."""

    return {
        "feed_name": f"DX-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "trade_id", "type": "uint64", "desc": "venue trade id (hash-derived)"},
            {"name": "type", "type": "char", "desc": "record type 'T'"},
            {"name": "side", "type": "char", "desc": "B=buy,S=sell"},
            {"name": "_", "type": "_6", "desc": "padding"},
            {"name": "price", "type": "float64", "desc": "trade price"},
            {"name": "size", "type": "float64", "desc": "trade size"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
    }


def l2_spec(pid: str) -> dict:
    """Build Deepwater feed spec for dYdX v4 orderbook levels."""

    return {
        "feed_name": f"DX-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "type", "type": "char", "desc": "s=snapshot, u=update"},
            {"name": "side", "type": "char", "desc": "B=bid,A=ask"},
            {"name": "_", "type": "_14", "desc": "padding"},
            {"name": "price", "type": "float64", "desc": "price level"},
            {"name": "qty", "type": "float64", "desc": "quantity at level (0=remove)"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
        "index_playback": True,
    }


def perp_ctx_spec(pid: str) -> dict:
    """Build Deepwater feed spec for dYdX v4 perpetual context snapshots.

    Captures per-market derivatives state delivered by the ``v4_markets``
    channel: next funding rate, open interest, oracle price, and price
    change.  Each record is a complete state snapshot.
    """

    return {
        "feed_name": f"DX-PERP-CTX-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "next_funding_rate", "type": "float64", "desc": "estimated next hourly funding rate"},
            {"name": "open_interest", "type": "float64", "desc": "total open interest in base asset"},
            {"name": "oracle_price", "type": "float64", "desc": "oracle / index price"},
            {"name": "price_change_24h", "type": "float64", "desc": "24-hour price change (absolute)"},
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


def _parse_iso_us(val: Any) -> int:
    """Parse an ISO 8601 timestamp string to microseconds since epoch."""
    if not val:
        return 0
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return int(dt.timestamp() * 1_000_000)
    except Exception:
        return 0


def _trade_id_from_str(s: str, recv_us: int) -> int:
    """Derive a stable uint64 trade id from a dYdX trade id string."""
    try:
        h = hash(s) & 0xFFFFFFFFFFFFFFFF
        return h if h else (recv_us & 0xFFFFFFFFFFFFFFFF)
    except Exception:
        return recv_us & 0xFFFFFFFFFFFFFFFF


class DydxPerpConnector:
    """Venue adapter for the dYdX v4 indexer WebSocket protocol."""

    venue = "dydx_perp"

    def __init__(
        self,
        *,
        uri: str = "wss://indexer.dydx.trade/v4/ws/v4",
    ) -> None:
        self.uri = uri
        self._parser = _JSONParser()
        self._hb_timeout = 45.0
        self._ping_interval = 15.0
        now = time.monotonic()
        self._last_ping = now
        self._req_id = 0
        # Track which markets subscription batch is active so we can handle
        # both initial snapshots and incremental updates for perp_ctx.
        self._markets_subscribed = False

    def feed_specs(self, pid: str) -> dict[str, dict]:
        """Return feed specs for dYdX trades, L2 orderbook, and perp context."""

        return {
            "trades": trades_spec(pid),
            "l2": l2_spec(pid),
            "perp_ctx": perp_ctx_spec(pid),
        }

    def on_connect(self, engine: Any) -> None:
        """Reset heartbeat and ping timers after websocket connect."""

        now = time.monotonic()
        engine._hb_last = now
        self._last_ping = now
        self._markets_subscribed = False

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None:
        """Send dYdX v4 subscribe messages for trades, orderbook, and markets."""

        pids = [p.strip().upper() for p in product_ids if p]
        if not pids:
            return
        for pid in pids:
            engine._ws.send(
                orjson.dumps({"type": "subscribe", "channel": "v4_trades", "id": pid, "batched": True})
            )
            engine._ws.send(
                orjson.dumps({"type": "subscribe", "channel": "v4_orderbook", "id": pid, "batched": True})
            )
        if not self._markets_subscribed:
            engine._ws.send(
                orjson.dumps({"type": "subscribe", "channel": "v4_markets", "batched": True})
            )
            self._markets_subscribed = True

    def send_unsubscribe(self, engine: Any, targets: Sequence[str]) -> None:
        """Send dYdX v4 unsubscribe messages for trades and orderbook channels."""

        pids = [p.strip().upper() for p in targets if p]
        if not pids:
            return
        for pid in pids:
            engine._ws.send(
                orjson.dumps({"type": "unsubscribe", "channel": "v4_trades", "id": pid})
            )
            engine._ws.send(
                orjson.dumps({"type": "unsubscribe", "channel": "v4_orderbook", "id": pid})
            )

    def on_timeout(self, engine: Any) -> None:
        """Send keepalive pings and reconnect on heartbeat timeout."""

        now_mono = time.monotonic()
        hb_age = now_mono - engine._hb_last
        if hb_age > self._hb_timeout:
            raise WebSocketConnectionClosedException(
                f"heartbeat timeout ({hb_age:.1f}s > {self._hb_timeout:.1f}s)"
            )
        if now_mono - self._last_ping >= self._ping_interval:
            try:
                engine._ws.ping("keepalive")
            except Exception:
                pass
            self._last_ping = now_mono

    def handle_raw(self, engine: Any, raw: bytes | str, recv_us: int, now_us: Callable[[], int]) -> None:
        """Parse dYdX v4 payloads and write trades, L2, and perp_ctx records."""

        try:
            doc = self._parser.parse(raw).as_dict()
        except Exception as e:
            engine.log.warning("JSON decode error: %s", e)
            return

        if not isinstance(doc, dict):
            return

        msg_type = str(doc.get("type") or "").strip()
        if msg_type in ("connected", "pong"):
            engine._hb_last = time.monotonic()
            return
        if msg_type == "error":
            engine.log.error("WS error message: %r", doc)
            return

        engine._hb_last = time.monotonic()

        channel = str(doc.get("channel") or "").strip()
        contents = doc.get("contents") or {}
        if not isinstance(contents, dict):
            return

        family_writers = engine.family_writers
        trade_writers = family_writers.get("trades", {})
        book_writers = family_writers.get("l2", {})
        ctx_writers = family_writers.get("perp_ctx", {})

        if channel == "v4_trades":
            pid = str(doc.get("id") or "").strip().upper()
            writer = trade_writers.get(pid)
            if writer is None:
                return
            for tr in contents.get("trades") or []:
                if not isinstance(tr, dict):
                    continue
                evt_us = _parse_iso_us(tr.get("createdAt")) or recv_us
                proc_us = now_us()
                tid_raw = str(tr.get("id") or "")
                trade_id = _trade_id_from_str(tid_raw, recv_us)
                side_raw = str(tr.get("side") or "").upper()
                side = b"B" if side_raw.startswith("B") else b"S"
                writer.write_values(
                    evt_us,
                    recv_us,
                    proc_us,
                    recv_us,
                    trade_id,
                    b"T",
                    side,
                    _to_float(tr.get("price")),
                    _to_float(tr.get("size")),
                )
            return

        if channel == "v4_orderbook":
            pid = str(doc.get("id") or "").strip().upper()
            writer = book_writers.get(pid)
            if writer is None:
                return
            proc_us = now_us()
            # initial snapshot on "subscribed"; incremental on "channel_data"
            is_snapshot = msg_type in ("subscribed", "channel_batch_data")
            level_type = b"s" if is_snapshot else b"u"
            bids = contents.get("bids") or []
            asks = contents.get("asks") or []
            idx = is_snapshot
            for level in bids:
                if not isinstance(level, dict):
                    continue
                writer.write_values(
                    recv_us,
                    recv_us,
                    proc_us,
                    recv_us,
                    level_type,
                    b"B",
                    _to_float(level.get("price")),
                    _to_float(level.get("size")),
                    create_index=idx,
                )
                idx = False
            for level in asks:
                if not isinstance(level, dict):
                    continue
                writer.write_values(
                    recv_us,
                    recv_us,
                    proc_us,
                    recv_us,
                    level_type,
                    b"A",
                    _to_float(level.get("price")),
                    _to_float(level.get("size")),
                    create_index=idx,
                )
                idx = False
            return

        if channel == "v4_markets":
            proc_us = now_us()
            # "subscribed" sends {"markets": {...}}; "channel_data" sends {"trading": {...}}
            market_map = contents.get("markets") or contents.get("trading") or {}
            if not isinstance(market_map, dict):
                return
            for pid, mkt in market_map.items():
                if not isinstance(mkt, dict):
                    continue
                writer = ctx_writers.get(str(pid).strip().upper())
                if writer is None:
                    continue
                writer.write_values(
                    recv_us,
                    recv_us,
                    proc_us,
                    # dYdX uses camelCase in "subscribed" snapshots and "channel_data"
                    # streaming updates; snake_case may appear in older indexer versions.
                    _to_float(mkt.get("nextFundingRate") or mkt.get("next_funding_rate")),
                    _to_float(mkt.get("openInterest") or mkt.get("open_interest")),
                    _to_float(mkt.get("oraclePrice") or mkt.get("oracle_price") or mkt.get("indexPrice")),
                    _to_float(mkt.get("priceChange24H") or mkt.get("price_change_24h")),
                    create_index=True,
                )

    def extra_status(self, _engine: Any) -> dict[str, Any]:
        """Return dYdX connector-specific runtime status fields."""

        return {
            "uri": self.uri,
            "venue": "dydx",
            "markets_subscribed": self._markets_subscribed,
        }
