"""Hyperliquid perpetual connector for shared WebSocket ingest runtime."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

import orjson
from simdjson import Parser as _JSONParser
from websocket import WebSocketConnectionClosedException


def trades_spec(pid: str) -> dict:
    """Build Deepwater feed spec for Hyperliquid trades."""

    return {
        "feed_name": f"HL-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "trade_id", "type": "uint64", "desc": "venue trade id"},
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
    """Build Deepwater feed spec for Hyperliquid L2 book snapshots."""

    return {
        "feed_name": f"HL-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "type", "type": "char", "desc": "record type 's' snapshot"},
            {"name": "side", "type": "char", "desc": "B=bid,A=ask"},
            {"name": "_", "type": "_14", "desc": "padding"},
            {"name": "price", "type": "float64", "desc": "price level"},
            {"name": "qty", "type": "float64", "desc": "quantity at level"},
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


def _to_us_millis(val: Any) -> int:
    try:
        ms = int(val)
    except Exception:
        return 0
    if ms <= 0:
        return 0
    return ms * 1_000


def _product_to_coin(product_id: str) -> str:
    return str(product_id or "").split("-", 1)[0].upper()


def _coin_to_product(coin: str) -> str:
    return f"{str(coin or '').upper()}-USD"


class HyperliquidPerpConnector:
    """Venue adapter that owns Hyperliquid websocket protocol details."""

    venue = "hyperliquid_perp"

    def __init__(
        self,
        *,
        uri: str = "wss://api.hyperliquid.xyz/ws",
    ) -> None:
        self.uri = uri
        self._parser = _JSONParser()
        self._hb_timeout = 30.0
        self._ping_interval = 10.0
        now = time.monotonic()
        self._last_ping = now
        self._trade_fallback_id = 0

    def feed_specs(self, pid: str) -> dict[str, dict]:
        """Return primary and optional extra feed specs for one product."""

        specs = {
            "trades": trades_spec(pid),
            "l2": l2_spec(pid),
        }
        extras = self.extra_feed_specs(pid)
        if extras:
            specs.update(extras)
        return specs

    def extra_feed_specs(self, _pid: str) -> dict[str, dict]:
        """Override in subclasses/connectors to provide additional feed families."""

        return {}

    def on_connect(self, engine: Any) -> None:
        """Initialize heartbeat and ping timers after websocket connect."""

        now = time.monotonic()
        engine._hb_last = now
        self._last_ping = now

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None:
        """Send Hyperliquid subscribe messages for trades and l2Book channels."""

        coins = [_product_to_coin(p) for p in product_ids if p]
        if not coins:
            return
        for coin in coins:
            engine._ws.send(
                orjson.dumps(
                    {
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin},
                    }
                )
            )
            engine._ws.send(
                orjson.dumps(
                    {
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": coin},
                    }
                )
            )

    def send_unsubscribe(self, engine: Any, targets: Sequence[str]) -> None:
        """Send Hyperliquid unsubscribe messages for trades and l2Book channels."""

        coins = [_product_to_coin(p) for p in targets if p]
        if not coins:
            return
        for coin in coins:
            engine._ws.send(
                orjson.dumps(
                    {
                        "method": "unsubscribe",
                        "subscription": {"type": "trades", "coin": coin},
                    }
                )
            )
            engine._ws.send(
                orjson.dumps(
                    {
                        "method": "unsubscribe",
                        "subscription": {"type": "l2Book", "coin": coin},
                    }
                )
            )

    def _next_trade_fallback_id(self, evt_us: int) -> int:
        self._trade_fallback_id = (self._trade_fallback_id + 1) & 0xFFFF
        return ((evt_us & 0xFFFFFFFFFFFF) << 16) | self._trade_fallback_id

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
        """Parse Hyperliquid payloads and write trades/l2Book records."""

        try:
            doc = self._parser.parse(raw).as_dict()
        except Exception as e:
            engine.log.warning("JSON decode error: %s", e)
            return

        if not isinstance(doc, dict):
            return

        channel = str(doc.get("channel") or "").strip()
        if channel in ("subscriptionResponse", "pong"):
            return
        if channel == "error":
            engine.log.error("WS error message: %r", doc)
            return

        family_writers = engine.family_writers
        trade_writers = family_writers.get("trades", {})
        book_writers = family_writers.get("l2", {})

        if channel == "trades":
            data = doc.get("data") or []
            if isinstance(data, dict):
                data = [data]
            for tr in data:
                if not isinstance(tr, dict):
                    continue
                pid = _coin_to_product(tr.get("coin"))
                writer = trade_writers.get(pid)
                if writer is None:
                    continue

                evt_us = _to_us_millis(tr.get("time"))
                if evt_us <= 0:
                    evt_us = recv_us
                proc_us = now_us()
                packet_us = evt_us

                trade_id_raw = tr.get("tid")
                try:
                    trade_id = int(trade_id_raw)
                except Exception:
                    trade_id = self._next_trade_fallback_id(evt_us)

                side_raw = str(tr.get("side") or "").upper()
                side = b"B" if side_raw.startswith("B") else b"S"

                writer.write_values(
                    evt_us,
                    recv_us,
                    proc_us,
                    packet_us,
                    trade_id,
                    b"T",
                    side,
                    _to_float(tr.get("px")),
                    _to_float(tr.get("sz")),
                )
            return

        if channel == "l2Book":
            data = doc.get("data") or {}
            if not isinstance(data, dict):
                return
            pid = _coin_to_product(data.get("coin"))
            writer = book_writers.get(pid)
            if writer is None:
                return

            entry_us = _to_us_millis(data.get("time"))
            if entry_us <= 0:
                entry_us = recv_us
            proc_us = now_us()
            levels = data.get("levels") or []
            bids = levels[0] if len(levels) > 0 and isinstance(levels[0], list) else []
            asks = levels[1] if len(levels) > 1 and isinstance(levels[1], list) else []

            idx = True
            for level in bids:
                if not isinstance(level, dict):
                    continue
                writer.write_values(
                    entry_us,
                    recv_us,
                    proc_us,
                    entry_us,
                    b"s",
                    b"B",
                    _to_float(level.get("px")),
                    _to_float(level.get("sz")),
                    create_index=idx,
                )
                idx = False

            for level in asks:
                if not isinstance(level, dict):
                    continue
                writer.write_values(
                    entry_us,
                    recv_us,
                    proc_us,
                    entry_us,
                    b"s",
                    b"A",
                    _to_float(level.get("px")),
                    _to_float(level.get("sz")),
                    create_index=idx,
                )
                idx = False

    def extra_status(self, _engine: Any) -> dict[str, Any]:
        """Return Hyperliquid connector-specific runtime status fields."""

        return {
            "uri": self.uri,
            "venue": "hyperliquid",
        }
