"""Binance spot connector for shared WebSocket ingest runtime."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

import orjson
from simdjson import Parser as _JSONParser
from websocket import WebSocketConnectionClosedException


def trades_spec(pid: str) -> dict:
    """Build Deepwater feed spec for Binance aggregated trades."""

    return {
        "feed_name": f"BN-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "exchange event packet time (us)"},
            {"name": "trade_id", "type": "uint64", "desc": "aggregate trade id"},
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
    """Build Deepwater feed spec for Binance L2 depth updates."""

    return {
        "feed_name": f"BN-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "exchange event packet time (us)"},
            {"name": "type", "type": "char", "desc": "record type 'u'"},
            {"name": "side", "type": "char", "desc": "B=bid,A=ask"},
            {"name": "_", "type": "_14", "desc": "padding"},
            {"name": "price", "type": "float64", "desc": "price level"},
            {"name": "qty", "type": "float64", "desc": "new quantity at level"},
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


def _to_us_millis(val: Any) -> int:
    try:
        ms = int(val)
    except Exception:
        return 0
    if ms <= 0:
        return 0
    return ms * 1_000


def _product_to_symbol(product_id: str) -> str:
    return str(product_id or "").replace("-", "").upper()


def _symbol_to_product(symbol: str) -> str:
    raw = str(symbol or "").upper()
    for quote in ("USDT", "USDC", "FDUSD", "BTC", "ETH", "BNB", "TRY", "EUR"):
        if raw.endswith(quote) and len(raw) > len(quote):
            return f"{raw[:-len(quote)]}-{quote}"
    return raw


class BinanceSpotConnector:
    """Venue adapter that owns Binance spot websocket protocol details."""

    venue = "binance_spot"

    def __init__(self, *, uri: str = "wss://stream.binance.com:9443/ws") -> None:
        self.uri = uri
        self._parser = _JSONParser()
        self._hb_timeout = 20.0
        self._ping_interval = 10.0
        now = time.monotonic()
        self._last_ping = now
        self._req_id = 0
        self._trade_fallback_id = 0

    def feed_specs(self, pid: str) -> dict[str, dict]:
        """Return feed specs for Binance aggregated trades and depth updates."""

        return {
            "trades": trades_spec(pid),
            "l2": l2_spec(pid),
        }

    def on_connect(self, engine: Any) -> None:
        """Initialize heartbeat and ping timestamps after websocket connect."""

        now = time.monotonic()
        engine._hb_last = now
        self._last_ping = now

    def _next_req_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None:
        """Send Binance subscription messages for aggTrade and depth streams."""

        symbols = [_product_to_symbol(p).lower() for p in product_ids if p]
        if not symbols:
            return
        params: list[str] = []
        for symbol in symbols:
            params.extend((f"{symbol}@aggTrade", f"{symbol}@depth@100ms"))
        engine._ws.send(orjson.dumps({"method": "SUBSCRIBE", "params": params, "id": self._next_req_id()}))

    def send_unsubscribe(self, engine: Any, targets: Sequence[str]) -> None:
        """Send Binance unsubscribe messages for aggTrade and depth streams."""

        symbols = [_product_to_symbol(p).lower() for p in targets if p]
        if not symbols:
            return
        params: list[str] = []
        for symbol in symbols:
            params.extend((f"{symbol}@aggTrade", f"{symbol}@depth@100ms"))
        engine._ws.send(orjson.dumps({"method": "UNSUBSCRIBE", "params": params, "id": self._next_req_id()}))

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
        """Parse Binance payloads and write aggTrade/depth records."""

        try:
            doc = self._parser.parse(raw).as_dict()
        except Exception as e:
            engine.log.warning("JSON decode error: %s", e)
            return

        if not isinstance(doc, dict):
            return
        if "result" in doc and doc.get("id") is not None:
            return
        if doc.get("error") is not None:
            engine.log.error("WS error message: %r", doc)
            return

        event_type = str(doc.get("e") or "").strip()
        if not event_type:
            return
        engine._hb_last = time.monotonic()

        family_writers = engine.family_writers
        trade_writers = family_writers.get("trades", {})
        book_writers = family_writers.get("l2", {})

        if event_type == "aggTrade":
            pid = _symbol_to_product(doc.get("s"))
            writer = trade_writers.get(pid)
            if writer is None:
                return
            evt_us = _to_us_millis(doc.get("T")) or recv_us
            packet_us = _to_us_millis(doc.get("E")) or evt_us
            proc_us = now_us()
            trade_id_raw = doc.get("a")
            try:
                trade_id = int(trade_id_raw)
            except Exception:
                trade_id = self._next_trade_fallback_id(evt_us)
            is_buyer_maker = bool(doc.get("m"))
            side = b"S" if is_buyer_maker else b"B"
            writer.write_values(
                evt_us,
                recv_us,
                proc_us,
                packet_us,
                trade_id,
                b"T",
                side,
                _to_float(doc.get("p")),
                _to_float(doc.get("q")),
            )
            return

        if event_type == "depthUpdate":
            pid = _symbol_to_product(doc.get("s"))
            writer = book_writers.get(pid)
            if writer is None:
                return
            evt_us = _to_us_millis(doc.get("T") or doc.get("E")) or recv_us
            packet_us = _to_us_millis(doc.get("E")) or evt_us
            proc_us = now_us()
            for price, qty in doc.get("b") or []:
                writer.write_values(
                    evt_us,
                    recv_us,
                    proc_us,
                    packet_us,
                    b"u",
                    b"B",
                    _to_float(price),
                    _to_float(qty),
                )
            for price, qty in doc.get("a") or []:
                writer.write_values(
                    evt_us,
                    recv_us,
                    proc_us,
                    packet_us,
                    b"u",
                    b"A",
                    _to_float(price),
                    _to_float(qty),
                )

    def extra_status(self, _engine: Any) -> dict[str, Any]:
        """Return Binance connector-specific runtime status fields."""

        return {
            "uri": self.uri,
            "venue": "binance",
        }
