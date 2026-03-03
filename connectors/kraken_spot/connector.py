"""Kraken spot connector for shared WebSocket ingest runtime."""

from __future__ import annotations

import time
from typing import Any, Optional

import orjson
from simdjson import Parser as _JSONParser
from websocket import WebSocketConnectionClosedException

from deepwater.utils.timestamps import parse_us_timestamp


def trades_spec(pid: str) -> dict:
    return {
        "feed_name": f"KR-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "trade_id", "type": "uint64", "desc": "exchange trade id"},
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
    return {
        "feed_name": f"KR-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "type", "type": "char", "desc": "record type 's' snapshot / 'u' update"},
            {"name": "side", "type": "char", "desc": "B=bid,A=ask"},
            {"name": "_", "type": "_14", "desc": "padding"},
            {"name": "price", "type": "float64", "desc": "price level"},
            {"name": "qty", "type": "float64", "desc": "new quantity at level"},
        ],
        "clock_level": 3,
        "chunk_size_bytes": 0.0625 * 1024 * 1024,
        "persist": True,
        "index_playback": True,
    }


def _ensure_bytes(val) -> Optional[bytes]:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytes(val)
    return str(val).encode("ascii")


def _parse_ts(val) -> int:
    data = _ensure_bytes(val)
    if not data:
        return 0
    try:
        return parse_us_timestamp(data)
    except Exception:
        return 0


def _to_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def _normalize_symbol(sym: str) -> str:
    out = str(sym or "").upper().replace("-", "/")
    if not out:
        return out
    if out.startswith("XBT/"):
        out = "BTC/" + out.split("/", 1)[1]
    if out.startswith("XXBT/"):
        out = "BTC/" + out.split("/", 1)[1]
    return out


def _symbol_to_product(sym: str) -> str:
    return _normalize_symbol(sym).replace("/", "-")


def _product_to_symbol(pid: str) -> str:
    return _normalize_symbol(str(pid or "").replace("-", "/"))


class KrakenSpotConnector:
    venue = "kraken_spot"

    def __init__(self, *, uri: str = "wss://ws.kraken.com/v2", book_depth: int = 1000) -> None:
        self.uri = uri
        self.book_depth = int(max(10, book_depth))
        self._parser = _JSONParser()
        self._hb_timeout = 20.0
        self._ping_interval = 10.0
        now = time.monotonic()
        self._last_ping = now
        self._trade_fallback_id = 0

    def trades_spec(self, pid: str) -> dict:
        return trades_spec(pid)

    def l2_spec(self, pid: str) -> dict:
        return l2_spec(pid)

    def on_connect(self, engine) -> None:
        now = time.monotonic()
        engine._hb_last = now
        self._last_ping = now

    def send_subscribe(self, engine, product_ids) -> None:
        symbols = [_product_to_symbol(p) for p in product_ids if p]
        if not symbols:
            return
        engine._ws.send(orjson.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": symbols}}))
        engine._ws.send(
            orjson.dumps(
                {
                    "method": "subscribe",
                    "params": {"channel": "book", "symbol": symbols, "depth": self.book_depth, "snapshot": True},
                }
            )
        )

    def send_unsubscribe(self, engine, targets) -> None:
        symbols = [_product_to_symbol(p) for p in targets if p]
        if not symbols:
            return
        engine._ws.send(orjson.dumps({"method": "unsubscribe", "params": {"channel": "trade", "symbol": symbols}}))
        engine._ws.send(orjson.dumps({"method": "unsubscribe", "params": {"channel": "book", "symbol": symbols}}))

    def _next_trade_fallback_id(self, evt_us: int) -> int:
        self._trade_fallback_id = (self._trade_fallback_id + 1) & 0xFFFF
        return ((evt_us & 0xFFFFFFFFFFFF) << 16) | self._trade_fallback_id

    def on_timeout(self, engine) -> None:
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

    def handle_raw(self, engine, raw, recv_us: int, now_us) -> None:
        try:
            doc = self._parser.parse(raw).as_dict()
        except Exception as e:
            engine.log.warning("JSON decode error: %s", e)
            return

        if not isinstance(doc, dict):
            return
        if doc.get("success") is False:
            engine.log.error("WS error message: %r", doc)
            return

        channel = str(doc.get("channel") or "").lower()
        msg_type = str(doc.get("type") or "").lower()

        if channel in ("heartbeat", "status"):
            return

        packet_us = _parse_ts(doc.get("time_in") or doc.get("time") or doc.get("timestamp"))
        if packet_us <= 0:
            packet_us = recv_us

        trade_writers = engine.trade_writers
        book_writers = engine.book_writers

        if channel == "trade":
            data = doc.get("data") or []
            for tr in data:
                if not isinstance(tr, dict):
                    continue
                pid = _symbol_to_product(tr.get("symbol"))
                writer = trade_writers.get(pid)
                if writer is None:
                    continue

                evt_us = _parse_ts(tr.get("timestamp") or tr.get("time")) or packet_us
                proc_us = now_us()
                trade_id_raw = tr.get("trade_id")
                try:
                    trade_id = int(trade_id_raw)
                except Exception:
                    trade_id = self._next_trade_fallback_id(evt_us)
                side_raw = str(tr.get("side") or "").lower()
                side = b"B" if side_raw.startswith("b") else b"S"
                writer.write_values(
                    evt_us,
                    recv_us,
                    proc_us,
                    packet_us,
                    trade_id,
                    b"T",
                    side,
                    _to_float(tr.get("price")),
                    _to_float(tr.get("qty") if tr.get("qty") is not None else tr.get("size")),
                )
            return

        if channel == "book":
            data = doc.get("data") or []
            l2_type = b"s" if msg_type == "snapshot" else b"u"
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                pid = _symbol_to_product(entry.get("symbol"))
                writer = book_writers.get(pid)
                if writer is None:
                    continue
                proc_us = now_us()
                entry_ts = _parse_ts(entry.get("timestamp") or doc.get("timestamp") or doc.get("time")) or packet_us
                idx = l2_type == b"s"

                bids = entry.get("bids") or []
                for u in bids:
                    if not isinstance(u, dict):
                        continue
                    evt_us = _parse_ts(u.get("timestamp")) or entry_ts
                    writer.write_values(evt_us, recv_us, proc_us, packet_us, l2_type, b"B", _to_float(u.get("price")), _to_float(u.get("qty")), create_index=idx)
                    idx = False

                asks = entry.get("asks") or []
                for u in asks:
                    if not isinstance(u, dict):
                        continue
                    evt_us = _parse_ts(u.get("timestamp")) or entry_ts
                    writer.write_values(evt_us, recv_us, proc_us, packet_us, l2_type, b"A", _to_float(u.get("price")), _to_float(u.get("qty")), create_index=idx)
                    idx = False

    def extra_status(self, _engine) -> dict:
        return {"uri": self.uri, "book_depth": self.book_depth}
