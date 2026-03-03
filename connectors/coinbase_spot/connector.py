"""Coinbase spot connector for shared WebSocket ingest runtime."""

from __future__ import annotations

import time
from typing import Optional

import orjson
from fastnumbers import fast_float as _ff
from fastnumbers import fast_int as _fi
from simdjson import Parser as _JSONParser
from websocket import WebSocketConnectionClosedException

from deepwater.utils.timestamps import parse_us_timestamp


def trades_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-TRADES-{pid}",
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
        "feed_name": f"CB-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name": "event_time", "type": "uint64", "desc": "event timestamp (us)"},
            {"name": "received_time", "type": "uint64", "desc": "time packet was received (us)"},
            {"name": "processed_time", "type": "uint64", "desc": "time packet was ingested (us)"},
            {"name": "packet_sent", "type": "uint64", "desc": "time packet was sent (us)"},
            {"name": "type", "type": "char", "desc": "record type 'U'"},
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
    return parse_us_timestamp(data)


class CoinbaseSpotConnector:
    venue = "coinbase_spot"

    def __init__(self, *, uri: str = "wss://advanced-trade-ws.coinbase.com", sample_size: int = 16) -> None:
        self.uri = uri
        self.sample_size = int(sample_size)
        self._parser = _JSONParser()
        self._last_seq = -1
        self._seq_gaps = 0
        self._hb_timeout = 12.0

    def trades_spec(self, pid: str) -> dict:
        return trades_spec(pid)

    def l2_spec(self, pid: str) -> dict:
        return l2_spec(pid)

    def on_connect(self, _engine) -> None:
        return

    def send_subscribe(self, engine, product_ids) -> None:
        pids = tuple(product_ids or ())
        engine._ws.send(orjson.dumps({"type": "subscribe", "channel": "heartbeats"}))
        engine._ws.send(orjson.dumps({"type": "subscribe", "channel": "market_trades", "product_ids": pids}))
        engine._ws.send(orjson.dumps({"type": "subscribe", "channel": "level2", "product_ids": pids}))

    def send_unsubscribe(self, engine, targets) -> None:
        pids = tuple(targets or ())
        engine._ws.send(orjson.dumps({"type": "unsubscribe", "channel": "market_trades", "product_ids": pids}))
        engine._ws.send(orjson.dumps({"type": "unsubscribe", "channel": "level2", "product_ids": pids}))

    def on_timeout(self, engine) -> None:
        hb_age = time.monotonic() - engine._hb_last
        if hb_age > self._hb_timeout:
            raise WebSocketConnectionClosedException(
                f"heartbeat timeout ({hb_age:.1f}s > {self._hb_timeout:.1f}s)"
            )

    def handle_raw(self, engine, raw, recv_us: int, now_us) -> None:
        try:
            doc = self._parser.parse(raw).as_dict()
        except Exception as e:
            engine.log.warning("JSON decode error: %s", e)
            return

        if doc.get("type") == "error":
            engine.log.error("WS error message: %r", doc)
            return

        seq = doc.get("sequence_num")
        if seq is not None:
            s = _fi(seq)
            if self._last_seq >= 0 and s != self._last_seq + 1:
                self._seq_gaps += 1
            self._last_seq = s

        channel = doc.get("channel")
        if channel == "heartbeats":
            engine._hb_last = time.monotonic()
            return

        trade_writers = engine.trade_writers
        book_writers = engine.book_writers

        if channel == "market_trades":
            packet_us = _parse_ts(doc.get("timestamp"))
            for ev in doc["events"]:
                proc_us = now_us()
                for tr in reversed(ev["trades"]):
                    writer = trade_writers.get(tr["product_id"])
                    if writer is None:
                        continue
                    writer.write_values(
                        _parse_ts(tr["time"]),
                        recv_us,
                        proc_us,
                        packet_us,
                        _fi(tr["trade_id"]),
                        b"T",
                        tr["side"][0].encode("ascii"),
                        _ff(tr["price"]),
                        _ff(tr["size"]),
                    )
            return

        if channel == "l2_data":
            packet_us = _parse_ts(doc.get("timestamp"))
            for ev in doc["events"]:
                writer = book_writers.get(ev["product_id"])
                if writer is None:
                    engine.log.warning("no writer for product %s", ev.get("product_id"))
                    continue

                l2_type = ev["type"][0].encode("ascii")
                idx = l2_type == b"s"
                proc_us = now_us()

                for u in reversed(ev["updates"]):
                    evt_us = _parse_ts(u.get("event_time") or u.get("time"))
                    writer.write_values(
                        evt_us,
                        recv_us,
                        proc_us,
                        packet_us,
                        l2_type,
                        u["side"][0].encode("ascii"),
                        _ff(u["price_level"]),
                        _ff(u["new_quantity"]),
                        create_index=idx,
                    )
                    idx = False

    def extra_status(self, _engine) -> dict:
        return {"seq_gaps": self._seq_gaps}
