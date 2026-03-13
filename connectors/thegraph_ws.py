"""Shared GraphQL-over-WebSocket protocol handler for The Graph subscriptions.

Implements the ``graphql-transport-ws`` protocol used by The Graph hosted
service and decentralised network gateway.  Connector subclasses provide the
subscription query and data handler; this module owns the protocol state
machine (connection_init / connection_ack / ping / pong / subscribe / next /
complete) and heartbeat/reconnect behaviour.

Protocol reference:
  https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

import orjson
from simdjson import Parser as _JSONParser
from websocket import WebSocketConnectionClosedException

_CONN_INIT = orjson.dumps({"type": "connection_init", "payload": {}})


class TheGraphWsConnector:
    """Base connector for venues accessed via The Graph's graphql-transport-ws protocol.

    Subclasses must implement:
    - ``venue`` class attribute (str)
    - ``feed_specs(pid)``
    - ``_build_subscription(product_ids)`` → (query_str, variables_dict)
    - ``_handle_data(engine, data, recv_us, now_us)``
    """

    venue: str = ""

    def __init__(self, *, uri: str, hb_timeout: float = 90.0, ping_interval: float = 30.0) -> None:
        self.uri = uri
        self._parser = _JSONParser()
        self._sub_counter = 0
        self._active_sub_ids: set[str] = set()
        self._connected = False
        self._hb_timeout = hb_timeout
        self._ping_interval = ping_interval
        now = time.monotonic()
        self._last_ping = now

    # ------------------------------------------------------------------
    # WsConnector protocol implementation
    # ------------------------------------------------------------------

    def on_connect(self, engine: Any) -> None:
        """Send connection_init and reset protocol state after websocket connect."""

        now = time.monotonic()
        engine._hb_last = now
        self._last_ping = now
        self._connected = False
        self._active_sub_ids.clear()
        engine._ws.send(_CONN_INIT)

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None:
        """Send a GraphQL subscription for the requested products."""

        query, variables = self._build_subscription(product_ids)
        sub_id = self._next_sub_id()
        self._active_sub_ids.add(sub_id)
        engine._ws.send(
            orjson.dumps(
                {
                    "id": sub_id,
                    "type": "subscribe",
                    "payload": {"query": query, "variables": variables},
                }
            )
        )

    def send_unsubscribe(self, engine: Any, targets: Sequence[str]) -> None:
        """Complete all active subscriptions."""

        for sub_id in list(self._active_sub_ids):
            try:
                engine._ws.send(orjson.dumps({"id": sub_id, "type": "complete"}))
            except Exception:
                pass
        self._active_sub_ids.clear()

    def on_timeout(self, engine: Any) -> None:
        """Send graphql-transport-ws ping and reconnect on heartbeat timeout."""

        now_mono = time.monotonic()
        hb_age = now_mono - engine._hb_last
        if hb_age > self._hb_timeout:
            raise WebSocketConnectionClosedException(
                f"heartbeat timeout ({hb_age:.1f}s > {self._hb_timeout:.1f}s)"
            )
        if now_mono - self._last_ping >= self._ping_interval:
            try:
                engine._ws.send(orjson.dumps({"type": "ping"}))
            except Exception:
                pass
            self._last_ping = now_mono

    def handle_raw(self, engine: Any, raw: bytes | str, recv_us: int, now_us: Callable[[], int]) -> None:
        """Dispatch graphql-transport-ws messages and forward data to subclass handler."""

        try:
            doc = self._parser.parse(raw).as_dict()
        except Exception as e:
            engine.log.warning("JSON decode error: %s", e)
            return

        if not isinstance(doc, dict):
            return

        msg_type = str(doc.get("type") or "").strip()

        if msg_type == "connection_ack":
            engine._hb_last = time.monotonic()
            self._connected = True
            return

        if msg_type == "ping":
            try:
                engine._ws.send(orjson.dumps({"type": "pong"}))
            except Exception:
                pass
            engine._hb_last = time.monotonic()
            return

        if msg_type == "pong":
            engine._hb_last = time.monotonic()
            return

        if msg_type == "error":
            engine.log.error("GraphQL WS error: %r", doc)
            return

        if msg_type == "next":
            engine._hb_last = time.monotonic()
            payload = doc.get("payload") or {}
            if not isinstance(payload, dict):
                return
            data = payload.get("data") or {}
            if isinstance(data, dict) and data:
                self._handle_data(engine, data, recv_us, now_us)
            return

        if msg_type == "complete":
            sub_id = str(doc.get("id") or "")
            self._active_sub_ids.discard(sub_id)
            return

    def extra_status(self, _engine: Any) -> dict[str, Any]:
        """Return GraphQL-WS connector-specific runtime status fields."""

        return {
            "uri": self.uri,
            "connected": self._connected,
            "active_subs": len(self._active_sub_ids),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _next_sub_id(self) -> str:
        self._sub_counter += 1
        return str(self._sub_counter)

    def _build_subscription(self, product_ids: Sequence[str]) -> tuple[str, dict]:
        """Return (graphql_query, variables) for the given products.

        Override in subclasses to provide the venue-specific subscription.
        """

        raise NotImplementedError(f"{type(self).__name__} must implement _build_subscription")

    def _handle_data(self, engine: Any, data: dict, recv_us: int, now_us: Callable[[], int]) -> None:
        """Process the data dict from a graphql-transport-ws 'next' message.

        Override in subclasses to parse venue-specific response shape and
        write to engine feed writers.
        """

        raise NotImplementedError(f"{type(self).__name__} must implement _handle_data")
