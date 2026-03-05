"""Connector protocols for shared websocket ingest runtime."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol


FeedSpec = Mapping[str, Any]


class WsConnector(Protocol):
    """Contract implemented by venue websocket connectors."""

    venue: str
    uri: str

    def feed_specs(self, pid: str) -> Mapping[str, FeedSpec]:
        """Return feed specs keyed by family name for one product."""

        ...

    def on_connect(self, engine: Any) -> None:
        """Run connector-specific setup immediately after websocket connect."""

        ...

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None:
        """Send subscription messages for provided product ids."""

        ...

    def send_unsubscribe(self, engine: Any, targets: Sequence[str]) -> None:
        """Send unsubscription messages for provided product ids."""

        ...

    def on_timeout(self, engine: Any) -> None:
        """Handle websocket read timeout (heartbeat/ping checks)."""

        ...

    def handle_raw(self, engine: Any, raw: bytes | str, recv_us: int, now_us: Callable[[], int]) -> None:
        """Parse and dispatch a raw websocket payload into feed writers."""

        ...

    def extra_status(self, engine: Any) -> Mapping[str, Any]:
        """Return connector-specific runtime status fields."""

        ...
