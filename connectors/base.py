"""Connector protocols for shared websocket ingest runtime."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence


FeedSpec = Mapping[str, Any]


class WsConnector(Protocol):
    venue: str
    uri: str

    def trades_spec(self, pid: str) -> FeedSpec: ...

    def l2_spec(self, pid: str) -> FeedSpec: ...

    def on_connect(self, engine: Any) -> None: ...

    def send_subscribe(self, engine: Any, product_ids: Sequence[str]) -> None: ...

    def send_unsubscribe(self, engine: Any, targets: Sequence[str]) -> None: ...

    def on_timeout(self, engine: Any) -> None: ...

    def handle_raw(self, engine: Any, raw: bytes | str, recv_us: int, now_us) -> None: ...

    def extra_status(self, engine: Any) -> Mapping[str, Any]: ...


class FamilyAwareWsConnector(WsConnector, Protocol):
    def feed_specs(self, pid: str) -> Mapping[str, FeedSpec]: ...
