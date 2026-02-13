"""Phase 0 contracts for strategy backtesting.

This module freezes event shapes, status transitions, and deterministic ordering rules.
It is intentionally lightweight so Phase 1 can build on stable interfaces.
"""
from dataclasses import dataclass
from typing import Literal, Optional, Tuple


SCHEMA_VERSION = 1

EventKind = Literal["market", "execution", "intent"]
IntentAction = Literal["PLACE", "CANCEL", "REPLACE"]
IntentSide = Literal["B", "S"]
OrderType = Literal["L", "M"]
TIF = Literal["GTC", "IOC", "FOK", "PO"]
ExecutionStatus = Literal["ACK", "REJECT", "PARTIAL_FILL", "FILL", "CANCEL"]
LiquidityFlag = Literal["M", "T", "U"]

TERMINAL_EXEC_STATUSES = {"REJECT", "FILL", "CANCEL"}

# Merge order when event_time is identical.
EVENT_KIND_PRIORITY = {
    "market": 0,
    "execution": 1,
    "intent": 2,
}


@dataclass(frozen=True)
class BaseEvent:
    event_id: str
    schema_version: int
    event_time: int
    processed_time: int
    product_id: str


@dataclass(frozen=True)
class IntentEvent(BaseEvent):
    strategy_id: str
    run_id: str
    client_order_id: str
    action: IntentAction
    side: Optional[IntentSide] = None
    order_type: Optional[OrderType] = None
    price: Optional[float] = None
    size: float = 0.0
    tif: Optional[TIF] = None
    client_tag: str = ""


@dataclass(frozen=True)
class ExecutionEvent(BaseEvent):
    strategy_id: str
    run_id: str
    client_order_id: str
    exchange_order_id: str
    status: ExecutionStatus
    filled_size_delta: float = 0.0
    fill_price: Optional[float] = None
    cum_filled_size: float = 0.0
    remaining_size: float = 0.0
    fee: float = 0.0
    liquidity_flag: LiquidityFlag = "U"
    reason_code: str = ""


def event_sort_key(kind: EventKind, event_time: int, event_id: str, source_seq: int = 0) -> Tuple[int, int, int, str]:
    """Stable sort key for scheduler merges.

    Ordering: event_kind priority, event_time, source_seq, event_id.
    """
    if kind not in EVENT_KIND_PRIORITY:
        raise ValueError(f"unknown event kind: {kind}")
    return (EVENT_KIND_PRIORITY[kind], event_time, source_seq, event_id)


def validate_base_event(event: BaseEvent) -> None:
    if event.schema_version != SCHEMA_VERSION:
        raise ValueError(
            f"schema_version {event.schema_version} != expected {SCHEMA_VERSION}"
        )
    if event.event_time < 0 or event.processed_time < 0:
        raise ValueError("timestamps must be uint64-compatible")
    if not event.product_id or event.product_id != event.product_id.upper():
        raise ValueError("product_id must be uppercase canonical symbol (e.g. BTC-USD)")
    if not event.event_id:
        raise ValueError("event_id is required")


def validate_intent_event(event: IntentEvent) -> None:
    validate_base_event(event)
    if not event.strategy_id or not event.run_id:
        raise ValueError("strategy_id and run_id are required")
    if not event.client_order_id:
        raise ValueError("client_order_id is required")
    if event.action in ("PLACE", "REPLACE"):
        if event.side not in ("B", "S"):
            raise ValueError("PLACE/REPLACE require side=B|S")
        if event.order_type not in ("L", "M"):
            raise ValueError("PLACE/REPLACE require order_type=L|M")
        if event.size <= 0:
            raise ValueError("PLACE/REPLACE require size > 0")
        if event.order_type == "L" and (event.price is None or event.price <= 0):
            raise ValueError("limit orders require positive price")
    if event.action == "CANCEL" and event.size not in (0.0, 0):
        raise ValueError("CANCEL intent must not carry a non-zero size")


def validate_execution_event(event: ExecutionEvent) -> None:
    validate_base_event(event)
    if not event.strategy_id or not event.run_id:
        raise ValueError("strategy_id and run_id are required")
    if not event.client_order_id:
        raise ValueError("client_order_id is required")
    if event.status in ("PARTIAL_FILL", "FILL"):
        if event.fill_price is None or event.fill_price <= 0:
            raise ValueError("fill events require positive fill_price")
        if event.filled_size_delta <= 0:
            raise ValueError("fill events require filled_size_delta > 0")
    if event.status in ("REJECT", "CANCEL") and not event.reason_code:
        raise ValueError("REJECT/CANCEL require reason_code")
    if event.fee < 0:
        raise ValueError("fee must be >= 0")


def validate_status_transition(prev: Optional[ExecutionStatus], nxt: ExecutionStatus) -> None:
    """Enforce monotonic lifecycle for a single client_order_id."""
    if prev is None:
        return
    if prev in TERMINAL_EXEC_STATUSES:
        raise ValueError(f"invalid transition from terminal state {prev} -> {nxt}")
    if prev == "ACK" and nxt not in ("PARTIAL_FILL", "FILL", "CANCEL", "REJECT"):
        raise ValueError(f"invalid transition ACK -> {nxt}")
    if prev == "PARTIAL_FILL" and nxt not in ("PARTIAL_FILL", "FILL", "CANCEL"):
        raise ValueError(f"invalid transition PARTIAL_FILL -> {nxt}")
