"""Strategy boundary validation and normalization.

Phase 1 uses this boundary to keep strategy logic independent of feed/runtime details.
"""
from typing import Any, List

from backtest.interfaces import OrderIntent


_VALID_SIDES = {"buy", "sell"}
_VALID_TYPES = {"limit", "market"}
_VALID_TIFS = {"GTC", "IOC", "FOK", "PO"}


def _to_intent(value: Any) -> OrderIntent:
    if isinstance(value, OrderIntent):
        return value
    if isinstance(value, dict):
        return OrderIntent(
            side=value["side"],
            price=value["price"],
            size=value.get("size", 1.0),
            tif=value.get("tif", "PO"),
            order_type=value.get("order_type", "limit"),
            client_tag=value.get("client_tag", ""),
            client_id=value.get("client_id"),
        )
    raise TypeError(f"strategy must return OrderIntent|dict, got {type(value).__name__}")


def _validate_intent(intent: OrderIntent) -> None:
    if intent.side not in _VALID_SIDES:
        raise ValueError(f"invalid side: {intent.side}")
    if intent.order_type not in _VALID_TYPES:
        raise ValueError(f"invalid order_type: {intent.order_type}")
    if intent.tif not in _VALID_TIFS:
        raise ValueError(f"invalid tif: {intent.tif}")
    if intent.size <= 0:
        raise ValueError("size must be > 0")
    if intent.order_type == "limit" and intent.price <= 0:
        raise ValueError("limit order price must be > 0")
    if intent.order_type == "market" and intent.price < 0:
        raise ValueError("market order price must be >= 0")


def normalize_snapshot_output(raw: Any, strict_contracts: bool = True) -> List[OrderIntent]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise TypeError(f"on_snapshot must return list, got {type(raw).__name__}")

    intents = [_to_intent(x) for x in raw]
    if strict_contracts:
        for intent in intents:
            _validate_intent(intent)
    return intents


def call_strategy_on_snapshot(strategy: Any, snapshot: tuple, meta: dict, strict_contracts: bool = True) -> List[OrderIntent]:
    if not hasattr(strategy, "on_snapshot"):
        raise TypeError("strategy is missing on_snapshot(snapshot, meta)")
    raw = strategy.on_snapshot(snapshot, meta)
    return normalize_snapshot_output(raw, strict_contracts=strict_contracts)


def call_strategy_on_status(strategy: Any, status: dict) -> None:
    cb = getattr(strategy, "on_status", None)
    if cb is not None:
        cb(status)
