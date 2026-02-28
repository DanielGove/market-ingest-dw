"""Strategy contracts and runtime boundary helpers."""
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Callable, List, Literal, Optional, Protocol

from backtest.feeds import execution_feed_name

Side = Literal["buy", "sell"]
TIF = Literal["GTC", "IOC", "FOK", "PO"]
OrderType = Literal["limit", "market"]


@dataclass
class OrderIntent:
    side: Side
    price: float
    size: float
    tif: TIF = "PO"
    order_type: OrderType = "limit"
    client_tag: str = ""
    client_id: Optional[str] = None
    product_id: Optional[str] = None


@dataclass(frozen=True)
class Subscription:
    feed: str
    base_path: str
    method: str


OutputRole = Literal["intent"]


@dataclass(frozen=True)
class OutputFeed:
    role: OutputRole
    feed: str
    base_path: str
    product_id: Optional[str] = None


class Strategy(Protocol):
    def subscriptions(self) -> List[Subscription]:
        ...

    def outputs(self) -> List[OutputFeed]:
        ...


def default_market_base_path() -> str:
    if Path("/deepwater/data").is_dir():
        return "/deepwater/data/coinbase-advanced"
    root = Path(__file__).resolve().parents[1]
    return str(root / "data" / "coinbase-main")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except Exception:
        return int(default)


def _norm_product_id(product_id: str) -> str:
    return str(product_id).strip().upper()


def build_default_subscriptions(
    *,
    strategy_name: str,
    product_id: str,
    market_base_path: str,
    io_base_path: str,
    ob_prefix: str = "OB",
    ob_depth: int = 256,
    ob_period: int = 100,
    trades_prefix: str = "CB-TRADES",
    include_trades: bool = True,
    include_status: bool = True,
) -> List[Subscription]:
    product = _norm_product_id(product_id)
    out: List[Subscription] = [
        Subscription(
            feed=f"{ob_prefix}{int(ob_depth)}{int(ob_period)}-{product}",
            base_path=market_base_path,
            method="on_snapshot",
        )
    ]
    if include_trades:
        out.append(
            Subscription(
                feed=f"{trades_prefix}-{product}",
                base_path=market_base_path,
                method="on_trade",
            )
        )
    if include_status:
        out.append(
            Subscription(
                feed=execution_feed_name(strategy_name),
                base_path=io_base_path,
                method="on_status",
            )
        )
    return out


class EventDrivenStrategy:
    """Low-boilerplate strategy base with overridable feed profile.

    New strategies can subclass this and implement on_snapshot/on_trade/on_status
    while inheriting standard subscriptions()/outputs().
    """

    def __init__(
        self,
        *,
        product: str = "BTC-USD",
        market_base_path: Optional[str] = None,
        io_base_path: str = "data/strategy-intents",
        ob_prefix: Optional[str] = None,
        ob_depth: Optional[int] = None,
        ob_period: Optional[int] = None,
        trades_prefix: Optional[str] = None,
        include_trades: bool = True,
        include_status: bool = True,
    ) -> None:
        self.product = _norm_product_id(product)
        self.market_base_path = market_base_path or os.environ.get("BT_MARKET_BASE_PATH") or default_market_base_path()
        self.io_base_path = io_base_path or os.environ.get("BT_IO_BASE_PATH") or "data/strategy-intents"
        self.ob_prefix = ob_prefix or os.environ.get("BT_OB_PREFIX") or "OB"
        self.ob_depth = int(ob_depth if ob_depth is not None else _env_int("BT_OB_DEPTH", 256))
        self.ob_period = int(ob_period if ob_period is not None else _env_int("BT_OB_PERIOD", 100))
        self.trades_prefix = trades_prefix or os.environ.get("BT_TRADES_PREFIX") or "CB-TRADES"
        self.include_trades = bool(include_trades)
        self.include_status = bool(include_status)

    def subscriptions(self) -> List[Subscription]:
        return build_default_subscriptions(
            strategy_name=self.__class__.__name__,
            product_id=self.product,
            market_base_path=self.market_base_path,
            io_base_path=self.io_base_path,
            ob_prefix=self.ob_prefix,
            ob_depth=self.ob_depth,
            ob_period=self.ob_period,
            trades_prefix=self.trades_prefix,
            include_trades=self.include_trades,
            include_status=self.include_status,
        )

    def outputs(self) -> List[OutputFeed]:
        return [
            OutputFeed(
                role="intent",
                feed=f"STRAT-ORDERS-{self.__class__.__name__.upper()}",
                base_path=self.io_base_path,
                product_id=self.product,
            )
        ]

    def on_snapshot(self, snapshot: tuple) -> List[Any]:  # pragma: no cover - interface stub
        return []

    def on_trade(self, trade: tuple) -> List[Any]:  # pragma: no cover - interface stub
        return []

    def on_status(self, record: tuple) -> List[Any]:  # pragma: no cover - interface stub
        return []


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
            product_id=value.get("product_id"),
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


def resolve_strategy_method(strategy: Any, method: str) -> Callable[[tuple], Any]:
    cb = getattr(strategy, method, None)
    if not callable(cb):
        raise TypeError(f"strategy is missing method '{method}(record)'")
    return cb
