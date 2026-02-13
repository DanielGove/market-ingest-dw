from dataclasses import dataclass
from typing import Optional, Literal, List, Protocol

Side = Literal["buy", "sell"]
TIF = Literal["GTC", "IOC", "FOK", "PO"]
OrderType = Literal["limit", "market"]


@dataclass
class OrderIntent:
    side: Side
    price: float
    size: float
    tif: TIF = "PO"  # default to post-only (maker)
    order_type: OrderType = "limit"
    client_tag: str = ""
    client_id: Optional[str] = None  # will be assigned by tracker/router


class Strategy(Protocol):
    def on_snapshot(self, snapshot: tuple, meta: dict) -> List[OrderIntent]:
        ...

    def on_status(self, status: dict) -> None:
        ...
