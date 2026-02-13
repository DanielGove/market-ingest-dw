"""Execution feed helpers for Phase 0 contract freeze.

This defines a dedicated Deepwater execution feed for strategy outcomes.
The simulator or live adapter will write events to this feed in later phases.
"""
from deepwater.platform import Platform


EXECUTION_FIELDS = (
    ("event_time", "uint64"),
    ("processed_time", "uint64"),
    ("event_id", "bytes16"),
    ("schema_version", "uint64"),
    ("product_id", "bytes16"),
    ("strategy_id", "bytes16"),
    ("run_id", "bytes16"),
    ("client_order_id", "bytes16"),
    ("exchange_order_id", "bytes16"),
    ("status", "bytes16"),
    ("side", "char"),
    ("fill_price", "float64"),
    ("filled_size_delta", "float64"),
    ("cum_filled_size", "float64"),
    ("remaining_size", "float64"),
    ("fee", "float64"),
    ("liquidity_flag", "char"),
    ("reason_code", "bytes16"),
)


def execution_feed_name(strategy_name: str) -> str:
    return f"STRAT-EXEC-{strategy_name.upper()}"


def ensure_execution_feed(plat: Platform, strategy_name: str) -> str:
    feed_name = execution_feed_name(strategy_name)
    spec = {
        "feed_name": feed_name,
        "mode": "UF",
        "fields": [{"name": n, "type": t} for n, t in EXECUTION_FIELDS],
        "persist": True,
        "clock_level": 2,
    }
    plat.create_feed(spec)
    return feed_name


def get_execution_writer(plat: Platform, strategy_name: str):
    feed_name = ensure_execution_feed(plat, strategy_name)
    return plat.create_writer(feed_name)
