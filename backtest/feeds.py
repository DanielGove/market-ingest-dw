"""Deepwater feed schemas and writer helpers for backtest runtime."""
from deepwater.platform import Platform


INTENT_FIELDS = (
    ("event_time", "uint64"),
    ("processed_time", "uint64"),
    ("product_id", "bytes16"),
    ("side", "char"),
    ("type", "char"),
    ("price", "float64"),
    ("size", "float64"),
    ("tif", "bytes8"),
    ("client_tag", "bytes8"),
)


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


def intent_feed_name(strategy_name: str) -> str:
    return f"STRAT-ORDERS-{strategy_name.upper()}"


def execution_feed_name(strategy_name: str) -> str:
    return f"STRAT-EXEC-{strategy_name.upper()}"


def ensure_intent_feed(plat: Platform, strategy_name: str) -> str:
    return ensure_intent_feed_named(plat, intent_feed_name(strategy_name))


def ensure_intent_feed_named(plat: Platform, feed_name: str) -> str:
    spec = {
        "feed_name": feed_name,
        "mode": "UF",
        "fields": [{"name": n, "type": t} for n, t in INTENT_FIELDS],
        "persist": True,
        "clock_level": 2,
    }
    plat.create_feed(spec)
    return feed_name


def ensure_execution_feed(plat: Platform, strategy_name: str) -> str:
    return ensure_execution_feed_named(plat, execution_feed_name(strategy_name))


def ensure_execution_feed_named(plat: Platform, feed_name: str) -> str:
    spec = {
        "feed_name": feed_name,
        "mode": "UF",
        "fields": [{"name": n, "type": t} for n, t in EXECUTION_FIELDS],
        "persist": True,
        "clock_level": 2,
    }
    plat.create_feed(spec)
    return feed_name


def get_intent_writer(plat: Platform, strategy_name: str):
    return plat.create_writer(ensure_intent_feed(plat, strategy_name))


def get_intent_writer_named(plat: Platform, feed_name: str):
    return plat.create_writer(ensure_intent_feed_named(plat, feed_name))


def get_execution_writer(plat: Platform, strategy_name: str):
    return plat.create_writer(ensure_execution_feed(plat, strategy_name))


def get_execution_writer_named(plat: Platform, feed_name: str):
    return plat.create_writer(ensure_execution_feed_named(plat, feed_name))
