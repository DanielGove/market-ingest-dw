from deepwater.platform import Platform
from typing import Tuple


INTENT_FIELDS = (
    ("event_time", "uint64"),
    ("processed_time", "uint64"),
    ("product_id", "bytes16"), # e.g. "BTC-USD"
    ("side", "char"),
    ("type", "char"),
    ("price", "float64"),
    ("size", "float64"),
    ("tif", "bytes8"),
    ("client_tag", "bytes8"),
)


def ensure_intent_feed(plat: Platform, strategy_name: str, base_dir: str = "data/strategy-intents"):
    feed_name = f"STRAT-ORDERS-{strategy_name.upper()}"
    spec = {
        "feed_name": feed_name,
        "mode": "UF",
        "fields": [
            {"name": n, "type": t} for n, t in INTENT_FIELDS
        ],
        "persist": True,
        "clock_level": 2,
    }
    plat.create_feed(spec)
    return feed_name


def get_intent_writer(plat: Platform, strategy_name: str, base_dir: str = "data/strategy-intents"):
    feed = ensure_intent_feed(plat, strategy_name, base_dir=base_dir)
    return plat.create_writer(feed)
