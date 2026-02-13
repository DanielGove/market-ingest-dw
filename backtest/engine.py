#!/usr/bin/env python3
"""Step 2 engine (Deepwater-native).

This run mode is intentionally simple:
- stream OB + trades by processed_time
- splice by earliest processed timestamp
- call strategy methods
- publish intents
- run a simple exchange splicer:
  - intents arrive at exchange at (intent.processed_time + latency)
  - trades stream on event_time
  - intersecting trades fill resting limits
  - execution processed_time = fill event_time + reverse latency
"""
import argparse
import hashlib
import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

# ensure project root on path before local imports
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from deepwater.platform import Platform

from backtest import ping_pong
from backtest.intent_feed import get_intent_writer
from backtest.execution_feed import get_execution_writer
from backtest.state_tracker import OrderStateTracker
from backtest.strategy_boundary import call_strategy_on_snapshot, call_strategy_on_status, normalize_snapshot_output


def load_strategy(name: str) -> Any:
    if ":" in name:
        mod_name, cls_name = name.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, cls_name)()
    if name == "ping_pong":
        return ping_pong.PingPong()
    raise ValueError(f"unknown strategy {name}")


def _strategy_feed_key(name: str) -> str:
    key = name.split(":", 1)[1] if ":" in name else name
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in key)


def _fixed_bytes(value: str, size: int) -> bytes:
    return value.encode("utf-8").ljust(size, b"\0")


def _product_bytes(product: str) -> bytes:
    return product.upper().encode("utf-8").ljust(16, b"\0")


def _intent_processed_us(base_processed_us: int, seq: int) -> int:
    return base_processed_us + seq + 1


def _git_commit_or_unknown() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out if out else "unknown"
    except Exception:
        return "unknown"


def _write_manifest(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, sort_keys=True, indent=2)


def _detect_proc_scale(sample_raw: int) -> int:
    # Deepwater contracts use microseconds, but OB processed_time may be ns in older feeds.
    return 1000 if sample_raw > 10**17 else 1


def _to_proc_us(raw_value: int, scale: int) -> int:
    return int(raw_value) // scale


def _normalize_trade_side(raw) -> str:
    tok = raw.decode("utf-8", errors="ignore").upper() if isinstance(raw, bytes) else str(raw).upper()
    if tok in ("B", "BUY"):
        return "buy"
    if tok in ("S", "SELL"):
        return "sell"
    return tok


def _decode_token(value) -> str:
    if isinstance(value, bytes):
        return value.split(b"\0", 1)[0].decode("utf-8", errors="ignore").strip()
    return str(value).strip()


def _to_side_char(raw) -> str:
    tok = _decode_token(raw).upper()
    if tok in ("B", "BUY"):
        return "B"
    if tok in ("S", "SELL"):
        return "S"
    raise ValueError(f"unknown side token: {raw}")


def _to_type_char(raw) -> str:
    tok = _decode_token(raw).upper()
    if tok in ("L", "LIMIT"):
        return "L"
    if tok in ("M", "MARKET"):
        return "M"
    raise ValueError(f"unknown type token: {raw}")


def _to_tif(raw) -> str:
    tok = _decode_token(raw).upper()
    return tok if tok in ("GTC", "IOC", "FOK", "PO") else "PO"


def _next_in_window(
    stream: Iterator[tuple],
    idx_proc: int,
    idx_event: int,
    start_proc_us: int,
    end_proc_us: int,
    proc_scale: int,
) -> Optional[Tuple[tuple, int, int]]:
    while True:
        try:
            rec = next(stream)
        except StopIteration:
            return None
        processed_us = _to_proc_us(rec[idx_proc], proc_scale)
        if processed_us < start_proc_us:
            continue
        if processed_us > end_proc_us:
            return None
        event_us = int(rec[idx_event])
        return rec, event_us, processed_us


def run(args):
    strategy_key = _strategy_feed_key(args.strategy)
    product = args.product.upper()
    strat = load_strategy(args.strategy)
    playback = bool(getattr(args, "playback", False))
    decision_latency_us = int(getattr(args, "decision_latency_us", 0))
    fixed_start_proc_us = getattr(args, "window_start_processed_us", None)
    fixed_end_proc_us = getattr(args, "window_end_processed_us", None)

    intent_plat = Platform(base_path=args.intent_base_path)
    intent_writer = get_intent_writer(intent_plat, strategy_key)
    execution_plat = Platform(base_path=args.execution_base_path)
    execution_writer = get_execution_writer(execution_plat, strategy_key)
    prod_bytes = _product_bytes(product)

    market_plat = Platform(base_path=args.base_path)
    ob_feed = f"OB{args.depth}{args.period}-{product}"
    tr_feed = f"CB-TRADES-{product}"
    ob_reader = market_plat.create_reader(ob_feed)
    tr_reader = market_plat.create_reader(tr_feed)

    ob_names = ob_reader.field_names
    tr_names = tr_reader.field_names
    idx_ob_event = ob_names.index("snapshot_time") if "snapshot_time" in ob_names else ob_names.index("snapshot_us")
    idx_ob_proc = ob_names.index("processed_time") if "processed_time" in ob_names else ob_names.index("processed_us")
    idx_ob_bid0 = ob_names.index("bid_price_0")
    idx_ob_ask0 = ob_names.index("ask_price_0")

    idx_tr_event = tr_names.index("event_time")
    idx_tr_proc = tr_names.index("processed_time")
    idx_tr_side = tr_names.index("side")
    idx_tr_price = tr_names.index("price")
    idx_tr_size = tr_names.index("size")

    ob_latest = ob_reader.latest(seconds=max(1, args.seconds))
    if not ob_latest:
        print(f"No OB data in last {args.seconds}s for {ob_feed}")
        return

    ob_proc_scale = _detect_proc_scale(int(ob_latest[-1][idx_ob_proc]))
    tr_latest = tr_reader.latest(seconds=max(1, args.seconds))
    tr_proc_scale = _detect_proc_scale(int(tr_latest[-1][idx_tr_proc])) if tr_latest else 1

    if fixed_end_proc_us is None:
        end_proc_us = _to_proc_us(ob_latest[-1][idx_ob_proc], ob_proc_scale)
    else:
        end_proc_us = int(fixed_end_proc_us)
    if fixed_start_proc_us is None:
        start_proc_us = end_proc_us - int(args.seconds * 1_000_000.0)
    else:
        start_proc_us = int(fixed_start_proc_us)

    ob_stream = ob_reader.stream(
        start=start_proc_us * ob_proc_scale,
        ts_key="processed_time" if "processed_time" in ob_names else "processed_us",
        playback=playback,
    )
    tr_stream = tr_reader.stream(
        start=start_proc_us * tr_proc_scale,
        ts_key="processed_time",
        playback=playback,
    )

    next_ob = _next_in_window(
        ob_stream,
        idx_proc=idx_ob_proc,
        idx_event=idx_ob_event,
        start_proc_us=start_proc_us,
        end_proc_us=end_proc_us,
        proc_scale=ob_proc_scale,
    )
    next_tr = _next_in_window(
        tr_stream,
        idx_proc=idx_tr_proc,
        idx_event=idx_tr_event,
        start_proc_us=start_proc_us,
        end_proc_us=end_proc_us,
        proc_scale=tr_proc_scale,
    )

    stream_checksum = hashlib.sha256()
    intent_checksum = hashlib.sha256()
    intent_records = 0
    total_events = 0
    snapshot_events = 0
    trade_events = 0
    saw_snapshot = False
    first_proc: Optional[int] = None
    last_proc: Optional[int] = None
    first_event: Optional[int] = None
    last_event: Optional[int] = None
    exchange_trade_rows: List[Tuple[int, str, float, float]] = []
    min_intent_proc_us: Optional[int] = None
    max_intent_proc_us: Optional[int] = None

    def _emit_intents(event_us: int, processed_base_us: int, intents: list) -> None:
        nonlocal intent_records, min_intent_proc_us, max_intent_proc_us
        for i, it in enumerate(intents):
            proc_us = _intent_processed_us(processed_base_us, i)
            side = "B" if it.side == "buy" else "S"
            typ = "L" if it.order_type == "limit" else "M"
            tif = it.tif
            client_tag = it.client_tag or f"A{intent_records + 1:07d}"
            payload = {
                "event_time": int(event_us),
                "processed_time": int(proc_us),
                "product_id": product,
                "side": side,
                "type": typ,
                "price": float(it.price),
                "size": float(it.size),
                "tif": tif,
                "client_tag": client_tag,
            }
            if min_intent_proc_us is None or proc_us < min_intent_proc_us:
                min_intent_proc_us = proc_us
            if max_intent_proc_us is None or proc_us > max_intent_proc_us:
                max_intent_proc_us = proc_us
            intent_checksum.update(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            intent_records += 1
            intent_writer.write_values(
                int(event_us),
                int(proc_us),
                prod_bytes,
                side.encode("utf-8"),
                typ.encode("utf-8"),
                it.price,
                it.size,
                _fixed_bytes(tif, 8),
                _fixed_bytes(client_tag, 8),
            )

    def _choose_lane() -> Optional[str]:
        candidates: List[Tuple[int, int, str]] = []
        if next_ob is not None:
            # Priority 0: snapshot (tie-break first at same processed time).
            candidates.append((next_ob[2], 0, "snapshot"))
        if next_tr is not None:
            # Priority 1: trade.
            candidates.append((next_tr[2], 1, "trade"))
        if not candidates:
            return None
        candidates.sort()
        return candidates[0][2]

    while next_ob is not None or next_tr is not None:
        lane = _choose_lane()
        if lane == "snapshot":
            rec, event_us, processed_us = next_ob
            saw_snapshot = True
            snapshot_events += 1
            total_events += 1
            if first_proc is None:
                first_proc = processed_us
            last_proc = processed_us
            if first_event is None:
                first_event = event_us
            last_event = event_us

            bid0 = rec[idx_ob_bid0]
            ask0 = rec[idx_ob_ask0]
            mid = (bid0 + ask0) / 2 if bid0 and ask0 else None
            meta = {
                "mid": mid,
                "best_bid": bid0,
                "best_ask": ask0,
                "snapshot_us": event_us,
                "processed_us": processed_us,
                "feed": ob_feed,
            }
            stream_checksum.update(
                f"snapshot|{event_us}|{processed_us}|{float(bid0):.10f}|{float(ask0):.10f}\n".encode("utf-8")
            )
            intents = call_strategy_on_snapshot(
                strat,
                rec,
                meta,
                strict_contracts=args.strict_contracts,
            )
            _emit_intents(
                event_us=event_us,
                processed_base_us=processed_us + decision_latency_us,
                intents=intents,
            )
            next_ob = _next_in_window(
                ob_stream,
                idx_proc=idx_ob_proc,
                idx_event=idx_ob_event,
                start_proc_us=start_proc_us,
                end_proc_us=end_proc_us,
                proc_scale=ob_proc_scale,
            )
            continue

        if lane == "trade":
            rec, event_us, processed_us = next_tr
            trade_events += 1
            total_events += 1
            if first_proc is None:
                first_proc = processed_us
            last_proc = processed_us
            if first_event is None:
                first_event = event_us
            last_event = event_us
            side = _normalize_trade_side(rec[idx_tr_side])
            price = float(rec[idx_tr_price])
            size = float(rec[idx_tr_size])
            exchange_trade_rows.append((event_us, side, price, size))
            stream_checksum.update(
                f"trade|{event_us}|{processed_us}|{side}|{price:.10f}|{size:.10f}\n".encode("utf-8")
            )

            # Optional trade callback for strategies that support it.
            trade_cb = getattr(strat, "on_trade", None)
            if callable(trade_cb):
                raw = trade_cb(
                    rec,
                    {
                        "event_us": event_us,
                        "processed_us": processed_us,
                        "side": side,
                        "price": price,
                        "size": size,
                        "feed": tr_feed,
                    },
                )
                intents = normalize_snapshot_output(raw, strict_contracts=args.strict_contracts)
                _emit_intents(
                    event_us=event_us,
                    processed_base_us=processed_us + decision_latency_us,
                    intents=intents,
                )

            next_tr = _next_in_window(
                tr_stream,
                idx_proc=idx_tr_proc,
                idx_event=idx_tr_event,
                start_proc_us=start_proc_us,
                end_proc_us=end_proc_us,
                proc_scale=tr_proc_scale,
            )
            continue

        # Defensive guard for impossible states.
        raise RuntimeError(f"unknown splice lane: {lane}")

    # Seal intent writes before exchange reads from the same Deepwater feed.
    intent_writer.close()

    # Exchange splicer (Step 2): merge intent-arrivals with trade events on exchange time.
    status_counts: Dict[str, int] = {}
    execution_checksum = hashlib.sha256()
    execution_records = 0
    status_callbacks = 0
    tracker = OrderStateTracker()
    forward_latency_us = int(args.latency_ms * 1_000.0)
    reverse_latency_us = int(args.reverse_latency_ms * 1_000.0)
    maker_fee_rate = float(args.maker_fee_bps) / 10_000.0
    taker_fee_rate = float(args.fee_bps) / 10_000.0
    working_orders: Dict[str, Dict[str, float]] = {}
    order_seq = 0
    strategy_id = strategy_key.upper()[:16]
    run_id = str(args.run_id).upper()[:16]
    execution_strategy_events: List[Tuple[int, Dict[str, Any]]] = []

    if intent_records > 0 and first_event is not None and last_event is not None:
        intent_feed_name = f"STRAT-ORDERS-{strategy_key.upper()}"
        intent_reader = intent_plat.create_reader(intent_feed_name)
        intent_fields = intent_reader.field_names
        idx_i_event = intent_fields.index("event_time")
        idx_i_proc = intent_fields.index("processed_time")
        idx_i_product = intent_fields.index("product_id")
        idx_i_side = intent_fields.index("side")
        idx_i_type = intent_fields.index("type")
        idx_i_price = intent_fields.index("price")
        idx_i_size = intent_fields.index("size")
        idx_i_tif = intent_fields.index("tif")
        idx_i_tag = intent_fields.index("client_tag")

        min_intent_proc = int(min_intent_proc_us) if min_intent_proc_us is not None else 0
        max_intent_proc = int(max_intent_proc_us) if max_intent_proc_us is not None else -1

        intent_events: List[Dict[str, Any]] = []
        intent_records_window = intent_reader.latest(seconds=max(1, int(args.seconds) + 2))
        for rec in intent_records_window:
            proc_us = int(rec[idx_i_proc])
            if proc_us < min_intent_proc:
                continue
            if proc_us > max_intent_proc:
                continue
            event_us = int(rec[idx_i_event])
            product_id = _decode_token(rec[idx_i_product]).upper()
            if product_id != product:
                continue
            side = _to_side_char(rec[idx_i_side])
            typ = _to_type_char(rec[idx_i_type])
            price = float(rec[idx_i_price])
            size = float(rec[idx_i_size])
            tif = _to_tif(rec[idx_i_tif])
            client_tag = _decode_token(rec[idx_i_tag])
            intent_events.append(
                {
                    "arrival_us": proc_us + forward_latency_us,
                    "event_us": event_us,
                    "processed_us": proc_us,
                    "side": side,
                    "type": typ,
                    "price": price,
                    "size": size,
                    "tif": tif,
                    "client_tag": client_tag,
                }
            )

        intent_events.sort(key=lambda x: (x["arrival_us"], x["processed_us"], x["event_us"], x["client_tag"]))
        exchange_trade_rows.sort(key=lambda x: x[0])
        next_intent_idx = 0
        next_trade_idx = 0

        def _emit_execution(
            order: Dict[str, float],
            fill_size: float,
            event_us: int,
            fill_price: float,
            liquidity_flag: str,
        ) -> None:
            nonlocal execution_records
            status = "FILL" if order["remaining"] <= 1e-12 else "PARTIAL_FILL"
            status_counts[status] = status_counts.get(status, 0) + 1
            fee_rate = maker_fee_rate if liquidity_flag == "M" else taker_fee_rate
            fee = abs(fill_price * fill_size) * fee_rate
            cum_filled = order["size"] - order["remaining"]
            proc_us = int(event_us + reverse_latency_us)
            event_id = f"E{execution_records + 1:015d}"
            exchange_order_id = f"X{int(order['seq']):015d}"
            payload = {
                "event_time": int(event_us),
                "processed_time": proc_us,
                "event_id": event_id,
                "schema_version": 1,
                "product_id": product,
                "strategy_id": strategy_id,
                "run_id": run_id,
                "client_order_id": order["client_id"],
                "exchange_order_id": exchange_order_id,
                "status": status,
                "side": order["side"],
                "fill_price": float(fill_price),
                "filled_size_delta": float(fill_size),
                "cum_filled_size": float(cum_filled),
                "remaining_size": float(order["remaining"]),
                "fee": float(fee),
                "liquidity_flag": liquidity_flag,
                "reason_code": "",
            }
            status_payload = {
                "type": "fill",
                "status": status.lower(),
                "event_us": int(event_us),
                "processed_us": proc_us,
                "client_id": order["client_id"],
                "exchange_order_id": exchange_order_id,
                "side": "buy" if order["side"] == "B" else "sell",
                "price": float(fill_price),
                "size": float(fill_size),
                "cum_filled": float(cum_filled),
                "remaining": float(order["remaining"]),
                "fee": float(fee),
                "liquidity_flag": liquidity_flag,
            }
            execution_checksum.update(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            execution_records += 1
            execution_strategy_events.append((proc_us, status_payload))
            execution_writer.write_values(
                int(event_us),
                proc_us,
                _fixed_bytes(event_id, 16),
                1,
                _fixed_bytes(product, 16),
                _fixed_bytes(strategy_id, 16),
                _fixed_bytes(run_id, 16),
                _fixed_bytes(order["client_id"], 16),
                _fixed_bytes(exchange_order_id, 16),
                _fixed_bytes(status, 16),
                order["side"].encode("utf-8"),
                float(fill_price),
                float(fill_size),
                float(cum_filled),
                float(order["remaining"]),
                float(fee),
                liquidity_flag.encode("utf-8"),
                _fixed_bytes("", 16),
            )
            tracker.apply_fill(
                {
                    "type": "fill",
                    "side": "buy" if order["side"] == "B" else "sell",
                    "size": float(fill_size),
                    "price": float(fill_price),
                    "fee": float(fee),
                }
            )

        while next_trade_idx < len(exchange_trade_rows) or next_intent_idx < len(intent_events):
            next_intent = intent_events[next_intent_idx] if next_intent_idx < len(intent_events) else None
            next_trade = exchange_trade_rows[next_trade_idx] if next_trade_idx < len(exchange_trade_rows) else None
            if next_intent is not None and (next_trade is None or next_intent["arrival_us"] <= next_trade[0]):
                if next_intent["size"] > 0 and next_intent["type"] in ("L", "M"):
                    order_seq += 1
                    client_id = next_intent["client_tag"] or f"C{order_seq:07d}"
                    working_orders[client_id] = {
                        "seq": float(order_seq),
                        "client_id": client_id,
                        "side": next_intent["side"],
                        "type": next_intent["type"],
                        "price": float(next_intent["price"]),
                        "size": float(next_intent["size"]),
                        "remaining": float(next_intent["size"]),
                    }
                next_intent_idx += 1
                continue

            event_us, trade_side, trade_price, trade_size = next_trade
            remaining_trade = float(trade_size)
            if remaining_trade > 0 and working_orders:
                orders = list(working_orders.values())
                if trade_side == "buy":
                    candidates = [
                        o
                        for o in orders
                        if o["side"] == "S"
                        and o["remaining"] > 0
                        and (o["type"] == "M" or o["price"] <= trade_price)
                    ]
                    candidates.sort(key=lambda o: (0 if o["type"] == "M" else 1, o["price"], o["seq"]))
                else:
                    candidates = [
                        o
                        for o in orders
                        if o["side"] == "B"
                        and o["remaining"] > 0
                        and (o["type"] == "M" or o["price"] >= trade_price)
                    ]
                    candidates.sort(key=lambda o: (0 if o["type"] == "M" else 1, -o["price"], o["seq"]))
                for order in candidates:
                    if remaining_trade <= 0:
                        break
                    fill_size = min(order["remaining"], remaining_trade)
                    if fill_size <= 0:
                        continue
                    order["remaining"] -= fill_size
                    remaining_trade -= fill_size
                    fill_price = trade_price if order["type"] == "M" else order["price"]
                    liquidity_flag = "T" if order["type"] == "M" else "M"
                    _emit_execution(
                        order,
                        fill_size=fill_size,
                        event_us=event_us,
                        fill_price=fill_price,
                        liquidity_flag=liquidity_flag,
                    )
                    if order["remaining"] <= 1e-12:
                        working_orders.pop(order["client_id"], None)
            next_trade_idx += 1

    # Strategy execution splice (processed-time ordering).
    execution_strategy_events.sort(
        key=lambda pair: (
            pair[0],
            int(pair[1]["event_us"]),
            str(pair[1]["client_id"]),
            str(pair[1]["exchange_order_id"]),
        )
    )
    for _, status_event in execution_strategy_events:
        call_strategy_on_status(strat, status_event)
        status_callbacks += 1

    summary = {
        "position": tracker.position,
        "pnl": tracker.pnl,
        "status_counts": status_counts,
        "status_callbacks": status_callbacks,
        "events": total_events,
        "snapshot_events": snapshot_events,
        "trade_events": trade_events,
        "saw_snapshot": saw_snapshot,
        "stream_checksum": stream_checksum.hexdigest(),
        "intent_feed": {
            "records": intent_records,
            "checksum": intent_checksum.hexdigest(),
        },
        "execution_feed": {
            "records": execution_records,
            "checksum": execution_checksum.hexdigest() if execution_records else hashlib.sha256(b"").hexdigest(),
        },
    }

    print(
        f"Engine done. events={total_events} snapshots={snapshot_events} trades={trade_events} "
        f"intents={intent_records}"
    )
    execution_writer.close()
    if args.manifest_path:
        cfg = dict(vars(args))
        cfg.pop("events_override", None)
        _write_manifest(
            args.manifest_path,
            {
                "run_id": args.run_id,
                "git_commit": _git_commit_or_unknown(),
                "config": cfg,
                "summary": summary,
                "window": {
                    "start_processed_us": int(first_proc) if first_proc is not None else None,
                    "end_processed_us": int(last_proc) if last_proc is not None else None,
                },
            },
        )
    return summary


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-path", default="data/coinbase-main")
    ap.add_argument("--intent-base-path", default="data/strategy-intents")
    ap.add_argument("--execution-base-path", default="data/strategy-intents")
    ap.add_argument("--product", required=True)
    ap.add_argument("--depth", type=int, default=200)
    ap.add_argument("--period", type=int, default=50)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--strategy", default="ping_pong")
    ap.add_argument("--run-id", default="RUN1")
    ap.add_argument("--latency-ms", type=float, default=0.0)
    ap.add_argument("--reverse-latency-ms", type=float, default=0.0)
    ap.add_argument("--fee-bps", type=float, default=0.0, help="Taker fee in bps.")
    ap.add_argument("--maker-fee-bps", type=float, default=0.0)
    ap.add_argument("--decision-latency-us", type=int, default=0)
    ap.add_argument("--playback", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--window-start-processed-us", type=int, default=None)
    ap.add_argument("--window-end-processed-us", type=int, default=None)
    ap.add_argument("--manifest-path", default=None)
    ap.add_argument(
        "--strict-contracts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Validate strategy output at the runtime boundary.",
    )
    return ap.parse_args()


if __name__ == "__main__":
    run(parse_args())
