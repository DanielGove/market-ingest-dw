#!/usr/bin/env python3
"""Lean subscription-driven backtest harness."""
import argparse
import heapq
import importlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from deepwater.platform import Platform

root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from backtest import ping_pong, taker_pulse
from backtest.exchange import ExchangeSimulator, normalize_trade_side
from backtest.feeds import (
    execution_feed_name,
    get_intent_writer_named,
)
from backtest.segments import get_segment
from backtest.strategy import Subscription, normalize_snapshot_output, resolve_strategy_method


_STATUS_METHOD = "on_status"
_TRADE_METHOD = "on_trade"
_SNAPSHOT_METHOD = "on_snapshot"


@dataclass
class _Lane:
    subscription: Subscription
    callback: Callable[[tuple], Any]
    idx_proc: int
    idx_event: int
    idx_trade_side: Optional[int]
    idx_trade_price: Optional[int]
    idx_trade_size: Optional[int]
    proc_scale: int
    stream: Iterator[tuple]
    next_item: Optional[Tuple[tuple, int, int]]


def load_strategy(name: str) -> Any:
    if ":" in name:
        mod_name, cls_name = name.split(":", 1)
        return getattr(importlib.import_module(mod_name), cls_name)()
    if name == "ping_pong":
        return ping_pong.PingPong()
    if name == "taker_pulse":
        return taker_pulse.TakerPulse()
    raise ValueError(f"unknown strategy {name}")


def _strategy_key(strategy_obj: Any) -> str:
    raw = strategy_obj.__class__.__name__
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in raw)


def _fixed_bytes(value: str, size: int) -> bytes:
    return value.encode("utf-8").ljust(size, b"\0")


def _detect_proc_scale(sample_raw: int) -> int:
    return 1000 if sample_raw > 10**17 else 1


def _to_proc_us(raw_value: int, scale: int) -> int:
    return int(raw_value) // scale


def _processed_field(field_names: Tuple[str, ...]) -> str:
    if "processed_time" in field_names:
        return "processed_time"
    if "processed_us" in field_names:
        return "processed_us"
    raise ValueError("feed is missing processed timestamp field")


def _event_index(field_names: Tuple[str, ...], idx_proc: int) -> int:
    for name in ("event_time", "snapshot_time", "snapshot_us", "event_us"):
        if name in field_names:
            return field_names.index(name)
    return idx_proc


def _first_index(field_names: Tuple[str, ...], candidates: Tuple[str, ...]) -> Optional[int]:
    for name in candidates:
        if name in field_names:
            return field_names.index(name)
    return None


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
        return rec, int(rec[idx_event]), processed_us


def _build_lane(
    subscription: Subscription,
    callback: Callable[[tuple], Any],
    platform_cache: Dict[str, Platform],
    start_proc_us: int,
    end_proc_us: int,
) -> _Lane:
    reader = platform_cache.setdefault(subscription.base_path, Platform(base_path=subscription.base_path)).create_reader(subscription.feed)
    names = tuple(reader.field_names)
    proc_field = _processed_field(names)
    idx_proc = names.index(proc_field)
    idx_event = _event_index(names, idx_proc)
    if subscription.method == _TRADE_METHOD:
        idx_trade_side = _first_index(names, ("side", "aggressor_side", "taker_side", "maker_side"))
        idx_trade_price = _first_index(names, ("price", "trade_price", "px"))
        idx_trade_size = _first_index(names, ("size", "qty", "amount", "trade_size"))
    else:
        idx_trade_side = None
        idx_trade_price = None
        idx_trade_size = None

    latest = reader.latest(seconds=60)
    if not latest:
        return _Lane(
            subscription=subscription,
            callback=callback,
            idx_proc=idx_proc,
            idx_event=idx_event,
            idx_trade_side=idx_trade_side,
            idx_trade_price=idx_trade_price,
            idx_trade_size=idx_trade_size,
            proc_scale=1,
            stream=iter(()),
            next_item=None,
        )

    proc_scale = _detect_proc_scale(int(latest[-1][idx_proc]))
    rows = reader.range(start=start_proc_us * proc_scale, end=(end_proc_us + 1) * proc_scale, ts_key=proc_field)
    stream = iter(rows)
    return _Lane(
        subscription=subscription,
        callback=callback,
        idx_proc=idx_proc,
        idx_event=idx_event,
        idx_trade_side=idx_trade_side,
        idx_trade_price=idx_trade_price,
        idx_trade_size=idx_trade_size,
        proc_scale=proc_scale,
        stream=stream,
        next_item=_next_in_window(stream, idx_proc, idx_event, start_proc_us, end_proc_us, proc_scale),
    )


def _git_commit_or_unknown() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(root), stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return "unknown"


def _write_manifest(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, sort_keys=True, indent=2)


def run(args):
    strat = load_strategy(args.strategy)
    strategy_key = _strategy_key(strat)
    subscriptions = list(strat.subscriptions())
    if not subscriptions:
        raise ValueError("strategy returned no subscriptions")

    outputs = list(strat.outputs())
    if len(outputs) != 1 or outputs[0].role != "intent":
        raise ValueError("strategy.outputs() must define exactly one strategy-produced output: role='intent'")
    intent_out = outputs[0]
    output_base_path = getattr(args, "output_base_path", None)
    intent_base_path = str(output_base_path) if output_base_path else intent_out.base_path
    default_product_id = (intent_out.product_id or "").upper()
    if not default_product_id:
        raise ValueError("intent output must define product_id")

    market_subs = [s for s in subscriptions if s.method != _STATUS_METHOD]
    status_subs = [s for s in subscriptions if s.method == _STATUS_METHOD]
    if not market_subs:
        raise ValueError("strategy must include market subscriptions")
    if status_subs:
        execution_feed = status_subs[0].feed
        execution_input_base_path = status_subs[0].base_path
        for sub in status_subs[1:]:
            if sub.feed != execution_feed or sub.base_path != execution_input_base_path:
                raise ValueError("all on_status subscriptions must target the same execution feed/base_path")
        status_callbacks = [resolve_strategy_method(strat, sub.method) for sub in status_subs]
    else:
        execution_feed = execution_feed_name(strat.__class__.__name__)
        execution_input_base_path = intent_out.base_path
        status_callbacks = []
    execution_base_path = str(output_base_path) if output_base_path else execution_input_base_path

    decision_latency_us = int(getattr(strat, "decision_latency_us", 0))
    latency_ms = float(getattr(strat, "exchange_latency_ms", 0.5))
    reverse_latency_ms = float(getattr(strat, "exchange_reverse_latency_ms", 0.5))
    taker_fee_bps = float(getattr(strat, "exchange_taker_fee_bps", 1.0))
    maker_fee_bps = float(getattr(strat, "exchange_maker_fee_bps", 0.0))

    platform_cache: Dict[str, Platform] = {}
    anchor = market_subs[0]
    anchor_reader = platform_cache.setdefault(anchor.base_path, Platform(base_path=anchor.base_path)).create_reader(anchor.feed)
    anchor_names = tuple(anchor_reader.field_names)
    anchor_idx_proc = anchor_names.index(_processed_field(anchor_names))
    latest = anchor_reader.latest(seconds=max(1, int(args.seconds)))
    if not latest:
        print(f"No anchor data in last {args.seconds}s for {anchor.feed}")
        return None
    anchor_scale = _detect_proc_scale(int(latest[-1][anchor_idx_proc]))

    segment_meta = None
    if args.segment_id:
        if args.window_start_processed_us is not None or args.window_end_processed_us is not None:
            raise ValueError("cannot combine --segment-id with explicit --window-start/--window-end")
        segment_meta = get_segment(args.segment_id, args.segments_file)
        start_proc_us = int(segment_meta["window_start_processed_us"])
        end_proc_us = int(segment_meta["window_end_processed_us"])
        seg_market = [
            (str(f.get("feed")), str(f.get("base_path")), str(f.get("method")))
            for f in segment_meta.get("feeds", [])
            if str(f.get("method")) != _STATUS_METHOD
        ]
        strat_market = [(s.feed, s.base_path, s.method) for s in market_subs]
        if seg_market and seg_market != strat_market:
            raise ValueError("segment feed set does not match strategy market subscriptions")
    else:
        end_proc_us = (
            int(args.window_end_processed_us)
            if args.window_end_processed_us is not None
            else _to_proc_us(latest[-1][anchor_idx_proc], anchor_scale)
        )
        start_proc_us = (
            int(args.window_start_processed_us)
            if args.window_start_processed_us is not None
            else end_proc_us - int(args.seconds * 1_000_000.0)
        )

    # Special harness opinion: initialize exchange simulator before lane setup.
    sim = ExchangeSimulator(
        execution_base_path=execution_base_path,
        execution_feed=execution_feed,
        strategy_key=strategy_key,
        product_id=default_product_id,
        run_id=args.run_id,
        latency_ms=latency_ms,
        reverse_latency_ms=reverse_latency_ms,
        taker_fee_bps=taker_fee_bps,
        maker_fee_bps=maker_fee_bps,
    )
    intent_writer = get_intent_writer_named(Platform(base_path=intent_base_path), intent_out.feed)

    lanes = [
        _build_lane(s, resolve_strategy_method(strat, s.method), platform_cache, start_proc_us, end_proc_us)
        for s in market_subs
    ]

    # strategy_heap items: (processed_us, seq, kind, payload)
    # kind='market' payload=(lane, rec, event_us, processed_us)
    # kind='status' payload=(record,)
    strategy_heap: List[Tuple[int, int, str, Any]] = []
    # trade_heap items: (event_us, seq, side, price, size)
    trade_heap: List[Tuple[int, int, str, float, float]] = []
    seq = 0

    # Build scheduler inputs once from each market lane.
    for lane in lanes:
        nxt = lane.next_item
        while nxt is not None:
            rec, event_us, processed_us = nxt
            seq += 1
            heapq.heappush(strategy_heap, (int(processed_us), seq, "market", (lane, rec, int(event_us), int(processed_us))))
            if lane.idx_trade_side is not None and lane.idx_trade_price is not None and lane.idx_trade_size is not None:
                side = normalize_trade_side(rec[lane.idx_trade_side])
                if side in ("buy", "sell"):
                    seq += 1
                    heapq.heappush(
                        trade_heap,
                        (
                            int(event_us),
                            seq,
                            side,
                            float(rec[lane.idx_trade_price]),
                            float(rec[lane.idx_trade_size]),
                        ),
                    )
            nxt = _next_in_window(
                lane.stream,
                idx_proc=lane.idx_proc,
                idx_event=lane.idx_event,
                start_proc_us=start_proc_us,
                end_proc_us=end_proc_us,
                proc_scale=lane.proc_scale,
            )

    total_events = 0
    snapshot_events = 0
    trade_events = 0
    saw_snapshot = False
    status_callback_count = 0
    intent_records = 0
    first_proc: Optional[int] = None
    last_proc: Optional[int] = None

    def _emit_intents(event_us: int, processed_base_us: int, intents: List[Any]) -> None:
        nonlocal intent_records
        for i, intent in enumerate(intents):
            proc_us = processed_base_us + i + 1
            client_tag = intent.client_tag or f"A{intent_records + 1:07d}"
            product_id = (intent.product_id or default_product_id).upper()
            intent_writer.write_values(
                int(event_us),
                int(proc_us),
                product_id.encode("utf-8").ljust(16, b"\0"),
                ("B" if intent.side == "buy" else "S").encode("utf-8"),
                ("L" if intent.order_type == "limit" else "M").encode("utf-8"),
                intent.price,
                intent.size,
                _fixed_bytes(intent.tif, 8),
                _fixed_bytes(client_tag, 8),
            )
            sim.submit_intent(
                event_us=int(event_us),
                processed_us=int(proc_us),
                product_id=product_id,
                side=("B" if intent.side == "buy" else "S"),
                order_type=("L" if intent.order_type == "limit" else "M"),
                price=float(intent.price),
                size=float(intent.size),
                tif=intent.tif,
                client_tag=client_tag,
            )
            intent_records += 1

    def _process_strategy_item(item: Tuple[int, int, str, Any]) -> None:
        nonlocal total_events, snapshot_events, trade_events, saw_snapshot, status_callback_count, first_proc, last_proc
        processed_us, _, kind, payload = item
        if kind == "status":
            (status_record,) = payload
            for cb in status_callbacks:
                cb(status_record)
                status_callback_count += 1
            return

        lane, rec, event_us, lane_proc_us = payload
        total_events += 1
        if first_proc is None:
            first_proc = lane_proc_us
        last_proc = lane_proc_us
        if lane.subscription.method == _SNAPSHOT_METHOD:
            snapshot_events += 1
            saw_snapshot = True
        elif lane.subscription.method == _TRADE_METHOD:
            trade_events += 1

        raw = lane.callback(rec)
        if raw:
            _emit_intents(
                event_us=event_us,
                processed_base_us=lane_proc_us + decision_latency_us,
                intents=normalize_snapshot_output(raw, strict_contracts=args.strict_contracts),
            )

    while True:
        next_trade_event_us = trade_heap[0][0] if trade_heap else None
        next_strategy_proc = strategy_heap[0][0] if strategy_heap else None

        if next_trade_event_us is None and next_strategy_proc is None:
            break

        # Invariant: before exchange processes market event_time=E, strategy sees only processed_time <= E.
        if next_trade_event_us is not None and next_strategy_proc is not None and next_strategy_proc <= next_trade_event_us:
            _process_strategy_item(heapq.heappop(strategy_heap))
            continue

        if next_trade_event_us is not None:
            event_us, _, side, price, size = heapq.heappop(trade_heap)
            fills = sim.process_trade(
                event_us=int(event_us),
                side=side,
                price=float(price),
                size=float(size),
            )
            for proc_us, _, record in fills:
                seq += 1
                heapq.heappush(strategy_heap, (int(proc_us), seq, "status", (record,)))
            continue

        # No remaining exchange events: drain strategy side.
        _process_strategy_item(heapq.heappop(strategy_heap))

    intent_writer.close()
    sim.close()
    exchange_summary = sim.summary()

    summary = {
        "position": exchange_summary["position"],
        "pnl": exchange_summary["pnl"],
        "status_counts": exchange_summary["status_counts"],
        "status_callbacks": status_callback_count,
        "events": total_events,
        "snapshot_events": snapshot_events,
        "trade_events": trade_events,
        "saw_snapshot": saw_snapshot,
        "intent_feed": {"name": intent_out.feed, "base_path": intent_base_path, "records": intent_records},
        "execution_feed": {"name": execution_feed, "base_path": execution_base_path, **exchange_summary["execution_feed"]},
    }

    print(
        f"Engine done. events={total_events} snapshots={snapshot_events} trades={trade_events} "
        f"intents={intent_records}"
    )

    if args.manifest_path:
        _write_manifest(
            args.manifest_path,
            {
                "run_id": args.run_id,
                "git_commit": _git_commit_or_unknown(),
                "config": dict(vars(args)),
                "summary": summary,
                "window": {
                    "start_processed_us": int(first_proc) if first_proc is not None else None,
                    "end_processed_us": int(last_proc) if last_proc is not None else None,
                },
                "segment": segment_meta,
            },
        )
    return summary


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--strategy", default="ping_pong")
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--run-id", default="RUN1")
    ap.add_argument("--output-base-path", default=None)
    ap.add_argument("--window-start-processed-us", type=int, default=None)
    ap.add_argument("--window-end-processed-us", type=int, default=None)
    ap.add_argument("--segment-id", default=None)
    ap.add_argument("--segments-file", default=None)
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
