#!/usr/bin/env python3
"""Manage named backtest dataset segments."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

from deepwater.platform import Platform

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from backtest import engine as bt_engine
from backtest.segments import DEFAULT_SEGMENTS_FILE, get_segment, load_segments, write_segment


def _to_us(raw: int) -> int:
    x = int(raw)
    ax = abs(x)
    if ax > 10**17:
        return x // 1_000
    if ax > 10**14:
        return x
    if ax > 10**11:
        return x * 1_000
    return x * 1_000_000


def _first_index(names: tuple[str, ...], candidates: tuple[str, ...]) -> int | None:
    for c in candidates:
        if c in names:
            return names.index(c)
    return None


def _freeze_from_feed(base_path: str, feed: str, seconds: int) -> tuple[int, int]:
    reader = Platform(base_path=base_path).create_reader(feed)
    names = tuple(reader.field_names)
    idx_proc = _first_index(names, ("processed_time", "processed_us"))
    if idx_proc is None:
        raise ValueError(f"feed {feed} missing processed timestamp field")
    recs = reader.latest(seconds=max(1, int(seconds)))
    if not recs:
        raise ValueError(f"no records in latest({seconds}s) for {feed}")
    end_proc_us = _to_us(int(recs[-1][idx_proc]))
    start_proc_us = end_proc_us - int(seconds * 1_000_000.0)
    return start_proc_us, end_proc_us


def _strategy_market_feeds(strategy_name: str) -> list[dict[str, str]]:
    strat = bt_engine.load_strategy(strategy_name)
    out = []
    for s in strat.subscriptions():
        if s.method == "on_status":
            continue
        out.append({"feed": s.feed, "base_path": s.base_path, "method": s.method})
    if not out:
        raise ValueError("strategy has no market subscriptions")
    return out


def _default_segment_id(strategy_name: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{strategy_name.upper()}-{stamp}"


def cmd_list(args: argparse.Namespace) -> int:
    segs = load_segments(args.segments_file)
    if args.json:
        print(json.dumps(segs, indent=2, sort_keys=True))
        return 0
    if not segs:
        print("no segments")
        return 0
    print("segment_id  start_us  end_us  description")
    for s in segs:
        print(
            f"{s.get('segment_id','')}  {s.get('window_start_processed_us','')}  "
            f"{s.get('window_end_processed_us','')}  {s.get('description','')}"
        )
    return 0


def cmd_get(args: argparse.Namespace) -> int:
    seg = get_segment(args.segment_id, args.segments_file)
    print(json.dumps(seg, indent=2, sort_keys=True))
    return 0


def cmd_create(args: argparse.Namespace) -> int:
    feeds: list[dict[str, str]]
    if args.strategy:
        feeds = _strategy_market_feeds(args.strategy)
        anchor = feeds[0]
        if args.start_processed_us is not None or args.end_processed_us is not None:
            if args.start_processed_us is None or args.end_processed_us is None:
                raise ValueError("both --start-processed-us and --end-processed-us are required")
            start_us = int(args.start_processed_us)
            end_us = int(args.end_processed_us)
        else:
            start_us, end_us = _freeze_from_feed(anchor["base_path"], anchor["feed"], args.seconds)
    else:
        if not args.feed or not args.base_path:
            raise ValueError("without --strategy, both --feed and --base-path are required")
        feeds = [{"feed": args.feed, "base_path": args.base_path, "method": "on_snapshot"}]
        if args.start_processed_us is not None or args.end_processed_us is not None:
            if args.start_processed_us is None or args.end_processed_us is None:
                raise ValueError("both --start-processed-us and --end-processed-us are required")
            start_us = int(args.start_processed_us)
            end_us = int(args.end_processed_us)
        else:
            start_us, end_us = _freeze_from_feed(args.base_path, args.feed, args.seconds)

    if start_us >= end_us:
        raise ValueError("segment window must satisfy start < end")

    sid = args.segment_id or _default_segment_id(args.strategy or "SEG")
    rec: dict[str, Any] = {
        "segment_id": sid,
        "description": args.description or "",
        "strategy": args.strategy or "",
        "window_start_processed_us": start_us,
        "window_end_processed_us": end_us,
        "feeds": feeds,
    }
    saved = write_segment(rec, args.segments_file)
    print(json.dumps(saved, indent=2, sort_keys=True))
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest dataset segment registry")
    ap.add_argument("--segments-file", default=str(DEFAULT_SEGMENTS_FILE))
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list")
    p_list.add_argument("--json", action="store_true")

    p_get = sub.add_parser("get")
    p_get.add_argument("--segment-id", required=True)

    p_create = sub.add_parser("create")
    p_create.add_argument("--segment-id", default=None)
    p_create.add_argument("--strategy", default=None, help="strategy name to snapshot market subscription set")
    p_create.add_argument("--description", default="")
    p_create.add_argument("--seconds", type=int, default=300, help="window span for automatic freezing")
    p_create.add_argument("--feed", default=None, help="anchor feed when strategy is omitted")
    p_create.add_argument("--base-path", default=None, help="anchor base-path when strategy is omitted")
    p_create.add_argument("--start-processed-us", type=int, default=None)
    p_create.add_argument("--end-processed-us", type=int, default=None)

    args = ap.parse_args()
    if args.cmd == "list":
        raise SystemExit(cmd_list(args))
    if args.cmd == "get":
        raise SystemExit(cmd_get(args))
    if args.cmd == "create":
        raise SystemExit(cmd_create(args))
    raise SystemExit(2)


if __name__ == "__main__":
    main()

