#!/usr/bin/env python3
"""Preflight readiness check for backtest strategies."""
import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Optional

from deepwater.platform import Platform

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from backtest import engine as bt_engine


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


@dataclass
class SubCheck:
    method: str
    feed: str
    base_path: str
    exists: bool
    rows: int
    last_event_us: Optional[int]
    last_proc_us: Optional[int]
    error: str = ""


def _first_index(names: tuple[str, ...], candidates: tuple[str, ...]) -> Optional[int]:
    for key in candidates:
        if key in names:
            return names.index(key)
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest readiness preflight")
    ap.add_argument("--strategy", default="ping_pong")
    ap.add_argument("--window", type=float, default=300.0, help="latest() lookback in seconds")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    strat = bt_engine.load_strategy(args.strategy)
    subs = list(strat.subscriptions())
    checks: list[SubCheck] = []
    ok = True

    plat_cache: dict[str, Platform] = {}
    for sub in subs:
        plat = plat_cache.setdefault(sub.base_path, Platform(base_path=sub.base_path))
        try:
            if not plat.feed_exists(sub.feed):
                optional = sub.method == "on_status"
                checks.append(
                    SubCheck(
                        method=sub.method,
                        feed=sub.feed,
                        base_path=sub.base_path,
                        exists=False,
                        rows=0,
                        last_event_us=None,
                        last_proc_us=None,
                        error="feed missing (optional for on_status preflight)" if optional else "feed missing",
                    )
                )
                if not optional:
                    ok = False
                continue

            reader = plat.create_reader(sub.feed)
            names = tuple(reader.field_names)
            idx_event = _first_index(names, ("event_time", "snapshot_time", "event_us"))
            idx_proc = _first_index(names, ("processed_time", "processed_us"))
            rows = reader.latest(seconds=max(1.0, float(args.window)))
            n = len(rows)
            last_event_us = _to_us(int(rows[-1][idx_event])) if (n > 0 and idx_event is not None) else None
            last_proc_us = _to_us(int(rows[-1][idx_proc])) if (n > 0 and idx_proc is not None) else None

            checks.append(
                SubCheck(
                    method=sub.method,
                    feed=sub.feed,
                    base_path=sub.base_path,
                    exists=True,
                    rows=n,
                    last_event_us=last_event_us,
                    last_proc_us=last_proc_us,
                )
            )

            if sub.method != "on_status" and n == 0:
                ok = False
        except Exception as exc:
            optional = sub.method == "on_status"
            checks.append(
                SubCheck(
                    method=sub.method,
                    feed=sub.feed,
                    base_path=sub.base_path,
                    exists=False,
                    rows=0,
                    last_event_us=None,
                    last_proc_us=None,
                    error=f"{exc} (optional for on_status preflight)" if optional else str(exc),
                )
            )
            if not optional:
                ok = False

    if args.json:
        import json

        print(
            json.dumps(
                {
                    "strategy": args.strategy,
                    "window": args.window,
                    "ok": ok,
                    "checks": [c.__dict__ for c in checks],
                },
                sort_keys=True,
            )
        )
    else:
        print(f"strategy={args.strategy} window={int(args.window)}s ok={'YES' if ok else 'NO'}")
        print("method     rows  exists  feed")
        for c in checks:
            exists = "Y" if c.exists else "N"
            tail = f" err={c.error}" if c.error else ""
            print(f"{c.method:<10} {c.rows:>4}  {exists:^6}  {c.feed} @ {c.base_path}{tail}")

    raise SystemExit(0 if ok else 2)


if __name__ == "__main__":
    main()
