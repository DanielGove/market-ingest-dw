#!/usr/bin/env python3
"""Feed health monitor for venue ingest runtime feeds."""

import argparse
import json
import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from deepwater.platform import Platform
from runtime.venue import default_products, make_venue, resolve_base_path, venue_key_from_env


def _default_base_path(venue) -> str:
    return resolve_base_path(venue, ROOT_DIR)


def _discover_live_products(instance: str) -> list[str]:
    ingest_sock = ROOT_DIR / "ops" / "pids" / f"ingest.{instance}.sock"
    if not ingest_sock.exists():
        return []
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.settimeout(0.5)
        s.connect(str(ingest_sock))
        s.sendall(b"LIST\n")
        data = s.recv(4096).decode("ascii", "ignore").strip()
        if not data or data.startswith("ERR"):
            return []
        return [p.strip().upper() for p in data.split(",") if p.strip()]
    except Exception:
        return []
    finally:
        try:
            s.close()
        except Exception:
            pass


def _discover_live_venue(instance: str) -> str | None:
    ingest_sock = ROOT_DIR / "ops" / "pids" / f"ingest.{instance}.sock"
    if not ingest_sock.exists():
        return None
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.settimeout(0.5)
        s.connect(str(ingest_sock))
        s.sendall(b"STATUS\n")
        data = s.recv(8192).decode("ascii", "ignore").strip()
        if not data or data.startswith("ERR"):
            return None
        payload = json.loads(data)
        raw = str(payload.get("venue", "") or "").strip().lower()
        if not raw:
            return None
        if raw.endswith("_spot") or raw.endswith("_perp"):
            raw = raw.split("_", 1)[0]
        return raw or None
    except Exception:
        return None
    finally:
        try:
            s.close()
        except Exception:
            pass


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


def _pct(values: Sequence[float], p: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(values)
    idx = int(round((len(xs) - 1) * p))
    return float(xs[idx])


def _fmt_num(v: Optional[float], places: int = 1) -> str:
    if v is None:
        return "-"
    return f"{v:.{places}f}"


def _first_field(names: tuple[str, ...], candidates: Sequence[str]) -> Optional[int]:
    for key in candidates:
        if key in names:
            return names.index(key)
    return None


def _field_size_bytes(type_name: str) -> Optional[int]:
    t = (type_name or "").strip().lower()
    if t in ("char", "bool", "int8", "uint8"):
        return 1
    if t in ("int16", "uint16"):
        return 2
    if t in ("int32", "uint32", "float32"):
        return 4
    if t in ("int64", "uint64", "float64"):
        return 8
    if t.startswith("bytes"):
        try:
            return int(t.replace("bytes", ""))
        except Exception:
            return None
    if t.startswith("_"):
        try:
            return int(t[1:])
        except Exception:
            return None
    return None


def _estimate_row_bytes(platform: Platform, feed: str, cache: Dict[str, Optional[int]]) -> Optional[int]:
    if feed in cache:
        return cache[feed]
    try:
        cfg_path = platform.feed_dir(feed) / "config.json"
        payload = json.loads(cfg_path.read_text(encoding="utf-8"))
        fields = payload.get("fields") or []
        total = 0
        for f in fields:
            sz = _field_size_bytes(str(f.get("type", "")))
            if sz is None:
                cache[feed] = None
                return None
            total += sz
        cache[feed] = total
        return total
    except Exception:
        cache[feed] = None
        return None


def _feed_sort_key(feed: str) -> tuple[int, str]:
    return (1, feed)


def _discover_feeds(platform: Platform, products: list[str]) -> list[str]:
    feeds = sorted(platform.list_feeds(), key=_feed_sort_key)
    if not products:
        return feeds

    product_set = {p.upper() for p in products}
    out: list[str] = []
    for feed in feeds:
        matched = False
        for pid in product_set:
            if feed.endswith(f"-{pid}"):
                matched = True
                break
        if matched:
            out.append(feed)
    return out


def _collect(
    platform: Platform,
    feeds: list[str],
    window_s: float,
    max_latency_ms: float,
) -> list[dict]:
    existing = set(platform.list_feeds())
    row_size_cache: Dict[str, Optional[int]] = {}
    stats: list[dict] = []

    for feed in feeds:
        if feed not in existing:
            stats.append(
                {
                    "feed": feed,
                    "status": "MISSING",
                    "rows": 0,
                    "rows_per_s": 0.0,
                    "bytes_per_s_est": 0.0,
                    "row_bytes_est": None,
                    "lat_ms_latest": None,
                    "lat_ms_p50": None,
                    "lat_ms_p95": None,
                    "lat_ms_p99": None,
                    "lat_ms_avg": None,
                    "note": "feed absent",
                }
            )
            continue

        try:
            reader = platform.create_reader(feed)
        except Exception as exc:
            stats.append(
                {
                    "feed": feed,
                    "status": "ERROR",
                    "rows": 0,
                    "rows_per_s": 0.0,
                    "bytes_per_s_est": 0.0,
                    "row_bytes_est": None,
                    "lat_ms_latest": None,
                    "lat_ms_p50": None,
                    "lat_ms_p95": None,
                    "lat_ms_p99": None,
                    "lat_ms_avg": None,
                    "note": f"reader error: {exc}",
                }
            )
            continue

        names = tuple(reader.field_names)
        idx_event = _first_field(names, ("event_time", "snapshot_time", "event_us"))
        idx_proc = _first_field(names, ("processed_time", "processed_us"))
        rows = reader.latest(seconds=max(1.0, window_s))
        n = len(rows)
        row_bytes_est = _estimate_row_bytes(platform, feed, row_size_cache)
        rows_per_s = float(n) / max(1.0, float(window_s))
        bytes_per_s_est = rows_per_s * row_bytes_est if row_bytes_est is not None else None

        if n == 0:
            stats.append(
                {
                    "feed": feed,
                    "status": "IDLE",
                    "rows": 0,
                    "rows_per_s": 0.0,
                    "bytes_per_s_est": 0.0,
                    "row_bytes_est": row_bytes_est,
                    "lat_ms_latest": None,
                    "lat_ms_p50": None,
                    "lat_ms_p95": None,
                    "lat_ms_p99": None,
                    "lat_ms_avg": None,
                    "note": "",
                }
            )
            continue

        latencies: list[float] = []
        if idx_proc is not None and idx_event is not None:
            for rec in rows:
                proc_us = _to_us(int(rec[idx_proc]))
                evt_us = _to_us(int(rec[idx_event]))
                latencies.append((proc_us - evt_us) / 1000.0)

        lat_latest = latencies[-1] if latencies else None
        lat_p50 = _pct(latencies, 0.50)
        lat_p95 = _pct(latencies, 0.95)
        lat_p99 = _pct(latencies, 0.99)
        lat_avg = (sum(latencies) / len(latencies)) if latencies else None
        status = "SLOW" if (lat_p95 is not None and lat_p95 > max_latency_ms) else "OK"

        stats.append(
            {
                "feed": feed,
                "status": status,
                "rows": n,
                "rows_per_s": rows_per_s,
                "bytes_per_s_est": bytes_per_s_est,
                "row_bytes_est": row_bytes_est,
                "lat_ms_latest": lat_latest,
                "lat_ms_p50": lat_p50,
                "lat_ms_p95": lat_p95,
                "lat_ms_p99": lat_p99,
                "lat_ms_avg": lat_avg,
                "note": "",
            }
        )

    return stats


def _overall(stats: Sequence[dict]) -> str:
    statuses = {s["status"] for s in stats}
    if "MISSING" in statuses or "ERROR" in statuses:
        return "DEGRADED"
    if statuses == {"IDLE"}:
        return "STALLED"
    if "SLOW" in statuses:
        return "WARN"
    return "HEALTHY"


def _print_text(stats: list[dict], base_path: str, window_s: float, hide_idle: bool) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    total_rows = sum(s["rows"] for s in stats)
    total_rows_s = sum(float(s.get("rows_per_s") or 0.0) for s in stats)
    total_bytes_s = sum(float(s.get("bytes_per_s_est") or 0.0) for s in stats)
    status_counts: Dict[str, int] = {}
    for s in stats:
        status_counts[s["status"]] = status_counts.get(s["status"], 0) + 1
    all_p95 = [s["lat_ms_p95"] for s in stats if s["lat_ms_p95"] is not None]

    print("=" * 120)
    print(
        f"{ts}Z  overall={_overall(stats)}  base_path={base_path}  window={int(window_s)}s  "
        f"feeds={len(stats)}  rows={total_rows}  rows_s={total_rows_s:.2f}  bytes_s_est={total_bytes_s:.1f}"
    )
    print(
        "status_counts "
        + " ".join(f"{k}={v}" for k, v in sorted(status_counts.items(), key=lambda x: x[0]))
        + f"  fleet_p95_latency_ms={_fmt_num(_pct(all_p95, 0.95), 1)}"
    )
    print(
        "status  feed                             rows  rows_s  bytes_s_est  "
        "lat_ms_p50  lat_ms_p95  lat_ms_p99  lat_ms_latest  lat_ms_avg  note"
    )
    for s in stats:
        if hide_idle and s["status"] == "IDLE":
            continue
        print(
            f"{s['status']:<7} {s['feed']:<32} "
            f"{int(s['rows']):>5}  "
            f"{_fmt_num(s.get('rows_per_s'), 2):>6}  "
            f"{_fmt_num(s.get('bytes_per_s_est'), 1):>11}  "
            f"{_fmt_num(s.get('lat_ms_p50'), 1):>10}  "
            f"{_fmt_num(s.get('lat_ms_p95'), 1):>10}  "
            f"{_fmt_num(s.get('lat_ms_p99'), 1):>10}  "
            f"{_fmt_num(s.get('lat_ms_latest'), 1):>13}  "
            f"{_fmt_num(s.get('lat_ms_avg'), 1):>10}  "
            f"{s.get('note', '')}"
        )
    print(flush=True)


def _print_json(stats: list[dict], base_path: str, window_s: float) -> None:
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "overall": _overall(stats),
        "base_path": base_path,
        "window_s": int(window_s),
        "stats": stats,
    }
    print(json.dumps(payload, sort_keys=True), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Feed health monitor (throughput + latency)")
    ap.add_argument("--venue", default=None, help="Venue key (coinbase|kraken|hyperliquid)")
    ap.add_argument("--instance", default=None, help="Instance name (default: DW_INSTANCE or DW_VENUE or 'default')")
    ap.add_argument("--base-path", default=None)
    ap.add_argument("--products", default=None)
    ap.add_argument("--window", type=float, default=60.0)
    ap.add_argument("--interval", type=float, default=60.0)
    ap.add_argument("--max-latency-ms", type=float, default=15_000.0)
    ap.add_argument("--hide-idle", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    instance = (args.instance or os.environ.get("DW_INSTANCE") or os.environ.get("DW_VENUE") or "default").strip() or "default"

    venue_key: str | None = args.venue.strip().lower() if args.venue else None
    if not venue_key:
        venue_key = _discover_live_venue(instance)
    if not venue_key:
        try:
            venue_key = venue_key_from_env(default=None)
        except ValueError:
            venue_key = None
    if not venue_key:
        raise SystemExit(
            "unable to resolve venue for feed health; pass --venue or set DW_VENUE_KEY/DW_VENUE, "
            "or ensure ingest control socket is responsive"
        )

    venue = make_venue(venue_key)
    base_path = args.base_path or _default_base_path(venue)

    products_raw = args.products
    if products_raw is None:
        products_raw = os.environ.get("PRODUCTS")
    if not products_raw:
        live_products = _discover_live_products(instance)
        if live_products:
            products = live_products
        else:
            products = default_products(venue)
    else:
        products = [p.strip().upper() for p in products_raw.split(",") if p.strip()]

    platform = Platform(base_path=str(Path(base_path)))

    while True:
        started = time.time()
        feeds = _discover_feeds(platform, products)
        stats = _collect(platform, feeds, args.window, args.max_latency_ms)

        if args.json:
            _print_json(stats, str(Path(base_path)), args.window)
        else:
            _print_text(stats, str(Path(base_path)), args.window, args.hide_idle)

        if args.once:
            return
        elapsed = time.time() - started
        time.sleep(max(0.0, float(args.interval) - elapsed))


if __name__ == "__main__":
    main()