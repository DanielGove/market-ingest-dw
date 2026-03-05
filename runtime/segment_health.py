#!/usr/bin/env python3
"""Segment integrity checks for ingest runtime feeds."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

from deepwater.platform import Platform
from runtime.venue import make_venue, resolve_base_path

ROOT_DIR = Path(__file__).resolve().parents[1]
PIDS_DIR = ROOT_DIR / "ops" / "pids"


def _sanitize_instance(raw: str | None) -> str:
    value = "".join(ch if (ch.isalnum() or ch in "_-") else "_" for ch in (raw or "").strip())
    return value or "default"


def _discover_instances() -> list[str]:
    out: list[str] = []
    for path in sorted(PIDS_DIR.glob("ingest.*.pid")):
        name = path.name
        out.append(name[len("ingest.") : -len(".pid")])
    return out


def _status_payload(instance: str) -> dict[str, Any]:
    sock_path = PIDS_DIR / f"ingest.{instance}.sock"
    if not sock_path.exists():
        return {}
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.settimeout(0.75)
        s.connect(str(sock_path))
        s.sendall(b"STATUS\n")
        raw = s.recv(8192).decode("ascii", "ignore").strip()
        if not raw:
            return {}
        return json.loads(raw)
    except Exception:
        return {}
    finally:
        try:
            s.close()
        except Exception:
            pass


def _venue_from_status(payload: dict[str, Any]) -> str | None:
    raw = str(payload.get("venue", "") or "").strip().lower()
    if not raw:
        return None
    if raw.endswith("_spot") or raw.endswith("_perp"):
        raw = raw.split("_", 1)[0]
    return raw or None


def _segments_bin() -> str:
    candidate = Path(sys.executable).resolve().parent / "deepwater-segments"
    if candidate.exists() and os.access(candidate, os.X_OK):
        return str(candidate)
    return "deepwater-segments"


def _segment_count(seg_bin: str, base_path: str, feed: str, status: str) -> tuple[int | None, str | None]:
    cmd = [seg_bin, "--base-path", base_path, "--feed", feed, "--status", status, "--json"]
    cp = subprocess.run(cmd, text=True, capture_output=True)
    if cp.returncode != 0:
        stderr = (cp.stderr or cp.stdout or "").strip() or "segment query failed"
        return None, stderr
    try:
        payload = json.loads(cp.stdout)
        return len(payload.get("segments") or []), None
    except Exception as exc:
        return None, f"json parse error: {exc}"


def _scan_instance(instance: str, base_path: str) -> dict[str, Any]:
    platform = Platform(base_path=base_path)
    feeds = sorted(platform.list_feeds())
    seg_bin = _segments_bin()

    checked = 0
    ok = 0
    warn = 0
    error = 0
    feed_results: list[dict[str, Any]] = []

    for feed in feeds:
        usable_count, usable_err = _segment_count(seg_bin, base_path, feed, "usable")
        invalid_count, invalid_err = _segment_count(seg_bin, base_path, feed, "invalid_empty")

        status = "OK"
        note = ""
        if usable_err or invalid_err:
            status = "ERROR"
            note = "; ".join(x for x in (usable_err, invalid_err) if x)
            error += 1
        else:
            invalid_n = int(invalid_count or 0)
            usable_n = int(usable_count or 0)
            if invalid_n > 0:
                status = "ERROR"
                note = f"invalid_empty={invalid_n}"
                error += 1
            elif usable_n == 0:
                status = "WARN"
                note = "no usable segments yet"
                warn += 1
            else:
                ok += 1

        checked += 1
        feed_results.append(
            {
                "feed": feed,
                "status": status,
                "usable_segments": usable_count,
                "invalid_empty_segments": invalid_count,
                "note": note,
            }
        )

    overall = "HEALTHY"
    if error > 0:
        overall = "DEGRADED"
    elif warn > 0:
        overall = "WARN"

    return {
        "instance": instance,
        "base_path": base_path,
        "overall": overall,
        "feeds_checked": checked,
        "ok": ok,
        "warn": warn,
        "error": error,
        "feeds": feed_results,
    }


def _print_text(results: list[dict[str, Any]]) -> None:
    for entry in results:
        print("=" * 100)
        print(
            f"instance={entry['instance']} overall={entry['overall']} base_path={entry['base_path']} "
            f"feeds_checked={entry['feeds_checked']} ok={entry['ok']} warn={entry['warn']} error={entry['error']}"
        )
        for feed in entry["feeds"]:
            if feed["status"] == "OK":
                continue
            print(
                f"{feed['status']:<7} {feed['feed']:<36} usable={feed['usable_segments']} "
                f"invalid_empty={feed['invalid_empty_segments']} {feed['note']}"
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Segment integrity checks for runtime feeds")
    ap.add_argument("--instance", action="append", default=[], help="Instance to check (repeatable)")
    ap.add_argument("--all-instances", action="store_true", help="Check all discovered ingest instances")
    ap.add_argument("--base-path", default=None, help="Override base path (single-instance only)")
    ap.add_argument("--json", action="store_true", help="Emit JSON")
    args = ap.parse_args()

    wanted = [_sanitize_instance(v) for v in args.instance if str(v).strip()]
    if args.all_instances or not wanted:
        for discovered in _discover_instances():
            if discovered not in wanted:
                wanted.append(discovered)

    if not wanted:
        raise SystemExit("no instances found")

    if args.base_path and len(wanted) > 1:
        raise SystemExit("--base-path may only be used with a single --instance")

    results: list[dict[str, Any]] = []
    for instance in sorted(set(wanted)):
        base_path = args.base_path
        if not base_path:
            status_payload = _status_payload(instance)
            venue_key = _venue_from_status(status_payload)
            if not venue_key:
                if instance in {"coinbase", "kraken", "hyperliquid"}:
                    venue_key = instance
            if not venue_key:
                results.append(
                    {
                        "instance": instance,
                        "base_path": None,
                        "overall": "DEGRADED",
                        "feeds_checked": 0,
                        "ok": 0,
                        "warn": 0,
                        "error": 1,
                        "feeds": [],
                        "note": "unable to resolve venue/base path",
                    }
                )
                continue
            venue = make_venue(venue_key)
            base_path = resolve_base_path(venue, ROOT_DIR)

        results.append(_scan_instance(instance, str(Path(base_path))))

    if args.json:
        print(json.dumps({"instances": results}, indent=2, sort_keys=True))
    else:
        _print_text(results)

    any_error = any(r.get("overall") == "DEGRADED" for r in results)
    raise SystemExit(1 if any_error else 0)


if __name__ == "__main__":
    main()
