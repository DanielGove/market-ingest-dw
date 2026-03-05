#!/usr/bin/env python3
"""Runtime discovery surface for feature/data pipelines."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path
from typing import Any

from connectors.loader import list_connector_profiles
from deepwater.platform import Platform
from runtime.venue import default_products, make_venue, resolve_base_path

ROOT_DIR = Path(__file__).resolve().parents[1]
OPS_DIR = ROOT_DIR / "ops"
PID_DIR = OPS_DIR / "pids"


def _sanitize_instance(raw: str) -> str:
    out = "".join(ch if (ch.isalnum() or ch in "_-") else "_" for ch in (raw or ""))
    return out or "default"


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _sock_cmd(sock_path: Path, cmd: str, timeout: float = 0.5) -> str | None:
    if not sock_path.exists():
        return None
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.settimeout(timeout)
        s.connect(str(sock_path))
        s.sendall((cmd + "\n").encode("ascii", "ignore"))
        data = s.recv(65536)
        return data.decode("ascii", "ignore").strip()
    except Exception:
        return None
    finally:
        try:
            s.close()
        except Exception:
            pass


def _extract_venue(status_payload: dict[str, Any]) -> str:
    raw = str(status_payload.get("venue", "") or "").lower()
    if raw.endswith("_spot") or raw.endswith("_perp"):
        raw = raw.split("_", 1)[0]
    return raw or "unknown"


def _feed_schema(platform: Platform, feed_name: str) -> dict[str, Any] | None:
    try:
        cfg = platform.feed_dir(feed_name) / "config.json"
        payload = json.loads(cfg.read_text(encoding="utf-8"))
        fields = payload.get("fields") or []
        return {
            "feed": feed_name,
            "mode": payload.get("mode"),
            "clock_level": payload.get("clock_level"),
            "persist": bool(payload.get("persist", False)),
            "fields": [{"name": f.get("name"), "type": f.get("type")} for f in fields],
        }
    except Exception:
        return None


def _instance_discovery(instance: str, include_schemas: bool) -> dict[str, Any]:
    token = _sanitize_instance(instance)
    pidfile = PID_DIR / f"ingest.{token}.pid"
    sock = PID_DIR / f"ingest.{token}.sock"

    pid = _read_pid(pidfile)
    ingest_up = _alive(pid)

    status_raw = _sock_cmd(sock, "STATUS")
    list_raw = _sock_cmd(sock, "LIST")

    status_payload: dict[str, Any] = {}
    if status_raw:
        try:
            status_payload = json.loads(status_raw)
        except Exception:
            status_payload = {}

    products = []
    if list_raw:
        products = [p.strip().upper() for p in list_raw.split(",") if p.strip()]
    elif isinstance(status_payload.get("subs"), list):
        products = [str(x).upper() for x in status_payload.get("subs", []) if str(x).strip()]

    venue_key = _extract_venue(status_payload)
    base_path = None
    families: list[str] = []
    feeds: list[str] = []
    schemas: list[dict[str, Any]] = []

    if venue_key != "unknown":
        try:
            venue = make_venue(venue_key)
            base_path = resolve_base_path(venue, ROOT_DIR)
            families = list(venue.families)
            platform = Platform(base_path=base_path)
            all_feeds = platform.list_feeds()
            product_set = {p.upper() for p in products}
            if product_set:
                feeds = sorted([f for f in all_feeds if any(f.endswith(f"-{p}") for p in product_set)])
            else:
                feeds = sorted(all_feeds)
            if include_schemas:
                for feed in feeds:
                    schema = _feed_schema(platform, feed)
                    if schema is not None:
                        schemas.append(schema)
        except Exception:
            pass

    return {
        "instance": token,
        "ingest": {
            "up": ingest_up,
            "pid": pid,
            "pidfile": str(pidfile),
            "socket": str(sock),
            "socket_healthy": bool(status_raw or list_raw),
        },
        "venue": venue_key,
        "products": products,
        "families": families,
        "base_path": base_path,
        "feeds": feeds,
        "schemas": schemas,
        "status": status_payload,
    }


def _discover_instances() -> list[str]:
    out: list[str] = []
    for path in sorted(PID_DIR.glob("ingest.*.pid")):
        name = path.name
        token = name[len("ingest.") : -len(".pid")]
        if token:
            out.append(token)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Discover runtime venues/instances/feeds/schemas")
    ap.add_argument("--instance", action="append", default=[], help="Instance to include (repeatable)")
    ap.add_argument("--all-instances", action="store_true", help="Include all discovered instances")
    ap.add_argument("--include-schemas", action="store_true", help="Include field-level schema for discovered feeds")
    ap.add_argument("--json", action="store_true", default=True, help="Emit JSON output")
    args = ap.parse_args()

    wanted = [_sanitize_instance(x) for x in args.instance if str(x).strip()]
    if args.all_instances or not wanted:
        discovered = _discover_instances()
        for inst in discovered:
            if inst not in wanted:
                wanted.append(inst)

    connector_profiles = list_connector_profiles()
    venues = []
    for profile in connector_profiles:
        key = str(profile.get("key") or "")
        if not key:
            continue
        venue = make_venue(key)
        venues.append(
            {
                "key": venue.key,
                "label": venue.label,
                "websocket_uri": venue.websocket_uri,
                "base_path": resolve_base_path(venue, ROOT_DIR),
                "default_products": default_products(venue),
                "families": list(venue.families),
            }
        )

    instances = [_instance_discovery(inst, include_schemas=args.include_schemas) for inst in sorted(set(wanted))]

    payload = {
        "ts_unix": int(time.time()),
        "root_dir": str(ROOT_DIR),
        "venues": venues,
        "instances": instances,
    }

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
