#!/usr/bin/env python3
"""Emit venue defaults for feed scripts and tooling."""

from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path

from runtime.venue import default_products, make_venue, resolve_base_path


def _shell_assignments(venue_key: str, root_dir: Path) -> str:
    venue = make_venue(venue_key)
    base_path = resolve_base_path(venue, root_dir)
    vals: dict[str, str] = {
        "VENUE_KEY": venue.key,
        "PRODUCTS_DEFAULT": ",".join(default_products(venue)),
        "DEFAULT_BASE_PATH": base_path,
        "FAMILIES": ",".join(venue.families),
    }
    return "\n".join(f"{k}={shlex.quote(v)}" for k, v in vals.items())


def _json_payload(venue_key: str, root_dir: Path) -> str:
    venue = make_venue(venue_key)
    payload = {
        "key": venue.key,
        "products_default": default_products(venue),
        "default_base_path": resolve_base_path(venue, root_dir),
        "families": list(venue.families),
    }
    return json.dumps(payload, sort_keys=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Emit venue defaults")
    ap.add_argument("--venue", required=True)
    ap.add_argument("--format", choices=["shell", "json"], default="shell")
    ap.add_argument("--root-dir", default=str(Path(__file__).resolve().parents[1]))
    args = ap.parse_args()

    root_dir = Path(args.root_dir).resolve()
    if args.format == "json":
        print(_json_payload(args.venue, root_dir))
        return
    print(_shell_assignments(args.venue, root_dir))


if __name__ == "__main__":
    main()