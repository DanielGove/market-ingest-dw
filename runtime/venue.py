"""Runtime venue profile helpers.

Connector packages own profile metadata; runtime constructs typed venue config
objects from those profiles.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from connectors.loader import get_connector_profile_for_venue, list_connector_profiles


@dataclass(frozen=True)
class VenueConfig:
    """Typed runtime view of connector-owned profile metadata."""

    key: str
    label: str
    ingest_description: str
    websocket_uri: str
    base_path: str
    default_products: str
    families: tuple[str, ...]


def make_venue(venue_key: str) -> VenueConfig:
    """Construct a typed venue config from connector profile metadata."""

    profile = get_connector_profile_for_venue(venue_key)
    return VenueConfig(
        key=profile["key"],
        label=profile["label"],
        ingest_description=profile["ingest_description"],
        websocket_uri=profile["websocket_uri"],
        base_path=profile["base_path"],
        default_products=profile["default_products"],
        families=tuple(profile.get("families") or ()),
    )


def known_venue_keys() -> tuple[str, ...]:
    """Return sorted connector-owned venue keys discoverable in this runtime."""

    keys = {str(profile.get("key") or "").strip().lower() for profile in list_connector_profiles()}
    keys.discard("")
    return tuple(sorted(keys))


def venue_key_from_env(default: str | None = None) -> str:
    """Resolve and validate venue key from environment.

    Raises ValueError when venue is missing or unknown.
    """

    raw = (os.environ.get("DW_VENUE_KEY") or os.environ.get("DW_VENUE") or default or "").strip().lower()
    if not raw:
        known = ", ".join(known_venue_keys()) or "<none discovered>"
        raise ValueError(f"venue key is required (set DW_VENUE_KEY/DW_VENUE). known venues: {known}")
    try:
        get_connector_profile_for_venue(raw)
    except Exception as exc:
        known = ", ".join(known_venue_keys()) or "<none discovered>"
        raise ValueError(f"unknown venue key '{raw}'. known venues: {known}") from exc
    return raw


def default_products(venue: VenueConfig) -> list[str]:
    """Return uppercase default product ids from profile CSV."""

    return [p.strip().upper() for p in venue.default_products.split(",") if p.strip()]


def resolve_base_path(venue: VenueConfig, root_dir: Path) -> str:
    """Resolve the venue base path, treating relative values as repo-root relative."""

    base_path = Path(venue.base_path)
    if base_path.is_absolute():
        return str(base_path)
    return str((root_dir / base_path).resolve())
