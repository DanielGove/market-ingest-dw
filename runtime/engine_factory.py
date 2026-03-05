"""Runtime engine construction from connector profiles."""

from __future__ import annotations

from logging import Logger
from pathlib import Path
from typing import Any

from connectors.loader import build_connector_for_venue
from runtime.venue import make_venue, resolve_base_path
from runtime.ws_engine import SharedWsIngestEngine


def _default_platform_base_path(venue_key: str) -> str:
    venue = make_venue(venue_key)
    repo_root = Path(__file__).resolve().parents[1]
    return resolve_base_path(venue, repo_root)


def create_configured_ws_engine(*, venue_key: str, logger: Logger, **kwargs: Any) -> SharedWsIngestEngine:
    """Build a websocket ingest engine for the selected venue."""

    venue = make_venue(venue_key)
    kw = dict(kwargs)
    connector = build_connector_for_venue(
        venue_key,
        kw,
        uri_default=venue.websocket_uri,
    )
    if kw:
        unknown = ", ".join(sorted(kw.keys()))
        raise TypeError(f"unsupported arguments for {venue_key} engine: {unknown}")

    engine = SharedWsIngestEngine(
        connector=connector,
        platform_base_path=_default_platform_base_path(venue_key),
        logger=logger,
    )
    return engine
