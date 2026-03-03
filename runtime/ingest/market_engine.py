"""Shared market-engine construction helpers for venue wrappers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from connectors.registry import get_venue_registration
from runtime.ingest.ws_engine import SharedWsIngestEngine
from runtime.venues import get_venue_defaults, resolve_base_path


def venue_trades_spec(venue_key: str) -> Callable[[str], dict]:
    return get_venue_registration(venue_key).trades_spec


def venue_l2_spec(venue_key: str) -> Callable[[str], dict]:
    return get_venue_registration(venue_key).l2_spec


def _build_connector(venue_key: str, kwargs: dict[str, Any]):
    venue = get_venue_defaults(venue_key)
    registration = get_venue_registration(venue_key)
    connector, compat_attrs = registration.build_connector(venue, kwargs)
    return connector, venue, compat_attrs


def _default_platform_base_path(venue_key: str) -> str:
    venue = get_venue_defaults(venue_key)
    repo_root = Path(__file__).resolve().parents[2]
    return resolve_base_path(venue, repo_root)


class ConfiguredMarketDataEngine(SharedWsIngestEngine):
    """Shared base for venue compatibility facades."""

    def __init__(self, *, venue_key: str, logger, **kwargs: Any) -> None:
        kw = dict(kwargs)
        connector, venue, compat_attrs = _build_connector(venue_key, kw)
        if kw:
            unknown = ", ".join(sorted(kw.keys()))
            raise TypeError(f"unsupported arguments for {venue_key} engine: {unknown}")
        for name, value in compat_attrs.items():
            setattr(self, name, value)
        super().__init__(
            connector=connector,
            platform_base_path=_default_platform_base_path(venue_key),
            logger=logger,
        )
