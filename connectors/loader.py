"""Dynamic connector loading for venue runtimes.

No static venue->connector registry is maintained; connector packages are
discovered by convention under `connectors/`.
"""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from types import ModuleType
from typing import Any

_REQUIRED_CONNECTOR_METHODS = (
    "feed_specs",
    "on_connect",
    "send_subscribe",
    "send_unsubscribe",
    "on_timeout",
    "handle_raw",
    "extra_status",
)

_REQUIRED_PROFILE_KEYS = (
    "key",
    "label",
    "ingest_description",
    "websocket_uri",
    "base_path",
    "default_products",
    "families",
)


def _candidate_packages_for_venue(venue_key: str) -> list[str]:
    base = Path(__file__).resolve().parent
    prefix = f"{venue_key}_"
    candidates: list[str] = []
    for child in base.iterdir():
        if not child.is_dir():
            continue
        if child.name.startswith("__"):
            continue
        if not (child / "connector.py").exists():
            continue
        if child.name == venue_key or child.name.startswith(prefix):
            candidates.append(child.name)
    return sorted(candidates)


def _resolve_package_for_venue(venue_key: str) -> str:
    candidates = _candidate_packages_for_venue(venue_key)
    if not candidates:
        raise ValueError(f"no connector package found for venue key: {venue_key}")
    if len(candidates) > 1:
        packages = ", ".join(candidates)
        raise ValueError(f"ambiguous connector packages for venue key {venue_key}: {packages}")
    return candidates[0]


def _load_connector_module_for_venue(venue_key: str) -> tuple[str, ModuleType]:
    package = _resolve_package_for_venue(venue_key)
    module = importlib.import_module(f"connectors.{package}.connector")
    return package, module


def _pick_connector_class(module: ModuleType, venue_key: str) -> type[Any]:
    connector_classes = []
    for obj in vars(module).values():
        if not inspect.isclass(obj):
            continue
        if obj.__module__ != module.__name__:
            continue
        if not obj.__name__.endswith("Connector"):
            continue
        connector_classes.append(obj)

    if not connector_classes:
        raise TypeError(f"no *Connector class found in {module.__name__}")

    if len(connector_classes) == 1:
        return connector_classes[0]

    scored = []
    for cls in connector_classes:
        venue_attr = str(getattr(cls, "venue", "") or "").lower()
        score = 0
        if venue_attr.startswith(venue_key):
            score += 2
        if venue_key in cls.__name__.lower():
            score += 1
        scored.append((score, cls.__name__, cls))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return scored[0][2]


def _connector_init_kwargs(connector_cls: type[Any], kwargs: dict[str, Any], *, uri_default: str) -> dict[str, Any]:
    sig = inspect.signature(connector_cls.__init__)
    params = sig.parameters
    accepts_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

    ctor: dict[str, Any] = {}
    if "uri" in params:
        ctor["uri"] = kwargs.pop("uri", uri_default)
    if "book_depth" in params:
        depth = int(kwargs.pop("book_depth", 1000))
        ctor["book_depth"] = int(max(10, depth))

    for name, value in list(kwargs.items()):
        if name in ("self", "uri", "book_depth"):
            continue
        if name in params or accepts_varkw:
            ctor[name] = kwargs.pop(name)

    return ctor


def _validate_connector_instance(connector: Any) -> None:
    missing: list[str] = []
    for name in _REQUIRED_CONNECTOR_METHODS:
        attr = getattr(connector, name, None)
        if not callable(attr):
            missing.append(name)
    if not getattr(connector, "uri", None):
        missing.append("uri")
    if not getattr(connector, "venue", None):
        missing.append("venue")
    if missing:
        raise TypeError(f"connector missing required protocol members: {', '.join(sorted(set(missing)))}")


def _normalize_profile(profile: dict[str, Any], *, venue_key: str) -> dict[str, Any]:
    for key in _REQUIRED_PROFILE_KEYS:
        if key not in profile:
            raise TypeError(f"connector profile missing required key '{key}' for venue '{venue_key}'")
    out = dict(profile)
    out["key"] = str(out["key"])
    out["label"] = str(out["label"])
    out["ingest_description"] = str(out["ingest_description"])
    out["websocket_uri"] = str(out["websocket_uri"])
    out["base_path"] = str(out["base_path"])
    out["default_products"] = str(out["default_products"])
    out["families"] = tuple(str(x) for x in (out.get("families") or ()))
    return out


def get_connector_profile_for_venue(venue_key: str) -> dict[str, Any]:
    """Return validated connector profile for a venue key."""

    package = _resolve_package_for_venue(venue_key)
    pkg_module = importlib.import_module(f"connectors.{package}")

    raw = getattr(pkg_module, "CONNECTOR_PROFILE", None)
    if raw is None:
        connector_module = importlib.import_module(f"connectors.{package}.connector")
        profile_fn = getattr(connector_module, "connector_profile", None)
        if not callable(profile_fn):
            raise TypeError(
                f"connector package {package} must define CONNECTOR_PROFILE or connector_profile()"
            )
        raw = profile_fn()

    if not isinstance(raw, dict):
        raise TypeError(f"connector profile for venue '{venue_key}' must be a dict")
    out = _normalize_profile(raw, venue_key=venue_key)
    if out["key"] != venue_key:
        raise ValueError(f"connector profile key mismatch: expected '{venue_key}' got '{out['key']}'")
    return out


def list_connector_profiles() -> list[dict[str, Any]]:
    """Return validated profiles for all discoverable connector venues."""

    base = Path(__file__).resolve().parent
    venue_keys: set[str] = set()
    for child in base.iterdir():
        if not child.is_dir():
            continue
        if child.name.startswith("__"):
            continue
        if not (child / "connector.py").exists():
            continue
        key = child.name.split("_", 1)[0]
        if key:
            venue_keys.add(key)

    out: list[dict[str, Any]] = []
    for key in sorted(venue_keys):
        try:
            out.append(get_connector_profile_for_venue(key))
        except Exception:
            continue
    return out


def build_connector_for_venue(venue_key: str, kwargs: dict[str, Any], *, uri_default: str | None = None) -> Any:
    """Instantiate and validate a connector for the given venue key."""

    _package, module = _load_connector_module_for_venue(venue_key)
    profile = get_connector_profile_for_venue(venue_key)
    connector_cls = _pick_connector_class(module, venue_key)
    ctor_kwargs = _connector_init_kwargs(
        connector_cls,
        kwargs,
        uri_default=uri_default or profile["websocket_uri"],
    )
    connector = connector_cls(**ctor_kwargs)
    _validate_connector_instance(connector)
    return connector
