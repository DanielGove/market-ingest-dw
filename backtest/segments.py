"""Application-level dataset segment registry for backtests.

This overlays Deepwater feed storage with a lightweight segment index
so we can refer to stable replay windows by ID.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEGMENTS_FILE = ROOT / "out" / "backtest" / "segments.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_segments(path: str | Path | None = None) -> list[dict[str, Any]]:
    p = Path(path) if path is not None else DEFAULT_SEGMENTS_FILE
    if not p.exists():
        return []
    out: list[dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            out.append(json.loads(raw))
    return out


def get_segment(segment_id: str, path: str | Path | None = None) -> dict[str, Any]:
    sid = str(segment_id).strip()
    if not sid:
        raise ValueError("segment_id must be non-empty")
    for rec in load_segments(path):
        if rec.get("segment_id") == sid:
            return rec
    raise KeyError(f"segment_id not found: {sid}")


def write_segment(record: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    p = Path(path) if path is not None else DEFAULT_SEGMENTS_FILE
    p.parent.mkdir(parents=True, exist_ok=True)

    sid = str(record.get("segment_id", "")).strip()
    if not sid:
        raise ValueError("record.segment_id is required")
    if record.get("window_start_processed_us") is None or record.get("window_end_processed_us") is None:
        raise ValueError("record must include window_start_processed_us and window_end_processed_us")
    if int(record["window_start_processed_us"]) >= int(record["window_end_processed_us"]):
        raise ValueError("segment window must satisfy start < end")

    existing = load_segments(p)
    if any(r.get("segment_id") == sid for r in existing):
        raise ValueError(f"segment_id already exists: {sid}")

    payload = dict(record)
    payload.setdefault("created_at", _now_iso())
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")
    return payload

