"""Normalized-only v4 live source adapter; no raw GFW response is persisted."""
from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any, Callable

from scripts.gfw_east_asia_v4_poc import (
    EXPECTED_TILE_COUNT, FIXED_BBOX, fetch_fishing_effort_phase,
    fetch_presence_phase, normalize_fishing_effort,
)
from scripts.gfw_hourly_tracks_poc import GFWReportClient, make_tiles


class GFWV4LiveSourceError(RuntimeError):
    pass


def _descriptor(path: Path, *, schema: str, row_count: int) -> dict[str, Any]:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "kind": "normalized_spool_descriptor", "schema": schema,
        "root": str(path.parent), "path": path.name,
        "bytes": path.stat().st_size, "sha256": digest, "row_count": row_count,
        "raw_response_saved": False,
    }


def fetch_normalized_v4_daily_source(*, token: str, selected_day: date, work_dir: Path,
    client_factory: Callable[[str], Any] = GFWReportClient) -> dict[str, Any]:
    """Fetch 42 HIGH reports + independent DAILY effort, retaining normalized rows only."""
    if len(make_tiles(FIXED_BBOX, tile_size_degrees=3.0)) != EXPECTED_TILE_COUNT:
        raise GFWV4LiveSourceError("frozen East Asia bbox must remain 42 report tiles")
    work_dir.mkdir(parents=True, exist_ok=False)
    client = client_factory(token)
    high_path = work_dir / "presence-high.ndjson"
    presence = fetch_presence_phase(client=client, resolution="HIGH", selected_day=selected_day, output_path=high_path)
    if presence.get("raw_response_saved") is not False or any(x.get("next_offset_complete") is not True for x in presence.get("tiles", [])):
        raise GFWV4LiveSourceError("presence pagination/raw-retention gate failed")
    effort_payloads, effort = fetch_fishing_effort_phase(client=client, selected_day=selected_day)
    rows, quality = normalize_fishing_effort(effort_payloads, selected_day=selected_day, resolved_dataset_version=effort["resolved_dataset_versions"][0])
    if any(quality[k] for k in ("invalid_rows", "negative_hours_rejected", "wrong_day_rows", "boundary_overlap_rows")):
        raise GFWV4LiveSourceError("fishing effort normalization quality gate failed")
    high_count = int(presence.get("normalized_row_count", 0))
    if not high_path.is_file():
        raise GFWV4LiveSourceError("presence phase did not produce its normalized spool")
    effort_path = work_dir / "fishing-effort-normalized.ndjson"
    with effort_path.open("wb") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
    # Do not return full-day rows in the envelope.  The descriptors are the
    # only source/finalizer handoff and carry immutable byte/hash/count proof.
    result = {"schema_version": 1, "kind": "gfw_v4_normalized_daily_source", "selected_utc_date": selected_day.isoformat(), "raw_response_saved": False,
        "presence": {"next_offset_complete": True, "resolved_dataset_version": presence["resolved_dataset_versions"][0], "records": _descriptor(high_path, schema="gfw-v4-presence-high-ndjson", row_count=high_count), "metrics": presence},
        "fishing_effort": {"next_offset_complete": True, "resolved_dataset_version": effort["resolved_dataset_versions"][0], "records": _descriptor(effort_path, schema="gfw-v4-fishing-effort-normalized-ndjson", row_count=len(rows)), "metrics": {**effort, "raw_response_saved": False, "quality": quality}}}
    del rows
    return result
