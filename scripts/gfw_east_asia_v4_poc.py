#!/usr/bin/env python3
"""Local-only East Asia GFW 0.1-degree v4 shadow POC.

The module deliberately has no DB, S3, scheduler, deploy, or production-root
integration.  A completed output directory is immutable: callers must provide
a path that does not already exist.  LOW and HIGH source phases can be run in
separate worker processes so their peak RSS values are comparable.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import resource
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from collectors.gfw_vessel_presence import (  # noqa: E402
    GFW_DATASET,
    GFWVesselPresenceCollector,
    _first,
)
from scripts.gfw_hourly_browser_assets import (  # noqa: E402
    _pmtiles,
    require_gfw_asset_toolchain,
)
from scripts.gfw_hourly_tracks_poc import (  # noqa: E402
    GFWReportClient,
    _haversine_nm,
    finalize_track_store,
    make_tiles,
)


SCHEMA_VERSION = 1
FIXED_BBOX = (115.93462, 20.36314, 134.73486, 36.52495)
TILE_SIZE_DEGREES = 3.0
EXPECTED_TILE_COUNT = 42
FISHING_EFFORT_DATASET = "public-global-fishing-effort:latest"
POPUP_FIELDS = (
    "vessel_id", "mmsi", "ship_name", "vessel_type", "flag", "hours",
    "entry_timestamp", "exit_timestamp", "imo", "callsign",
    "first_transmission_date", "last_transmission_date", "dataset", "geartype",
)
SHIP_TYPE_BUCKETS = ("cargo", "tanker", "passenger", "fishing", "other")
TYPED_MAGIC = b"GFW4TRK1"
TYPED_HEADER = struct.Struct("<8sIIII")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(payload)
    temporary.replace(path)


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_bytes(path, _canonical_bytes(value))


def _gzip_bytes(value: Any) -> bytes:
    return gzip.compress(_canonical_bytes(value), compresslevel=9, mtime=0)


def _atomic_gzip_json(path: Path, value: Any) -> None:
    _atomic_bytes(path, _gzip_bytes(value))


def _peak_rss_bytes() -> int:
    raw = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return raw if sys.platform == "darwin" else raw * 1024


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc_hour(value: str) -> str:
    return _parse_utc(value).replace(
        minute=0, second=0, microsecond=0
    ).strftime("%Y-%m-%dT%H:00:00Z")


def _day_hours(selected_day: date) -> list[str]:
    current = datetime.combine(selected_day, datetime.min.time(), tzinfo=timezone.utc)
    return [
        (current + timedelta(hours=hour)).strftime("%Y-%m-%dT%H:00:00Z")
        for hour in range(24)
    ]


def _clean_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _cell_center_hundredths(value: Any) -> int:
    """Map a coordinate to a globally aligned 0.1-degree cell center.

    The result is an integer number of hundredths, avoiding binary-float
    rounding at tenth-degree boundaries.  ``125.005`` and ``125.095`` map to
    center ``125.05`` (integer 12505); ``125.1`` maps to ``125.15``.
    """
    coordinate = Decimal(str(value))
    tenth_index = int((coordinate * 10).to_integral_value(rounding=ROUND_FLOOR))
    return tenth_index * 10 + 5


def canonical_cell(longitude: Any, latitude: Any) -> tuple[int, int]:
    lon = Decimal(str(longitude))
    lat = Decimal(str(latitude))
    if not Decimal("-180") <= lon <= Decimal("180"):
        raise ValueError("longitude outside WGS84")
    if not Decimal("-90") <= lat <= Decimal("90"):
        raise ValueError("latitude outside WGS84")
    return _cell_center_hundredths(lon), _cell_center_hundredths(lat)


def _cell_id(hour: str, cell: tuple[int, int]) -> str:
    digest = hashlib.sha256(f"{hour}|{cell[0]}|{cell[1]}".encode()).hexdigest()[:20]
    return f"g01-{digest}"


def _cell_polygon(cell: tuple[int, int]) -> list[list[list[float]]]:
    center_lon = Decimal(cell[0]) / 100
    center_lat = Decimal(cell[1]) / 100
    half = Decimal("0.05")
    west, east = center_lon - half, center_lon + half
    south, north = center_lat - half, center_lat + half
    return [[
        [float(west), float(south)], [float(east), float(south)],
        [float(east), float(north)], [float(west), float(north)],
        [float(west), float(south)],
    ]]


def _project_presence(row: dict[str, Any]) -> dict[str, Any]:
    source = row.get("source_properties") or {}
    hours = _first(source, "hours", "presenceHours", "presence_hours", "value")
    try:
        hours = None if hours in (None, "") else float(hours)
    except (TypeError, ValueError):
        hours = None
    return {
        "vessel_id": row.get("vessel_id"),
        "observed_at": row.get("observed_at"),
        "longitude": row.get("longitude"),
        "latitude": row.get("latitude"),
        "mmsi": row.get("mmsi"),
        "ship_name": row.get("ship_name"),
        "vessel_type": row.get("vessel_type"),
        "flag": row.get("flag"),
        "hours": hours,
        "entry_timestamp": _clean_text(_first(source, "entryTimestamp", "entry_timestamp")),
        "exit_timestamp": _clean_text(_first(source, "exitTimestamp", "exit_timestamp")),
        "imo": _clean_text(_first(source, "imo", "IMO")),
        "callsign": _clean_text(_first(source, "callsign", "callSign", "call_sign")),
        "first_transmission_date": _clean_text(_first(
            source, "firstTransmissionDate", "first_transmission_date", "first_transmission",
        )),
        "last_transmission_date": _clean_text(_first(
            source, "lastTransmissionDate", "last_transmission_date", "last_transmission",
        )),
        "dataset": _clean_text(_first(source, "dataset", "datasetId", "dataset_id")),
        "geartype": _clean_text(_first(source, "geartype", "gearType", "gear_type")),
    }


def _report_row_candidates(value: Any) -> list[dict[str, Any]]:
    """Count report rows before the production normalizer can silently reject them."""
    if isinstance(value, list):
        rows: list[dict[str, Any]] = []
        for item in value:
            rows.extend(_report_row_candidates(item))
        return rows
    if not isinstance(value, dict):
        return []
    row_markers = {
        "vessel_id", "vesselId", "vesselIdRaw", "id", "ship_id",
        "longitude", "lon", "lng", "latitude", "lat", "date", "hours",
    }
    if row_markers & value.keys() and (
        {"longitude", "latitude"} <= value.keys()
        or {"lon", "lat"} <= value.keys()
        or {"lng", "lat"} <= value.keys()
    ):
        return [value]
    rows = []
    for nested in value.values():
        rows.extend(_report_row_candidates(nested))
    return rows


class MeasuredGFWReportClient(GFWReportClient):
    """GFW client telemetry that never records token or response bodies."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stats.update({
            "response_body_bytes": 0,
            "declared_content_length_bytes": 0,
            "responses_with_content_length": 0,
            "status_429": 0,
            "status_524": 0,
        })

    def _record_response(self, response: Any) -> None:
        super()._record_response(response)
        content = getattr(response, "content", None)
        if content is None:
            content = str(getattr(response, "text", "")).encode("utf-8")
        self.stats["response_body_bytes"] += len(content)
        content_length = response.headers.get("content-length")
        if content_length is not None:
            try:
                self.stats["declared_content_length_bytes"] += int(content_length)
                self.stats["responses_with_content_length"] += 1
            except (TypeError, ValueError):
                pass
        if int(response.status_code) == 429:
            self.stats["status_429"] += 1
        if int(response.status_code) == 524:
            self.stats["status_524"] += 1


class FixtureSequenceReportClient:
    """Offline response sequence with the same narrow interface as the live client."""

    def __init__(self, fixture: dict[str, Any]):
        responses = fixture.get("responses")
        if not isinstance(responses, list):
            raise ValueError("fixture must contain a responses array")
        self.responses = list(responses)
        self.stats = {
            "post_requests": 0, "recovery_requests": 0, "retries": 0,
            "http_statuses": {}, "response_body_bytes": 0,
            "status_429": 0, "status_524": 0,
        }

    def fetch(
        self, bbox: tuple[float, float, float, float], start: str, end: str,
        *, spatial_resolution: str, dataset: str = GFW_DATASET,
        group_by: str | None = "VESSEL_ID", filters: tuple[str, ...] = (),
        temporal_resolution: str = "HOURLY",
    ) -> tuple[Any, str | None]:
        del bbox, start, end, dataset, group_by, filters, temporal_resolution
        if not self.responses:
            raise RuntimeError("fixture response sequence exhausted")
        entry = self.responses.pop(0)
        if entry.get("expected_spatial_resolution", spatial_resolution) != spatial_resolution:
            raise ValueError("fixture spatial resolution mismatch")
        status = int(entry.get("status", 200))
        self.stats["post_requests"] += 1
        statuses = self.stats["http_statuses"]
        statuses[str(status)] = int(statuses.get(str(status), 0)) + 1
        payload = entry.get("payload")
        self.stats["response_body_bytes"] += int(
            entry.get("body_bytes", len(_canonical_bytes(payload)))
        )
        if status == 429:
            self.stats["status_429"] += 1
        if status == 524:
            self.stats["status_524"] += 1
        if status != 200:
            raise RuntimeError(f"offline fixture HTTP {status}")
        return payload, entry.get("resolved_dataset_version")


def _write_ndjson(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    count = 0
    with temporary.open("wb") as handle:
        for row in rows:
            handle.write(_canonical_bytes(row) + b"\n")
            count += 1
    temporary.replace(path)
    return count


def read_ndjson(path: Path) -> Iterator[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _count_ndjson(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _count_report_row_candidates(value: Any) -> int:
    """Count provider-shaped rows without retaining a second response-wide list."""
    if isinstance(value, list):
        return sum(_count_report_row_candidates(item) for item in value)
    if not isinstance(value, dict):
        return 0
    row_markers = {
        "vessel_id", "vesselId", "vesselIdRaw", "id", "ship_id",
        "longitude", "lon", "lng", "latitude", "lat", "date", "hours",
    }
    if row_markers & value.keys() and (
        {"longitude", "latitude"} <= value.keys()
        or {"lon", "lat"} <= value.keys()
        or {"lng", "lat"} <= value.keys()
    ):
        return 1
    return sum(_count_report_row_candidates(nested) for nested in value.values())


def _write_accepted_checkpoint(path: Path, normalized: Iterable[dict[str, Any]]) -> tuple[int, int]:
    """Stream a single report tile's accepted rows and count rejections once."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    accepted = rejected_invalid_coordinates = 0
    with temporary.open("wb") as handle:
        for row in normalized:
            if row.get("presence_quality") != "accepted":
                rejected_invalid_coordinates += 1
                continue
            handle.write(_canonical_bytes(_project_presence(row)) + b"\n")
            accepted += 1
    temporary.replace(path)
    return accepted, rejected_invalid_coordinates


def _assemble_checkpoint_parts(output_path: Path, parts: Iterable[Path]) -> int:
    """Atomically concatenate validated per-tile checkpoints without a daily rows list."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.name}.tmp")
    total = 0
    with temporary.open("wb") as target:
        for part in parts:
            with part.open("rb") as source:
                for line in source:
                    if line.strip():
                        target.write(line)
                        total += 1
    temporary.replace(output_path)
    return total


def fetch_presence_phase(
    *,
    client: GFWReportClient,
    resolution: str,
    selected_day: date,
    output_path: Path,
) -> dict[str, Any]:
    """Run one sequential 42-report source phase and persist no raw body."""
    if resolution not in {"LOW", "HIGH"}:
        raise ValueError("resolution must be LOW or HIGH")
    tiles = make_tiles(FIXED_BBOX, tile_size_degrees=TILE_SIZE_DEGREES)
    if len(tiles) != EXPECTED_TILE_COUNT:
        raise AssertionError("fixed East Asia bbox no longer produces 42 report tiles")
    start = selected_day.isoformat()
    end = (selected_day + timedelta(days=1)).isoformat()
    checkpoint_root = output_path.with_name(f"{output_path.name}.parts")
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    resolved_versions: set[str] = set()
    tile_ledger: list[dict[str, Any]] = []
    checkpoint_parts: list[Path] = []
    for tile_index, tile in enumerate(tiles, start=1):
        part_path = checkpoint_root / f"{tile.tile_id}.ndjson"
        ledger_path = checkpoint_root / f"{tile.tile_id}.metrics.json"
        if part_path.is_file() and ledger_path.is_file():
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            if _count_ndjson(part_path) != ledger.get("normalized_rows"):
                raise RuntimeError(f"checkpoint row count mismatch: {tile.tile_id}")
            ledger = {**ledger, "resumed": True}
            tile_ledger.append(ledger)
            checkpoint_parts.append(part_path)
            resolved_versions.add(str(ledger["resolved_dataset_version"]))
            print(f"{resolution} tile {tile_index}/{len(tiles)} resumed", flush=True)
            continue
        if part_path.exists() or ledger_path.exists():
            raise RuntimeError(f"incomplete checkpoint pair: {tile.tile_id}")
        received_at = datetime.now(timezone.utc).isoformat()
        before = json.loads(json.dumps(client.stats))
        tile_started = time.perf_counter()
        payload, resolved = client.fetch(
            tile.bbox, start, end, spatial_resolution=resolution,
        )
        if not resolved:
            raise RuntimeError(f"{resolution} tile {tile.tile_id} lacks x-datasets")
        upstream_rows = _count_report_row_candidates(payload)
        normalized = GFWVesselPresenceCollector.normalize_entries(
            payload,
            snapshot_date=start,
            received_at=received_at,
            zone=tile.tile_id,
            dataset=resolved,
        )
        accepted_count, rejected_invalid_coordinates = _write_accepted_checkpoint(part_path, normalized)
        after = dict(client.stats)
        statuses_before = before.get("http_statuses", {})
        statuses_after = after.get("http_statuses", {})
        status_delta = {
            key: int(value) - int(statuses_before.get(key, 0))
            for key, value in statuses_after.items()
            if int(value) - int(statuses_before.get(key, 0))
        }
        ledger = {
            "tile_id": tile.tile_id,
            "bbox": list(tile.bbox),
            "normalized_rows": accepted_count,
            "upstream_rows": upstream_rows,
            "rejected_missing_identity": upstream_rows - len(normalized),
            "rejected_invalid_coordinates": rejected_invalid_coordinates,
            "next_offset_complete": True,
            "resolved_dataset_version": resolved,
            "wall_time_seconds": round(time.perf_counter() - tile_started, 6),
            "peak_rss_bytes": _peak_rss_bytes(),
            "response_body_bytes": int(after.get("response_body_bytes", 0)) - int(before.get("response_body_bytes", 0)),
            "declared_content_length_bytes": int(after.get("declared_content_length_bytes", 0)) - int(before.get("declared_content_length_bytes", 0)),
            "responses_with_content_length": int(after.get("responses_with_content_length", 0)) - int(before.get("responses_with_content_length", 0)),
            "http_statuses": status_delta,
            "retries": int(after.get("retries", 0)) - int(before.get("retries", 0)),
            "status_429": int(after.get("status_429", 0)) - int(before.get("status_429", 0)),
            "status_524": int(after.get("status_524", 0)) - int(before.get("status_524", 0)),
            "post_requests": int(after.get("post_requests", 0)) - int(before.get("post_requests", 0)),
            "recovery_requests": int(after.get("recovery_requests", 0)) - int(before.get("recovery_requests", 0)),
            "resumed": False,
        }
        _atomic_json(ledger_path, ledger)
        resolved_versions.add(resolved)
        tile_ledger.append(ledger)
        checkpoint_parts.append(part_path)
        print(f"{resolution} tile {tile_index}/{len(tiles)} complete", flush=True)
    if len(resolved_versions) != 1:
        raise RuntimeError(f"{resolution} phase resolved multiple dataset versions")
    normalized_row_count = _assemble_checkpoint_parts(output_path, checkpoint_parts)
    expected_row_count = sum(int(item["normalized_rows"]) for item in tile_ledger)
    if normalized_row_count != expected_row_count:
        raise RuntimeError("assembled checkpoint row count mismatch")
    status_totals: dict[str, int] = defaultdict(int)
    for ledger in tile_ledger:
        for key, value in ledger["http_statuses"].items():
            status_totals[key] += int(value)
    return {
        "resolution": resolution,
        "logical_report_count": len(tiles),
        "report_page_count": len(tiles),
        "normalized_row_count": normalized_row_count,
        "upstream_row_count": sum(item["upstream_rows"] for item in tile_ledger),
        "rejected_missing_identity": sum(item["rejected_missing_identity"] for item in tile_ledger),
        "rejected_invalid_coordinates": sum(item["rejected_invalid_coordinates"] for item in tile_ledger),
        "resolved_dataset_versions": sorted(resolved_versions),
        "wall_time_seconds": round(sum(item["wall_time_seconds"] for item in tile_ledger), 6),
        "peak_rss_bytes": max(item["peak_rss_bytes"] for item in tile_ledger),
        "response_body_bytes": sum(item["response_body_bytes"] for item in tile_ledger),
        "declared_content_length_bytes": sum(item["declared_content_length_bytes"] for item in tile_ledger),
        "responses_with_content_length": sum(item["responses_with_content_length"] for item in tile_ledger),
        "http_statuses": dict(status_totals),
        "retries": sum(item["retries"] for item in tile_ledger),
        "status_429": sum(item["status_429"] for item in tile_ledger),
        "status_524": sum(item["status_524"] for item in tile_ledger),
        "post_requests": sum(item["post_requests"] for item in tile_ledger),
        "recovery_requests": sum(item["recovery_requests"] for item in tile_ledger),
        "raw_response_saved": False,
        "checkpoint_contract": {
            "mode": "per_tile_ndjson_atomic_then_streamed_assembly",
            "checkpoint_count": len(checkpoint_parts),
            "resumed_checkpoint_count": sum(
                1 for item in tile_ledger if bool(item.get("resumed"))
            ),
        },
        "tiles": tile_ledger,
    }


def run_presence_phase_isolated(
    *, resolution: str, selected_day: date, output_path: Path,
    metrics_path: Path, fixture_path: Path | None = None,
) -> dict[str, Any]:
    """Run one phase in a fresh process so ru_maxrss is phase-local.

    LOW and HIGH must be invoked sequentially by the caller.  The access token
    is read backend-only by the worker and is never placed in argv or output.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable, str(Path(__file__).resolve()), "--_presence-worker",
        "--resolution", resolution,
        "--selected-day", selected_day.isoformat(),
        "--phase-output", str(output_path),
        "--phase-metrics", str(metrics_path),
    ]
    if fixture_path is not None:
        command.extend(("--phase-fixture", str(fixture_path)))
    subprocess.run(command, check=True)
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    if metrics.get("resolution") != resolution:
        raise RuntimeError("isolated presence phase metrics resolution mismatch")
    return metrics


def _presence_worker_main(arguments: list[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--resolution", choices=("LOW", "HIGH"), required=True)
    parser.add_argument("--selected-day", type=date.fromisoformat, required=True)
    parser.add_argument("--phase-output", type=Path, required=True)
    parser.add_argument("--phase-metrics", type=Path, required=True)
    parser.add_argument("--phase-fixture", type=Path)
    args = parser.parse_args(arguments)
    if args.phase_output.exists() or args.phase_metrics.exists():
        parser.error("isolated phase outputs must not already exist")
    if args.phase_fixture is not None:
        client: Any = FixtureSequenceReportClient(_load_fixture(args.phase_fixture))
    else:
        if not config.GFW_ACCESS_TOKEN:
            parser.error("GFW_ACCESS_TOKEN is required for a live isolated phase")
        client = MeasuredGFWReportClient(config.GFW_ACCESS_TOKEN)
    metrics = fetch_presence_phase(
        client=client, resolution=args.resolution,
        selected_day=args.selected_day, output_path=args.phase_output,
    )
    _atomic_json(args.phase_metrics, metrics)
    return 0


@dataclass
class PresenceIndex:
    members_by_key: dict[tuple[str, tuple[int, int], str], dict[str, Any]]
    global_vessel_hours: set[tuple[str, str]]
    cell_by_vessel_hour: dict[tuple[str, str], tuple[int, int]]
    stats: dict[str, Any]


def index_presence(rows: Iterable[dict[str, Any]]) -> PresenceIndex:
    """Canonicalize one point per vessel/hour and retain complete popup identity."""
    exact_seen: set[tuple[Any, ...]] = set()
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    invalid_rows = exact_duplicates = 0
    for source in rows:
        try:
            vessel_id = str(source["vessel_id"]).strip()
            observed_at = _parse_utc(str(source["observed_at"])).isoformat()
            longitude = float(source["longitude"])
            latitude = float(source["latitude"])
            cell = canonical_cell(longitude, latitude)
        except (KeyError, TypeError, ValueError, OverflowError):
            invalid_rows += 1
            continue
        if not vessel_id:
            invalid_rows += 1
            continue
        row = {
            "vessel_id": vessel_id,
            "observed_at": observed_at,
            "longitude": longitude,
            "latitude": latitude,
            "cell": cell,
            "mmsi": _clean_text(source.get("mmsi")),
            "ship_name": _clean_text(source.get("ship_name")),
            "vessel_type": _clean_text(source.get("vessel_type")),
            "flag": _clean_text(source.get("flag")),
        }
        exact = tuple(row.get(field) for field in (
            "vessel_id", "observed_at", "longitude", "latitude",
            "mmsi", "ship_name", "vessel_type", "flag",
        ))
        if exact in exact_seen:
            exact_duplicates += 1
            continue
        exact_seen.add(exact)
        grouped[(_utc_hour(observed_at), vessel_id)].append(row)

    members_by_key: dict[tuple[str, tuple[int, int], str], dict[str, Any]] = {}
    global_vessel_hours: set[tuple[str, str]] = set()
    cell_by_vessel_hour: dict[tuple[str, str], tuple[int, int]] = {}
    conflict_count = popup_conflict_count = 0
    null_counts = {field: 0 for field in POPUP_FIELDS[1:]}
    for vessel_hour in sorted(grouped):
        candidates = sorted(
            grouped[vessel_hour],
            key=lambda row: (
                row["observed_at"], row["longitude"], row["latitude"],
                *(row.get(field) or "" for field in POPUP_FIELDS[1:]),
            ),
        )
        if len({row["cell"] for row in candidates}) > 1:
            conflict_count += 1
        if len({tuple(row.get(field) for field in POPUP_FIELDS) for row in candidates}) > 1:
            popup_conflict_count += 1
        selected = candidates[0]
        member = {field: selected.get(field) for field in POPUP_FIELDS}
        for field in POPUP_FIELDS[1:]:
            if member[field] is None:
                null_counts[field] += 1
        hour, vessel_id = vessel_hour
        key = (hour, selected["cell"], vessel_id)
        members_by_key[key] = member
        global_vessel_hours.add(vessel_hour)
        cell_by_vessel_hour[vessel_hour] = selected["cell"]
    denominator = len(global_vessel_hours)
    return PresenceIndex(
        members_by_key=members_by_key,
        global_vessel_hours=global_vessel_hours,
        cell_by_vessel_hour=cell_by_vessel_hour,
        stats={
            "input_rows": len(exact_seen) + exact_duplicates + invalid_rows,
            "valid_unique_rows": len(exact_seen),
            "invalid_rows": invalid_rows,
            "exact_duplicate_rows": exact_duplicates,
            "same_vessel_hour_cell_conflicts": conflict_count,
            "same_vessel_hour_popup_conflicts": popup_conflict_count,
            "canonical_vessel_hours": denominator,
            "identity_field_null_counts": null_counts,
            "identity_field_null_rates": {
                field: (count / denominator if denominator else 0.0)
                for field, count in null_counts.items()
            },
        },
    )


def _sample(values: Iterable[Any], limit: int = 100) -> list[Any]:
    return list(sorted(values, key=str))[:limit]


def compare_presence(low: PresenceIndex, high: PresenceIndex) -> dict[str, Any]:
    low_global, high_global = low.global_vessel_hours, high.global_vessel_hours
    low_keys, high_keys = set(low.members_by_key), set(high.members_by_key)
    common_global = low_global & high_global
    boundary_mismatches = {
        vessel_hour for vessel_hour in common_global
        if low.cell_by_vessel_hour[vessel_hour] != high.cell_by_vessel_hour[vessel_hour]
    }
    member_mismatches = {
        key for key in low_keys & high_keys
        if low.members_by_key[key] != high.members_by_key[key]
    }
    return {
        "parity_key": ["UTC_hour", "canonical_0_1_cell", "vessel_id"],
        "global_vessel_hour": {
            "low_count": len(low_global),
            "high_count": len(high_global),
            "equal": low_global == high_global,
            "missing_from_low_count": len(high_global - low_global),
            "missing_from_high_count": len(low_global - high_global),
            "missing_from_low_sample": _sample(high_global - low_global),
            "missing_from_high_sample": _sample(low_global - high_global),
        },
        "per_cell_members": {
            "low_count": len(low_keys),
            "high_count": len(high_keys),
            "equal": low_keys == high_keys and not member_mismatches,
            "missing_from_low_count": len(high_keys - low_keys),
            "missing_from_high_count": len(low_keys - high_keys),
            "missing_from_low_sample": _sample(high_keys - low_keys),
            "missing_from_high_sample": _sample(low_keys - high_keys),
            "popup_member_mismatch_count": len(member_mismatches),
            "popup_member_mismatch_sample": _sample(member_mismatches),
        },
        "boundary_assignment_mismatch_count": len(boundary_mismatches),
        "boundary_assignment_mismatch_sample": _sample(boundary_mismatches),
        "low_quality": low.stats,
        "high_quality": high.stats,
    }


def compare_presence_ndjson(
    low_path: Path, high_path: Path, *, sqlite_path: Path,
) -> dict[str, Any]:
    """Disk-backed LOW/HIGH canonical comparison for full-day live shards."""
    if sqlite_path.exists():
        raise FileExistsError(f"comparison SQLite already exists: {sqlite_path}")
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(sqlite_path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("""
        CREATE TABLE raw (
          source TEXT NOT NULL, hour TEXT NOT NULL, vessel_id TEXT NOT NULL,
          observed_at TEXT NOT NULL, lon REAL NOT NULL, lat REAL NOT NULL,
          cell_lon INTEGER NOT NULL, cell_lat INTEGER NOT NULL,
          member_json TEXT NOT NULL, row_hash TEXT NOT NULL
        )
    """)
    input_stats: dict[str, dict[str, int]] = {}
    try:
        for source, path in (("LOW", low_path), ("HIGH", high_path)):
            input_rows = invalid_rows = 0
            batch = []
            for row in read_ndjson(path):
                input_rows += 1
                try:
                    vessel_id = str(row["vessel_id"]).strip()
                    observed_at = _parse_utc(str(row["observed_at"])).isoformat()
                    hour = _utc_hour(observed_at)
                    lon, lat = float(row["longitude"]), float(row["latitude"])
                    cell_lon, cell_lat = canonical_cell(lon, lat)
                except (KeyError, TypeError, ValueError, OverflowError):
                    invalid_rows += 1
                    continue
                if not vessel_id:
                    invalid_rows += 1
                    continue
                member = {field: row.get(field) for field in POPUP_FIELDS}
                member["vessel_id"] = vessel_id
                member_json = _canonical_bytes(member).decode("utf-8")
                row_hash = hashlib.sha256(_canonical_bytes([
                    hour, vessel_id, observed_at, lon, lat, member,
                ])).hexdigest()
                batch.append((
                    source, hour, vessel_id, observed_at, lon, lat,
                    cell_lon, cell_lat, member_json, row_hash,
                ))
                if len(batch) >= 10_000:
                    connection.executemany("INSERT INTO raw VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
                    connection.commit()
                    batch.clear()
            if batch:
                connection.executemany("INSERT INTO raw VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
                connection.commit()
            input_stats[source] = {"input_rows": input_rows, "invalid_rows": invalid_rows}
        connection.execute("CREATE INDEX raw_identity_idx ON raw(source, hour, vessel_id)")
        connection.execute("""
            CREATE TABLE canonical AS
            SELECT source, hour, vessel_id, cell_lon, cell_lat, member_json
            FROM (
              SELECT raw.*,
                ROW_NUMBER() OVER (
                  PARTITION BY source, hour, vessel_id
                  ORDER BY observed_at, lon, lat, member_json
                ) AS canonical_rank
              FROM raw
            )
            WHERE canonical_rank = 1
        """)
        connection.execute(
            "CREATE UNIQUE INDEX canonical_identity_idx ON canonical(source, hour, vessel_id)"
        )
        connection.commit()

        def scalar(sql: str, parameters: tuple[Any, ...] = ()) -> int:
            return int(connection.execute(sql, parameters).fetchone()[0])

        def sample(sql: str) -> list[list[Any]]:
            return [list(row) for row in connection.execute(f"{sql} LIMIT 100")]

        quality: dict[str, Any] = {}
        for source in ("LOW", "HIGH"):
            canonical_count = scalar(
                "SELECT COUNT(*) FROM canonical WHERE source = ?", (source,),
            )
            exact_duplicates = scalar("""
                SELECT COUNT(*) - COUNT(DISTINCT row_hash)
                FROM raw WHERE source = ?
            """, (source,))
            cell_conflicts = scalar("""
                SELECT COUNT(*) FROM (
                  SELECT 1 FROM raw WHERE source = ?
                  GROUP BY hour, vessel_id
                  HAVING COUNT(DISTINCT printf('%d/%d', cell_lon, cell_lat)) > 1
                )
            """, (source,))
            popup_conflicts = scalar("""
                SELECT COUNT(*) FROM (
                  SELECT 1 FROM raw WHERE source = ?
                  GROUP BY hour, vessel_id HAVING COUNT(DISTINCT member_json) > 1
                )
            """, (source,))
            null_counts = {field: 0 for field in POPUP_FIELDS[1:]}
            for (member_json,) in connection.execute(
                "SELECT member_json FROM canonical WHERE source = ?", (source,),
            ):
                member = json.loads(member_json)
                for field in null_counts:
                    if member.get(field) is None:
                        null_counts[field] += 1
            quality[source] = {
                **input_stats[source],
                "valid_unique_rows": input_stats[source]["input_rows"] - input_stats[source]["invalid_rows"],
                "exact_duplicate_rows": exact_duplicates,
                "same_vessel_hour_cell_conflicts": cell_conflicts,
                "same_vessel_hour_popup_conflicts": popup_conflicts,
                "canonical_vessel_hours": canonical_count,
                "identity_field_null_counts": null_counts,
                "identity_field_null_rates": {
                    field: (count / canonical_count if canonical_count else 0.0)
                    for field, count in null_counts.items()
                },
            }

        low_count = quality["LOW"]["canonical_vessel_hours"]
        high_count = quality["HIGH"]["canonical_vessel_hours"]
        missing_from_low = scalar("""
            SELECT COUNT(*) FROM canonical h
            LEFT JOIN canonical l ON l.source='LOW' AND l.hour=h.hour AND l.vessel_id=h.vessel_id
            WHERE h.source='HIGH' AND l.vessel_id IS NULL
        """)
        missing_from_high = scalar("""
            SELECT COUNT(*) FROM canonical l
            LEFT JOIN canonical h ON h.source='HIGH' AND h.hour=l.hour AND h.vessel_id=l.vessel_id
            WHERE l.source='LOW' AND h.vessel_id IS NULL
        """)
        boundary_mismatches = scalar("""
            SELECT COUNT(*) FROM canonical l JOIN canonical h
              ON l.hour=h.hour AND l.vessel_id=h.vessel_id
            WHERE l.source='LOW' AND h.source='HIGH'
              AND (l.cell_lon != h.cell_lon OR l.cell_lat != h.cell_lat)
        """)
        member_mismatches = scalar("""
            SELECT COUNT(*) FROM canonical l JOIN canonical h
              ON l.hour=h.hour AND l.vessel_id=h.vessel_id
            WHERE l.source='LOW' AND h.source='HIGH'
              AND l.cell_lon=h.cell_lon AND l.cell_lat=h.cell_lat
              AND l.member_json != h.member_json
        """)
        low_keys = low_count
        high_keys = high_count
        return {
            "parity_key": ["UTC_hour", "canonical_0_1_cell", "vessel_id"],
            "global_vessel_hour": {
                "low_count": low_count, "high_count": high_count,
                "equal": missing_from_low == 0 and missing_from_high == 0,
                "missing_from_low_count": missing_from_low,
                "missing_from_high_count": missing_from_high,
                "missing_from_low_sample": sample("""
                    SELECT h.hour,h.vessel_id FROM canonical h LEFT JOIN canonical l
                      ON l.source='LOW' AND l.hour=h.hour AND l.vessel_id=h.vessel_id
                    WHERE h.source='HIGH' AND l.vessel_id IS NULL ORDER BY h.hour,h.vessel_id
                """),
                "missing_from_high_sample": sample("""
                    SELECT l.hour,l.vessel_id FROM canonical l LEFT JOIN canonical h
                      ON h.source='HIGH' AND h.hour=l.hour AND h.vessel_id=l.vessel_id
                    WHERE l.source='LOW' AND h.vessel_id IS NULL ORDER BY l.hour,l.vessel_id
                """),
            },
            "per_cell_members": {
                "low_count": low_keys, "high_count": high_keys,
                "equal": missing_from_low == 0 and missing_from_high == 0
                    and boundary_mismatches == 0 and member_mismatches == 0,
                "missing_from_low_count": missing_from_low + boundary_mismatches,
                "missing_from_high_count": missing_from_high + boundary_mismatches,
                "popup_member_mismatch_count": member_mismatches,
                "popup_member_mismatch_sample": sample("""
                    SELECT l.hour,l.vessel_id,l.cell_lon,l.cell_lat
                    FROM canonical l JOIN canonical h
                      ON l.hour=h.hour AND l.vessel_id=h.vessel_id
                    WHERE l.source='LOW' AND h.source='HIGH'
                      AND l.cell_lon=h.cell_lon AND l.cell_lat=h.cell_lat
                      AND l.member_json != h.member_json
                    ORDER BY l.hour,l.vessel_id
                """),
            },
            "boundary_assignment_mismatch_count": boundary_mismatches,
            "boundary_assignment_mismatch_sample": sample("""
                SELECT l.hour,l.vessel_id,l.cell_lon,l.cell_lat,h.cell_lon,h.cell_lat
                FROM canonical l JOIN canonical h
                  ON l.hour=h.hour AND l.vessel_id=h.vessel_id
                WHERE l.source='LOW' AND h.source='HIGH'
                  AND (l.cell_lon != h.cell_lon OR l.cell_lat != h.cell_lat)
                ORDER BY l.hour,l.vessel_id
            """),
            "low_quality": quality["LOW"],
            "high_quality": quality["HIGH"],
            "sqlite_path": str(sqlite_path),
        }
    finally:
        connection.close()
def _asset(path: Path, *, root: Path, asset_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
        "type": asset_type,
        **extra,
    }


def _split_detail_payloads(
    items: list[tuple[str, dict[str, Any]]], *,
    selected_hour: str, target_compressed_bytes: int,
) -> list[tuple[dict[str, Any], bytes]]:
    def payload_for(chunk: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
        entries = dict(chunk)
        return {
            "schema_version": 1,
            "observed_at": selected_hour,
            "key": "cell_id",
            "entry_count": len(entries),
            "vessel_count": sum(value["vessel_count"] for value in entries.values()),
            "entries": entries,
        }

    def split(chunk: list[tuple[str, dict[str, Any]]]) -> list[tuple[dict[str, Any], bytes]]:
        payload = payload_for(chunk)
        compressed = _gzip_bytes(payload)
        if len(compressed) <= target_compressed_bytes or len(chunk) <= 1:
            return [(payload, compressed)]
        middle = len(chunk) // 2
        return split(chunk[:middle]) + split(chunk[middle:])

    return split(items) if items else [(payload_for([]), _gzip_bytes(payload_for([])))]


def build_grid_artifacts(
    high: PresenceIndex,
    *,
    selected_day: date,
    root: Path,
    pmtiles_builder: Callable[..., None] = _pmtiles,
    detail_target_compressed_bytes: int = 256 * 1024,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    grouped: dict[tuple[str, tuple[int, int]], list[dict[str, Any]]] = defaultdict(list)
    for (hour, cell, _vessel_id), member in high.members_by_key.items():
        if hour[:10] == selected_day.isoformat():
            grouped[(hour, cell)].append(member)
    assets: list[dict[str, Any]] = []
    hours_index = []
    for hour in _day_hours(selected_day):
        stamp = _parse_utc(hour).strftime("%Y%m%dT%HZ")
        cells = []
        for (candidate_hour, cell), members in sorted(grouped.items()):
            if candidate_hour != hour:
                continue
            members = sorted(members, key=lambda member: member["vessel_id"])
            cell_id = _cell_id(hour, cell)
            cells.append((cell_id, cell, {
                "vessel_count": len(members), "members": members,
            }))
        detail_items = [(cell_id, detail) for cell_id, _cell, detail in cells]
        detail_payloads = _split_detail_payloads(
            detail_items,
            selected_hour=hour,
            target_compressed_bytes=detail_target_compressed_bytes,
        )
        shard_by_cell: dict[str, str] = {}
        detail_index = []
        for index, (payload, compressed) in enumerate(detail_payloads):
            detail_path = root / "grid" / "details" / stamp / f"part-{index:04d}.json.gz"
            _atomic_bytes(detail_path, compressed)
            shard_name = detail_path.name
            for cell_id in payload["entries"]:
                shard_by_cell[cell_id] = shard_name
            entry = _asset(
                detail_path, root=root, asset_type="grid_detail",
                features=payload["entry_count"], vessels=payload["vessel_count"],
            )
            detail_index.append(entry)
            assets.append(entry)
        features = []
        for cell_id, cell, detail in cells:
            features.append({
                "type": "Feature", "id": cell_id,
                "properties": {
                    "cell_id": cell_id, "observed_at": hour,
                    "center_lon": cell[0] / 100, "center_lat": cell[1] / 100,
                    "vessel_count": detail["vessel_count"],
                    "detail_shard": shard_by_cell[cell_id],
                    "geometry_semantics": "globally_aligned_0_1_degree_cell",
                },
                "geometry": {"type": "Polygon", "coordinates": _cell_polygon(cell)},
            })
        ndjson = root / ".inputs" / "grid" / f"{stamp}.ndjson"
        _write_ndjson(ndjson, features)
        pmtiles_path = root / "grid" / "hours" / f"{stamp}.pmtiles"
        pmtiles_builder(
            named_inputs=[("gfw_grid_0_1", ndjson)], output=pmtiles_path,
            minimum_zoom=3, maximum_zoom=10,
        )
        ndjson.unlink()
        pmtiles_asset = _asset(
            pmtiles_path, root=root, asset_type="grid_hour_pmtiles",
            features=len(features), vessels=sum(value[2]["vessel_count"] for value in cells),
        )
        assets.append(pmtiles_asset)
        if sum(entry["vessels"] for entry in detail_index) != pmtiles_asset["vessels"]:
            raise RuntimeError("grid PMTiles count does not match complete detail membership")
        hours_index.append({
            "observed_at": hour,
            "pmtiles": pmtiles_asset,
            "details": detail_index,
        })
    input_grid = root / ".inputs" / "grid"
    if input_grid.is_dir():
        input_grid.rmdir()
    input_root = root / ".inputs"
    if input_root.is_dir():
        input_root.rmdir()
    return {
        "resolution_degrees": 0.1,
        "source": "HIGH locally aggregated to globally aligned 0.1-degree cells",
        "source_layer": "gfw_grid_0_1",
        "hour_count": 24,
        "detail_target_compressed_bytes": detail_target_compressed_bytes,
        "hours": hours_index,
    }, assets


def _protobuf_fields(payload: bytes) -> Iterator[tuple[int, int, Any]]:
    """Yield the small protobuf wire subset needed for MVT property readback."""
    offset = 0

    def varint() -> int:
        nonlocal offset
        value = 0
        shift = 0
        while offset < len(payload):
            byte = payload[offset]
            offset += 1
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return value
            shift += 7
            if shift >= 70:
                raise ValueError("invalid protobuf varint")
        raise ValueError("truncated protobuf varint")

    while offset < len(payload):
        key = varint()
        field_number, wire_type = key >> 3, key & 7
        if wire_type == 0:
            value: Any = varint()
        elif wire_type == 1:
            if offset + 8 > len(payload):
                raise ValueError("truncated protobuf fixed64")
            value = payload[offset:offset + 8]
            offset += 8
        elif wire_type == 2:
            length = varint()
            if offset + length > len(payload):
                raise ValueError("truncated protobuf bytes")
            value = payload[offset:offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(payload):
                raise ValueError("truncated protobuf fixed32")
            value = payload[offset:offset + 4]
            offset += 4
        else:
            raise ValueError(f"unsupported protobuf wire type: {wire_type}")
        yield field_number, wire_type, value


def _mvt_value(payload: bytes) -> Any:
    for field, wire_type, value in _protobuf_fields(payload):
        if field == 1 and wire_type == 2:
            return value.decode("utf-8")
        if field == 2 and wire_type == 5:
            return struct.unpack("<f", value)[0]
        if field == 3 and wire_type == 1:
            return struct.unpack("<d", value)[0]
        if field in {4, 5} and wire_type == 0:
            return value
        if field == 6 and wire_type == 0:
            return (value >> 1) ^ -(value & 1)
        if field == 7 and wire_type == 0:
            return bool(value)
    return None


def _raw_varints(payload: bytes) -> Iterator[int]:
    offset = 0
    while offset < len(payload):
        value = 0
        shift = 0
        while True:
            if offset >= len(payload):
                raise ValueError("truncated packed protobuf varint")
            byte = payload[offset]
            offset += 1
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        yield value


def _decode_mvt_properties(tile_data: bytes, *, source_layer: str) -> list[dict[str, Any]]:
    """Decode feature properties only; geometry is intentionally not materialized."""
    if tile_data[:2] == b"\x1f\x8b":
        tile_data = gzip.decompress(tile_data)
    decoded: list[dict[str, Any]] = []
    for field, wire_type, layer_payload in _protobuf_fields(tile_data):
        if field != 3 or wire_type != 2:
            continue
        layer_name = None
        features: list[bytes] = []
        keys: list[str] = []
        values: list[Any] = []
        for layer_field, layer_wire, layer_value in _protobuf_fields(layer_payload):
            if layer_field == 1 and layer_wire == 2:
                layer_name = layer_value.decode("utf-8")
            elif layer_field == 2 and layer_wire == 2:
                features.append(layer_value)
            elif layer_field == 3 and layer_wire == 2:
                keys.append(layer_value.decode("utf-8"))
            elif layer_field == 4 and layer_wire == 2:
                values.append(_mvt_value(layer_value))
        if layer_name != source_layer:
            continue
        for feature_payload in features:
            tags: list[int] = []
            for feature_field, feature_wire, feature_value in _protobuf_fields(feature_payload):
                if feature_field == 2 and feature_wire == 2:
                    tags.extend(_raw_varints(feature_value))
            if len(tags) % 2:
                raise ValueError("MVT feature contains an odd property tag count")
            properties = {}
            for index in range(0, len(tags), 2):
                key_index, value_index = tags[index:index + 2]
                if key_index >= len(keys) or value_index >= len(values):
                    raise ValueError("MVT feature property index outside layer tables")
                properties[keys[key_index]] = values[value_index]
            decoded.append(properties)
    return decoded


def _pmtiles_directory(payload: bytes) -> list[tuple[int, int, int, int]]:
    """Decode PMTiles v3 directory entries as tile_id, offset, length, run_length."""
    values = iter(_raw_varints(payload))
    try:
        count = next(values)
        tile_ids = []
        current_id = 0
        for _ in range(count):
            current_id += next(values)
            tile_ids.append(current_id)
        run_lengths = [next(values) for _ in range(count)]
        lengths = [next(values) for _ in range(count)]
        offsets = []
        for index in range(count):
            encoded = next(values)
            if encoded == 0 and index:
                offsets.append(offsets[index - 1] + lengths[index - 1])
            else:
                offsets.append(encoded - 1)
    except StopIteration as error:
        raise ValueError("truncated PMTiles directory") from error
    return list(zip(tile_ids, offsets, lengths, run_lengths))


def _decompress_pmtiles_blob(payload: bytes, compression: int) -> bytes:
    if compression == 1:
        return payload
    if compression == 2:
        return gzip.decompress(payload)
    raise ValueError(f"unsupported PMTiles internal compression: {compression}")


def _iter_pmtiles_maxzoom_tiles(
    pmtiles_path: Path, *, maxzoom: int,
) -> Iterator[tuple[int, bytes]]:
    """Enumerate every logical maxzoom tile directly from PMTiles v3 directories."""
    with pmtiles_path.open("rb") as handle:
        header = handle.read(127)
        if len(header) != 127 or header[:7] != b"PMTiles" or header[7] != 3:
            raise ValueError("unsupported PMTiles header/version")
        root_offset, root_length = struct.unpack_from("<QQ", header, 8)
        leaf_offset = struct.unpack_from("<Q", header, 40)[0]
        tile_data_offset = struct.unpack_from("<Q", header, 56)[0]
        internal_compression = header[97]
        header_maxzoom = header[101]
        if header_maxzoom != maxzoom:
            raise ValueError("PMTiles CLI/header maxzoom mismatch")
        zoom_start = (4 ** maxzoom - 1) // 3
        zoom_end = zoom_start + 4 ** maxzoom

        def read_blob(offset: int, length: int) -> bytes:
            handle.seek(offset)
            payload = handle.read(length)
            if len(payload) != length:
                raise ValueError("truncated PMTiles archive")
            return payload

        def walk(directory_payload: bytes) -> Iterator[tuple[int, bytes]]:
            directory = _pmtiles_directory(
                _decompress_pmtiles_blob(directory_payload, internal_compression)
            )
            for tile_id, offset, length, run_length in directory:
                if run_length == 0:
                    yield from walk(read_blob(leaf_offset + offset, length))
                    continue
                first = max(tile_id, zoom_start)
                last = min(tile_id + run_length, zoom_end)
                if first >= last:
                    continue
                tile_data = read_blob(tile_data_offset + offset, length)
                for logical_tile_id in range(first, last):
                    yield logical_tile_id, tile_data

        yield from walk(read_blob(root_offset, root_length))


def semantic_readback_grid_pmtiles(
    pmtiles_path: Path,
    *,
    expected_features: Iterable[dict[str, Any]],
    source_layer: str = "gfw_grid_0_1",
    pmtiles_cli: Path = Path("/opt/homebrew/bin/pmtiles"),
) -> dict[str, Any]:
    """Decode every maxzoom tile and compare the wire properties to build input."""
    expected = {
        str(feature["properties"]["cell_id"]): {
            "vessel_count": int(feature["properties"]["vessel_count"]),
            "detail_shard": str(feature["properties"]["detail_shard"]),
        }
        for feature in expected_features
    }
    header_result = subprocess.run(
        [str(pmtiles_cli), "show", str(pmtiles_path), "--header-json"],
        check=True, capture_output=True, text=True,
    )
    header = json.loads(header_result.stdout)
    if header.get("tile_type") != "mvt":
        raise RuntimeError(f"PMTiles is not MVT: {pmtiles_path}")
    maxzoom = int(header["maxzoom"])
    observed: dict[str, dict[str, Any]] = {}
    feature_occurrences = 0
    decoded_tile_count = 0
    for _tile_id, tile_data in _iter_pmtiles_maxzoom_tiles(
        pmtiles_path, maxzoom=maxzoom,
    ):
        decoded_tile_count += 1
        for properties in _decode_mvt_properties(tile_data, source_layer=source_layer):
            feature_occurrences += 1
            cell_id = str(properties.get("cell_id"))
            actual = {
                "vessel_count": int(properties.get("vessel_count")),
                "detail_shard": str(properties.get("detail_shard")),
            }
            if cell_id not in expected:
                raise RuntimeError(f"unexpected maxzoom Grid cell: {cell_id}")
            if actual != expected[cell_id]:
                raise RuntimeError(
                    f"maxzoom Grid property mismatch for {cell_id}: "
                    f"expected {expected[cell_id]}, got {actual}"
                )
            previous = observed.setdefault(cell_id, actual)
            if previous != actual:
                raise RuntimeError(f"conflicting maxzoom Grid copies for {cell_id}")
    missing = sorted(set(expected) - set(observed))
    if missing:
        raise RuntimeError(f"maxzoom Grid readback missing cells: {missing[:10]}")
    return {
        "status": "passed",
        "source_layer": source_layer,
        "maxzoom": maxzoom,
        "decoded_maxzoom_tiles": decoded_tile_count,
        "feature_occurrences": feature_occurrences,
        "unique_cells": len(observed),
        "expected_cells": len(expected),
        "properties": ["cell_id", "vessel_count", "detail_shard"],
    }


def build_grid_artifacts_from_compare_sqlite(
    sqlite_path: Path,
    *,
    selected_day: date,
    root: Path,
    pmtiles_builder: Callable[..., None] = _pmtiles,
    detail_target_compressed_bytes: int = 256 * 1024,
    semantic_readback: bool = True,
    pmtiles_cli: Path | None = None,
    include_member: Callable[[dict[str, Any]], bool] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build 24 Grid hours from canonical HIGH rows, retaining one hour in memory.

    ``include_member`` is an explicit projection gate for a downstream release
    contract (for example, excluding non-vessel GEAR/FAD observations).  The
    default deliberately retains the historical POC semantics.
    """
    connection = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    assets: list[dict[str, Any]] = []
    hours_index = []
    max_hour_members = max_hour_cells = 0
    try:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(canonical)")
        }
        required = {"source", "hour", "vessel_id", "cell_lon", "cell_lat", "member_json"}
        if not required <= columns:
            raise ValueError("compare SQLite canonical table has an incompatible schema")
        for hour in _day_hours(selected_day):
            grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
            cursor = connection.execute(
                "SELECT vessel_id,cell_lon,cell_lat,member_json FROM canonical "
                "WHERE source='HIGH' AND hour=? ORDER BY cell_lon,cell_lat,vessel_id",
                (hour,),
            )
            for vessel_id, cell_lon, cell_lat, member_json in cursor:
                member = json.loads(member_json)
                member["vessel_id"] = str(vessel_id)
                if include_member is not None and not include_member(member):
                    continue
                grouped[(int(cell_lon), int(cell_lat))].append(
                    {field: member.get(field) for field in POPUP_FIELDS}
                )
            max_hour_members = max(
                max_hour_members, sum(len(members) for members in grouped.values()),
            )
            max_hour_cells = max(max_hour_cells, len(grouped))
            stamp = _parse_utc(hour).strftime("%Y%m%dT%HZ")
            cells = []
            for cell, members in sorted(grouped.items()):
                members.sort(key=lambda member: member["vessel_id"])
                cell_id = _cell_id(hour, cell)
                cells.append((cell_id, cell, {
                    "vessel_count": len(members), "members": members,
                }))
            detail_payloads = _split_detail_payloads(
                [(cell_id, detail) for cell_id, _cell, detail in cells],
                selected_hour=hour,
                target_compressed_bytes=detail_target_compressed_bytes,
            )
            shard_by_cell: dict[str, str] = {}
            detail_index = []
            for index, (payload, compressed) in enumerate(detail_payloads):
                detail_path = root / "grid" / "details" / stamp / f"part-{index:04d}.json.gz"
                _atomic_bytes(detail_path, compressed)
                for cell_id in payload["entries"]:
                    shard_by_cell[cell_id] = detail_path.name
                entry = _asset(
                    detail_path, root=root, asset_type="grid_detail",
                    features=payload["entry_count"], vessels=payload["vessel_count"],
                )
                detail_index.append(entry)
                assets.append(entry)
            features = [{
                "type": "Feature", "id": cell_id,
                "properties": {
                    "cell_id": cell_id, "observed_at": hour,
                    "center_lon": cell[0] / 100, "center_lat": cell[1] / 100,
                    "vessel_count": detail["vessel_count"],
                    "detail_shard": shard_by_cell[cell_id],
                    "geometry_semantics": "globally_aligned_0_1_degree_cell",
                },
                "geometry": {"type": "Polygon", "coordinates": _cell_polygon(cell)},
            } for cell_id, cell, detail in cells]
            ndjson = root / ".inputs" / "grid" / f"{stamp}.ndjson"
            _write_ndjson(ndjson, features)
            pmtiles_path = root / "grid" / "hours" / f"{stamp}.pmtiles"
            pmtiles_builder(
                named_inputs=[("gfw_grid_0_1", ndjson)], output=pmtiles_path,
                minimum_zoom=3, maximum_zoom=10,
            )
            ndjson.unlink()
            pmtiles_asset = _asset(
                pmtiles_path, root=root, asset_type="grid_hour_pmtiles",
                features=len(features),
                vessels=sum(detail["vessel_count"] for _cell_id, _cell, detail in cells),
            )
            if semantic_readback:
                pmtiles_asset["semantic_readback"] = semantic_readback_grid_pmtiles(
                    pmtiles_path,
                    expected_features=features,
                    pmtiles_cli=pmtiles_cli or require_gfw_asset_toolchain()[1],
                )
            assets.append(pmtiles_asset)
            if sum(entry["vessels"] for entry in detail_index) != pmtiles_asset["vessels"]:
                raise RuntimeError("grid PMTiles count does not match complete detail membership")
            hours_index.append({
                "observed_at": hour, "pmtiles": pmtiles_asset, "details": detail_index,
            })
    finally:
        connection.close()
    input_grid = root / ".inputs" / "grid"
    if input_grid.is_dir():
        input_grid.rmdir()
    input_root = root / ".inputs"
    if input_root.is_dir():
        input_root.rmdir()
    return {
        "resolution_degrees": 0.1,
        "source": "compare SQLite canonical source='HIGH' locally aggregated",
        "source_layer": "gfw_grid_0_1",
        "hour_count": 24,
        "hour_query_count": 24,
        "memory_scope": "one UTC hour plus one adaptive detail shard",
        "max_hour_members": max_hour_members,
        "max_hour_cells": max_hour_cells,
        "detail_target_compressed_bytes": detail_target_compressed_bytes,
        "hours": hours_index,
    }, assets


def ship_type_bucket(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"CARGO", "CARRIER"}:
        return "cargo"
    if text == "TANKER":
        return "tanker"
    if text == "PASSENGER":
        return "passenger"
    if text == "FISHING":
        return "fishing"
    return "other"


def _selected_day_segment(
    feature: dict[str, Any], selected_day: date,
    *, popup_member_lookup: Callable[[str, str], dict[str, Any] | None] | None = None,
) -> dict[str, Any] | None:
    props = feature["properties"]
    coordinates = feature["geometry"]["coordinates"]
    if feature["geometry"]["type"] == "Point":
        coordinates = [coordinates]
    points = [
        (coordinate, _parse_utc(timestamp))
        for coordinate, timestamp in zip(coordinates, props["observed_times"])
        if _parse_utc(timestamp).date() == selected_day
    ]
    if not points:
        return None
    vessel_id = str(props["vessel_id"])
    fallback_member = {field: props.get(field) for field in POPUP_FIELDS}
    fallback_member["vessel_id"] = vessel_id
    point_members = []
    for _coordinate, observed_at in points:
        member = None
        if popup_member_lookup is not None:
            member = popup_member_lookup(vessel_id, observed_at.isoformat())
        point_members.append({
            field: (member or fallback_member).get(field) for field in POPUP_FIELDS
        })
    segment = {
        "track_id": str(props["track_id"]),
        "coordinates": [[float(point[0][0]), float(point[0][1])] for point in points],
        "epochs": [int(point[1].timestamp()) for point in points],
        "point_members": point_members,
    }
    segment.update(point_members[0])
    return segment


def _validate_segments(
    segments: list[dict[str, Any]], *, selected_day: date,
    gap_hours: float, max_speed_knots: float,
) -> dict[str, int]:
    day_start = int(datetime.combine(selected_day, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    day_end = day_start + 86400
    same_coordinate_edges = 0
    for segment in segments:
        epochs = segment["epochs"]
        coordinates = segment["coordinates"]
        if len(epochs) != len(coordinates) or not epochs:
            raise ValueError("track day-pack time/coordinate alignment failed")
        if any(not day_start <= epoch < day_end for epoch in epochs):
            raise ValueError("track day-pack contains future or adjacent-day geometry")
        if any(current <= previous for previous, current in zip(epochs, epochs[1:])):
            raise ValueError("track day-pack timestamps are not strictly increasing")
        for index, (previous, current) in enumerate(zip(epochs, epochs[1:])):
            elapsed = (current - previous) / 3600
            if elapsed > gap_hours:
                raise ValueError("track day-pack crosses an exporter gap split")
            first, second = coordinates[index], coordinates[index + 1]
            if first == second:
                same_coordinate_edges += 1
            speed = _haversine_nm(
                {"longitude": first[0], "latitude": first[1]},
                {"longitude": second[0], "latitude": second[1]},
            ) / elapsed
            if speed > max_speed_knots + 1e-9:
                raise ValueError("track day-pack crosses an exporter speed split")
    return {"same_coordinate_edges": same_coordinate_edges}


def _head_groups(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, float, float], list[dict[str, Any]]] = defaultdict(list)
    for segment in segments:
        point_members = segment.get("point_members") or [
            {field: segment.get(field) for field in POPUP_FIELDS}
        ] * len(segment["epochs"])
        for coordinate, epoch, member in zip(
            segment["coordinates"], segment["epochs"], point_members,
        ):
            grouped[(epoch, coordinate[0], coordinate[1])].append(member)
    return [{
        "epoch": epoch, "lon": lon, "lat": lat,
        "member_count": len(members),
        "members": sorted(members, key=lambda item: item["vessel_id"]),
    } for (epoch, lon, lat), members in sorted(grouped.items())]


def _head_group_counts(segments: list[dict[str, Any]]) -> dict[str, int]:
    """Measure runtime endpoint grouping without duplicating all members in the pack."""
    counts: dict[tuple[int, float, float], int] = defaultdict(int)
    for segment in segments:
        for coordinate, epoch in zip(segment["coordinates"], segment["epochs"]):
            counts[(epoch, coordinate[0], coordinate[1])] += 1
    return {
        "head_group_count": len(counts),
        "coincident_head_group_count": sum(count > 1 for count in counts.values()),
        "complete_head_member_count": sum(counts.values()),
    }


def _vessel_wire(segment: dict[str, Any]) -> dict[str, Any]:
    return {field: segment.get(field) for field in POPUP_FIELDS}


def _json_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{
        "track_id": segment["track_id"],
        "vessel": _vessel_wire(segment),
        "points": [
            [coordinate[0], coordinate[1], epoch]
            for coordinate, epoch in zip(segment["coordinates"], segment["epochs"])
        ],
    } for segment in segments]


def _typed_daypack(
    segments: list[dict[str, Any]], *, selected_day: date, bucket: str,
) -> tuple[bytes, dict[str, Any]]:
    vessels: list[dict[str, Any]] = []
    vessel_indexes: dict[tuple[Any, ...], int] = {}
    metadata_segments = []
    longitudes: list[float] = []
    latitudes: list[float] = []
    epochs: list[int] = []
    for segment in segments:
        vessel = _vessel_wire(segment)
        vessel_key = tuple(vessel.get(field) for field in POPUP_FIELDS)
        vessel_index = vessel_indexes.get(vessel_key)
        if vessel_index is None:
            vessel_index = len(vessels)
            vessel_indexes[vessel_key] = vessel_index
            vessels.append(vessel)
        count = len(segment["epochs"])
        metadata_segments.append({
            "track_id": segment["track_id"],
            "vessel_index": vessel_index,
            "point_offset": len(epochs),
            "point_count": count,
        })
        for coordinate, epoch in zip(segment["coordinates"], segment["epochs"]):
            longitudes.append(coordinate[0])
            latitudes.append(coordinate[1])
            if not 0 <= epoch <= 0xFFFFFFFF:
                raise ValueError("typed day-pack UTC epoch exceeds uint32")
            epochs.append(epoch)
    metadata = {
        "schema_version": 1,
        "display_date": selected_day.isoformat(),
        "bucket": bucket,
        "vessels": vessels,
        "segments": metadata_segments,
    }
    metadata_bytes = _canonical_bytes(metadata)
    metadata_bytes += b" " * (-(TYPED_HEADER.size + len(metadata_bytes)) % 4)
    point_count = len(epochs)
    arrays = b"".join((
        struct.pack(f"<{point_count}f", *longitudes),
        struct.pack(f"<{point_count}f", *latitudes),
        struct.pack(f"<{point_count}I", *epochs),
    ))
    header = TYPED_HEADER.pack(
        TYPED_MAGIC, 1, len(metadata_bytes), point_count, len(segments),
    )
    return header + metadata_bytes + arrays, {
        "schema_version": 1,
        "point_layout": "little-endian planar float32 lon[N], float32 lat[N], uint32 UTC epoch[N]",
        "point_stride_bytes": 12,
        "segments": metadata_segments,
    }


def _open_popup_member_lookup(shard: Path, sqlite_path: Path) -> sqlite3.Connection:
    """Persist full projected popup rows that the production exporter omits."""
    connection = sqlite3.connect(sqlite_path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("""
        CREATE TABLE popup_member (
          vessel_id TEXT NOT NULL,
          hour TEXT NOT NULL,
          observed_at TEXT NOT NULL,
          lon REAL NOT NULL,
          lat REAL NOT NULL,
          member_json TEXT NOT NULL,
          PRIMARY KEY (vessel_id, hour)
        ) WITHOUT ROWID
    """)
    batch = []
    for row in read_ndjson(shard):
        try:
            vessel_id = str(row["vessel_id"]).strip()
            observed_at = _parse_utc(str(row["observed_at"])).isoformat()
            hour = _utc_hour(observed_at)
            lon, lat = float(row["longitude"]), float(row["latitude"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if not vessel_id:
            continue
        member = {field: row.get(field) for field in POPUP_FIELDS}
        member["vessel_id"] = vessel_id
        batch.append((
            vessel_id, hour, observed_at, lon, lat,
            _canonical_bytes(member).decode("utf-8"),
        ))
        if len(batch) >= 10_000:
            connection.executemany("""
                INSERT INTO popup_member VALUES (?,?,?,?,?,?)
                ON CONFLICT(vessel_id,hour) DO UPDATE SET
                  observed_at=excluded.observed_at, lon=excluded.lon, lat=excluded.lat,
                  member_json=excluded.member_json
                WHERE (excluded.observed_at,excluded.lon,excluded.lat,excluded.member_json)
                    < (popup_member.observed_at,popup_member.lon,
                       popup_member.lat,popup_member.member_json)
            """, batch)
            connection.commit()
            batch.clear()
    if batch:
        connection.executemany("""
            INSERT INTO popup_member VALUES (?,?,?,?,?,?)
            ON CONFLICT(vessel_id,hour) DO UPDATE SET
              observed_at=excluded.observed_at, lon=excluded.lon, lat=excluded.lat,
              member_json=excluded.member_json
            WHERE (excluded.observed_at,excluded.lon,excluded.lat,excluded.member_json)
                < (popup_member.observed_at,popup_member.lon,
                   popup_member.lat,popup_member.member_json)
        """, batch)
        connection.commit()
    return connection


def build_track_daypacks(
    high_rows: Iterable[dict[str, Any]], *, selected_day: date,
    root: Path, gap_hours: float = 2.0, max_speed_knots: float = 80.0,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    work = root / ".track-work"
    work.mkdir(parents=True, exist_ok=False)
    shard = work / "high.points.ndjson"
    _write_ndjson(shard, high_rows)
    popup_connection = _open_popup_member_lookup(shard, work / "popup.sqlite")

    def popup_member_lookup(vessel_id: str, observed_at: str) -> dict[str, Any] | None:
        row = popup_connection.execute(
            "SELECT member_json FROM popup_member WHERE vessel_id=? AND hour=?",
            (vessel_id, _utc_hour(observed_at)),
        ).fetchone()
        return json.loads(row[0]) if row is not None else None

    store = finalize_track_store(
        [shard], work_dir=work, gap_hours=gap_hours,
        max_speed_knots=max_speed_knots,
    )
    by_bucket: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in SHIP_TYPE_BUCKETS}
    try:
        for feature in store.iter_features():
            segment = _selected_day_segment(
                feature, selected_day, popup_member_lookup=popup_member_lookup,
            )
            if segment is not None:
                by_bucket[ship_type_bucket(segment["vessel_type"])].append(segment)
        source_counts = {**store.stats, **store.counts()}
    finally:
        store.close()
        popup_connection.close()
    assets: list[dict[str, Any]] = []
    buckets = []
    for bucket in SHIP_TYPE_BUCKETS:
        segments = sorted(by_bucket[bucket], key=lambda item: (item["vessel_id"], item["track_id"]))
        validation = _validate_segments(
            segments, selected_day=selected_day, gap_hours=gap_hours,
            max_speed_knots=max_speed_knots,
        )
        head_counts = _head_group_counts(segments)
        json_payload = {
            "schema_version": 1, "display_date": selected_day.isoformat(),
            "bucket": bucket,
            "segment_count": len(segments),
            "point_count": sum(len(item["epochs"]) for item in segments),
            "render_contract": {
                "selected_day_preload": True,
                "network_request_per_timeline_tick": False,
                "no_future_geometry": True,
                "interpolation": "within_same_exporter_segment_only",
                "same_coordinate_head_member_key": ["UTC_epoch", "lon", "lat"],
            },
            "segments": _json_segments(segments),
        }
        json_path = root / "tracks" / selected_day.isoformat() / f"{bucket}.json.gz"
        started = time.perf_counter()
        _atomic_gzip_json(json_path, json_payload)
        json_encode_seconds = time.perf_counter() - started
        started = time.perf_counter()
        json.loads(gzip.decompress(json_path.read_bytes()))
        json_decode_seconds = time.perf_counter() - started
        json_asset = _asset(
            json_path, root=root, asset_type="track_daypack_gzip_json",
            bucket=bucket, segments=len(segments),
            points=sum(len(item["epochs"]) for item in segments),
        )
        assets.append(json_asset)

        binary, typed_metadata = _typed_daypack(
            segments, selected_day=selected_day, bucket=bucket,
        )
        typed_metadata.update({
            "display_date": selected_day.isoformat(), "ship_type_bucket": bucket,
        })
        binary_path = root / "tracks" / selected_day.isoformat() / f"{bucket}.typed.bin.gz"
        started = time.perf_counter()
        _atomic_bytes(binary_path, gzip.compress(binary, compresslevel=9, mtime=0))
        typed_encode_seconds = time.perf_counter() - started
        started = time.perf_counter()
        decoded_binary = gzip.decompress(binary_path.read_bytes())
        TYPED_HEADER.unpack_from(decoded_binary)
        typed_decode_seconds = time.perf_counter() - started
        binary_asset = _asset(
            binary_path, root=root, asset_type="track_daypack_typed_binary",
            bucket=bucket, segments=len(segments),
            points=sum(len(item["epochs"]) for item in segments),
        )
        assets.append(binary_asset)
        buckets.append({
            "bucket": bucket,
            "segment_count": len(segments),
            "point_count": sum(len(item["epochs"]) for item in segments),
            **head_counts,
            **validation,
            "candidates": {
                "gzip_json": {
                    "assets": [json_asset], "transfer_bytes": json_asset["bytes"],
                    "encode_seconds": round(json_encode_seconds, 6),
                    "python_decode_seconds": round(json_decode_seconds, 6),
                },
                "typed_binary": {
                    "assets": [binary_asset],
                    "transfer_bytes": binary_asset["bytes"],
                    "encode_seconds": round(typed_encode_seconds, 6),
                    "python_decode_seconds": round(typed_decode_seconds, 6),
                    "point_layout": typed_metadata["point_layout"],
                },
            },
        })
    # Remove only the exact scratch tree created by this function.
    shutil.rmtree(work)
    return {
        "display_date": selected_day.isoformat(),
        "split_by_ship_type_before_download": True,
        "default_enabled_buckets": ["cargo", "tanker", "passenger"],
        "gap_hours_gt": gap_hours,
        "max_implied_speed_knots": max_speed_knots,
        "source_counts": source_counts,
        "buckets": buckets,
    }, assets


def _iter_effort_rows(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, list):
        for item in value:
            yield from _iter_effort_rows(item)
        return
    if not isinstance(value, dict):
        return
    metric_keys = {"hours", "fishingHours", "fishing_hours", "value"}
    if metric_keys & value.keys() and {"lat", "lon"} <= value.keys():
        yield value
        return
    for nested in value.values():
        yield from _iter_effort_rows(nested)


def fetch_fishing_effort_phase(
    *, client: GFWReportClient, selected_day: date,
) -> tuple[list[Any], dict[str, Any]]:
    """Fetch one independent DAILY/LOW apparent-fishing-effort sample.

    Payloads remain memory-only until the separate effort normalizer projects
    them; they are never passed through the vessel-presence identity contract.
    """
    tiles = make_tiles(FIXED_BBOX, tile_size_degrees=TILE_SIZE_DEGREES)
    if len(tiles) != EXPECTED_TILE_COUNT:
        raise AssertionError("fixed East Asia bbox no longer produces 42 report tiles")
    start = selected_day.isoformat()
    end = (selected_day + timedelta(days=1)).isoformat()
    started = time.perf_counter()
    payloads = []
    resolved_versions: set[str] = set()
    for tile_index, tile in enumerate(tiles, start=1):
        payload, resolved = client.fetch(
            tile.bbox, start, end,
            dataset=FISHING_EFFORT_DATASET,
            group_by=None,
            spatial_resolution="LOW",
            temporal_resolution="DAILY",
        )
        if not resolved:
            raise RuntimeError(f"Fishing Effort tile {tile.tile_id} lacks x-datasets")
        payloads.append(payload)
        resolved_versions.add(resolved)
        print(f"FISHING tile {tile_index}/{len(tiles)} complete", flush=True)
    if len(resolved_versions) != 1:
        raise RuntimeError("Fishing Effort phase resolved multiple dataset versions")
    stats = dict(client.stats)
    return payloads, {
        "dataset_alias": FISHING_EFFORT_DATASET,
        "resolved_dataset_versions": sorted(resolved_versions),
        "temporal_resolution": "DAILY",
        "spatial_resolution": "LOW",
        "logical_report_count": len(tiles),
        "wall_time_seconds": round(time.perf_counter() - started, 6),
        "peak_rss_bytes": _peak_rss_bytes(),
        "response_body_bytes": int(stats.get("response_body_bytes", 0)),
        "http_statuses": stats.get("http_statuses", {}),
        "retries": int(stats.get("retries", 0)),
        "status_429": int(stats.get("status_429", 0)),
        "status_524": int(stats.get("status_524", 0)),
        "post_requests": int(stats.get("post_requests", 0)),
        "recovery_requests": int(stats.get("recovery_requests", 0)),
        "raw_response_saved": False,
        "presence_identity_contract_shared": False,
    }


def normalize_fishing_effort(
    payloads: Iterable[Any], *, selected_day: date,
    resolved_dataset_version: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows = []
    invalid = negative = wrong_day = duplicates = boundary_overlaps = 0
    seen_exact: set[tuple[Any, ...]] = set()
    payloads_by_identity: dict[tuple[Any, ...], set[int]] = defaultdict(set)
    for payload_index, payload in enumerate(payloads):
        for source in _iter_effort_rows(payload):
            try:
                metric = next(
                    float(source[key]) for key in (
                        "hours", "fishingHours", "fishing_hours", "value"
                    ) if source.get(key) is not None
                )
                longitude, latitude = float(source["lon"]), float(source["lat"])
                observed_day = date.fromisoformat(str(source.get("date") or selected_day.isoformat())[:10])
                cell = canonical_cell(longitude, latitude)
            except (StopIteration, TypeError, ValueError, OverflowError):
                invalid += 1
                continue
            if metric < 0:
                negative += 1
                continue
            if observed_day != selected_day:
                wrong_day += 1
                continue
            excluded = {
                "hours", "fishingHours", "fishing_hours", "value",
                "lon", "lat", "date",
            }
            facets = {
                key: value for key, value in source.items()
                if key not in excluded
            }
            facets_key = json.dumps(
                facets, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            )
            identity = (cell, facets_key)
            exact = (payload_index, identity, metric)
            if exact in seen_exact:
                duplicates += 1
                continue
            seen_exact.add(exact)
            prior_payloads = payloads_by_identity[identity]
            if prior_payloads and payload_index not in prior_payloads:
                boundary_overlaps += 1
            prior_payloads.add(payload_index)
            rows.append({
                "cell": cell, "apparent_fishing_hours": metric,
                "facets": facets,
                "resolved_dataset_version": resolved_dataset_version,
                "source_payload_index": payload_index,
            })
    return rows, {
        "valid_rows": len(rows), "invalid_rows": invalid,
        "negative_hours_rejected": negative, "wrong_day_rows": wrong_day,
        "exact_duplicate_rows": duplicates,
        "boundary_overlap_rows": boundary_overlaps,
    }


def build_fishing_effort_sample(
    payloads: Iterable[Any], *, selected_day: date,
    resolved_dataset_version: str, root: Path,
    latest_observed_active_date: str | None = None,
    source_response_sha256: str | None = None,
    source_accessed_at: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows, quality = normalize_fishing_effort(
        payloads, selected_day=selected_day,
        resolved_dataset_version=resolved_dataset_version,
    )
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["cell"]].append(row)
    features = []
    for cell, components in sorted(grouped.items()):
        hours = sum(component["apparent_fishing_hours"] for component in components)
        features.append({
            "type": "Feature", "id": f"effort-{cell[0]}-{cell[1]}",
            "properties": {
                "date": selected_day.isoformat(),
                "apparent_fishing_hours": hours,
                "component_count": len(components),
                "aggregation_facets_json": json.dumps(
                    [component["facets"] for component in components],
                    ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                ),
                "resolved_dataset_version": resolved_dataset_version,
                "metric_semantics": "apparent_model_derived_fishing_hours",
            },
            "geometry": {"type": "Polygon", "coordinates": _cell_polygon(cell)},
        })
    payload = {
        "type": "FeatureCollection",
        "metadata": {
            "schema_version": 1, "date": selected_day.isoformat(),
            "temporal_resolution": "DAILY", "spatial_resolution": "LOW",
            "metric": "apparent_fishing_hours", "unit": "hours",
            "resolved_dataset_version": resolved_dataset_version,
            "latest_available_date": None,
            "latest_available_date_status": "not_provided_by_gfw",
            "latest_observed_active_date": latest_observed_active_date,
            "source_response_sha256": source_response_sha256,
            "source_accessed_at": source_accessed_at,
            "finalization_status": "not_provided_by_gfw",
            "revision_semantics": "dynamic_api_data_may_be_revised",
            "attribution": "Powered by Global Fishing Watch. https://globalfishingwatch.org/",
            "access_date": datetime.now(timezone.utc).date().isoformat(),
            "caveat": "Apparent/model-derived and non-realtime; not vessel presence",
            "quality": quality,
        },
        "features": features,
    }
    path = root / "fishing-effort" / f"{selected_day.isoformat()}.geojson.gz"
    _atomic_gzip_json(path, payload)
    asset = _asset(
        path, root=root, asset_type="fishing_effort_daily_sample",
        features=len(features), apparent_fishing_hours=sum(
            feature["properties"]["apparent_fishing_hours"] for feature in features
        ),
    )
    return {
        "independent_layer": True,
        "presence_identity_contract_shared": False,
        "dataset_alias": FISHING_EFFORT_DATASET,
        "resolved_dataset_version": resolved_dataset_version,
        "date": selected_day.isoformat(),
        "latest_observed_active_date": latest_observed_active_date,
        "finalization_status": "not_provided_by_gfw",
        "revision_semantics": "dynamic_api_data_may_be_revised",
        "quality": quality,
        "asset": asset,
    }, [asset]


def readback_artifacts(
    assets: Iterable[dict[str, Any]], *, root: Path,
    verify_pmtiles: bool = False,
) -> dict[str, Any]:
    checked = 0
    total_bytes = 0
    grid_semantic_reports = []
    grid_archive_count = 0
    for asset in assets:
        path = root / asset["path"]
        if not path.is_file() or path.stat().st_size != asset["bytes"]:
            raise RuntimeError(f"artifact size readback failed: {asset['path']}")
        if _sha256(path) != asset["sha256"]:
            raise RuntimeError(f"artifact hash readback failed: {asset['path']}")
        if asset["path"].endswith(".json.gz") or asset["path"].endswith(".geojson.gz"):
            with gzip.open(path, "rb") as handle:
                decoded_json = json.loads(handle.read())
            if asset["type"] == "grid_detail":
                entries = decoded_json.get("entries")
                if not isinstance(entries, dict):
                    raise RuntimeError(f"Grid detail entries readback failed: {asset['path']}")
                vessel_count = 0
                for detail in entries.values():
                    members = detail.get("members") if isinstance(detail, dict) else None
                    if not isinstance(members, list) or detail.get("vessel_count") != len(members):
                        raise RuntimeError(f"Grid detail member count readback failed: {asset['path']}")
                    vessel_count += len(members)
                if (
                    decoded_json.get("entry_count") != len(entries)
                    or decoded_json.get("vessel_count") != vessel_count
                    or asset.get("features") != len(entries)
                    or asset.get("vessels") != vessel_count
                ):
                    raise RuntimeError(f"Grid detail aggregate readback failed: {asset['path']}")
            elif asset["type"] == "track_daypack_gzip_json":
                segments = decoded_json.get("segments")
                if not isinstance(segments, list):
                    raise RuntimeError(f"JSON day-pack segment readback failed: {asset['path']}")
                point_count = sum(len(item.get("points", [])) for item in segments)
                if (
                    decoded_json.get("segment_count") != len(segments)
                    or decoded_json.get("point_count") != point_count
                    or asset.get("segments") != len(segments)
                    or asset.get("points") != point_count
                ):
                    raise RuntimeError(f"JSON day-pack semantic readback failed: {asset['path']}")
            elif asset["type"] == "fishing_effort_daily_sample":
                features = decoded_json.get("features")
                if not isinstance(features, list):
                    raise RuntimeError(f"Fishing sample feature readback failed: {asset['path']}")
                hours = sum(float(item["properties"]["apparent_fishing_hours"]) for item in features)
                if (
                    any(float(item["properties"]["apparent_fishing_hours"]) < 0 for item in features)
                    or asset.get("features") != len(features)
                    or not math.isclose(float(asset.get("apparent_fishing_hours")), hours)
                ):
                    raise RuntimeError(f"Fishing sample semantic readback failed: {asset['path']}")
        elif asset["type"] == "track_daypack_typed_binary":
            decoded = gzip.decompress(path.read_bytes())
            magic, version, metadata_bytes, point_count, segment_count = TYPED_HEADER.unpack_from(decoded)
            if magic != TYPED_MAGIC or version != 1 or segment_count != asset["segments"]:
                raise RuntimeError(f"typed day-pack header readback failed: {asset['path']}")
            if len(decoded) != TYPED_HEADER.size + metadata_bytes + point_count * 12:
                raise RuntimeError(f"typed day-pack length readback failed: {asset['path']}")
        elif asset["type"] == "grid_hour_pmtiles":
            grid_archive_count += 1
            if asset.get("semantic_readback") is not None:
                semantic = asset["semantic_readback"]
                if (
                    semantic.get("status") != "passed"
                    or semantic.get("unique_cells") != asset.get("features")
                    or semantic.get("expected_cells") != asset.get("features")
                ):
                    raise RuntimeError(f"PMTiles semantic readback failed: {asset['path']}")
                grid_semantic_reports.append(semantic)
            if verify_pmtiles:
                subprocess.run(
                    ["/opt/homebrew/bin/pmtiles", "verify", str(path)],
                    check=True, capture_output=True, text=True,
                )
                subprocess.run(
                    ["/opt/homebrew/bin/pmtiles", "show", str(path), "--header-json"],
                    check=True, capture_output=True, text=True,
                )
            elif path.stat().st_size == 0:
                raise RuntimeError(f"empty PMTiles artifact: {asset['path']}")
        checked += 1
        total_bytes += asset["bytes"]
    return {
        "status": "passed", "checked_assets": checked,
        "checked_bytes": total_bytes,
        "pmtiles_archive_structure_verified": verify_pmtiles,
        "individual_mvt_content_verified": bool(grid_archive_count)
        and len(grid_semantic_reports) == grid_archive_count and all(
            report.get("status") == "passed" for report in grid_semantic_reports
        ),
        "semantic_pmtiles_archives": len(grid_semantic_reports),
        "semantic_grid_cells": sum(
            int(report.get("unique_cells", 0)) for report in grid_semantic_reports
        ),
    }


def build_local_shadow_poc(
    *, low_rows: Iterable[dict[str, Any]], high_rows: Iterable[dict[str, Any]],
    fishing_payloads: Iterable[Any], fishing_resolved_dataset_version: str,
    selected_day: date, output_dir: Path,
    phase_metrics: dict[str, dict[str, Any]] | None = None,
    pmtiles_builder: Callable[..., None] = _pmtiles,
    verify_pmtiles: bool = False,
) -> dict[str, Any]:
    if output_dir.exists():
        raise FileExistsError(f"immutable POC output already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    low_rows, high_rows = list(low_rows), list(high_rows)
    low, high = index_presence(low_rows), index_presence(high_rows)
    parity = compare_presence(low, high)
    if high.stats["invalid_rows"] or high.stats["same_vessel_hour_cell_conflicts"]:
        raise RuntimeError("HIGH source failed no-drop coordinate/vessel-hour conflict gate")
    grid, grid_assets = build_grid_artifacts(
        high, selected_day=selected_day, root=output_dir,
        pmtiles_builder=pmtiles_builder,
    )
    tracks, track_assets = build_track_daypacks(
        high_rows, selected_day=selected_day, root=output_dir,
    )
    effort, effort_assets = build_fishing_effort_sample(
        fishing_payloads, selected_day=selected_day,
        resolved_dataset_version=fishing_resolved_dataset_version,
        root=output_dir,
    )
    effort_quality = effort["quality"]
    if any(effort_quality[key] for key in (
        "invalid_rows", "negative_hours_rejected", "wrong_day_rows",
        "boundary_overlap_rows",
    )):
        raise RuntimeError("Fishing Effort failed validity/boundary-conflict gate")
    assets = [*grid_assets, *track_assets, *effort_assets]
    bench_assets = []
    for bucket in tracks["buckets"]:
        json_asset = bucket["candidates"]["gzip_json"]["assets"][0]
        binary_asset = bucket["candidates"]["typed_binary"]["assets"][0]
        bench_assets.extend((
            {"bucket": bucket["bucket"], "format": "json.gz", **json_asset},
            {"bucket": bucket["bucket"], "format": "binary", **binary_asset},
        ))
    readback = readback_artifacts(
        assets, root=output_dir, verify_pmtiles=verify_pmtiles,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "poc": True,
        "immutable_local_output": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bbox": list(FIXED_BBOX),
        "release_id": selected_day.isoformat(),
        "selected_utc_date": selected_day.isoformat(),
        "days": [{
            "display_date": selected_day.isoformat(),
            "assets": bench_assets,
        }],
        "production_cutover": False,
        "presence_route_decision": (
            "LOW_preserves_HIGH_identity_and_popup_members"
            if parity["global_vessel_hour"]["equal"] and parity["per_cell_members"]["equal"]
            else "HIGH_required_for_lossless_derived_product"
        ),
        "source_phases": phase_metrics or {},
        "presence_parity": parity,
        "grid": grid,
        "tracks": tracks,
        "fishing_effort": effort,
        "artifacts": assets,
        "artifact_bytes": sum(asset["bytes"] for asset in assets),
        "readback": readback,
        "release_truth": {
            "build": "passed_local_fixture_or_shadow",
            "contract_wire": "local_POC_only",
            "stage": "local_immutable_directory",
            "upload": "not_run",
            "readback": "passed_local",
            "pull": "not_run",
            "deploy": "not_run",
            "HTTP": "not_run",
            "browser": "not_run",
        },
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    decoded = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    if decoded["artifact_bytes"] != readback["checked_bytes"]:
        raise RuntimeError("manifest artifact byte total failed local readback")
    return manifest


def _load_fixture(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--_presence-worker":
        return _presence_worker_main(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--selected-day", type=date.fromisoformat, required=True)
    parser.add_argument("--low-normalized-fixture", type=Path, required=True)
    parser.add_argument("--high-normalized-fixture", type=Path, required=True)
    parser.add_argument("--fishing-effort-fixture", type=Path, required=True)
    parser.add_argument("--fishing-resolved-version", required=True)
    parser.add_argument("--verify-pmtiles", action="store_true")
    args = parser.parse_args()
    low = _load_fixture(args.low_normalized_fixture)
    high = _load_fixture(args.high_normalized_fixture)
    fishing = _load_fixture(args.fishing_effort_fixture)
    if not isinstance(low, list) or not isinstance(high, list):
        parser.error("presence fixtures must be normalized row arrays")
    manifest = build_local_shadow_poc(
        low_rows=low,
        high_rows=high,
        fishing_payloads=[fishing],
        fishing_resolved_dataset_version=args.fishing_resolved_version,
        selected_day=args.selected_day,
        output_dir=args.output_dir,
        verify_pmtiles=args.verify_pmtiles,
    )
    print(json.dumps({
        "output_dir": str(args.output_dir.resolve()),
        "artifact_bytes": manifest["artifact_bytes"],
        "presence_parity": manifest["presence_parity"],
        "release_truth": manifest["release_truth"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
