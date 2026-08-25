#!/usr/bin/env python3
"""Export local-only GFW hourly grid-cell vessel presence GeoJSON files.

This POC reuses the sequential, resumable GFW HOURLY/HIGH/VESSEL_ID report
client from ``gfw_hourly_tracks_poc``. It writes no DB, S3, or raw-response
archive. Each feature is a GFW HIGH grid-cell center, not a raw AIS position.

Example:
    python3 scripts/gfw_hourly_grid_poc.py \
      --output-dir ../mini-taiwan-pulse/public/gfw-hourly-grid-poc
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from collectors.gfw_vessel_presence import (  # noqa: E402
    GFW_DATASET,
    GFWVesselPresenceCollector,
)
from scripts.gfw_hourly_tracks_poc import (  # noqa: E402
    DEFAULT_BBOX,
    DEFAULT_LATEST_COMPLETE_DAY,
    GFWReportClient,
    Tile,
    _canonical,
    _request_counter_delta,
    _request_counter_snapshot,
    make_tiles,
)


SCHEMA_VERSION = 1
PROJECTED_FIELDS = (
    "vessel_id",
    "observed_at",
    "longitude",
    "latitude",
    "mmsi",
    "ship_name",
    "vessel_type",
    "flag",
)


def _atomic_json(path: Path, value: Any, *, minified: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":") if minified else None,
            indent=None if minified else 2,
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hour_text(value: str) -> str:
    hour = _parse_utc(value).replace(minute=0, second=0, microsecond=0)
    return hour.strftime("%Y-%m-%dT%H:00:00Z")


def _hour_filename(observed_at: str) -> str:
    parsed = _parse_utc(observed_at)
    return f"hours/{parsed.strftime('%Y%m%dT%HZ')}.geojson"


def _clean_identity(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _mode(values: Iterable[Any]) -> str | None:
    cleaned = [_clean_identity(value) for value in values]
    present = [value for value in cleaned if value is not None]
    if not present:
        return None
    counts = Counter(present)
    return min(counts, key=lambda value: (-counts[value], value))


def _normalize_row(row: dict[str, Any]) -> dict[str, Any] | None:
    try:
        vessel_id = str(row["vessel_id"]).strip()
        observed_at = _parse_utc(str(row["observed_at"])).isoformat()
        longitude = float(row["longitude"])
        latitude = float(row["latitude"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    if not vessel_id or not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
        return None
    return {
        "vessel_id": vessel_id,
        "observed_at": observed_at,
        "longitude": longitude,
        "latitude": latitude,
        "mmsi": _clean_identity(row.get("mmsi")),
        "ship_name": _clean_identity(row.get("ship_name")),
        "vessel_type": _clean_identity(row.get("vessel_type")),
        "flag": _clean_identity(row.get("flag")),
    }


def _write_points(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, int]:
    temporary = path.with_name(f".{path.name}.tmp")
    written = invalid = 0
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            normalized = _normalize_row(row)
            if normalized is None:
                invalid += 1
                continue
            handle.write(_canonical(normalized) + "\n")
            written += 1
    temporary.replace(path)
    return written, invalid


def _read_points(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def build_hourly_grid_features(
    rows: Iterable[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    """Pure deterministic aggregator used for tests and small inputs."""
    normalized: dict[tuple[Any, ...], dict[str, Any]] = {}
    invalid_rows = 0
    duplicate_rows = 0
    for row in rows:
        clean = _normalize_row(row)
        if clean is None:
            invalid_rows += 1
            continue
        key = (
            clean["vessel_id"], clean["observed_at"],
            clean["longitude"], clean["latitude"],
        )
        if key in normalized:
            duplicate_rows += 1
            continue
        normalized[key] = clean
    by_vessel_hour: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in normalized.values():
        key = (_hour_text(row["observed_at"]), row["vessel_id"])
        by_vessel_hour.setdefault(key, []).append(row)

    grouped: dict[tuple[str, float, float], dict[str, list[dict[str, Any]]]] = {}
    same_vessel_hour_position_conflicts = 0
    for (hour, vessel_id), vessel_rows in sorted(by_vessel_hour.items()):
        ordered = sorted(
            vessel_rows,
            key=lambda row: (
                row["observed_at"], row["longitude"], row["latitude"],
                row.get("mmsi") or "", row.get("ship_name") or "",
                row.get("vessel_type") or "", row.get("flag") or "",
            ),
        )
        selected = ordered[0]
        selected_center = (selected["longitude"], selected["latitude"])
        centers = {(row["longitude"], row["latitude"]) for row in ordered}
        if len(centers) > 1:
            same_vessel_hour_position_conflicts += 1
        selected_rows = [
            row for row in ordered
            if (row["longitude"], row["latitude"]) == selected_center
        ]
        cell_key = (hour, selected["longitude"], selected["latitude"])
        grouped.setdefault(cell_key, {})[vessel_id] = selected_rows

    by_hour: dict[str, list[dict[str, Any]]] = {}
    for (observed_at, longitude, latitude), vessels in sorted(grouped.items()):
        feature, _observation_count = _grid_feature(
            observed_at, longitude, latitude, vessels
        )
        by_hour.setdefault(observed_at, []).append(feature)
    return by_hour, {
        "input_rows": len(normalized) + duplicate_rows,
        "unique_observations": len(normalized),
        "duplicate_observations": duplicate_rows,
        "invalid_rows": invalid_rows,
        "same_vessel_hour_position_conflicts": same_vessel_hour_position_conflicts,
        "hour_count": len(by_hour),
        "feature_count": sum(len(features) for features in by_hour.values()),
    }


def _grid_feature(
    observed_at: str,
    longitude: float,
    latitude: float,
    vessels: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], int]:
    vessel_list = []
    observation_count = 0
    for vessel_id in sorted(vessels):
        rows = vessels[vessel_id]
        observation_count += len(rows)
        vessel_list.append({
            "vessel_id": vessel_id,
            "mmsi": _mode(row.get("mmsi") for row in rows),
            "ship_name": _mode(row.get("ship_name") for row in rows),
            "vessel_type": _mode(row.get("vessel_type") for row in rows),
            "flag": _mode(row.get("flag") for row in rows),
        })
    feature_id = hashlib.sha256(
        _canonical([observed_at, longitude, latitude]).encode()
    ).hexdigest()[:20]
    return {
        "type": "Feature",
        "id": feature_id,
        "properties": {
            "observed_at": observed_at,
            "grid_lon": longitude,
            "grid_lat": latitude,
            "vessel_count": len(vessel_list),
            "vessels_json": _canonical(vessel_list),
            "source_dataset": GFW_DATASET,
            "coordinate_semantics": "GFW_HIGH_grid_cell_center",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
    }, observation_count


def _load_or_create_resume(
    work_dir: Path, signature: dict[str, Any], tiles: list[Tile]
) -> dict[str, Any]:
    path = work_dir / "resume.json"
    if path.exists():
        resume = json.loads(path.read_text(encoding="utf-8"))
        if _canonical(resume.get("signature")) != _canonical(signature):
            raise RuntimeError(
                f"resume work directory signature mismatch: {work_dir}; use another --work-dir"
            )
        return resume
    work_dir.mkdir(parents=True, exist_ok=True)
    resume = {
        "signature": signature,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tiles": [{"tile_id": tile.tile_id, "bbox": tile.bbox} for tile in tiles],
        "completed_tiles": {},
    }
    _atomic_json(path, resume, minified=False)
    return resume


def _finalize_disk_backed(
    shard_paths: Iterable[Path], *, work_dir: Path, output_dir: Path,
    poc: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    database_path = work_dir / "hourly-grid.sqlite3"
    if database_path.exists():
        database_path.unlink()
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("""
        CREATE TABLE observations (
            vessel_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            hour_at TEXT NOT NULL,
            longitude REAL NOT NULL,
            latitude REAL NOT NULL,
            mmsi TEXT,
            ship_name TEXT,
            vessel_type TEXT,
            flag TEXT,
            PRIMARY KEY (vessel_id, observed_at, longitude, latitude)
        ) WITHOUT ROWID
    """)
    sql = """
        INSERT OR IGNORE INTO observations
        (vessel_id, observed_at, hour_at, longitude, latitude, mmsi, ship_name, vessel_type, flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    input_rows = invalid_rows = 0
    batch: list[tuple[Any, ...]] = []
    for source in _read_points(shard_paths):
        row = _normalize_row(source)
        if row is None:
            invalid_rows += 1
            continue
        input_rows += 1
        batch.append((
            row["vessel_id"], row["observed_at"], _hour_text(row["observed_at"]),
            row["longitude"], row["latitude"], row["mmsi"], row["ship_name"],
            row["vessel_type"], row["flag"],
        ))
        if len(batch) >= 5000:
            connection.executemany(sql, batch)
            batch.clear()
    if batch:
        connection.executemany(sql, batch)
    connection.commit()
    unique_observations = int(
        connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    )

    same_vessel_hour_position_conflicts = int(connection.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT hour_at, vessel_id
            FROM observations
            GROUP BY hour_at, vessel_id
            HAVING COUNT(DISTINCT printf('%.17g,%.17g', longitude, latitude)) > 1
        )
    """).fetchone()[0])
    connection.execute("""
        CREATE TABLE selected AS
        SELECT vessel_id, observed_at, hour_at, longitude, latitude,
               mmsi, ship_name, vessel_type, flag
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY hour_at, vessel_id
                ORDER BY observed_at, longitude, latitude,
                         COALESCE(mmsi, ''), COALESCE(ship_name, ''),
                         COALESCE(vessel_type, ''), COALESCE(flag, '')
            ) AS position_rank
            FROM observations
        )
        WHERE position_rank = 1
    """)

    cursor = connection.execute("""
        SELECT hour_at, longitude, latitude, vessel_id, observed_at,
               mmsi, ship_name, vessel_type, flag
        FROM selected
        ORDER BY hour_at, longitude, latitude, vessel_id, observed_at,
                 COALESCE(mmsi, ''), COALESCE(ship_name, ''),
                 COALESCE(vessel_type, ''), COALESCE(flag, '')
    """)
    hour_entries: list[dict[str, Any]] = []
    total_features = total_vessel_presence = 0
    current_hour: str | None = None
    current_cell: tuple[str, float, float] | None = None
    current_vessels: dict[str, list[dict[str, Any]]] = {}
    hour_features: list[dict[str, Any]] = []
    hour_observations = 0

    def flush_cell() -> None:
        nonlocal current_vessels, hour_observations
        if current_cell is None:
            return
        feature, observations = _grid_feature(*current_cell, current_vessels)
        hour_features.append(feature)
        hour_observations += observations
        current_vessels = {}

    def flush_hour() -> None:
        nonlocal hour_features, hour_observations, total_features, total_vessel_presence
        if current_hour is None:
            return
        file_name = _hour_filename(current_hour)
        collection = {
            "type": "FeatureCollection",
            "metadata": {
                "poc": poc,
                "observed_at": current_hour,
                "temporal_resolution": "HOURLY",
                "spatial_resolution": "HIGH",
                "coordinate_semantics": "GFW_HIGH_grid_cell_center",
                "position_note": "GFW grid-cell center; not a raw AIS position",
                "feature_count": len(hour_features),
                "vessel_presence_count": sum(
                    feature["properties"]["vessel_count"] for feature in hour_features
                ),
                "observation_count": hour_observations,
            },
            "features": hour_features,
        }
        _atomic_json(output_dir / file_name, collection)
        vessel_presence_count = collection["metadata"]["vessel_presence_count"]
        hour_entries.append({
            "observed_at": current_hour,
            "path": file_name,
            "cell_count": len(hour_features),
            "vessel_count": vessel_presence_count,
        })
        total_features += len(hour_features)
        total_vessel_presence += vessel_presence_count
        hour_features = []
        hour_observations = 0

    for (
        hour_at, longitude, latitude, vessel_id, observed_at,
        mmsi, ship_name, vessel_type, flag,
    ) in cursor:
        cell = (hour_at, longitude, latitude)
        if current_cell != cell:
            flush_cell()
            if current_hour != hour_at:
                flush_hour()
                current_hour = hour_at
            current_cell = cell
        current_vessels.setdefault(vessel_id, []).append({
            "vessel_id": vessel_id,
            "observed_at": observed_at,
            "mmsi": mmsi,
            "ship_name": ship_name,
            "vessel_type": vessel_type,
            "flag": flag,
        })
    flush_cell()
    flush_hour()
    connection.close()
    return hour_entries, {
        "input_rows": input_rows,
        "unique_observations": unique_observations,
        "duplicate_observations": input_rows - unique_observations,
        "invalid_rows": invalid_rows,
        "same_vessel_hour_position_conflicts": same_vessel_hour_position_conflicts,
        "hour_count": len(hour_entries),
        "feature_count": total_features,
        "vessel_presence_count": total_vessel_presence,
    }


def run_poc(
    *,
    output_dir: Path,
    token: str,
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX,
    latest_complete_day: str = DEFAULT_LATEST_COMPLETE_DAY,
    days: int = 7,
    tile_size_degrees: float = 3.0,
    work_dir: Path | None = None,
    keep_work_dir: bool = False,
    client: GFWReportClient | None = None,
) -> dict[str, Any]:
    latest = date.fromisoformat(latest_complete_day)
    if not 1 <= days <= 31:
        raise ValueError("days must be between 1 and 31")
    start = latest - timedelta(days=days - 1)
    end = latest + timedelta(days=1)
    start_text, end_text = start.isoformat(), end.isoformat()
    tiles = make_tiles(bbox, tile_size_degrees=tile_size_degrees)
    signature = {
        "bbox": bbox,
        "start": start_text,
        "end_exclusive": end_text,
        "days": days,
        "tile_size_degrees": tile_size_degrees,
        "dataset": GFW_DATASET,
        "temporal_resolution": "HOURLY",
        "spatial_resolution": "HIGH",
        "group_by": "VESSEL_ID",
        "aggregation": "UTC_hour_and_exact_grid_center",
    }
    signature_hash = hashlib.sha256(_canonical(signature).encode()).hexdigest()[:16]
    work_dir = work_dir or Path(tempfile.gettempdir()) / f"gfw-hourly-grid-poc-{signature_hash}"
    resume = _load_or_create_resume(work_dir, signature, tiles)
    report_client = client or GFWReportClient(token)
    received_at = datetime.now(timezone.utc).isoformat()
    resolved_versions: set[str] = set()
    resumed_tiles = 0

    for position, tile in enumerate(tiles, start=1):
        shard = work_dir / f"{tile.tile_id}.points.ndjson"
        completed = resume["completed_tiles"].get(tile.tile_id)
        if completed and shard.exists():
            resumed_tiles += 1
            if completed.get("resolved_dataset_version"):
                resolved_versions.add(completed["resolved_dataset_version"])
            print(f"[{position}/{len(tiles)}] resume {tile.tile_id}: {completed['row_count']} rows", flush=True)
            continue
        print(f"[{position}/{len(tiles)}] fetch {tile.tile_id} {tile.bbox}", flush=True)
        try:
            before = _request_counter_snapshot(report_client.stats)
            payload, resolved = report_client.fetch(tile.bbox, start_text, end_text)
            rows = GFWVesselPresenceCollector.normalize_entries(
                payload,
                snapshot_date=latest_complete_day,
                received_at=received_at,
                zone=tile.tile_id,
                dataset=resolved or GFW_DATASET,
            )
            accepted = (
                row for row in rows
                if row.get("presence_quality") == "accepted"
                and row.get("longitude") is not None
                and row.get("latitude") is not None
            )
            row_count, invalid_count = _write_points(shard, accepted)
            resume["completed_tiles"][tile.tile_id] = {
                "row_count": row_count,
                "invalid_count": invalid_count,
                "resolved_dataset_version": resolved,
                "request_counts": _request_counter_delta(before, report_client.stats),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_json(work_dir / "resume.json", resume, minified=False)
            if resolved:
                resolved_versions.add(resolved)
            print(f"[{position}/{len(tiles)}] done {tile.tile_id}: {row_count} rows", flush=True)
        except Exception as exc:
            raise RuntimeError(
                f"POC incomplete at {tile.tile_id}; resumable normalized shards retained at "
                f"{work_dir}: {exc}"
            ) from exc

    if len(resume["completed_tiles"]) != len(tiles):
        raise RuntimeError(f"POC incomplete; resumable normalized shards retained at {work_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    hour_entries, counts = _finalize_disk_backed(
        [work_dir / f"{tile.tile_id}.points.ndjson" for tile in tiles],
        work_dir=work_dir,
        output_dir=output_dir,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "poc": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Global Fishing Watch 4Wings AIS Vessel Presence",
        "source_dataset": GFW_DATASET,
        "resolved_dataset_versions": sorted(resolved_versions),
        "bbox": list(bbox),
        "date_start": start_text,
        "date_end_inclusive": latest_complete_day,
        "date_end_exclusive": end_text,
        "temporal_resolution": "HOURLY",
        "spatial_resolution": "HIGH",
        "coordinate_semantics": "GFW_HIGH_grid_cell_center",
        "position_note": "GFW grid-cell center; not a raw AIS position",
        "hours": hour_entries,
        "counts": counts,
        "tiling": {
            "tile_size_degrees": tile_size_degrees,
            "tile_count": len(tiles),
            "sequential": True,
            "resumed_tile_count": resumed_tiles,
        },
        "requests": {
            "successful_tile_reports": len(resume["completed_tiles"]),
            "current_process": report_client.stats,
        },
        "storage": "No DB/S3/raw archive; normalized resume shards deleted after success unless --keep-work-dir",
    }
    _atomic_json(output_dir / "manifest.json", manifest)
    summary = {
        "output_dir": str(output_dir.resolve()),
        "manifest": str((output_dir / "manifest.json").resolve()),
        "counts": counts,
        "requests": manifest["requests"],
        "work_dir": str(work_dir),
        "work_dir_kept": keep_work_dir,
    }
    if not keep_work_dir:
        shutil.rmtree(work_dir)
        summary["work_dir"] = None
    return summary


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    try:
        parts = tuple(float(item.strip()) for item in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("bbox values must be numbers") from exc
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox must be west,south,east,north")
    make_tiles(parts, tile_size_degrees=360)
    return parts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--bbox", type=_parse_bbox, default=DEFAULT_BBOX)
    parser.add_argument("--latest-complete-day", default=DEFAULT_LATEST_COMPLETE_DAY)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--tile-size-degrees", type=float, default=3.0)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--keep-work-dir", action="store_true")
    parser.add_argument("--request-timeout", type=float, default=120.0)
    args = parser.parse_args()
    if not config.GFW_ACCESS_TOKEN:
        parser.error("GFW_ACCESS_TOKEN is missing; token is read backend-only from data-collectors/.env")
    summary = run_poc(
        output_dir=args.output_dir,
        token=config.GFW_ACCESS_TOKEN,
        bbox=args.bbox,
        latest_complete_day=args.latest_complete_day,
        days=args.days,
        tile_size_degrees=args.tile_size_degrees,
        work_dir=args.work_dir,
        keep_work_dir=args.keep_work_dir,
        client=GFWReportClient(config.GFW_ACCESS_TOKEN, timeout=args.request_timeout),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
