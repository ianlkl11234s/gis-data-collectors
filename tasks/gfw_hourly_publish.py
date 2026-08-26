"""Production daily publisher for unified GFW hourly grid and track assets.

The job performs one sequential rolling-window fetch per configured tile, then
fans the same normalized NDJSON shards out to both products.  It never stores
raw GFW responses.  A Supabase run-ledger write is a hard gate before the first
GFW request; S3 immutable objects are verified before the single root manifest
is cut over.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import config
from collectors.gfw_vessel_presence import GFW_DATASET, GFWVesselPresenceCollector
from scripts.gfw_hourly_grid_poc import (
    _finalize_disk_backed as finalize_grid,
    _write_points,
)
from scripts.gfw_hourly_release import (
    DEFAULT_LOOKAHEAD_HOURS,
    DEFAULT_LOOKBACK_HOURS,
    publish_release_to_s3,
)
from scripts.gfw_hourly_browser_assets import (
    build_grid_browser_assets,
    build_track_browser_assets,
)
from scripts.gfw_hourly_tracks_poc import (
    GFWReportClient,
    Tile,
    _request_counter_delta,
    _request_counter_snapshot,
    finalize_track_store,
    make_tiles,
)

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3
LEDGER_FUNCTION = "live.upsert_gfw_hourly_publish_run"
DEFAULT_BBOX = (122.43400, 23.22953, 132.85274, 34.35812)
SAR_DATASET = "public-global-sar-presence:latest"
SAR_FILTER = "matched='false'"
_UTC_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SPOOL_RUN = re.compile(
    r"^\d{4}-\d{2}-\d{2}-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_TILE_FILE = re.compile(r"^r\d{2}c\d{2}\.(?:points|sar-unmatched)\.ndjson$")
_HOUR_FILE = re.compile(r"^\.?\d{8}T\d{2}Z\.geojson(?:\.tmp)?$")
_DAY_FILE = re.compile(r"^\.?\d{4}-\d{2}-\d{2}\.geojson(?:\.tmp)?$")
_HOUR_PM = re.compile(r"^\d{8}T\d{2}Z\.pmtiles$")
_DAY_PM = re.compile(r"^\d{4}-\d{2}-\d{2}\.pmtiles$")
_FRAME_GZIP = re.compile(r"^\.?\d{8}T\d{2}Z\.geojson\.gz(?:\.tmp)?$")
_BUCKET_GZIP = re.compile(r"^\.?[0-9a-f]\.json\.gz(?:\.tmp)?$")
_GRID_INPUT = re.compile(r"^\.?\d{8}T\d{2}Z\.ndjson(?:\.tmp)?$")
_TRACK_INPUT = re.compile(r"^\.?\d{4}-\d{2}-\d{2}-(?:edges|singletons)\.ndjson(?:\.tmp)?$")
_HOUR_STAMP = re.compile(r"^\d{8}T\d{2}Z$")


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(_canonical(value))
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_bbox(value: str | Iterable[float]) -> tuple[float, float, float, float]:
    if isinstance(value, str):
        try:
            parts = tuple(float(item.strip()) for item in value.split(","))
        except ValueError as exc:
            raise ValueError("GFW_HOURLY_BBOX must contain four numbers") from exc
    else:
        parts = tuple(float(item) for item in value)
    if len(parts) != 4:
        raise ValueError("GFW_HOURLY_BBOX must be west,south,east,north")
    make_tiles(parts, tile_size_degrees=360.0)
    return parts


@dataclass(frozen=True)
class GFWHourlyPublishSettings:
    token: str = field(repr=False)
    db_url: str = field(repr=False)
    bucket: str
    public_url_prefix: str
    spool_root: Path
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX
    key_prefix: str = "deploy-assets/global-maritime/gfw-hourly"
    shadow_key_prefix: str = "deploy-assets/global-maritime/gfw-hourly/v3-shadow"
    shadow_public_url_prefix: str = ""
    data_lag_days: int = 5
    rolling_days: int = 7
    tile_size_degrees: float = 3.0
    expected_tile_count: int = 16
    gap_hours: float = 2.0
    max_speed_knots: float = 80.0
    releases_to_keep: int = 2
    failed_spool_retention_days: int = 7

    def validate(self) -> None:
        if not self.token:
            raise ValueError("GFW_ACCESS_TOKEN is required")
        if not self.db_url:
            raise ValueError("SUPABASE_DB_URL is required")
        if not self.bucket:
            raise ValueError("S3_BUCKET is required")
        if not self.public_url_prefix.startswith("https://"):
            raise ValueError("GFW_HOURLY_PUBLIC_URL_PREFIX must be HTTPS")
        if not self.shadow_public_url_prefix.startswith("https://"):
            raise ValueError("GFW_HOURLY_SHADOW_PUBLIC_URL_PREFIX must be HTTPS")
        if not self.shadow_key_prefix or self.shadow_key_prefix == self.key_prefix:
            raise ValueError("GFW hourly v3 shadow prefix must be non-empty and differ from v2")
        if self.rolling_days != 7:
            raise ValueError("production rolling window is fixed at 7 days")
        if self.expected_tile_count <= 0:
            raise ValueError("GFW_HOURLY_EXPECTED_TILE_COUNT must be positive")
        if self.releases_to_keep != 2:
            raise ValueError("production GFW_HOURLY_RELEASES_TO_KEEP is fixed at 2")
        if self.failed_spool_retention_days < 1:
            raise ValueError("GFW_HOURLY_FAILED_SPOOL_RETENTION_DAYS must be positive")
        tiles = make_tiles(self.bbox, tile_size_degrees=self.tile_size_degrees)
        if len(tiles) != self.expected_tile_count:
            raise ValueError(
                f"bbox produces {len(tiles)} tiles, expected {self.expected_tile_count}; "
                "refusing an unreviewed report-count change"
            )

    @classmethod
    def from_config(cls) -> "GFWHourlyPublishSettings":
        settings = cls(
            token=config.GFW_ACCESS_TOKEN,
            db_url=config.SUPABASE_DB_URL or "",
            bucket=config.S3_BUCKET or "",
            public_url_prefix=config.GFW_HOURLY_PUBLIC_URL_PREFIX,
            spool_root=config.GFW_HOURLY_SPOOL_DIR,
            bbox=parse_bbox(config.GFW_HOURLY_BBOX),
            key_prefix=config.GFW_HOURLY_S3_PREFIX,
            shadow_key_prefix=config.GFW_HOURLY_SHADOW_S3_PREFIX,
            shadow_public_url_prefix=config.GFW_HOURLY_SHADOW_PUBLIC_URL_PREFIX,
            data_lag_days=config.GFW_DATA_LAG_DAYS,
            rolling_days=config.GFW_HOURLY_ROLLING_DAYS,
            tile_size_degrees=config.GFW_HOURLY_TILE_SIZE_DEGREES,
            expected_tile_count=config.GFW_HOURLY_EXPECTED_TILE_COUNT,
            gap_hours=config.GFW_HOURLY_TRACK_GAP_HOURS,
            max_speed_knots=config.GFW_HOURLY_MAX_SPEED_KNOTS,
            releases_to_keep=config.GFW_HOURLY_RELEASES_TO_KEEP,
            failed_spool_retention_days=config.GFW_HOURLY_FAILED_SPOOL_RETENTION_DAYS,
        )
        settings.validate()
        return settings


class SupabasePublishLedger:
    """Service-role-only JSON contract implemented by migration 375 upstream."""

    def __init__(self, db_url: str):
        if not db_url:
            raise ValueError("SUPABASE_DB_URL is required for GFW publish ledger")
        self.db_url = db_url

    def write(self, payload: dict[str, Any]) -> None:
        import psycopg2
        from psycopg2.extras import Json

        connection = psycopg2.connect(
            self.db_url, connect_timeout=config.SUPABASE_CONNECT_TIMEOUT
        )
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {LEDGER_FUNCTION}(%s::jsonb)",  # fixed constant, never env input
                    (Json(payload),),
                )
        finally:
            connection.close()


def _redacted_error(exc: BaseException, token: str) -> str:
    value = f"{type(exc).__name__}: {exc}"
    if token:
        value = value.replace(token, "[REDACTED]")
    return value[:1000]


def _date_window(now: datetime, *, data_lag_days: int, rolling_days: int) -> tuple[date, date]:
    current = now.astimezone(timezone.utc)
    latest = current.date() - timedelta(days=data_lag_days)
    return latest - timedelta(days=rolling_days - 1), latest


def _report_next_offset_complete(payload: Any) -> bool:
    """Duplicate the source completion guard in the persisted fetch ledger."""
    if not isinstance(payload, dict):
        raise ValueError("GFW report payload must be an object")
    return payload.get("nextOffset") in (None, 0, "0")


def _validate_fetch_completeness(
    state: dict[str, Any], *, tiles: list[Tile], expected_dataset_prefix: str,
) -> dict[str, Any]:
    entries = state["tiles"]
    expected_ids = [tile.tile_id for tile in tiles]
    completed_ids = [str(entry.get("tile_id", "")) for entry in entries]
    if len(entries) != len(tiles) or sorted(completed_ids) != sorted(expected_ids):
        raise RuntimeError("GFW fetch did not complete every expected tile exactly once")
    versions = [str(entry.get("resolved_dataset_version") or "") for entry in entries]
    if not all(version.startswith(expected_dataset_prefix) for version in versions):
        raise RuntimeError("GFW fetch lacks a resolved expected dataset version")
    if len(set(versions)) != 1:
        raise RuntimeError("GFW full-fidelity release requires exactly one resolved dataset version")
    if not all(entry.get("next_offset_complete") is True for entry in entries):
        raise RuntimeError("GFW fetch ledger records an incomplete paginated report")
    row_count = sum(int(entry.get("row_count", 0)) for entry in entries)
    invalid_count = sum(int(entry.get("invalid_count", 0)) for entry in entries)
    if row_count != int(state["normalized_row_count"]) or invalid_count != int(state.get("invalid_row_count", 0)):
        raise RuntimeError("GFW fetch row accounting does not match tile ledger")
    return {
        "expected_tile_count": len(tiles),
        "completed_tile_count": len(entries),
        "completed_tile_ids": sorted(completed_ids),
        "all_expected_tiles_completed": True,
        "resolved_dataset_version": versions[0],
        "single_resolved_dataset_version": True,
        "next_offset_complete": True,
        "normalized_row_count": row_count,
        "invalid_row_count": invalid_count,
    }


def fetch_shared_normalized_shards(
    *,
    client: GFWReportClient,
    tiles: list[Tile],
    start: date,
    latest: date,
    work_dir: Path,
) -> dict[str, Any]:
    """Fetch each tile once and persist only minimum normalized point fields."""
    work_dir.mkdir(parents=True, exist_ok=False)
    start_text = start.isoformat()
    end_text = (latest + timedelta(days=1)).isoformat()
    received_at = datetime.now(timezone.utc).isoformat()
    completed_tiles: list[dict[str, Any]] = []
    resolved_versions: set[str] = set()
    total_rows = invalid_rows = 0
    shard_paths: list[Path] = []

    for tile in tiles:
        before = _request_counter_snapshot(client.stats)
        payload, resolved = client.fetch(tile.bbox, start_text, end_text)
        if not _report_next_offset_complete(payload):
            raise RuntimeError(f"GFW tile {tile.tile_id} returned a non-zero nextOffset")
        normalized = GFWVesselPresenceCollector.normalize_entries(
            payload,
            snapshot_date=latest.isoformat(),
            received_at=received_at,
            zone=tile.tile_id,
            dataset=resolved or GFW_DATASET,
        )
        accepted = (
            row for row in normalized
            if row.get("presence_quality") == "accepted"
            and row.get("longitude") is not None
            and row.get("latitude") is not None
        )
        shard = work_dir / f"{tile.tile_id}.points.ndjson"
        row_count, invalid_count = _write_points(shard, accepted)
        shard_paths.append(shard)
        total_rows += row_count
        invalid_rows += invalid_count
        if not resolved:
            raise RuntimeError(f"GFW tile {tile.tile_id} lacks x-datasets resolved version")
        resolved_versions.add(resolved)
        completed_tiles.append({
            "tile_id": tile.tile_id,
            "bbox": list(tile.bbox),
            "row_count": row_count,
            "invalid_count": invalid_count,
            "resolved_dataset_version": resolved,
            "next_offset_complete": True,
            "request_counts": _request_counter_delta(before, client.stats),
        })
        # The raw payload is deliberately neither serialized nor retained.
        del payload, normalized

    state = {
        "schema_version": 1,
        "source_window": {
            "date_start": start_text,
            "date_end": latest.isoformat(),
            "date_end_exclusive": end_text,
        },
        "shared_normalized_fetch": True,
        "raw_gfw_response_saved": False,
        "logical_tile_report_count": len(tiles),
        "resolved_dataset_versions": sorted(resolved_versions),
        "normalized_row_count": total_rows,
        "invalid_row_count": invalid_rows,
        "tiles": completed_tiles,
        "requests": dict(client.stats),
    }
    state["completeness"] = _validate_fetch_completeness(
        state, tiles=tiles, expected_dataset_prefix="public-global-presence:",
    )
    _atomic_json(work_dir / "shared-fetch.json", state)
    state["shard_paths"] = shard_paths
    return state


def _parse_sar_observed_at(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("SAR row date must be a non-empty string")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid SAR row date: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return parsed.strftime("%Y-%m-%dT%H:00:00Z")


def normalize_sar_unmatched_entries(
    payload: Any, *, resolved_dataset: str | None
) -> list[dict[str, Any]]:
    """Normalize only the documented SAR report wrapper; schema drift fails closed."""
    if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
        raise ValueError("SAR report must contain an entries array")
    rows: list[dict[str, Any]] = []
    for wrapper in payload["entries"]:
        if not isinstance(wrapper, dict) or len(wrapper) != 1:
            raise ValueError("SAR entry must be a one-key dataset wrapper")
        dataset, values = next(iter(wrapper.items()))
        if not isinstance(dataset, str) or not dataset.startswith("public-global-sar-presence:"):
            raise ValueError(f"unexpected SAR dataset wrapper: {dataset!r}")
        if resolved_dataset and dataset != resolved_dataset:
            raise ValueError(
                f"SAR wrapper {dataset!r} differs from x-datasets {resolved_dataset!r}"
            )
        # Live API can return {resolved_dataset: null} when this tile/hour has no detections.
        if values is None:
            continue
        if not isinstance(values, list):
            raise ValueError("SAR dataset wrapper value must be an array or null")
        for source in values:
            if not isinstance(source, dict):
                raise ValueError("SAR detection row must be an object")
            try:
                longitude = float(source["lon"])
                latitude = float(source["lat"])
                detections = float(source["detections"])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError("SAR row lacks numeric lon/lat/detections") from exc
            if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
                raise ValueError("SAR row has out-of-range coordinates")
            if detections <= 0:
                raise ValueError("SAR row detections must be positive")
            if not detections.is_integer():
                raise ValueError("SAR row detections must be an integer count")
            rows.append({
                "observed_at": _parse_sar_observed_at(source.get("date")),
                "longitude": longitude,
                "latitude": latitude,
                "detections": int(detections),
                "source_dataset": dataset,
            })
    return rows


def _write_sar_points(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    temporary = path.with_name(f".{path.name}.tmp")
    count = 0
    with temporary.open("wb") as handle:
        for row in rows:
            handle.write(_canonical(row) + b"\n")
            count += 1
    temporary.replace(path)
    return count


def fetch_sar_unmatched_shards(
    *,
    client: GFWReportClient,
    tiles: list[Tile],
    start: date,
    latest: date,
    work_dir: Path,
) -> dict[str, Any]:
    work_dir.mkdir(parents=True, exist_ok=False)
    start_text = start.isoformat()
    end_text = (latest + timedelta(days=1)).isoformat()
    shards = []
    completed_tiles = []
    resolved_versions: set[str] = set()
    total_rows = 0
    requests_before = _request_counter_snapshot(client.stats)
    for tile in tiles:
        before = _request_counter_snapshot(client.stats)
        payload, resolved = client.fetch(
            tile.bbox,
            start_text,
            end_text,
            dataset=SAR_DATASET,
            group_by=None,
            filters=(SAR_FILTER,),
        )
        if not _report_next_offset_complete(payload):
            raise RuntimeError(f"GFW SAR tile {tile.tile_id} returned a non-zero nextOffset")
        rows = normalize_sar_unmatched_entries(payload, resolved_dataset=resolved)
        shard = work_dir / f"{tile.tile_id}.sar-unmatched.ndjson"
        row_count = _write_sar_points(shard, rows)
        shards.append(shard)
        total_rows += row_count
        if not resolved:
            raise RuntimeError(f"GFW SAR tile {tile.tile_id} lacks x-datasets resolved version")
        resolved_versions.add(resolved)
        completed_tiles.append({
            "tile_id": tile.tile_id,
            "bbox": list(tile.bbox),
            "row_count": row_count,
            "resolved_dataset_version": resolved,
            "next_offset_complete": True,
            "request_counts": _request_counter_delta(before, client.stats),
        })
        del payload, rows
    state = {
        "dataset_alias": SAR_DATASET,
        "filter": SAR_FILTER,
        "semantic_label": "SAR detection unmatched to AIS",
        "logical_tile_report_count": len(tiles),
        "normalized_row_count": total_rows,
        "resolved_dataset_versions": sorted(resolved_versions),
        "tiles": completed_tiles,
        "requests": _request_counter_delta(requests_before, client.stats),
        "combined_request_telemetry": dict(client.stats),
        "raw_gfw_response_saved": False,
    }
    state["invalid_row_count"] = 0
    state["completeness"] = _validate_fetch_completeness(
        state, tiles=tiles, expected_dataset_prefix="public-global-sar-presence:",
    )
    _atomic_json(work_dir / "sar-fetch.json", state)
    state["shard_paths"] = shards
    return state


def finalize_sar_hours(
    shards: Iterable[Path], *, work_dir: Path, output_dir: Path,
    start: date, latest: date,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Disk-backed exact-grid aggregation for sparse SAR unmatched detections."""
    database = work_dir / "sar-finalize.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("""
        CREATE TABLE detections (
            observed_at TEXT NOT NULL,
            longitude REAL NOT NULL,
            latitude REAL NOT NULL,
            detections REAL NOT NULL,
            source_dataset TEXT NOT NULL,
            PRIMARY KEY (observed_at, longitude, latitude)
        ) WITHOUT ROWID
    """)
    input_rows = 0
    for shard in shards:
        with shard.open("rb") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                connection.execute("""
                    INSERT INTO detections
                    (observed_at, longitude, latitude, detections, source_dataset)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (observed_at, longitude, latitude) DO UPDATE SET
                      detections = MAX(detections, excluded.detections),
                      source_dataset = excluded.source_dataset
                """, (
                    row["observed_at"], row["longitude"], row["latitude"],
                    row["detections"], row["source_dataset"],
                ))
                input_rows += 1
    connection.commit()
    by_hour: dict[str, list[dict[str, Any]]] = defaultdict(list)
    detection_total = 0.0
    for observed_at, longitude, latitude, detections, dataset in connection.execute("""
        SELECT observed_at, longitude, latitude, detections, source_dataset
        FROM detections ORDER BY observed_at, longitude, latitude
    """):
        detection_value: int | float = (
            int(detections) if float(detections).is_integer() else float(detections)
        )
        detection_total += float(detections)
        identifier = hashlib.sha256(
            _canonical([observed_at, longitude, latitude])
        ).hexdigest()[:20]
        by_hour[observed_at].append({
            "type": "Feature",
            "id": identifier,
            "properties": {
                "observed_at": observed_at,
                "grid_lon": longitude,
                "grid_lat": latitude,
                "detections": detection_value,
                "source_dataset": dataset,
                "matched_to_ais": False,
                "match_filter": SAR_FILTER,
                "matching_semantics": "SAR_detection_not_matched_to_AIS",
                "semantic_label": "SAR detection unmatched to AIS",
                "interpretation_note": (
                    "Suspected vessel detection without an AIS match; not proof of an "
                    "AIS-off, dark, or illegal vessel"
                ),
                "coordinate_semantics": "GFW_HIGH_grid_cell_center",
            },
            "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
        })
    connection.close()

    hour_entries = []
    current_hour = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    end_exclusive = datetime.combine(
        latest + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )
    while current_hour < end_exclusive:
        observed_at = current_hour.strftime("%Y-%m-%dT%H:00:00Z")
        features = by_hour.get(observed_at, [])
        stamp = current_hour.strftime("%Y%m%dT%HZ")
        relative = Path("hours") / f"{stamp}.geojson"
        collection = {
            "type": "FeatureCollection",
            "metadata": {
                "observed_at": observed_at,
                "temporal_resolution": "HOURLY",
                "spatial_resolution": "HIGH",
                "semantic_label": "SAR detection unmatched to AIS",
                "not_proof_of_dark_or_illegal_vessel": True,
                "feature_count": len(features),
                "detection_count": sum(
                    float(feature["properties"]["detections"]) for feature in features
                ),
            },
            "features": features,
        }
        _atomic_json(output_dir / relative, collection)
        hour_entries.append({
            "observed_at": observed_at,
            "path": relative.as_posix(),
            "features": len(features),
            "detections": collection["metadata"]["detection_count"],
        })
        current_hour += timedelta(hours=1)
    return hour_entries, {
        "input_rows": input_rows,
        "unique_grid_hours": sum(len(value) for value in by_hour.values()),
        "hour_count": len(hour_entries),
        "detection_count": int(detection_total) if detection_total.is_integer() else detection_total,
    }


def _asset_entry(path: Path, *, root: Path, asset_type: str, **extra: Any) -> dict[str, Any]:
    relative = path.relative_to(root).as_posix()
    return {
        "path": relative,
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
        "type": asset_type,
        **extra,
    }


def build_unified_release(
    *,
    release_dir: Path,
    work_dir: Path,
    shards: list[Path],
    sar_work_dir: Path,
    sar_shards: list[Path],
    settings: GFWHourlyPublishSettings,
    start: date,
    latest: date,
    fetch_state: dict[str, Any],
    sar_state: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    """Fan one normalized shard set out to lossless schema-v3 browser assets."""
    release_dir.mkdir(parents=True, exist_ok=False)
    release_id = latest.isoformat()
    grid_source_root = work_dir / "grid-source"
    hour_entries, grid_counts = finalize_grid(
        shards, work_dir=work_dir, output_dir=grid_source_root, poc=False
    )
    hour_by_observed = {entry["observed_at"]: entry for entry in hour_entries}
    current_hour = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
    end_exclusive = datetime.combine(
        latest + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )
    while current_hour < end_exclusive:
        observed_at = current_hour.strftime("%Y-%m-%dT%H:00:00Z")
        if observed_at not in hour_by_observed:
            relative = Path("hours") / f"{current_hour.strftime('%Y%m%dT%HZ')}.geojson"
            _atomic_json(grid_source_root / relative, {
                "type": "FeatureCollection",
                "metadata": {
                    "poc": False,
                    "observed_at": observed_at,
                    "temporal_resolution": "HOURLY",
                    "spatial_resolution": "HIGH",
                    "coordinate_semantics": "GFW_HIGH_grid_cell_center",
                    "position_note": "GFW grid-cell center; not a raw AIS position",
                    "feature_count": 0,
                    "vessel_presence_count": 0,
                    "observation_count": 0,
                },
                "features": [],
            })
            hour_by_observed[observed_at] = {
                "observed_at": observed_at,
                "path": relative.as_posix(),
                "cell_count": 0,
                "vessel_count": 0,
            }
        current_hour += timedelta(hours=1)
    hour_entries = [hour_by_observed[key] for key in sorted(hour_by_observed)]
    grid_counts["hour_count"] = len(hour_entries)
    assets: list[dict[str, Any]] = []
    browser_grid_hours, browser_grid_counts = build_grid_browser_assets(
        hour_entries,
        source_root=grid_source_root,
        output_root=release_dir,
        release_id=release_id,
    )
    grid_index: list[dict[str, Any]] = []
    for entry in browser_grid_hours:
        pmtiles_asset = _asset_entry(
            release_dir / entry["path"], root=release_dir,
            asset_type="grid_hour_pmtiles", features=int(entry["cell_count"]),
            vessel_count=int(entry["vessel_count"]),
        )
        assets.append(pmtiles_asset)
        detail_index = []
        for detail in entry["detail_buckets"]:
            detail_asset = _asset_entry(
                release_dir / detail["path"], root=release_dir,
                asset_type="grid_detail_bucket", features=int(detail["entry_count"]),
                vessel_count=int(detail["vessel_count"]), bucket=detail["bucket"],
            )
            assets.append(detail_asset)
            detail_index.append({
                "bucket": detail["bucket"], "path": detail_asset["path"],
                "sha256": detail_asset["sha256"], "bytes": detail_asset["bytes"],
                "features": detail_asset["features"],
                "vessel_count": detail_asset["vessel_count"],
            })
        grid_index.append({
            "observed_at": entry["observed_at"], "path": pmtiles_asset["path"],
            "sha256": pmtiles_asset["sha256"], "bytes": pmtiles_asset["bytes"],
            "features": pmtiles_asset["features"],
            "vessel_count": pmtiles_asset["vessel_count"],
            "format": "pmtiles", "source_layer": entry["source_layer"],
            "detail_buckets": detail_index,
        })
    if int(browser_grid_counts["cell_count"]) != int(grid_counts["feature_count"]):
        raise RuntimeError("grid browser cell count does not match canonical grid count")
    if int(browser_grid_counts["member_count"]) != int(grid_counts["vessel_presence_count"]):
        raise RuntimeError("grid browser member count does not match canonical vessel-hour count")
    for entry in hour_entries:
        (grid_source_root / entry["path"]).unlink()
    (grid_source_root / "hours").rmdir()
    grid_source_root.rmdir()

    track_store = finalize_track_store(
        shards,
        work_dir=work_dir,
        gap_hours=settings.gap_hours,
        max_speed_knots=settings.max_speed_knots,
    )
    try:
        segment_counts = track_store.stats
        track_counts = track_store.counts()
        track_input_rows = track_store.tile_rows
        browser_track_days, browser_track_frames, browser_track_counts = build_track_browser_assets(
            track_store, start=start, latest=latest, output_root=release_dir,
            release_id=release_id,
        )
        track_days: list[dict[str, Any]] = []
        for entry in browser_track_days:
            day_asset = _asset_entry(
                release_dir / entry["path"], root=release_dir,
                asset_type="tracks_day_pmtiles",
                features=int(entry["edge_count"]) + int(entry["singleton_count"]),
                edge_count=int(entry["edge_count"]),
                singleton_count=int(entry["singleton_count"]),
            )
            assets.append(day_asset)
            detail_index = []
            for detail in entry["detail_buckets"]:
                detail_asset = _asset_entry(
                    release_dir / detail["path"], root=release_dir,
                    asset_type="track_detail_bucket", features=int(detail["entry_count"]),
                    points=int(detail["point_count"]), bucket=detail["bucket"],
                )
                assets.append(detail_asset)
                detail_index.append({
                    "bucket": detail["bucket"], "path": detail_asset["path"],
                    "sha256": detail_asset["sha256"], "bytes": detail_asset["bytes"],
                    "features": detail_asset["features"], "points": detail_asset["points"],
                })
            track_days.append({
                "display_date": entry["display_date"], "path": day_asset["path"],
                "sha256": day_asset["sha256"], "bytes": day_asset["bytes"],
                "features": day_asset["features"], "edge_count": day_asset["edge_count"],
                "singleton_count": day_asset["singleton_count"], "format": "pmtiles",
                "points": int(day_asset["edge_count"]) * 2 + int(day_asset["singleton_count"]),
                "overlap": {
                    "lookback_hours": DEFAULT_LOOKBACK_HOURS,
                    "lookahead_hours": DEFAULT_LOOKAHEAD_HOURS,
                },
                "source_layers": entry["source_layers"], "detail_buckets": detail_index,
            })
        track_frames: list[dict[str, Any]] = []
        for entry in browser_track_frames:
            frame_asset = _asset_entry(
                release_dir / entry["path"], root=release_dir,
                asset_type="track_frame_hour", features=int(entry["features"]),
            )
            assets.append(frame_asset)
            track_frames.append({
                "observed_at": entry["observed_at"], "path": frame_asset["path"],
                "sha256": frame_asset["sha256"], "bytes": frame_asset["bytes"],
                "features": frame_asset["features"], "format": "geojson",
                "content_encoding": "gzip",
            })
        if int(browser_track_counts["frame_vessel_count"]) != int(track_counts["canonical_points"]):
            raise RuntimeError("track frame node count does not match canonical point count")
    finally:
        track_store.close()

    dark_root = release_dir / "dark_vessels"
    dark_hours_raw, dark_counts = finalize_sar_hours(
        sar_shards, work_dir=sar_work_dir, output_dir=dark_root,
        start=start, latest=latest,
    )
    dark_hours = []
    for entry in dark_hours_raw:
        path = dark_root / entry["path"]
        asset = _asset_entry(
            path,
            root=release_dir,
            asset_type="sar_unmatched_hour",
            features=int(entry["features"]),
            detections=entry["detections"],
        )
        assets.append(asset)
        dark_hours.append({
            "observed_at": entry["observed_at"],
            "path": asset["path"],
            "sha256": asset["sha256"],
            "bytes": asset["bytes"],
            "features": asset["features"],
            "detections": asset["detections"],
        })

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "full_fidelity": True,
        "release_id": release_id,
        "latest_complete_date": release_id,
        "date_start": start.isoformat(),
        "date_end": latest.isoformat(),
        "generated_at": generated_at,
        "bbox": list(settings.bbox),
        "source": {
            "dataset_alias": GFW_DATASET,
            "resolved_dataset_versions": fetch_state["resolved_dataset_versions"],
            "temporal_resolution": "HOURLY",
            "spatial_resolution": "HIGH",
            "coordinate_semantics": "GFW_HIGH_grid_cell_center",
        },
        "tracks": {
            "days": track_days,
            "frames": track_frames,
            "source_layers": {
                "edges": "gfw_track_edges",
                "singletons": "gfw_track_singletons",
            },
            "detail_contract": {
                "bucket_count": 16,
                "hash": "sha256_hex_prefix",
                "prefix_length": 1,
                "key": "track_id",
                "format": "json",
                "content_encoding": "gzip",
            },
            "counts": {
                "input_rows": track_input_rows,
                **segment_counts,
                **track_counts,
                "candidate_features": (
                    int(track_counts["eligible_segment_count"])
                    + int(track_counts["singleton_node_count"])
                ),
                "displayed_features": (
                    int(track_counts["published_segment_count"])
                    + int(track_counts["singleton_node_count"])
                ),
                "published_features": (
                    int(track_counts["published_segment_count"])
                    + int(track_counts["singleton_node_count"])
                ),
                "candidate_points": int(track_counts["canonical_points"]),
                "displayed_points": int(track_counts["canonical_points"]),
                "published_points": int(track_counts["canonical_points"]),
                "cap_applied": False,
                "omitted_by_display_cap": 0,
                "omitted_features": 0,
                "omitted_points": 0,
            },
            "contract": {
                "frontend_load": "one_UTC_day_PMTiles_plus_two_adjacent_hour_frames",
                "supported_trail_hours": [0.5, 1.0, 2.0, 3.0],
                "maximum_lookback_hours": DEFAULT_LOOKBACK_HOURS,
                "lookahead_hours_for_linear_interpolation": DEFAULT_LOOKAHEAD_HOURS,
                "default_trail_hours": 0.5,
                "interpolation": "linear_only_when_frame_has_same_segment_adjacent_hour_target",
            },
        },
        "grid": {
            "hours": grid_index,
            "source_layer": "gfw_grid",
            "geometry_semantics": "inferred_0_01_degree_footprint",
            "coordinate_semantics": "GFW_HIGH_grid_cell_center",
            "geometry_note": "Visualization footprint center +/- 0.005 degrees; not an official GFW boundary",
            "detail_contract": {
                "bucket_count": 16,
                "hash": "sha256_hex_prefix",
                "prefix_length": 1,
                "key": "cell_id",
                "format": "json",
                "content_encoding": "gzip",
            },
            "counts": {**grid_counts, **browser_grid_counts},
        },
        "dark_vessels": {
            "latest_complete_date": latest.isoformat(),
            "date_start": start.isoformat(),
            "date_end": latest.isoformat(),
            "hours": dark_hours,
            "counts": dark_counts,
            "source": {
                "dataset_alias": SAR_DATASET,
                "resolved_dataset_versions": sar_state["resolved_dataset_versions"],
                "filter": SAR_FILTER,
                "matching_semantics": "SAR_detection_not_matched_to_AIS",
                "coordinate_semantics": "GFW_HIGH_grid_cell_center",
            },
            "semantic_label": "SAR detection unmatched to AIS",
            "interpretation_note": (
                "Suspected vessel detection without an AIS match; not proof of an "
                "AIS-off, dark, or illegal vessel"
            ),
        },
        "assets": assets,
        "pipeline": {
            "shared_normalized_fetch": True,
            "logical_tile_report_count": (
                fetch_state["logical_tile_report_count"]
                + sar_state["logical_tile_report_count"]
            ),
            "ais_tile_report_count": fetch_state["logical_tile_report_count"],
            "sar_unmatched_tile_report_count": sar_state["logical_tile_report_count"],
            "requests": {
                "ais_after_fetch": fetch_state["requests"],
                "sar_delta": sar_state["requests"],
                "combined_current_process": sar_state["combined_request_telemetry"],
            },
            "raw_gfw_response_saved": False,
            "rolling_refetch": "full_7_day_window_each_daily_run",
        },
        "retention": {
            "rolling_source_days": settings.rolling_days,
            "published_releases_kept": settings.releases_to_keep,
            "rollback_release_count": settings.releases_to_keep - 1,
        },
        "cache_contract": {
            "root_manifest": "public,max-age=60,s-maxage=60,stale-while-revalidate=300",
            "immutable_release": "public,max-age=604800,s-maxage=604800,immutable",
            "retired_edge_tail": "up_to_7_days_without_exact_Cloudflare_URL_purge",
        },
        "attribution": {
            "label": "Powered by Global Fishing Watch",
            "href": "https://globalfishingwatch.org/",
        },
    }
    required_types = {
        "tracks_day_pmtiles", "grid_hour_pmtiles", "track_frame_hour",
        "grid_detail_bucket", "track_detail_bucket", "sar_unmatched_hour",
    }
    if {asset["type"] for asset in assets} != required_types:
        raise ValueError("unified v3 release must contain all six typed asset products")
    expected_hours = settings.rolling_days * 24
    if (
        len(track_days) != settings.rolling_days
        or len(track_frames) != expected_hours
        or len(grid_index) != expected_hours
        or len(dark_hours) != expected_hours
    ):
        raise ValueError("unified v3 release indexes are not a complete rolling window")
    _atomic_json(release_dir / "manifest.json", manifest)
    _atomic_json(release_dir / "run.json", {
        "release_id": latest.isoformat(),
        "status": "ready_for_publish",
        "generated_at": generated_at,
        "shared_normalized_fetch": True,
        "raw_gfw_response_saved": False,
    })
    return manifest


def _load_previous_root_manifest(client: Any, *, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    if int(response.get("ContentLength", 0)) > 10 * 1024 * 1024:
        raise RuntimeError("existing GFW root manifest exceeds 10 MiB")
    body = response["Body"].read()
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("existing GFW root manifest is not an object")
    return parsed


def _ledger_release_contract(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build the compact DB contract using reader-visible immutable paths."""
    release_prefix = f"releases/{manifest['release_id']}/"
    assets = [
        {**asset, "path": f"{release_prefix}{asset['path']}"}
        for asset in manifest["assets"]
    ]

    def published_index(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        published = []
        for entry in entries:
            value = {**entry, "path": f"{release_prefix}{entry['path']}"}
            if entry.get("detail_buckets"):
                value["detail_buckets"] = [
                    {**detail, "path": f"{release_prefix}{detail['path']}"}
                    for detail in entry["detail_buckets"]
                ]
            published.append(value)
        return published

    counts = manifest["tracks"]["counts"]
    summary = {
        "full_fidelity": True,
        "candidate_canonical_features": counts["candidate_features"],
        "published_canonical_features": counts["published_features"],
        "candidate_canonical_points": counts["candidate_points"],
        "published_canonical_points": counts["published_points"],
        "display_cap_applied": False,
        "omitted_features": counts["omitted_features"],
        "omitted_points": counts["omitted_points"],
        "tracks": {
            "days": published_index(manifest["tracks"]["days"]),
            "frames": published_index(manifest["tracks"]["frames"]),
            "counts": counts,
        },
        "grid": {"hours": published_index(manifest["grid"]["hours"])},
        "dark_vessels": {
            "hours": published_index(manifest["dark_vessels"]["hours"])
        },
        # Migration uses concise keys; the public root keeps the frontend names.
        "cache_contract": {
            "root": manifest["cache_contract"]["root_manifest"],
            "release": manifest["cache_contract"]["immutable_release"],
        },
    }
    return assets, summary


def _cleanup_success_spool_exact(run_root: Path, manifest: dict[str, Any], tiles: list[Tile]) -> str | None:
    """Delete only known successful spool paths; preserve everything on mismatch."""
    try:
        work_dir = run_root / "work" / "ais"
        sar_work_dir = run_root / "work" / "sar"
        release_dir = run_root / "release" / str(manifest["release_id"])
        files = [run_root / "spool.json"]
        files.extend(work_dir / f"{tile.tile_id}.points.ndjson" for tile in tiles)
        files.extend([work_dir / "shared-fetch.json", work_dir / "hourly-grid.sqlite3", work_dir / "finalize.sqlite3"])
        files.extend([sar_work_dir / f"{tile.tile_id}.sar-unmatched.ndjson" for tile in tiles])
        files.extend([sar_work_dir / "sar-fetch.json", sar_work_dir / "sar-finalize.sqlite3"])
        asset_directories: set[Path] = set()
        for asset in manifest["assets"]:
            relative = Path(str(asset["path"]))
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"unsafe local asset path: {relative}")
            asset_path = release_dir / relative
            files.append(asset_path)
            parent = asset_path.parent
            while parent != release_dir:
                asset_directories.add(parent)
                parent = parent.parent
        files.extend([release_dir / "manifest.json", release_dir / "run.json"])
        for path in files:
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"unexpected successful spool file: {path}")
        for path in files:
            path.unlink()
        for directory in (
            *sorted(asset_directories, key=lambda value: len(value.parts), reverse=True),
            release_dir,
            run_root / "release",
            work_dir,
            sar_work_dir,
            run_root / "work",
            run_root,
        ):
            directory.rmdir()
        return None
    except Exception as exc:
        return str(exc)


def _validated_failed_spool_paths(run_root: Path) -> tuple[list[Path], list[Path]]:
    """Return exact files/dirs only when the entire failed spool tree is known."""
    if run_root.is_symlink() or not run_root.is_dir() or not _SPOOL_RUN.fullmatch(run_root.name):
        raise ValueError(f"unsafe failed spool root: {run_root}")
    release_id = run_root.name[:10]
    allowed_dirs = {
        ("work",), ("work", "ais"), ("work", "sar"), ("release",),
        ("work", "ais", "grid-source"),
        ("work", "ais", "grid-source", "hours"),
        ("release", release_id),
        ("release", release_id, "grid"),
        ("release", release_id, "grid", "hours"),
        ("release", release_id, "grid", "details"),
        ("release", release_id, "grid", "input"),
        ("release", release_id, "tracks"),
        ("release", release_id, "tracks", "days"),
        ("release", release_id, "tracks", "details"),
        ("release", release_id, "tracks", "frames"),
        ("release", release_id, "tracks", "input"),
        ("release", release_id, "dark_vessels"),
        ("release", release_id, "dark_vessels", "hours"),
    }
    files: list[Path] = []
    directories: list[Path] = []
    pending = [run_root]
    while pending:
        parent = pending.pop()
        for child in parent.iterdir():
            if child.is_symlink():
                raise ValueError(f"failed spool contains symlink: {child}")
            relative = child.relative_to(run_root)
            parts = relative.parts
            if child.is_dir():
                dynamic_dir = (
                    len(parts) == 5
                    and parts[:4] == ("release", release_id, "grid", "details")
                    and bool(_HOUR_STAMP.fullmatch(parts[4]))
                ) or (
                    len(parts) == 5
                    and parts[:4] == ("release", release_id, "tracks", "details")
                    and bool(_UTC_DATE.fullmatch(parts[4]))
                )
                if parts not in allowed_dirs and not dynamic_dir:
                    raise ValueError(f"failed spool contains unknown directory: {relative}")
                directories.append(child)
                pending.append(child)
                continue
            if not child.is_file():
                raise ValueError(f"failed spool contains non-file: {relative}")
            allowed = False
            if parts == ("spool.json",):
                allowed = True
            elif parts[:2] == ("work", "ais") and len(parts) == 3:
                allowed = bool(_TILE_FILE.fullmatch(parts[2])) or parts[2] in {
                    "shared-fetch.json", ".shared-fetch.json.tmp",
                    "hourly-grid.sqlite3", "hourly-grid.sqlite3-journal",
                    "finalize.sqlite3", "finalize.sqlite3-journal",
                }
            elif parts[:4] == ("work", "ais", "grid-source", "hours") and len(parts) == 5:
                allowed = bool(_HOUR_FILE.fullmatch(parts[4]))
            elif parts[:2] == ("work", "sar") and len(parts) == 3:
                allowed = bool(_TILE_FILE.fullmatch(parts[2])) or parts[2] in {
                    "sar-fetch.json", ".sar-fetch.json.tmp",
                    "sar-finalize.sqlite3", "sar-finalize.sqlite3-journal",
                }
            elif parts[:2] == ("release", release_id) and len(parts) == 3:
                allowed = parts[2] in {
                    "manifest.json", ".manifest.json.tmp",
                    "run.json", ".run.json.tmp",
                }
            elif parts[:4] == ("release", release_id, "grid", "hours") and len(parts) == 5:
                allowed = bool(_HOUR_PM.fullmatch(parts[4]))
            elif parts[:4] == ("release", release_id, "grid", "input") and len(parts) == 5:
                allowed = bool(_GRID_INPUT.fullmatch(parts[4]))
            elif len(parts) == 6 and parts[:4] == ("release", release_id, "grid", "details"):
                allowed = bool(_HOUR_STAMP.fullmatch(parts[4])) and bool(_BUCKET_GZIP.fullmatch(parts[5]))
            elif parts[:4] == ("release", release_id, "dark_vessels", "hours") and len(parts) == 5:
                allowed = bool(_HOUR_FILE.fullmatch(parts[4]))
            elif parts[:4] == ("release", release_id, "tracks", "days") and len(parts) == 5:
                allowed = bool(_DAY_PM.fullmatch(parts[4]))
            elif parts[:4] == ("release", release_id, "tracks", "frames") and len(parts) == 5:
                allowed = bool(_FRAME_GZIP.fullmatch(parts[4]))
            elif parts[:4] == ("release", release_id, "tracks", "input") and len(parts) == 5:
                allowed = bool(_TRACK_INPUT.fullmatch(parts[4]))
            elif len(parts) == 6 and parts[:4] == ("release", release_id, "tracks", "details"):
                allowed = bool(_UTC_DATE.fullmatch(parts[4])) and bool(_BUCKET_GZIP.fullmatch(parts[5]))
            if not allowed:
                raise ValueError(f"failed spool contains unknown file: {relative}")
            files.append(child)
    return files, sorted(directories, key=lambda value: len(value.parts), reverse=True)


def prune_expired_failed_spools(
    spool_root: Path, *, now: datetime, retention_days: int
) -> dict[str, list[Any]]:
    """Prune only failed spools older than the bounded retention contract."""
    if retention_days < 1:
        raise ValueError("failed spool retention must be at least one day")
    spool_root.mkdir(parents=True, exist_ok=True)
    threshold = now.astimezone(timezone.utc) - timedelta(days=retention_days)
    pruned: list[str] = []
    warnings: list[Any] = []
    for candidate in spool_root.iterdir():
        if not candidate.is_dir() or candidate.is_symlink() or not _SPOOL_RUN.fullmatch(candidate.name):
            continue
        ledger_path = candidate / "spool.json"
        try:
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            if ledger.get("status") != "failed":
                continue
            failed_at = datetime.fromisoformat(str(ledger["failed_at"]).replace("Z", "+00:00"))
            if failed_at.tzinfo is None:
                raise ValueError("failed_at must include timezone")
            if failed_at.astimezone(timezone.utc) > threshold:
                continue
            files, directories = _validated_failed_spool_paths(candidate)
            for path in files:
                path.unlink()
            for directory in directories:
                directory.rmdir()
            candidate.rmdir()
            pruned.append(candidate.name)
        except Exception as exc:
            warnings.append({"spool": candidate.name, "error": str(exc)})
    return {"pruned": pruned, "warnings": warnings}


class GFWHourlyPublishTask:
    name = "gfw_hourly_publish"

    def __init__(
        self,
        settings: GFWHourlyPublishSettings | None = None,
        *,
        ledger: Any | None = None,
        report_client_factory: Callable[[str], GFWReportClient] | None = None,
        s3_client_factory: Callable[[], Any] | None = None,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
    ):
        self.settings = settings or GFWHourlyPublishSettings.from_config()
        self.settings.validate()
        self.ledger = ledger or SupabasePublishLedger(self.settings.db_url)
        self.report_client_factory = report_client_factory or (lambda token: GFWReportClient(token))
        self.s3_client_factory = s3_client_factory or self._default_s3_client
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep or time.sleep

    def _write_succeeded_ledger_with_retry(self, payload: dict[str, Any]) -> None:
        """Bounded retry after reader-visible cutover; never relabel it failed."""
        delays = (0.5, 2.0)
        for attempt in range(len(delays) + 1):
            try:
                self.ledger.write(payload)
                return
            except Exception:
                if attempt == len(delays):
                    raise
                self.sleep(delays[attempt])

    @staticmethod
    def _default_s3_client() -> Any:
        from storage.s3 import S3Storage

        return S3Storage().s3

    def run(self) -> dict[str, Any]:
        started_at = self.now().astimezone(timezone.utc)
        start, latest = _date_window(
            started_at,
            data_lag_days=self.settings.data_lag_days,
            rolling_days=self.settings.rolling_days,
        )
        release_id = latest.isoformat()
        if not _UTC_DATE.fullmatch(release_id):
            raise AssertionError("release id is not an ISO UTC date")
        run_id = str(uuid.uuid4())
        prune_result = prune_expired_failed_spools(
            self.settings.spool_root,
            now=started_at,
            retention_days=self.settings.failed_spool_retention_days,
        )
        for warning in prune_result["warnings"]:
            logger.warning("GFW failed-spool cleanup retained an unknown tree: %s", warning)
        run_root = self.settings.spool_root / f"{release_id}-{run_id}"
        run_root.mkdir(parents=True, exist_ok=False)
        _atomic_json(run_root / "spool.json", {
            "run_id": run_id,
            "release_id": release_id,
            "status": "running",
            "started_at": started_at.isoformat(),
            "retention_days_after_failure": self.settings.failed_spool_retention_days,
        })
        base_ledger = {
            "run_id": run_id,
            "release_id": release_id,
            "latest_complete_date": release_id,
            "date_start": start.isoformat(),
            "date_end": release_id,
            "bbox": list(self.settings.bbox),
            "source_dataset_alias": GFW_DATASET,
            "started_at": started_at.isoformat(),
            # The first DB row must be v3/shadow too: migration defaults may
            # otherwise describe an in-progress run as canonical v2.
            "manifest_schema_version": SCHEMA_VERSION,
            "root_manifest_key": f"{self.settings.shadow_key_prefix}/manifest.json",
        }
        running_written = False
        cutover_done = False
        succeeded_ledger_payload: dict[str, Any] | None = None
        try:
            # Hard gate: migration/DB failure must happen before any GFW request.
            self.ledger.write({**base_ledger, "status": "running"})
            running_written = True

            tiles = make_tiles(
                self.settings.bbox,
                tile_size_degrees=self.settings.tile_size_degrees,
            )
            client = self.report_client_factory(self.settings.token)
            fetch_state = fetch_shared_normalized_shards(
                client=client,
                tiles=tiles,
                start=start,
                latest=latest,
                work_dir=run_root / "work" / "ais",
            )
            # Distinct source and semantics; sequential on the same client because
            # GFW permits only one in-flight report per account.
            sar_state = fetch_sar_unmatched_shards(
                client=client,
                tiles=tiles,
                start=start,
                latest=latest,
                work_dir=run_root / "work" / "sar",
            )
            generated_at = self.now().astimezone(timezone.utc).isoformat()
            release_dir = run_root / "release" / release_id
            manifest = build_unified_release(
                release_dir=release_dir,
                work_dir=run_root / "work" / "ais",
                shards=fetch_state.pop("shard_paths"),
                sar_work_dir=run_root / "work" / "sar",
                sar_shards=sar_state.pop("shard_paths"),
                settings=self.settings,
                start=start,
                latest=latest,
                fetch_state=fetch_state,
                sar_state=sar_state,
                generated_at=generated_at,
            )
            s3_client = self.s3_client_factory()
            root_key = f"{self.settings.shadow_key_prefix}/manifest.json"
            previous = _load_previous_root_manifest(
                s3_client, bucket=self.settings.bucket, key=root_key
            )
            published = publish_release_to_s3(
                s3_client,
                release_dir=release_dir,
                bucket=self.settings.bucket,
                key_prefix=self.settings.shadow_key_prefix,
                public_url_prefix=self.settings.shadow_public_url_prefix,
                previous_root_manifest=previous,
                releases_to_keep=self.settings.releases_to_keep,
            )
            cutover_done = True
            finished_at = self.now().astimezone(timezone.utc).isoformat()
            ledger_assets, manifest_summary = _ledger_release_contract(manifest)
            succeeded_ledger_payload = {
                **base_ledger,
                "status": "succeeded",
                "completed_at": finished_at,
                "generated_at": generated_at,
                "published_at": finished_at,
                "sar_latest_complete_date": release_id,
                "manifest_schema_version": SCHEMA_VERSION,
                "root_manifest_key": published["root_manifest_key"],
                "root_manifest_sha256": published["root_manifest_sha256"],
                "root_manifest_bytes": published["root_manifest_bytes"],
                "source_dataset_id": GFW_DATASET,
                "resolved_dataset_version": ",".join(fetch_state["resolved_dataset_versions"]),
                "assets": ledger_assets,
                "manifest_summary": manifest_summary,
                "request_summary": {
                    **manifest["pipeline"]["requests"],
                    "logical_tile_report_count": manifest["pipeline"]["logical_tile_report_count"],
                    "ais_tile_report_count": manifest["pipeline"]["ais_tile_report_count"],
                    "sar_unmatched_tile_report_count": manifest["pipeline"]["sar_unmatched_tile_report_count"],
                    "normalized_row_count": fetch_state["normalized_row_count"],
                    "sar_unmatched_row_count": sar_state["normalized_row_count"],
                },
            }
            self._write_succeeded_ledger_with_retry(succeeded_ledger_payload)
            _atomic_json(run_root / "spool.json", {
                "run_id": run_id,
                "release_id": release_id,
                "status": "succeeded",
                "finished_at": finished_at,
            })
            cleanup_warning = _cleanup_success_spool_exact(run_root, manifest, tiles)
            return {
                **published,
                "run_id": run_id,
                "latest_complete_date": release_id,
                "logical_tile_report_count": (
                    fetch_state["logical_tile_report_count"]
                    + sar_state["logical_tile_report_count"]
                ),
                "normalized_row_count": fetch_state["normalized_row_count"],
                "cleanup_warning": cleanup_warning,
            }
        except (Exception, KeyboardInterrupt) as exc:
            try:
                if cutover_done:
                    _atomic_json(run_root / "spool.json", {
                        "run_id": run_id,
                        "release_id": release_id,
                        "status": "cutover_succeeded_ledger_pending",
                        "updated_at": self.now().astimezone(timezone.utc).isoformat(),
                        "error": _redacted_error(exc, self.settings.token),
                        "reconciliation_file": "reconcile-ledger.json",
                    })
                    if succeeded_ledger_payload is not None:
                        _atomic_json(
                            run_root / "reconcile-ledger.json", succeeded_ledger_payload
                        )
                else:
                    _atomic_json(run_root / "spool.json", {
                        "run_id": run_id,
                        "release_id": release_id,
                        "status": "failed",
                        "failed_at": self.now().astimezone(timezone.utc).isoformat(),
                        "error": _redacted_error(exc, self.settings.token),
                        "retention_days": self.settings.failed_spool_retention_days,
                    })
            except Exception:
                pass
            if running_written and not cutover_done:
                try:
                    self.ledger.write({
                        **base_ledger,
                        "status": "failed",
                        "completed_at": self.now().astimezone(timezone.utc).isoformat(),
                        "error_message": _redacted_error(exc, self.settings.token),
                    })
                except Exception as ledger_exc:
                    logger.error(
                        "GFW hourly publish failed and failed-ledger write also failed: %s",
                        _redacted_error(ledger_exc, self.settings.token),
                    )
            # A reader-visible cutover is never relabelled failed merely because
            # the final succeeded-ledger call failed.  The spool is retained.
            if cutover_done:
                logger.error(
                    "GFW root cutover succeeded but succeeded ledger is pending reconciliation; "
                    "spool retained at %s",
                    run_root,
                )
            else:
                logger.error("GFW hourly publish failed; spool retained at %s", run_root)
            raise
