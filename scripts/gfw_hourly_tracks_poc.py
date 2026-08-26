#!/usr/bin/env python3
"""Generate a browser-sized GFW hourly approximate-tracks GeoJSON POC.

This is an explicit local/export POC. It does not use BaseCollector, Supabase,
S3, or the raw-response archive. GFW report responses are projected immediately
to the minimum normalized point fields. Completed normalized tile shards are
kept only as resumable work files and removed after a successful export.

Example:
    python3 scripts/gfw_hourly_tracks_poc.py \
      --output ../mini-taiwan-pulse/public/gfw_hourly_tracks_poc.geojson

Partitioned local release (manifest + one GeoJSON per UTC display day):
    python3 scripts/gfw_hourly_tracks_poc.py \
      --output-dir /tmp/gfw-hourly-tracks-release
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sqlite3
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests

# Running ``python3 scripts/...`` puts scripts/ rather than the repository root
# first on sys.path. Keep this executable without requiring package install.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from collectors.gfw_vessel_presence import (  # noqa: E402
    GFW_DATASET,
    GFWVesselPresenceCollector,
    _next_offset,
    _polygon,
    _report_body,
)
from scripts.gfw_hourly_release import publish_track_release  # noqa: E402


DEFAULT_BBOX = (122.43400, 23.22953, 132.85274, 34.35812)
DEFAULT_LATEST_COMPLETE_DAY = "2026-08-21"
LAST_REPORT_URL = "https://gateway.api.globalfishingwatch.org/v3/4wings/last-report"


class IncompleteReportError(RuntimeError):
    """Raised when GFW indicates that a report response is paginated."""


class ReportHTTPError(RuntimeError):
    """Raised for a non-recoverable GFW HTTP response."""


@dataclass(frozen=True)
class Tile:
    tile_id: str
    bbox: tuple[float, float, float, float]


def _round_coord(value: float) -> float:
    return round(value, 8)


def make_tiles(
    bbox: tuple[float, float, float, float], *, tile_size_degrees: float = 3.0
) -> list[Tile]:
    """Split west/south/east/north bbox into stable row-major tiles."""
    west, south, east, north = bbox
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("bbox must be west,south,east,north within WGS84 bounds")
    if tile_size_degrees <= 0:
        raise ValueError("tile_size_degrees must be positive")
    columns = math.ceil((east - west) / tile_size_degrees)
    rows = math.ceil((north - south) / tile_size_degrees)
    tiles: list[Tile] = []
    for row in range(rows):
        tile_south = south + row * tile_size_degrees
        tile_north = min(north, tile_south + tile_size_degrees)
        for column in range(columns):
            tile_west = west + column * tile_size_degrees
            tile_east = min(east, tile_west + tile_size_degrees)
            tiles.append(Tile(
                tile_id=f"r{row:02d}c{column:02d}",
                bbox=tuple(map(_round_coord, (tile_west, tile_south, tile_east, tile_north))),
            ))
    return tiles


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _request_signature(params: dict[str, str], body: dict[str, Any]) -> dict[str, Any]:
    return {"params": params, "body": body}


def _request_counter_snapshot(stats: dict[str, Any]) -> dict[str, int]:
    return {
        "post_requests": int(stats.get("post_requests", 0)),
        "recovery_requests": int(stats.get("recovery_requests", 0)),
        "retries": int(stats.get("retries", 0)),
    }


def _request_counter_delta(before: dict[str, int], after: dict[str, Any]) -> dict[str, int]:
    return {
        key: int(after.get(key, 0)) - value
        for key, value in before.items()
    }


def _extract_current_report(payload: Any) -> tuple[str | None, Any]:
    if not isinstance(payload, dict):
        return None, None
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        metadata = message.get("metadata")
        if isinstance(metadata, dict):
            return metadata.get("currentReportUrl"), metadata.get("currentReportBody")
    return None, None


def _current_report_matches(url: str | None, body: Any, params: dict[str, str], expected_body: dict[str, Any]) -> bool:
    """Only recover a 429 last-report when it describes our exact report."""
    if not url:
        return False
    current = parse_qs(urlparse(url).query)
    # Match the complete query signature, including dataset-specific filters.
    # This keeps last-report recovery safe for the sequential AIS + SAR job.
    if any(current.get(key, [None])[0] != value for key, value in params.items()):
        return False
    if body is None:
        return False
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            return False
    return _canonical(body) == _canonical(expected_body)


class GFWReportClient:
    """Sequential GFW report client with bounded last-report recovery."""

    def __init__(
        self,
        token: str,
        *,
        report_url: str = config.GFW_REPORT_URL,
        last_report_url: str = LAST_REPORT_URL,
        session: requests.Session | None = None,
        timeout: float = 120.0,
        retries: int = 3,
        max_polls: int = 20,
        poll_interval: float = 3.0,
    ):
        if not token:
            raise RuntimeError("GFW_ACCESS_TOKEN is required")
        self.token = token
        self.report_url = report_url
        self.last_report_url = last_report_url
        self.session = session or requests.Session()
        self.timeout = timeout
        self.retries = retries
        self.max_polls = max_polls
        self.poll_interval = poll_interval
        self.stats: dict[str, Any] = {
            "post_requests": 0,
            "recovery_requests": 0,
            "retries": 0,
            "http_statuses": {},
            "last_rate_limit_headers": {},
        }

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "GIS-DataCollectors/gfw-hourly-tracks-poc",
        }

    def _record_response(self, response: Any) -> None:
        status = str(response.status_code)
        statuses = self.stats["http_statuses"]
        statuses[status] = statuses.get(status, 0) + 1
        self.stats["last_rate_limit_headers"] = {
            key: value for key, value in response.headers.items()
            if "ratelimit" in key.lower() or "rate-limit" in key.lower()
        }

    @staticmethod
    def _json(response: Any) -> Any:
        try:
            return response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise ReportHTTPError(
                f"GFW returned non-JSON HTTP {response.status_code}"
            ) from exc

    def _validate_complete(self, payload: Any) -> None:
        next_offset = _next_offset(payload)
        if next_offset is not None:
            raise IncompleteReportError(
                f"paginated response nextOffset={next_offset}; refusing truncated report"
            )

    def _poll_last_report(self) -> tuple[Any, str | None] | None:
        for _ in range(self.max_polls):
            response = self.session.request(
                "GET", self.last_report_url, headers=self._headers, timeout=self.timeout
            )
            self.stats["recovery_requests"] += 1
            self._record_response(response)
            if response.status_code == 404:
                return None
            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(self.poll_interval)
                continue
            payload = self._json(response)
            if response.status_code >= 400:
                raise ReportHTTPError(
                    f"last-report HTTP {response.status_code}: {str(payload)[:400]}"
                )
            if isinstance(payload, dict) and str(payload.get("status", "")).lower() == "running":
                time.sleep(self.poll_interval)
                continue
            if isinstance(payload, dict) and isinstance(payload.get("status"), int):
                raise ReportHTTPError(f"last-report failed: {str(payload)[:400]}")
            self._validate_complete(payload)
            resolved = response.headers.get("x-datasets") or response.headers.get("X-Datasets")
            return payload, resolved
        raise ReportHTTPError("last-report remained running beyond bounded poll limit")

    def fetch(
        self,
        bbox: tuple[float, float, float, float],
        start: str,
        end: str,
        *,
        dataset: str = GFW_DATASET,
        group_by: str | None = "VESSEL_ID",
        filters: tuple[str, ...] = (),
    ) -> tuple[Any, str | None]:
        west, south, east, north = bbox
        params = {
            "format": "JSON",
            "temporal-resolution": "HOURLY",
            "datasets[0]": dataset,
            "date-range": f"{start},{end}",
            "spatial-aggregation": "false",
            "spatial-resolution": "HIGH",
        }
        if group_by is not None:
            params["group-by"] = group_by
        for index, value in enumerate(filters):
            params[f"filters[{index}]"] = value
        body = _report_body(_polygon((south, west, north, east)))
        for attempt in range(self.retries + 1):
            response = self.session.request(
                "POST",
                self.report_url,
                params=params,
                json=body,
                headers=self._headers,
                timeout=self.timeout,
            )
            self.stats["post_requests"] += 1
            self._record_response(response)
            payload = self._json(response)
            if response.status_code == 200:
                self._validate_complete(payload)
                resolved = response.headers.get("x-datasets") or response.headers.get("X-Datasets")
                return payload, resolved
            if response.status_code == 524:
                recovered = self._poll_last_report()
                if recovered is not None:
                    return recovered
            elif response.status_code == 429:
                current_url, current_body = _extract_current_report(payload)
                if _current_report_matches(current_url, current_body, params, body):
                    recovered = self._poll_last_report()
                    if recovered is not None:
                        return recovered
            elif response.status_code not in (500, 502, 503, 504):
                raise ReportHTTPError(
                    f"report HTTP {response.status_code}: {str(payload)[:400]}"
                )
            if attempt < self.retries:
                self.stats["retries"] += 1
                time.sleep(min(2 ** attempt, 10))
                continue
            raise ReportHTTPError(
                f"report HTTP {response.status_code} after {attempt + 1} attempts: "
                f"{str(payload)[:400]}"
            )
        raise AssertionError("unreachable")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hour_key(value: str) -> datetime:
    parsed = _parse_utc(value)
    return parsed.replace(minute=0, second=0, microsecond=0)


def _haversine_nm(a: dict[str, Any], b: dict[str, Any]) -> float:
    lon1, lat1 = math.radians(float(a["longitude"])), math.radians(float(a["latitude"]))
    lon2, lat2 = math.radians(float(b["longitude"])), math.radians(float(b["latitude"]))
    dlon, dlat = lon2 - lon1, lat2 - lat1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 3440.065 * 2 * math.asin(min(1.0, math.sqrt(h)))


def _mode(rows: list[dict[str, Any]], key: str) -> Any:
    values = [str(row[key]).strip() for row in rows if row.get(key) not in (None, "")]
    if not values:
        return None
    counts = Counter(values)
    return min(counts, key=lambda value: (-counts[value], value))


def validate_feature_time_contract(feature: dict[str, Any]) -> None:
    """Fail closed unless vertex timestamps align 1:1 and increase in UTC."""
    try:
        properties = feature["properties"]
        coordinates = feature["geometry"]["coordinates"]
        observed_times = properties["observed_times"]
    except (KeyError, TypeError) as exc:
        raise ValueError("track feature is missing coordinates or observed_times") from exc
    if not isinstance(coordinates, list) or not isinstance(observed_times, list):
        raise ValueError("track coordinates and observed_times must be arrays")
    if len(coordinates) < 2 or len(observed_times) != len(coordinates):
        raise ValueError("track observed_times must align 1:1 with coordinates")
    try:
        parsed_times = []
        for value in observed_times:
            if not isinstance(value, str):
                raise ValueError("timestamp is not a string")
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
                raise ValueError("timestamp is not explicitly UTC")
            parsed_times.append(parsed.astimezone(timezone.utc))
    except (TypeError, ValueError) as exc:
        raise ValueError("track observed_times must contain ISO UTC timestamps") from exc
    if any(current <= previous for previous, current in zip(parsed_times, parsed_times[1:])):
        raise ValueError("track observed_times must be strictly increasing")
    if properties.get("start_at") != observed_times[0] or properties.get("end_at") != observed_times[-1]:
        raise ValueError("track start_at/end_at must match observed_times endpoints")


def _feature(vessel_id: str, rows: list[dict[str, Any]], index: int) -> dict[str, Any]:
    coordinates = [[row["longitude"], row["latitude"]] for row in rows]
    observed_times = [_parse_utc(str(row["observed_at"])).isoformat() for row in rows]
    start_time = observed_times[0]
    end_time = observed_times[-1]
    digest = hashlib.sha256(
        _canonical({
            "vessel_id": vessel_id,
            "start": start_time,
            "end": end_time,
            "coordinates": coordinates,
        }).encode()
    ).hexdigest()[:20]
    feature = {
        "type": "Feature",
        "id": digest,
        "properties": {
            "track_id": digest,
            "vessel_id": vessel_id,
            "segment_index": index,
            "source_dataset": GFW_DATASET,
            "mmsi": _mode(rows, "mmsi"),
            "ship_name": _mode(rows, "ship_name"),
            "vessel_type": _mode(rows, "vessel_type"),
            "flag": _mode(rows, "flag"),
            "start_at": start_time,
            "end_at": end_time,
            "observed_times": observed_times,
            "point_count": len(rows),
            "approximate": True,
            "coordinate_semantics": "GFW_HIGH_grid_cell_center",
            "temporal_resolution": "HOURLY",
        },
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates,
        },
    }
    validate_feature_time_contract(feature)
    return feature


def _singleton_feature(vessel_id: str, row: dict[str, Any], index: int) -> dict[str, Any]:
    """Represent a one-point canonical segment without inventing a line."""
    observed_at = _parse_utc(str(row["observed_at"])).isoformat()
    coordinate = [float(row["longitude"]), float(row["latitude"])]
    digest = hashlib.sha256(
        _canonical({
            "vessel_id": vessel_id,
            "observed_at": observed_at,
            "coordinate": coordinate,
            "node_type": "singleton",
        }).encode()
    ).hexdigest()[:20]
    return {
        "type": "Feature",
        "id": digest,
        "properties": {
            "track_id": digest,
            "vessel_id": vessel_id,
            "segment_index": index,
            "node_type": "singleton",
            "source_dataset": GFW_DATASET,
            "mmsi": _mode([row], "mmsi"),
            "ship_name": _mode([row], "ship_name"),
            "vessel_type": _mode([row], "vessel_type"),
            "flag": _mode([row], "flag"),
            "start_at": observed_at,
            "end_at": observed_at,
            "observed_times": [observed_at],
            "point_count": 1,
            "approximate": True,
            "coordinate_semantics": "GFW_HIGH_grid_cell_center",
            "temporal_resolution": "HOURLY",
        },
        "geometry": {"type": "Point", "coordinates": coordinate},
    }


def build_track_segments(
    rows: Iterable[dict[str, Any]],
    *,
    gap_hours: float = 2.0,
    max_speed_knots: float = 80.0,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Build deterministic per-vessel segments from normalized hourly points."""
    by_vessel: dict[str, list[dict[str, Any]]] = defaultdict(list)
    exact_seen: set[tuple[Any, ...]] = set()
    invalid_rows = 0
    duplicate_rows = 0
    for row in rows:
        try:
            vessel_id = str(row["vessel_id"])
            observed_at = _parse_utc(str(row["observed_at"])).isoformat()
            longitude = float(row["longitude"])
            latitude = float(row["latitude"])
        except (KeyError, TypeError, ValueError, OverflowError):
            invalid_rows += 1
            continue
        if not vessel_id or not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
            invalid_rows += 1
            continue
        key = (vessel_id, observed_at, longitude, latitude)
        if key in exact_seen:
            duplicate_rows += 1
            continue
        exact_seen.add(key)
        normalized = dict(row)
        normalized.update({
            "vessel_id": vessel_id,
            "observed_at": observed_at,
            "longitude": longitude,
            "latitude": latitude,
        })
        by_vessel[vessel_id].append(normalized)

    features: list[dict[str, Any]] = []
    gap_splits = speed_splits = same_hour_conflicts = singleton_nodes = 0
    for vessel_id in sorted(by_vessel):
        ordered = sorted(
            by_vessel[vessel_id],
            key=lambda row: (_parse_utc(row["observed_at"]), row["longitude"], row["latitude"]),
        )
        unique_hours: list[dict[str, Any]] = []
        used_hours: set[datetime] = set()
        for row in ordered:
            hour = _hour_key(row["observed_at"])
            if hour in used_hours:
                same_hour_conflicts += 1
                continue
            used_hours.add(hour)
            unique_hours.append(row)

        segment: list[dict[str, Any]] = []
        segment_index = 0

        def flush() -> None:
            nonlocal segment, segment_index, singleton_nodes
            if len(segment) >= 2:
                features.append(_feature(vessel_id, segment, segment_index))
                segment_index += 1
            elif segment:
                features.append(_singleton_feature(vessel_id, segment[0], segment_index))
                segment_index += 1
                singleton_nodes += 1
            segment = []

        for row in unique_hours:
            if not segment:
                segment = [row]
                continue
            elapsed_hours = (
                _parse_utc(row["observed_at"]) - _parse_utc(segment[-1]["observed_at"])
            ).total_seconds() / 3600
            speed = _haversine_nm(segment[-1], row) / elapsed_hours if elapsed_hours > 0 else math.inf
            if elapsed_hours > gap_hours:
                gap_splits += 1
                flush()
            elif speed > max_speed_knots:
                speed_splits += 1
                flush()
            segment.append(row)
        flush()

    features.sort(key=lambda feature: (
        feature["properties"]["vessel_id"],
        feature["properties"]["start_at"],
        feature["properties"]["track_id"],
    ))
    return features, {
        "valid_unique_points": len(exact_seen),
        "invalid_rows": invalid_rows,
        "duplicate_rows": duplicate_rows,
        "same_hour_conflicts": same_hour_conflicts,
        "gap_splits": gap_splits,
        "speed_splits": speed_splits,
        # Kept as a zero-valued compatibility field: canonical singletons are
        # now published as Point nodes instead of being silently discarded.
        "dropped_singletons": 0,
        "singleton_nodes": singleton_nodes,
    }


def cap_features(
    features: Iterable[dict[str, Any]], *, max_features: int, max_points: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = list(features)
    candidate_points = sum(int(feature["properties"]["point_count"]) for feature in candidates)
    ranked = sorted(
        candidates,
        key=lambda feature: (
            -int(feature["properties"]["point_count"]),
            str(feature["properties"]["track_id"]),
        ),
    )
    kept: list[dict[str, Any]] = []
    displayed_points = 0
    for feature in ranked:
        point_count = int(feature["properties"]["point_count"])
        if len(kept) >= max_features:
            break
        if displayed_points + point_count > max_points:
            continue
        kept.append(feature)
        displayed_points += point_count
    kept.sort(key=lambda feature: (
        str(feature["properties"].get("vessel_id", "")),
        str(feature["properties"].get("start_at", "")),
        str(feature["properties"]["track_id"]),
    ))
    return kept, {
        "candidate_features": len(candidates),
        "displayed_features": len(kept),
        "candidate_points": candidate_points,
        "displayed_points": displayed_points,
        "cap_applied": len(kept) < len(candidates),
    }


def _atomic_json(path: Path, value: Any, *, indent: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=None if indent else (",", ":"), indent=indent),
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_points(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    temporary = path.with_name(f".{path.name}.tmp")
    count = 0
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            projected = {
                key: row.get(key) for key in (
                    "vessel_id", "observed_at", "longitude", "latitude",
                    "mmsi", "ship_name", "vessel_type", "flag",
                )
            }
            handle.write(_canonical(projected) + "\n")
            count += 1
    temporary.replace(path)
    return count


def _read_points(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


class SQLiteTrackStore:
    """Disposable canonical track/node store with deterministic streaming readers."""

    def __init__(self, path: Path, connection: sqlite3.Connection, *, tile_rows: int, stats: dict[str, int]):
        self.path = path
        self.connection = connection
        self.tile_rows = tile_rows
        self.stats = stats

    def iter_tracks(self) -> Iterable[dict[str, Any]]:
        yield from self._iter_nodes("track")

    def iter_singleton_nodes(self) -> Iterable[dict[str, Any]]:
        yield from self._iter_nodes("singleton")

    def iter_features(self) -> Iterable[dict[str, Any]]:
        cursor = self.connection.execute("""
            SELECT feature_json FROM nodes
            ORDER BY vessel_id, start_at, track_id
        """)
        for (feature_json,) in cursor:
            yield json.loads(feature_json)

    def _iter_nodes(self, node_kind: str) -> Iterable[dict[str, Any]]:
        cursor = self.connection.execute("""
            SELECT feature_json FROM nodes
            WHERE node_kind = ?
            ORDER BY vessel_id, start_at, track_id
        """, (node_kind,))
        for (feature_json,) in cursor:
            yield json.loads(feature_json)

    def counts(self) -> dict[str, Any]:
        tracks, track_points, singletons, singleton_points, vessels = self.connection.execute("""
            SELECT
                SUM(CASE WHEN node_kind = 'track' THEN 1 ELSE 0 END),
                COALESCE(SUM(CASE WHEN node_kind = 'track' THEN point_count ELSE 0 END), 0),
                SUM(CASE WHEN node_kind = 'singleton' THEN 1 ELSE 0 END),
                COALESCE(SUM(CASE WHEN node_kind = 'singleton' THEN point_count ELSE 0 END), 0),
                COUNT(DISTINCT vessel_id)
            FROM nodes
        """).fetchone()
        canonical_points = int(self.stats["canonical_vessel_hour_count"])
        track_points = int(track_points)
        singleton_points = int(singleton_points)
        if canonical_points != track_points + singleton_points:
            raise RuntimeError("canonical point accounting mismatch")
        return {
            "eligible_segment_count": int(tracks or 0),
            "published_segment_count": int(tracks or 0),
            "eligible_segment_points": track_points,
            "published_segment_points": track_points,
            "singleton_node_count": int(singletons or 0),
            "singleton_node_points": singleton_points,
            "canonical_vessel_hour_count": canonical_points,
            "canonical_points": canonical_points,
            "candidate_vessels": int(vessels),
            "published_vessels": int(vessels),
            # Legacy aliases remain accurate but no longer imply selection.
            "candidate_features": int(tracks or 0),
            "displayed_features": int(tracks or 0),
            "candidate_points": track_points,
            "displayed_points": track_points,
            "cap_applied": False,
            "omitted_by_display_cap": 0,
        }

    def close(self) -> None:
        self.connection.close()


def _normalize_store_row(row: dict[str, Any]) -> tuple[Any, ...] | None:
    try:
        vessel_id = str(row["vessel_id"]).strip()
        observed_at = _parse_utc(str(row["observed_at"])).isoformat()
        longitude = float(row["longitude"])
        latitude = float(row["latitude"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    if not vessel_id or not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
        return None
    return (
        vessel_id, observed_at, longitude, latitude,
        row.get("mmsi"), row.get("ship_name"), row.get("vessel_type"), row.get("flag"),
    )


def finalize_track_store(
    shard_paths: Iterable[Path],
    *,
    work_dir: Path,
    gap_hours: float,
    max_speed_knots: float,
) -> SQLiteTrackStore:
    """Build a SQLite-backed full-fidelity node store without selecting a subset."""
    database_path = work_dir / "finalize.sqlite3"
    if database_path.exists():
        database_path.unlink()
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("""
        CREATE TABLE points (
            vessel_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            longitude REAL NOT NULL,
            latitude REAL NOT NULL,
            mmsi TEXT,
            ship_name TEXT,
            vessel_type TEXT,
            flag TEXT,
            PRIMARY KEY (vessel_id, observed_at, longitude, latitude)
        ) WITHOUT ROWID
    """)
    insert_sql = """
        INSERT OR IGNORE INTO points
        (vessel_id, observed_at, longitude, latitude, mmsi, ship_name, vessel_type, flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    tile_row_count = invalid_rows = 0
    batch: list[tuple[Any, ...]] = []
    for row in _read_points(shard_paths):
        tile_row_count += 1
        normalized = _normalize_store_row(row)
        if normalized is None:
            invalid_rows += 1
            continue
        batch.append(normalized)
        if len(batch) >= 5000:
            connection.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        connection.executemany(insert_sql, batch)
    connection.commit()
    unique_point_count = int(connection.execute("SELECT COUNT(*) FROM points").fetchone()[0])

    connection.execute("""
        CREATE TABLE nodes (
            track_id TEXT PRIMARY KEY,
            vessel_id TEXT NOT NULL,
            start_at TEXT NOT NULL,
            node_kind TEXT NOT NULL CHECK (node_kind IN ('track', 'singleton')),
            point_count INTEGER NOT NULL,
            feature_json TEXT NOT NULL
        ) WITHOUT ROWID
    """)
    segment_stats = {
        "valid_unique_points": unique_point_count,
        "invalid_rows": invalid_rows,
        "duplicate_rows": tile_row_count - invalid_rows - unique_point_count,
        "same_hour_conflicts": 0,
        "gap_splits": 0,
        "speed_splits": 0,
        "dropped_singletons": 0,
        "singleton_nodes": 0,
    }
    current_vessel: str | None = None
    last_hour: datetime | None = None
    segment: list[dict[str, Any]] = []
    segment_index = 0
    segment_batch: list[tuple[str, str, str, str, int, str]] = []

    def insert_feature(feature: dict[str, Any]) -> None:
        properties = feature["properties"]
        segment_batch.append((
            properties["track_id"], properties["vessel_id"], properties["start_at"],
            "singleton" if feature["geometry"]["type"] == "Point" else "track",
            int(properties["point_count"]), _canonical(feature),
        ))
        if len(segment_batch) >= 1000:
            connection.executemany(
                "INSERT INTO nodes (track_id, vessel_id, start_at, node_kind, point_count, feature_json) VALUES (?, ?, ?, ?, ?, ?)",
                segment_batch,
            )
            segment_batch.clear()

    def flush_segment() -> None:
        nonlocal segment, segment_index
        if len(segment) >= 2:
            insert_feature(_feature(current_vessel or "", segment, segment_index))
        elif segment:
            insert_feature(_singleton_feature(current_vessel or "", segment[0], segment_index))
            segment_stats["singleton_nodes"] += 1
        if segment:
            segment_index += 1
        segment = []

    cursor = connection.execute("""
        SELECT vessel_id, observed_at, longitude, latitude, mmsi, ship_name, vessel_type, flag
        FROM points
        ORDER BY vessel_id, observed_at, longitude, latitude
    """)
    for vessel_id, observed_at, longitude, latitude, mmsi, ship_name, vessel_type, flag in cursor:
        row = {
            "vessel_id": vessel_id, "observed_at": observed_at,
            "longitude": longitude, "latitude": latitude,
            "mmsi": mmsi, "ship_name": ship_name,
            "vessel_type": vessel_type, "flag": flag,
        }
        if current_vessel != vessel_id:
            flush_segment()
            current_vessel = vessel_id
            last_hour = None
            segment_index = 0
        hour = _hour_key(observed_at)
        if last_hour == hour:
            segment_stats["same_hour_conflicts"] += 1
            continue
        last_hour = hour
        if not segment:
            segment = [row]
            continue
        elapsed_hours = (
            _parse_utc(observed_at) - _parse_utc(segment[-1]["observed_at"])
        ).total_seconds() / 3600
        speed = _haversine_nm(segment[-1], row) / elapsed_hours if elapsed_hours > 0 else math.inf
        if elapsed_hours > gap_hours:
            segment_stats["gap_splits"] += 1
            flush_segment()
        elif speed > max_speed_knots:
            segment_stats["speed_splits"] += 1
            flush_segment()
        segment.append(row)
    flush_segment()
    if segment_batch:
        connection.executemany(
            "INSERT INTO nodes (track_id, vessel_id, start_at, node_kind, point_count, feature_json) VALUES (?, ?, ?, ?, ?, ?)",
            segment_batch,
        )
    connection.commit()

    canonical_count = int(connection.execute("""
        SELECT COUNT(*) FROM (
            SELECT vessel_id, substr(observed_at, 1, 13) AS hour FROM points
            GROUP BY vessel_id, hour
        )
    """).fetchone()[0])
    if canonical_count != unique_point_count - segment_stats["same_hour_conflicts"]:
        raise RuntimeError("same-hour canonicalization accounting mismatch")
    segment_stats["canonical_vessel_hour_count"] = canonical_count
    return SQLiteTrackStore(
        database_path, connection, tile_rows=tile_row_count, stats=segment_stats,
    )


def _finalize_disk_backed(
    shard_paths: Iterable[Path], *, work_dir: Path, gap_hours: float,
    max_speed_knots: float, max_features: int, max_points: int,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, Any], int, int, int]:
    """Compatibility helper for legacy callers; production uses ``finalize_track_store``."""
    del max_features, max_points
    store = finalize_track_store(
        shard_paths, work_dir=work_dir, gap_hours=gap_hours,
        max_speed_knots=max_speed_knots,
    )
    try:
        counts = store.counts()
        return (
            list(store.iter_features()), store.stats, counts,
            counts["candidate_vessels"], counts["published_vessels"], store.tile_rows,
        )
    finally:
        store.close()


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    try:
        parts = tuple(float(item.strip()) for item in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("bbox values must be numbers") from exc
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox must be west,south,east,north")
    return parts  # make_tiles performs WGS84 validation


def _load_or_create_manifest(work_dir: Path, signature: dict[str, Any], tiles: list[Tile]) -> dict[str, Any]:
    manifest_path = work_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        # JSON round-trips tuples as lists; compare canonical serialized forms.
        if _canonical(manifest.get("signature")) != _canonical(signature):
            raise RuntimeError(
                f"resume work directory signature mismatch: {work_dir}; use another --work-dir"
            )
        return manifest
    work_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "signature": signature,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "completed_tiles": {},
        "tiles": [{"tile_id": tile.tile_id, "bbox": tile.bbox} for tile in tiles],
    }
    _atomic_json(manifest_path, manifest, indent=2)
    return manifest


def write_feature_collection_stream(
    path: Path, *, metadata: dict[str, Any], features: Iterable[dict[str, Any]],
) -> int:
    """Atomically serialize a FeatureCollection while consuming one feature at a time."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write('{"type":"FeatureCollection","metadata":')
        handle.write(_canonical(metadata))
        handle.write(',"features":[')
        first = True
        for feature in features:
            if not first:
                handle.write(",")
            handle.write(_canonical(feature))
            first = False
        handle.write("]}")
    temporary.replace(path)
    return path.stat().st_size


def write_features_ndjson(path: Path, features: Iterable[dict[str, Any]]) -> int:
    """Atomically write canonical feature NDJSON without retaining a feature list."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    count = 0
    with temporary.open("w", encoding="utf-8") as handle:
        for feature in features:
            handle.write(_canonical(feature) + "\n")
            count += 1
    temporary.replace(path)
    return count


class _StreamingFeatureCollection(dict[str, Any]):
    """Mapping-shaped release input that provides a fresh deterministic feature iterator."""

    def __init__(self, metadata: dict[str, Any], store: SQLiteTrackStore):
        super().__init__(type="FeatureCollection", metadata=metadata)
        self._store = store

    def get(self, key: str, default: Any = None) -> Any:
        if key == "features":
            return self._store.iter_features()
        return super().get(key, default)


def run_poc(
    *,
    output: Path | None = None,
    output_dir: Path | None = None,
    token: str,
    bbox: tuple[float, float, float, float] = DEFAULT_BBOX,
    latest_complete_day: str = DEFAULT_LATEST_COMPLETE_DAY,
    days: int = 7,
    tile_size_degrees: float = 3.0,
    max_features: int = 5000,
    max_points: int = 150_000,
    gap_hours: float = 2.0,
    max_speed_knots: float = 80.0,
    work_dir: Path | None = None,
    keep_work_dir: bool = False,
    client: GFWReportClient | None = None,
) -> dict[str, Any]:
    if output is None and output_dir is None:
        raise ValueError("at least one of output or output_dir is required")
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
    }
    signature_hash = hashlib.sha256(_canonical(signature).encode()).hexdigest()[:16]
    work_dir = work_dir or Path(tempfile.gettempdir()) / f"gfw-hourly-tracks-poc-{signature_hash}"
    manifest = _load_or_create_manifest(work_dir, signature, tiles)
    report_client = client or GFWReportClient(token)
    received_at = datetime.now(timezone.utc).isoformat()
    resumed_tiles = 0
    resolved_versions: set[str] = set()
    errors: list[str] = []

    for position, tile in enumerate(tiles, start=1):
        shard = work_dir / f"{tile.tile_id}.points.ndjson"
        completed = manifest["completed_tiles"].get(tile.tile_id)
        if completed and shard.exists():
            resumed_tiles += 1
            if completed.get("resolved_dataset_version"):
                resolved_versions.add(completed["resolved_dataset_version"])
            print(f"[{position}/{len(tiles)}] resume {tile.tile_id}: {completed['row_count']} rows", flush=True)
            continue
        print(f"[{position}/{len(tiles)}] fetch {tile.tile_id} {tile.bbox}", flush=True)
        try:
            request_before = _request_counter_snapshot(report_client.stats)
            payload, resolved = report_client.fetch(tile.bbox, start_text, end_text)
            rows = GFWVesselPresenceCollector.normalize_entries(
                payload,
                snapshot_date=latest_complete_day,
                received_at=received_at,
                zone=tile.tile_id,
                dataset=resolved or GFW_DATASET,
            )
            accepted = [
                row for row in rows
                if row.get("presence_quality") == "accepted"
                and row.get("longitude") is not None
                and row.get("latitude") is not None
            ]
            row_count = _write_points(shard, accepted)
            manifest["completed_tiles"][tile.tile_id] = {
                "row_count": row_count,
                "resolved_dataset_version": resolved,
                "request_counts": _request_counter_delta(request_before, report_client.stats),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_json(work_dir / "manifest.json", manifest, indent=2)
            if resolved:
                resolved_versions.add(resolved)
            print(f"[{position}/{len(tiles)}] done {tile.tile_id}: {row_count} rows", flush=True)
        except Exception as exc:
            errors.append(f"{tile.tile_id}: {exc}")
            break

    if errors or len(manifest["completed_tiles"]) != len(tiles):
        raise RuntimeError(
            "POC incomplete; resumable normalized shards retained at "
            f"{work_dir}: {'; '.join(errors) or 'missing tiles'}"
        )

    shard_paths = [work_dir / f"{tile.tile_id}.points.ndjson" for tile in tiles]
    store = finalize_track_store(
        shard_paths,
        work_dir=work_dir,
        gap_hours=gap_hours,
        max_speed_knots=max_speed_knots,
    )
    segment_stats = store.stats
    cap_stats = store.counts()
    candidate_vessel_count = cap_stats["candidate_vessels"]
    displayed_vessel_count = cap_stats["published_vessels"]
    tile_row_count = store.tile_rows
    generated_at = datetime.now(timezone.utc).isoformat()
    metadata = {
        "schema_version": 3,
        "poc": True,
        "generated_at": generated_at,
        "source": "Global Fishing Watch 4Wings AIS Vessel Presence",
        "source_dataset": GFW_DATASET,
        "resolved_dataset_versions": sorted(resolved_versions),
        "bbox": bbox,
        "date_start": start_text,
        "date_end_inclusive": latest_complete_day,
        "date_end_exclusive": end_text,
        "days": days,
        "temporal_resolution": "HOURLY",
        "spatial_resolution": "HIGH (0.01 degree grid)",
        "coordinate_semantics": "grid_cell_center_not_raw_AIS_position",
        "approximate": True,
        # Stable frontend aliases. Keep the detailed accounting under counts.
        "row_count": tile_row_count,
        "vessel_count": candidate_vessel_count,
        "segment_count": cap_stats["eligible_segment_count"],
        "displayed_segment_count": cap_stats["published_segment_count"],
        "track_rules": {
            "gap_hours_gt": gap_hours,
            "max_implied_speed_knots": max_speed_knots,
            "minimum_points": 2,
            "same_vessel_hour": "keep earliest observation deterministically",
        },
        "track_contract": {
            "vertex_time_property": "observed_times",
            "vertex_time_alignment": "one_to_one_with_geometry_coordinates",
            "vertex_time_order": "strictly_increasing_utc",
        },
        "tiling": {
            "tile_size_degrees": tile_size_degrees,
            "tile_count": len(tiles),
            "sequential": True,
            "resumed_tile_count": resumed_tiles,
        },
        "counts": {
            "raw_normalized_tile_rows": tile_row_count,
            "candidate_vessels": candidate_vessel_count,
            "displayed_vessels": displayed_vessel_count,
            **segment_stats,
            **cap_stats,
        },
        "display_cap": {
            "max_features": None,
            "max_points": None,
            "selection": "none; full-fidelity streaming output",
            "cap_applied": False,
            "omitted_by_display_cap": 0,
        },
        "requests": {
            "successful_tile_reports": len(manifest["completed_tiles"]),
            "completed_tiles_without_request_telemetry": sum(
                1 for tile in manifest["completed_tiles"].values()
                if "request_counts" not in tile
            ),
            "recorded_completed_tile_requests": {
                key: sum(
                    int(tile.get("request_counts", {}).get(key, 0))
                    for tile in manifest["completed_tiles"].values()
                )
                for key in ("post_requests", "recovery_requests", "retries")
            },
            "current_process": report_client.stats,
        },
        "errors": errors,
        "storage": "No DB/S3/raw archive; normalized resume shards deleted after success unless --keep-work-dir",
    }
    size_bytes = None
    partition_release = None
    try:
        if output is not None:
            size_bytes = write_feature_collection_stream(
                output, metadata=metadata, features=store.iter_features(),
            )
        if output_dir is not None:
            partition_release = publish_track_release(
                _StreamingFeatureCollection(metadata, store),
                root=output_dir,
                latest_complete_date=latest_complete_day,
                date_start=start_text,
                date_end=latest_complete_day,
                generated_at=generated_at,
            )
        summary = {
            "output": str(output.resolve()) if output is not None else None,
            "output_dir": str(output_dir.resolve()) if output_dir is not None else None,
            "partition_release": partition_release,
            "file_size_bytes": size_bytes,
            "rows": tile_row_count,
            "unique_points": segment_stats["valid_unique_points"],
            "candidate_segments": cap_stats["eligible_segment_count"],
            "displayed_segments": cap_stats["published_segment_count"],
            "candidate_vessels": candidate_vessel_count,
            "displayed_vessels": displayed_vessel_count,
            "requests": metadata["requests"],
            "errors": errors,
            "work_dir": str(work_dir),
            "work_dir_kept": keep_work_dir,
        }
    finally:
        store.close()
    if not keep_work_dir:
        shutil.rmtree(work_dir)
        summary["work_dir"] = None
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="legacy monolithic GeoJSON output path")
    parser.add_argument(
        "--output-dir", type=Path,
        help="partitioned release root (manifest + immutable per-UTC-day GeoJSON)",
    )
    parser.add_argument("--bbox", type=_parse_bbox, default=DEFAULT_BBOX, help="west,south,east,north")
    parser.add_argument("--latest-complete-day", default=DEFAULT_LATEST_COMPLETE_DAY)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--tile-size-degrees", type=float, default=3.0)
    parser.add_argument("--max-features", type=int, default=5000)
    parser.add_argument("--max-points", type=int, default=150_000)
    parser.add_argument("--gap-hours", type=float, default=2.0)
    parser.add_argument("--max-speed-knots", type=float, default=80.0)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--keep-work-dir", action="store_true")
    parser.add_argument("--request-timeout", type=float, default=120.0)
    args = parser.parse_args()

    if args.output is None and args.output_dir is None:
        parser.error("at least one of --output or --output-dir is required")
    if not config.GFW_ACCESS_TOKEN:
        parser.error("GFW_ACCESS_TOKEN is missing; token is read backend-only from data-collectors/.env")
    client = GFWReportClient(config.GFW_ACCESS_TOKEN, timeout=args.request_timeout)
    summary = run_poc(
        output=args.output,
        output_dir=args.output_dir,
        token=config.GFW_ACCESS_TOKEN,
        bbox=args.bbox,
        latest_complete_day=args.latest_complete_day,
        days=args.days,
        tile_size_degrees=args.tile_size_degrees,
        max_features=args.max_features,
        max_points=args.max_points,
        gap_hours=args.gap_hours,
        max_speed_knots=args.max_speed_knots,
        work_dir=args.work_dir,
        keep_work_dir=args.keep_work_dir,
        client=client,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
