#!/usr/bin/env python3
"""Pure local release lifecycle for partitioned GFW hourly track GeoJSON.

The module deliberately has no API, credential, database, S3, or Zeabur
dependency.  It accepts an already-normalized track FeatureCollection, stages
and validates UTC-day partitions, then performs a manifest-last cutover.
"""

from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import urlparse
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
SUPPORTED_TRAIL_HOURS = (0.5, 1.0, 2.0, 3.0)
DEFAULT_LOOKBACK_HOURS = 3.0
DEFAULT_LOOKAHEAD_HOURS = 1.0
DEFAULT_RELEASES_TO_KEEP = 2
ROOT_CACHE_CONTROL = "public,max-age=60,s-maxage=60,stale-while-revalidate=300"
RELEASE_CACHE_CONTROL = "public,max-age=604800,s-maxage=604800,immutable"
_RELEASE_ID = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_KEY_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_ASSET_TYPE = re.compile(r"^[a-z][a-z0-9_]*$")


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"timestamp must be explicitly UTC: {value!r}")
    return parsed.astimezone(timezone.utc)


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(_canonical(value), encoding="utf-8")
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _validate_s3_config(
    *, bucket: str, key_prefix: str, public_url_prefix: str
) -> tuple[str, str, str]:
    if not _BUCKET.fullmatch(bucket):
        raise ValueError("bucket must be a valid lowercase S3 bucket name")
    if (
        not key_prefix
        or key_prefix.startswith("/")
        or key_prefix.endswith("/")
        or "//" in key_prefix
        or any(not _KEY_PART.fullmatch(part) for part in key_prefix.split("/"))
    ):
        raise ValueError("key_prefix must be a strict relative S3 key prefix")
    parsed = urlparse(public_url_prefix)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
        or public_url_prefix.endswith("/")
    ):
        raise ValueError("public_url_prefix must be an HTTPS URL without query, fragment, or trailing slash")
    return bucket, key_prefix, public_url_prefix


def _validated_asset_relative_path(value: str) -> Path:
    relative = Path(value)
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or len(relative.parts) < 2
        or any(not _KEY_PART.fullmatch(part) for part in relative.parts)
    ):
        raise ValueError(f"unsafe asset path: {value!r}")
    return relative


def _frontend_index_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    def append_entries(values: list[dict[str, Any]] | None) -> None:
        for entry in values or []:
            entries.append(entry)
            entries.extend(entry.get("detail_buckets") or [])

    for index_name in ("days", "hours"):
        append_entries(manifest.get(index_name) or [])
    tracks = manifest.get("tracks") or {}
    grid = manifest.get("grid") or {}
    dark_vessels = manifest.get("dark_vessels") or {}
    if any(not isinstance(section, dict) for section in (tracks, grid, dark_vessels)):
        raise ValueError("tracks, grid, and dark_vessels manifest sections must be objects")
    append_entries(tracks.get("days") or [])
    append_entries(tracks.get("singleton_days") or [])
    append_entries(tracks.get("frames") or [])
    append_entries(grid.get("hours") or [])
    append_entries(dark_vessels.get("hours") or [])
    if any(not isinstance(entry, dict) for entry in entries):
        raise ValueError("frontend manifest indexes must contain objects")
    return entries


def manifest_assets(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the canonical generic asset list, deriving legacy track days if needed."""
    assets = manifest.get("assets")
    if assets is None:
        assets = [
            {
                "path": entry["path"],
                "sha256": entry["sha256"],
                "bytes": entry["bytes"],
                "type": "tracks_day",
                "features": entry.get("features", 0),
            }
            for entry in manifest.get("days") or []
        ]
    if not isinstance(assets, list) or not assets:
        raise ValueError("release manifest must contain at least one asset")
    normalized = []
    seen_paths: set[str] = set()
    for asset in assets:
        if not isinstance(asset, dict):
            raise ValueError("release asset entries must be objects")
        relative = _validated_asset_relative_path(str(asset.get("path", "")))
        asset_type = str(asset.get("type", ""))
        sha256 = str(asset.get("sha256", ""))
        byte_size = asset.get("bytes")
        if not _ASSET_TYPE.fullmatch(asset_type):
            raise ValueError(f"invalid asset type: {asset_type!r}")
        if not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ValueError(f"invalid asset sha256: {relative}")
        if not isinstance(byte_size, int) or byte_size < 0:
            raise ValueError(f"invalid asset bytes: {relative}")
        if relative.as_posix() in seen_paths:
            raise ValueError(f"duplicate asset path: {relative}")
        seen_paths.add(relative.as_posix())
        normalized.append({**asset, "path": relative.as_posix(), "type": asset_type})
    indexed_paths = {str(entry.get("path")) for entry in _frontend_index_entries(manifest)}
    if not indexed_paths.issubset(seen_paths):
        raise ValueError("days/hours index contains a path missing from assets")
    return normalized


def _day_bounds(display_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(display_date, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _interpolate(
    before_time: datetime,
    before_coord: list[float],
    after_time: datetime,
    after_coord: list[float],
    target: datetime,
) -> list[float]:
    elapsed = (after_time - before_time).total_seconds()
    if elapsed <= 0:
        raise ValueError("track timestamps must be strictly increasing")
    ratio = (target - before_time).total_seconds() / elapsed
    return [
        float(before_coord[0]) + (float(after_coord[0]) - float(before_coord[0])) * ratio,
        float(before_coord[1]) + (float(after_coord[1]) - float(before_coord[1])) * ratio,
    ]


def _vertex_at(
    times: list[datetime], coordinates: list[list[float]], target: datetime
) -> tuple[list[float], bool] | None:
    for index, current in enumerate(times):
        if current == target:
            return list(map(float, coordinates[index])), False
        if current > target:
            if index == 0:
                return None
            return _interpolate(
                times[index - 1], coordinates[index - 1], current, coordinates[index], target
            ), True
    return None


def clip_track_feature(
    feature: dict[str, Any], *, window_start: datetime, window_end: datetime
) -> dict[str, Any] | None:
    """Clip one track to an inclusive UTC window with linear boundary vertices."""
    if feature.get("geometry", {}).get("type") == "Point":
        try:
            properties = feature["properties"]
            observed_times = properties["observed_times"]
            coordinate = feature["geometry"]["coordinates"]
        except (KeyError, TypeError) as exc:
            raise ValueError("singleton feature is missing coordinates or observed_times") from exc
        if not isinstance(coordinate, list) or len(coordinate) != 2 or len(observed_times) != 1:
            raise ValueError("singleton feature must contain one coordinate and one observed time")
        observed = _parse_utc(observed_times[0])
        if observed < window_start or observed > window_end:
            return None
        if properties.get("start_at") != observed_times[0] or properties.get("end_at") != observed_times[0]:
            raise ValueError("singleton start_at/end_at must match observed time")
        clipped = deepcopy(feature)
        clipped["properties"]["partition_clipped"] = False
        clipped["properties"]["partition_boundary_interpolated"] = False
        return clipped
    if feature.get("geometry", {}).get("type") != "LineString":
        raise ValueError("track feature geometry must be LineString or Point")
    try:
        properties = feature["properties"]
        coordinates = feature["geometry"]["coordinates"]
        observed_times = properties["observed_times"]
    except (KeyError, TypeError) as exc:
        raise ValueError("track feature is missing coordinates or observed_times") from exc
    if len(coordinates) != len(observed_times) or len(coordinates) < 2:
        raise ValueError("track coordinates and observed_times must align 1:1")
    times = [_parse_utc(value) for value in observed_times]
    if any(current <= previous for previous, current in zip(times, times[1:])):
        raise ValueError("track observed_times must be strictly increasing")
    if times[-1] < window_start or times[0] > window_end:
        return None

    clipped_start = max(window_start, times[0])
    clipped_end = min(window_end, times[-1])
    if clipped_end <= clipped_start:
        return None

    vertices: list[tuple[datetime, list[float], bool]] = []
    start_vertex = _vertex_at(times, coordinates, clipped_start)
    if start_vertex is None:
        raise ValueError("cannot resolve clipped track start")
    vertices.append((clipped_start, start_vertex[0], start_vertex[1]))
    for current, coordinate in zip(times, coordinates):
        if clipped_start < current < clipped_end:
            vertices.append((current, list(map(float, coordinate)), False))
    end_vertex = _vertex_at(times, coordinates, clipped_end)
    if end_vertex is None:
        raise ValueError("cannot resolve clipped track end")
    vertices.append((clipped_end, end_vertex[0], end_vertex[1]))

    # Exact end/start observations can enter twice only in degenerate inputs.
    deduped: list[tuple[datetime, list[float], bool]] = []
    for vertex in vertices:
        if deduped and deduped[-1][0] == vertex[0]:
            deduped[-1] = vertex
        else:
            deduped.append(vertex)
    if len(deduped) < 2:
        return None

    clipped = deepcopy(feature)
    clipped_times = [current.isoformat() for current, _, _ in deduped]
    clipped["geometry"]["coordinates"] = [coordinate for _, coordinate, _ in deduped]
    clipped["properties"]["observed_times"] = clipped_times
    clipped["properties"]["start_at"] = clipped_times[0]
    clipped["properties"]["end_at"] = clipped_times[-1]
    clipped["properties"]["point_count"] = len(deduped)
    clipped["properties"]["partition_clipped"] = (
        clipped_start > times[0] or clipped_end < times[-1]
    )
    clipped["properties"]["partition_boundary_interpolated"] = any(
        interpolated for _, _, interpolated in deduped
    )
    return clipped


def build_daily_track_partition(
    collection: dict[str, Any],
    *,
    display_date: date,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    lookahead_hours: float = DEFAULT_LOOKAHEAD_HOURS,
) -> dict[str, Any]:
    if not 0 <= lookback_hours <= DEFAULT_LOOKBACK_HOURS:
        raise ValueError("lookback_hours must be between 0 and 3")
    if not 0 <= lookahead_hours <= DEFAULT_LOOKAHEAD_HOURS:
        raise ValueError("lookahead_hours must be between 0 and 1")
    day_start, day_end = _day_bounds(display_date)
    window_start = day_start - timedelta(hours=lookback_hours)
    window_end = day_end + timedelta(hours=lookahead_hours)
    features = []
    for feature in collection.get("features", []):
        clipped = clip_track_feature(
            feature, window_start=window_start, window_end=window_end
        )
        if clipped is not None:
            features.append(clipped)
    features.sort(key=lambda feature: (
        str(feature.get("properties", {}).get("vessel_id", "")),
        str(feature.get("properties", {}).get("start_at", "")),
        str(feature.get("properties", {}).get("track_id", "")),
    ))
    return {
        "type": "FeatureCollection",
        "metadata": {
            "schema_version": SCHEMA_VERSION,
            "display_date": display_date.isoformat(),
            "display_timezone": "UTC",
            "overlap": {
                "lookback_hours": lookback_hours,
                "lookahead_hours": lookahead_hours,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
            },
            "supported_trail_hours": list(SUPPORTED_TRAIL_HOURS),
            "interpolation": "linear_between_adjacent_hourly_grid_centers",
            "feature_count": len(features),
            "point_count": sum(
                int(feature.get("properties", {}).get("point_count", 0))
                for feature in features
            ),
            "line_feature_count": sum(
                feature.get("geometry", {}).get("type") == "LineString"
                for feature in features
            ),
            "singleton_feature_count": sum(
                feature.get("geometry", {}).get("type") == "Point"
                for feature in features
            ),
            "coordinate_semantics": "GFW_HIGH_grid_cell_center_not_raw_AIS_position",
        },
        "features": features,
    }


def _validate_partition(path: Path, expected_date: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("type") != "FeatureCollection":
        raise ValueError(f"partition is not a FeatureCollection: {path}")
    metadata = value.get("metadata") or {}
    if metadata.get("display_date") != expected_date:
        raise ValueError(f"partition display_date mismatch: {path}")
    for feature in value.get("features") or []:
        geometry_type = feature.get("geometry", {}).get("type")
        coordinates = feature.get("geometry", {}).get("coordinates") or []
        observed_times = feature.get("properties", {}).get("observed_times") or []
        if geometry_type == "Point":
            if not isinstance(coordinates, list) or len(coordinates) != 2 or len(observed_times) != 1:
                raise ValueError(f"partition has invalid singleton node contract: {path}")
            if feature.get("properties", {}).get("start_at") != observed_times[0] or feature.get("properties", {}).get("end_at") != observed_times[0]:
                raise ValueError(f"partition singleton endpoints do not match observed time: {path}")
            _parse_utc(observed_times[0])
            continue
        if geometry_type != "LineString" or len(coordinates) < 2 or len(coordinates) != len(observed_times):
            raise ValueError(f"partition has invalid track vertex contract: {path}")
        parsed = [_parse_utc(value) for value in observed_times]
        if any(current <= previous for previous, current in zip(parsed, parsed[1:])):
            raise ValueError(f"partition has non-increasing track times: {path}")
    return value


def stage_track_release(
    collection: dict[str, Any],
    *,
    root: Path,
    latest_complete_date: str,
    date_start: str,
    date_end: str,
    generated_at: str | None = None,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    lookahead_hours: float = DEFAULT_LOOKAHEAD_HOURS,
) -> Path:
    """Write and validate one release under an immutable staging directory."""
    latest = date.fromisoformat(latest_complete_date)
    start = date.fromisoformat(date_start)
    end = date.fromisoformat(date_end)
    if not start <= end <= latest:
        raise ValueError("release dates must satisfy date_start <= date_end <= latest_complete_date")
    release_id = latest_complete_date
    if not _RELEASE_ID.fullmatch(release_id):
        raise ValueError("release id must be an ISO date")
    staging_dir = root / "staging" / release_id
    if staging_dir.exists():
        raise FileExistsError(f"staging release already exists and was preserved: {staging_dir}")
    (staging_dir / "days").mkdir(parents=True, exist_ok=False)

    generated = generated_at or datetime.now(timezone.utc).isoformat()
    day_entries = []
    current = start
    try:
        while current <= end:
            partition = build_daily_track_partition(
                collection,
                display_date=current,
                lookback_hours=lookback_hours,
                lookahead_hours=lookahead_hours,
            )
            relative = Path("days") / f"{current.isoformat()}.geojson"
            output = staging_dir / relative
            _atomic_json(output, partition)
            checked = _validate_partition(output, current.isoformat())
            day_entries.append({
                "display_date": current.isoformat(),
                "path": relative.as_posix(),
                "sha256": _sha256(output),
                "bytes": output.stat().st_size,
                "features": len(checked.get("features") or []),
                "points": int(checked["metadata"]["point_count"]),
                "overlap": checked["metadata"]["overlap"],
            })
            current += timedelta(days=1)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "release_id": release_id,
            "latest_complete_date": latest_complete_date,
            "date_start": date_start,
            "date_end": date_end,
            "generated_at": generated,
            "days": day_entries,
            "assets": [
                {
                    "path": item["path"],
                    "sha256": item["sha256"],
                    "bytes": item["bytes"],
                    "type": "tracks_day",
                    "features": item["features"],
                }
                for item in day_entries
            ],
            "retention": {
                "rolling_source_days": (end - start).days + 1,
                "published_releases_kept": DEFAULT_RELEASES_TO_KEEP,
                # Two seven-day releases contain fourteen release-day payloads,
                # but consecutive rolling windows overlap.  Keep both numbers
                # explicit so this is not misread as fourteen unique UTC dates.
                "retained_release_day_payloads": (
                    (end - start).days + 1
                ) * DEFAULT_RELEASES_TO_KEEP,
                "calendar_date_union_if_published_daily": (
                    (end - start).days + DEFAULT_RELEASES_TO_KEEP
                ),
                "rollback_release_count": DEFAULT_RELEASES_TO_KEEP - 1,
            },
            "track_contract": {
                "frontend_load": "one_UTC_display_day_partition",
                "supported_trail_hours": list(SUPPORTED_TRAIL_HOURS),
                "maximum_lookback_hours": DEFAULT_LOOKBACK_HOURS,
                "lookahead_hours_for_linear_interpolation": DEFAULT_LOOKAHEAD_HOURS,
                "interpolation": "linear_between_adjacent_hourly_grid_centers",
            },
            "pipeline_limitations": {
                "shared_grid_track_normalized_fetch": False,
                "todo": "Fan out grid and tracks from one normalized fetch before production scheduling",
            },
        }
        _atomic_json(staging_dir / "manifest.json", manifest)
        _atomic_json(staging_dir / "run.json", {
            "release_id": release_id,
            "status": "staged",
            "generated_at": generated,
            "day_count": len(day_entries),
            "feature_count": sum(item["features"] for item in day_entries),
            "source": "normalized_track_feature_collection",
            "raw_gfw_response_saved": False,
        })
    except Exception:
        # Staging is intentionally retained for diagnosis and safe resume/retry.
        raise
    return staging_dir


def _validated_release_child(releases_dir: Path, candidate: Path) -> Path:
    resolved_parent = releases_dir.resolve()
    resolved = candidate.resolve()
    if resolved.parent != resolved_parent or not _RELEASE_ID.fullmatch(resolved.name):
        raise ValueError(f"refusing unsafe release path: {candidate}")
    return resolved


def _delete_release_exact(release_dir: Path) -> None:
    """Delete only files enumerated by one validated immutable release.

    An unexpected file, directory, or symlink fails closed and leaves the whole
    release in place.  This deliberately avoids recursive deletion and globs.
    """
    if release_dir.is_symlink() or not release_dir.is_dir():
        raise ValueError(f"release is not a plain directory: {release_dir}")
    manifest_path = release_dir / "manifest.json"
    run_path = release_dir / "run.json"
    days_dir = release_dir / "days"
    if any(path.is_symlink() for path in (manifest_path, run_path, days_dir)):
        raise ValueError(f"release contains a symlink: {release_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("release_id") != release_dir.name:
        raise ValueError(f"release manifest id mismatch: {release_dir}")

    expected_day_names: set[str] = set()
    for entry in manifest.get("days") or []:
        relative = Path(str(entry.get("path", "")))
        if (
            relative.is_absolute()
            or len(relative.parts) != 2
            or relative.parts[0] != "days"
            or relative.suffix != ".geojson"
            or ".." in relative.parts
        ):
            raise ValueError(f"unsafe partition path in release manifest: {relative}")
        expected_day_names.add(relative.name)

    top_level = {child.name for child in release_dir.iterdir()}
    if top_level != {"manifest.json", "run.json", "days"}:
        raise ValueError(f"release contains unexpected top-level entries: {release_dir}")
    if not days_dir.is_dir():
        raise ValueError(f"release days directory is missing: {release_dir}")
    actual_days = {child.name for child in days_dir.iterdir()}
    if actual_days != expected_day_names:
        raise ValueError(f"release contains unexpected day entries: {release_dir}")
    for name in sorted(expected_day_names):
        exact = days_dir / name
        if exact.is_symlink() or not exact.is_file():
            raise ValueError(f"release partition is not a plain file: {exact}")
        exact.unlink()
    days_dir.rmdir()
    run_path.unlink()
    manifest_path.unlink()
    release_dir.rmdir()


def publish_staged_release(
    *, root: Path, staging_dir: Path, releases_to_keep: int = DEFAULT_RELEASES_TO_KEEP
) -> dict[str, Any]:
    """Publish immutable files, cut over root manifest last, then prune exact old releases."""
    if releases_to_keep < 2:
        raise ValueError("releases_to_keep must be at least 2 for rollback")
    staging_dir = staging_dir.resolve()
    expected_staging_parent = (root / "staging").resolve()
    if staging_dir.parent != expected_staging_parent or not _RELEASE_ID.fullmatch(staging_dir.name):
        raise ValueError(f"refusing unsafe staging path: {staging_dir}")
    manifest_path = staging_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("release_id") != staging_dir.name:
        raise ValueError("staging manifest release_id mismatch")
    for entry in manifest.get("days") or []:
        relative = Path(str(entry["path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe partition path in staging manifest")
        partition_path = staging_dir / relative
        _validate_partition(partition_path, str(entry["display_date"]))
        if _sha256(partition_path) != entry["sha256"] or partition_path.stat().st_size != entry["bytes"]:
            raise ValueError(f"partition hash/size mismatch: {partition_path}")

    releases_dir = root / "releases"
    releases_dir.mkdir(parents=True, exist_ok=True)
    release_dir = _validated_release_child(releases_dir, releases_dir / staging_dir.name)
    if release_dir.exists():
        raise FileExistsError(f"immutable release already exists: {release_dir}")
    staging_dir.replace(release_dir)

    published = deepcopy(manifest)
    published["release_path"] = f"releases/{release_dir.name}"
    rolling_days = int(published["retention"]["rolling_source_days"])
    published["retention"].update({
        "published_releases_kept": releases_to_keep,
        "retained_release_day_payloads": rolling_days * releases_to_keep,
        "calendar_date_union_if_published_daily": rolling_days + releases_to_keep - 1,
        "rollback_release_count": releases_to_keep - 1,
    })
    for entry in published["days"]:
        entry["path"] = f"releases/{release_dir.name}/{entry['path']}"
    run_ledger_dir = root / "run-ledger"
    ledger_path = run_ledger_dir / f"{release_dir.name}.json"
    _atomic_json(ledger_path, {
        "release_id": release_dir.name,
        "status": "ready_for_cutover",
        "generated_at": published["generated_at"],
        "raw_gfw_response_saved": False,
    })
    # Atomic root manifest replacement is the sole reader-visible cutover.
    try:
        _atomic_json(root / "manifest.json", published)
    except Exception:
        # The old root manifest is still authoritative. Move the unpublished
        # immutable candidate back to the exact staging path for diagnosis.
        release_dir.replace(staging_dir)
        raise

    ledger_warning = None
    try:
        _atomic_json(ledger_path, {
            "release_id": release_dir.name,
            "status": "published",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "manifest_sha256": _sha256(root / "manifest.json"),
            "raw_gfw_response_saved": False,
        })
    except Exception as exc:
        # Reader-visible cutover has already succeeded.  Do not report the
        # release as failed or roll it back because an audit sidecar failed.
        ledger_warning = str(exc)

    release_names = sorted(
        child.name for child in releases_dir.iterdir()
        if child.is_dir() and _RELEASE_ID.fullmatch(child.name)
    )
    pruned = []
    prune_warnings = []
    for name in release_names[:-releases_to_keep]:
        exact = _validated_release_child(releases_dir, releases_dir / name)
        try:
            _delete_release_exact(exact)
            pruned.append(name)
        except Exception as exc:
            # Cleanup is after cutover and fail-closed: retain an unexpected
            # old release rather than broad-delete operator or diagnostic data.
            prune_warnings.append({"release_id": name, "error": str(exc)})
    return {
        "manifest": str((root / "manifest.json").resolve()),
        "release": str(release_dir),
        "release_id": release_dir.name,
        "pruned_release_ids": pruned,
        "prune_warnings": prune_warnings,
        "ledger_warning": ledger_warning,
    }


def _put_and_verify_s3(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    sha256: str,
    content_type: str,
    cache_control: str,
    content_encoding: str | None = None,
) -> None:
    request = dict(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        CacheControl=cache_control,
        Metadata={"sha256": sha256},
    )
    if content_encoding is not None:
        request["ContentEncoding"] = content_encoding
    client.put_object(**request)
    head = client.head_object(Bucket=bucket, Key=key)
    metadata = {str(name).lower(): str(value) for name, value in (head.get("Metadata") or {}).items()}
    if int(head.get("ContentLength", -1)) != len(body):
        raise RuntimeError(f"S3 HEAD ContentLength mismatch: {key}")
    if metadata.get("sha256") != sha256:
        raise RuntimeError(f"S3 HEAD sha256 metadata mismatch: {key}")


def _validate_previous_release_entry(
    entry: dict[str, Any], *, key_prefix: str
) -> dict[str, Any]:
    release_id = str(entry.get("release_id", ""))
    if not _RELEASE_ID.fullmatch(release_id):
        raise ValueError("previous root manifest contains an invalid release_id")
    expected_prefix = f"{key_prefix}/releases/{release_id}/"
    object_keys = entry.get("object_keys")
    if not isinstance(object_keys, list) or not object_keys:
        raise ValueError(f"previous release {release_id} lacks exact object_keys")
    normalized_keys = []
    for value in object_keys:
        key = str(value)
        suffix = key.removeprefix(expected_prefix)
        if (
            not key.startswith(expected_prefix)
            or not suffix
            or "//" in suffix
            or any(not _KEY_PART.fullmatch(part) for part in suffix.split("/"))
        ):
            raise ValueError(f"unknown or unsafe previous release key: {key}")
        normalized_keys.append(key)
    if len(set(normalized_keys)) != len(normalized_keys):
        raise ValueError(f"previous release {release_id} repeats an object key")
    manifest_key = str(entry.get("manifest_key", ""))
    if manifest_key != f"{expected_prefix}manifest.json" or manifest_key not in normalized_keys:
        raise ValueError(f"previous release {release_id} has an invalid manifest_key")
    return {**entry, "release_id": release_id, "manifest_key": manifest_key, "object_keys": normalized_keys}


def publish_release_to_s3(
    client: Any,
    *,
    release_dir: Path,
    bucket: str,
    key_prefix: str,
    public_url_prefix: str,
    previous_root_manifest: dict[str, Any] | None = None,
    releases_to_keep: int = DEFAULT_RELEASES_TO_KEEP,
) -> dict[str, Any]:
    """Upload one generic immutable release and cut over its S3 root manifest last.

    ``client`` is intentionally injected and only needs boto3-compatible
    ``put_object``, ``head_object``, and ``delete_object`` methods. No S3 list or
    prefix delete is used; retention deletes only exact keys enumerated by the
    prior root manifest after a verified root-manifest cutover.
    """
    bucket, key_prefix, public_url_prefix = _validate_s3_config(
        bucket=bucket, key_prefix=key_prefix, public_url_prefix=public_url_prefix
    )
    if releases_to_keep < 2:
        raise ValueError("releases_to_keep must be at least 2 for rollback")
    release_dir = release_dir.resolve()
    release_id = release_dir.name
    if release_dir.is_symlink() or not release_dir.is_dir() or not _RELEASE_ID.fullmatch(release_id):
        raise ValueError(f"release_dir must be a plain strict-date directory: {release_dir}")
    source_manifest_path = release_dir / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if source_manifest.get("release_id") != release_id:
        raise ValueError("release manifest release_id mismatch")
    assets = manifest_assets(source_manifest)

    previous_entries = []
    if previous_root_manifest is not None:
        if not isinstance(previous_root_manifest, dict):
            raise ValueError("previous_root_manifest must be an object")
        previous_entries = [
            _validate_previous_release_entry(entry, key_prefix=key_prefix)
            for entry in (previous_root_manifest.get("published_releases") or [])
        ]
        if any(entry["release_id"] == release_id for entry in previous_entries):
            raise FileExistsError(f"immutable S3 release already recorded: {release_id}")

    origin_mapping = {
        "s3_key_prefix": key_prefix,
        "public_url_prefix": public_url_prefix,
        "path_rule": "public_url_prefix + '/' + key relative to s3_key_prefix",
    }
    remote_release_manifest = deepcopy(source_manifest)
    remote_release_manifest["assets"] = assets
    remote_release_manifest["origin_mapping"] = origin_mapping
    release_prefix = f"{key_prefix}/releases/{release_id}"

    asset_keys = []
    for asset in assets:
        relative = _validated_asset_relative_path(asset["path"])
        local_path = release_dir / relative
        if local_path.is_symlink() or not local_path.is_file():
            raise ValueError(f"manifest asset is not a plain local file: {local_path}")
        if local_path.stat().st_size != asset["bytes"] or _sha256(local_path) != asset["sha256"]:
            raise ValueError(f"local asset hash/size mismatch: {local_path}")
        asset_keys.append(f"{release_prefix}/{relative.as_posix()}")

    run_path = release_dir / "run.json"
    if run_path.exists() and (run_path.is_symlink() or not run_path.is_file()):
        raise ValueError(f"run ledger is not a plain file: {run_path}")
    release_manifest_key = f"{release_prefix}/manifest.json"
    run_key = f"{release_prefix}/run.json" if run_path.is_file() else None
    object_keys = [*asset_keys]
    if run_key:
        object_keys.append(run_key)
    object_keys.append(release_manifest_key)
    new_release_entry = {
        "release_id": release_id,
        "manifest_key": release_manifest_key,
        "object_keys": object_keys,
    }
    all_entries = sorted(
        [*previous_entries, new_release_entry],
        key=lambda entry: entry["release_id"],
        reverse=True,
    )
    kept_entries = all_entries[:releases_to_keep]
    retired_entries = all_entries[releases_to_keep:]
    # Validate every future exact delete before the first upload/cutover.
    for entry in retired_entries:
        _validate_previous_release_entry(entry, key_prefix=key_prefix)

    for asset, key in zip(assets, asset_keys):
        body = (release_dir / asset["path"]).read_bytes()
        _put_and_verify_s3(
            client,
            bucket=bucket,
            key=key,
            body=body,
            sha256=asset["sha256"],
            content_type=(
                "application/geo+json" if asset["path"].endswith(".geojson")
                else "application/gzip" if asset["path"].endswith(".gz")
                else "application/vnd.pmtiles" if asset["path"].endswith(".pmtiles")
                else "application/x-ndjson" if asset["path"].endswith(".ndjson")
                else "application/octet-stream"
            ),
            cache_control=RELEASE_CACHE_CONTROL,
        )
    if run_key:
        run_body = run_path.read_bytes()
        _put_and_verify_s3(
            client, bucket=bucket, key=run_key, body=run_body,
            sha256=_sha256_bytes(run_body), content_type="application/json",
            cache_control=RELEASE_CACHE_CONTROL,
        )
    release_manifest_body = _canonical(remote_release_manifest).encode("utf-8")
    _put_and_verify_s3(
        client, bucket=bucket, key=release_manifest_key,
        body=release_manifest_body, sha256=_sha256_bytes(release_manifest_body),
        content_type="application/json", cache_control=RELEASE_CACHE_CONTROL,
    )

    root_manifest = deepcopy(remote_release_manifest)
    root_manifest["release_path"] = f"releases/{release_id}"
    root_manifest["origin_mapping"] = origin_mapping
    for index_name in ("assets", "days", "hours"):
        for entry in root_manifest.get(index_name) or []:
            entry["path"] = f"releases/{release_id}/{entry['path']}"
    for section_name, index_name in (
        ("tracks", "days"), ("tracks", "singleton_days"), ("tracks", "frames"),
        ("grid", "hours"), ("dark_vessels", "hours")
    ):
        for entry in (root_manifest.get(section_name) or {}).get(index_name) or []:
            entry["path"] = f"releases/{release_id}/{entry['path']}"
            for detail in entry.get("detail_buckets") or []:
                detail["path"] = f"releases/{release_id}/{detail['path']}"
    root_manifest["published_releases"] = kept_entries
    root_key = f"{key_prefix}/manifest.json"
    root_body = _canonical(root_manifest).encode("utf-8")
    try:
        _put_and_verify_s3(
            client, bucket=bucket, key=root_key, body=root_body,
            sha256=_sha256_bytes(root_body), content_type="application/json",
            cache_control=ROOT_CACHE_CONTROL,
        )
    except Exception:
        # Restore the prior reader-visible root when a root HEAD check fails.
        if previous_root_manifest is None:
            client.delete_object(Bucket=bucket, Key=root_key)
        else:
            previous_body = _canonical(previous_root_manifest).encode("utf-8")
            _put_and_verify_s3(
                client, bucket=bucket, key=root_key, body=previous_body,
                sha256=_sha256_bytes(previous_body), content_type="application/json",
                cache_control=ROOT_CACHE_CONTROL,
            )
        raise

    deleted_keys = []
    delete_warnings = []
    for entry in retired_entries:
        for key in entry["object_keys"]:
            try:
                client.delete_object(Bucket=bucket, Key=key)
                deleted_keys.append(key)
            except Exception as exc:
                delete_warnings.append({"key": key, "error": str(exc)})
    return {
        "bucket": bucket,
        "root_manifest_key": root_key,
        "root_manifest_sha256": _sha256_bytes(root_body),
        "root_manifest_bytes": len(root_body),
        "release_id": release_id,
        "uploaded_object_keys": object_keys,
        "deleted_object_keys": deleted_keys,
        "delete_warnings": delete_warnings,
        "public_manifest_url": f"{public_url_prefix}/manifest.json",
    }


def publish_track_release(
    collection: dict[str, Any],
    *,
    root: Path,
    latest_complete_date: str,
    date_start: str,
    date_end: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    staging_dir = stage_track_release(
        collection,
        root=root,
        latest_complete_date=latest_complete_date,
        date_start=date_start,
        date_end=date_end,
        generated_at=generated_at,
    )
    return publish_staged_release(root=root, staging_dir=staging_dir)
