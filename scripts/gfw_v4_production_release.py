#!/usr/bin/env python3
"""Build one immutable, local-only GFW East Asia schema-4 release.

This is intentionally a release *builder*, not a publisher: it reads only
already-normalized local inputs, makes no network calls, and never writes S3,
Cloudflare, a database ledger, or a production root pointer.  The target root
must not exist; a complete release is assembled in a sibling staging directory
and moved into place only after every local readback passes.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.gfw_east_asia_v4_poc import (
    FIXED_BBOX,
    build_grid_artifacts_from_compare_sqlite,
    read_ndjson,
)
from scripts.gfw_hourly_browser_assets import (
    build_track_browser_assets,
    require_gfw_asset_toolchain,
)
from scripts.gfw_hourly_tracks_poc import finalize_track_store
from scripts.gfw_v4_spatial_frames import SPATIAL_FRAME_ZOOM, build_spatial_frame
from tasks.gfw_v4_manifest_publisher import TIER2_BINDING_ALGORITHM, tier2_core_digest


SCHEMA_VERSION = 4
IDENTITY_ENCODING = "identity"
UTC_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PRESENCE_DATASET_PREFIX = "public-global-presence:"
FISHING_DATASET_PREFIX = "public-global-fishing-effort:"
TRACK_BUCKETS = ("fishing", "cargo", "passenger", "carrier", "other", "unknown")
DEFAULT_TRACK_BUCKETS = ("fishing", "cargo", "passenger")
RELEASE_ID = re.compile(r"^\d{4}-\d{2}-\d{2}__(?:[a-z0-9][a-z0-9._-]*)$")
FORMAL_ASSET_TYPES = frozenset({
    "tracks_day_pmtiles", "track_frame_pmtiles", "track_detail_bucket",
    "grid_hour_pmtiles", "grid_detail_bucket", "fishing_effort_day", "gear_observations",
})
FROZEN_TOP_LEVEL = frozenset({
    "schema_version", "release_id", "selected_utc_date", "bbox", "source_dataset_id",
    "resolved_dataset_version", "days", "grid", "tracks", "fishing_effort",
    "layer_separation", "artifacts", "release_truth",
})


class ProductionReleaseError(RuntimeError):
    """Raised before a candidate can be promoted to its immutable local root."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(_canonical_bytes(value))
    temporary.replace(path)


def _release_token(value: str) -> str:
    token = re.sub(r"[^a-z0-9._-]+", "-", value.lower()).strip(".-")
    if not token:
        raise ProductionReleaseError("resolved dataset version cannot form a legal release token")
    return token


def validate_schema4_release_manifest(manifest: dict[str, Any]) -> None:
    """Reject a candidate that would fail the frozen platform schema-4 contract."""
    missing = sorted(FROZEN_TOP_LEVEL - set(manifest))
    if missing or manifest.get("schema_version") != SCHEMA_VERSION:
        raise ProductionReleaseError(f"schema-4 manifest missing/invalid fields: {missing}")
    release_id = manifest.get("release_id")
    if not isinstance(release_id, str) or not RELEASE_ID.fullmatch(release_id):
        raise ProductionReleaseError("schema-4 release_id must use YYYY-MM-DD__dataset__dataset")
    if release_id.split("__", 1)[1] != _release_token(str(manifest.get("resolved_dataset_version") or "")):
        raise ProductionReleaseError("schema-4 release_id must carry the slugged resolved dataset version")
    if not isinstance(manifest.get("artifacts"), list) or any(
        not isinstance(asset, dict) or asset.get("type") not in FORMAL_ASSET_TYPES
        for asset in manifest["artifacts"]
    ):
        raise ProductionReleaseError("schema-4 release has an unsupported asset type")
    if manifest.get("layer_separation") != {
        "grid": "gfwHourlyGrid", "tracks": "gfwHourlyTracks",
        "fishing_effort": "gfwFishingEffort", "dark_vessels": "gfwDarkVessels",
    }:
        raise ProductionReleaseError("schema-4 layer_separation must declare all four stable layer IDs")
    tracks = manifest.get("tracks") or {}
    if tracks.get("buckets") != [bucket.upper() for bucket in TRACK_BUCKETS] or tracks.get("default_buckets") != [bucket.upper() for bucket in DEFAULT_TRACK_BUCKETS]:
        raise ProductionReleaseError("schema-4 tracks taxonomy/default buckets are frozen")
    for asset in manifest["artifacts"]:
        if asset.get("type") == "track_frame_pmtiles":
            validate_track_frame_artifact(asset)


def validate_track_frame_artifact(asset: dict[str, Any]) -> None:
    """Mirror the consumer's frozen track_frame_pmtiles artifact contract.

    Kept equivalent to install-gfw-v4-local-release.sh and to
    tasks.gfw_v4_manifest_publisher._validate_track_frame_pmtiles, so a missing
    or inconsistent identity/no-drop proof fails at build time instead of being
    discovered by the installer.
    """
    path = asset.get("path")
    counts = asset.get("semantic_counts")
    spatial = asset.get("spatial_contract")
    if asset.get("content_type") != "application/octet-stream" or asset.get("content_encoding") != IDENTITY_ENCODING:
        raise ProductionReleaseError(f"track_frame_pmtiles must be identity octet-stream: {path}")
    if not isinstance(counts, dict) or not str(counts.get("observed_at") or "") or not str(counts.get("bucket") or ""):
        raise ProductionReleaseError(f"track_frame_pmtiles lacks observed_at/bucket: {path}")
    if not isinstance(spatial, dict) or spatial.get("fixed_zoom") != SPATIAL_FRAME_ZOOM:
        raise ProductionReleaseError(f"track_frame_pmtiles must carry a fixed-z6 spatial_contract: {path}")
    source_count = spatial.get("source_feature_count")
    if (
        not isinstance(source_count, int) or isinstance(source_count, bool) or source_count < 0
        or spatial.get("decoded_feature_count") != source_count
        or counts.get("feature_count") != source_count
        or spatial.get("identity_duplicate_count") != 0
        or spatial.get("identity_missing_count") != 0
    ):
        raise ProductionReleaseError(f"track_frame_pmtiles identity/no-drop proof failed: {path}")


def _track_bucket(value: Any) -> str | None:
    kind = str(value or "").strip().upper()
    mapped = {
        "FISHING": "fishing",
        "CARGO": "cargo",
        "PASSENGER": "passenger",
        "CARRIER": "carrier",
    }.get(kind)
    if mapped is not None:
        return mapped
    if kind in {"", "NA", "UNKNOWN", "NULL", "NONE"}:
        return "unknown"
    if kind in {"GEAR", "FAD", "TANKER"}:
        return None
    return "other"


def _is_non_vessel(member: dict[str, Any]) -> bool:
    return any(
        str(member.get(field) or "").strip().upper() in {"GEAR", "FAD"}
        for field in ("vessel_type", "geartype")
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ProductionReleaseError(f"invalid JSON source: {path}") from exc
    if not isinstance(value, dict):
        raise ProductionReleaseError(f"JSON source must be an object: {path}")
    return value


def _validate_presence_inputs(
    *, sqlite_path: Path, high_ndjson: Path, high_metrics_path: Path, selected_day: date,
) -> dict[str, Any]:
    if not sqlite_path.is_file() or not high_ndjson.is_file() or not high_metrics_path.is_file():
        raise ProductionReleaseError("presence SQLite, HIGH NDJSON, and HIGH metrics must all exist")
    metrics = _load_json(high_metrics_path)
    if metrics.get("raw_response_saved") is not False:
        raise ProductionReleaseError("presence evidence must prove raw_response_saved=false")
    versions = metrics.get("resolved_dataset_versions")
    if not isinstance(versions, list) or len(versions) != 1 or not str(versions[0]).startswith(PRESENCE_DATASET_PREFIX):
        raise ProductionReleaseError("presence evidence lacks one resolved v4 dataset version")
    tiles = metrics.get("tiles")
    if not isinstance(tiles, list) or not tiles or any(tile.get("next_offset_complete") is not True for tile in tiles if isinstance(tile, dict)):
        raise ProductionReleaseError("presence evidence lacks complete nextOffset proof for every tile")
    if any(not isinstance(tile, dict) for tile in tiles):
        raise ProductionReleaseError("presence tile evidence is malformed")

    connection = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(canonical)")}
        required = {"source", "hour", "vessel_id", "cell_lon", "cell_lat", "member_json"}
        if not required <= columns:
            raise ProductionReleaseError("presence comparison SQLite has an incompatible canonical schema")
        hourly = list(connection.execute(
            "SELECT hour, COUNT(*) FROM canonical WHERE source='HIGH' GROUP BY hour ORDER BY hour"
        ))
        expected_hours = [f"{selected_day.isoformat()}T{hour:02d}:00:00Z" for hour in range(24)]
        if [row[0] for row in hourly] != expected_hours or any(int(row[1]) <= 0 for row in hourly):
            raise ProductionReleaseError("presence SQLite must contain each selected UTC hour exactly once")
        high_rows = sum(int(row[1]) for row in hourly)
        non_vessel_rows = int(connection.execute("""
            SELECT COUNT(*) FROM canonical
            WHERE source='HIGH' AND (
                UPPER(COALESCE(json_extract(member_json, '$.vessel_type'), '')) IN ('GEAR','FAD')
                OR UPPER(COALESCE(json_extract(member_json, '$.geartype'), '')) IN ('GEAR','FAD')
            )
        """).fetchone()[0])
    finally:
        connection.close()
    return {
        "selected_utc_date": selected_day.isoformat(),
        "resolved_dataset_version": str(versions[0]),
        "raw_response_saved": False,
        "next_offset_complete": True,
        "tile_count": len(tiles),
        "canonical_high_rows": high_rows,
        "grid_excluded_non_vessel_rows": non_vessel_rows,
        "sqlite": {"path": str(sqlite_path), "sha256": _sha256(sqlite_path), "bytes": sqlite_path.stat().st_size},
        "high_ndjson": {"path": str(high_ndjson), "sha256": _sha256(high_ndjson), "bytes": high_ndjson.stat().st_size},
        "metrics": {"path": str(high_metrics_path), "sha256": _sha256(high_metrics_path)},
    }


def _validate_fishing_asset(
    path: Path, *, selected_day: date, fishing_asset_sha256: str | None,
    fishing_lineage: dict[str, Any] | None,
) -> dict[str, Any]:
    if not path.is_file():
        raise ProductionReleaseError("verified Fishing Effort derived asset is missing")
    actual_sha256 = _sha256(path)
    if not isinstance(fishing_asset_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", fishing_asset_sha256):
        raise ProductionReleaseError("Fishing Effort requires the daily derived asset SHA-256")
    if actual_sha256 != fishing_asset_sha256:
        raise ProductionReleaseError("Fishing Effort asset SHA-256 does not match the daily derived artifact")
    if not isinstance(fishing_lineage, dict):
        raise ProductionReleaseError("Fishing Effort requires daily lineage metadata")
    try:
        payload = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, ValueError) as exc:
        raise ProductionReleaseError("Fishing Effort artifact is not valid gzip JSON") from exc
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    if not isinstance(metadata, dict):
        raise ProductionReleaseError("Fishing Effort artifact lacks lineage metadata")
    if _canonical_bytes(metadata) != _canonical_bytes(fishing_lineage):
        raise ProductionReleaseError("Fishing Effort lineage does not exactly match the daily derived artifact")
    if metadata.get("date") != selected_day.isoformat() or not str(metadata.get("resolved_dataset_version") or "").startswith(FISHING_DATASET_PREFIX):
        raise ProductionReleaseError("Fishing Effort artifact date or resolved dataset version is incompatible")
    quality = metadata.get("quality")
    if not isinstance(quality, dict) or any(int(quality.get(key, -1)) != 0 for key in (
        "invalid_rows", "negative_hours_rejected", "wrong_day_rows", "boundary_overlap_rows",
    )):
        raise ProductionReleaseError("Fishing Effort artifact does not meet the approved quality gate")
    return {
        "source_path": str(path), "sha256": actual_sha256, "bytes": path.stat().st_size,
        "feature_count": len(payload.get("features") or []),
        "lineage": metadata,
    }


def _filter_track_rows(rows: Iterable[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    grouped = {bucket: [] for bucket in TRACK_BUCKETS}
    observed: Counter[str] = Counter()
    for row in rows:
        kind = str(row.get("vessel_type") or "").strip().upper() or "UNKNOWN"
        observed[kind] += 1
        bucket = _track_bucket(kind)
        if bucket is not None:
            grouped[bucket].append(row)
    if observed.get("TANKER", 0):
        raise ProductionReleaseError("TANKER observations require an explicit taxonomy review; no silent remap is allowed")
    return grouped, dict(sorted(observed.items()))


def _write_ndjson(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(_canonical_bytes(row).decode("utf-8"))
            handle.write("\n")


def _asset(
    path: Path, *, artifact_root: Path, asset_type: str, scope: dict[str, Any],
    spatial_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    byte_size = path.stat().st_size
    sha256 = _sha256(path)
    record = {
        "path": path.relative_to(artifact_root).as_posix(), "type": asset_type,
        "bytes": byte_size, "content_length": byte_size, "sha256": sha256,
        "etag": f'"{sha256}"', "cache_control": "public,max-age=604800,s-maxage=604800,immutable",
        "semantic_counts": scope,
    }
    # track_frame_pmtiles carries its identity/no-drop readback proof at the top
    # level too; the consumer validates the artifact entry, not the nested index.
    if spatial_contract is not None:
        record["spatial_contract"] = spatial_contract
    if path.suffix == ".gz":
        record.update({"content_type": "application/json", "content_encoding": "gzip"})
    elif path.suffix == ".pmtiles":
        record.update({"content_type": "application/octet-stream", "content_encoding": "identity"})
    return record


def _prefix_track_asset_paths(
    *, bucket: str, days: list[dict[str, Any]], frames: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Convert bucket-local browser-builder paths to immutable release paths."""
    prefix = Path("tracks") / bucket
    for entry in days:
        entry["path"] = (prefix / entry["path"]).as_posix()
        for detail in entry["detail_buckets"]:
            detail["path"] = (prefix / detail["path"]).as_posix()
    for entry in frames:
        entry["path"] = (prefix / entry["path"]).as_posix()
    return days, frames


def _rewrite_nested_frame(frame: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    """Rewrite a nested track frame in place to the frozen consumer shape.

    ``build_spatial_frame`` returns *artifact*-shaped keys, so overwriting a
    nested frame with it verbatim drops the fields the release parser validates
    (``format``, ``observed_at``, ``features``) and leaks an artifact-only
    ``type``.  The nested index and the top-level ``artifacts`` array are two
    different contracts over the same file; keep them apart.
    """
    observed_at = str(frame["observed_at"])
    frame.clear()
    frame.update({key: value for key, value in metadata.items() if key != "type"})
    frame["format"] = "pmtiles"
    frame["observed_at"] = observed_at
    frame["features"] = metadata["semantic_counts"]["feature_count"]
    return frame


def _readback_assets(artifact_root: Path, assets: list[dict[str, Any]], pmtiles: Path) -> dict[str, Any]:
    checked_bytes = 0
    for asset in assets:
        path = artifact_root / asset["path"]
        if not path.is_file() or path.stat().st_size != int(asset["bytes"]) or _sha256(path) != asset["sha256"]:
            raise ProductionReleaseError(f"asset bytes/SHA readback failed: {asset['path']}")
        checked_bytes += path.stat().st_size
        if path.suffix == ".gz":
            try:
                json.loads(gzip.decompress(path.read_bytes()))
            except (OSError, ValueError) as exc:
                raise ProductionReleaseError(f"gzip JSON readback failed: {asset['path']}") from exc
        elif path.suffix == ".pmtiles":
            result = subprocess.run([str(pmtiles), "verify", str(path)], check=False, capture_output=True, text=True)
            if result.returncode != 0:
                raise ProductionReleaseError(f"PMTiles readback failed: {asset['path']}: {result.stderr[-500:]}")
    return {"status": "passed", "asset_count": len(assets), "checked_wire_bytes": checked_bytes}


def build_production_release(
    *, output_root: Path, sqlite_path: Path, high_ndjson: Path, high_metrics_path: Path,
    fishing_asset: Path, selected_day: date, fishing_asset_sha256: str | None = None,
    fishing_lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a schema-4 immutable local candidate without any external side effect."""
    if output_root.exists():
        raise FileExistsError(f"immutable production output already exists: {output_root}")
    if not UTC_DAY.fullmatch(selected_day.isoformat()):
        raise ProductionReleaseError("selected day must be a UTC YYYY-MM-DD date")
    _tippecanoe, pmtiles = require_gfw_asset_toolchain()
    presence = _validate_presence_inputs(
        sqlite_path=sqlite_path, high_ndjson=high_ndjson,
        high_metrics_path=high_metrics_path, selected_day=selected_day,
    )
    fishing = _validate_fishing_asset(
        fishing_asset, selected_day=selected_day,
        fishing_asset_sha256=fishing_asset_sha256, fishing_lineage=fishing_lineage,
    )
    track_rows, observed_taxonomy = _filter_track_rows(read_ndjson(high_ndjson))
    release_id = f"{selected_day.isoformat()}__{_release_token(presence['resolved_dataset_version'])}"

    output_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".gfw-v4-production-staging-", dir=output_root.parent))
    release_dir = staging / "releases" / release_id
    try:
        grid, _grid_assets = build_grid_artifacts_from_compare_sqlite(
            sqlite_path, selected_day=selected_day, root=release_dir,
            semantic_readback=True, pmtiles_cli=pmtiles, include_member=lambda member: not _is_non_vessel(member),
        )
        track_manifest: dict[str, Any] = {"default_enabled_buckets": list(DEFAULT_TRACK_BUCKETS), "buckets": {}}
        work_root = staging / ".work"
        for bucket in TRACK_BUCKETS:
            bucket_rows = track_rows[bucket]
            input_path = work_root / f"{bucket}.ndjson"
            _write_ndjson(input_path, bucket_rows)
            bucket_work = work_root / bucket
            bucket_work.mkdir(parents=True, exist_ok=False)
            store = finalize_track_store([input_path], work_dir=bucket_work, gap_hours=2.0, max_speed_knots=80.0)
            try:
                days, frames, counts = build_track_browser_assets(
                    store, start=selected_day, latest=selected_day,
                    output_root=release_dir / "tracks" / bucket, release_id=release_id,
                )
                days, frames = _prefix_track_asset_paths(bucket=bucket, days=days, frames=frames)
                for frame in frames:
                    source = release_dir / frame["path"]
                    output = source.with_suffix("").with_suffix(".pmtiles")
                    metadata = build_spatial_frame(
                        source=source, output=output,
                        observed_at=str(frame["observed_at"]), bucket=bucket,
                        release_root=release_dir,
                    )
                    source.unlink()
                    _rewrite_nested_frame(frame, metadata)
                store_counts = store.counts()
            finally:
                store.close()
            track_manifest["buckets"][bucket] = {
                "source_vessel_hour_rows": len(bucket_rows), "days": days, "frames": frames,
                "counts": {**counts, **store_counts},
            }
        shutil.rmtree(work_root)

        fishing_target = release_dir / "fishing-effort" / f"{selected_day.isoformat()}.geojson.gz"
        fishing_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(fishing_asset, fishing_target)

        assets: list[dict[str, Any]] = []
        for hour in grid["hours"]:
            observed_at = hour["observed_at"]
            assets.append(_asset(release_dir / hour["pmtiles"]["path"], artifact_root=staging, asset_type="grid_hour_pmtiles", scope={"observed_at": observed_at, "cell_count": hour["pmtiles"]["features"], "vessel_count": hour["pmtiles"]["vessels"]}))
            for detail in hour["details"]:
                assets.append(_asset(release_dir / detail["path"], artifact_root=staging, asset_type="grid_detail_bucket", scope={"observed_at": observed_at, "entry_count": detail["features"], "vessel_count": detail["vessels"]}))
        for bucket, result in track_manifest["buckets"].items():
            for day_entry in result["days"]:
                assets.append(_asset(release_dir / day_entry["path"], artifact_root=staging, asset_type="tracks_day_pmtiles", scope={"bucket": bucket, "display_date": day_entry["display_date"], "edge_count": day_entry["edge_count"], "singleton_count": day_entry["singleton_count"]}))
                for detail in day_entry["detail_buckets"]:
                    assets.append(_asset(release_dir / detail["path"], artifact_root=staging, asset_type="track_detail_bucket", scope={"bucket": bucket, "display_date": day_entry["display_date"], "entry_count": detail["entry_count"], "point_count": detail["point_count"]}))
            for frame in result["frames"]:
                assets.append(_asset(release_dir / frame["path"], artifact_root=staging, asset_type="track_frame_pmtiles", scope=frame["semantic_counts"], spatial_contract=frame["spatial_contract"]))
        assets.append(_asset(fishing_target, artifact_root=staging, asset_type="fishing_effort_day", scope={"display_date": selected_day.isoformat(), "feature_count": fishing["feature_count"]}))
        assets.sort(key=lambda item: item["path"])
        readback = _readback_assets(staging, assets, pmtiles)
        if any("raw" in item["path"].lower() or item["path"].endswith(".ndjson") for item in assets):
            raise ProductionReleaseError("release asset set must not retain raw inputs")

        release_manifest = {
            "schema_version": SCHEMA_VERSION, "release_id": release_id,
            "required_top_level_fields": sorted(FROZEN_TOP_LEVEL),
            "selected_utc_date": selected_day.isoformat(), "immutable": True,
            "source_dataset_id": "public-global-presence",
            "resolved_dataset_version": presence["resolved_dataset_version"],
            "days": [selected_day.isoformat()],
            "production_cutover": False,
            "cutover_blocker": "Tier 2 production-consumer desktop and real-mobile browser budgets are not yet accepted",
            "bbox": list(FIXED_BBOX),
            "root_contract": {
                "root_path": "deploy-assets/global-maritime/gfw-hourly/v4/manifest.json",
                "release_prefix": f"deploy-assets/global-maritime/gfw-hourly/v4/releases/{release_id}/",
            },
            "source_proof": {
                "presence": presence,
                "fishing_effort": {
                    "raw_response_saved": False,
                    "source_asset_sha256": fishing["sha256"],
                    "source_asset_bytes": fishing["bytes"],
                    "lineage": fishing["lineage"],
                },
            },
            "taxonomy": {
                "tanker": "quarantine", "carrier": "independent_default_off",
                "gear_fad": "independent_non_vessel_observation",
                "observed_source_vessel_type_rows": observed_taxonomy,
                "grid_excludes": ["GEAR", "FAD"],
                "tracks_excludes": ["GEAR", "FAD", "TANKER"],
                "tanker_rows": 0,
            },
            "layer_separation": {
                "grid": "gfwHourlyGrid", "tracks": "gfwHourlyTracks",
                "fishing_effort": "gfwFishingEffort", "dark_vessels": "gfwDarkVessels",
            },
            "cache_contract": {
                "root": "public,max-age=60,s-maxage=60,stale-while-revalidate=300",
                "release": "public,max-age=604800,s-maxage=604800,immutable",
            },
            "grid": grid,
            "tracks": {
                "buckets": [bucket.upper() for bucket in TRACK_BUCKETS],
                "default_buckets": [bucket.upper() for bucket in DEFAULT_TRACK_BUCKETS],
                "bucket_data": track_manifest["buckets"],
            },
            "fishing_effort": {"path": fishing_target.relative_to(staging).as_posix(), "feature_count": fishing["feature_count"]},
            "artifacts": assets, "readback": readback,
            "release_truth": {
                "build": "passed_local_candidate", "tier1_status": "passed",
                "tier2_status": "not_run", "readback_status": "passed", "upload": "not_run", "deploy": "not_run",
                "root_cutover": "blocked_until_tier2_passed",
            },
        }
        validate_schema4_release_manifest(release_manifest)
        # Advertise the Tier 2 binding target so the browser operator can bind
        # evidence by copying this value, instead of reimplementing canonical
        # JSON in another language.  The field is outside the bound core, so
        # adding it does not change the digest it names, and the validator
        # recomputes the digest independently rather than trusting it.
        release_manifest["tier2_binding"] = {
            "algorithm": TIER2_BINDING_ALGORITHM,
            "core_sha256": tier2_core_digest(release_manifest),
        }
        _atomic_json(release_dir / "manifest.json", release_manifest)
        release_manifest_sha = _sha256(release_dir / "manifest.json")
        root_manifest = {
            "schema_version": SCHEMA_VERSION, "immutable_release_contract": True,
            "release_id": release_id, "selected_utc_date": selected_day.isoformat(),
            "release_manifest": {"path": f"releases/{release_id}/manifest.json", "bytes": (release_dir / "manifest.json").stat().st_size, "sha256": release_manifest_sha},
            "production_cutover": False,
        }
        _atomic_json(staging / "manifest.json", root_manifest)
        staging.replace(output_root)
        return {"output_root": str(output_root), "release_id": release_id, "assets": len(assets), "readback": readback}
    except Exception:
        # The destination is still absent; retain the uniquely named staging
        # directory for diagnosis rather than deleting potentially useful evidence.
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--presence-compare", type=Path, required=True)
    parser.add_argument("--high-ndjson", type=Path, required=True)
    parser.add_argument("--high-metrics", type=Path, required=True)
    parser.add_argument("--fishing-asset", type=Path, required=True)
    parser.add_argument("--fishing-asset-sha256", required=True)
    parser.add_argument("--fishing-lineage-json", type=Path, required=True)
    parser.add_argument("--selected-day", default="2026-08-21")
    args = parser.parse_args()
    result = build_production_release(
        output_root=args.output_root.expanduser().resolve(),
        sqlite_path=args.presence_compare.expanduser().resolve(),
        high_ndjson=args.high_ndjson.expanduser().resolve(),
        high_metrics_path=args.high_metrics.expanduser().resolve(),
        fishing_asset=args.fishing_asset.expanduser().resolve(),
        fishing_asset_sha256=args.fishing_asset_sha256,
        fishing_lineage=_load_json(args.fishing_lineage_json.expanduser().resolve()),
        selected_day=date.fromisoformat(args.selected_day),
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
