"""Fail-closed production boundary for a future GFW East Asia v4 daily job.

The v4 drivers are deliberately local POCs.  This module is the only allowed
entry point for promoting their *normalized* output later: it refuses POC
manifests and raw payloads, and it is intentionally not registered in
``main.py`` until an independently reviewed source, finalizer, and publisher
are injected.
"""

from __future__ import annotations

import re
import uuid
import gzip
import hashlib
import inspect
import json
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import config
from scripts.gfw_hourly_browser_assets import require_gfw_asset_toolchain
from tasks.gfw_v4_manifest_publisher import (
    TIER2_BINDING_ALGORITHM,
    tier2_binding_failure,
    tier2_core_digest,
)


_UTC_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PRESENCE_DATASET = "public-global-presence:"
_FISHING_DATASET = "public-global-fishing-effort:"
_POPUP_FIELDS = (
    "vessel_id", "mmsi", "ship_name", "vessel_type", "flag", "hours",
    "entry_timestamp", "exit_timestamp", "imo", "callsign",
    "first_transmission_date", "last_transmission_date", "dataset", "geartype",
)


class GFWV4SourceContractBlocked(RuntimeError):
    """Raised before any ledger, network, S3, or publication side effect."""


@dataclass(frozen=True)
class GFWV4DailyPublishSettings:
    """Immutable v4 scheduler boundary; all production values are explicit."""
    enabled: bool
    redistribution_approved: bool
    single_writer: bool
    tier2_evidence_id: str
    token: str
    db_url: str
    bucket: str
    s3_prefix: str
    public_url_prefix: str
    work_root: Path = Path("data/gfw_v4_daily_publish_spool")
    publish_time: str = "08:30"

    @classmethod
    def from_config(cls) -> "GFWV4DailyPublishSettings":
        return cls(
            enabled=config.GFW_V4_DAILY_PUBLISH_ENABLED,
            redistribution_approved=config.GFW_V4_DAILY_REDISTRIBUTION_APPROVED,
            single_writer=config.GFW_V4_DAILY_SINGLE_WRITER,
            tier2_evidence_id=config.GFW_V4_DAILY_TIER2_EVIDENCE_ID,
            token=config.GFW_ACCESS_TOKEN, db_url=config.SUPABASE_DB_URL or "",
            bucket=config.S3_BUCKET or "", s3_prefix=config.GFW_V4_DAILY_S3_PREFIX,
            public_url_prefix=config.GFW_V4_DAILY_PUBLIC_URL_PREFIX,
            work_root=config.GFW_V4_DAILY_WORK_DIR,
            publish_time=config.GFW_V4_DAILY_PUBLISH_TIME,
        )

    def validate_preflight(self) -> None:
        if not self.enabled or not self.redistribution_approved or not self.single_writer:
            raise GFWV4SourceContractBlocked("v4 daily requires enabled, redistribution approval, and exactly one writer")
        if not self.tier2_evidence_id:
            raise GFWV4SourceContractBlocked("v4 daily requires accepted Tier2 evidence ID")
        if not self.token or not self.db_url or not self.bucket:
            raise GFWV4SourceContractBlocked("v4 daily requires GFW token, DB ledger URL, and S3 bucket")
        parsed_public = urlparse(self.public_url_prefix)
        if (
            self.s3_prefix != "deploy-assets/global-maritime/gfw-hourly/v4"
            or parsed_public.scheme != "https"
            or not parsed_public.netloc
            or parsed_public.query
            or parsed_public.fragment
            or parsed_public.path.rstrip("/") == ""
        ):
            raise GFWV4SourceContractBlocked("v4 daily requires frozen S3 prefix and HTTPS public prefix")
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", self.publish_time):
            raise GFWV4SourceContractBlocked("v4 daily publish time must be HH:MM")


def validate_normalized_v4_daily_source(value: Any) -> dict[str, Any]:
    """Accept only a reviewed, raw-free normalized v4 daily source envelope."""
    if not isinstance(value, dict):
        raise GFWV4SourceContractBlocked("v4 source adapter must return an object")
    if value.get("poc") or value.get("shadow_only") or value.get("production_cutover") is False:
        raise GFWV4SourceContractBlocked("local v4 POC manifests are never publishable")
    if value.get("schema_version") != 1 or value.get("kind") != "gfw_v4_normalized_daily_source":
        raise GFWV4SourceContractBlocked("v4 source adapter has no reviewed normalized schema")
    selected_day = value.get("selected_utc_date")
    if not isinstance(selected_day, str) or not _UTC_DATE.fullmatch(selected_day):
        raise GFWV4SourceContractBlocked("v4 source selected_utc_date must be YYYY-MM-DD")
    if value.get("raw_response_saved") is not False:
        raise GFWV4SourceContractBlocked("v4 production contract forbids raw GFW payload retention")
    for field, prefix in (("presence", _PRESENCE_DATASET), ("fishing_effort", _FISHING_DATASET)):
        section = value.get(field)
        if not isinstance(section, dict):
            raise GFWV4SourceContractBlocked(f"v4 source missing {field} contract")
        if section.get("next_offset_complete") is not True:
            raise GFWV4SourceContractBlocked(f"v4 {field} must prove nextOffset completion")
        if not str(section.get("resolved_dataset_version") or "").startswith(prefix):
            raise GFWV4SourceContractBlocked(f"v4 {field} lacks a resolved dataset version")
        records = section.get("records")
        if isinstance(records, list):
            if any(not isinstance(row, dict) for row in records):
                raise GFWV4SourceContractBlocked(f"v4 {field} records must be normalized rows")
        else:
            path = _descriptor_file(records, field=field)
            if int(records.get("row_count", -1)) != sum(1 for line in path.open(encoding="utf-8") if line.strip()):
                raise GFWV4SourceContractBlocked(f"v4 {field} descriptor row count failed readback")
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _spool_descriptor(path: Path, *, schema: str) -> dict[str, Any]:
    row_count = sum(1 for line in path.open(encoding="utf-8") if line.strip())
    return {
        "kind": "normalized_spool_descriptor", "schema": schema,
        "root": str(path.parent), "path": path.name,
        "bytes": path.stat().st_size, "sha256": _sha256(path), "row_count": row_count,
        "raw_response_saved": False,
    }


def _descriptor_file(value: Any, *, field: str) -> Path:
    if not isinstance(value, dict) or value.get("kind") != "normalized_spool_descriptor":
        raise GFWV4SourceContractBlocked(f"v4 {field} must be an immutable spool descriptor")
    root = Path(str(value.get("root") or "")).resolve()
    relative = Path(str(value.get("path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise GFWV4SourceContractBlocked(f"v4 {field} descriptor path escapes spool root")
    path = (root / relative).resolve()
    if path.parent != root or not path.is_file():
        raise GFWV4SourceContractBlocked(f"v4 {field} descriptor file is missing or escapes spool root")
    if path.stat().st_size != int(value.get("bytes", -1)) or _sha256(path) != value.get("sha256"):
        raise GFWV4SourceContractBlocked(f"v4 {field} descriptor hash/readback failed")
    return path


def _iter_rows(value: Any, *, field: str):
    if isinstance(value, list):
        for row in value:
            if not isinstance(row, dict):
                raise GFWV4SourceContractBlocked(f"v4 {field} rows must be objects")
            yield row
        return
    path = _descriptor_file(value, field=field)
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise GFWV4SourceContractBlocked(f"v4 {field} spool contains a non-object row")
                yield row


def _write_live_finalizer_inputs(normalized: dict[str, Any], *, selected_day: date, root: Path) -> dict[str, Any]:
    """Materialize the raw-free source handoff consumed by schema-4 builder.

    The live adapter intentionally returns records rather than paths.  This
    boundary makes the handoff auditable on disk, and refuses sparse/incomplete
    days instead of manufacturing empty hours or silently accepting bad effort.
    """
    presence = normalized["presence"]
    effort = normalized["fishing_effort"]
    records = presence["records"]
    high_count = int(records.get("row_count", -1)) if isinstance(records, dict) else len(records)
    metrics = presence.get("metrics")
    if not isinstance(metrics, dict) or metrics.get("raw_response_saved") is not False:
        raise GFWV4SourceContractBlocked("normalized HIGH metrics lack raw-free evidence")
    tiles = metrics.get("tiles")
    if not isinstance(tiles, list) or len(tiles) != 42 or any(
        not isinstance(tile, dict) or tile.get("next_offset_complete") is not True for tile in tiles
    ):
        raise GFWV4SourceContractBlocked("normalized HIGH metrics lack complete 42-tile pagination proof")
    resolved = str(presence.get("resolved_dataset_version") or "")
    if not resolved.startswith(_PRESENCE_DATASET):
        raise GFWV4SourceContractBlocked("normalized HIGH dataset lineage is missing")
    input_root = root / "inputs" / selected_day.isoformat()
    input_root.mkdir(parents=True, exist_ok=False)
    high_ndjson = input_root / "presence-high.ndjson"
    with high_ndjson.open("wb") as handle:
        for row in _iter_rows(records, field="presence"):
            handle.write(_canonical_bytes(row) + b"\n")

    # The builder's canonical contract is deliberately narrow.  Keep only the
    # required comparison projection; no raw provider response is retained.
    sqlite_path = input_root / "presence-compare.sqlite"
    connection = sqlite3.connect(sqlite_path)
    connection.execute("""
        CREATE TABLE canonical (
          source TEXT NOT NULL, hour TEXT NOT NULL, vessel_id TEXT NOT NULL,
          cell_lon INTEGER NOT NULL, cell_lat INTEGER NOT NULL, member_json TEXT NOT NULL
        )
    """)
    try:
        from scripts.gfw_east_asia_v4_poc import canonical_cell

        rows_by_key: dict[tuple[str, str], tuple[Any, ...]] = {}
        for row in _iter_rows(records, field="presence"):
            try:
                observed = datetime.fromisoformat(str(row["observed_at"]).replace("Z", "+00:00")).astimezone(timezone.utc)
                hour = observed.strftime("%Y-%m-%dT%H:00:00Z")
                vessel_id = str(row["vessel_id"]).strip()
                cell_lon, cell_lat = canonical_cell(row["longitude"], row["latitude"])
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            if not vessel_id or not hour.startswith(selected_day.isoformat()):
                continue
            member = {field: row.get(field) for field in _POPUP_FIELDS}
            member["vessel_id"] = vessel_id
            rows_by_key.setdefault((hour, vessel_id), (
                "HIGH", hour, vessel_id, cell_lon, cell_lat,
                _canonical_bytes(member).decode("utf-8"),
            ))
        connection.executemany("INSERT INTO canonical VALUES (?,?,?,?,?,?)", rows_by_key.values())
        connection.commit()
        expected_hours = [f"{selected_day.isoformat()}T{hour:02d}:00:00Z" for hour in range(24)]
        observed_hours = {row[0] for row in connection.execute("SELECT DISTINCT hour FROM canonical")}
        if observed_hours != set(expected_hours) or any(
            connection.execute("SELECT COUNT(*) FROM canonical WHERE hour=?", (hour,)).fetchone()[0] <= 0
            for hour in expected_hours
        ):
            raise GFWV4SourceContractBlocked("normalized HIGH source does not cover every UTC hour")
    finally:
        connection.close()

    high_metrics = input_root / "presence-high.metrics.json"
    high_metrics.write_bytes(_canonical_bytes({
        **metrics,
        "raw_response_saved": False,
        "resolved_dataset_versions": [resolved],
        "normalized_row_count": high_count,
    }))

    effort_records = effort["records"]
    quality = (effort.get("metrics") or {}).get("quality")
    effort_version = str(effort.get("resolved_dataset_version") or "")
    if not isinstance(quality, dict) or any(int(quality.get(key, -1)) != 0 for key in (
        "invalid_rows", "negative_hours_rejected", "wrong_day_rows", "boundary_overlap_rows",
    )) or not effort_version.startswith(_FISHING_DATASET):
        raise GFWV4SourceContractBlocked("normalized Fishing Effort lacks an approved quality/lineage proof")
    features = []
    from scripts.gfw_east_asia_v4_poc import _cell_polygon
    for index, row in enumerate(_iter_rows(effort_records, field="fishing_effort")):
        cell = row.get("cell")
        if not isinstance(cell, (list, tuple)) or len(cell) != 2:
            raise GFWV4SourceContractBlocked("normalized Fishing Effort row lacks canonical cell")
        features.append({
            "type": "Feature", "id": f"effort-{cell[0]}-{cell[1]}-{index}",
            "properties": {
                "date": selected_day.isoformat(),
                "apparent_fishing_hours": row.get("apparent_fishing_hours"),
                "facets": row.get("facets", {}),
                "resolved_dataset_version": effort_version,
                "metric_semantics": "apparent_model_derived_fishing_hours",
            }, "geometry": {"type": "Polygon", "coordinates": _cell_polygon((int(cell[0]), int(cell[1])))},
        })
    fishing_asset = input_root / "fishing-effort.geojson.gz"
    fishing_payload = {
        "type": "FeatureCollection",
        "metadata": {
            "schema_version": 1, "date": selected_day.isoformat(),
            "temporal_resolution": "DAILY", "spatial_resolution": "LOW",
            "metric": "apparent_fishing_hours", "unit": "hours",
            "resolved_dataset_version": effort_version, "raw_response_saved": False,
            "quality": quality, "revision_semantics": "dynamic_api_data_may_be_revised",
            "attribution": "Powered by Global Fishing Watch. https://globalfishingwatch.org/",
        }, "features": features,
    }
    with fishing_asset.open("wb") as handle:
        handle.write(gzip.compress(_canonical_bytes(fishing_payload), compresslevel=9, mtime=0))
    return {
        "presence_compare": sqlite_path, "high_ndjson": high_ndjson,
        "high_metrics": high_metrics, "fishing_asset": fishing_asset,
        "fishing_asset_sha256": _sha256(fishing_asset),
        "fishing_lineage": fishing_payload["metadata"],
    }


def _release_json(candidate: Path) -> tuple[dict[str, Any], Path, dict[str, Any], Path]:
    try:
        root = json.loads((candidate / "manifest.json").read_text(encoding="utf-8"))
        release_path = Path(str(root["release_manifest"]["path"]))
        if release_path.is_absolute() or ".." in release_path.parts:
            raise ValueError("release manifest path escapes candidate root")
        release_file = candidate / release_path
        release = json.loads(release_file.read_text(encoding="utf-8"))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise GFWV4SourceContractBlocked("schema4 candidate manifests are unreadable") from exc
    if not isinstance(root, dict) or not isinstance(release, dict):
        raise GFWV4SourceContractBlocked("schema4 candidate manifests must be objects")
    return root, release_path, release, release_file


def _artifact_relative_path(value: Any, *, release_id: str) -> tuple[Path, Path]:
    """Return (root-relative artifact path, nested release path) safely."""
    artifact = Path(str(value or ""))
    prefix = Path("releases") / release_id
    if artifact.is_absolute() or ".." in artifact.parts or len(artifact.parts) <= len(prefix.parts) or artifact.parts[:len(prefix.parts)] != prefix.parts:
        raise GFWV4SourceContractBlocked("artifact path must use the exact releases/<release_id>/ prefix")
    nested = Path(*artifact.parts[len(prefix.parts):])
    if not nested.parts or nested.parts[0] != "tracks":
        raise GFWV4SourceContractBlocked("track artifact path must normalize into tracks/")
    return artifact, nested


def default_v4_spatial_repackager(candidate: Path, *, tier2_evidence_id: str = "") -> Path:
    """Convert every legacy frame to fixed-shard PMTiles in a new root.

    This is kept here as the integration adapter so the spatial worker can
    later expose a batch callable without changing the scheduler contract.
    The existing ``build_spatial_frame`` performs tippecanoe identity/no-drop
    verification and PMTiles readback; absent that callable, this fails closed.
    """
    try:
        from scripts.gfw_v4_spatial_frames import build_spatial_frame
    except (ImportError, AttributeError) as exc:
        raise GFWV4SourceContractBlocked("fixed-shard spatial frame callable is unavailable") from exc
    if not callable(build_spatial_frame):
        raise GFWV4SourceContractBlocked("fixed-shard spatial frame callable is not callable")
    root, release_path, release, _release_file = _release_json(candidate)
    release_id = str(release.get("release_id") or "")
    if not release_id:
        raise GFWV4SourceContractBlocked("schema4 candidate lacks release_id")
    artifacts = release.get("artifacts")
    tracks = release.get("tracks", {}).get("bucket_data")
    if not isinstance(artifacts, list) or not isinstance(tracks, dict):
        raise GFWV4SourceContractBlocked("schema4 candidate lacks nested track frame index")
    frame_assets = [item for item in artifacts if isinstance(item, dict) and item.get("type") == "track_frame_hour"]
    if not frame_assets:
        if not any(isinstance(item, dict) and item.get("type") == "track_frame_pmtiles" for item in artifacts):
            raise GFWV4SourceContractBlocked("schema4 candidate has neither PMTiles nor legacy frames")
        # A formal builder candidate is already immutable and fully gated. Do
        # not rewrite it or manufacture Tier2 truth; only the common assertion
        # may admit it to the publisher.
        _assert_pmtiles_candidate(candidate, tier2_evidence_id=tier2_evidence_id)
        return candidate
    output_root = candidate.parent / f"{candidate.name}.spatial-{uuid.uuid4()}"
    stage = Path(tempfile.mkdtemp(prefix=f".{candidate.name}.spatial-", dir=candidate.parent))
    try:
        shutil.copytree(candidate, stage, dirs_exist_ok=True)
        converted: list[dict[str, Any]] = []
        nested_seen: set[tuple[str, str]] = set()
        for asset in frame_assets:
            counts = asset.get("semantic_counts")
            if not isinstance(counts, dict) or not counts.get("bucket") or not counts.get("observed_at"):
                raise GFWV4SourceContractBlocked("track frame lacks semantic bucket/time identity")
            bucket = str(counts["bucket"])
            old_path, nested_old_path = _artifact_relative_path(asset.get("path"), release_id=release_id)
            if not old_path.name.endswith(".geojson.gz"):
                raise GFWV4SourceContractBlocked("track frame path is not a safe root-relative gzip asset")
            source = stage / old_path
            new_path = old_path.with_suffix("").with_suffix(".pmtiles")
            output = stage / new_path
            if not source.is_file():
                raise GFWV4SourceContractBlocked(f"track frame source is missing: {old_path}")
            meta = build_spatial_frame(
                source=source, output=output, observed_at=str(counts["observed_at"]),
                bucket=bucket, release_root=stage,
            )
            if meta.get("type") != "track_frame_pmtiles" or meta.get("path") != new_path.as_posix():
                raise GFWV4SourceContractBlocked("spatial callable returned an incompatible PMTiles identity")
            _artifact_path, nested_new_path = _artifact_relative_path(meta["path"], release_id=release_id)
            spatial = meta.get("spatial_contract")
            if not isinstance(spatial, dict) or spatial.get("source_feature_count") != spatial.get("decoded_feature_count") or spatial.get("identity_duplicate_count") != 0 or spatial.get("identity_missing_count") != 0:
                raise GFWV4SourceContractBlocked("spatial PMTiles semantic identity/no-drop readback failed")
            source.unlink()
            asset.clear()
            asset.update(meta)
            converted.append(asset)
            bucket_frames = tracks.get(bucket, {}).get("frames") if isinstance(tracks.get(bucket), dict) else None
            if not isinstance(bucket_frames, list):
                raise GFWV4SourceContractBlocked(f"nested frame index missing bucket: {bucket}")
            matches = [frame for frame in bucket_frames if isinstance(frame, dict) and frame.get("path") == nested_old_path.as_posix()]
            if len(matches) != 1:
                raise GFWV4SourceContractBlocked("nested frame index does not match artifact exactly once")
            frame = matches[0]
            frame.update({
                "path": nested_new_path.as_posix(), "format": "pmtiles",
                "content_type": meta["content_type"], "content_encoding": "identity",
                "cache_control": meta["cache_control"], "bytes": meta["bytes"],
                "content_length": meta["content_length"], "sha256": meta["sha256"],
                "etag": meta["etag"], "semantic_counts": meta["semantic_counts"],
                "spatial_contract": meta["spatial_contract"],
            })
            identity = (bucket, str(counts["observed_at"]))
            if identity in nested_seen:
                raise GFWV4SourceContractBlocked("duplicate nested track frame identity")
            nested_seen.add(identity)
        if len(converted) != len(frame_assets) or any(item.get("type") != "track_frame_pmtiles" for item in converted):
            raise GFWV4SourceContractBlocked("spatial conversion dropped or retained a legacy frame")
        release_file = stage / release_path
        release_file.write_bytes(_canonical_bytes(release))
        release_sha = _sha256(release_file)
        root["release_manifest"] = {
            "path": release_path.as_posix(), "bytes": release_file.stat().st_size, "sha256": release_sha,
        }
        (stage / "manifest.json").write_bytes(_canonical_bytes(root))
        stage.replace(output_root)
        return output_root
    except Exception:
        # Keep failed staging evidence for diagnosis, but never expose it as a
        # publishable candidate.
        raise


def _assert_pmtiles_candidate(candidate: Path, *, tier2_evidence_id: str = "") -> None:
    root, _release_path, release, _release_file = _release_json(candidate)
    assets = release.get("artifacts")
    if not isinstance(assets, list):
        raise GFWV4SourceContractBlocked("PMTiles candidate has no artifact manifest")
    frame_assets = [asset for asset in assets if isinstance(asset, dict) and asset.get("type") == "track_frame_pmtiles"]
    if not frame_assets or any(asset.get("type") == "track_frame_hour" for asset in assets):
        raise GFWV4SourceContractBlocked("publisher input must contain PMTiles frames and no legacy gzip frames")
    tracks = release.get("tracks", {}).get("bucket_data")
    if not isinstance(tracks, dict):
        raise GFWV4SourceContractBlocked("PMTiles candidate lacks nested track index")
    for asset in frame_assets:
        path, nested_path = _artifact_relative_path(asset.get("path"), release_id=str(release.get("release_id") or ""))
        if not path.name.endswith(".pmtiles"):
            raise GFWV4SourceContractBlocked("PMTiles artifact path is unsafe")
        local = candidate / path
        if not local.is_file() or local.stat().st_size != int(asset.get("bytes", -1)) or _sha256(local) != asset.get("sha256"):
            raise GFWV4SourceContractBlocked("PMTiles artifact bytes/SHA readback failed")
        counts = asset.get("semantic_counts") or {}
        bucket = str(counts.get("bucket") or "")
        frames = tracks.get(bucket, {}).get("frames") if isinstance(tracks.get(bucket), dict) else None
        if not isinstance(frames, list) or sum(1 for frame in frames if isinstance(frame, dict) and frame.get("path") == nested_path.as_posix()) != 1:
            raise GFWV4SourceContractBlocked("PMTiles nested frame index mismatch")
    if root.get("production_cutover") not in (True, "passed"):
        raise GFWV4SourceContractBlocked("PMTiles candidate has not passed production cutover evidence")
    truth = release.get("release_truth") or {}
    if truth.get("tier2_status") != "passed" or truth.get("readback_status") != "passed":
        raise GFWV4SourceContractBlocked("PMTiles candidate lacks Tier2/readback truth")
    if tier2_evidence_id and truth.get("tier2_evidence_id") not in (None, tier2_evidence_id):
        raise GFWV4SourceContractBlocked("PMTiles candidate Tier2 evidence ID mismatch")
    binding_failure = tier2_binding_failure(release)
    if binding_failure is not None:
        raise GFWV4SourceContractBlocked(f"PMTiles candidate Tier2 evidence binding failed: {binding_failure}")


def _load_tier2_evidence(value: Any) -> dict[str, Any]:
    if isinstance(value, (str, Path)):
        path = Path(value).resolve()
        if not path.is_file() or path.suffix != ".json":
            raise GFWV4SourceContractBlocked("Tier2 evidence must be a JSON file")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise GFWV4SourceContractBlocked("Tier2 evidence JSON is unreadable") from exc
    if not isinstance(value, dict):
        raise GFWV4SourceContractBlocked("Tier2 evidence must be an object")
    if value.get("schema_version") != 1 or value.get("kind") != "gfw_v4_tier2_browser_evidence":
        raise GFWV4SourceContractBlocked("Tier2 evidence schema is not frozen")
    if not re.fullmatch(r"[A-Za-z0-9._:-]{3,128}", str(value.get("evidence_id") or "")):
        raise GFWV4SourceContractBlocked("Tier2 evidence ID is invalid")
    return value


def _validate_tier2_evidence(
    evidence: dict[str, Any], *, core_sha256: str,
    frame_assets: list[dict[str, Any]],
) -> None:
    binding = evidence.get("release_manifest")
    if not isinstance(binding, dict):
        raise GFWV4SourceContractBlocked("Tier2 evidence carries no release_manifest binding")
    declared = binding.get("core_sha256")
    if not isinstance(declared, str) or declared != core_sha256:
        # Binding on whole-manifest bytes cannot survive promotion: recording the
        # verdict changes those bytes, so the evidence would end up naming a
        # manifest that is not the one shipped.  Only the core digest is stable.
        raise GFWV4SourceContractBlocked(
            "Tier2 evidence is not bound to this release manifest's core digest"
        )
    profiles = evidence.get("profiles")
    thresholds = evidence.get("thresholds")
    if not isinstance(profiles, dict) or not isinstance(thresholds, dict) or set(("default", "all")) - set(profiles):
        raise GFWV4SourceContractBlocked("Tier2 evidence requires default and all browser profiles")
    asset_budgets = evidence.get("asset_budgets")
    frame_budget = asset_budgets.get("track_frame_pmtiles") if isinstance(asset_budgets, dict) else None
    if not isinstance(frame_budget, dict) or not isinstance(frame_budget.get("max_bytes"), int) or frame_budget["max_bytes"] <= 0 or not isinstance(frame_budget.get("max_feature_count"), int) or frame_budget["max_feature_count"] < 0:
        raise GFWV4SourceContractBlocked("Tier2 evidence lacks a track-frame byte/feature budget")
    for asset in frame_assets:
        feature_count = (asset.get("semantic_counts") or {}).get("feature_count")
        if not isinstance(feature_count, int) or int(asset.get("bytes", -1)) > frame_budget["max_bytes"] or feature_count > frame_budget["max_feature_count"]:
            raise GFWV4SourceContractBlocked("track frame exceeds the accepted Tier2 asset envelope")
    metric_limits = ("initial_bytes_max", "transfer_bytes_max", "max_heap_bytes_max")
    for profile_name in ("default", "all"):
        profile_thresholds = thresholds.get(profile_name)
        if not isinstance(profile_thresholds, dict):
            raise GFWV4SourceContractBlocked(f"Tier2 evidence lacks {profile_name} frame thresholds")
        for device in ("desktop", "mobile"):
            limit = profile_thresholds.get(device)
            if not isinstance(limit, dict) or any(not isinstance(limit.get(key), int) or limit[key] <= 0 for key in metric_limits):
                raise GFWV4SourceContractBlocked(f"Tier2 evidence lacks positive {profile_name}/{device} thresholds")
            if not isinstance(limit.get("frame_p95_ms_max"), (int, float)) or limit["frame_p95_ms_max"] <= 0 or not isinstance(limit.get("frame_updates_min"), int) or limit["frame_updates_min"] < 96:
                raise GFWV4SourceContractBlocked(f"Tier2 evidence lacks frozen {profile_name}/{device} frame budget")
    for profile_name in ("default", "all"):
        profile = profiles[profile_name]
        if not isinstance(profile, dict) or profile.get("status") != "passed":
            raise GFWV4SourceContractBlocked(f"Tier2 profile {profile_name} is not passed")
        for device in ("desktop", "mobile"):
            run = profile.get(device)
            if not isinstance(run, dict) or run.get("status") != "passed":
                raise GFWV4SourceContractBlocked(f"Tier2 {profile_name}/{device} profile is not passed")
            metrics = run.get("metrics")
            if not isinstance(metrics, dict):
                raise GFWV4SourceContractBlocked(f"Tier2 {profile_name}/{device} metrics are missing")
            limit = thresholds[profile_name][device]
            for metric, limit_key in (("initial_bytes", "initial_bytes_max"), ("transfer_bytes", "transfer_bytes_max"), ("max_heap_bytes", "max_heap_bytes_max")):
                if not isinstance(metrics.get(metric), int) or metrics[metric] < 0 or metrics[metric] > limit[limit_key]:
                    raise GFWV4SourceContractBlocked(f"Tier2 {profile_name}/{device} exceeds {limit_key}")
            if not isinstance(metrics.get("frame_p95_ms"), (int, float)) or metrics["frame_p95_ms"] >= limit["frame_p95_ms_max"] or not isinstance(metrics.get("frame_updates"), int) or metrics["frame_updates"] < limit["frame_updates_min"]:
                raise GFWV4SourceContractBlocked(f"Tier2 {profile_name}/{device} frame budget failed")
            heap = run.get("heap")
            if not isinstance(heap, dict) or heap.get("status") in ("unavailable", "unknown") or heap.get("status") != "passed" or not isinstance(heap.get("bytes"), int) or metrics.get("max_heap_bytes") != heap.get("bytes"):
                raise GFWV4SourceContractBlocked(f"Tier2 {profile_name}/{device} heap evidence is unavailable")
            wire = run.get("wire")
            if not isinstance(wire, dict) or wire.get("status") != "passed" or wire.get("range_206") is not True:
                raise GFWV4SourceContractBlocked(f"Tier2 {profile_name}/{device} lacks HTTP 206 wire proof")


def promote_v4_candidate_with_tier2_evidence(
    candidate: Path, evidence: dict[str, Any] | str | Path, promoted_root: Path,
) -> Path:
    """Create a new immutable promoted root after browser evidence gates pass."""
    candidate = Path(candidate).resolve()
    promoted_root = Path(promoted_root).resolve()
    if candidate == promoted_root or promoted_root.exists():
        raise GFWV4SourceContractBlocked("promoted v4 root must be a new, absent directory")
    root, release_path, release, release_file = _release_json(candidate)
    if root.get("schema_version") != 4 or release.get("schema_version") != 4:
        raise GFWV4SourceContractBlocked("only schema4 candidates may be promoted")
    release_bytes = release_file.read_bytes()
    release_sha256 = hashlib.sha256(release_bytes).hexdigest()
    pointer = root.get("release_manifest") or {}
    if pointer.get("sha256") != release_sha256 or int(pointer.get("bytes", -1)) != len(release_bytes):
        raise GFWV4SourceContractBlocked("candidate root/release bytes or SHA mismatch")
    truth = release.get("release_truth") or {}
    if truth.get("tier1_status") != "passed" or truth.get("readback_status") != "passed":
        raise GFWV4SourceContractBlocked("candidate Tier1/readback truth is not passed")
    evidence_value = _load_tier2_evidence(evidence)
    frame_assets = [asset for asset in release.get("artifacts", []) if isinstance(asset, dict) and asset.get("type") == "track_frame_pmtiles"]
    candidate_core = tier2_core_digest(release)
    _validate_tier2_evidence(
        evidence_value, core_sha256=candidate_core, frame_assets=frame_assets,
    )
    stage = Path(tempfile.mkdtemp(prefix=f".{promoted_root.name}-", dir=promoted_root.parent))
    try:
        shutil.copytree(candidate, stage, dirs_exist_ok=True)
        staged_release = stage / release_path
        promoted_release = dict(release)
        promoted_release["tier2_evidence_id"] = evidence_value["evidence_id"]
        promoted_release["tier2_evidence"] = evidence_value
        promoted_release["release_truth"] = {
            **truth, "tier2_status": "passed", "tier2_evidence_id": evidence_value["evidence_id"],
            "readback_status": "passed", "production_cutover": "passed",
            "root_cutover": "passed_local",
        }
        promoted_release["production_cutover"] = True
        promoted_release["tier2_binding"] = {
            "algorithm": TIER2_BINDING_ALGORITHM, "core_sha256": candidate_core,
        }
        # The candidate's blocker text is now false; leaving it would reproduce
        # the second half of the v8 self-contradiction.
        promoted_release.pop("cutover_blocker", None)
        # Promotion may only rewrite the excluded bookkeeping keys.  If it ever
        # touches a bound field, the evidence would no longer describe what
        # ships -- fail closed rather than publish a manifest that lies.
        promoted_core = tier2_core_digest(promoted_release)
        if promoted_core != candidate_core:
            raise GFWV4SourceContractBlocked(
                "promotion mutated a Tier2-bound manifest field; evidence no longer describes the release"
            )
        staged_release.write_bytes(_canonical_bytes(promoted_release))
        promoted_release_bytes = staged_release.read_bytes()
        promoted_root_manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
        promoted_root_manifest["release_manifest"] = {
            "path": release_path.as_posix(), "bytes": len(promoted_release_bytes),
            "sha256": hashlib.sha256(promoted_release_bytes).hexdigest(),
        }
        promoted_root_manifest["production_cutover"] = True
        (stage / "manifest.json").write_bytes(_canonical_bytes(promoted_root_manifest))
        _assert_pmtiles_candidate(stage, tier2_evidence_id=evidence_value["evidence_id"])
        stage.replace(promoted_root)
        return promoted_root
    except Exception:
        raise


def _strip_local_source_paths(candidate: Path) -> None:
    """Keep source hashes/counts while forbidding local filesystem paths in manifests."""
    root, release_path, release, _release_file = _release_json(candidate)
    proof = release.get("source_proof")
    if not isinstance(proof, dict):
        return
    changed = False
    presence = proof.get("presence")
    if isinstance(presence, dict):
        for name in ("sqlite", "high_ndjson", "metrics"):
            entry = presence.get(name)
            if isinstance(entry, dict) and "path" in entry:
                entry.pop("path")
                changed = True
    fishing = proof.get("fishing_effort")
    if isinstance(fishing, dict) and "source_path" in fishing:
        fishing.pop("source_path")
        changed = True
    if not changed:
        return
    release_file = candidate / release_path
    release_file.write_bytes(_canonical_bytes(release))
    root["release_manifest"] = {
        "path": release_path.as_posix(), "bytes": release_file.stat().st_size,
        "sha256": _sha256(release_file),
    }
    (candidate / "manifest.json").write_bytes(_canonical_bytes(root))


@dataclass
class GFWV4LiveSourceAdapter:
    """Build the raw-free normalized source envelope for one UTC day."""

    token: str
    work_root: Path
    client_factory: Callable[[str], Any] | None = None
    selected_day: date | None = None

    def __call__(self) -> dict[str, Any]:
        from tasks.gfw_v4_live_source import fetch_normalized_v4_daily_source
        from scripts.gfw_hourly_tracks_poc import GFWReportClient

        day = self.selected_day or (
            datetime.now(timezone.utc).date() - timedelta(days=config.GFW_DATA_LAG_DAYS)
        )
        self.work_root.mkdir(parents=True, exist_ok=True)
        work_dir = self.work_root / f"{day.isoformat()}--{uuid.uuid4()}"
        normalized = fetch_normalized_v4_daily_source(
            token=self.token,
            selected_day=day,
            work_dir=work_dir,
            client_factory=self.client_factory or GFWReportClient,
        )
        if isinstance(normalized.get("presence", {}).get("records"), dict):
            # New live-source contract already emits descriptors directly.
            return normalized
        # The source helper's POC API currently returns lists.  Do not let a
        # full day escape this boundary as Python objects: hand downstream only
        # immutable, hash/readback-checked spool descriptors.
        normalized["presence"]["records"] = _spool_descriptor(
            work_dir / "presence-high.ndjson", schema="gfw-v4-presence-high-ndjson",
        )
        effort_path = work_dir / "fishing-effort-normalized.ndjson"
        with effort_path.open("wb") as handle:
            for row in normalized["fishing_effort"]["records"]:
                handle.write(_canonical_bytes(row) + b"\n")
        normalized["fishing_effort"]["records"] = _spool_descriptor(
            effort_path, schema="gfw-v4-fishing-effort-normalized-ndjson",
        )
        return normalized


@dataclass
class DefaultV4CandidateFinalizer:
    """Production builder boundary with an explicit, reviewable input contract.

    The live source is deliberately not allowed to smuggle raw payloads into
    the builder.  A reviewed source/finalizer handoff must provide paths for
    the HIGH comparison SQLite, HIGH metrics, normalized HIGH rows, and the
    approved Fishing Effort artifact.  The existing schema-4 builder then owns
    the immutable staging and local readback gates.
    """

    work_root: Path
    builder: Callable[..., dict[str, Any]] | None = None
    spatial_repackager: Callable[[Path], Path] | None = None
    tier2_evidence_id: str = ""

    def __call__(self, normalized: dict[str, Any]) -> Path:
        inputs = normalized.get("finalizer_inputs")
        if not isinstance(inputs, dict):
            selected_day = date.fromisoformat(str(normalized["selected_utc_date"]))
            try:
                inputs = _write_live_finalizer_inputs(
                    normalized, selected_day=selected_day,
                    root=self.work_root / "finalizer-inputs" / str(uuid.uuid4()),
                )
            except Exception as exc:
                if isinstance(exc, GFWV4SourceContractBlocked):
                    raise
                raise GFWV4SourceContractBlocked(
                    f"v4 source cannot produce reviewed schema4 finalizer inputs: {exc}"
                ) from exc
        required = ("presence_compare", "high_ndjson", "high_metrics", "fishing_asset")
        missing = [name for name in required if not inputs.get(name)]
        if missing:
            raise GFWV4SourceContractBlocked(
                f"v4 schema4 finalizer inputs missing: {', '.join(missing)}"
            )
        selected_day = date.fromisoformat(str(normalized["selected_utc_date"]))
        output_root = self.work_root / "finalized" / f"{selected_day.isoformat()}--{uuid.uuid4()}"
        from scripts.gfw_v4_production_release import build_production_release

        builder = self.builder or build_production_release
        try:
            builder_kwargs = dict(
                output_root=output_root,
                sqlite_path=Path(inputs["presence_compare"]),
                high_ndjson=Path(inputs["high_ndjson"]),
                high_metrics_path=Path(inputs["high_metrics"]),
                fishing_asset=Path(inputs["fishing_asset"]),
                selected_day=selected_day,
            )
            # The legacy builder used a pinned sample SHA.  Production must
            # verify today's derived asset lineage instead; the builder-side
            # adapter accepts these optional keyword arguments during rollout.
            if inputs.get("fishing_asset_sha256"):
                signature = inspect.signature(builder)
                accepts_kwargs = any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
                if not accepts_kwargs and not {
                    "fishing_asset_sha256", "fishing_lineage",
                } <= set(signature.parameters):
                    raise GFWV4SourceContractBlocked(
                        "production builder must accept fishing_asset_sha256 and fishing_lineage"
                    )
                builder_kwargs.update({
                    "fishing_asset_sha256": str(inputs["fishing_asset_sha256"]),
                    "fishing_lineage": inputs.get("fishing_lineage", {}),
                })
            result = builder(**builder_kwargs)
        except Exception as exc:
            raise GFWV4SourceContractBlocked(
                f"v4 schema4 finalizer failed before publication: {exc}"
            ) from exc
        candidate = Path(result.get("output_root", output_root)) if isinstance(result, dict) else output_root
        if not candidate.is_dir() or not (candidate / "manifest.json").is_file():
            raise GFWV4SourceContractBlocked("schema4 finalizer did not return a complete candidate root")
        try:
            _strip_local_source_paths(candidate)
            if self.spatial_repackager is None:
                candidate = default_v4_spatial_repackager(
                    candidate, tier2_evidence_id=self.tier2_evidence_id,
                )
            else:
                candidate = self.spatial_repackager(candidate)
            _assert_pmtiles_candidate(candidate, tier2_evidence_id=self.tier2_evidence_id)
        except Exception as exc:
            if isinstance(exc, GFWV4SourceContractBlocked):
                raise
            raise GFWV4SourceContractBlocked(
                f"v4 spatial PMTiles finalization/readback failed before publication: {exc}"
            ) from exc
        return candidate


def default_v4_publisher_factory(
    release_root: Path, settings: GFWV4DailyPublishSettings,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Lazily construct the DI publisher after the source/build gates pass."""
    from tasks.gfw_hourly_publish import SupabasePublishLedger
    from tasks.gfw_v4_manifest_publisher import V4ManifestPublisher
    from storage.s3 import S3Storage

    return V4ManifestPublisher(
        s3_client=S3Storage().s3,
        ledger=SupabasePublishLedger(settings.db_url),
        bucket=settings.bucket,
        release_root=release_root,
        tier2_evidence_id=settings.tier2_evidence_id,
    )


@dataclass
class GFWV4DailyPublishTask:
    """Explicitly injected future source/publisher boundary; disabled by omission."""

    source_provider: Callable[[], dict[str, Any]] | None = None
    publisher: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    settings: GFWV4DailyPublishSettings | None = None
    asset_toolchain_preflight: Callable[[], Any] = require_gfw_asset_toolchain
    candidate_finalizer: Callable[[dict[str, Any]], Path] | None = None
    publisher_factory: Callable[[Path, GFWV4DailyPublishSettings], Callable[[dict[str, Any]], dict[str, Any]]] | None = None
    name: str = "gfw_v4_daily_publish"

    def complete_preflight(self) -> None:
        active = self.settings or GFWV4DailyPublishSettings.from_config()
        active.validate_preflight()
        self.asset_toolchain_preflight()
        if self.source_provider is None:
            raise GFWV4SourceContractBlocked(
                "v4 daily publish is blocked: live source adapter is not wired; refusing scheduler registration"
            )
        if self.publisher is None and (self.candidate_finalizer is None or self.publisher_factory is None):
            raise GFWV4SourceContractBlocked(
                "v4 daily publish is blocked: schema4 finalizer/publisher are not wired; refusing scheduler registration"
            )

    def run(self) -> dict[str, Any]:
        active = self.settings or GFWV4DailyPublishSettings.from_config()
        # This ordering is intentional: no source provider, ledger or S3 client
        # may be constructed before configuration and binaries are safe.
        self.complete_preflight()
        normalized = validate_normalized_v4_daily_source(self.source_provider())
        if self.publisher is not None:
            return self.publisher(normalized)
        if self.candidate_finalizer is None or self.publisher_factory is None:
            raise GFWV4SourceContractBlocked("v4 daily publish is blocked: candidate finalizer/publisher are unavailable")
        release_root = self.candidate_finalizer(normalized)
        if not isinstance(release_root, Path):
            release_root = Path(release_root)
        publisher = self.publisher_factory(release_root, active)
        if publisher is None:
            raise GFWV4SourceContractBlocked("v4 daily publisher factory returned no publisher")
        return publisher(normalized)
