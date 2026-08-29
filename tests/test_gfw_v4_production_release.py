from __future__ import annotations

from datetime import date
import gzip
import hashlib
import json

import pytest

import re

from scripts.gfw_v4_production_release import (
    ProductionReleaseError,
    _filter_track_rows,
    _is_non_vessel,
    _prefix_track_asset_paths,
    _rewrite_nested_frame,
    _validate_fishing_asset,
    build_production_release,
    validate_schema4_release_manifest,
    validate_track_frame_artifact,
)


def _spatial_frame_metadata(bucket: str, observed_at: str) -> dict:
    """Exactly the key set scripts/gfw_v4_spatial_frames.build_spatial_frame returns."""
    sha = "a" * 64
    return {
        "path": f"tracks/{bucket}/tracks/frames/20260821T00Z.pmtiles",
        "type": "track_frame_pmtiles", "bytes": 1234, "content_length": 1234,
        "sha256": sha, "etag": f'"{sha}"',
        "content_type": "application/octet-stream", "content_encoding": "identity",
        "cache_control": "public,max-age=604800,s-maxage=604800,immutable",
        "semantic_counts": {"observed_at": observed_at, "bucket": bucket, "feature_count": 14185},
        "spatial_contract": {
            "fixed_zoom": 6, "source_feature_count": 14185, "decoded_feature_count": 14185,
            "identity_duplicate_count": 0, "identity_missing_count": 0,
        },
    }


def _frame_artifact() -> dict:
    sha = "b" * 64
    return {
        "path": "releases/2026-08-21__public-global-presence-v4.0/tracks/other/tracks/frames/20260821T00Z.pmtiles",
        "type": "track_frame_pmtiles", "bytes": 10, "content_length": 10, "sha256": sha,
        "etag": f'"{sha}"', "content_type": "application/octet-stream",
        "content_encoding": "identity",
        "cache_control": "public,max-age=604800,s-maxage=604800,immutable",
        "semantic_counts": {"observed_at": "2026-08-21T00:00:00+00:00", "bucket": "other", "feature_count": 7},
        "spatial_contract": {
            "fixed_zoom": 6, "source_feature_count": 7, "decoded_feature_count": 7,
            "identity_duplicate_count": 0, "identity_missing_count": 0,
        },
    }


def test_top_level_track_frame_artifact_requires_spatial_contract() -> None:
    """Regression: v9 shipped track_frame artifacts with no spatial_contract.

    _asset() rebuilds the top-level record from scratch, so the identity/no-drop
    proof computed during the PMTiles build has to be carried across explicitly.
    Mirrors install-gfw-v4-local-release.sh's frame checks.
    """
    validate_track_frame_artifact(_frame_artifact())

    for mutate, match in (
        (lambda a: a.pop("spatial_contract"), "fixed-z6 spatial_contract"),
        (lambda a: a.update(spatial_contract=None), "fixed-z6 spatial_contract"),
        (lambda a: a["spatial_contract"].update(fixed_zoom=7), "fixed-z6 spatial_contract"),
        (lambda a: a["spatial_contract"].update(decoded_feature_count=6), "identity/no-drop"),
        (lambda a: a["spatial_contract"].update(identity_duplicate_count=1), "identity/no-drop"),
        (lambda a: a["spatial_contract"].update(identity_missing_count=1), "identity/no-drop"),
        (lambda a: a["semantic_counts"].update(feature_count=6), "identity/no-drop"),
        (lambda a: a.update(content_encoding="gzip"), "identity octet-stream"),
    ):
        asset = _frame_artifact()
        mutate(asset)
        with pytest.raises(ProductionReleaseError, match=match):
            validate_track_frame_artifact(asset)


def test_release_manifest_validation_rejects_frame_without_spatial_contract() -> None:
    """The builder's own manifest gate must catch it before anything is written."""
    frame = _frame_artifact()
    frame.pop("spatial_contract")
    manifest = {
        "schema_version": 4, "release_id": "2026-08-21__public-global-presence-v4.0",
        "selected_utc_date": "2026-08-21", "bbox": [115.9, 20.3, 134.7, 36.5],
        "source_dataset_id": "public-global-presence",
        "resolved_dataset_version": "public-global-presence:v4.0",
        "days": [], "grid": {}, "fishing_effort": {},
        "layer_separation": {"grid": "gfwHourlyGrid", "tracks": "gfwHourlyTracks", "fishing_effort": "gfwFishingEffort", "dark_vessels": "gfwDarkVessels"},
        "tracks": {"buckets": ["FISHING", "CARGO", "PASSENGER", "CARRIER", "OTHER", "UNKNOWN"], "default_buckets": ["FISHING", "CARGO", "PASSENGER"]},
        "artifacts": [frame], "release_truth": {},
    }
    with pytest.raises(ProductionReleaseError, match="fixed-z6 spatial_contract"):
        validate_schema4_release_manifest(manifest)


def test_nested_track_frame_keeps_frozen_consumer_fields() -> None:
    """Regression: the PMTiles rewrite must not strip the nested-frame contract.

    A v9 candidate was rejected by the frontend's frozen parser
    ("invalid frozen spatial release") because the nested frames had been
    overwritten with the artifact-shaped return of build_spatial_frame, which
    carries no format/observed_at/features.
    """
    observed_at = "2026-08-21T00:00:00+00:00"
    frame = {
        "path": "tracks/other/tracks/frames/20260821T00Z.geojson.gz",
        "observed_at": observed_at, "format": "geojson.gz", "features": 14185,
    }
    metadata = _spatial_frame_metadata("other", observed_at)
    _rewrite_nested_frame(frame, metadata)

    # Mirror src/data/gfwV4SpatialTracksLoader.ts validBucketData().
    assert frame["format"] == "pmtiles"
    assert frame["content_encoding"] == "identity"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", frame["observed_at"])
    assert frame["observed_at"][:10] == "2026-08-21"
    assert frame["path"].startswith("tracks/other/tracks/frames/")
    assert frame["path"].endswith(".pmtiles")
    # v8 nested shape is the ground truth: 13 keys, and no artifact-only "type".
    assert "type" not in frame
    assert set(frame) == {
        "bytes", "cache_control", "content_encoding", "content_length", "content_type",
        "etag", "features", "format", "observed_at", "path", "semantic_counts",
        "sha256", "spatial_contract",
    }
    assert frame["features"] == metadata["semantic_counts"]["feature_count"]
    assert frame["sha256"] == metadata["sha256"] and frame["bytes"] == metadata["bytes"]


def test_formal_taxonomy_keeps_other_and_unknown_separate() -> None:
    grouped, observed = _filter_track_rows([
        {"vessel_type": "FISHING"}, {"vessel_type": "CARGO"},
        {"vessel_type": "PASSENGER"}, {"vessel_type": "CARRIER"},
        {"vessel_type": "BUNKER"}, {"vessel_type": "NA"},
        {"vessel_type": "GEAR"},
    ])
    assert [len(grouped[key]) for key in ("fishing", "cargo", "passenger", "carrier", "other", "unknown")] == [1, 1, 1, 1, 1, 1]
    assert observed["GEAR"] == 1


def test_formal_taxonomy_refuses_tanker_instead_of_remapping() -> None:
    with pytest.raises(ProductionReleaseError, match="TANKER"):
        _filter_track_rows([{"vessel_type": "TANKER"}])


def test_grid_non_vessel_gate_only_excludes_explicit_gear_or_fad() -> None:
    assert _is_non_vessel({"vessel_type": "GEAR"})
    assert _is_non_vessel({"geartype": "FAD"})
    assert not _is_non_vessel({"vessel_type": "FISHING", "geartype": "SET_GILLNETS"})


def test_existing_immutable_output_refuses_before_source_or_tool_access(tmp_path) -> None:
    output = tmp_path / "already-there"
    output.mkdir()
    with pytest.raises(FileExistsError, match="already exists"):
        build_production_release(
            output_root=output,
            sqlite_path=tmp_path / "missing.sqlite",
            high_ndjson=tmp_path / "missing.ndjson",
            high_metrics_path=tmp_path / "missing.metrics.json",
            fishing_asset=tmp_path / "missing.geojson.gz",
            selected_day=date(2026, 8, 21),
        )


def test_platform_schema4_contract_requires_frozen_paths_and_asset_types() -> None:
    manifest = {
        "schema_version": 4,
        "release_id": "2026-08-21__public-global-presence-v4.0",
        "selected_utc_date": "2026-08-21", "bbox": [115.9, 20.3, 134.7, 36.5],
        "source_dataset_id": "gfw:public-global-presence",
        "resolved_dataset_version": "public-global-presence:v4.0",
        "days": [], "grid": {}, "tracks": {}, "fishing_effort": {},
        "layer_separation": {"grid": "gfwHourlyGrid", "tracks": "gfwHourlyTracks", "fishing_effort": "gfwFishingEffort", "dark_vessels": "gfwDarkVessels"},
        "artifacts": [{
            "path": "releases/2026-08-21__public-global-presence-v4.0/grid/hours/20260821T00Z.pmtiles",
            "type": "grid_hour_pmtiles",
        }],
        "release_truth": {},
    }
    manifest["tracks"] = {"buckets": ["FISHING", "CARGO", "PASSENGER", "CARRIER", "OTHER", "UNKNOWN"], "default_buckets": ["FISHING", "CARGO", "PASSENGER"]}
    validate_schema4_release_manifest(manifest)
    manifest["artifacts"][0]["type"] = "grid_detail"
    with pytest.raises(ProductionReleaseError, match="asset type"):
        validate_schema4_release_manifest(manifest)
    manifest["artifacts"][0]["type"] = "track_frame_hour"
    with pytest.raises(ProductionReleaseError, match="asset type"):
        validate_schema4_release_manifest(manifest)


def test_fishing_asset_requires_daily_hash_and_exact_lineage(tmp_path) -> None:
    lineage = {
        "date": "2026-08-21", "resolved_dataset_version": "public-global-fishing-effort:v4.0",
        "quality": {"invalid_rows": 0, "negative_hours_rejected": 0, "wrong_day_rows": 0, "boundary_overlap_rows": 0},
    }
    asset = tmp_path / "fishing.geojson.gz"
    asset.write_bytes(gzip.compress(json.dumps({"type": "FeatureCollection", "metadata": lineage, "features": []}).encode(), mtime=0))
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    verified = _validate_fishing_asset(asset, selected_day=date(2026, 8, 21), fishing_asset_sha256=digest, fishing_lineage=lineage)
    assert verified["sha256"] == digest
    with pytest.raises(ProductionReleaseError, match="SHA-256"):
        _validate_fishing_asset(asset, selected_day=date(2026, 8, 21), fishing_asset_sha256="0" * 64, fishing_lineage=lineage)
    with pytest.raises(ProductionReleaseError, match="lineage"):
        _validate_fishing_asset(asset, selected_day=date(2026, 8, 21), fishing_asset_sha256=digest, fishing_lineage={**lineage, "date": "2026-08-20"})


def test_bucket_local_track_paths_become_release_root_relative() -> None:
    days, frames = _prefix_track_asset_paths(
        bucket="carrier",
        days=[{"path": "tracks/days/2026-08-21.pmtiles", "detail_buckets": [{"path": "tracks/details/2026-08-21/a.json.gz"}]}],
        frames=[{"path": "tracks/frames/20260821T00Z.geojson.gz"}],
    )
    assert days[0]["path"] == "tracks/carrier/tracks/days/2026-08-21.pmtiles"
    assert days[0]["detail_buckets"][0]["path"] == "tracks/carrier/tracks/details/2026-08-21/a.json.gz"
    assert frames[0]["path"] == "tracks/carrier/tracks/frames/20260821T00Z.geojson.gz"
