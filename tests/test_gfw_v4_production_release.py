from __future__ import annotations

from datetime import date
import gzip
import hashlib
import json

import pytest

from scripts.gfw_v4_production_release import (
    ProductionReleaseError,
    _filter_track_rows,
    _is_non_vessel,
    _prefix_track_asset_paths,
    _validate_fishing_asset,
    build_production_release,
    validate_schema4_release_manifest,
)


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
