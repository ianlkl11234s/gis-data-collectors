from __future__ import annotations

import gzip
import json
import subprocess
from datetime import date

from scripts.gfw_hourly_browser_assets import (
    _edge_features,
    _singleton_feature,
    build_grid_browser_assets,
    build_track_browser_assets,
    grid_polygon_feature,
)
from scripts.gfw_hourly_tracks_poc import finalize_track_store


def _grid_feature():
    return {
        "type": "Feature", "id": "cell-1",
        "properties": {
            "cell_id": "cell-1", "observed_at": "2026-08-15T00:00:00+00:00",
            "grid_lon": 125.0, "grid_lat": 25.0, "vessel_count": 2,
            "vessels_json": json.dumps([
                {"vessel_id": "v-1", "ship_name": "ONE"},
                {"vessel_id": "v-2", "ship_name": "TWO"},
            ]),
        },
        "geometry": {"type": "Point", "coordinates": [125.0, 25.0]},
    }


def test_grid_browser_asset_is_inferred_polygon_with_lossless_members(tmp_path):
    browser, members = grid_polygon_feature(_grid_feature())
    assert browser["properties"]["geometry_semantics"] == "inferred_0_01_degree_footprint"
    assert browser["geometry"]["coordinates"][0] == [
        [124.995, 24.995], [125.005, 24.995], [125.005, 25.005],
        [124.995, 25.005], [124.995, 24.995],
    ]
    assert [member["vessel_id"] for member in members] == ["v-1", "v-2"]

    source = tmp_path / "source"
    source.mkdir()
    (source / "20260815T00Z.geojson").write_text(json.dumps({
        "type": "FeatureCollection", "features": [_grid_feature()],
    }), encoding="utf-8")
    outputs, counts = build_grid_browser_assets(
        [{"observed_at": "2026-08-15T00:00:00+00:00", "path": "20260815T00Z.geojson"}],
        source_root=source, output_root=tmp_path / "assets", release_id="2026-08-15",
    )
    assert counts == {"hour_count": 1, "cell_count": 1, "member_count": 2}
    entry = outputs[0]
    assert entry["source_layer"] == "gfw_grid"
    assert len(entry["detail_buckets"]) == 16
    assert sum(bucket["vessel_count"] for bucket in entry["detail_buckets"]) == 2
    assert (tmp_path / "assets" / entry["path"]).is_file()
    assert not (tmp_path / "assets" / "grid" / "input").exists()
    bucket = next(item for item in entry["detail_buckets"] if item["vessel_count"] == 2)
    shown = subprocess.run(
        ["/opt/homebrew/bin/pmtiles", "show", str(tmp_path / "assets" / entry["path"])],
        check=True, capture_output=True, text=True,
    )
    assert "gfw_grid" in shown.stdout
    with gzip.open(tmp_path / "assets" / bucket["path"], "rt", encoding="utf-8") as handle:
        detail = json.load(handle)
    assert detail["schema_version"] == 1
    assert detail["release_id"] == "2026-08-15"
    assert detail["entry_count"] == 1
    assert detail["vessel_count"] == 2
    assert detail["entries"]["cell-1"] == {"vessel_count": 2, "vessels": members}
    first_gzip = (tmp_path / "assets" / bucket["path"]).read_bytes()
    build_grid_browser_assets(
        [{"observed_at": "2026-08-15T00:00:00+00:00", "path": "20260815T00Z.geojson"}],
        source_root=source, output_root=tmp_path / "assets", release_id="2026-08-15",
    )
    assert (tmp_path / "assets" / bucket["path"]).read_bytes() == first_gzip


def test_track_browser_assets_keep_edges_singletons_and_hour_frames(tmp_path):
    shard = tmp_path / "points.ndjson"
    rows = [
        {"vessel_id": "v-1", "observed_at": "2026-08-15T00:00:00Z", "longitude": 125, "latitude": 25, "mmsi": "111", "ship_name": "ONE", "vessel_type": "Cargo", "flag": "TW"},
        {"vessel_id": "v-1", "observed_at": "2026-08-15T01:00:00Z", "longitude": 125.1, "latitude": 25, "mmsi": "111", "ship_name": "ONE", "vessel_type": "Cargo", "flag": "TW"},
        {"vessel_id": "v-2", "observed_at": "2026-08-15T00:00:00Z", "longitude": 126, "latitude": 26, "mmsi": "222", "ship_name": "TWO", "vessel_type": "Tanker", "flag": "JP"},
    ]
    shard.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    store = finalize_track_store([shard], work_dir=tmp_path, gap_hours=2, max_speed_knots=80)
    try:
        root = tmp_path / "assets"
        days, frames, counts = build_track_browser_assets(
            store, start=date(2026, 8, 15), latest=date(2026, 8, 15), output_root=root,
            release_id="2026-08-15",
        )
    finally:
        store.close()
    assert counts["day_count"] == 1
    assert days[0]["source_layers"] == {"edges": "gfw_track_edges", "singletons": "gfw_track_singletons"}
    assert days[0]["edge_count"] == 1
    assert days[0]["singleton_count"] == 1
    assert len(days[0]["detail_buckets"]) == 16
    assert len(frames) == 24
    assert counts["frame_count"] == 24
    assert not (root / "tracks" / "input").exists()
    assert (root / days[0]["path"]).is_file()
    shown = subprocess.run(
        ["/opt/homebrew/bin/pmtiles", "show", str(root / days[0]["path"])],
        check=True, capture_output=True, text=True,
    )
    assert "gfw_track_edges" in shown.stdout and "gfw_track_singletons" in shown.stdout
    first = next(entry for entry in frames if entry["observed_at"] == "2026-08-15T00:00:00+00:00")
    assert first["content_encoding"] == "gzip"
    with gzip.open(root / first["path"], "rt", encoding="utf-8") as handle:
        frame_payload = json.load(handle)
    assert frame_payload["metadata"] == {
        "schema_version": 1, "release_id": "2026-08-15",
        "observed_at": "2026-08-15T00:00:00+00:00", "entry_count": 2,
    }
    frame = frame_payload["features"]
    v1 = next(feature for feature in frame if feature["properties"]["vessel_id"] == "v-1")
    assert v1["properties"]["observed_epoch"] == 1786752000
    assert v1["properties"]["to_epoch"] == 1786755600
    assert v1["properties"]["to_at"] == "2026-08-15T01:00:00+00:00"
    assert v1["properties"]["mmsi"] == "111"
    assert v1["properties"]["ship_name"] == "ONE"
    assert v1["properties"]["ship_type_bucket"] == "cargo"
    empty = next(entry for entry in frames if entry["observed_at"] == "2026-08-15T23:00:00+00:00")
    with gzip.open(root / empty["path"], "rt", encoding="utf-8") as handle:
        assert json.load(handle)["metadata"]["entry_count"] == 0

    detail_bucket = next(item for item in days[0]["detail_buckets"] if item["entry_count"] > 0)
    with gzip.open(root / detail_bucket["path"], "rt", encoding="utf-8") as handle:
        detail = json.load(handle)
    assert detail["schema_version"] == 1
    assert detail["release_id"] == "2026-08-15"
    assert detail["entry_count"] == len(detail["entries"])
    assert detail["point_count"] == sum(item["point_count"] for item in detail["entries"].values())


def test_track_edges_overlap_both_utc_day_partitions():
    feature = {
        "type": "Feature",
        "properties": {
            "track_id": "t-1", "vessel_id": "v-1", "vessel_type": "Cargo",
            "observed_times": ["2026-08-15T23:00:00+00:00", "2026-08-16T00:00:00+00:00"],
        },
        "geometry": {"type": "LineString", "coordinates": [[125, 25], [125.1, 25]]},
    }
    first = list(_edge_features(feature, display_day="2026-08-15"))
    second = list(_edge_features(feature, display_day="2026-08-16"))
    assert len(first) == len(second) == 1
    assert first[0]["properties"]["display_date"] == "2026-08-15"
    assert second[0]["properties"]["display_date"] == "2026-08-16"
    assert first[0]["properties"]["ship_type_bucket"] == "cargo"


def test_singleton_feature_has_epoch_bucket_display_date_and_popup_identifiers():
    feature = {
        "type": "Feature",
        "properties": {
            "track_id": "singleton-1", "vessel_id": "v-1", "vessel_type": "Tanker",
            "start_at": "2026-08-15T00:00:00+00:00",
        },
        "geometry": {"type": "Point", "coordinates": [125, 25]},
    }
    singleton = _singleton_feature(feature, display_day="2026-08-15")
    assert singleton is not None
    assert singleton["properties"] == {
        "track_id": "singleton-1", "vessel_id": "v-1", "vessel_type": "Tanker",
        "ship_type_bucket": "tanker", "display_date": "2026-08-15",
        "observed_at": "2026-08-15T00:00:00+00:00", "observed_epoch": 1786752000,
    }
