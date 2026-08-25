from __future__ import annotations

import json

from scripts.gfw_hourly_grid_poc import (
    _finalize_disk_backed,
    _write_points,
    build_hourly_grid_features,
    run_poc,
)


def _point(vessel_id: str, observed_at: str, lon: float, lat: float, **extra):
    return {
        "vessel_id": vessel_id,
        "observed_at": observed_at,
        "longitude": lon,
        "latitude": lat,
        "mmsi": extra.get("mmsi"),
        "ship_name": extra.get("ship_name"),
        "vessel_type": extra.get("vessel_type"),
        "flag": extra.get("flag"),
    }


def test_groups_exact_grid_center_by_utc_hour_and_keeps_all_vessels():
    rows = [
        _point("v-2", "2026-08-15T00:45:00Z", 125.005, 25.005, ship_name="B"),
        _point("v-1", "2026-08-15T08:10:00+08:00", 125.005, 25.005, mmsi="416000001", ship_name="A"),
        # Same vessel and cell within the hour stays one vessel but retains observation count.
        _point("v-1", "2026-08-15T00:30:00Z", 125.005, 25.005, mmsi="416000001", ship_name="A"),
        # Exact grid center means a nearby center is a different feature.
        _point("v-3", "2026-08-15T00:00:00Z", 125.015, 25.005),
    ]
    by_hour, stats = build_hourly_grid_features(rows)
    features = by_hour["2026-08-15T00:00:00Z"]
    assert [feature["geometry"]["coordinates"] for feature in features] == [
        [125.005, 25.005],
        [125.015, 25.005],
    ]
    first = features[0]["properties"]
    assert first["observed_at"] == "2026-08-15T00:00:00Z"
    assert first["vessel_count"] == 2
    vessels = json.loads(first["vessels_json"])
    assert [vessel["vessel_id"] for vessel in vessels] == ["v-1", "v-2"]
    assert set(vessels[0]) == {"vessel_id", "mmsi", "ship_name", "vessel_type", "flag"}
    assert first["grid_lon"] == 125.005
    assert first["grid_lat"] == 25.005
    assert first["coordinate_semantics"] == "GFW_HIGH_grid_cell_center"
    assert stats["feature_count"] == 2


def test_boundary_duplicates_are_removed_with_stable_identity_selection():
    duplicate = _point(
        "v-1", "2026-08-15T00:15:00Z", 125.005, 25.005,
        mmsi="416000001", ship_name="ALPHA", vessel_type="cargo", flag="TWN",
    )
    by_hour, stats = build_hourly_grid_features([duplicate, dict(duplicate)])
    feature = by_hour["2026-08-15T00:00:00Z"][0]
    assert feature["properties"]["vessel_count"] == 1
    assert json.loads(feature["properties"]["vessels_json"]) == [{
        "vessel_id": "v-1",
        "mmsi": "416000001",
        "ship_name": "ALPHA",
        "vessel_type": "cargo",
        "flag": "TWN",
    }]
    assert stats["duplicate_observations"] == 1


def test_same_vessel_hour_position_conflict_keeps_earliest_cell_and_counts_it():
    rows = [
        _point("v-1", "2026-08-15T00:45:00Z", 125.015, 25.005),
        _point("v-1", "2026-08-15T00:10:00Z", 125.005, 25.005),
    ]
    by_hour, stats = build_hourly_grid_features(rows)
    features = by_hour["2026-08-15T00:00:00Z"]
    assert len(features) == 1
    assert features[0]["geometry"]["coordinates"] == [125.005, 25.005]
    assert features[0]["properties"]["vessel_count"] == 1
    assert stats["same_vessel_hour_position_conflicts"] == 1


def test_disk_finalize_writes_minified_hour_files_and_manifest_ready_entries(tmp_path):
    shard_a = tmp_path / "a.ndjson"
    shard_b = tmp_path / "b.ndjson"
    duplicate = _point("v-1", "2026-08-15T00:10:00Z", 125.005, 25.005)
    _write_points(shard_a, [duplicate, _point("v-2", "2026-08-15T00:20:00Z", 125.005, 25.005)])
    _write_points(shard_b, [duplicate, _point("v-3", "2026-08-15T01:00:00Z", 125.015, 25.005)])
    output_dir = tmp_path / "output"
    entries, counts = _finalize_disk_backed(
        [shard_a, shard_b], work_dir=tmp_path, output_dir=output_dir
    )
    assert [entry["observed_at"] for entry in entries] == [
        "2026-08-15T00:00:00Z",
        "2026-08-15T01:00:00Z",
    ]
    assert counts["input_rows"] == 4
    assert counts["unique_observations"] == 3
    assert counts["duplicate_observations"] == 1
    assert entries[0]["path"] == "hours/20260815T00Z.geojson"
    assert entries[0]["cell_count"] == 1
    assert entries[0]["vessel_count"] == 2
    first_path = output_dir / entries[0]["path"]
    raw = first_path.read_text(encoding="utf-8")
    assert "\n" not in raw
    first = json.loads(raw)
    assert first["metadata"]["coordinate_semantics"] == "GFW_HIGH_grid_cell_center"
    assert first["features"][0]["properties"]["vessel_count"] == 2


def test_run_poc_uses_sequential_tiles_and_emits_frontend_manifest(tmp_path):
    class FakeClient:
        def __init__(self):
            self.calls = []
            self.stats = {
                "post_requests": 0,
                "recovery_requests": 0,
                "retries": 0,
                "http_statuses": {},
                "last_rate_limit_headers": {},
            }

        def fetch(self, bbox, start, end):
            self.calls.append((bbox, start, end))
            self.stats["post_requests"] += 1
            index = len(self.calls)
            return {
                "entries": [{
                    "vesselId": f"v-{index}",
                    "date": "2026-08-15T00:00:00Z",
                    "lon": bbox[0] + 0.005,
                    "lat": bbox[1] + 0.005,
                    "shipName": f"SHIP {index}",
                }],
                "nextOffset": 0,
            }, "public-global-presence:v4.0"

    client = FakeClient()
    output_dir = tmp_path / "output"
    summary = run_poc(
        output_dir=output_dir,
        token="fake",
        bbox=(125, 25, 127, 27),
        latest_complete_day="2026-08-15",
        days=1,
        tile_size_degrees=1,
        work_dir=tmp_path / "work",
        client=client,
    )
    assert len(client.calls) == 4
    assert client.calls[0][0] == (125.0, 25.0, 126.0, 26.0)
    assert client.calls[-1][0] == (126.0, 26.0, 127.0, 27.0)
    manifest_raw = (output_dir / "manifest.json").read_text(encoding="utf-8")
    assert "\n" not in manifest_raw
    manifest = json.loads(manifest_raw)
    assert manifest["schema_version"] == 1
    assert manifest["temporal_resolution"] == "HOURLY"
    assert manifest["spatial_resolution"] == "HIGH"
    assert manifest["coordinate_semantics"] == "GFW_HIGH_grid_cell_center"
    assert manifest["position_note"] == "GFW grid-cell center; not a raw AIS position"
    assert manifest["tiling"]["sequential"] is True
    assert manifest["tiling"]["tile_count"] == 4
    assert manifest["counts"]["feature_count"] == 4
    assert len(manifest["hours"]) == 1
    assert summary["work_dir"] is None
    assert not (tmp_path / "work").exists()
    assert "fake" not in manifest_raw


def test_resume_skips_completed_tiles_without_refetching(tmp_path):
    class InterruptingClient:
        def __init__(self):
            self.calls = 0
            self.stats = {
                "post_requests": 0,
                "recovery_requests": 0,
                "retries": 0,
                "http_statuses": {},
                "last_rate_limit_headers": {},
            }

        def fetch(self, bbox, _start, _end):
            self.calls += 1
            self.stats["post_requests"] += 1
            if self.calls == 2:
                raise RuntimeError("intentional stop")
            return {
                "entries": [{
                    "vesselId": f"v-{bbox[0]}",
                    "date": "2026-08-15T00:00:00Z",
                    "lon": bbox[0] + 0.005,
                    "lat": bbox[1] + 0.005,
                }],
                "nextOffset": 0,
            }, "public-global-presence:v4.0"

    work_dir = tmp_path / "work"
    first = InterruptingClient()
    try:
        run_poc(
            output_dir=tmp_path / "output",
            token="fake",
            bbox=(125, 25, 127, 26),
            latest_complete_day="2026-08-15",
            days=1,
            tile_size_degrees=1,
            work_dir=work_dir,
            client=first,
        )
    except RuntimeError as exc:
        assert "resumable normalized shards retained" in str(exc)
    else:
        raise AssertionError("expected intentional interruption")

    second = InterruptingClient()
    second.calls = -100  # Disable the one-time interruption while preserving counting.
    run_poc(
        output_dir=tmp_path / "output",
        token="fake",
        bbox=(125, 25, 127, 26),
        latest_complete_day="2026-08-15",
        days=1,
        tile_size_degrees=1,
        work_dir=work_dir,
        client=second,
    )
    assert second.stats["post_requests"] == 1
