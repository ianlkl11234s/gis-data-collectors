from __future__ import annotations

import json

import pytest

from scripts.gfw_hourly_tracks_poc import (
    GFWReportClient,
    IncompleteReportError,
    _finalize_disk_backed,
    finalize_track_store,
    _load_or_create_manifest,
    build_track_segments,
    cap_features,
    make_tiles,
    run_poc,
    validate_feature_time_contract,
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


def test_make_tiles_covers_bbox_deterministically():
    bbox = (122.434, 23.22953, 132.85274, 34.35812)
    tiles = make_tiles(bbox, tile_size_degrees=3.0)
    assert len(tiles) == 16
    assert tiles[0].tile_id == "r00c00"
    assert tiles[0].bbox == (122.434, 23.22953, 125.434, 26.22953)
    assert tiles[-1].bbox == (131.434, 32.22953, 132.85274, 34.35812)


def test_resume_manifest_accepts_json_tuple_round_trip(tmp_path):
    signature = {"bbox": (122.434, 23.22953, 132.85274, 34.35812)}
    tiles = make_tiles(signature["bbox"], tile_size_degrees=20)
    first = _load_or_create_manifest(tmp_path, signature, tiles)
    second = _load_or_create_manifest(tmp_path, signature, tiles)
    assert tuple(second["signature"]["bbox"]) == signature["bbox"]
    assert second["created_at"] == first["created_at"]


def test_tracks_split_on_gap_or_implausible_speed_and_keep_identity():
    rows = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.0, 25.0, mmsi="416000001", ship_name="TEST"),
        _point("v-1", "2026-08-15T01:00:00Z", 125.1, 25.0, mmsi="416000001", ship_name="TEST"),
        # > 2 h gap cuts the first segment.
        _point("v-1", "2026-08-15T04:00:00Z", 125.2, 25.0, mmsi="416000001", ship_name="TEST"),
        _point("v-1", "2026-08-15T05:00:00Z", 125.3, 25.0, mmsi="416000001", ship_name="TEST"),
        # About 550 nm in one hour; > 80 kn cuts again and the isolated point is a Point node.
        _point("v-1", "2026-08-15T06:00:00Z", 135.0, 25.0, mmsi="416000001", ship_name="TEST"),
    ]
    features, stats = build_track_segments(rows, gap_hours=2.0, max_speed_knots=80.0)
    assert len(features) == 3
    assert [feature["properties"]["point_count"] for feature in features] == [2, 2, 1]
    assert features[0]["properties"]["mmsi"] == "416000001"
    assert features[0]["properties"]["ship_name"] == "TEST"
    assert features[0]["properties"]["approximate"] is True
    assert features[0]["properties"]["source_dataset"] == "public-global-presence:latest"
    assert features[0]["properties"]["start_at"] == "2026-08-15T00:00:00+00:00"
    assert features[0]["properties"]["end_at"] == "2026-08-15T01:00:00+00:00"
    assert features[0]["properties"]["observed_times"] == [
        "2026-08-15T00:00:00+00:00",
        "2026-08-15T01:00:00+00:00",
    ]
    assert len(features[0]["properties"]["observed_times"]) == len(
        features[0]["geometry"]["coordinates"]
    )
    assert "start_time" not in features[0]["properties"]
    assert "end_time" not in features[0]["properties"]
    assert features[0]["geometry"]["type"] == "LineString"
    assert features[-1]["geometry"]["type"] == "Point"
    assert features[-1]["properties"]["node_type"] == "singleton"
    assert stats["gap_splits"] == 1
    assert stats["speed_splits"] == 1


def test_same_vessel_hour_conflict_is_resolved_deterministically():
    rows = [
        _point("v-1", "2026-08-15T00:30:00Z", 125.02, 25.0),
        _point("v-1", "2026-08-15T00:15:00Z", 125.01, 25.0),
        _point("v-1", "2026-08-15T01:00:00Z", 125.1, 25.0),
    ]
    features, stats = build_track_segments(rows)
    assert features[0]["geometry"]["coordinates"][0] == [125.01, 25.0]
    assert features[0]["properties"]["observed_times"] == [
        "2026-08-15T00:15:00+00:00",
        "2026-08-15T01:00:00+00:00",
    ]
    assert stats["same_hour_conflicts"] == 1


def test_feature_time_contract_fails_closed_on_misaligned_or_unsorted_times():
    feature = {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": [[125, 25], [126, 26]]},
        "properties": {
            "start_at": "2026-08-15T00:00:00+00:00",
            "end_at": "2026-08-15T01:00:00+00:00",
            "observed_times": ["2026-08-15T00:00:00+00:00"],
        },
    }
    with pytest.raises(ValueError, match="align 1:1"):
        validate_feature_time_contract(feature)

    feature["properties"]["observed_times"] = [
        "2026-08-15T01:00:00+00:00",
        "2026-08-15T00:00:00+00:00",
    ]
    feature["properties"]["start_at"] = "2026-08-15T01:00:00+00:00"
    feature["properties"]["end_at"] = "2026-08-15T00:00:00+00:00"
    with pytest.raises(ValueError, match="strictly increasing"):
        validate_feature_time_contract(feature)

    feature["properties"]["observed_times"] = [
        "2026-08-15T00:00:00",
        "2026-08-15T01:00:00",
    ]
    feature["properties"]["start_at"] = "2026-08-15T00:00:00"
    feature["properties"]["end_at"] = "2026-08-15T01:00:00"
    with pytest.raises(ValueError, match="ISO UTC"):
        validate_feature_time_contract(feature)


def test_display_cap_is_deterministic_and_reports_omissions():
    features = [
        {"type": "Feature", "properties": {"track_id": "b", "point_count": 4}, "geometry": {"type": "LineString", "coordinates": [[0, 0]] * 4}},
        {"type": "Feature", "properties": {"track_id": "a", "point_count": 4}, "geometry": {"type": "LineString", "coordinates": [[0, 0]] * 4}},
        {"type": "Feature", "properties": {"track_id": "c", "point_count": 2}, "geometry": {"type": "LineString", "coordinates": [[0, 0]] * 2}},
    ]
    kept, stats = cap_features(features, max_features=2, max_points=8)
    assert [feature["properties"]["track_id"] for feature in kept] == ["a", "b"]
    assert stats == {"candidate_features": 3, "displayed_features": 2, "candidate_points": 10, "displayed_points": 8, "cap_applied": True}


def test_disk_backed_finalize_keeps_large_shard_full_fidelity(tmp_path):
    shard = tmp_path / "tile.points.ndjson"
    with shard.open("w", encoding="utf-8") as handle:
        for vessel_number in range(400):
            for hour in range(4):
                row = _point(
                    f"v-{vessel_number:04d}",
                    f"2026-08-15T{hour:02d}:00:00+00:00",
                    125 + vessel_number / 10000 + hour / 1000,
                    25.0,
                )
                handle.write(json.dumps(row) + "\n")
        # Cross-tile exact duplicate is removed by SQLite primary key.
        handle.write(json.dumps(_point("v-0000", "2026-08-15T00:00:00+00:00", 125.0, 25.0)) + "\n")
    displayed, segment_stats, cap_stats, candidate_vessels, displayed_vessels, tile_rows = _finalize_disk_backed(
        [shard],
        work_dir=tmp_path,
        gap_hours=2,
        max_speed_knots=80,
        max_features=25,
        max_points=100,
    )
    assert tile_rows == 1601
    assert segment_stats["valid_unique_points"] == 1600
    assert segment_stats["duplicate_rows"] == 1
    assert candidate_vessels == 400
    assert displayed_vessels == 400
    assert len(displayed) == 400
    assert cap_stats["displayed_points"] == 1600
    assert cap_stats["cap_applied"] is False
    assert cap_stats["omitted_by_display_cap"] == 0


def test_run_poc_emits_frontend_summary_aliases_and_canonical_track_fields(tmp_path):
    class FakeClient:
        stats = {
            "post_requests": 0,
            "recovery_requests": 0,
            "retries": 0,
            "http_statuses": {},
            "last_rate_limit_headers": {},
        }

        def fetch(self, _bbox, _start, _end):
            self.stats["post_requests"] += 1
            return {
                "entries": [
                    {"vesselId": "v-1", "date": "2026-08-15T00:00:00Z", "lon": 125.01, "lat": 25.01},
                    {"vesselId": "v-1", "date": "2026-08-15T01:00:00Z", "lon": 125.02, "lat": 25.01},
                ],
                "nextOffset": 0,
            }, "public-global-presence:v4.0"

    output = tmp_path / "tracks.geojson"
    run_poc(
        output=output,
        token="fake",
        bbox=(125, 25, 125.1, 25.1),
        latest_complete_day="2026-08-15",
        days=1,
        work_dir=tmp_path / "work",
        client=FakeClient(),
    )
    collection = json.loads(output.read_text(encoding="utf-8"))
    metadata = collection["metadata"]
    assert metadata["row_count"] == 2
    assert metadata["vessel_count"] == 1
    assert metadata["segment_count"] == 1
    assert metadata["displayed_segment_count"] == 1
    assert metadata["schema_version"] == 3
    assert metadata["track_contract"] == {
        "vertex_time_property": "observed_times",
        "vertex_time_alignment": "one_to_one_with_geometry_coordinates",
        "vertex_time_order": "strictly_increasing_utc",
    }
    assert metadata["counts"]["candidate_features"] == 1
    assert metadata["counts"]["eligible_segment_count"] == metadata["counts"]["published_segment_count"]
    assert metadata["counts"]["canonical_points"] == (
        metadata["counts"]["eligible_segment_points"]
        + metadata["counts"]["singleton_node_points"]
    )
    assert metadata["counts"]["cap_applied"] is False
    assert metadata["counts"]["omitted_by_display_cap"] == 0
    properties = collection["features"][0]["properties"]
    assert set(("start_at", "end_at", "source_dataset")) <= set(properties)
    assert properties["observed_times"] == [
        "2026-08-15T00:00:00+00:00",
        "2026-08-15T01:00:00+00:00",
    ]
    assert len(properties["observed_times"]) == len(collection["features"][0]["geometry"]["coordinates"])
    assert "start_time" not in properties and "end_time" not in properties


def test_run_poc_can_publish_partitioned_release_without_monolith(tmp_path):
    class FakeClient:
        stats = {
            "post_requests": 0,
            "recovery_requests": 0,
            "retries": 0,
            "http_statuses": {},
            "last_rate_limit_headers": {},
        }

        def fetch(self, _bbox, _start, _end):
            self.stats["post_requests"] += 1
            return {
                "entries": [
                    {"vesselId": "v-1", "date": "2026-08-15T00:00:00Z", "lon": 125.01, "lat": 25.01},
                    {"vesselId": "v-1", "date": "2026-08-15T01:00:00Z", "lon": 125.02, "lat": 25.01},
                ],
                "nextOffset": 0,
            }, "public-global-presence:v4.0"

    release_root = tmp_path / "release"
    summary = run_poc(
        output_dir=release_root,
        token="fake",
        bbox=(125, 25, 125.1, 25.1),
        latest_complete_day="2026-08-15",
        days=1,
        work_dir=tmp_path / "work-partitioned",
        client=FakeClient(),
    )
    assert summary["output"] is None
    assert summary["partition_release"]["release_id"] == "2026-08-15"
    manifest = json.loads((release_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["latest_complete_date"] == "2026-08-15"
    assert manifest["days"][0]["path"] == (
        "releases/2026-08-15/days/2026-08-15.geojson"
    )


def test_sqlite_track_store_streams_tracks_and_canonical_singletons(tmp_path):
    shard = tmp_path / "tile.points.ndjson"
    shard.write_text("\n".join(map(json.dumps, [
        _point("v-1", "2026-08-15T00:00:00Z", 125, 25),
        _point("v-1", "2026-08-15T01:00:00Z", 125.1, 25),
        _point("v-2", "2026-08-15T00:00:00Z", 126, 26),
        # Same vessel/hour deterministically keeps the earlier coordinate order.
        _point("v-2", "2026-08-15T00:30:00Z", 126.1, 26),
    ])) + "\n", encoding="utf-8")
    store = finalize_track_store(
        [shard], work_dir=tmp_path, gap_hours=2, max_speed_knots=80,
    )
    try:
        tracks = list(store.iter_tracks())
        singletons = list(store.iter_singleton_nodes())
        assert len(tracks) == 1
        assert len(singletons) == 1
        assert singletons[0]["geometry"]["type"] == "Point"
        assert [feature["properties"]["vessel_id"] for feature in store.iter_features()] == ["v-1", "v-2"]
        counts = store.counts()
        assert counts["eligible_segment_count"] == counts["published_segment_count"] == 1
        assert counts["canonical_points"] == counts["eligible_segment_points"] + counts["singleton_node_points"]
        assert counts["cap_applied"] is False
        assert counts["omitted_by_display_cap"] == 0
    finally:
        store.close()


class _Response:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = str(payload)

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def test_report_uses_hourly_high_vessel_id_and_recovers_524(monkeypatch):
    session = _Session([
        _Response(524, {"error": "timeout"}),
        _Response(200, {"status": "running"}),
        _Response(200, {"entries": [{"vesselId": "v-1", "lat": 25, "lon": 125}], "nextOffset": 0}, {"x-datasets": "public-global-presence:v4.0"}),
    ])
    monkeypatch.setattr("scripts.gfw_hourly_tracks_poc.time.sleep", lambda *_args: None)
    client = GFWReportClient("secret", session=session, max_polls=3)
    payload, resolved = client.fetch((124, 24, 126, 26), "2026-08-15", "2026-08-22")
    assert payload["entries"][0]["vesselId"] == "v-1"
    assert resolved == "public-global-presence:v4.0"
    post = session.calls[0]
    assert post[0] == "POST"
    assert post[2]["params"]["temporal-resolution"] == "HOURLY"
    assert post[2]["params"]["spatial-resolution"] == "HIGH"
    assert post[2]["params"]["group-by"] == "VESSEL_ID"
    assert session.calls[1][1].endswith("/last-report")
    assert client.stats["post_requests"] == 1
    assert client.stats["recovery_requests"] == 2


def test_nonzero_next_offset_fails_closed():
    session = _Session([_Response(200, {"entries": [], "nextOffset": 100})])
    client = GFWReportClient("secret", session=session)
    with pytest.raises(IncompleteReportError, match="nextOffset=100"):
        client.fetch((124, 24, 126, 26), "2026-08-15", "2026-08-22")
