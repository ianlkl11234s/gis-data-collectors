from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path

import pytest

from scripts.gfw_east_asia_v4_poc import (
    EXPECTED_TILE_COUNT,
    FIXED_BBOX,
    POPUP_FIELDS,
    TYPED_HEADER,
    TYPED_MAGIC,
    build_fishing_effort_sample,
    build_grid_artifacts,
    build_grid_artifacts_from_compare_sqlite,
    build_local_shadow_poc,
    build_track_daypacks,
    canonical_cell,
    compare_presence,
    compare_presence_ndjson,
    fetch_fishing_effort_phase,
    fetch_presence_phase,
    index_presence,
    normalize_fishing_effort,
    run_presence_phase_isolated,
)
from scripts.gfw_hourly_tracks_poc import make_tiles


DAY = date(2026, 8, 15)


def _point(
    vessel_id: str,
    observed_at: str,
    lon: float,
    lat: float,
    *,
    vessel_type: str | None = "CARGO",
    mmsi: str | None = None,
    ship_name: str | None = None,
    flag: str | None = None,
    **popup,
):
    return {
        "vessel_id": vessel_id,
        "observed_at": observed_at,
        "longitude": lon,
        "latitude": lat,
        "mmsi": mmsi,
        "ship_name": ship_name,
        "vessel_type": vessel_type,
        "flag": flag,
        **popup,
    }


def _fake_pmtiles(calls):
    def build(*, named_inputs, output, minimum_zoom, maximum_zoom):
        assert all(path.is_file() for _layer, path in named_inputs)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"PMTiles-fixture-v1")
        calls.append({
            "layers": [layer for layer, _path in named_inputs],
            "output": output,
            "minimum_zoom": minimum_zoom,
            "maximum_zoom": maximum_zoom,
        })

    return build


def test_fixed_bbox_is_42_tiles_and_decimal_cells_are_globally_aligned():
    assert len(make_tiles(FIXED_BBOX, tile_size_degrees=3.0)) == EXPECTED_TILE_COUNT == 42
    assert canonical_cell(125.005, 25.095) == (12505, 2505)
    assert canonical_cell("125.099999999999", "25.099999999999") == (12505, 2505)
    assert canonical_cell("125.1", "25.1") == (12515, 2515)
    assert canonical_cell("-0.0001", "-0.0001") == (-5, -5)


def test_presence_reports_global_parity_before_cell_member_and_boundary_differences():
    low_rows = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.05, 25.05, mmsi="111", ship_name="ONE"),
        _point("v-2", "2026-08-15T00:00:00Z", 125.15, 25.05, mmsi="222"),
    ]
    low_rows.append(dict(low_rows[0]))  # exact duplicate must be accounted for, not published.
    high_rows = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.005, 25.005, mmsi="111", ship_name="ONE"),
        # Same global vessel-hour but a different canonical 0.1 cell.
        _point("v-2", "2026-08-15T00:00:00Z", 125.095, 25.005, mmsi="DIFFERENT"),
        # Same-vessel-hour cell conflict: deterministic earliest observation wins.
        _point("v-2", "2026-08-15T00:30:00Z", 125.105, 25.005, mmsi="222"),
    ]
    low = index_presence(low_rows)
    high = index_presence(high_rows)
    report = compare_presence(low, high)

    assert report["global_vessel_hour"]["equal"] is True
    assert report["global_vessel_hour"]["low_count"] == 2
    assert report["boundary_assignment_mismatch_count"] == 1
    assert report["per_cell_members"]["equal"] is False
    assert low.stats["exact_duplicate_rows"] == 1
    assert high.stats["same_vessel_hour_cell_conflicts"] == 1
    assert low.stats["identity_field_null_counts"]["flag"] == 2


def test_disk_backed_presence_compare_matches_identity_and_boundary_semantics(tmp_path):
    low_path = tmp_path / "low.ndjson"
    high_path = tmp_path / "high.ndjson"
    low_rows = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.05, 25.05),
        _point("v-2", "2026-08-15T00:00:00Z", 125.15, 25.05),
    ]
    high_rows = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.005, 25.005),
        _point("v-2", "2026-08-15T00:00:00Z", 125.095, 25.005),
    ]
    low_path.write_text("".join(json.dumps(row) + "\n" for row in low_rows), encoding="utf-8")
    high_path.write_text("".join(json.dumps(row) + "\n" for row in high_rows), encoding="utf-8")
    report = compare_presence_ndjson(
        low_path, high_path, sqlite_path=tmp_path / "compare.sqlite",
    )
    assert report["global_vessel_hour"]["equal"] is True
    assert report["boundary_assignment_mismatch_count"] == 1
    assert report["per_cell_members"]["equal"] is False


def test_fetch_phase_is_sequential_42_reports_and_records_resource_contract(tmp_path):
    class FakeClient:
        def __init__(self):
            self.calls = []
            self.stats = {
                "post_requests": 0,
                "recovery_requests": 0,
                "retries": 0,
                "http_statuses": {},
                "response_body_bytes": 0,
                "status_429": 0,
                "status_524": 0,
            }

        def fetch(self, bbox, start, end, *, spatial_resolution):
            self.calls.append((bbox, start, end, spatial_resolution))
            index = len(self.calls)
            self.stats["post_requests"] += 1
            self.stats["http_statuses"]["200"] = index
            if index == 1:
                self.stats.update({
                    "recovery_requests": 2, "retries": 1,
                    "response_body_bytes": 123456,
                    "status_429": 1, "status_524": 1,
                })
                self.stats["http_statuses"].update({"429": 1, "524": 1})
            return {
                "entries": [{
                    "vesselId": f"v-{index}",
                    "date": "2026-08-15T00:00:00Z",
                    "lon": bbox[0] + 0.005,
                    "lat": bbox[1] + 0.005,
                }],
                "nextOffset": 0,
            }, "public-global-presence:v4.0"

    client = FakeClient()
    output = tmp_path / "low.ndjson"
    metrics = fetch_presence_phase(
        client=client, resolution="LOW", selected_day=DAY, output_path=output,
    )
    assert len(client.calls) == 42
    assert all(call[3] == "LOW" for call in client.calls)
    assert metrics["logical_report_count"] == metrics["report_page_count"] == 42
    assert metrics["response_body_bytes"] == 123456
    assert metrics["status_429"] == metrics["status_524"] == 1
    assert metrics["retries"] == 1
    assert metrics["wall_time_seconds"] >= 0
    assert metrics["peak_rss_bytes"] > 0
    assert metrics["raw_response_saved"] is False
    assert metrics["checkpoint_contract"] == {
        "mode": "per_tile_ndjson_atomic_then_streamed_assembly",
        "checkpoint_count": 42,
        "resumed_checkpoint_count": 0,
    }
    assert len(output.read_text(encoding="utf-8").splitlines()) == 42


def test_fetch_phase_resumes_validated_parts_without_refetching_or_daily_row_list(tmp_path):
    class Client:
        def __init__(self):
            self.stats = {"post_requests": 0, "recovery_requests": 0, "retries": 0, "http_statuses": {}, "response_body_bytes": 0, "status_429": 0, "status_524": 0}
        def fetch(self, bbox, start, end, *, spatial_resolution):
            self.stats["post_requests"] += 1
            return {"entries": [{"vesselId": f"v-{self.stats['post_requests']}", "date": "2026-08-15T00:00:00Z", "lon": bbox[0] + .01, "lat": bbox[1] + .01}]}, "public-global-presence:v4.0"
    output = tmp_path / "high.ndjson"
    first = fetch_presence_phase(client=Client(), resolution="HIGH", selected_day=DAY, output_path=output)
    class NoFetch(Client):
        def fetch(self, *args, **kwargs):
            raise AssertionError("validated checkpoints must resume without a refetch")
    resumed = fetch_presence_phase(client=NoFetch(), resolution="HIGH", selected_day=DAY, output_path=output)
    assert first["normalized_row_count"] == resumed["normalized_row_count"] == 42
    assert resumed["checkpoint_contract"]["resumed_checkpoint_count"] == 42
    assert len(output.read_text(encoding="utf-8").splitlines()) == 42


def test_presence_phase_worker_isolates_peak_rss_with_offline_fixture(tmp_path):
    fixture = tmp_path / "low-fixture.json"
    fixture.write_text(json.dumps({
        "responses": [{
            "expected_spatial_resolution": "LOW",
            "status": 200,
            "body_bytes": 17,
            "resolved_dataset_version": "public-global-presence:v4.0",
            "payload": {
                "entries": [{
                    "vesselId": f"v-{index}", "date": "2026-08-15T00:00:00Z",
                    "lon": 125.05, "lat": 25.05,
                }],
                "nextOffset": 0,
            },
        } for index in range(42)],
    }), encoding="utf-8")
    output = tmp_path / "isolated" / "low.ndjson"
    metrics_path = tmp_path / "isolated" / "low.metrics.json"
    metrics = run_presence_phase_isolated(
        resolution="LOW", selected_day=DAY, output_path=output,
        metrics_path=metrics_path, fixture_path=fixture,
    )
    assert metrics["logical_report_count"] == 42
    assert metrics["post_requests"] == 42
    assert metrics["response_body_bytes"] == 42 * 17
    assert metrics["peak_rss_bytes"] > 0
    assert output.is_file() and metrics_path.is_file()


def test_hourly_grid_pmtiles_and_adaptive_details_are_lossless(tmp_path):
    high = index_presence([
        _point("v-1", "2026-08-15T00:00:00Z", 125.005, 25.005, mmsi="111"),
        _point("v-2", "2026-08-15T00:00:00Z", 125.015, 25.005, mmsi="222"),
        # Child-cell duplicate vessel must remain one canonical vessel/hour.
        _point("v-1", "2026-08-15T00:30:00Z", 125.025, 25.005, mmsi="111"),
    ])
    calls = []
    grid, assets = build_grid_artifacts(
        high, selected_day=DAY, root=tmp_path,
        pmtiles_builder=_fake_pmtiles(calls),
        detail_target_compressed_bytes=220,
    )
    assert len(grid["hours"]) == len(calls) == 24
    assert all(call["layers"] == ["gfw_grid_0_1"] for call in calls)
    first = grid["hours"][0]
    assert first["pmtiles"]["features"] == 1
    assert first["pmtiles"]["vessels"] == 2
    assert sum(detail["vessels"] for detail in first["details"]) == 2
    with gzip.open(tmp_path / first["details"][0]["path"], "rt", encoding="utf-8") as handle:
        detail = json.load(handle)
    entry = next(iter(detail["entries"].values()))
    assert entry["vessel_count"] == len(entry["members"]) == 2
    assert set(entry["members"][0]) == set(POPUP_FIELDS)
    assert len([asset for asset in assets if asset["type"] == "grid_hour_pmtiles"]) == 24


def test_compare_sqlite_builds_24_high_only_grid_hours_with_lossless_details(tmp_path):
    low_path = tmp_path / "low.ndjson"
    high_path = tmp_path / "high.ndjson"
    low_rows = [_point(
        "low-only", "2026-08-15T00:00:00Z", 125.205, 25.005,
        mmsi="000",
    )]
    high_rows = [
        _point(
            "high-1", "2026-08-15T00:00:00Z", 125.005, 25.005,
            mmsi="111", imo="IMO111", callsign="CALL1", hours=0.75,
            entry_timestamp="2026-08-15T00:00:00Z",
            exit_timestamp="2026-08-15T00:59:59Z",
            dataset="public-global-presence:v4.0", geartype="trawler",
        ),
        _point(
            "high-2", "2026-08-15T01:00:00Z", 125.105, 25.005,
            mmsi="222",
        ),
    ]
    low_path.write_text("".join(json.dumps(row) + "\n" for row in low_rows), encoding="utf-8")
    high_path.write_text("".join(json.dumps(row) + "\n" for row in high_rows), encoding="utf-8")
    sqlite_path = tmp_path / "compare.sqlite"
    compare_presence_ndjson(low_path, high_path, sqlite_path=sqlite_path)
    calls = []
    root = tmp_path / "grid-build"
    grid, assets = build_grid_artifacts_from_compare_sqlite(
        sqlite_path, selected_day=DAY, root=root,
        pmtiles_builder=_fake_pmtiles(calls), semantic_readback=False,
        detail_target_compressed_bytes=160,
    )

    assert len(grid["hours"]) == len(calls) == 24
    assert grid["hour_query_count"] == 24
    assert grid["max_hour_members"] == 1
    assert sum(hour["pmtiles"]["vessels"] for hour in grid["hours"]) == 2
    assert all("low-only" not in str(hour) for hour in grid["hours"])
    first_detail_path = root / grid["hours"][0]["details"][0]["path"]
    with gzip.open(first_detail_path, "rt", encoding="utf-8") as handle:
        detail = json.load(handle)
    member = next(iter(detail["entries"].values()))["members"][0]
    assert member["vessel_id"] == "high-1"
    assert member["imo"] == "IMO111"
    assert member["callsign"] == "CALL1"
    assert member["hours"] == 0.75
    assert member["dataset"] == "public-global-presence:v4.0"
    assert len([asset for asset in assets if asset["type"] == "grid_hour_pmtiles"]) == 24


@pytest.mark.skipif(
    not Path("/opt/homebrew/bin/tippecanoe").is_file()
    or not Path("/opt/homebrew/bin/pmtiles").is_file(),
    reason="tippecanoe/pmtiles CLIs unavailable",
)
def test_real_pmtiles_readback_decodes_every_maxzoom_tile_and_properties(tmp_path):
    low_path = tmp_path / "low.ndjson"
    high_path = tmp_path / "high.ndjson"
    rows = [_point(
        "v-1", "2026-08-15T00:00:00Z", 125.005, 25.005, mmsi="111",
    )]
    payload = "".join(json.dumps(row) + "\n" for row in rows)
    low_path.write_text(payload, encoding="utf-8")
    high_path.write_text(payload, encoding="utf-8")
    sqlite_path = tmp_path / "compare.sqlite"
    compare_presence_ndjson(low_path, high_path, sqlite_path=sqlite_path)
    grid, assets = build_grid_artifacts_from_compare_sqlite(
        sqlite_path, selected_day=DAY, root=tmp_path / "real-grid",
    )

    grid_assets = [asset for asset in assets if asset["type"] == "grid_hour_pmtiles"]
    assert len(grid_assets) == 24
    assert all(asset["semantic_readback"]["status"] == "passed" for asset in grid_assets)
    first = grid["hours"][0]["pmtiles"]["semantic_readback"]
    assert first["decoded_maxzoom_tiles"] >= 1
    assert first["unique_cells"] == first["expected_cells"] == 1
    assert first["properties"] == ["cell_id", "vessel_count", "detail_shard"]


def test_per_type_daypacks_compare_gzip_json_and_typed_binary_without_future_lines(tmp_path):
    rows = [
        _point("cargo-1", "2026-08-15T00:00:00Z", 125.0, 25.0, vessel_type="CARGO", mmsi="111"),
        _point("cargo-1", "2026-08-15T01:00:00Z", 125.0, 25.0, vessel_type="CARGO", mmsi="111"),
        # Coincident head at hour 0 must preserve both members.
        _point("cargo-2", "2026-08-15T00:00:00Z", 125.0, 25.0, vessel_type="CARGO", mmsi="222"),
        _point("cargo-2", "2026-08-15T01:00:00Z", 125.1, 25.0, vessel_type="CARGO", mmsi="222"),
        # >2h gap produces separate exporter segments.
        _point("tank-1", "2026-08-15T00:00:00Z", 126.0, 26.0, vessel_type="TANKER"),
        _point("tank-1", "2026-08-15T04:00:00Z", 126.1, 26.0, vessel_type="TANKER"),
        # Adjacent day input may exist upstream but must not enter selected-day geometry.
        _point("cargo-1", "2026-08-16T00:00:00Z", 125.2, 25.0, vessel_type="CARGO", mmsi="111"),
    ]
    tracks, assets = build_track_daypacks(rows, selected_day=DAY, root=tmp_path)
    assert tracks["split_by_ship_type_before_download"] is True
    assert tracks["default_enabled_buckets"] == ["cargo", "tanker", "passenger"]
    cargo = next(item for item in tracks["buckets"] if item["bucket"] == "cargo")
    assert cargo["same_coordinate_edges"] == 1
    assert cargo["coincident_head_group_count"] >= 1
    assert cargo["complete_head_member_count"] == cargo["point_count"]
    assert cargo["candidates"]["gzip_json"]["transfer_bytes"] > 0
    assert cargo["candidates"]["typed_binary"]["transfer_bytes"] > 0
    assert cargo["candidates"]["gzip_json"]["python_decode_seconds"] >= 0
    assert cargo["candidates"]["typed_binary"]["python_decode_seconds"] >= 0

    json_asset = cargo["candidates"]["gzip_json"]["assets"][0]
    with gzip.open(tmp_path / json_asset["path"], "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert payload["render_contract"]["no_future_geometry"] is True
    assert all(
        all(point[2] < 1786838400 for point in segment["points"])
        for segment in payload["segments"]
    )
    grouped = {}
    for segment in payload["segments"]:
        for lon, lat, epoch in segment["points"]:
            grouped.setdefault((lon, lat, epoch), []).append(segment["vessel"])
    coincident = next(members for members in grouped.values() if len(members) == 2)
    assert [member["vessel_id"] for member in coincident] == ["cargo-1", "cargo-2"]

    binary_asset = cargo["candidates"]["typed_binary"]["assets"][0]
    decoded = gzip.decompress((tmp_path / binary_asset["path"]).read_bytes())
    magic, version, _metadata_bytes, points, segments = TYPED_HEADER.unpack_from(decoded)
    assert magic == TYPED_MAGIC
    assert version == 1
    assert segments == cargo["segment_count"]
    assert points == cargo["point_count"]
    assert len(assets) == 10  # five type buckets × JSON/binary.


def test_track_daypacks_restore_full_popup_members_after_exporter_projection(tmp_path):
    shared = {
        "hours": 1.25,
        "entry_timestamp": "2026-08-15T00:00:00Z",
        "exit_timestamp": "2026-08-15T00:59:59Z",
        "first_transmission_date": "2020-01-01T00:00:00Z",
        "last_transmission_date": "2026-08-15T00:00:00Z",
        "dataset": "public-global-presence:v4.0",
        "geartype": "trawler",
    }
    rows = [
        _point(
            "cargo-1", "2026-08-15T00:00:00Z", 125.0, 25.0,
            vessel_type="CARGO", mmsi="111", ship_name="ONE", flag="TWN",
            imo="IMO111", callsign="CALL1", **shared,
        ),
        _point(
            "cargo-2", "2026-08-15T00:00:00Z", 125.0, 25.0,
            vessel_type="CARGO", mmsi="222", ship_name="TWO", flag="JPN",
            imo="IMO222", callsign="CALL2", **shared,
        ),
    ]
    tracks, _assets = build_track_daypacks(rows, selected_day=DAY, root=tmp_path)
    cargo = next(item for item in tracks["buckets"] if item["bucket"] == "cargo")
    json_asset = cargo["candidates"]["gzip_json"]["assets"][0]
    with gzip.open(tmp_path / json_asset["path"], "rt", encoding="utf-8") as handle:
        payload = json.load(handle)

    vessels = {segment["vessel"]["vessel_id"]: segment["vessel"] for segment in payload["segments"]}
    assert vessels["cargo-1"] == {
        "vessel_id": "cargo-1", "mmsi": "111", "ship_name": "ONE",
        "vessel_type": "CARGO", "flag": "TWN", "hours": 1.25,
        "entry_timestamp": shared["entry_timestamp"],
        "exit_timestamp": shared["exit_timestamp"], "imo": "IMO111",
        "callsign": "CALL1",
        "first_transmission_date": shared["first_transmission_date"],
        "last_transmission_date": shared["last_transmission_date"],
        "dataset": shared["dataset"], "geartype": shared["geartype"],
    }
    grouped = {}
    for segment in payload["segments"]:
        for lon, lat, epoch in segment["points"]:
            grouped.setdefault((lon, lat, epoch), []).append(segment["vessel"])
    coincident = next(members for members in grouped.values() if len(members) == 2)
    members = {member["vessel_id"]: member for member in coincident}
    assert members["cargo-1"] == vessels["cargo-1"]
    assert members["cargo-2"]["imo"] == "IMO222"
    assert members["cargo-2"]["callsign"] == "CALL2"


def test_fishing_effort_is_independent_daily_low_nonnegative_contract(tmp_path):
    payload = {
        "public-global-fishing-effort:v3.0": [
            {"date": "2026-08-15", "lon": 125.05, "lat": 25.05, "hours": 1.5, "flag": "TWN"},
            {"date": "2026-08-15", "lon": 125.06, "lat": 25.05, "fishingHours": 2.0, "gearType": "trawler"},
            {"date": "2026-08-15", "lon": 125.15, "lat": 25.05, "hours": -1},
            {"date": "2026-08-14", "lon": 125.15, "lat": 25.05, "hours": 8},
        ]
    }
    rows, quality = normalize_fishing_effort(
        [payload], selected_day=DAY,
        resolved_dataset_version="public-global-fishing-effort:v3.0",
    )
    assert len(rows) == 2
    assert quality["negative_hours_rejected"] == 1
    assert quality["wrong_day_rows"] == 1
    assert quality["boundary_overlap_rows"] == 0
    result, assets = build_fishing_effort_sample(
        [payload], selected_day=DAY,
        resolved_dataset_version="public-global-fishing-effort:v3.0",
        root=tmp_path,
        latest_observed_active_date="2026-08-18",
        source_response_sha256="a" * 64,
        source_accessed_at="2026-08-19T00:00:00+00:00",
    )
    assert result["independent_layer"] is True
    assert result["presence_identity_contract_shared"] is False
    assert result["asset"]["apparent_fishing_hours"] == 3.5
    with gzip.open(tmp_path / assets[0]["path"], "rt", encoding="utf-8") as handle:
        collection = json.load(handle)
    assert collection["metadata"]["metric"] == "apparent_fishing_hours"
    assert collection["metadata"]["temporal_resolution"] == "DAILY"
    assert collection["metadata"]["spatial_resolution"] == "LOW"
    assert collection["metadata"]["latest_available_date"] is None
    assert collection["metadata"]["latest_available_date_status"] == "not_provided_by_gfw"
    assert collection["metadata"]["latest_observed_active_date"] == "2026-08-18"
    assert collection["metadata"]["source_response_sha256"] == "a" * 64
    assert collection["metadata"]["finalization_status"] == "not_provided_by_gfw"


def test_fishing_effort_sums_distinct_components_but_dedupes_exact_rows():
    component = {
        "date": "2026-08-15", "lon": 125.05, "lat": 25.05,
        "hours": 1.5, "flag": "TWN",
    }
    rows, quality = normalize_fishing_effort(
        [{"dataset": [component, dict(component), {**component, "hours": 2.0}]}],
        selected_day=DAY,
        resolved_dataset_version="public-global-fishing-effort:v4.0",
    )
    assert [row["apparent_fishing_hours"] for row in rows] == [1.5, 2.0]
    assert quality["exact_duplicate_rows"] == 1
    assert quality["boundary_overlap_rows"] == 0


def test_fishing_effort_fetch_is_42_sequential_daily_low_reports():
    class FakeClient:
        def __init__(self):
            self.calls = []
            self.stats = {
                "http_statuses": {"200": 42}, "response_body_bytes": 4200,
                "retries": 0, "status_429": 0, "status_524": 0,
            }

        def fetch(self, bbox, start, end, **kwargs):
            self.calls.append((bbox, start, end, kwargs))
            return {
                "entries": [{
                    "date": "2026-08-15", "lon": bbox[0] + 0.05,
                    "lat": bbox[1] + 0.05, "hours": 1.0,
                }],
                "nextOffset": 0,
            }, "public-global-fishing-effort:v3.0"

    client = FakeClient()
    payloads, metrics = fetch_fishing_effort_phase(client=client, selected_day=DAY)
    assert len(payloads) == len(client.calls) == 42
    assert all(call[3] == {
        "dataset": "public-global-fishing-effort:latest",
        "group_by": None,
        "spatial_resolution": "LOW",
        "temporal_resolution": "DAILY",
    } for call in client.calls)
    assert metrics["presence_identity_contract_shared"] is False
    assert metrics["raw_response_saved"] is False
    assert metrics["response_body_bytes"] == 4200


def test_local_poc_manifest_is_immutable_read_back_and_never_publishable(tmp_path):
    low = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.05, 25.05, vessel_type="CARGO"),
        _point("v-1", "2026-08-15T01:00:00Z", 125.05, 25.05, vessel_type="CARGO"),
    ]
    high = [
        _point("v-1", "2026-08-15T00:00:00Z", 125.005, 25.005, vessel_type="CARGO"),
        _point("v-1", "2026-08-15T01:00:00Z", 125.005, 25.005, vessel_type="CARGO"),
    ]
    effort = {"entries": [{
        "date": "2026-08-15", "lon": 125.05, "lat": 25.05, "hours": 1.0,
    }]}
    output = tmp_path / "immutable-poc"
    calls = []
    manifest = build_local_shadow_poc(
        low_rows=low, high_rows=high, fishing_payloads=[effort],
        fishing_resolved_dataset_version="public-global-fishing-effort:v3.0",
        selected_day=DAY, output_dir=output,
        phase_metrics={
            "LOW": {"wall_time_seconds": 1, "peak_rss_bytes": 10, "response_body_bytes": 20},
            "HIGH": {"wall_time_seconds": 2, "peak_rss_bytes": 30, "response_body_bytes": 40},
        },
        pmtiles_builder=_fake_pmtiles(calls),
    )
    assert manifest["presence_parity"]["global_vessel_hour"]["equal"] is True
    assert manifest["readback"]["status"] == "passed"
    assert manifest["readback"]["checked_assets"] == len(manifest["artifacts"])
    assert manifest["artifact_bytes"] == manifest["readback"]["checked_bytes"]
    assert manifest["release_id"] == DAY.isoformat()
    assert len(manifest["days"][0]["assets"]) == 10
    assert manifest["release_truth"] == {
        "build": "passed_local_fixture_or_shadow",
        "contract_wire": "local_POC_only",
        "stage": "local_immutable_directory",
        "upload": "not_run",
        "readback": "passed_local",
        "pull": "not_run",
        "deploy": "not_run",
        "HTTP": "not_run",
        "browser": "not_run",
    }
    assert len(calls) == 24
    with pytest.raises(FileExistsError, match="immutable POC output"):
        build_local_shadow_poc(
            low_rows=low, high_rows=high, fishing_payloads=[effort],
            fishing_resolved_dataset_version="public-global-fishing-effort:v3.0",
            selected_day=DAY, output_dir=output,
            pmtiles_builder=_fake_pmtiles([]),
        )
