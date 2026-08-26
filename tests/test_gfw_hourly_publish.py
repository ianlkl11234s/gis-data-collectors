import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.gfw_hourly_grid_poc import _write_points
from tasks.gfw_hourly_publish import (
    DEFAULT_BBOX,
    GFW_DATASET,
    SAR_DATASET,
    GFWHourlyPublishSettings,
    GFWHourlyPublishTask,
    _ledger_release_contract,
    _write_sar_points,
    build_unified_release,
    fetch_sar_unmatched_shards,
    finalize_sar_hours,
    normalize_sar_unmatched_entries,
    prune_expired_failed_spools,
)
from scripts.gfw_hourly_tracks_poc import make_tiles


def _settings(tmp_path: Path, *, expected_tiles: int = 1) -> GFWHourlyPublishSettings:
    return GFWHourlyPublishSettings(
        token="test-token",
        db_url="postgresql://test.invalid/db",
        bucket="test-bucket",
        public_url_prefix="https://cdn.example/global-maritime/gfw-hourly",
        shadow_public_url_prefix="https://cdn.example/global-maritime/gfw-hourly/v3-shadow",
        spool_root=tmp_path / "spool",
        bbox=(122.0, 23.0, 122.2, 23.2),
        expected_tile_count=expected_tiles,
    )


def _ais_rows() -> list[dict]:
    return [
        {
            "vessel_id": "vessel-1", "observed_at": "2026-08-19T00:00:00+00:00",
            "longitude": 122.1, "latitude": 23.1, "mmsi": "123456789",
            "ship_name": "TEST", "vessel_type": "CARGO", "flag": "TWN",
        },
        {
            "vessel_id": "vessel-1", "observed_at": "2026-08-19T01:00:00+00:00",
            "longitude": 122.11, "latitude": 23.11, "mmsi": "123456789",
            "ship_name": "TEST", "vessel_type": "CARGO", "flag": "TWN",
        },
    ]


def _sar_payload(values=None):
    if values is None:
        values = [{"date": "2026-08-19 01:00", "detections": 2, "lat": 23.15, "lon": 122.15}]
    return {"entries": [{"public-global-sar-presence:v4.0": values}], "nextOffset": None}


def test_production_bbox_is_16_tiles_and_two_distinct_sources_mean_32_reports(tmp_path):
    settings = GFWHourlyPublishSettings(
        token="test-token", db_url="postgresql://test.invalid/db",
        bucket="test-bucket",
        public_url_prefix="https://cdn.example/global-maritime/gfw-hourly",
        shadow_public_url_prefix="https://cdn.example/global-maritime/gfw-hourly/v3-shadow",
        spool_root=tmp_path, bbox=DEFAULT_BBOX,
    )
    settings.validate()
    assert len(make_tiles(settings.bbox, tile_size_degrees=3.0)) == 16
    assert 16 + 16 == 32  # AIS shared grid/tracks + distinct SAR unmatched.


def test_production_release_retention_is_fixed_at_two(tmp_path):
    settings = _settings(tmp_path)
    settings.validate()
    invalid = GFWHourlyPublishSettings(
        **{**settings.__dict__, "releases_to_keep": 3}
    )
    with pytest.raises(ValueError, match="fixed at 2"):
        invalid.validate()


def test_sar_normalizer_accepts_official_wrapper_and_documented_null_empty():
    rows = normalize_sar_unmatched_entries(
        _sar_payload(), resolved_dataset="public-global-sar-presence:v4.0"
    )
    assert rows == [{
        "observed_at": "2026-08-19T01:00:00Z",
        "longitude": 122.15,
        "latitude": 23.15,
        "detections": 2,
        "source_dataset": "public-global-sar-presence:v4.0",
    }]
    assert normalize_sar_unmatched_entries(
        _sar_payload(None), resolved_dataset="public-global-sar-presence:v4.0"
    ) == rows
    assert normalize_sar_unmatched_entries(
        {"entries": [{"public-global-sar-presence:v4.0": None}]},
        resolved_dataset="public-global-sar-presence:v4.0",
    ) == []


def test_sar_normalizer_fails_closed_on_schema_drift():
    with pytest.raises(ValueError, match="unexpected SAR dataset"):
        normalize_sar_unmatched_entries(
            {"entries": [{"unknown:v1": []}]}, resolved_dataset=None
        )
    with pytest.raises(ValueError, match="integer"):
        normalize_sar_unmatched_entries(
            _sar_payload([{"date": "2026-08-19 01:00", "detections": 1.5, "lat": 1, "lon": 2}]),
            resolved_dataset="public-global-sar-presence:v4.0",
        )


def test_sar_finalize_emits_zero_feature_hours_for_complete_window(tmp_path):
    work = tmp_path / "work"
    output = tmp_path / "dark_vessels"
    work.mkdir()
    shard = work / "r00c00.sar-unmatched.ndjson"
    _write_sar_points(shard, [])
    hours, counts = finalize_sar_hours(
        [shard], work_dir=work, output_dir=output,
        start=datetime(2026, 8, 19, tzinfo=timezone.utc).date(),
        latest=datetime(2026, 8, 19, tzinfo=timezone.utc).date(),
    )
    assert len(hours) == 24
    assert counts["detection_count"] == 0
    first = json.loads((output / hours[0]["path"]).read_text())
    assert hours[0]["observed_at"] == "2026-08-19T00:00:00Z"
    assert first["features"] == []
    assert first["metadata"]["observed_at"] == "2026-08-19T00:00:00Z"
    assert first["metadata"]["not_proof_of_dark_or_illegal_vessel"] is True


def test_unified_manifest_v3_has_full_fidelity_track_contract(tmp_path):
    settings = _settings(tmp_path)
    ais_work = tmp_path / "ais-work"
    sar_work = tmp_path / "sar-work"
    ais_work.mkdir()
    sar_work.mkdir()
    ais_shard = ais_work / "r00c00.points.ndjson"
    sar_shard = sar_work / "r00c00.sar-unmatched.ndjson"
    _write_points(ais_shard, _ais_rows())
    _write_sar_points(sar_shard, normalize_sar_unmatched_entries(
        _sar_payload(), resolved_dataset="public-global-sar-presence:v4.0"
    ))
    start = datetime(2026, 8, 14, tzinfo=timezone.utc).date()
    latest = datetime(2026, 8, 20, tzinfo=timezone.utc).date()
    manifest = build_unified_release(
        release_dir=tmp_path / "release" / "2026-08-20",
        work_dir=ais_work,
        shards=[ais_shard],
        sar_work_dir=sar_work,
        sar_shards=[sar_shard],
        settings=settings,
        start=start,
        latest=latest,
        fetch_state={
            "resolved_dataset_versions": ["public-global-presence:v3.0"],
            "logical_tile_report_count": 1,
            "requests": {},
        },
        sar_state={
            "resolved_dataset_versions": ["public-global-sar-presence:v4.0"],
            "logical_tile_report_count": 1,
            "requests": {},
            "combined_request_telemetry": {},
        },
        generated_at="2026-08-25T00:00:00+00:00",
    )
    assert manifest["schema_version"] == 3
    assert manifest["full_fidelity"] is True
    assert manifest["release_id"] == "2026-08-20"
    assert manifest["tracks"]["days"]
    assert manifest["grid"]["hours"]
    assert len(manifest["dark_vessels"]["hours"]) == 168
    assert all(
        entry["observed_at"].endswith("Z")
        and "+00:00" not in entry["observed_at"]
        for entry in manifest["dark_vessels"]["hours"]
    )
    assert {asset["type"] for asset in manifest["assets"]} == {
        "tracks_day_pmtiles", "grid_hour_pmtiles", "track_frame_hour",
        "grid_detail_bucket", "track_detail_bucket", "sar_unmatched_hour",
    }
    assert len(manifest["tracks"]["frames"]) == 168
    assert len(manifest["grid"]["hours"]) == 168
    assert all(len(entry["detail_buckets"]) == 16 for entry in manifest["grid"]["hours"])
    assert all(len(entry["detail_buckets"]) == 16 for entry in manifest["tracks"]["days"])
    assert manifest["tracks"]["counts"]["cap_applied"] is False
    assert manifest["tracks"]["counts"]["omitted_by_display_cap"] == 0
    assert manifest["tracks"]["counts"]["omitted_features"] == 0
    assert manifest["tracks"]["counts"]["candidate_points"] == manifest["tracks"]["counts"]["published_points"]
    dark_path = manifest["dark_vessels"]["hours"][0]["path"]
    assert dark_path.startswith("dark_vessels/hours/")
    feature_file = next(
        tmp_path / "release" / "2026-08-20" / entry["path"]
        for entry in manifest["dark_vessels"]["hours"] if entry["detections"]
    )
    feature_collection = json.loads(feature_file.read_text())
    feature = feature_collection["features"][0]
    assert feature_collection["metadata"]["observed_at"] == "2026-08-19T01:00:00Z"
    assert feature["properties"]["observed_at"] == "2026-08-19T01:00:00Z"
    assert feature["properties"]["matching_semantics"] == "SAR_detection_not_matched_to_AIS"
    assert feature["properties"]["coordinate_semantics"] == "GFW_HIGH_grid_cell_center"

    assets, summary = _ledger_release_contract(manifest)
    assert all(asset["path"].startswith("releases/2026-08-20/") for asset in assets)
    assert {asset["type"] for asset in assets} == {
        "tracks_day_pmtiles", "grid_hour_pmtiles", "track_frame_hour",
        "grid_detail_bucket", "track_detail_bucket", "sar_unmatched_hour",
    }
    assert summary["full_fidelity"] is True
    assert summary["candidate_canonical_points"] == summary["published_canonical_points"]
    assert summary["cache_contract"] == {
        "root": "public,max-age=60,s-maxage=60,stale-while-revalidate=300",
        "release": "public,max-age=604800,s-maxage=604800,immutable",
    }


class _MissingKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class _FakeS3:
    def __init__(self):
        self.objects = {}
        self.put_order = []

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise _MissingKey()
        value = self.objects[Key]
        return {"Body": io.BytesIO(value["Body"]), "ContentLength": len(value["Body"])}

    def put_object(self, **kwargs):
        body = kwargs["Body"]
        if isinstance(body, str):
            body = body.encode()
        self.objects[kwargs["Key"]] = {**kwargs, "Body": body}
        self.put_order.append(kwargs["Key"])

    def head_object(self, *, Bucket, Key):
        value = self.objects[Key]
        return {"ContentLength": len(value["Body"]), "Metadata": value.get("Metadata", {})}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop(Key, None)


class _FakeReportClient:
    def __init__(self):
        self.calls = []
        self.stats = {
            "post_requests": 0, "recovery_requests": 0, "retries": 0,
            "http_statuses": {}, "last_rate_limit_headers": {},
        }

    def fetch(self, bbox, start, end, **kwargs):
        self.calls.append(kwargs)
        self.stats["post_requests"] += 1
        if kwargs.get("dataset") == SAR_DATASET:
            return _sar_payload(), "public-global-sar-presence:v4.0"
        return {"entries": [
            {"vesselId": "vessel-1", "date": "2026-08-19T00:00:00Z", "lat": 23.1, "lon": 122.1},
            {"vesselId": "vessel-1", "date": "2026-08-19T01:00:00Z", "lat": 23.11, "lon": 122.11},
        ]}, "public-global-presence:v3.0"


class _FakeLedger:
    def __init__(self, fail_first=False):
        self.payloads = []
        self.fail_first = fail_first

    def write(self, payload):
        self.payloads.append(payload)
        if self.fail_first and len(self.payloads) == 1:
            raise RuntimeError("migration missing")


class _CutoverLedgerFailure(_FakeLedger):
    def write(self, payload):
        self.payloads.append(payload)
        if payload["status"] == "succeeded":
            raise RuntimeError("temporary ledger outage")


class _InterruptedReportClient(_FakeReportClient):
    def __init__(self):
        super().__init__()

    def fetch(self, *_args, **_kwargs):
        raise KeyboardInterrupt("operator cancelled")


def test_task_uses_one_ais_fetch_for_grid_tracks_then_sar_and_manifest_last(tmp_path):
    settings = _settings(tmp_path)
    client = _FakeReportClient()
    ledger = _FakeLedger()
    s3 = _FakeS3()
    task = GFWHourlyPublishTask(
        settings,
        ledger=ledger,
        report_client_factory=lambda _token: client,
        s3_client_factory=lambda: s3,
        now=lambda: datetime(2026, 8, 25, tzinfo=timezone.utc),
    )
    result = task.run()
    assert len(client.calls) == 2
    assert client.calls[0] == {}
    assert client.calls[1] == {
        "dataset": SAR_DATASET, "group_by": None, "filters": ("matched='false'",)
    }
    assert [payload["status"] for payload in ledger.payloads] == ["running", "succeeded"]
    running = ledger.payloads[0]
    succeeded = ledger.payloads[-1]
    assert running["manifest_schema_version"] == succeeded["manifest_schema_version"] == 3
    assert running["root_manifest_key"] == (
        "deploy-assets/global-maritime/gfw-hourly/v3-shadow/manifest.json"
    )
    assert running["root_manifest_key"] == succeeded["root_manifest_key"]
    assert "token" not in running and "token" not in succeeded
    assert {asset["type"] for asset in succeeded["assets"]} == {
        "tracks_day_pmtiles", "grid_hour_pmtiles", "track_frame_hour",
        "grid_detail_bucket", "track_detail_bucket", "sar_unmatched_hour",
    }
    assert succeeded["root_manifest_bytes"] > 0
    assert s3.put_order[-1] == "deploy-assets/global-maritime/gfw-hourly/v3-shadow/manifest.json"
    assert result["logical_tile_report_count"] == 2
    assert list(settings.spool_root.iterdir()) == []


def test_ledger_gate_fails_before_report_network_and_preserves_failed_spool(tmp_path):
    settings = _settings(tmp_path)
    ledger = _FakeLedger(fail_first=True)
    client_factory_calls = []
    task = GFWHourlyPublishTask(
        settings,
        ledger=ledger,
        report_client_factory=lambda token: client_factory_calls.append(token),
        s3_client_factory=lambda: _FakeS3(),
        now=lambda: datetime(2026, 8, 25, tzinfo=timezone.utc),
    )
    with pytest.raises(RuntimeError, match="migration missing"):
        task.run()
    assert client_factory_calls == []
    spools = list(settings.spool_root.iterdir())
    assert len(spools) == 1
    assert json.loads((spools[0] / "spool.json").read_text())["status"] == "failed"


def test_keyboard_interrupt_marks_running_attempt_failed_before_cutover(tmp_path):
    settings = _settings(tmp_path)
    ledger = _FakeLedger()
    task = GFWHourlyPublishTask(
        settings,
        ledger=ledger,
        report_client_factory=lambda _token: _InterruptedReportClient(),
        s3_client_factory=lambda: _FakeS3(),
        now=lambda: datetime(2026, 8, 25, tzinfo=timezone.utc),
    )
    with pytest.raises(KeyboardInterrupt, match="operator cancelled"):
        task.run()
    assert [payload["status"] for payload in ledger.payloads] == ["running", "failed"]
    spools = list(settings.spool_root.iterdir())
    assert len(spools) == 1
    spool = json.loads((spools[0] / "spool.json").read_text())
    assert spool["status"] == "failed"
    assert "operator cancelled" in spool["error"]


def test_cutover_ledger_failure_retries_without_writing_failed_and_keeps_reconcile_state(tmp_path):
    settings = _settings(tmp_path)
    ledger = _CutoverLedgerFailure()
    s3 = _FakeS3()
    task = GFWHourlyPublishTask(
        settings,
        ledger=ledger,
        report_client_factory=lambda _token: _FakeReportClient(),
        s3_client_factory=lambda: s3,
        now=lambda: datetime(2026, 8, 25, tzinfo=timezone.utc),
        sleep=lambda _delay: None,
    )
    with pytest.raises(RuntimeError, match="temporary ledger outage"):
        task.run()
    assert [payload["status"] for payload in ledger.payloads] == [
        "running", "succeeded", "succeeded", "succeeded",
    ]
    assert "deploy-assets/global-maritime/gfw-hourly/v3-shadow/manifest.json" in s3.objects
    spools = list(settings.spool_root.iterdir())
    assert len(spools) == 1
    spool = json.loads((spools[0] / "spool.json").read_text())
    reconcile = json.loads((spools[0] / "reconcile-ledger.json").read_text())
    assert spool["status"] == "cutover_succeeded_ledger_pending"
    assert reconcile["status"] == "succeeded"


def test_failed_spool_prune_is_bounded_and_preserves_unknown_tree(tmp_path):
    root = tmp_path / "spool"
    old_name = "2026-08-20-11111111-1111-1111-1111-111111111111"
    old = root / old_name
    old.mkdir(parents=True)
    (old / "spool.json").write_text(json.dumps({
        "status": "failed", "failed_at": "2026-08-20T00:00:00+00:00"
    }))
    result = prune_expired_failed_spools(
        root, now=datetime(2026, 8, 30, tzinfo=timezone.utc), retention_days=7
    )
    assert result["pruned"] == [old_name]

    unknown = root / "2026-08-20-22222222-2222-2222-2222-222222222222"
    unknown.mkdir()
    (unknown / "spool.json").write_text(json.dumps({
        "status": "failed", "failed_at": "2026-08-20T00:00:00+00:00"
    }))
    (unknown / "operator-note.txt").write_text("keep")
    result = prune_expired_failed_spools(
        root, now=datetime(2026, 8, 30, tzinfo=timezone.utc), retention_days=7
    )
    assert result["warnings"]
    assert (unknown / "operator-note.txt").read_text() == "keep"
