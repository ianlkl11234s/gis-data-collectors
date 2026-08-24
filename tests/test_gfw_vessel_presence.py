import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import collectors.gfw_vessel_presence as gfw_module
from collectors.gfw_vessel_presence import GFWVesselPresenceCollector, _next_offset, _polygon
from storage.supabase_writer import SupabaseWriter
from tasks.archive import ArchiveTask


def test_defensive_fixture_parser_accepts_aliases_and_quality_flags():
    fixture = json.loads((Path(__file__).parent / "fixtures/gfw_presence_report.json").read_text())
    rows = GFWVesselPresenceCollector.normalize_entries(
        fixture, snapshot_date="2026-08-23", received_at="2026-08-24T00:00:00+00:00", zone="test"
    )
    assert len(rows) == 4
    assert rows[0]["mmsi"] == "416000001"
    assert rows[2]["vessel_id"] == "gfw-002"
    assert rows[3]["presence_quality"] == "suspect"
    assert rows[3]["quality_flags"] == ["grid_cell_center", "missing_or_invalid_coordinates"]
    assert all(len(row["record_hash"]) == 64 for row in rows)


def test_parser_accepts_official_dataset_version_wrapper():
    payload = {
        "entries": [{
            "public-global-presence:v3.0": [{
                "vesselId": "gfw-wrapped-1",
                "lat": 24.5,
                "lon": 123.1,
                "timestamp": "2026-08-19T12:00:00Z",
            }],
        }],
    }
    rows = GFWVesselPresenceCollector.normalize_entries(
        payload, snapshot_date="2026-08-19", received_at="2026-08-24T00:00:00+00:00", zone="test"
    )
    assert [row["vessel_id"] for row in rows] == ["gfw-wrapped-1"]


def test_raw_archive_license_gate_blocks_local_and_archive_task(monkeypatch, tmp_path):
    monkeypatch.setattr("config.GFW_RAW_ARCHIVE_ENABLED", False)
    collector = GFWVesselPresenceCollector.__new__(GFWVesselPresenceCollector)
    assert not collector.should_persist_local()

    date_dir = tmp_path / "gfw_vessel_presence" / "2020" / "01" / "02"
    date_dir.mkdir(parents=True)
    (date_dir / "gfw_vessel_presence_000000.json").write_text("{}")
    monkeypatch.setattr("config.LOCAL_DATA_DIR", tmp_path)

    class NoUpload:
        def archive_exists(self, *_args, **_kwargs):
            raise AssertionError("license-gated collector must not inspect/upload archive")

        def upload_archive(self, *_args, **_kwargs):
            raise AssertionError("license-gated collector must not upload archive")

    task = ArchiveTask.__new__(ArchiveTask)
    task.s3 = NoUpload()
    assert task._archive_to_s3() == {"uploaded": 0, "skipped": 0, "failed": 0}


def test_missing_token_never_calls_http(monkeypatch):
    monkeypatch.setattr("config.GFW_ACCESS_TOKEN", "")
    collector = GFWVesselPresenceCollector.__new__(GFWVesselPresenceCollector)
    collector._token = ""
    collector._session = type("NoHTTP", (), {"post": lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("live API called"))})()
    try:
        collector.collect()
    except RuntimeError as exc:
        assert "GFW_ACCESS_TOKEN" in str(exc)
    else:
        raise AssertionError("missing token must skip")


def test_report_request_uses_official_geojson_string_body(monkeypatch):
    captured = {}

    class Response:
        headers = {"x-datasets": "public-global-presence:v4"}
        def raise_for_status(self):
            pass
        def json(self):
            return {"entries": []}

    class Session:
        def post(self, url, **kwargs):
            captured.update({"url": url, **kwargs})
            return Response()

    collector = GFWVesselPresenceCollector.__new__(GFWVesselPresenceCollector)
    collector._token = "test-token"
    collector._session = Session()
    payload, resolved = collector._fetch_report(_polygon((24, 120, 25, 121)), "2026-08-19", "2026-08-20")
    assert payload == {"entries": []}
    assert resolved == "public-global-presence:v4"
    body = captured["json"]
    assert set(body) == {"geojson"}
    feature_collection = json.loads(body["geojson"])
    assert feature_collection["type"] == "FeatureCollection"
    assert feature_collection["features"][0]["geometry"]["type"] == "Polygon"


def test_nonzero_next_offset_is_detected_as_incomplete():
    assert _next_offset({"metadata": {"nextOffset": 100, "limit": 100, "total": 250}}) == "100"
    assert _next_offset({"nextOffset": 0, "entries": []}) is None


def test_paginated_response_cannot_be_marked_succeeded(monkeypatch):
    collector = GFWVesselPresenceCollector.__new__(GFWVesselPresenceCollector)
    collector._token = "test-token"
    collector._fetch_report = lambda *_args: ({
        "entries": [{"vesselId": "truncated", "lat": 24.5, "lon": 123.1}],
        "nextOffset": 100,
        "limit": 100,
        "total": 250,
    }, "public-global-presence:v4")
    result = collector.collect()
    assert result["status"] == "failed"
    assert result["record_count"] == 0
    assert "nextOffset=100" in result["_collector_error"]


def test_snapshot_date_uses_utc_lag_day_and_records_lag(monkeypatch):
    frozen = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen if tz else frozen.replace(tzinfo=None)

    monkeypatch.setattr(gfw_module, "datetime", FrozenDateTime)
    monkeypatch.setattr("config.GFW_DATA_LAG_DAYS", 5)
    collector = GFWVesselPresenceCollector.__new__(GFWVesselPresenceCollector)
    collector._token = "test-token"
    collector._fetch_report = lambda *_args: ({"entries": []}, "public-global-presence:v4")
    result = collector.collect()
    run = result["data"][0]
    assert run["snapshot_date"] == "2026-08-19"
    assert run["query_parameters"]["data_lag_days"] == 5
    assert run["quality_summary"]["data_lag_days"] == 5


class _Cursor:
    def __init__(self):
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.sql.append((sql, params))


class _Conn:
    def __init__(self):
        self.autocommit = True
        self.cursor_obj = _Cursor()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass


def test_writer_gfw_contract_inserts_geom_and_current_monotonic(monkeypatch):
    calls = []

    def capture(cur, sql, values, **kwargs):
        calls.append((sql, values, kwargs.get("template")))

    monkeypatch.setattr("storage.supabase_writer.execute_values", capture)
    writer = SupabaseWriter.__new__(SupabaseWriter)
    writer._pool = type("Pool", (), {"statement_timeout_ms": 1000})()
    run_id = "00000000-0000-0000-0000-000000000001"
    records = [{
        "_type": "run", "run_id": run_id, "provider": "global_fishing_watch",
        "source_dataset_id": "public-global-presence:latest", "snapshot_date": "2026-08-23",
        "status": "succeeded", "started_at": "2026-08-24T00:00:00+00:00", "completed_at": "2026-08-24T00:01:00+00:00",
        "source_window_start": "2026-08-23", "source_window_end": "2026-08-24", "query_parameters": {},
        "result_count": 1, "duplicate_count": 0, "rejected_count": 0, "response_sha256": "a" * 64,
        "quality_summary": {},
    }, {
        "_type": "snapshot", "run_id": run_id, "snapshot_date": "2026-08-23", "provider": "global_fishing_watch",
        "source_dataset_id": "public-global-presence:latest", "vessel_id": "v-1", "mmsi": "416000001",
        "observed_at": "2026-08-23T12:00:00+00:00", "received_at": "2026-08-24T00:00:00+00:00",
        "source_event_key": "e" * 64, "record_hash": "b" * 64, "ship_name": "TEST", "vessel_type": "cargo",
        "flag": "TW", "longitude": 123.1, "latitude": 24.5, "presence_quality": "accepted",
        "quality_flags": [], "source_properties": {}, "raw_archive_key": None,
    }]
    conn = _Conn()
    writer._write_multi_table(conn, "gfw_vessel_presence", records)
    assert len(calls) == 3  # run, immutable snapshot, current projection
    partition_call = next(
        call for call in conn.cursor_obj.sql
        if call[0].startswith("SELECT live.create_gfw_vessel_presence_snapshot_partition")
    )
    assert partition_call[1] == ("2026-08-23",)
    assert all("ST_GeomFromText" in (call[2] or "") for call in calls[1:])
    assert "source_snapshot_date" in calls[2][0] and "observed_at <= EXCLUDED.observed_at" in calls[2][0]


def test_writer_partial_run_records_ledger_only(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "storage.supabase_writer.execute_values",
        lambda cur, sql, values, **kwargs: calls.append((sql, values, kwargs.get("template"))),
    )
    writer = SupabaseWriter.__new__(SupabaseWriter)
    writer._pool = type("Pool", (), {"statement_timeout_ms": 1000})()
    run_id = "00000000-0000-0000-0000-000000000002"
    records = [{
        "_type": "run", "run_id": run_id, "provider": "global_fishing_watch",
        "source_dataset_id": "public-global-presence:latest", "snapshot_date": "2026-08-19",
        "status": "partial", "started_at": "2026-08-24T00:00:00+00:00",
        "completed_at": "2026-08-24T00:01:00+00:00", "source_window_start": "2026-08-19",
        "source_window_end": "2026-08-20", "query_parameters": {}, "result_count": 1,
        "duplicate_count": 0, "rejected_count": 0, "response_sha256": "a" * 64,
        "quality_summary": {"errors": ["one corridor failed"]},
    }, {
        "_type": "snapshot", "run_id": run_id, "snapshot_date": "2026-08-19",
        "provider": "global_fishing_watch", "source_dataset_id": "public-global-presence:latest",
        "vessel_id": "partial-vessel",
    }]
    conn = _Conn()
    writer._write_multi_table(conn, "gfw_vessel_presence", records)
    assert len(calls) == 1
    assert "gfw_vessel_presence_runs" in calls[0][0]
    assert all("create_gfw_vessel_presence_snapshot_partition" not in sql for sql, _ in conn.cursor_obj.sql)
