"""Cloudflare Radar + IODA canonical internet-health contract tests."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import requests
import pytest
import yaml

import config
from collectors.base import BaseCollector
from collectors.internet_health import (
    CloudflareRadarCollector,
    IodaInternetHealthCollector,
    _cloudflare_incidents,
    _cloudflare_provider_normal,
    _cloudflare_series,
    _ioda_observations,
)
from collectors.registry import get_entry_by_name
from storage.supabase_tables import TABLE_MAP
from storage.supabase_writer import SupabaseWriter


FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_provider_jobs_are_independent_and_disabled_by_default():
    assert get_entry_by_name("cloudflare_radar") is not None
    assert get_entry_by_name("ioda_internet_health") is not None
    assert config.CLOUDFLARE_RADAR_ENABLED is False
    assert config.IODA_INTERNET_HEALTH_ENABLED is False
    assert TABLE_MAP["cloudflare_radar"]["is_multi_table"] is True
    assert TABLE_MAP["ioda_internet_health"]["is_multi_table"] is True


def test_token_is_only_attached_to_cloudflare_session(monkeypatch):
    monkeypatch.setattr(BaseCollector, "__init__", lambda self: None)
    monkeypatch.setattr(config, "CLOUDFLARE_RADAR_API_TOKEN", "test-secret")
    cloudflare = CloudflareRadarCollector()
    ioda = IodaInternetHealthCollector()
    assert cloudflare._session.headers["Authorization"] == "Bearer test-secret"
    assert "Authorization" not in ioda._session.headers


def test_cloudflare_series_preserves_null_and_provider_metadata():
    records, rejected = _cloudflare_series(
        _fixture("cloudflare_radar_netflows.json"),
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:40:00+00:00",
    )
    assert rejected == 0
    assert len(records) == 2
    assert records[0]["value"] == 41.5
    assert records[1]["value"] is None
    assert records[1]["quality_flags"]["missing_value"] is True
    assert records[0]["evidence_family"] == records[1]["evidence_family"] == "cloudflare"
    assert records[0]["metadata"]["normalization"] == "PERCENTAGE"
    assert records[0]["confidence"] == 1.0
    assert records[0]["metadata"]["confidence_info"]["level"] == 5


def test_cloudflare_unassigned_confidence_level_four_stays_null():
    payload = _fixture("cloudflare_radar_netflows.json")
    payload["result"]["meta"]["confidenceInfo"]["level"] = 4
    records, rejected = _cloudflare_series(
        payload,
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:40:00+00:00",
    )
    assert rejected == 0
    assert {record["confidence"] for record in records} == {None}
    assert records[0]["metadata"]["confidence_info"]["level"] == 4


def test_cloudflare_provider_event_is_single_family_watch_not_composite_incident():
    incidents = _cloudflare_incidents(
        _fixture("cloudflare_radar_anomalies.json"),
        "traffic_anomalies",
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:40:00+00:00",
        "2026-08-30T08:10:00+00:00",
        "2026-08-30T08:40:00+00:00",
    )
    assert len(incidents) == 1
    assert incidents[0]["entity_id"] == "TW"
    assert incidents[0]["_type"] == "observation"
    assert incidents[0]["signal"] == "traffic_anomaly"
    assert incidents[0]["reported_status"] == "watch"
    assert incidents[0]["incident_kind"] == "national_outage"
    assert incidents[0]["evidence_family"] == "cloudflare"
    assert "geom" not in incidents[0]


def test_cloudflare_ended_historical_event_does_not_block_provider_normal():
    events = _cloudflare_incidents(
        _fixture("cloudflare_radar_anomalies.json"),
        "traffic_anomalies",
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:15:00+00:00",
        "2026-08-30T08:05:00+00:00",
        "2026-08-30T08:15:00+00:00",
    )
    assert events == []
    rows, _ = _cloudflare_series(
        _fixture("cloudflare_radar_netflows.json"),
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:15:00+00:00",
    )
    endpoint_status = {
        "netflows": {"status": "succeeded", "records": len(rows)},
        "traffic_anomalies": {"status": "succeeded", "records": len(events)},
        "outages": {"status": "succeeded", "records": 0},
    }
    heartbeat = _cloudflare_provider_normal(
        rows,
        endpoint_status,
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:15:00+00:00",
        datetime.fromisoformat("2026-08-30T08:15:00+00:00"),
    )
    assert heartbeat is not None
    assert heartbeat["reported_status"] == "normal"


def test_cloudflare_provider_normal_requires_fresh_flow_and_two_empty_event_endpoints():
    rows, _ = _cloudflare_series(
        _fixture("cloudflare_radar_netflows.json"),
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:40:00+00:00",
    )
    status = {
        "netflows": {"status": "succeeded", "records": 2},
        "traffic_anomalies": {"status": "succeeded", "records": 0},
        "outages": {"status": "succeeded", "records": 0},
    }
    heartbeat = _cloudflare_provider_normal(
        rows,
        status,
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:40:00+00:00",
        datetime.fromisoformat("2026-08-30T08:40:00+00:00"),
    )
    assert heartbeat is not None
    assert heartbeat["signal"] == "provider_status"
    assert heartbeat["reported_status"] == "normal"
    assert heartbeat["value"] is None
    assert heartbeat["metadata"]["scope"] == "single_provider_family"
    assert heartbeat["metadata"]["composite_detector"] == "deferred"

    status["outages"] = {"status": "failed", "error_code": "http_timeout"}
    assert _cloudflare_provider_normal(
        rows,
        status,
        "00000000-0000-0000-0000-000000000001",
        "2026-08-30T08:40:00+00:00",
        datetime.fromisoformat("2026-08-30T08:40:00+00:00"),
    ) is None


def test_cloudflare_unknown_event_envelope_is_not_treated_as_empty():
    with pytest.raises(ValueError, match="no recognized event list"):
        _cloudflare_incidents(
            {"success": True, "result": {"unexpected": []}},
            "outages",
            "00000000-0000-0000-0000-000000000001",
            "2026-08-30T08:40:00+00:00",
        )


def test_cloudflare_config_missing_writes_failed_ledger_without_http(monkeypatch):
    monkeypatch.setattr(config, "CLOUDFLARE_RADAR_API_TOKEN", "")
    collector = CloudflareRadarCollector.__new__(CloudflareRadarCollector)
    result = collector.collect()
    run = result["data"][0]
    assert run["_type"] == "source_run"
    assert run["status"] == "failed"
    assert run["error_code"] == "config_missing"
    assert result["_collector_error"]
    assert result["observation_count"] == 0


def test_cloudflare_endpoint_failure_isolated_and_raw_archive_retained(monkeypatch):
    monkeypatch.setattr(config, "CLOUDFLARE_RADAR_API_TOKEN", "test-secret")
    collector = CloudflareRadarCollector.__new__(CloudflareRadarCollector)

    requested_params = {}

    def fake_get(path, params):
        requested_params[path] = params
        if path == "netflows/timeseries":
            return _fixture("cloudflare_radar_netflows.json")
        if path == "traffic_anomalies":
            payload = _fixture("cloudflare_radar_anomalies.json")
            payload["result"]["annotations"] = [payload["result"]["annotations"][-1]]
            return payload
        raise requests.Timeout("outages unavailable test-secret")

    collector._get = fake_get
    result = collector.collect()
    assert result["run_status"] == "partial"
    assert result["observation_count"] == 3
    assert result["incident_count"] == 0
    assert set(result["raw_payload"]) == {"netflows", "traffic_anomalies"}
    assert result["endpoint_status"]["outages"]["error_code"] == "http_timeout"
    assert "test-secret" not in result["endpoint_status"]["outages"]["error"]
    assert "dateStart" not in requested_params["traffic_anomalies"]
    assert "dateEnd" not in requested_params["traffic_anomalies"]
    assert requested_params["traffic_anomalies"]["limit"] == 100
    assert "dateStart" in requested_params["annotations/outages"]
    assert "_collector_error" not in result


def test_ioda_observed_nested_envelope_preserves_trailing_nulls():
    payload = _fixture("ioda_country_tw_signals.json")
    records, rejected, source_updated_at = _ioda_observations(
        payload,
        "00000000-0000-0000-0000-000000000002",
        "2026-08-30T08:40:01+00:00",
    )
    assert rejected == 0
    assert len(records) == 9
    assert all(record["evidence_family"] == "ioda" for record in records)
    assert {record["reported_status"] for record in records} == {"unknown"}
    assert all(record["entity_id"] == "TW" for record in records)
    assert any(record["value"] is None for record in records)
    assert source_updated_at is not None
    bgp_missing = [record for record in records if record["signal"] == "bgp_visibility" and record["value"] is None]
    assert bgp_missing[0]["quality_flags"] == {"missing_value": True}
    trailing = bgp_missing[-1]
    assert trailing["quality_flags"] == {"missing_value": True, "trailing_null": True}
    assert trailing["source_updated_at"] == source_updated_at
    loss_records = [record for record in records if record["signal"] == "ping_slash24_loss_all"]
    merit_records = [record for record in records if record["signal"] == "merit_nt_raw"]
    assert loss_records[0]["source_updated_at"] < source_updated_at
    assert merit_records[0]["source_updated_at"] < source_updated_at
    assert {record["source_updated_at"] for record in loss_records} == {loss_records[0]["observed_at"]}
    assert {record["source_updated_at"] for record in merit_records} == {merit_records[0]["observed_at"]}
    loss = next(record for record in records if record["signal"] == "ping_slash24_loss_all" and record["value"] == 1.25)
    assert loss["sample_count"] == 41
    assert loss["unit"] == "percent"
    assert "geom" not in loss


def test_ioda_alerts_are_schema_gated_and_signal_failure_is_fail_closed(monkeypatch):
    monkeypatch.setattr(config, "IODA_ALERTS_ENABLED", False)
    collector = IodaInternetHealthCollector.__new__(IodaInternetHealthCollector)
    collector._get = lambda *_args, **_kwargs: (_ for _ in ()).throw(requests.Timeout("signals unavailable"))
    result = collector.collect()
    run = result["data"][0]
    assert run["status"] == "failed"
    assert run["error_code"] == "http_timeout"
    assert result["endpoint_status"]["alerts"] == {"status": "disabled", "detail": "schema_unvalidated"}
    assert result["_collector_error"]


def test_ioda_all_null_signals_are_stale_not_normal(monkeypatch):
    monkeypatch.setattr(config, "IODA_ALERTS_ENABLED", False)
    payload = _fixture("ioda_country_tw_signals.json")
    for group in payload["data"]:
        for series in group:
            series["values"] = [None, None]
    collector = IodaInternetHealthCollector.__new__(IodaInternetHealthCollector)
    collector._get = lambda *_args, **_kwargs: payload
    result = collector.collect()
    run = result["data"][0]
    assert run["status"] == "partial"
    assert run["error_code"] == "stale"
    assert run["source_updated_at"] is None
    observations = [row for row in result["data"] if row.get("_type") == "observation"]
    assert observations
    assert {row["reported_status"] for row in observations} == {"unknown"}
    assert all(row["value"] is None for row in observations)


def test_writer_is_atomic_preserves_null_and_delegates_current_to_platform_trigger(monkeypatch):
    calls = []

    def fake_execute_values(_cur, sql, values, **kwargs):
        calls.append((sql, values, kwargs))
        return None

    @contextmanager
    def fake_txn():
        yield object()

    monkeypatch.setattr("storage.supabase_writer.execute_values", fake_execute_values)
    writer = object.__new__(SupabaseWriter)
    writer._txn = lambda _conn: fake_txn()
    run = {
        "_type": "source_run", "run_id": "00000000-0000-0000-0000-000000000002",
        "source": "ioda", "started_at": "2026-08-30T08:40:00+00:00",
        "finished_at": "2026-08-30T08:40:01+00:00", "status": "succeeded",
        "records_received": 1, "records_written": 1, "records_rejected": 0, "metadata": {},
    }
    observation = {
        "_type": "observation", "run_id": run["run_id"], "source": "ioda",
        "evidence_family": "ioda", "source_observation_id": "ioda:test",
        "entity_type": "country", "entity_id": "TW", "entity_name": "Taiwan",
        "signal": "bgp_visibility", "observed_at": "2026-08-30T08:30:00+00:00",
        "window_start": "2026-08-30T08:30:00+00:00", "value": None,
        "unit": "provider_native", "reported_status": "unknown", "stale_after_seconds": 3600,
        "source_updated_at": "2026-08-30T08:25:00+00:00", "collected_at": run["started_at"],
        "quality_flags": {"missing_value": True}, "metadata": {},
    }
    writer._write_multi_table(None, "ioda_internet_health", [run, observation])
    observation_call = next(call for call in calls if "internet_health_observations" in call[0])
    # value is the 12th canonical observation column and must remain NULL.
    assert observation_call[1][0][11] is None
    # Migration 379 owns current via a monotonic observation trigger; the
    # collector must not bypass that 11-column projection with its own upsert.
    assert not any("INSERT INTO live.internet_health_current" in call[0] for call in calls)
    assert "ON CONFLICT (source,entity_type,entity_id,signal,observed_at)" in observation_call[0]
    assert sum(1 for call in calls if "internet_health_source_runs" in call[0]) == 1


def test_monitor_and_archive_registries_cover_both_provider_jobs():
    repo_dir = Path(__file__).resolve().parent.parent
    config_dir = repo_dir / "config"
    cross_layer = yaml.safe_load((config_dir / "cross_layer_map.yaml").read_text(encoding="utf-8"))
    realtime = yaml.safe_load((config_dir / "realtime_tables.yaml").read_text(encoding="utf-8"))["tables"]
    backup = yaml.safe_load((config_dir / "backup_manifest.yaml").read_text(encoding="utf-8"))
    expected = {
        "live.internet_health_source_runs",
        "live.internet_health_observations",
        "live.internet_health_current",
        "live.internet_health_incidents",
    }
    for collector_name in ("cloudflare_radar", "ioda_internet_health"):
        assert cross_layer[collector_name]["enabled"] is False
        assert set(cross_layer[collector_name]["supabase_tables"]) == expected
    monitored = {f"{row['schema']}.{row['table']}" for row in realtime}
    assert expected <= monitored
    assert "live.internet_health_current" in backup["exclude"]
    assert expected - {"live.internet_health_current"} <= set(backup["archive_py_covered"])
    zeabur_env = json.loads((repo_dir / "zeabur.json").read_text(encoding="utf-8"))["env"]
    assert "CLOUDFLARE_RADAR_API_TOKEN" in zeabur_env
    assert zeabur_env["CLOUDFLARE_RADAR_ENABLED"]["default"] == "false"
    assert zeabur_env["IODA_INTERNET_HEALTH_ENABLED"]["default"] == "false"
    assert zeabur_env["IODA_ALERTS_ENABLED"]["default"] == "false"
