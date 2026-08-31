"""RIPE Atlas internal-only aggregate collector contract tests."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

import config
from collectors.ripe_atlas_internet_health import (
    RipeAtlasInternetHealthCollector,
    _atlas_measurements,
    _normalize_results,
    load_ripe_roster,
)
from collectors.registry import get_entry_by_name
from storage.supabase_tables import TABLE_MAP


UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures"


def _fixture():
    return json.loads((FIXTURES / "ripe_atlas_ping_results.json").read_text(encoding="utf-8"))


def _approved_roster(path: Path) -> Path:
    roster = {
        "schema_version": "ripe_internet_health_roster.v1",
        "version": 7,
        "review_status": "approved",
        "internal_only": True,
        "country_code": "TW",
        "ripe_atlas": {
            "measurements": [{
                "measurement_id": 1001,
                "measurement_type": "ping",
                "address_family": 4,
                "interval_seconds": 300,
                "target_group": "documentation_target",
                "probes": [
                    {"probe_id": 11, "asn": 64510},
                    {"probe_id": 12, "asn": 64511},
                ],
            }],
        },
        "ripe_ris_live": {"prefixes": ["203.0.113.0/24"], "origin_asns": []},
    }
    path.write_text(yaml.safe_dump(roster, sort_keys=False), encoding="utf-8")
    return path


def test_atlas_registry_table_map_and_defaults_are_safe():
    entry = get_entry_by_name("ripe_atlas_internet_health")
    assert entry is not None
    assert entry.config_prefix == "RIPE_ATLAS_INTERNET_HEALTH"
    assert config.RIPE_ATLAS_INTERNET_HEALTH_ENABLED is False
    assert TABLE_MAP["ripe_atlas_internet_health"]["is_multi_table"] is True


def test_committed_roster_is_approved_bounded_and_internal_only():
    roster = yaml.safe_load(
        (Path(__file__).resolve().parent.parent / "config" / "ripe_internet_health.yaml")
        .read_text(encoding="utf-8")
    )
    assert roster["review_status"] == "approved"
    assert roster["internal_only"] is True
    measurements = roster["ripe_atlas"]["measurements"]
    assert [(item["measurement_id"], item["address_family"]) for item in measurements] == [
        (1001, 4), (2001, 6),
    ]
    assert 1 <= len(measurements[0]["probes"]) <= 100
    assert 1 <= len(measurements[1]["probes"]) <= 100
    assert roster["ripe_ris_live"]["prefixes"] == []
    assert 1 <= len(roster["ripe_ris_live"]["origin_asns"]) <= 64


def test_reviewed_roster_is_versioned_and_ping_only(tmp_path):
    roster = load_ripe_roster(_approved_roster(tmp_path / "roster.yaml"))
    measurements = _atlas_measurements(roster)
    assert measurements[0]["measurement_id"] == 1001
    assert measurements[0]["probes"][0] == {"probe_id": 11, "asn": 64510}
    roster["ripe_atlas"]["measurements"][0]["measurement_type"] = "traceroute"
    try:
        _atlas_measurements(roster)
    except ValueError as exc:
        assert "ping" in str(exc)
    else:
        raise AssertionError("unreviewed Atlas result types must fail closed")


def test_atlas_overlap_dedup_and_null_semantics(tmp_path):
    roster = load_ripe_roster(_approved_roster(tmp_path / "roster.yaml"))
    measurements = _atlas_measurements(roster)
    records, rejected, duplicates, latest = _normalize_results(
        {1001: _fixture()},
        measurements,
        run_id="00000000-0000-0000-0000-000000000101",
        collected_at="2026-08-31T00:00:00+00:00",
    )
    assert rejected == 0
    assert duplicates == 1
    assert latest is not None
    assert len(records) == 8  # four signals across two 5-minute buckets
    first = records[:4]
    by_signal = {record["signal"]: record for record in first}
    assert by_signal["probe_connectivity_ratio_ipv4"]["value"] == 1.0
    assert by_signal["ping_success_ratio_ipv4"]["value"] == 0.5
    assert by_signal["median_rtt_ms_ipv4"]["value"] == 10.5
    assert by_signal["reachable_asn_ratio_ipv4"]["value"] == 0.5
    second_median = [record for record in records[4:] if record["signal"] == "median_rtt_ms_ipv4"][0]
    assert second_median["value"] is None
    assert second_median["quality_flags"] == {"missing_value": True}
    assert {record["reported_status"] for record in records} == {"unknown"}
    assert {record["source"] for record in records} == {"ripe_atlas"}
    assert {record["evidence_family"] for record in records} == {"ripe_atlas"}
    assert all(record["metadata"]["independence_group"] == "ripe_ncc" for record in records)
    assert all("geom" not in record for record in records)


def test_atlas_collector_isolates_measurement_failures_and_archives_raw(monkeypatch, tmp_path):
    roster_path = _approved_roster(tmp_path / "roster.yaml")
    roster = yaml.safe_load(roster_path.read_text(encoding="utf-8"))
    second = dict(roster["ripe_atlas"]["measurements"][0])
    second["measurement_id"] = 1002
    roster["ripe_atlas"]["measurements"].append(second)
    roster_path.write_text(yaml.safe_dump(roster, sort_keys=False), encoding="utf-8")
    monkeypatch.setattr(config, "RIPE_INTERNET_HEALTH_ROSTER_PATH", str(roster_path))
    collector = RipeAtlasInternetHealthCollector.__new__(RipeAtlasInternetHealthCollector)

    now = int(datetime.now(UTC).timestamp())
    rows = _fixture()[:2]
    for index, row in enumerate(rows):
        row["timestamp"] = now - 60 + index

    def fake_get(measurement, _start, _stop):
        if measurement["measurement_id"] == 1002:
            raise RuntimeError("bounded endpoint failure")
        return rows

    collector._get_results = fake_get
    result = collector.collect()
    run = result["data"][0]
    assert run["status"] == "partial"
    assert run["error_code"] == "endpoint_partial"
    assert result["observation_count"] == 4
    assert set(result["raw_payload"]) == {"1001"}
    assert run["metadata"]["public_visibility"] == "internal_only"
    assert "_collector_error" not in result


def test_atlas_pending_roster_records_config_failure(monkeypatch, tmp_path):
    pending = {
        "schema_version": "ripe_internet_health_roster.v1",
        "version": "pending-fixture",
        "review_status": "pending",
        "internal_only": True,
        "country_code": "TW",
        "ripe_atlas": {"measurements": []},
        "ripe_ris_live": {"prefixes": [], "origin_asns": []},
    }
    pending_path = tmp_path / "pending-roster.yaml"
    pending_path.write_text(yaml.safe_dump(pending, sort_keys=False), encoding="utf-8")
    collector = RipeAtlasInternetHealthCollector.__new__(RipeAtlasInternetHealthCollector)
    monkeypatch.setattr(config, "RIPE_INTERNET_HEALTH_ROSTER_PATH", str(pending_path))
    result = collector.collect()
    assert result["data"][0]["status"] == "failed"
    assert result["data"][0]["error_code"] == "config_missing"
    assert result["_collector_error"]


def test_atlas_optional_key_is_redacted_from_config_errors(monkeypatch, tmp_path):
    from collectors import ripe_atlas_internet_health as module

    monkeypatch.setattr(config, "RIPE_ATLAS_API_KEY", "test-atlas-secret")
    message = module._safe_error(RuntimeError("failed test-atlas-secret"))
    assert "test-atlas-secret" not in message
    assert "[redacted]" in message
