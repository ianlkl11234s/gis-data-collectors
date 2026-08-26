from contextlib import contextmanager
import ssl

import config
from collectors.cwa_marine_observation import CwaMarineObservationCollector, _CwaTlsAdapter
from collectors.isohe_port_marine import IsohePortMarineCollector
from collectors.marine_observation import cwa_metadata_to_stations, cwa_readings_to_long, isohe_payload_to_long
from collectors.registry import get_entry_by_name
from storage.supabase_tables import TABLE_MAP
from storage.supabase_writer import SupabaseWriter


def test_cwa_wide_readings_become_long_and_missing_is_not_zero():
    payload = {"records": {"Station": [{
        "StationID": "A001", "DateTime": "2026-08-26T12:00:00+08:00",
        "WaveHeight": "1.2", "WindSpeed": "-99", "CurrentSpeed": "0",
    }]}}
    rows = cwa_readings_to_long(payload, "2026-08-26T12:15:00+08:00")
    wave = next(r for r in rows if r["metric_code"] == "wave_height")
    missing_wind = next(r for r in rows if r["metric_code"] == "wind_speed")
    current = next(r for r in rows if r["metric_code"] == "current_speed")
    assert wave["station_uid"] == "cwa:A001" and wave["value_numeric"] == 1.2
    assert missing_wind["value_numeric"] is None and missing_wind["quality_flags"]["missing"] is True
    assert missing_wind["is_missing"] is True and missing_wind["is_valid"] is False
    assert current["value_numeric"] == 0.0  # real zero must remain zero


def test_cwa_station_metadata_retains_origin_and_distributor():
    xml = b"""<cwaopendata><dataset><location><stationID>T001</stationID><locationName>Test</locationName><longitude>121.2</longitude><latitude>25.1</latitude><affiliation>WRA</affiliation><stationStatus>1</stationStatus></location></dataset></cwaopendata>"""
    stations = cwa_metadata_to_stations(xml, "2026-08-26T12:15:00+08:00")
    assert stations[0]["station_uid"] == "cwa:T001"
    assert stations[0]["origin_org"] == "WRA" and stations[0]["distribution_org"] == "CWA"
    assert stations[0]["first_seen_at"] == stations[0]["last_seen_at"]


def test_cwa_actual_nested_contract_is_parsed():
    payload = {"Records": {"SeaSurfaceObs": {"Location": [{
        "Station": {"StationID": "46761F"},
        "StationObsTimes": {"StationObsTime": [{
            "DateTime": "2026-08-26T12:00:00+08:00",
            "WeatherElements": {"WaveHeight": "1.4", "PrimaryAnemometer": {"WindSpeed": "7.5"}},
        }]},
    }]}}}
    rows = cwa_readings_to_long(payload, "2026-08-26T12:15:00+08:00")
    assert next(r for r in rows if r["metric_code"] == "wave_height")["value_numeric"] == 1.4
    assert next(r for r in rows if r["metric_code"] == "wind_speed")["value_numeric"] == 7.5


def test_isohe_keeps_tide_datums_and_negative_999_missing():
    payload = [{"DateTime": "2026-08-26T12:00:00+08:00", "Tide_TWVD_m": "1.1", "Tide_CDL_m": "-999", "Tide_REF_m": "0.8", "Station_Longitude": 121.5, "Station_Latitude": 25.2}]
    stations, rows = isohe_payload_to_long("TP", "tide", payload, "2026-08-26T12:15:00+08:00")
    assert stations[0]["station_uid"] == "isohe:tp:tide"
    assert stations[0]["longitude"] == 121.5 and stations[0]["latitude"] == 25.2
    assert stations[0]["first_seen_at"] == stations[0]["last_seen_at"]
    assert {r["metric_code"] for r in rows} == {"tide_twvd", "tide_cdl", "tide_ref"}
    assert next(r for r in rows if r["metric_code"] == "tide_cdl")["value_numeric"] is None
    assert next(r for r in rows if r["metric_code"] == "tide_cdl")["is_missing"] is True


def test_isohe_direction_alias_and_current_unit_are_canonicalized():
    payload = [{"DateTime": "2026-08-26T12:00:00+08:00", "Velocity_cms": "45",
                "Current_Direction": "180", "Station_Longitude": 121.5, "Station_Latitude": 25.2}]
    _stations, rows = isohe_payload_to_long("TP", "current", payload, "2026-08-26T12:15:00+08:00")
    speed = next(r for r in rows if r["metric_code"] == "current_speed")
    direction = next(r for r in rows if r["metric_code"] == "current_direction_deg")
    assert speed["value_numeric"] == 0.45 and speed["unit_source"] == "cm/s" and speed["unit_canonical"] == "m/s"
    assert direction["value_numeric"] == 180.0


def test_collectors_are_registered_as_shared_canonical_multi_table_writers():
    assert get_entry_by_name("cwa_marine_observation") is not None
    assert get_entry_by_name("isohe_port_marine") is not None
    assert TABLE_MAP["cwa_marine_observation"]["is_multi_table"] is True
    assert TABLE_MAP["isohe_port_marine"]["is_multi_table"] is True


def test_marine_collectors_fail_closed_on_db_and_default_to_three_day_local_retention():
    assert CwaMarineObservationCollector.require_db_write(None) is True
    assert IsohePortMarineCollector.require_db_write(None) is True
    assert config.get_retention_days("cwa_marine_observation") == 3
    assert config.get_retention_days("isohe_port_marine") == 3


def test_cwa_legacy_tls_adapter_keeps_ca_and_hostname_verification_enabled():
    adapter = _CwaTlsAdapter()
    assert adapter._ssl_context.verify_mode == ssl.CERT_REQUIRED
    assert adapter._ssl_context.check_hostname is True


def test_multi_table_writer_keeps_missing_in_history_but_not_current(monkeypatch):
    calls = []

    def fake_execute_values(_cur, sql, values, **kwargs):
        calls.append((sql, values, kwargs))

    @contextmanager
    def fake_txn():
        yield object()

    monkeypatch.setattr("storage.supabase_writer.execute_values", fake_execute_values)
    writer = object.__new__(SupabaseWriter)
    writer._txn = lambda _conn: fake_txn()
    station = {
        "_type": "station", "station_uid": "cwa:A001", "source_network": "cwa",
        "source_station_id": "A001", "origin_org": "WRA", "distribution_org": "CWA",
        "station_type": "buoy", "name_zh": "測試站", "aliases": [], "longitude": 121.2,
        "latitude": 25.1, "observed_elements": [], "provenance": {},
        "first_seen_at": "2026-08-26T12:15:00+08:00",
        "last_seen_at": "2026-08-26T12:15:00+08:00",
    }
    valid = {
        "_type": "reading", "station_uid": "cwa:A001", "source_network": "cwa",
        "source_station_id": "A001", "observed_at": "2026-08-26T12:00:00+08:00",
        "metric_code": "wave_height", "depth_key": "surface", "value_raw": "1.2",
        "value_numeric": 1.2, "unit_source": "m", "unit_canonical": "m",
        "is_missing": False, "is_valid": True, "quality_flags": {"missing": False, "valid": True},
        "payload_sha256": "a" * 64, "collected_at": "2026-08-26T12:15:00+08:00",
        "longitude": 121.2, "latitude": 25.1,
    }
    missing = dict(valid, metric_code="wind_speed", value_raw="-99", value_numeric=None,
                   is_missing=True, is_valid=False, missing_reason="source_missing_sentinel",
                   quality_flags={"missing": True, "valid": False})

    writer._write_multi_table(None, "cwa_marine_observation", [station, valid, missing])

    history_call = next(call for call in calls if "marine_observation_readings" in call[0])
    current_call = next(call for call in calls if "marine_observation_current" in call[0])
    assert len(history_call[1]) == 2
    assert len(current_call[1]) == 1
    assert "is_missing,is_valid,missing_reason" in history_call[0]
