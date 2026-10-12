from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import ssl

import config
from collectors.cwa_marine_observation import CwaMarineObservationCollector, _CwaTlsAdapter
from collectors.isohe_port_marine import IsohePortMarineCollector
from collectors.marine_observation import cwa_metadata_to_stations, cwa_readings_to_long, isohe_payload_to_long
from collectors.registry import get_entry_by_name
from storage.supabase_tables import TABLE_MAP
from storage import marine_write
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


def test_isohe_actual_datas_envelope_uses_top_level_coordinates_and_compact_time():
    payload = [{
        "Port": "臺北港", "Station_Longitude": 121.37277, "Station_Latitude": 25.18055,
        "Datas": [{"DateTime": 20260826160000, "Hs_m": 0.7, "Tp_sec": 13.4,
                   "Wave_Direction_degree": 348.0, "Wave_Direction": "北北西"}],
    }]
    stations, rows = isohe_payload_to_long("TP", "wave", payload, "2026-08-26T16:42:00+08:00")
    assert stations[0]["longitude"] == 121.37277 and stations[0]["latitude"] == 25.18055
    assert {row["metric_code"] for row in rows} == {"wave_height", "wave_period", "wave_direction_deg"}
    direction = next(row for row in rows if row["metric_code"] == "wave_direction_deg")
    assert direction["value_numeric"] == 348.0
    assert direction["observed_at"] == "2026-08-26T16:00:00+08:00"


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


class _FakeCursor:
    def __init__(self, latest_rows=()):
        self.executed = []
        self._latest_rows = list(latest_rows)

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._latest_rows


def _station(**overrides):
    row = {
        "_type": "station", "station_uid": "cwa:A001", "source_network": "cwa",
        "source_station_id": "A001", "origin_org": "WRA", "distribution_org": "CWA",
        "station_type": "buoy", "name_zh": "測試站", "aliases": [], "longitude": 121.2,
        "latitude": 25.1, "observed_elements": [], "provenance": {},
        "first_seen_at": "2026-08-26T12:15:00+08:00",
        "last_seen_at": "2026-08-26T12:15:00+08:00",
    }
    row.update(overrides)
    return row


def _reading(**overrides):
    row = {
        "_type": "reading", "station_uid": "cwa:A001", "source_network": "cwa",
        "source_station_id": "A001", "observed_at": "2026-08-26T12:00:00+08:00",
        "metric_code": "wave_height", "depth_key": "surface", "value_raw": "1.2",
        "value_numeric": 1.2, "unit_source": "m", "unit_canonical": "m",
        "is_missing": False, "is_valid": True, "quality_flags": {"missing": False, "valid": True},
        "payload_sha256": "a" * 64, "collected_at": "2026-08-26T12:15:00+08:00",
        "longitude": 121.2, "latitude": 25.1,
    }
    row.update(overrides)
    return row


def _fake_writer(monkeypatch, latest_rows=()):
    calls, txns = [], []

    def fake_execute_values(_cur, sql, values, **kwargs):
        calls.append((sql, values, kwargs))

    @contextmanager
    def fake_txn():
        cur = _FakeCursor(latest_rows)
        txns.append(cur)
        yield cur

    monkeypatch.setattr("storage.supabase_writer.execute_values", fake_execute_values)
    writer = object.__new__(SupabaseWriter)
    writer._txn = lambda _conn: fake_txn()
    return writer, calls, txns


def test_multi_table_writer_keeps_missing_in_history_but_not_current(monkeypatch):
    writer, calls, _txns = _fake_writer(monkeypatch)
    valid = _reading()
    missing = _reading(metric_code="wind_speed", value_raw="-99", value_numeric=None,
                       is_missing=True, is_valid=False, missing_reason="source_missing_sentinel",
                       quality_flags={"missing": True, "valid": False})

    writer._write_multi_table(None, "cwa_marine_observation", [_station(), valid, missing])

    history_call = next(call for call in calls if "marine_observation_readings" in call[0])
    current_call = next(call for call in calls if "marine_observation_current" in call[0])
    assert len(history_call[1]) == 2
    assert len(current_call[1]) == 1
    assert "is_missing,is_valid,missing_reason" in history_call[0]


def test_readings_upsert_only_rewrites_changed_content_and_never_older_collection():
    sql = marine_write.readings_upsert_sql()
    where = sql.split(" WHERE ", 1)[1]
    assert "t.collected_at <= EXCLUDED.collected_at" in where
    assert "IS DISTINCT FROM" in where
    compared = where.split("IS DISTINCT FROM", 1)[0]
    assert "payload_sha256" not in compared and "collected_at" not in compared.split("AND", 1)[1]
    for col in ("value_raw", "value_numeric", "is_missing", "is_valid", "quality_flags", "geom_at_observation"):
        assert f"t.{col}" in compared and f"EXCLUDED.{col}" in where
    set_clause = sql.split(" DO UPDATE SET ", 1)[1].split(" WHERE ", 1)[0]
    assert "payload_sha256=EXCLUDED.payload_sha256" in set_clause
    assert "collected_at=EXCLUDED.collected_at" in set_clause


def test_current_upsert_guards_observed_and_collected_order_and_skips_unchanged():
    where = marine_write.current_upsert_sql().split(" WHERE ", 1)[1]
    assert "t.observed_at <= EXCLUDED.observed_at" in where
    assert "t.collected_at <= EXCLUDED.collected_at" in where
    compared = where.split("IS DISTINCT FROM", 1)[0].rsplit("AND", 1)[1]
    assert "payload_sha256" not in compared and "collected_at" not in compared


def test_stations_upsert_rejects_older_roster():
    sql = marine_write.stations_upsert_sql()
    assert "WHERE t.last_seen_at <= EXCLUDED.last_seen_at" in sql
    assert "first_seen_at=EXCLUDED" not in sql


def test_filter_recent_readings_uses_per_metric_db_latest_minus_lookback():
    latest = {("cwa:A001", "wave_height", "surface"): datetime(2026, 8, 26, 12, 0, tzinfo=timezone(timedelta(hours=8)))}
    rows = [
        _reading(observed_at="2026-08-26T08:00:00+08:00"),            # older than 12:00-3h -> dropped
        _reading(observed_at="2026-08-26T09:00:00+08:00"),            # exactly cutoff -> kept
        _reading(observed_at="2026-08-26T13:00:00+08:00"),            # new -> kept
        _reading(observed_at="2026-08-26T01:00:00+08:00", metric_code="wind_speed"),  # unknown key -> kept
        _reading(observed_at="not-a-time"),                           # unparseable -> kept
        _reading(observed_at="2026-08-26T01:00:00"),                  # naive -> kept
    ]
    kept = marine_write.filter_recent_readings(rows, latest, 3)
    assert [r["observed_at"] for r in kept] == [
        "2026-08-26T09:00:00+08:00", "2026-08-26T13:00:00+08:00",
        "2026-08-26T01:00:00+08:00", "not-a-time", "2026-08-26T01:00:00",
    ]


def test_writer_sends_only_recent_rows_and_sets_timeouts(monkeypatch):
    tz = timezone(timedelta(hours=8))
    latest = [("cwa:A001", "wave_height", "surface", datetime(2026, 8, 26, 12, 0, tzinfo=tz))]
    writer, calls, txns = _fake_writer(monkeypatch, latest)
    rows = [_reading(observed_at=f"2026-08-{d:02d}T{h:02d}:00:00+08:00") for d in (25, 26) for h in range(0, 24)]

    writer._write_multi_table(None, "cwa_marine_observation", [_station()] + rows)

    history_call = next(call for call in calls if "marine_observation_readings" in call[0])
    assert {v[3] for v in history_call[1]} == {f"2026-08-26T{h:02d}:00:00+08:00" for h in range(9, 24)}
    for cur in txns:
        stmts = [sql for sql, _ in cur.executed]
        assert stmts[0] == "SELECT set_config('statement_timeout', '120000', true)"
        assert stmts[1] == "SELECT set_config('lock_timeout', '10000', true)"
    reading_txn = txns[-1]
    assert reading_txn.executed[2][0] == marine_write.CUTOFF_SQL
    assert reading_txn.executed[2][1] == (["cwa:A001"],)


def test_station_roster_written_in_own_txn_only_when_hash_changes(monkeypatch):
    writer, calls, txns = _fake_writer(monkeypatch)
    writer._write_multi_table(None, "cwa_marine_observation", [_station(), _reading()])
    assert len(txns) == 2
    assert sum("marine_observation_stations" in c[0] for c in calls) == 1

    # Same roster, new seen timestamps -> no station transaction.
    later = _station(first_seen_at="2026-08-26T12:30:00+08:00", last_seen_at="2026-08-26T12:30:00+08:00")
    writer._write_multi_table(None, "cwa_marine_observation", [later, _reading()])
    assert len(txns) == 3
    assert sum("marine_observation_stations" in c[0] for c in calls) == 1

    # Content change -> station transaction again.
    moved = _station(longitude=121.3)
    writer._write_multi_table(None, "cwa_marine_observation", [moved, _reading()])
    assert len(txns) == 5
    assert sum("marine_observation_stations" in c[0] for c in calls) == 2


def test_station_write_due_refreshes_periodically():
    now = datetime(2026, 10, 12, 12, 0)
    assert marine_write.station_write_due("h", None, None, now)
    assert not marine_write.station_write_due("h", "h", now - timedelta(hours=1), now)
    assert marine_write.station_write_due("h", "h", now - marine_write.STATION_REFRESH, now)
    assert marine_write.station_write_due("h2", "h", now, now)


def test_station_roster_hash_ignores_seen_timestamps_and_order():
    a, b = _station(), _station(station_uid="cwa:B002", source_station_id="B002")
    later_a = _station(last_seen_at="2027-01-01T00:00:00+08:00", first_seen_at="2027-01-01T00:00:00+08:00")
    assert marine_write.station_roster_hash([a, b]) == marine_write.station_roster_hash([b, later_a])
    assert marine_write.station_roster_hash([a]) != marine_write.station_roster_hash([_station(name_zh="改名")])


def test_vm_marine_write_is_byte_identical_copy():
    root = Path(__file__).resolve().parents[1]
    assert (root / "storage/marine_write.py").read_bytes() == \
        (root / "external/isohe_port_marine_vm/marine_write.py").read_bytes()


def test_cwa_marine_uses_fixed_minutes_off_db_cron_peaks():
    import schedule
    import main

    assert CwaMarineObservationCollector.fixed_minutes == (7, 22, 37, 52)
    assert not {14, 15, 40} & set(CwaMarineObservationCollector.fixed_minutes)

    class _Sched:
        def submit(self, _collector):
            return None

    collector = object.__new__(CwaMarineObservationCollector)
    schedule.clear()
    try:
        main._schedule_collector(collector, _Sched())
        jobs = schedule.get_jobs()
        assert len(jobs) == 4
        assert sorted(job.at_time.minute for job in jobs) == [7, 22, 37, 52]
        assert all(job.unit == "hours" for job in jobs)
    finally:
        schedule.clear()
