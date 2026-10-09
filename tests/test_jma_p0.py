"""日本氣象廳 P0 三 collector（jma_amedas / jma_warnings / jma_quake）單元測試。

不打網路、不打 DB：fixture 為 2026-10-09 實打 bosai JSON 裁剪樣本
（tests/fixtures/jma_p0_sample.json）；writer 以 mock conn 攔截 execute_values。
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.global_climate import jma_amedas, jma_quake, jma_warnings
from collectors.global_climate.jma_amedas import build_station_index, parse_amedas_map
from collectors.global_climate.jma_quake import (
    build_volcano_index, parse_cod, parse_quake_list, parse_tsunami_list, parse_volcano_warning,
)
from collectors.global_climate.jma_warnings import (
    KIND_NAMES, SENTINEL_CODE, parse_warning_map, sentinel_row,
)
from storage.supabase_tables import TABLE_MAP
from storage.supabase_writer import SupabaseWriter

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "jma_p0_sample.json").read_text())
TS = datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)
COLLECTED = "2026-10-09T15:00:00+08:00"


@pytest.fixture
def writer():
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._pool = MagicMock()
    w._pool.statement_timeout_ms = 30_000
    return w


@pytest.fixture
def capture(monkeypatch):
    captured: list[tuple[str, list]] = []
    monkeypatch.setattr(
        "storage.supabase_writer.execute_values",
        lambda cur, sql, values, page_size=100: captured.append((sql, list(values))),
    )
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = MagicMock()
    conn.cursor.return_value.__exit__.return_value = None
    return conn, captured


# ───────────────────────── AMeDAS ─────────────────────────

def _amedas_rows(observed_at="2026-10-09T15:40:00+09:00"):
    stations = build_station_index(FIXTURE["amedastable"])
    return parse_amedas_map(FIXTURE["amedas_map"], stations, observed_at=observed_at,
                            source_time=observed_at, collected_at=COLLECTED)


def test_amedas_station_index_converts_degree_minutes_and_snow_gauge():
    idx = build_station_index({
        "11001": {"elems": "11112010", "lat": [45, 31.2], "lon": [141, 56.1], "alt": 26,
                  "kjName": "宗谷岬", "enName": "Cape Soya"},
        "99999": {"elems": "11111110", "lat": [43, 3.0], "lon": [141, 19.8], "alt": 17,
                  "kjName": "雪站", "enName": "Snow"},
    })
    assert idx["11001"]["lat"] == pytest.approx(45.52)
    assert idx["11001"]["lon"] == pytest.approx(141.935)
    assert idx["11001"]["has_snow_gauge"] is False
    assert idx["99999"]["has_snow_gauge"] is True


def test_amedas_quality_none_is_null_not_zero_and_snow_null_without_gauge():
    stations = {
        "A": {"has_snow_gauge": False}, "B": {"has_snow_gauge": True},
    }
    data = {
        "A": {"temp": [12.3, 0], "humidity": [None, None], "snow1h": [0, None],
              "snow": [5, 0], "windDirection": [12, 0], "precipitation1h": [0.0, 0]},
        "B": {"temp": [1.0, None], "snow": [42, 0], "snow1h": [0, None], "snow24h": [3, 0]},
    }
    rows = {r["station_id"]: r for r in parse_amedas_map(
        data, stations, observed_at="2026-01-10T06:00:00+09:00",
        source_time="2026-01-10T06:00:00+09:00", collected_at=COLLECTED)}
    a, b = rows["A"], rows["B"]
    assert a["temp"] == 12.3 and a["humidity"] is None
    assert a["precip1h"] == 0.0          # 真 0 仍是 0
    assert a["wind_dir"] == 12 and isinstance(a["wind_dir"], int)
    assert all(a[f] is None for f in ("snow", "snow1h", "snow6h", "snow12h", "snow24h"))
    assert b["temp"] is None             # 品質碼 None → NULL
    assert b["snow"] == 42 and b["snow24h"] == 3
    assert b["snow1h"] is None           # 有積雪計但品質碼 None → NULL
    assert b["snow6h"] is None           # 來源沒有 → NULL


def test_amedas_fixture_rows_cover_all_stations_and_are_json_safe():
    rows = _amedas_rows()
    assert len(rows) == len(FIXTURE["amedas_map"])
    json.dumps(rows)  # collect() 回傳不可含 datetime/bytes
    assert all(r["lat"] is not None and r["lon"] is not None for r in rows)


def test_amedas_transform_only_keeps_top_of_hour_observations(writer):
    cur = [{**r, "_type": "current"} for r in _amedas_rows("2026-10-09T15:40:00+09:00")]
    off = [{**r, "_type": "observation"} for r in _amedas_rows("2026-10-09T15:40:00+09:00")]
    hour = [{**r, "_type": "observation"} for r in _amedas_rows("2026-10-09T15:00:00+09:00")]
    recs = writer._transform_jma_amedas({"data": cur + off + hour}, TS)
    kinds = [r["_type"] for r in recs]
    assert kinds.count("current") == len(cur)
    assert kinds.count("observation") == len(hour)
    assert all(r["observed_at"].startswith("2026-10-09T15:00") for r in recs if r["_type"] == "observation")


def test_amedas_multi_table_sql(writer, capture):
    conn, captured = capture
    cur = [{**r, "_type": "current"} for r in _amedas_rows("2026-10-09T15:00:00+09:00")]
    obs = [{**r, "_type": "observation"} for r in _amedas_rows("2026-10-09T15:00:00+09:00")]
    recs = writer._transform_jma_amedas({"data": cur + obs}, TS)
    writer._write_to_db(conn, "jma_amedas", recs, TS)
    sqls = {sql.split()[2]: (sql, vals) for sql, vals in captured}
    cur_sql, cur_vals = sqls["live.jma_amedas_current"]
    assert "ON CONFLICT (station_id) DO UPDATE SET" in cur_sql
    assert "updated_at=now()" in cur_sql
    assert "collected_at=EXCLUDED.collected_at" in cur_sql
    assert "temp=EXCLUDED.temp" in cur_sql and "snow24h=EXCLUDED.snow24h" in cur_sql
    assert len(cur_vals) == len(cur)
    obs_sql, obs_vals = sqls["live.jma_amedas_observations"]
    assert obs_sql.rstrip().endswith("ON CONFLICT DO NOTHING")
    assert len(obs_vals) == len(obs)


def test_amedas_collect_backfills_missing_hour(monkeypatch):
    calls = []

    def fake_fetch(session, path, as_text=False, **kw):
        calls.append(path)
        if path == jma_amedas.URL_LATEST_TIME:
            return "2026-01-01T00:40:00+09:00"
        if path == jma_amedas.URL_TABLE:
            return FIXTURE["amedastable"]
        return FIXTURE["amedas_map"]

    monkeypatch.setattr(jma_amedas, "fetch", fake_fetch)
    c = jma_amedas.JmaAmedasCollector.__new__(jma_amedas.JmaAmedasCollector)
    c.name, c._session = "jma_amedas", None
    c._stations, c._stations_loaded_at, c._hours_done = {}, None, set()
    out = c.collect()
    json.dumps(out)
    n = len(FIXTURE["amedas_map"])
    assert out["station_count"] == n and out["hourly_count"] == n
    assert "amedas/data/map/20260101004000.json" in calls
    assert "amedas/data/map/20260101000000.json" in calls
    assert out["stale"] is True  # latest_time 遠早於現在 → stale
    # 同一整點第二輪不再補抓
    calls.clear()
    out2 = c.collect()
    assert out2["hourly_count"] == 0
    assert "amedas/data/map/20260101000000.json" not in calls


# ───────────────────────── warnings ─────────────────────────

CTRL = "2026-10-09T06:25:28+00:00"


def test_kind_names_cover_fixture_codes_and_r8_levels():
    assert KIND_NAMES["29"] == "レベル２土砂災害注意報"
    assert KIND_NAMES["48"] == "レベル４高潮危険警報"
    assert KIND_NAMES["03"] == "レベル３大雨警報"
    assert KIND_NAMES["21"] == "乾燥注意報"


def test_warning_map_keeps_only_active_with_snapshot_control_datetime():
    rows, unknown = parse_warning_map(FIXTURE["warning_map"], FIXTURE["area"],
                                      control_datetime=CTRL, collected_at=COLLECTED)
    assert rows, "fixture 內有有效警報"
    assert not unknown
    assert {r["status"] for r in rows} <= {"発表", "継続"}
    assert all(r["control_datetime"] == CTRL for r in rows)
    assert {r["area_level"] for r in rows} <= {"class10", "class20"}
    assert all(r["kind_name"] for r in rows)
    assert all(r["office_code"] and r["area_name"] for r in rows)
    keys = [(r["area_code"], r["kind_code"]) for r in rows]
    assert len(keys) == len(set(keys))
    json.dumps(rows)


def test_warning_dedup_keeps_latest_report():
    def entry(report, status):
        return {"reportDatetime": report, "publishingOffice": "X",
                "warning": {"class20Items": [{"areaCode": "0110000", "kinds": [{"code": "15", "status": status}]}]}}
    rows, _ = parse_warning_map(
        [entry("2026-10-09T09:00:00+09:00", "発表"), entry("2026-10-09T12:00:00+09:00", "継続"),
         entry("2026-10-09T10:00:00+09:00", "発表")],
        {}, control_datetime=CTRL, collected_at=COLLECTED)
    assert len(rows) == 1
    assert rows[0]["status"] == "継続"
    assert rows[0]["report_datetime"] == "2026-10-09T12:00:00+09:00"


def test_warning_all_cleared_yields_no_rows_and_sentinel_shape():
    cleared = [e for e in FIXTURE["warning_map"]
               if not any(k.get("status") in ("発表", "継続")
                          for items in e["warning"].values() for it in items for k in it["kinds"])]
    rows, _ = parse_warning_map(cleared, FIXTURE["area"], control_datetime=CTRL, collected_at=COLLECTED)
    assert rows == []
    s = sentinel_row(CTRL, COLLECTED)
    assert s["area_code"] == SENTINEL_CODE and s["kind_code"] == SENTINEL_CODE and s["status"] == "none"
    assert s["control_datetime"] == CTRL


def _warnings_collector(monkeypatch, tmp_path, map_entries):
    def fake_fetch(session, path, as_text=False, **kw):
        if path == jma_warnings.URL_MAP_TIME:
            return {"latestControlDatetime": "2026-10-09T06:25:28Z"}
        if path == jma_warnings.URL_AREA:
            return FIXTURE["area"]
        if path == jma_warnings.URL_MAP:
            return map_entries
        raise AssertionError(path)

    monkeypatch.setattr(jma_warnings, "fetch", fake_fetch)
    c = jma_warnings.JmaWarningsCollector.__new__(jma_warnings.JmaWarningsCollector)
    c.name, c._session = "jma_warnings", None
    c._area, c._area_loaded_at, c._last_control, c._pending_control = {}, None, None, None
    c.state_path = tmp_path / "state" / "jma_warnings_state.json"
    return c


def _fake_base_run(result_error=None):
    def run(self):
        out = self.collect()
        return {"error": result_error} if result_error else {k: v for k, v in out.items() if k != "data"}
    return run


def test_warnings_collect_no_change_and_survives_restart(monkeypatch, tmp_path):
    from collectors.base import BaseCollector
    monkeypatch.setattr(BaseCollector, "run", _fake_base_run())
    c = _warnings_collector(monkeypatch, tmp_path, FIXTURE["warning_map"])
    first = c.collect()
    assert first["data"] and first["control_datetime"] == CTRL
    c._pending_control = None
    c.run()  # 成功的 run 才會前進 state
    second = c.collect()
    assert "data" not in second and second["no_change"] is True
    restarted = _warnings_collector(monkeypatch, tmp_path, FIXTURE["warning_map"])
    assert "data" not in restarted.collect()


def test_warnings_state_not_advanced_when_db_write_fails(monkeypatch, tmp_path):
    from collectors.base import BaseCollector
    monkeypatch.setattr(BaseCollector, "run", _fake_base_run("required Supabase write failed"))
    c = _warnings_collector(monkeypatch, tmp_path, FIXTURE["warning_map"])
    c.run()
    assert not c.state_path.exists() and c._last_control is None
    assert c.collect()["data"]  # 下一輪仍會重抓同一快照


def test_warnings_require_db_write():
    assert jma_warnings.JmaWarningsCollector.require_db_write(None) is True


def test_warnings_collect_writes_sentinel_when_nothing_active(monkeypatch, tmp_path):
    c = _warnings_collector(monkeypatch, tmp_path, [])
    out = c.collect()
    assert out["active_count"] == 0
    assert [r["area_code"] for r in out["data"]] == [SENTINEL_CODE]


def test_warnings_transform_and_do_nothing_sql(writer, capture):
    conn, captured = capture
    rows, _ = parse_warning_map(FIXTURE["warning_map"], FIXTURE["area"],
                                control_datetime=CTRL, collected_at=COLLECTED)
    rows.append(sentinel_row(CTRL, COLLECTED))
    recs = writer._transform_jma_warnings({"data": rows}, TS)
    assert len(recs) == len(rows)
    assert set(recs[0]) == set(TABLE_MAP["jma_warnings"]["columns"])
    writer._write_to_db(conn, "jma_warnings", recs, TS)
    sql, vals = captured[0]
    assert "INSERT INTO live.jma_warnings" in sql and "ON CONFLICT DO NOTHING" in sql
    assert len(vals) == len(rows)


# ───────────────────────── quake / tsunami / volcano ─────────────────────────

def test_parse_cod():
    assert parse_cod("+32.4+130.5-10000/") == (32.4, 130.5, 10.0)
    assert parse_cod("+43.2+145.7-60000/") == (43.2, 145.7, 60.0)
    assert parse_cod("-21.0-175.2/") == (-21.0, -175.2, None)
    assert parse_cod("") == (None, None, None)
    assert parse_cod(None) == (None, None, None)


def test_quake_list_parsing_keeps_nulls():
    rows = parse_quake_list(FIXTURE["quake_list"], COLLECTED)
    assert len(rows) == len(FIXTURE["quake_list"])
    full = rows[0]
    assert full["json_id"].endswith(".json") and full["event_id"]
    assert full["magnitude"] is not None and full["lat"] is not None
    assert isinstance(full["intensity_by_pref"], list)
    sokuho = [r for r in rows if r["title"] == "震度速報"][0]
    assert sokuho["lat"] is None and sokuho["magnitude"] is None and sokuho["depth_km"] is None
    bad = parse_quake_list([{**FIXTURE["quake_list"][0], "json": "x.json", "mag": "Ｍ不明", "cod": ""}], COLLECTED)
    assert bad[0]["magnitude"] is None and bad[0]["lat"] is None
    json.dumps(rows)


def test_volcano_parsing_and_missing_report_time_skipped():
    idx = build_volcano_index(FIXTURE["volcano_list"])
    rows, skipped = parse_volcano_warning(FIXTURE["volcano_warning"], idx, COLLECTED)
    assert skipped == 0 and len(rows) == len(FIXTURE["volcano_warning"])
    r = rows[0]
    assert r["volcano_code"] and r["report_time"] and r["level_code"] and r["level_name"]
    assert r["lat"] is not None and r["lon"] is not None
    no_time = copy.deepcopy(FIXTURE["volcano_warning"][:1])
    no_time[0].pop("reportDatetime")
    rows2, skipped2 = parse_volcano_warning(no_time, idx, COLLECTED)
    assert rows2 == [] and skipped2 == 1


def test_quake_transform_and_multi_table_sql(writer, capture):
    conn, captured = capture
    data = (parse_quake_list(FIXTURE["quake_list"], COLLECTED)
            + parse_tsunami_list(FIXTURE["tsunami_list"], COLLECTED)
            + parse_volcano_warning(FIXTURE["volcano_warning"],
                                    build_volcano_index(FIXTURE["volcano_list"]), COLLECTED)[0])
    data.append({"_type": "volcano", "volcano_code": "999", "report_time": None})  # 防線：transformer 丟棄
    recs = writer._transform_jma_quake({"data": data}, TS)
    assert len(recs) == len(data) - 1
    writer._write_to_db(conn, "jma_quake", recs, TS)
    tables = {sql.split()[2]: (sql, vals) for sql, vals in captured}
    assert set(tables) == {"live.jma_quake_reports", "live.jma_tsunami_reports", "live.jma_volcano_warnings"}
    for sql, _vals in tables.values():
        assert sql.rstrip().endswith("ON CONFLICT DO NOTHING")
    assert len(tables["live.jma_quake_reports"][1]) == len(FIXTURE["quake_list"])


def test_quake_collect_partial_failure_flags_error(monkeypatch):
    def fake_fetch(session, path, **kw):
        if path == jma_quake.URL_TSUNAMI_LIST:
            raise jma_quake.JmaFetchError("boom")
        return {
            jma_quake.URL_QUAKE_LIST: FIXTURE["quake_list"],
            jma_quake.URL_VOLCANO_WARNING: FIXTURE["volcano_warning"],
            jma_quake.URL_VOLCANO_LIST: FIXTURE["volcano_list"],
        }[path]

    monkeypatch.setattr(jma_quake, "fetch", fake_fetch)
    c = jma_quake.JmaQuakeCollector.__new__(jma_quake.JmaQuakeCollector)
    c.name, c._session = "jma_quake", None
    c._volcanoes, c._volcanoes_loaded_at = {}, None
    out = c.collect()
    json.dumps(out)
    assert out["counts"]["quake"] == len(FIXTURE["quake_list"])
    assert out["counts"]["tsunami"] == 0
    assert "tsunami" in out["source_failures"] and out["_collector_error"]
