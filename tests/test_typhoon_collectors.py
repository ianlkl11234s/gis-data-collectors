"""颱風 collector（G02）單元測試：氣象署 W-C0034-005、JMA specifications 修正、JTWC 名稱／DTG、
NCDR 各國預報 KML、跨來源 storm_key 與寫入層。

不打網路、不打 DB：fixture 為 2026-10-17 實打樣本（颱風薔琵 CHAMPI；tests/fixtures/typhoon/）。
writer 以 mock conn 攔截 execute_values。
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from collectors.base import TAIPEI_TZ
from collectors.global_climate import cwa_typhoon, ncdr_typhoon_routes
from collectors.global_climate.cwa_typhoon import CwaTyphoonCollector, parse_cwa_typhoons
from collectors.global_climate.jma_typhoon import JmaFetchError, JmaTyphoonCollector, parse_jma_specifications
from collectors.global_climate.jtwc import JtwcCollector, _parse_dtg
from collectors.global_climate.ncdr_typhoon_routes import parse_multityroute, resolve_issued
from collectors.global_climate.typhoon_common import storm_key
from storage.supabase_tables import TABLE_MAP
from storage.supabase_writer import SupabaseWriter

FX = Path(__file__).parent / "fixtures" / "typhoon"
CWA = json.loads((FX / "cwa_w_c0034_005_20261017.json").read_text())
JMA_FORECAST = json.loads((FX / "jma_TC2636_forecast_20261017.json").read_text())
JMA_SPECS = json.loads((FX / "jma_TC2636_specifications_20261017.json").read_text())
JMA_SPECS_ASYM = json.loads((FX / "jma_TC2636_specifications_20261019.json").read_text())
JTWC_TXT = (FX / "jtwc_wp2926web_20261017.txt").read_text()
NCDR_KML = (FX / "ncdr_multityroute_20261017.kml").read_text(encoding="utf-8")
NOW = datetime(2026, 10, 17, 23, 30, tzinfo=TAIPEI_TZ)
TS = NOW
NEW_COLS = ("max_wind_ms", "max_gust_ms", "prob70_radius_km", "wind_radii", "storm_key")


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


# ───────────────────────── storm_key ─────────────────────────

@pytest.mark.parametrize("year,name,expected", [
    (2026, "Champi", "2026:CHAMPI"),
    (2026, "CHAMPI", "2026:CHAMPI"),
    (2026, "Choi-wan", "2026:CHOIWAN"),
    (2026, "Twentynine", None),
    (2026, "TWENTY-NINE", None),
    (2026, "One", None),
    (2026, "Nineteen", None),
    (2026, "Fifty", None),
    (2026, "Invest", None),
    (2026, "", None),
    (2026, None, None),
    (None, "Champi", None),
])
def test_storm_key_matches_sql_rules(year, name, expected):
    # 與 gis-platform 476 public.typhoon_normalize_storm_key() 的測試案例一致
    assert storm_key(year, name) == expected


# ───────────────────────── CWA ─────────────────────────

def test_cwa_parse_sample():
    out = parse_cwa_typhoons(CWA, NOW)
    rows = out["rows"]
    assert out["storms"] == ["cwa2633"] and not out["no_active"] and out["rejected"] == 0
    obs = [r for r in rows if r["point_type"] == "observed"]
    fc = [r for r in rows if r["point_type"] == "forecast"]
    assert len(obs) == 15 and len(fc) == 9
    assert {r["storm_key"] for r in rows} == {"2026:CHAMPI"}
    assert {r["source"] for r in rows} == {"cwa"}
    assert all(not r["storm_id"].startswith("TC") for r in rows)
    assert {r["advisory_number"] for r in rows} == {int(datetime(2026, 10, 17, 20, tzinfo=TAIPEI_TZ).timestamp() // 60)}
    json.dumps(rows)  # 不可含 datetime

    latest = max(obs, key=lambda r: r["valid_at"])
    assert latest["valid_at"].startswith("2026-10-17T20:00")
    assert latest["max_wind_ms"] == 51 and latest["max_gust_ms"] == 63
    assert latest["max_wind_kt"] == pytest.approx(99.1, abs=0.1)
    assert latest["center_pressure_hpa"] == 925
    assert latest["gale_radius_km"] == 180 and latest["storm_radius_km"] == 80
    wr = json.loads(latest["wind_radii"])
    assert wr["gale"] == {"all": 180, "ne": 200, "se": 160, "sw": 150, "nw": 200}
    assert wr["storm"]["ne"] == 90 and wr["unit"] == "km"
    assert latest["prob70_radius_km"] is None

    # 早期分析點沒有風圈 → NULL，不是 0
    earliest = min(obs, key=lambda r: r["valid_at"])
    assert earliest["gale_radius_km"] is None and earliest["storm_radius_km"] is None
    assert earliest["wind_radii"] is None

    # 預報點：InitialTime + ForecastHour；只有單一半徑＋70% 機率圈
    f120 = max(fc, key=lambda r: r["valid_at"])
    assert f120["valid_at"].startswith("2026-10-22T20:00")
    assert f120["prob70_radius_km"] == 510 and f120["gale_radius_km"] == 280
    assert set(json.loads(f120["wind_radii"])["gale"]) == {"all"}


def test_cwa_single_storm_as_dict_and_no_storm():
    p = copy.deepcopy(CWA)
    p["records"]["TropicalCyclones"]["TropicalCyclone"] = p["records"]["TropicalCyclones"]["TropicalCyclone"][0]
    assert len(parse_cwa_typhoons(p, NOW)["rows"]) == 24

    for empty in ({"TropicalCyclone": []}, {"TropicalCyclone": None}, {}):
        q = copy.deepcopy(CWA)
        q["records"]["TropicalCyclones"] = empty
        out = parse_cwa_typhoons(q, NOW)
        assert out["no_active"] and out["rows"] == []


def test_cwa_missing_or_bad_cyclone_container_raises():
    # review F193：容器缺失／型別不符不可靜默當無颱風
    for bad in ("MISSING", None, "", [], "x", {"Other": 1}, {"TropicalCyclone": "x"}):
        q = copy.deepcopy(CWA)
        if bad == "MISSING":
            q["records"].pop("TropicalCyclones")
        else:
            q["records"]["TropicalCyclones"] = bad
        with pytest.raises(ValueError):
            parse_cwa_typhoons(q, NOW)


def test_cwa_container_shape_variants():
    # XML 轉 JSON：單一象限變物件、容器直接是 list、或空字串，都不可讓整輪失敗
    p = copy.deepcopy(CWA)
    tc = p["records"]["TropicalCyclones"]["TropicalCyclone"][0]
    tc["AnalysisData"] = tc["AnalysisData"]["Fix"]
    fix = tc["AnalysisData"][-1]
    fix["Circle15ms"]["QuadrantRadii"] = {"Radius": {"dir": "NE", "value": "300"}}
    fix["Circle25ms"]["QuadrantRadii"] = ""
    out = parse_cwa_typhoons(p, NOW)
    assert len(out["rows"]) == 24
    row = [r for r in out["rows"] if r["point_type"] == "observed"][-1]
    assert json.loads(row["wind_radii"])["gale"]["ne"] == 300


def test_cwa_bad_payload_raises():
    with pytest.raises(ValueError):
        parse_cwa_typhoons({"success": "false"}, NOW)
    with pytest.raises(ValueError):
        parse_cwa_typhoons({"success": "true"}, NOW)


def test_cwa_guard_rejects_bad_rows():
    p = copy.deepcopy(CWA)
    tc = p["records"]["TropicalCyclones"]["TropicalCyclone"][0]
    tc["AnalysisData"]["Fix"][0]["CoordinateLatitude"] = "999"
    tc["AnalysisData"]["Fix"][1]["DateTime"] = "2026-11-30T08:00:00+08:00"  # 未來的觀測點
    out = parse_cwa_typhoons(p, NOW)
    assert out["rejected"] == 2 and len(out["rows"]) == 22


def _cwa_collector(payload):
    c = CwaTyphoonCollector.__new__(CwaTyphoonCollector)
    c._fetch = lambda: payload
    return c


def test_cwa_collect_idle_has_no_data_key():
    q = copy.deepcopy(CWA)
    q["records"]["TropicalCyclones"] = {"TropicalCyclone": []}
    out = _cwa_collector(q).collect()
    assert "data" not in out  # base.run() 不存檔、不寫 DB
    assert out["status"] == "no_active_storm" and out["storm_count"] == 0


def test_cwa_collect_active_and_http_error():
    out = _cwa_collector(CWA).collect()
    assert out["status"] == "ok" and len(out["data"]) == 24 and "_collector_error" not in out

    c = CwaTyphoonCollector.__new__(CwaTyphoonCollector)

    def boom():
        raise RuntimeError("HTTP 503")
    c._fetch = boom
    with pytest.raises(RuntimeError):
        c.collect()


# ───────────────────────── JMA ─────────────────────────

def test_jma_specifications_parse():
    specs = parse_jma_specifications(JMA_SPECS)
    assert sorted(specs) == [0, 12, 24, 48, 72, 96, 120]
    a = specs[0]
    assert a["center_pressure_hpa"] == 945 and a["max_wind_kt"] == 95 and a["max_wind_ms"] == 50
    assert a["max_gust_ms"] == 70
    assert a["gale_radius_km"] == 440 and a["storm_radius_km"] == 140
    assert a["prob70_radius_km"] is None
    wr = json.loads(a["wind_radii"])
    assert wr["gale"] == {"all": 440} and wr["storm"] == {"all": 140}
    f12 = specs[12]
    # 預報段 stormWarning＝暴風警戒域：不進 storm_radius_km
    assert f12["storm_radius_km"] is None and f12["gale_radius_km"] is None
    assert json.loads(f12["wind_radii"])["storm_warning_area"] == {"all": 185}
    assert f12["prob70_radius_km"] == 45


def test_jma_asymmetric_areas_take_longest():
    # 2026-10-18 起非對稱強風域的 area 是純字串方位（不是 {"jp","en"} 物件）
    specs = parse_jma_specifications(JMA_SPECS_ASYM)
    a = specs[0]
    assert a["gale_radius_km"] == 650 and a["storm_radius_km"] == 130
    radii = json.loads(a["wind_radii"])
    assert radii["gale"]["areas"] == [{"area": "東", "km": 650.0}, {"area": "西", "km": 440.0}]
    assert radii["storm"] == {"all": 130.0}


def test_jma_specs_parse_failure_keeps_positions(monkeypatch):
    from collectors.global_climate import jma_typhoon
    c = JmaTyphoonCollector.__new__(JmaTyphoonCollector)
    target = json.loads((FX / "jma_targetTc_20261017.json").read_text())
    c._fetch_json = lambda url: (target if url.endswith("targetTc.json")
                                 else JMA_FORECAST if url.endswith("forecast.json") else JMA_SPECS)

    def boom(_):
        raise AttributeError("unexpected shape")
    monkeypatch.setattr(jma_typhoon, "parse_jma_specifications", boom)
    out = c.collect()
    assert out["data"] and "specifications" in out["_collector_error"]
    assert all(r["gale_radius_km"] is None for r in out["data"])


def test_jma_decompose_fills_numbers():
    c = JmaTyphoonCollector.__new__(JmaTyphoonCollector)
    rows = c._decompose_forecast("TC2636", JMA_FORECAST, NOW, parse_jma_specifications(JMA_SPECS))
    assert rows and {r["storm_key"] for r in rows} == {"2026:CHAMPI"}
    cur = [r for r in rows if r["point_type"] == "observed" and r["raw_json"]]
    assert len(cur) == 1
    assert cur[0]["center_pressure_hpa"] == 945 and cur[0]["max_wind_kt"] == 95
    assert cur[0]["gale_radius_km"] == 440 and cur[0]["storm_radius_km"] == 140
    fc = [r for r in rows if r["point_type"] == "forecast"]
    assert fc and all(r["center_pressure_hpa"] is not None for r in fc)
    assert all(r["storm_radius_km"] is None for r in fc)
    assert all(r["prob70_radius_km"] is not None for r in fc)
    json.dumps(rows)


def test_jma_collect_idle_and_errors():
    c = JmaTyphoonCollector.__new__(JmaTyphoonCollector)
    c._fetch_json = lambda url: []
    out = c.collect()
    assert "data" not in out and out["status"] == "no_active_storm"

    def fail(url):
        raise JmaFetchError("timeout")
    c._fetch_json = fail
    with pytest.raises(JmaFetchError):  # 逾時不可當成沒颱風
        c.collect()

    target = json.loads((FX / "jma_targetTc_20261017.json").read_text())

    def partial(url):
        if url.endswith("targetTc.json"):
            return target
        if url.endswith("forecast.json"):
            return JMA_FORECAST
        raise JmaFetchError("specifications 500")
    c._fetch_json = partial
    out = c.collect()
    assert out["status"] == "ok" and "_collector_error" in out
    assert out["data"]  # specifications 失敗時位置照寫

    def specs_404(url):
        if url.endswith("targetTc.json"):
            return target
        if url.endswith("forecast.json"):
            return JMA_FORECAST
        return None  # 404
    c._fetch_json = specs_404
    out = c.collect()
    assert out["status"] == "ok" and "_collector_error" not in out
    assert out["specs_missing"] == ["TC2636"] and out["data"]


# ───────────────────────── JTWC ─────────────────────────

def test_jtwc_title_name_and_key():
    c = JtwcCollector.__new__(JtwcCollector)
    rows = c._parse_warning("wp2926", JTWC_TXT, datetime(2026, 10, 18, 2, 0, tzinfo=TAIPEI_TZ))
    assert rows
    assert {r["name_en"] for r in rows} == {"Champi"}
    assert {r["storm_key"] for r in rows} == {"2026:CHAMPI"}
    obs = [r for r in rows if r["point_type"] == "observed"]
    assert obs[0]["valid_at"].startswith("2026-10-17T12:00")


def test_jtwc_dtg_month_rollover():
    # 11/01 收到 31 日的 DTG → 10/31，不是 11/31（無效）或三週後
    assert _parse_dtg("311800", (2026, 11, 1)) == datetime(2026, 10, 31, 18, tzinfo=timezone.utc)
    assert _parse_dtg("301800", (2026, 10, 1)) == datetime(2026, 9, 30, 18, tzinfo=timezone.utc)
    # 12/31 收到 1 日的預報 DTG → 隔年 1/1
    assert _parse_dtg("011200", (2026, 12, 31)) == datetime(2027, 1, 1, 12, tzinfo=timezone.utc)
    assert _parse_dtg("171200", (2026, 10, 17)) == datetime(2026, 10, 17, 12, tzinfo=timezone.utc)


# ───────────────────────── NCDR ─────────────────────────

def test_ncdr_parse_sample():
    out = parse_multityroute(NCDR_KML, NOW)
    assert out["agencies"] == ["jtwc", "jma", "nmc", "cwa", "kma"] and not out["no_active"]
    rows = out["rows"]
    assert len(rows) == 30
    assert {r["lead_hour"] for r in rows} == {0, 24, 48, 72, 96, 120}
    assert {r["track_index"] for r in rows} == {0}
    jtwc0 = next(r for r in rows if r["agency"] == "jtwc" and r["lead_hour"] == 0)
    assert jtwc0["issued_at"] == "2026-10-17T20:00:00+08:00" and jtwc0["issued_label"] == "10/17 20"
    assert jtwc0["center_lat"] == pytest.approx(12.6) and jtwc0["center_lon"] == pytest.approx(146.1)
    j120 = next(r for r in rows if r["agency"] == "jtwc" and r["lead_hour"] == 120)
    assert j120["valid_at"] == "2026-10-22T20:00:00+08:00"
    json.dumps(rows)


def test_ncdr_empty_and_errors():
    empty = '<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://www.opengis.net/kml/2.2"><Document></Document></kml>'
    assert parse_multityroute(empty, NOW)["no_active"]
    with pytest.raises(ValueError):
        parse_multityroute("<kml><Document>", NOW)
    no_time = NCDR_KML.replace("發佈時間:10/17 20", "")
    with pytest.raises(ValueError):
        parse_multityroute(no_time, NOW)


def test_ncdr_second_storm_gets_track_index():
    i = NCDR_KML.index("<Folder><name>JTWC</name>")
    j = NCDR_KML.index("</Folder>", i) + len("</Folder>")
    doubled = NCDR_KML[:j] + NCDR_KML[i:j] + NCDR_KML[j:]
    rows = parse_multityroute(doubled, NOW)["rows"]
    assert len(rows) == 36
    assert {r["track_index"] for r in rows if r["agency"] == "jtwc"} == {0, 1}


def test_ncdr_year_rollover():
    assert resolve_issued(12, 31, 20, datetime(2027, 1, 1, 2, tzinfo=TAIPEI_TZ)).year == 2026
    assert resolve_issued(1, 1, 2, datetime(2026, 12, 31, 23, tzinfo=TAIPEI_TZ)).year == 2027


# ───────────────────────── 寫入層 ─────────────────────────

def test_table_map_has_new_columns():
    for name in ("global_climate_jma_typhoon", "global_climate_jtwc", "global_climate_cwa_typhoon"):
        cols = TABLE_MAP[name]["columns"]
        assert all(c in cols for c in NEW_COLS), name
        assert TABLE_MAP[name]["upsert_strategy"] == "do_nothing"
    assert TABLE_MAP["global_climate_ncdr_typhoon_routes"]["history"] == "live.typhoon_agency_forecasts"


def test_writer_cwa_rows_sql(writer, capture):
    conn, captured = capture
    rows = parse_cwa_typhoons(CWA, NOW)["rows"]
    recs = writer._transform_global_climate_typhoon_positions({"data": rows}, TS)
    assert len(recs) == 24
    writer._write_to_db(conn, "global_climate_cwa_typhoon", recs, TS)
    sql, vals = captured[0]
    assert sql.startswith("INSERT INTO live.typhoon_positions") and "ON CONFLICT DO NOTHING" in sql
    cols = TABLE_MAP["global_climate_cwa_typhoon"]["columns"]
    row = dict(zip(cols, vals[-1]))
    assert row["source"] == "cwa" and row["storm_key"] == "2026:CHAMPI"
    assert row["geom"].startswith("SRID=4326;POINT(")


def test_writer_ncdr_rows_sql(writer, capture):
    conn, captured = capture
    rows = parse_multityroute(NCDR_KML, NOW)["rows"]
    recs = writer._transform_global_climate_typhoon_agency_forecasts({"data": rows}, TS)
    writer._write_to_db(conn, "global_climate_ncdr_typhoon_routes", recs, TS)
    sql, vals = captured[0]
    assert sql.startswith("INSERT INTO live.typhoon_agency_forecasts") and "ON CONFLICT DO NOTHING" in sql
    assert len(vals) == 30
