"""jma_forecast（C5）與 env_jp_wbgt（C7）單元測試：不打網路、不打 DB。"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

import config
from collectors.global_climate import env_jp_wbgt as W
from collectors.global_climate import jma_forecast as F
from storage.supabase_writer import SupabaseWriter, reject_mojibake_records

COLLECTED = "2026-10-12T09:00:00+08:00"
TS = datetime(2026, 10, 12, 1, 0, tzinfo=timezone.utc)

# ───────────── fixtures（2026-10-12 實打 130000.json／map.json 裁剪）─────────────
SHORT = {
    "publishingOffice": "気象庁", "reportDatetime": "2026-10-12T05:00:00+09:00",
    "timeSeries": [
        {"timeDefines": ["2026-10-12T05:00:00+09:00", "2026-10-13T00:00:00+09:00"],
         "areas": [{"area": {"name": "東京地方", "code": "130010"},
                    "weatherCodes": ["101", "201"],
                    "weathers": ["晴れ　朝晩　くもり", "くもり"],
                    "winds": ["北の風", "北の風　後　東の風"],
                    "waves": ["０．５メートル", ""]}]},
        {"timeDefines": ["2026-10-12T06:00:00+09:00", "2026-10-12T12:00:00+09:00",
                         "2026-10-13T00:00:00+09:00"],
         "areas": [{"area": {"name": "東京地方", "code": "130010"}, "pops": ["0", "", "10"]}]},
        {"timeDefines": ["2026-10-12T09:00:00+09:00", "2026-10-12T00:00:00+09:00",
                         "2026-10-13T00:00:00+09:00", "2026-10-13T09:00:00+09:00"],
         "areas": [{"area": {"name": "東京", "code": "44132"}, "temps": ["27", "27", "17", "25"]}]},
    ],
}
WEEK = {
    "publishingOffice": "気象庁", "reportDatetime": "2026-10-11T17:00:00+09:00",
    "timeSeries": [
        {"timeDefines": ["2026-10-12T00:00:00+09:00", "2026-10-13T00:00:00+09:00"],
         "areas": [{"area": {"name": "東京地方", "code": "130010"}, "weatherCodes": ["101", "201"],
                    "pops": ["", "20"], "reliabilities": ["", "C"]}]},
        {"timeDefines": ["2026-10-12T00:00:00+09:00", "2026-10-13T00:00:00+09:00"],
         "areas": [{"area": {"name": "東京", "code": "44132"}, "tempsMin": ["", "17"],
                    "tempsMinUpper": ["", "18"], "tempsMinLower": ["", "15"],
                    "tempsMax": ["", "25"], "tempsMaxUpper": ["", "27"], "tempsMaxLower": ["", "24"]}]},
    ],
    "tempAverage": {"areas": []},
}
AREA = {"offices": {"130000": {}, "011000": {}}, "class10s": {"130010": {"parent": "130000"},
                                                              "011000": {"parent": "011000"}}}
PROB = [[
    {"reportDatetime": "2026-10-12T05:00:00+09:00", "timeSeries": [{
        "timeDefines": ["2026-10-12T06:00:00+09:00", "2026-10-12T12:00:00+09:00"],
        "areas": [{"code": "130010", "properties": [
            {"type": "大雨の警報級の可能性", "probabilities": ["", "中"]},
            {"type": "波の警報級の可能性", "probabilities": ["", ""]}]}]}]},
    {"reportDatetime": "2026-10-11T17:00:00+09:00", "timeSeries": [{
        "timeDefines": ["2026-10-13T00:00:00+09:00"],
        "areas": [{"code": "130010", "properties": [
            {"type": "雨の警報級の可能性", "probabilities": [""]}]}]}]},
]]


def _by(rows, area, date):
    return next(r for r in rows if r["area_code"] == area and r["target_date"] == date)


def test_short_forecast_parse_and_today_min_placeholder_null():
    rows = F.parse_forecast_block(SHORT, "short", "130000", COLLECTED)
    a12 = _by(rows, "130010", "2026-10-12")
    assert a12["weather_code"] == "101" and "　" in a12["weather_text"]
    assert a12["pops_6h"] == {"06": 0, "12": None}           # 空字串 → NULL，不是 0
    assert a12["area_kind"] == "area"
    assert _by(rows, "130010", "2026-10-13")["wave_text"] is None
    t12 = _by(rows, "44132", "2026-10-12")
    assert t12["temp_max"] == 27.0 and t12["temp_min"] is None  # 今日最低＝最高 → 佔位值存 NULL
    t13 = _by(rows, "44132", "2026-10-13")
    assert (t13["temp_min"], t13["temp_max"]) == (17.0, 25.0)  # 明日不受影響
    assert t13["area_kind"] == "amedas"
    assert all(r["report_datetime"] == "2026-10-12T05:00:00+09:00" for r in rows)


def test_week_forecast_blank_first_day_is_null():
    rows = F.parse_forecast_block(WEEK, "week", "130000", COLLECTED)
    a12 = _by(rows, "130010", "2026-10-12")
    assert a12["pop"] is None and a12["reliability"] is None and a12["pops_6h"] is None
    assert _by(rows, "130010", "2026-10-13")["pop"] == 20
    t13 = _by(rows, "44132", "2026-10-13")
    assert (t13["temp_min_lower"], t13["temp_max_upper"]) == (15.0, 27.0)
    assert _by(rows, "44132", "2026-10-12")["temp_min"] is None


def test_map_report_times_maps_class10_to_office():
    office_of = F.build_office_index(AREA)
    fmap = [[{"reportDatetime": "2026-10-12T05:00:00+09:00",
              "timeSeries": [{"areas": [{"area": {"code": "130010"}}]}]},
             {"reportDatetime": "2026-10-11T17:00:00+09:00",
              "timeSeries": [{"areas": [{"area": {"code": "130010"}}]}]}]]
    assert F.map_report_times(fmap, office_of) == {
        "130000": {"short": "2026-10-12T05:00:00+09:00", "week": "2026-10-11T17:00:00+09:00"}}


def test_probability_blank_kept_as_empty_string():
    out = F.parse_probability(PROB, F.build_office_index(AREA), COLLECTED)
    rdt, rows = out[("130010", "short")]
    assert rdt == "2026-10-12T05:00:00+09:00" and len(rows) == 2
    assert rows[0]["levels"] == ["", "中"] and rows[0]["office_code"] == "130000"
    assert len(rows[0]["time_defines"]) == 2
    assert out[("130010", "week")][1][0]["levels"] == [""]


def _fc_collector(tmp_path, responses):
    c = F.JmaForecastCollector.__new__(F.JmaForecastCollector)
    c.name = "jma_forecast"
    c._office_of, c._area_loaded_at = F.build_office_index(AREA), datetime.now(timezone.utc)
    c.state_path = tmp_path / "s.json"
    c._state, c._pending_state = None, None
    c._session = MagicMock()
    calls = []

    def fake_fetch(session, path, **kw):
        calls.append(path)
        return responses[path]
    return c, calls, fake_fetch


def _fmap(short_rdt, week_rdt):
    return [[{"reportDatetime": short_rdt, "timeSeries": [{"areas": [{"area": {"code": "130010"}}]}]},
             {"reportDatetime": week_rdt, "timeSeries": [{"areas": [{"area": {"code": "130010"}}]}]}]]


def test_forecast_only_changed_blocks_are_written(tmp_path, monkeypatch):
    resp = {F.URL_MAP: _fmap(SHORT["reportDatetime"], WEEK["reportDatetime"]),
            "forecast/data/forecast/130000.json": [SHORT, WEEK], F.URL_PROB: PROB}
    c, calls, fake = _fc_collector(tmp_path, resp)
    monkeypatch.setattr(F, "fetch", fake)
    out = c.collect()
    assert out["changed"] == {"forecast_blocks": 2, "probability_blocks": 2}
    assert "raw_payload" in out and "130000" in out["raw_payload"]
    c._state = c._pending_state
    calls.clear()
    out2 = c.collect()
    assert out2.get("no_change") and "forecast/data/forecast/130000.json" not in calls
    # 只有短期更新 → 只寫短期區塊
    short2 = dict(SHORT, reportDatetime="2026-10-12T11:00:00+09:00")
    resp[F.URL_MAP] = _fmap(short2["reportDatetime"], WEEK["reportDatetime"])
    resp["forecast/data/forecast/130000.json"] = [short2, WEEK]
    c._state = c._pending_state
    out3 = c.collect()
    assert {r["series"] for r in out3["data"]} == {"short"}
    assert out3["changed"]["forecast_blocks"] == 1


def test_forecast_state_not_saved_when_db_write_fails(tmp_path, monkeypatch):
    resp = {F.URL_MAP: _fmap(SHORT["reportDatetime"], WEEK["reportDatetime"]),
            "forecast/data/forecast/130000.json": [SHORT, WEEK], F.URL_PROB: PROB}
    c, _, fake = _fc_collector(tmp_path, resp)
    monkeypatch.setattr(F, "fetch", fake)
    c.storage = MagicMock()
    c.supabase_writer = MagicMock()
    c.supabase_writer.write.return_value = False
    c.run_count = c.error_count = c.consecutive_errors = 0
    c.last_run = c.last_success_at = None
    monkeypatch.setattr("collectors.base.notify_error", lambda *a, **k: None)
    monkeypatch.setattr("collectors.base.notify_success", lambda *a, **k: None)
    assert c.run().get("error") and not c.state_path.exists()
    c.supabase_writer.write.return_value = True
    assert not c.run().get("error") and c.state_path.exists()


def test_forecast_writer_sql_and_japanese_not_rejected(monkeypatch):
    captured = []
    monkeypatch.setattr("storage.supabase_writer.execute_values",
                        lambda cur, sql, values, template=None, page_size=100:
                        captured.append((sql, list(values), template)))
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._txn = MagicMock()
    rows = [{"_type": "forecast", **r} for r in F.parse_forecast_block(SHORT, "short", "130000", COLLECTED)]
    rows += [{"_type": "probability", **r}
             for r in F.parse_probability(PROB, F.build_office_index(AREA), COLLECTED)[("130010", "short")][1]]
    recs = w._transform("jma_forecast", {"data": rows}, TS)
    assert reject_mojibake_records("jma_forecast", recs) == recs
    w._write_multi_table(MagicMock(), "jma_forecast", recs)
    assert "live.jma_forecasts" in captured[0][0] and "DO NOTHING" in captured[0][0]
    assert "live.jma_forecast_probabilities" in captured[1][0]
    assert "::timestamptz[]" in captured[1][2]


# ───────────────────────────── WBGT ─────────────────────────────
OBS_CSV = (
    "Date,Time,11001,11016,44132\n"
    "2026/10/1,23:00,10.5,11.2,\n"
    "2026/10/1,24:00,10.4,,20.0\n"
    "2026/10/2,1:00,,,\n"
)
FCST_CSV = (
    ",,2026101209,2026101212,2026101224\n"
    "11001,2026/10/12 08:25, 140, 160,\n"
    "44132,2026/10/12 08:25, 250, 270, 200\n"
)


def test_obs_parse_24h_and_blank_is_none():
    stations, rows = W.parse_obs_csv(OBS_CSV)
    assert stations == ["11001", "11016", "44132"]
    assert rows[1][0] == "2026-10-02T00:00:00+09:00"          # 24:00 → 次日 00:00 JST
    assert rows[0][1] == [10.5, 11.2, None]                     # 空格 → None，不是 0
    assert rows[2][1] == [None, None, None]


def test_forecast_parse_divides_by_ten_and_blank_none():
    issued, rows = W.parse_forecast_csv(FCST_CSV)
    assert issued == "2026-10-12T08:25:00+09:00"
    assert rows[0]["wbgt"] == [14.0, 16.0, None]
    assert rows[1]["target_times"][-1] == "2026-10-13T00:00:00+09:00"


def test_season_switch():
    assert W.in_season(datetime(2026, 7, 1, tzinfo=timezone.utc), "4,5,6,7,8,9,10")
    assert not W.in_season(datetime(2026, 11, 1, tzinfo=timezone.utc), "4,5,6,7,8,9,10")
    # 3/31 15:00 UTC＝4/1 00:00 JST → 季內（以 JST 判斷）
    assert W.in_season(datetime(2026, 3, 31, 15, 0, tzinfo=timezone.utc), "4")
    assert W.season_months("4, 5,x,13") == {4, 5}


class _Resp:
    def __init__(self, status, text="", headers=None):
        self.status_code, self.content, self.headers = status, text.encode(), headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def _wbgt(tmp_path, monkeypatch, now, pages):
    c = W.EnvJpWbgtCollector.__new__(W.EnvJpWbgtCollector)
    c.name = "env_jp_wbgt"
    c.state_path = tmp_path / "w.json"
    c._state, c._pending_state = None, None
    c._session = MagicMock()
    c._session.get.side_effect = lambda url, headers=None, timeout=None: pages(url, headers or {})

    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is timezone.utc else now.astimezone(tz)
    monkeypatch.setattr(W, "datetime", _DT)
    monkeypatch.setattr(config, "JAPAN_ENV_WBGT_SEASON_MONTHS", "4,5,6,7,8,9,10")
    monkeypatch.setattr(config, "JAPAN_ENV_WBGT_INITIAL_LOOKBACK_DAYS", 30)
    return c


def test_off_season_makes_no_request(tmp_path, monkeypatch):
    c = _wbgt(tmp_path, monkeypatch, datetime(2026, 12, 1, 3, tzinfo=timezone.utc), lambda u, h: 1 / 0)
    out = c.collect()
    assert out["off_season"] and out["no_change"] and c._session.get.call_count == 0


def test_wbgt_dedup_revision_and_conditional_get(tmp_path, monkeypatch):
    state = {"obs": OBS_CSV, "fc": FCST_CSV}

    def pages(url, headers):
        if headers.get("If-None-Match") == "e1":
            return _Resp(304)
        body = state["fc"] if "yohou" in url else (state["obs"] if "202610" in url else None)
        return _Resp(200, body, {"ETag": "e1"}) if body else _Resp(404)
    now = datetime(2026, 10, 2, 3, 0, tzinfo=timezone.utc)  # 10/2 JST → 也看 9 月檔（404 不算錯）
    c = _wbgt(tmp_path, monkeypatch, now, pages)
    out = c.collect()
    obs = [r for r in out["data"] if r["_type"] == "obs"]
    assert len(obs) == 4                                       # 只寫非空格
    assert out["http_status"]["obs:202609"] == "missing" and not out["source_failures"]
    assert sum(r["_type"] == "forecast" for r in out["data"]) == 2
    c._state = c._pending_state
    out2 = c.collect()
    assert out2.get("no_change") and set(out2["http_status"].values()) == {"not_modified", "missing"}
    # 上游回溯修正一格＋ETag 變更 → 只送該時刻列；預報作成時刻沒變 → 不重送
    state["obs"] = OBS_CSV.replace("10.4,,20.0", "10.6,,20.0")
    c._state = dict(c._pending_state, http={})
    out3 = c.collect()
    assert [(r["station_id"], r["wbgt"]) for r in out3["data"]] == [("11001", 10.6), ("44132", 20.0)]


def test_wbgt_writer_sql(monkeypatch):
    captured = []
    monkeypatch.setattr("storage.supabase_writer.execute_values",
                        lambda cur, sql, values, template=None, page_size=100:
                        captured.append((sql, list(values), template)))
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._txn = MagicMock()
    _, frows = W.parse_forecast_csv(FCST_CSV)
    data = [{"_type": "obs", "station_id": "11001", "observed_at": "2026-10-02T00:00:00+09:00",
             "wbgt": 10.4, "collected_at": COLLECTED},
            {"_type": "obs", "station_id": "11016", "observed_at": "2026-10-02T00:00:00+09:00",
             "wbgt": None, "collected_at": COLLECTED}]
    data += [{"_type": "forecast", **r, "collected_at": COLLECTED} for r in frows]
    recs = w._transform("env_jp_wbgt", {"data": data}, TS)
    assert sum(r["_type"] == "obs" for r in recs) == 1          # NULL 值不寫
    w._write_multi_table(MagicMock(), "env_jp_wbgt", recs)
    assert "IS DISTINCT FROM EXCLUDED.wbgt" in captured[0][0]
    assert "::real[]" in captured[1][2] and len(captured[1][1]) == 2


@pytest.mark.parametrize("prefix", ["JAPAN_JMA_FORECAST", "JAPAN_ENV_WBGT"])
def test_registered_default_disabled(prefix):
    from collectors.registry import COLLECTOR_REGISTRY
    assert any(e.config_prefix == prefix for e in COLLECTOR_REGISTRY)
    assert {p: en for p, en, _ in config._COLLECTOR_TOGGLES}[prefix] is False
