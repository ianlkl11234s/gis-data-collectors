"""openaq：用 2026-11-20 實測回應（tests/fixtures/openaq/，不含 key）驗解析、去重呼叫與缺值語意。"""
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import config
from collectors import openaq
from storage.supabase_tables import TABLE_MAP

FIX = Path(__file__).parent / "fixtures" / "openaq"
NOW = datetime(2026, 11, 20, 7, 0, tzinfo=timezone.utc)


def _load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, body, status=200, headers=None):
        self._body, self.status_code, self.headers = body, status, headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code}")


def _collector(monkeypatch, routes, countries="PH"):
    monkeypatch.setattr(config, "OPENAQ_API_KEY", "test-key", raising=False)
    monkeypatch.setattr(config, "OPENAQ_COUNTRIES", countries, raising=False)
    c = openaq.OpenAQCollector.__new__(openaq.OpenAQCollector)
    c.name = "openaq"
    c.countries = [x.strip().upper() for x in countries.split(",") if x.strip()]
    c.max_age = openaq.timedelta(hours=48)
    c.min_interval = 0
    c.sleeps = []
    c._sleep = c.sleeps.append
    c._now = lambda: NOW
    c._last_call = 0.0
    c.calls = 0
    c.supabase_writer = MagicMock()
    c.requested = []

    def get(url, params=None, timeout=None):
        path = url.replace(openaq.API_BASE, "")
        c.requested.append((path, params))
        r = routes[path]
        return r.pop(0) if isinstance(r, list) else r
    c._session = MagicMock()
    c._session.get.side_effect = get
    return c


def _routes():
    return {
        "/locations": _Resp(_load("locations_ph_sample.json")),
        "/locations/2812740/latest": _Resp(_load("latest_2812740.json")),
        "/locations/2879671/latest": _Resp(_load("latest_2879671.json")),
    }


def test_normalize_location_keeps_license_provider_and_low_cost_flag():
    locs = {x["id"]: x for x in _load("locations_ph_sample.json")["results"]}
    clarity = openaq.normalize_location(locs[2812740], "PH")
    assert clarity["is_monitor"] is False
    assert clarity["provider_name"] == "Clarity"
    assert clarity["license_names"] == "CC0 1.0"
    assert clarity["attribution_name"] == "Clarity"
    assert clarity["country_code"] == "PH"
    assert clarity["sensors"] == [{"id": 8966895, "parameter": "pm25", "units": "µg/m³"}]
    # 授權缺 → None（不是「無限制」），座標照原值
    spartan = openaq.normalize_location(locs[11568], "PH")
    assert spartan["licenses"] is None and spartan["license_names"] is None
    assert spartan["is_monitor"] is True
    assert spartan["datetime_last"] is None


def test_collect_polls_only_active_and_drops_stale_latest(monkeypatch):
    c = _collector(monkeypatch, _routes())
    result = c.collect()

    paths = [p for p, _ in c.requested]
    # 11568 沒有 datetimeLast（停更）→ 不打 /latest
    assert paths == ["/locations", "/locations/2812740/latest", "/locations/2879671/latest"]
    assert c.requested[0][1]["iso"] == "PH"
    assert result["api_calls"] == 3
    assert result["skipped_inactive"] == 1

    rows = result["data"]
    # 2879671 的 pm10 停在 2025-01-13 → 超過 48h 不寫
    assert result["stale_readings_dropped"] == 1
    assert not any(r["parameter"] == "pm10" for r in rows)
    assert len(rows) == 1 + 5
    pm25 = next(r for r in rows if r["sensor_id"] == 8966895)
    assert pm25 == {
        "observed_at": "2026-11-20T05:06:49+00:00", "sensor_id": 8966895, "location_id": 2812740,
        "parameter": "pm25", "units": "µg/m³", "value": 35.6, "collected_at": NOW.isoformat(),
    }
    # 測站 metadata 交給 writer（有變才 UPDATE），也留在結果裡供 S3 歸檔
    c.supabase_writer._upsert_openaq_locations.assert_called_once()
    assert len(result["stations"]) == 3


def test_second_run_polls_again_and_db_dedups(monkeypatch):
    """/locations 的 datetimeLast 會落後 /latest，所以每輪都打 /latest；重複值交給 DB DO NOTHING。"""
    routes = _routes()
    routes["/locations"] = [_Resp(_load("locations_ph_sample.json")), _Resp(_load("locations_ph_sample.json"))]
    routes["/locations/2812740/latest"] = [_Resp(_load("latest_2812740.json"))] * 2
    routes["/locations/2879671/latest"] = [_Resp(_load("latest_2879671.json"))] * 2
    c = _collector(monkeypatch, routes)
    first = c.collect()
    second = c.collect()
    assert second["api_calls"] == 3
    assert second["data"] == first["data"]


def test_missing_values_stay_null_not_zero():
    station = {"location_id": 1, "sensors": [{"id": 9, "parameter": "pm25", "units": "µg/m³"}]}
    cutoff = NOW - openaq.timedelta(hours=48)
    res = [{"sensorsId": 9, "value": v, "datetime": {"utc": f"2026-11-20T0{i}:00:00Z"}}
           for i, v in enumerate([None, -999, float("nan"), 0.0, -3.5])]
    rows, stale, unknown = openaq.normalize_latest(res, station, cutoff, NOW.isoformat())
    assert [r["value"] for r in rows] == [None, None, None, 0.0, -3.5]
    assert (stale, unknown) == (0, 0)
    # 對不回 sensor 的值不猜參數
    rows, _, unknown = openaq.normalize_latest([{"sensorsId": 77, "value": 1, "datetime": {"utc": "2026-11-20T06:00:00Z"}}],
                                               station, cutoff, NOW.isoformat())
    assert rows == [] and unknown == 1


def test_one_location_failure_does_not_fail_run_but_all_failing_does(monkeypatch):
    routes = _routes()
    routes["/locations/2812740/latest"] = _Resp({}, status=500)
    c = _collector(monkeypatch, routes)
    result = c.collect()
    assert result["failed_locations"] == 1 and result["polled_locations"] == 1

    routes = _routes()
    routes["/locations/2812740/latest"] = _Resp({}, status=500)
    routes["/locations/2879671/latest"] = _Resp({}, status=500)
    c = _collector(monkeypatch, routes)
    with pytest.raises(RuntimeError, match="全部失敗"):
        c.collect()


def test_rate_limited_request_waits_and_retries_once(monkeypatch):
    routes = _routes()
    routes["/locations"] = [_Resp({}, status=429, headers={"x-ratelimit-reset": "7"}),
                            _Resp(_load("locations_ph_sample.json"))]
    c = _collector(monkeypatch, routes)
    result = c.collect()
    assert 7.0 in c.sleeps
    assert result["total_locations"] == 3


def test_missing_api_key_raises(monkeypatch):
    c = _collector(monkeypatch, _routes())
    monkeypatch.setattr(config, "OPENAQ_API_KEY", None, raising=False)
    with pytest.raises(RuntimeError, match="OPENAQ_API_KEY"):
        c.collect()


def test_transformer_matches_table_map(monkeypatch):
    from storage.supabase_writer import SupabaseWriter
    c = _collector(monkeypatch, _routes())
    result = c.collect()
    w = SupabaseWriter.__new__(SupabaseWriter)
    recs = SupabaseWriter._transform_openaq(w, result, NOW)
    cols = TABLE_MAP["openaq"]["columns"]
    assert recs and all(list(r) == cols for r in recs)
    assert TABLE_MAP["openaq"]["upsert_key"] == "observed_at,sensor_id"
    assert TABLE_MAP["openaq"]["upsert_strategy"] == "do_nothing"
    # write() 的亂碼防護與執行收據（µg/m³ 不可被誤判；upstream_max_at 取 observed_at 最大值）
    from storage.supabase_writer import reject_mojibake_records
    from storage import run_receipt
    assert len(reject_mojibake_records("openaq", recs)) == len(recs)
    info = run_receipt.run_info(result, recs, TABLE_MAP["openaq"]["upstream_time_col"], (len(recs), "dedup"))
    assert info["upstream_max_at"] == datetime(2026, 11, 20, 6, 0, tzinfo=timezone.utc)


def test_upsert_locations_sql_only_updates_changed_rows(monkeypatch):
    from contextlib import contextmanager
    from storage import supabase_writer as sw

    captured = {}

    def fake_execute_values(cur, sql, values, template=None, page_size=None):
        captured.update(sql=sql, values=values, template=template)
    monkeypatch.setattr(sw, "execute_values", fake_execute_values)

    @contextmanager
    def cm(*a, **k):
        yield MagicMock()
    w = sw.SupabaseWriter.__new__(sw.SupabaseWriter)
    w._pool = MagicMock(borrow=cm)
    w._txn = cm

    locs = _load("locations_ph_sample.json")["results"]
    stations = [openaq.normalize_location(x, "PH") for x in locs]
    w._upsert_openaq_locations(stations + stations[:1])  # 同批重複只留一列

    sql = captured["sql"]
    assert "INSERT INTO live.openaq_locations" in sql
    assert "ON CONFLICT (location_id) DO UPDATE" in sql and "IS DISTINCT FROM" in sql
    # 必須是 storage.bulk 能解析的形狀（上線首輪曾因 INSERT ... SELECT 被拒）
    from storage.bulk import _INSERT_RE
    assert _INSERT_RE.match(sql), sql[:120]
    assert "geom = EXCLUDED.geom" in sql
    assert len(captured["values"]) == 3
    clarity = next(v for v in captured["values"] if v[0] == 2812740)
    assert json.loads(clarity[12]) == [{"id": 8966895, "parameter": "pm25", "units": "µg/m³"}]
    assert clarity[-1].startswith("SRID=4326;POINT(")
