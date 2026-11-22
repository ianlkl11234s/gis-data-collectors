"""metoffice_warnings／metoffice_obs。

fixtures（tests/fixtures/metoffice/，不含 key）：
  - *_20261120*：2026-11-20 實測回應（警報當下 0 筆；觀測 Edinburgh 9 參數、London 只有溫濕度）
  - *_doc_example*：官方 v1.0 文件範例（https://metoffice.github.io/nswws-public-api/），v1.1 真實警報待驗證
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import config
from collectors import metoffice_obs as mo
from collectors import metoffice_warnings as mw
from storage.supabase_tables import TABLE_MAP

FIX = Path(__file__).parent / "fixtures" / "metoffice"
NOW = datetime(2026, 11, 20, 8, 30, tzinfo=timezone.utc)
ISSUED_URL = f"{mw.API_BASE}/objects/issued/3f4ca4cf-32ea-4eef-aed0-75db50d94e02/"
UPDATED_URL = f"{mw.API_BASE}/objects/updated/c9ce3dca-13e0-4eaf-8f74-cf44ec04279c/"


def _text(name):
    return (FIX / name).read_text(encoding="utf-8")


def _load(name):
    return json.loads(_text(name))


class _Resp:
    def __init__(self, body=None, status=200, text=None):
        self._body, self.status_code = body, status
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(str(self.status_code), response=self)


# ───────────────────────── warnings ─────────────────────────

def _warn(monkeypatch, tmp_path, routes):
    monkeypatch.setattr(config, "METOFFICE_WARNINGS_API_KEY", "test-key", raising=False)
    c = mw.MetofficeWarningsCollector.__new__(mw.MetofficeWarningsCollector)
    c.name = "metoffice_warnings"
    c._now = lambda: NOW
    c.state_path = tmp_path / "state" / "w.json"
    c._state = c._pending_state = None
    c.supabase_writer = MagicMock()
    c.requested = []

    def get(url, timeout=None):
        c.requested.append(url)
        r = routes[url]
        return r.pop(0) if isinstance(r, list) else r
    c._session = MagicMock()
    c._session.get.side_effect = get
    return c


def _commit(c):
    c._save_state(c._pending_state)
    c._pending_state = None


def test_parse_real_feed_without_entries():
    feed = mw.parse_feed(_text("feed_20261120_no_entries.xml"))
    assert feed["related_href"].startswith(f"{mw.API_BASE}/objects/issued/e2865408")
    assert feed["feed_updated"] == "2026-11-17T11:00:00+00:00"
    assert feed["entries"] == []


def test_no_warnings_now_writes_empty_then_no_change(monkeypatch, tmp_path):
    feed = _text("feed_20261120_no_entries.xml")
    issued = mw.parse_feed(feed)["related_href"]
    c = _warn(monkeypatch, tmp_path, {mw.FEED_URL: _Resp(text=feed),
                                      issued: _Resp(_load("issued_20261120_empty.json"))})
    first = c.collect()
    assert first["data"] == [] and first["issued_features"] == 0 and first["api_calls"] == 2
    _commit(c)
    second = c.collect()
    assert second.get("no_change") is True and "data" not in second
    assert c.requested == [mw.FEED_URL, issued, mw.FEED_URL]


def test_doc_example_issued_and_cancelled_update(monkeypatch, tmp_path):
    routes = {
        mw.FEED_URL: _Resp(text=_text("feed_doc_example_with_entry.xml")),
        ISSUED_URL: _Resp(_load("issued_doc_example_v1.0.json")),
        UPDATED_URL: _Resp(_load("updated_doc_example_cancelled_v1.0.json")),
    }
    c = _warn(monkeypatch, tmp_path, routes)
    res = c.collect()
    rows = {r["warning_status"]: r for r in res["data"]}
    assert set(rows) == {"ISSUED", "CANCELLED"} and res["new_entries"] == 1
    iss = rows["ISSUED"]
    assert iss["warning_id"] == "79bb1df2-8983-4bbd-888a-66da9c099a1c"
    assert iss["warning_version"] == "1.0" and iss["warning_level"] == "YELLOW"
    assert iss["weather_types"] == ["THUNDERSTORM"]
    assert (iss["warning_likelihood"], iss["warning_impact"]) == (4, 2)
    assert iss["valid_from"] == "2022-06-22T11:54:00+00:00"
    assert iss["valid_to"] == "2022-06-22T22:59:59+00:00"
    assert iss["region_codes"] == ["HE", "OS"]
    assert iss["geom"].startswith("SRID=4326;MULTIPOLYGON")
    assert iss["properties"]["whatToExpect"]  # 完整 properties 原樣保留
    assert iss["source"] == "issued"
    can = rows["CANCELLED"]
    assert can["warning_version"] == "3.0" and can["update_description"] == "Warning no longer required."
    assert can["source"] == "updated" and can["source_uuid"] == "c9ce3dca-13e0-4eaf-8f74-cf44ec04279c"

    # 同一 feed 再跑：沒有新東西
    _commit(c)
    assert c.collect().get("no_change") is True


def test_status_is_part_of_key_expired_shares_version_with_issued():
    v2 = _load("updated_doc_example_v2_v1.0.json")["features"][0]
    exp = _load("updated_doc_example_expired_v1.0.json")["features"][0]
    kw = dict(source="updated", source_uuid="u", feed_updated=None, collected_at=NOW.isoformat())
    a, b = mw.normalize_feature(v2, **kw), mw.normalize_feature(exp, **kw)
    assert a["warning_version"] == b["warning_version"] == "2.0"
    assert mw.version_key(a) != mw.version_key(b)


def test_seen_versions_not_resent_when_issued_snapshot_changes(monkeypatch, tmp_path):
    routes = {
        mw.FEED_URL: [_Resp(text=_text("feed_doc_example_with_entry.xml")),
                      _Resp(text=_text("feed_doc_example_with_entry.xml").replace(
                          "<updated>2022-06-22T12:07:40Z</updated>", "<updated>2022-06-22T13:00:00Z</updated>", 1))],
        ISSUED_URL: [_Resp(_load("issued_doc_example_v1.0.json"))] * 2,
        UPDATED_URL: _Resp(_load("updated_doc_example_cancelled_v1.0.json")),
    }
    c = _warn(monkeypatch, tmp_path, routes)
    c.collect()
    _commit(c)
    again = c.collect()
    assert again["issued_features"] == 1 and again["data"] == []  # 已寫過的版本不再送
    assert c.requested.count(UPDATED_URL) == 1  # 看過的 entry 不再抓


def test_lenient_parsing_missing_fields_become_null():
    feat = {"type": "Feature", "properties": {"warningId": "x1", "issuedDate": "2026-11-20T01:00:00Z"},
            "geometry": {"type": "Point", "coordinates": [0, 51]}}
    r = mw.normalize_feature(feat, source="issued", source_uuid=None, feed_updated=None,
                             collected_at=NOW.isoformat())
    assert r["modified_at"] == "2026-11-20T01:00:00+00:00"  # modifiedDate 缺 → issuedDate
    assert r["warning_version"] == "unknown" and r["warning_status"] == "ISSUED"
    assert r["warning_level"] is None and r["valid_to"] is None and r["weather_types"] is None
    assert r["geom"] is None and r["geometry_type"] == "Point"
    assert mw.normalize_feature({"properties": {}}, source="issued", source_uuid=None,
                                feed_updated=None, collected_at="") is None


def test_key_not_sent_outside_api_base(monkeypatch, tmp_path):
    evil = _text("feed_20261120_no_entries.xml").replace(
        "https://data.hub.api.metoffice.gov.uk/nswws/v1.1/objects/issued", "https://evil.example/objects/issued")
    c = _warn(monkeypatch, tmp_path, {mw.FEED_URL: _Resp(text=evil)})
    with pytest.raises(ValueError, match="網域外"):
        c.collect()
    assert all(u.startswith(mw.API_BASE) for u in c.requested)


def test_warnings_transformer_matches_table_map(monkeypatch, tmp_path):
    from storage.supabase_writer import SupabaseWriter, reject_mojibake_records
    c = _warn(monkeypatch, tmp_path, {
        mw.FEED_URL: _Resp(text=_text("feed_doc_example_with_entry.xml")),
        ISSUED_URL: _Resp(_load("issued_doc_example_v1.0.json")),
        UPDATED_URL: _Resp(_load("updated_doc_example_cancelled_v1.0.json")),
    })
    res = c.collect()
    w = SupabaseWriter.__new__(SupabaseWriter)
    recs = SupabaseWriter._transform_metoffice_warnings(w, res, NOW)
    cols = TABLE_MAP["metoffice_warnings"]["columns"]
    assert len(recs) == 2 and all(list(r) == cols for r in recs)
    assert TABLE_MAP["metoffice_warnings"]["upsert_strategy"] == "do_nothing"
    assert len(reject_mojibake_records("metoffice_warnings", recs)) == 2


# ───────────────────────── observations ─────────────────────────

GRID = [(-3.35, 55.95), (-0.12, 51.5), (-1.0, 52.0)]


def _obs(monkeypatch, tmp_path, routes, cap=330, now=NOW, grid=GRID, db_stations=None):
    monkeypatch.setattr(config, "METOFFICE_OBS_API_KEY", "test-key", raising=False)
    c = mo.MetofficeObsCollector.__new__(mo.MetofficeObsCollector)
    c.name = "metoffice_obs"
    c._sleep = lambda s: None
    c._now = lambda: now
    c._last_call = 0.0
    c.calls = 0
    c.daily_cap, c.discovery_per_run, c.rescan_per_week, c.min_interval = cap, 20, 0, 0
    c.quota_path = tmp_path / "state" / "q.json"
    c.state_path = tmp_path / "state" / "s.json"
    c.grid = list(grid)
    c._state = c._pending_state = None
    c.supabase_writer = MagicMock()
    c._db_stations = lambda: dict(db_stations or {})
    c.requested = []

    def get(url, params=None, timeout=None):
        path = url.replace(mo.API_BASE, "")
        key = path if path != "/nearest" else f"/nearest?{params['lat']},{params['lon']}"
        c.requested.append(key)
        r = routes.get(key, routes.get("*"))
        return r.pop(0) if isinstance(r, list) else r
    c._session = MagicMock()
    c._session.get.side_effect = get
    return c


def _commit_obs(c):
    c._save_json(c.state_path, c._pending_state)
    c._state, c._pending_state = c._pending_state, None


def test_geohash_center_and_slot():
    lon, lat = mo.geohash_center("gcvw5v")
    assert 55.9 < lat < 56.0 and -3.4 < lon < -3.3
    assert mo.geohash_center("gcv!") is None
    assert mo.station_slot("gcvw5v") == mo.station_slot("gcvw5v") and 0 <= mo.station_slot("gcvw5v") < 24


def test_normalize_obs_real_samples_keep_nulls():
    full, bad = mo.normalize_obs(_load("obs_gcvw5v_20261120.json"), "gcvw5v", NOW.isoformat())
    assert len(full) == 48 and bad == 0
    r = next(x for x in full if x["observed_at"] == "2026-11-18T08:00:00+00:00")
    assert r["wind_direction"] == "S" and r["pressure_tendency"] == "R" and r["weather_code"] == 5
    assert r["mslp"] == 984 and r["visibility"] == 6660.0 and r["extra"] is None
    part, _ = mo.normalize_obs(_load("obs_gcpvj0_20261120.json"), "gcpvj0", NOW.isoformat())
    assert part[0]["temperature"] is not None
    assert part[0]["wind_speed"] is None and part[0]["mslp"] is None  # 缺的參數是 NULL，不是 0
    odd, bad = mo.normalize_obs([{"datetime": "x"}, {"datetime": "2026-11-20T00:00:00Z", "new_param": 1}],
                                "g", "")
    assert bad == 1 and odd[0]["extra"] == {"new_param": 1}


def test_nearest_query_uses_two_decimals_and_discovers_station(monkeypatch, tmp_path):
    routes = {"*": _Resp([]), "/nearest?55.95,-3.35": _Resp(_load("nearest_edinburgh_20261120.json")),
              "/gcvw5v": _Resp(_load("obs_gcvw5v_20261120.json"))}
    c = _obs(monkeypatch, tmp_path, routes, grid=[(-3.3500001, 55.9499999)],
             now=NOW.replace(hour=23))  # 23 時：所有時段都已到
    res = c.collect()
    assert c.requested[0] == "/nearest?55.95,-3.35"
    assert res["stations_new"] == 1 and res["stations_fetched"] == 1 and len(res["data"]) == 48
    st = c.supabase_writer._upsert_metoffice_obs_stations.call_args[0][0][0]
    assert st["geohash"] == "gcvw5v" and st["area"] == "Edinburgh" and st["country"] == "Scotland"
    assert res["api_calls_today"] == 2 and json.loads(c.quota_path.read_text())["calls"] == 2


def test_station_fetched_once_per_day(monkeypatch, tmp_path):
    routes = {"*": _Resp(_load("nearest_edinburgh_20261120.json")),
              "/gcvw5v": _Resp(_load("obs_gcvw5v_20261120.json"))}
    late = NOW.replace(hour=23)
    c = _obs(monkeypatch, tmp_path, routes, now=late)
    c.collect()
    _commit_obs(c)
    c.requested.clear()
    res = c.collect()
    assert "/gcvw5v" not in c.requested and res["stations_fetched"] == 0


def test_daily_cap_is_hard_and_persisted_before_call(monkeypatch, tmp_path):
    routes = {"*": _Resp([{"geohash": "gcpvj0", "area": "Greater London"}]),
              "/gcpvj0": _Resp(_load("obs_gcpvj0_20261120.json"))}
    grid = [(-1.0 - i * 0.01, 52.0) for i in range(50)]
    c = _obs(monkeypatch, tmp_path, routes, cap=5, grid=grid, now=NOW.replace(hour=23))
    res = c.collect()
    assert res["api_calls_today"] <= 5 and len(c.requested) <= 5
    q = json.loads(c.quota_path.read_text())
    assert (q["day"], q["calls"]) == ("2026-11-20", len(c.requested))
    # 一個已知站還沒抓時，探站一定會留一次給它
    assert "/gcpvj0" in c.requested
    _commit_obs(c)
    c.requested.clear()
    res2 = c.collect()
    assert c.requested == [] and res2.get("no_work") is True
    # 隔天 UTC 歸零
    c._now = lambda: datetime(2026, 11, 21, 23, 0, tzinfo=timezone.utc)
    res3 = c.collect()
    assert 0 < res3["api_calls_today"] <= 5


def test_quota_file_unwritable_fails_closed(monkeypatch, tmp_path):
    c = _obs(monkeypatch, tmp_path, {"*": _Resp([])})
    c._save_json(c.quota_path, {"day": "2026-11-20", "calls": 0})

    def boom(path, data):
        raise OSError("read-only")
    monkeypatch.setattr(c, "_save_json", boom)
    res = c.collect()
    assert c.requested == [] and res.get("quota_exhausted") is True


def test_lost_quota_file_with_known_stations_counts_day_as_used(monkeypatch, tmp_path):
    db = {"gcvw5v": {"area": "Edinburgh", "region": "dg", "country": "Scotland",
                     "olson_time_zone": "Europe/London", "lon": -3.35, "lat": 55.95}}
    c = _obs(monkeypatch, tmp_path, {"*": _Resp([])}, db_stations=db, now=NOW.replace(hour=23))
    res = c.collect()
    assert c.requested == [] and res["api_calls_today"] == 330
    assert res["stations_known"] == 1 and res["grid_done"] is True  # 站表由 DB 重建、不重掃網格


def test_first_deploy_starts_from_zero(monkeypatch, tmp_path):
    c = _obs(monkeypatch, tmp_path, {"*": _Resp([])})
    res = c.collect()
    assert res["api_calls_today"] == len(c.requested) == len(GRID)


def test_rescan_after_grid_done_uses_offset_points_and_weekly_limit(monkeypatch, tmp_path):
    c = _obs(monkeypatch, tmp_path, {"*": _Resp([])}, grid=[(-1.0, 52.0)])
    c.rescan_per_week = 2
    res = c.collect()
    # 網格掃完後同一輪就接著用剩餘的探站次數做每週重掃（半格偏移）
    assert c.requested == ["/nearest?52.00,-1.00", "/nearest?52.10,-0.83", "/nearest?52.10,-0.83"]
    assert res["grid_done"] is True and res["rescan_this_week"] == 2
    _commit_obs(c)
    c.requested.clear()
    c.collect()
    assert c.requested == []  # 本週額度用完
    # 下一週再重掃
    _commit_obs(c)
    c._now = lambda: datetime(2026, 11, 27, 8, 30, tzinfo=timezone.utc)
    c.collect()
    assert len(c.requested) == 2


def test_all_calls_failing_raises(monkeypatch, tmp_path):
    c = _obs(monkeypatch, tmp_path, {"*": _Resp({}, status=500)})
    with pytest.raises(RuntimeError, match="全部失敗"):
        c.collect()
    assert json.loads(c.quota_path.read_text())["calls"] == len(GRID) * mo.HTTP_MAX_ATTEMPTS  # 失敗（含重試）也算額度


def test_obs_transformer_matches_table_map(monkeypatch, tmp_path):
    from storage.supabase_writer import SupabaseWriter
    routes = {"*": _Resp(_load("nearest_edinburgh_20261120.json")),
              "/gcvw5v": _Resp(_load("obs_gcvw5v_20261120.json"))}
    c = _obs(monkeypatch, tmp_path, routes, now=NOW.replace(hour=23))
    res = c.collect()
    w = SupabaseWriter.__new__(SupabaseWriter)
    recs = SupabaseWriter._transform_metoffice_obs(w, res, NOW)
    assert len(recs) == 48 and all(list(r) == TABLE_MAP["metoffice_obs"]["columns"] for r in recs)
    assert TABLE_MAP["metoffice_obs"]["upsert_key"] == "observed_at,geohash"


def test_station_upsert_sql_is_bulk_compatible(monkeypatch):
    from contextlib import contextmanager
    from storage import bulk
    from storage import supabase_writer as sw
    captured = {}

    def fake_execute_values(cur, sql, values, template=None, page_size=None):
        captured.update(sql=sql, values=values)
    monkeypatch.setattr(sw, "execute_values", fake_execute_values)

    @contextmanager
    def cm(*a, **k):
        yield MagicMock()
    w = sw.SupabaseWriter.__new__(sw.SupabaseWriter)
    w._pool = MagicMock(borrow=cm)
    w._txn = cm
    s = mo.normalize_station(_load("nearest_edinburgh_20261120.json")[0])
    w._upsert_metoffice_obs_stations([s, s])
    assert bulk._INSERT_RE.match(captured["sql"])  # storage.bulk 只接受 INSERT ... VALUES %s
    assert "IS DISTINCT FROM" in captured["sql"]
    assert len(captured["values"]) == 1 and captured["values"][0][-1].startswith("SRID=4326;POINT(")


def test_grid_file_is_within_limits():
    pts = mo.load_grid()
    assert 300 <= len(pts) <= 600
    assert all(-8.3 <= lon <= 1.9 and 49.8 <= lat <= 61.0 for lon, lat in pts)
    assert all(round(lon, 2) == lon and round(lat, 2) == lat for lon, lat in pts)


def test_expired_updated_entry_404_does_not_block_issued(monkeypatch, tmp_path):
    c = _warn(monkeypatch, tmp_path, {
        mw.FEED_URL: _Resp(text=_text("feed_doc_example_with_entry.xml")),
        ISSUED_URL: _Resp(_load("issued_doc_example_v1.0.json")),
        UPDATED_URL: _Resp({}, status=404),
    })
    res = c.collect()
    assert [r["warning_status"] for r in res["data"]] == ["ISSUED"] and res["failed_entries"] == 1
    assert res["raw_payload"]["issued"][0]["properties"]["warningId"]  # 原始 FeatureCollection 進 S3 歸檔
    _commit(c)
    assert c.collect().get("no_change") is True  # 404 的 entry 記成看過、不再重試


def test_warning_record_serializes_for_bulk_insert(monkeypatch, tmp_path):
    from storage import bulk
    c = _warn(monkeypatch, tmp_path, {
        mw.FEED_URL: _Resp(text=_text("feed_doc_example_with_entry.xml")),
        ISSUED_URL: _Resp(_load("issued_doc_example_v1.0.json")),
        UPDATED_URL: _Resp(_load("updated_doc_example_cancelled_v1.0.json")),
    })
    cols = TABLE_MAP["metoffice_warnings"]["columns"]
    types = {c_: ("text", "text") for c_ in cols}
    types.update({"modified_at": ("timestamp with time zone", "timestamptz"),
                  "valid_from": ("timestamp with time zone", "timestamptz"),
                  "valid_to": ("timestamp with time zone", "timestamptz"),
                  "issued_at": ("timestamp with time zone", "timestamptz"),
                  "feed_updated_at": ("timestamp with time zone", "timestamptz"),
                  "collected_at": ("timestamp with time zone", "timestamptz"),
                  "weather_types": ("text[]", "_text"), "region_codes": ("text[]", "_text"),
                  "warning_likelihood": ("smallint", "int2"), "warning_impact": ("smallint", "int2"),
                  "affected_areas": ("jsonb", "jsonb"), "properties": ("jsonb", "jsonb"),
                  "geom": ("geometry(MultiPolygon,4326)", "geometry")})
    sql = (f"INSERT INTO live.metoffice_warning_events ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({TABLE_MAP['metoffice_warnings']['upsert_key'].replace(',', ', ')}) DO NOTHING")
    fixed, spec = bulk.build_insert_sql(sql, types, None)
    row = c.collect()["data"][0]
    payload = json.loads(bulk.serialize_rows([tuple(row.get(k) for k in cols)], spec))[0]
    by_col = {cols[i]: payload[f"c{i}"] for i in range(len(cols))}
    assert by_col["weather_types"] == ["THUNDERSTORM"] and by_col["region_codes"] == ["HE", "OS"]
    assert isinstance(by_col["properties"], dict) and isinstance(by_col["affected_areas"], list)
    assert by_col["geom"].startswith("SRID=4326;MULTIPOLYGON")
    assert "json_to_recordset" in fixed and "DO NOTHING" in fixed


def test_quota_rereads_file_written_by_another_run(monkeypatch, tmp_path):
    routes = {"*": _Resp([])}
    c = _obs(monkeypatch, tmp_path, routes, cap=5, grid=[(-1.0 - i * 0.01, 52.0) for i in range(10)])
    c._quota = c._load_quota("2026-11-20", False)
    c._save_json(c.quota_path, {"day": "2026-11-20", "calls": 4})  # 另一個行程（手動跑）已用 4 次
    c._spend()
    with pytest.raises(mo.QuotaExhausted):
        c._spend()
    assert json.loads(c.quota_path.read_text())["calls"] == 5


def test_rate_limited_stops_run(monkeypatch, tmp_path):
    c = _obs(monkeypatch, tmp_path, {"*": _Resp({}, status=429)})
    res = c.collect()
    assert len(c.requested) == 1 and res["quota_exhausted"] is True


# ───────────────────────── Codex review 2026-11-21（#216）─────────────────────────

def test_obs_transient_5xx_is_retried_with_quota_per_attempt(monkeypatch, tmp_path):
    """C3：5xx／逾時有界重試，每次嘗試都記額度；成功後才算已抓。"""
    import requests
    seq = [_Resp({}, status=503), requests.Timeout("t"), _Resp(_load("obs_gcvw5v_20261120.json"))]
    routes = {"/gcvw5v": seq, "*": _Resp(_load("nearest_edinburgh_20261120.json"))}
    c = _obs(monkeypatch, tmp_path, routes, now=NOW.replace(hour=23))

    def get(url, params=None, timeout=None):
        path = url.replace(mo.API_BASE, "")
        key = path if path != "/nearest" else f"/nearest?{params['lat']},{params['lon']}"
        c.requested.append(key)
        r = routes.get(key, routes.get("*"))
        r = r.pop(0) if isinstance(r, list) else r
        if isinstance(r, Exception):
            raise r
        return r
    c._session.get.side_effect = get
    res = c.collect()
    assert c.requested.count("/gcvw5v") == 3
    assert res["stations_fetched"] == 1 and res["stations_failed"] == 0 and res["total_readings"] > 0
    assert json.loads(c.quota_path.read_text())["calls"] == len(c.requested)


def test_obs_persistent_5xx_gives_up_after_bounded_attempts(monkeypatch, tmp_path):
    c = _obs(monkeypatch, tmp_path, {"/gcvw5v": _Resp({}, status=503),
                                     "*": _Resp(_load("nearest_edinburgh_20261120.json"))},
             now=NOW.replace(hour=23))
    res = c.collect()
    assert c.requested.count("/gcvw5v") == mo.HTTP_MAX_ATTEMPTS and res["stations_failed"] == 1


def test_spend_rolls_quota_day_at_utc_midnight(monkeypatch, tmp_path):
    """C4：跨午夜時請求記在新的一天，不是 collect() 開頭載入的舊日期。"""
    c = _obs(monkeypatch, tmp_path, {"*": _Resp([])}, cap=5)
    c._quota = c._load_quota("2026-11-20", False)
    c._quota["calls"] = 3
    c._now = lambda: datetime(2026, 11, 21, 0, 5, tzinfo=timezone.utc)
    c._spend()
    q = json.loads(c.quota_path.read_text())
    assert (q["day"], q["calls"]) == ("2026-11-21", 1)
    assert c._remaining() == 4


def test_nearest_429_restores_discovery_cursor(monkeypatch, tmp_path):
    """C5：/nearest 429 時網格進度不前進，下輪從同一點重來。"""
    c = _obs(monkeypatch, tmp_path, {"*": _Resp({}, status=429)})
    res = c.collect()
    assert res["quota_exhausted"] is True and res["grid_progress"] == f"0/{len(GRID)}"
    assert c._pending_state["grid_next"] == 0


def test_all_updated_entries_failing_is_reported_as_failure(monkeypatch, tmp_path):
    """C6：feed 沒變、只剩 updated entry 且全失敗 → 帶 _collector_error，不當成功。"""
    routes = {mw.FEED_URL: _Resp(text=_text("feed_doc_example_with_entry.xml")),
              ISSUED_URL: _Resp(_load("issued_doc_example_v1.0.json")),
              UPDATED_URL: [_Resp({}, status=503)]}
    c = _warn(monkeypatch, tmp_path, routes)
    c.collect()
    _commit(c)  # issued 已成功處理；entry 503 未記為看過
    routes[mw.FEED_URL] = _Resp(text=_text("feed_doc_example_with_entry.xml"))
    routes[UPDATED_URL] = [_Resp({}, status=503)]
    res = c.collect()
    assert res["failed_entries"] == 1 and "_collector_error" in res


# ───────────────────────── Codex review 2026-11-22（#219）─────────────────────────

def _late_known(n):
    """挑 n 個時段 > 0 的 geohash，00:xx 時它們都還沒到時段（保持「未抓」）。"""
    gs = [g for g in ("gcpvj0", "gcvw5v", "gcpuz1", "gcpv5x", "gcw2h0", "gfjmzt") if mo.station_slot(g) > 0]
    assert len(gs) >= n
    return {g: {"area": "x"} for g in gs[:n]}


def _seq_get(c, nearest_seq):
    def get(url, params=None, timeout=None):
        path = url.replace(mo.API_BASE, "")
        c.requested.append(path)
        r = nearest_seq.pop(0)
        if isinstance(r, Exception):
            raise r
        return r
    c._session.get.side_effect = get


def test_discovery_retries_do_not_consume_reserved_quota(monkeypatch, tmp_path):
    """D1：只剩 3 次、2 站未抓 → 探站重試不得吃掉留給已知站的額度。"""
    c = _obs(monkeypatch, tmp_path, {"*": _Resp([])}, cap=3, now=NOW.replace(hour=0),
             db_stations=_late_known(2))
    c.rescan_per_week = 5
    c._save_json(c.quota_path, {"day": "2026-11-20", "calls": 0})
    _seq_get(c, [_Resp({}, status=503), _Resp({}, status=503), _Resp([])])
    with pytest.raises(RuntimeError, match="全部失敗"):  # 失敗如實浮出，不被重試蓋掉
        c.collect()
    assert len(c.requested) == 1  # 3 - 2 保留 = 只有 1 次可用於探站，不重試
    assert json.loads(c.quota_path.read_text())["calls"] == 1
