"""HiCloud VM 即時 119＋警廣 runner（external/fire119_vm/）。

fixture 在 tests/fixtures/fire119/：取自 analytics 2026-11-16 實測原始檔，已裁到資料區塊、去門牌；
臺北另加 3 列人造邊界案例（夾帶 csX 的模糊列、0.0 座標、「測試」列），新北的軌跡與救護車位置換成假座標。
"""
import json
import sqlite3
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "external" / "fire119_vm"
FIX = ROOT / "tests" / "fixtures" / "fire119"
sys.path.insert(0, str(ROOT / "external" / "vm_common"))
sys.path.insert(0, str(APP))
import bulk  # noqa: E402
import fire119_collect as C  # noqa: E402
import fire119_parse as P  # noqa: E402
import fire119_store as S  # noqa: E402
from match import Gazetteer  # noqa: E402

TZ = P.TZ
NOW = datetime(2026, 11, 16, 14, 21, 25, tzinfo=TZ)  # 14:21 → 奇數分鐘

FIXTURE_OF = {
    "taipei": "taipei.json", "new_taipei": "new_taipei.json", "kaohsiung": "kaohsiung_dts.html",
    "tainan": "tainan_dts.html", "yilan": "yilan_dts_empty.html", "taoyuan": "taoyuan.html",
    "taichung": "taichung.html", "changhua": "changhua.html", "nantou": "nantou.html", "pbs": "pbs.json",
}


def fx(name: str) -> bytes:
    return (FIX / name).read_bytes()


def parsed(source: str) -> list[dict]:
    return P.parse(source, P.sanitize(source, fx(FIXTURE_OF[source])))


@pytest.fixture
def gaz(tmp_path):
    """最小路名對照表（同 analytics build_gazetteer 的表結構）。"""
    p = tmp_path / "gz.sqlite"
    db = sqlite3.connect(p)
    db.executescript("""
        CREATE TABLE roads (id INTEGER PRIMARY KEY, road_key TEXT, county TEXT, town TEXT, town_code TEXT,
          name_full TEXT, name_base TEXT, kind TEXT, has_lane INTEGER, mid_lon REAL, mid_lat REAL,
          length_m REAL, bbox TEXT, alt_count INTEGER, n_ways INTEGER);
        CREATE TABLE towns (county TEXT, town TEXT, town_code TEXT, centroid_lon REAL, centroid_lat REAL);
        CREATE TABLE intersections (a INTEGER, b INTEGER, lon REAL, lat REAL, dist_m REAL);
    """)
    db.executemany("INSERT INTO towns VALUES (?,?,?,0,0)", [
        ("高雄市", "苓雅區", "64000080"), ("高雄市", "岡山區", "64000190"),
        ("臺北市", "大安區", "63000030"), ("臺北市", "中正區", "63000050")])
    db.executemany("INSERT INTO roads VALUES (?,?,?,?,?,?,?,?,0,?,?,?,'[]',0,1)", [
        (1, "rk_zhengyan01", "高雄市", "苓雅區", "64000080", "正言路", "正言路", "road", 120.31, 22.62, 800.0),
        (2, "rk_dunhua0001", "臺北市", "大安區", "63000030", "敦化南路", "敦化南路", "base_agg", 121.549, 25.03, 2000.0)])
    db.commit()
    db.close()
    g = Gazetteer(str(p))
    yield g
    g.close()


# ── 座標轉換 ───────────────────────────────────────────────────────────
@pytest.mark.parametrize("x,y,lon,lat", [
    # 期望值由 pyproj 3.8 Transformer(3826→4326, always_xy) 算出
    (301786, 2770885, 121.513221, 25.045225),
    (170000, 2540000, 120.219856, 22.959401),
    (330000, 2420000, 121.774115, 21.875735),
    (195000, 2780000, 120.454563, 25.127401),
])
def test_twd97_matches_pyproj(x, y, lon, lat):
    got = P.twd97_to_wgs84(x, y)
    assert abs(got[0] - lon) <= 1e-6 and abs(got[1] - lat) <= 1e-6


# ── 隱私剔除 ───────────────────────────────────────────────────────────
def test_taipei_hidden_coordinates_removed_before_landing():
    d = json.loads(P.sanitize("taipei", fx("taipei.json")))
    for r in d["rows"]:
        if r.get("fuzzyAble") != "N":
            assert not {"csX", "csY", "csPlace"} & set(r), r
    assert any("csX" in r for r in d["rows"] if r["fuzzyAble"] == "N")  # 公共場所精確點保留


def test_new_taipei_track_and_ambulance_position_removed():
    raw = fx("new_taipei.json").decode()
    assert '"path"' in raw.replace("'", '"') or "path" in raw  # fixture 本身有（假）軌跡
    clean = P.sanitize("new_taipei", fx("new_taipei.json")).decode()
    for banned in ("path", "startPoint\"", "startPointKey", "distance"):
        assert banned not in clean
    recs = P.parse("new_taipei", clean.encode())
    assert [r["hospital"] for r in recs] == ["輔大醫院", "桃園聖保祿醫院"]
    assert recs[0]["units"] == ["救護車泰山91"]
    assert recs[0]["dest_lon"] == pytest.approx(121.4309023) and recs[0]["lon"] is None


# ── 解析與結構標記 ─────────────────────────────────────────────────────
@pytest.mark.parametrize("source,n", [("taipei", 7), ("new_taipei", 2), ("kaohsiung", 19), ("tainan", 18),
                                      ("taoyuan", 6), ("taichung", 4), ("changhua", 4), ("nantou", 6),
                                      ("pbs", 6), ("yilan", 0)])
def test_parsers_on_real_fixtures(source, n):
    assert len(parsed(source)) == n


@pytest.mark.parametrize("source", ["kaohsiung", "taoyuan", "taichung", "changhua", "nantou", "taipei",
                                    "new_taipei", "pbs"])
def test_layout_change_is_parse_error_not_zero(source):
    with pytest.raises(P.ParseError):
        P.parse(source, b"<html><body><p>maintenance</p></body></html>")


def test_taipei_zero_coordinates_are_missing_and_test_rows_dropped(gaz):
    recs = parsed("taipei")
    zero = next(r for r in recs if r["location_text"] == "臺北市大安區仁愛路")
    assert zero["lon"] is None and zero["precision"] == "none"
    leak = next(r for r in recs if r["location_text"] == "臺北市中正區重慶南路" and
                r["received_at"] == datetime(2026, 11, 16, 13, 40, tzinfo=TZ))
    assert leak["precision"] == "fuzzy"  # 夾帶的 csX 不採用
    test_row = next(r for r in recs if r["location_text"].startswith("測試"))
    assert S.build_incident("taipei", "臺北市", test_row, NOW, gaz) is None
    row = S.build_incident("taipei", "臺北市", zero, NOW, gaz)
    assert row["geom_precision"] == "district" and row["lon"] is None and row["town_code"] == "63000030"
    assert row["received_at"] == "2026-11-16T13:41:00+08:00"


def test_status_and_category_mapping():
    assert P.map_status("AmbulanceBack") == "transporting"
    assert P.map_status("離開現場", "ems") == "transporting"
    assert P.map_status("離開現場", "fire") == "returned"
    assert P.map_status("已出勤") == "dispatched"
    assert P.map_status("處理中") == "other"
    assert P.map_category("災害搶救/山域事故") == "rescue"
    assert P.pbs_status("北上在262.1公里(已排除)") == "cleared"


# ── 去重 key ─────────────────────────────────────────────────────────
def test_incident_keys_prefix_native_ids_and_ignore_status():
    ntpc = parsed("new_taipei")[0]
    assert P.incident_key("new_taipei", ntpc) == "ntpc:261116141789"
    tn = parsed("tainan")[0]
    assert P.incident_key("tainan", tn) == "tainan:20261116-141217"
    k = parsed("kaohsiung")[0]
    k2 = dict(k, status_raw="已返隊", units=["別的分隊"])
    assert P.incident_key("kaohsiung", k) == P.incident_key("kaohsiung", k2)
    assert len(P.incident_key("kaohsiung", k)) == 16
    assert P.incident_key("kaohsiung", k) != P.incident_key("keelung", k)


# ── 精度與逐列驗證 ─────────────────────────────────────────────────────
def test_road_match_gives_midpoint_unmatched_degrades_to_district(gaz):
    recs = parsed("kaohsiung")
    road = S.build_incident("kaohsiung", "高雄市", recs[0], NOW, gaz)  # 苓雅區正言路
    assert (road["geom_precision"], road["road_key"], road["lon"]) == ("road", "rk_zhengyan01", 120.31)
    dist = S.build_incident("kaohsiung", "高雄市", recs[1], NOW, gaz)  # 只到岡山區
    assert dist["geom_precision"] == "district" and dist["lon"] is None and dist["town_code"] == "64000190"


def test_taipei_fuzzy_point_kept_but_gets_road_key(gaz):
    fuzzy = next(r for r in parsed("taipei") if r["location_text"] == "臺北市大安區敦化南路")
    row = S.build_incident("taipei", "臺北市", fuzzy, NOW, gaz)
    assert row["geom_precision"] == "fuzzy" and row["lon"] == fuzzy["lon"]
    assert row["road_key"] == "rk_dunhua0001" and row["town_code"] == "63000030"


def test_no_gazetteer_means_none_not_guess():
    row = S.build_incident("kaohsiung", "高雄市", parsed("kaohsiung")[0], NOW, None)
    assert row["geom_precision"] == "none" and row["lon"] is None


def _row(**k):
    base = dict(incident_key="k1", source="kaohsiung", county="高雄市", category="ems", status="dispatched",
                status_history=[["t", "已派遣"]], lon=None, lat=None, geom_precision="none", town_code=None,
                dest_lon=None, dest_lat=None, first_seen_at="2026-11-16T14:00:00+08:00",
                last_seen_at="2026-11-16T14:00:00+08:00")
    base.update(k)
    return base


@pytest.mark.parametrize("given,expect_prec,expect_pt", [
    (dict(geom_precision="road"), "none", False),                                  # road 沒點、沒鄉鎮 → none
    (dict(geom_precision="road", town_code="64000080"), "district", False),        # road 沒點 → district
    (dict(geom_precision="district", lon=120.3, lat=22.6), "district", False),     # district 有點 → 移除點
    (dict(geom_precision="exact", lon=0.0001, lat=0.0001), "none", False),         # 外框外 → 移除並降級
    (dict(geom_precision="fuzzy", lon=121.5, lat=25.0), "fuzzy", True),
    (dict(geom_precision="bogus", lon=121.5, lat=25.0), "none", False),
])
def test_validate_mirrors_mig555_checks(given, expect_prec, expect_pt):
    row, _ = S.validate_incident(_row(**given))
    assert row["geom_precision"] == expect_prec
    assert (row["lon"] is not None) == expect_pt
    has_geom = row["lon"] is not None
    assert has_geom == (row["geom_precision"] in ("exact", "fuzzy", "road"))  # fire119_incidents_geom_precision_ck


def test_validate_dest_geom_only_new_taipei_and_bad_enums():
    row, notes = S.validate_incident(_row(dest_lon=121.4, dest_lat=25.0, category="x", status="y"))
    assert row["dest_lon"] is None and row["category"] == "other" and row["status"] == "other" and notes
    row, _ = S.validate_incident(_row(source="new_taipei", dest_lon=121.4, dest_lat=25.0))
    assert row["dest_lon"] == 121.4
    assert S.validate_incident(_row(county=""))[0] is None


# ── 差異狀態與狀態追加 ─────────────────────────────────────────────────
def test_seen_state_only_new_or_changed(tmp_path):
    st = S.SeenState(tmp_path / "s.json")
    assert st.changed("k", "已派遣", ["A"], "h")
    st.commit("k", "已派遣", ["A"], "h", "2026-11-16T14:00:00+08:00")
    assert not st.changed("k", "已派遣", ["A"], "h")
    assert not st.changed("k", "已派遣", [], "h")         # 分隊只增不減
    assert st.changed("k", "已到達", ["A"], "h")           # 狀態變
    assert st.changed("k", "已派遣", ["A", "B"], "h")      # 分隊變多
    assert st.changed("k", "已派遣", ["A"], "h2")          # 內容變
    st.save()
    st2 = S.SeenState(tmp_path / "s.json")
    assert not st2.changed("k", "已派遣", ["A"], "h")
    assert st2.prune(datetime(2026, 11, 17, 3, 0, tzinfo=TZ)) == 1


def test_dedupe_keeps_last():
    assert S.dedupe([{"k": 1, "v": "a"}, {"k": 1, "v": "b"}, {"k": 2, "v": "c"}], "k") == \
        [{"k": 1, "v": "b"}, {"k": 2, "v": "c"}]


# ── SQL 形狀（不連 DB；以 bulk 的固定形狀改寫驗證）────────────────────
INCIDENT_TYPES = {c: ("text", "text") for c in S.INCIDENT_COLS}
INCIDENT_TYPES.update(status_history=("jsonb", "jsonb"), received_at=("timestamp with time zone", "timestamptz"),
                      geom=("geometry(Point,4326)", "geometry"), dest_geom=("geometry(Point,4326)", "geometry"),
                      units=("text[]", "_text"), first_seen_at=("timestamp with time zone", "timestamptz"),
                      last_seen_at=("timestamp with time zone", "timestamptz"),
                      updated_at=("timestamp with time zone", "timestamptz"))


def test_incident_upsert_is_fixed_shape_and_replay_safe():
    fixed, spec = bulk.build_insert_sql(S.UPSERT_INCIDENTS_SQL, INCIDENT_TYPES)
    assert "json_to_recordset(%s::json)" in fixed and fixed.count("%s") == 1
    assert "ON CONFLICT (incident_key) DO UPDATE" in fixed
    assert "WHERE EXCLUDED.updated_at > t.updated_at" in fixed
    assert "status_raw IS DISTINCT FROM t.status_raw THEN t.status_history || EXCLUDED.status_history" in fixed
    assert "first_seen_at" not in fixed.split("DO UPDATE SET", 1)[1]  # 首見時間不被覆蓋
    row, _ = S.validate_incident(_row(geom_precision="road", lon=120.31, lat=22.62, units=["A"]))
    tup = S.incident_tuple(dict(row, updated_at=row["first_seen_at"], status_raw="已派遣"))
    payload = json.loads(bulk.serialize_rows([tup], spec))
    assert payload[0]["c10"] == "SRID=4326;POINT(120.31 22.62)" and payload[0]["c16"] is None


def test_pbs_upsert_is_fixed_shape():
    types = {c: ("text", "text") for c in S.PBS_COLS}
    types.update(status_history=("jsonb", "jsonb"), geom=("geometry(Point,4326)", "geometry"))
    fixed, _ = bulk.build_insert_sql(S.UPSERT_PBS_SQL, types)
    assert "ON CONFLICT (uid) DO UPDATE" in fixed and "WHERE EXCLUDED.updated_at > t.updated_at" in fixed


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))


class FakeConn:
    def __init__(self):
        self.executed, self.commits = [], 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


def test_write_payload_dedupes_and_uses_set_local():
    conn = FakeConn()
    calls = []
    r1 = dict(_row(), status_raw="a", updated_at="2026-11-16T14:00:00+08:00", units=[])
    r2 = dict(r1, status_raw="b")
    out = S.write_payload(conn, {"fire119": [r1, r2], "pbs": []},
                          lambda cur, sql, rows, page_size: calls.append(rows))
    assert out == {"fire119": 1, "pbs": 0} and len(calls[0]) == 1 and calls[0][0][6] == "b"
    assert conn.executed[0][0].startswith("SET LOCAL statement_timeout") and conn.commits == 1


# ── 分頻排程 ───────────────────────────────────────────────────────────
def test_schedule_phases_and_overdue():
    odd = datetime(2026, 11, 16, 14, 21, 25, tzinfo=TZ)
    names = C.due_sources(odd, {}, 0, "03:00-03:25", set())
    assert set(names) == set(P.SOURCES)                     # 第一次（沒抓過）全部到期
    assert names[:3] == ["taichung", "new_taipei", "yunlin"]  # 1 分鐘來源排最前
    just = {n: (odd - timedelta(minutes=P.SOURCES[n]["interval"])).isoformat() for n in P.SOURCES}
    due = set(C.due_sources(odd, just, 0, "03:00-03:25", set()))
    assert {"taichung", "new_taipei", "yunlin", "taipei", "kaohsiung"} <= due
    assert "hsinchu_city" not in due                         # 偶數分鐘的 2 分鐘來源
    assert "pingtung" in due and "taoyuan" not in due        # 21 % 5 == 1
    assert not C.is_due("taipei", odd, {"taipei": (odd - timedelta(seconds=60)).isoformat()}, 0)  # 剛抓過
    even = odd + timedelta(minutes=1)
    last2 = {n: (even - timedelta(minutes=P.SOURCES[n]["interval"])).isoformat() for n in P.SOURCES}
    due2 = set(C.due_sources(even, last2, 0, "03:00-03:25", set()))
    assert "hsinchu_city" in due2 and "taipei" not in due2
    # 降頻一級：1 分鐘來源變 2 分鐘
    assert not C.is_due("taichung", odd, {"taichung": (odd - timedelta(seconds=60)).isoformat()}, 1)
    # 漏抓太久一律到期
    assert C.is_due("taoyuan", odd, {"taoyuan": (odd - timedelta(minutes=6)).isoformat()}, 0)


def test_archive_window_only_one_minute_sources_and_disabled():
    t = datetime(2026, 11, 17, 3, 10, 25, tzinfo=TZ)
    names = C.due_sources(t, {}, 0, "03:00-03:25", {"yunlin"})
    assert names == ["taichung", "new_taipei"]


# ── runner 整輪（假 session，不連網、不連 DB）───────────────────────────
class FakeResp:
    def __init__(self, content, status=200):
        self.content, self.status_code = content, status


class FakeSession:
    def __init__(self, overrides=None):
        self.calls = []
        self.overrides = overrides or {}
        self.by_url = {P.SOURCES[s]["url"]: s for s in P.SOURCES}

    def _resp(self, url):
        s = self.by_url[url]
        self.calls.append(s)
        if s in self.overrides:
            return self.overrides[s]
        return FakeResp(fx(FIXTURE_OF.get(s, "yilan_dts_empty.html")))

    def get(self, url, headers=None, timeout=None):
        return self._resp(url)

    def post(self, url, headers=None, data=None, timeout=None):
        return self._resp(url)


@pytest.fixture
def env(tmp_path, monkeypatch, gaz):
    monkeypatch.setenv("RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("GAZETTEER_PATH", str(tmp_path / "gz.sqlite"))
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    return SimpleNamespace(tmp=tmp_path, args=SimpleNamespace(
        state_dir=str(tmp_path / "state"), data_dir=str(tmp_path / "data"), dry_run=True,
        out=str(tmp_path / "out"), all=True, only=None, deadline=999, fixed_now=True))


def _round(env, now=NOW, session=None, meminfo=lambda: 900):
    return C.run_round(env.args, now=now, session=session or FakeSession(), sleep=lambda _s: None,
                       meminfo=meminfo)


def test_memory_threshold_skips_round(env, caplog):
    sess = FakeSession()
    assert _round(env, session=sess, meminfo=lambda: 120) == 0
    assert sess.calls == []
    h = json.loads((env.tmp / "state" / "source_health.json").read_text())
    assert sum(v.get("mem_skip", 0) for v in h["runner"]["hourly"].values()) == 1
    assert "記憶體不足" in caplog.text


def test_full_dry_round_then_only_changes(env, caplog):
    caplog.set_level("INFO")
    assert _round(env) == 0
    lines = [json.loads(x) for x in (env.tmp / "out").glob("*.jsonl").__next__().read_text().splitlines()]
    inc = [x for x in lines if x["kind"] == "fire119"]
    assert len({x["incident_key"] for x in inc}) == len(inc) > 50
    assert not any((x["location_text"] or "").startswith("測試") for x in inc)
    for x in inc:  # 寫入契約：geom ⇔ precision、dest 只給新北
        assert (x["lon"] is not None) == (x["geom_precision"] in ("exact", "fuzzy", "road"))
        assert x["dest_lon"] is None or x["source"] == "new_taipei"
        assert x["received_at"] is None or x["received_at"].endswith("+08:00")
    # 原始檔已剔除隱私欄位、放在 RAW_DIR（不在歸檔目錄）
    import gzip
    raw_ntpc = next((env.tmp / "raw" / "new_taipei").rglob("*.gz"))
    assert "path" not in gzip.decompress(raw_ntpc.read_bytes()).decode()
    assert list((env.tmp / "out" / "data" / "fire119").rglob("fire119_*.json"))
    assert not (env.tmp / "data" / "fire119").exists()  # dry-run 不進歸檔目錄
    assert "本輪結束 sources=19 ok=19" in caplog.text
    # 第二輪內容不變 → 0 列要寫
    caplog.clear()
    assert _round(env, now=NOW + timedelta(minutes=1)) == 0
    assert "changed=0+0" in caplog.text
    # 第三輪臺中狀態改變 → 只寫那幾列
    caplog.clear()
    changed = fx("taichung.html").replace("已出動".encode(), "已到達".encode())
    _round(env, now=NOW + timedelta(minutes=2), session=FakeSession({"taichung": FakeResp(changed)}))
    assert "changed=2+0" in caplog.text
    lines = [json.loads(x) for x in next((env.tmp / "out").glob("*.jsonl")).read_text().splitlines()]
    last = [x for x in lines if x["kind"] == "fire119" and x["source"] == "taichung"][-1]
    assert last["status_raw"] == "已到達" and last["status_history"][0][1] == "已到達"


def test_failures_and_zero_rows_are_separate_signals(env):
    sess = FakeSession({"taipei": FakeResp(b"", 500), "kaohsiung": FakeResp(b"<html>maintenance</html>")})
    _round(env, session=sess)
    h = json.loads((env.tmp / "state" / "source_health.json").read_text())
    assert h["sources"]["taipei"]["last_error"] == "HTTP 500"
    assert h["sources"]["kaohsiung"]["last_error"].startswith("parse:")
    assert h["sources"]["yilan"]["last_rows"] == 0 and "last_error" not in h["sources"]["yilan"]
    assert any("taipei" in w for w in h["warnings"]) and any("kaohsiung" in w for w in h["warnings"])
    # 7 小時後仍失敗 → 紅；宜蘭常態 0 筆不告警，臺中 0 筆超過 3 小時 → 黃
    later = NOW + timedelta(hours=7)
    zero_tc = b"<div class='timely'><ul><li class='timely_head'><span>x</span></li></ul></div>"
    _round(env, now=later, session=FakeSession({"taipei": FakeResp(b"", 500), "taichung": FakeResp(zero_tc)}))
    _round(env, now=later + timedelta(hours=4), session=FakeSession({"taipei": FakeResp(b"", 500),
                                                                     "taichung": FakeResp(zero_tc)}))
    h = json.loads((env.tmp / "state" / "source_health.json").read_text())
    assert any("taipei" in a for a in h["alerts"])
    assert any("taichung" in w and "0 筆" in w for w in h["warnings"])
    assert not any("yilan" in w for w in h["warnings"] + h["alerts"])


def test_unfinished_and_slow_rounds_trigger_degrade(env, monkeypatch):
    state = env.tmp / "state"
    state.mkdir(parents=True)
    # 上一輪被 timeout 砍掉（留下 running 標記）＋之後每輪都超過 OVERRUN_SEC → 連續 3 次逾時
    (state / "runner.json").write_text(json.dumps({"running": (NOW - timedelta(minutes=1)).isoformat()}))
    monkeypatch.setitem(C.DEFAULTS, "OVERRUN_SEC", "-1")
    env.args.all, env.args.only = False, "yilan"
    _round(env, now=NOW)
    assert json.loads((state / "runner.json").read_text())["overrun_streak"] == 2
    _round(env, now=NOW + timedelta(minutes=1))
    assert json.loads((state / "runner.json").read_text())["degrade_level"] == 0
    _round(env, now=NOW + timedelta(minutes=2))
    runner = json.loads((state / "runner.json").read_text())
    assert runner["degrade_level"] == 1 and (state / "ALERT_degraded").exists()
    assert "running" not in runner
    h = json.loads((state / "source_health.json").read_text())
    assert any("降頻" in a for a in h["alerts"])


def test_db_failure_buffers_and_keeps_state_consistent(env, monkeypatch):
    env.args.dry_run = False
    env.args.only = "taichung"
    env.args.all = False
    calls = {}

    def fake_db(payload, last_seen, buffer_dir):
        calls["n"] = len(payload["fire119"])
        return "fail", {}
    monkeypatch.setattr(C, "_db_write", fake_db)
    assert _round(env) == 1
    st = json.loads((env.tmp / "state" / "incidents_seen.json").read_text())
    assert st == {}  # 沒落地 → 不記為已寫，下一輪重送
    monkeypatch.setattr(C, "_db_write", lambda p, l, b: ("buffered", {}))
    assert _round(env, now=NOW + timedelta(minutes=1)) == 0
    assert len(json.loads((env.tmp / "state" / "incidents_seen.json").read_text())) == calls["n"] == 4


# ── 記憶體上限（只在 Linux 驗：ulimit 模式的位址空間上限要能跑完整條解析）──
@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="RLIMIT_AS 只在 Linux 有意義")
def test_pipeline_runs_under_address_space_limit(tmp_path):
    code = textwrap.dedent(f"""
        import resource, sys
        resource.setrlimit(resource.RLIMIT_AS, (400 * 1024 * 1024, 400 * 1024 * 1024))
        sys.path[:0] = [{str(APP)!r}, {str(ROOT / 'external' / 'vm_common')!r}]
        import requests, psycopg2, bs4, sqlite3, gzip, json
        import fire119_parse as P, fire119_store as S, fire119_collect, bulk, vm_buffer
        from pathlib import Path
        fx = Path({str(FIX)!r})
        n = 0
        for src, fn in {FIXTURE_OF!r}.items():
            n += len(P.parse(src, P.sanitize(src, (fx / fn).read_bytes())))
        print(n)
    """)
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-2000:]
    assert int(out.stdout.strip()) > 50


# ── 歸檔：原始檔清理 ───────────────────────────────────────────────────
def test_archive_purges_old_raw_only(tmp_path, monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location("archive_fire119_t", APP / "archive_fire119.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for d in ("20261112", "20261113", "20261114", "20261115", "20261116"):
        (tmp_path / "taipei" / d).mkdir(parents=True)
    today = datetime(2026, 11, 16, tzinfo=TZ)
    assert mod.purge_raw(tmp_path, today, 3) == 2
    assert sorted(p.name for p in (tmp_path / "taipei").iterdir()) == ["20261114", "20261115", "20261116"]
    assert mod.COLLECTORS == ("fire119", "pbs_road_events")
