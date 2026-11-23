"""B8 即時快照三件：utils/tw_live_snapshots.py 純解析＋三支 collector 的註冊與 R2 寫入行為。

fixtures 取自 2026-11-21 實抓（登革熱用 Wayback 2024-11-15 新北快照，因為當天 23 檔全空）。
"""
import json
from datetime import datetime
from pathlib import Path

import pytest

from utils import tw_live_snapshots as T

FX = Path(__file__).parent / "fixtures" / "tw_live_snapshots"
NOW = datetime(2026, 11, 21, 17, 0, tzinfo=T.TPE)


def _load(name):
    return json.loads((FX / name).read_text(encoding="utf-8"))


# ─── dengue ──────────────────────────────────────────────────────────────────
def _dengue_files(newtaipei):
    empty = {"type": "FeatureCollection", "features": []}
    files = {c: empty for c in T.DENGUE_CITIES}
    files["NewTaipei"] = newtaipei
    files["all"] = newtaipei
    return files


def test_dengue_sample_normalises_and_crosschecks_all():
    fc, st = T.dengue_normalise(_dengue_files(_load("dengue_newtaipei_20241115_wayback.json")), "20241115", "x")
    assert st["feature_count"] == 3 and st["all_matches_city_union"] is True
    p = fc["features"][0]["properties"]
    assert p["county"] == "新北市" and isinstance(p["case_count"], int)
    assert st["issues"].get("count_lt_2") == 2  # 來源說明寫 ≥2，但歷史快照有 COUNT=1：照實記，不篩掉


def test_dengue_empty_day_is_not_failure():
    empty = {"type": "FeatureCollection", "features": []}
    fc, st = T.dengue_normalise({**{c: empty for c in T.DENGUE_CITIES}, "all": empty}, "20261121", "x")
    assert fc["features"] == [] and st["snapshot_empty"] is True and st["all_matches_city_union"] is True


def test_dengue_rejects_non_featurecollection():
    with pytest.raises(ValueError):
        T.dengue_normalise({"Tainan": {"type": "Feature"}}, "20261121", "x")


# ─── nav notices ─────────────────────────────────────────────────────────────
def test_nav_merges_duplicates_and_unswaps_windows():
    src = _load("nav_notices_subset_20261121.json")
    latest, allfc, st = T.nav_normalise(src, NOW)
    dup = [f for f in allfc["features"] if f["properties"]["notice_code"] == "20250195"]
    assert len(dup) == 9  # 來源 81 列＝9 個圖形各重複 9 次，名稱與座標一一對應
    for f in allfc["features"]:
        for w in json.loads(f["properties"]["windows"] or "[]"):
            assert w["from"] is None or w["to"] is None or w["from"] <= w["to"]


def test_nav_derives_circle_and_sector_polygons():
    _, allfc, _ = T.nav_normalise(_load("nav_notices_subset_20261121.json"), NOW)
    derived = [f for f in allfc["features"] if f["properties"]["geometry_derived"]]
    shapes = {f["properties"]["source_shape"] for f in derived}
    assert {"Circle", "Sector"} <= shapes
    for f in derived:
        ring = f["geometry"]["coordinates"][0]
        assert f["geometry"]["type"] == "Polygon" and ring[0] == ring[-1]
        assert f["properties"]["center_lon"] is not None and f["properties"]["radius"]


def test_nav_excludes_suspect_coords_and_masks_phone():
    latest, allfc, st = T.nav_normalise(_load("nav_notices_subset_20261121.json"), NOW)
    assert "20260194:L" in st["suspect_coords_excluded"]
    assert all(f["properties"]["feature_id"] != "20260194:L" for f in latest["features"])
    for f in latest["features"]:
        assert not T._PHONE.search(f["properties"]["info"] or "")


def test_nav_null_geometry_counted_not_drawn():
    src = _load("nav_notices_subset_20261121.json")
    p = dict(src["features"][0]["properties"], code="99999999", shape="")
    src["features"].append({"type": "Feature", "geometry": None, "properties": p})
    latest, _, st = T.nav_normalise(src, NOW)
    assert st["unlocated_notices"] >= 1
    assert all(f["properties"]["notice_code"] != "99999999" for f in latest["features"])


def test_nav_state_filters_future_and_expired():
    src = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [120.0, 23.0]},
         "properties": {"code": c, "shape_name": "A", "name": c, "noticekind": "礙航公告", "startdate": s, "enddate": e,
                        "lifetime": "非永久", "shape": "Point"}}
        for c, s, e in [("old", "2026/01/01 00:00:00", "2026/02/01 00:00:00"),
                        ("future", "2027/01/01 00:00:00", ""), ("perm", "2017/01/01 00:00:00", "")]]}
    latest, _, st = T.nav_normalise(src, NOW)
    assert [f["properties"]["notice_code"] for f in latest["features"]] == ["perm"]
    assert st["state"] == {"expired": 1, "not_started": 1, "active": 1}


# ─── marine alerts ───────────────────────────────────────────────────────────
def _marine():
    return (_load("O-B0069-001_20261121.json")["cwaopendata"], _load("O-B0070-001_20261121.json")["cwaopendata"],
            _load("O-B0076-001_20261121_trimmed.json")["cwaopendata"])


def test_marine_alerts_match_counts():
    fc, st = T.marine_alerts_build(*_marine(), "x")
    assert st["feature_count"] == 40 and st["unlocated"] == []
    by = {f["properties"]["site_id"]: f["properties"] for f in fc["features"]}
    assert by["spring:基隆"]["station_uid"] == "cwa:C4B01" and by["spring:基隆"]["match_method"] == "exact_name"
    # 地區名監測點：以最大潮高對 O-B0075 逐時潮高比對出的站（兩個視窗都唯一）
    for site, sid in [("雲林", "C4J01"), ("嘉義", "C4L02"), ("臺東", "1586"), ("宜蘭", "C4U02"), ("恆春", "C4Q03"),
                      ("苗栗", "C4E01"), ("桃園", "C4C01")]:
        assert by[f"spring:{site}"]["station_uid"] == f"cwa:{sid}"
        assert by[f"spring:{site}"]["match_method"] == "max_tide_crosscheck"
    assert by["swell:臺北港浮標"]["station_uid"] is None  # 舊站表座標，不在既有測站層
    assert all(p["alert_level"] == 0 for p in by.values())


def test_marine_yunlin_surge_warning_20261122():
    """2026-11-22 雲林「暴潮警戒」（最大潮高 2.77 m ≥ 潮位警戒值 2.35 m）：要畫在萡子寮、等級 2、會脈動。"""
    spring = _load("O-B0069-001_20261122.json")["cwaopendata"]
    swell = _load("O-B0070-001_20261122.json")["cwaopendata"]
    stations = _load("O-B0076-001_20261121_trimmed.json")["cwaopendata"]
    fc, st = T.marine_alerts_build(spring, swell, stations, "x")
    p = next(f["properties"] for f in fc["features"] if f["properties"]["site_id"] == "spring:雲林")
    assert p["warning_status"] == "暴潮警戒" and p["alert_level"] == 2
    assert p["station_name"] == "萡子寮潮位站" and p["max_height_m"] == 2.77 and p["surge_criteria_m"] == 2.35
    assert st["alert_sites"] == ["spring:雲林"] and st["unknown_status"] == [] and st["unlocated"] == []


def test_marine_unknown_status_kept_not_crash():
    spring, swell, stations = _marine()
    swell["Resources"]["Resource"]["Data"]["SeaSurfaceObs"][0]["Location"]["StationObsTimes"]["StationObsTime"][
        "WeatherElements"]["WarningStatus"] = "某新狀態"
    fc, st = T.marine_alerts_build(spring, swell, stations, "x")
    assert st["unknown_status"] and any(f["properties"]["alert_level"] is None for f in fc["features"])


def test_content_hash_ignores_volatile():
    fc, _ = T.marine_alerts_build(*_marine(), "a")
    fc2, _ = T.marine_alerts_build(*_marine(), "b")
    assert T.content_hash(fc) == T.content_hash(fc2)


# ─── registry／collector 行為 ────────────────────────────────────────────────
def test_registered_disabled_by_default():
    import config
    from collectors.registry import get_entry_by_name
    for name, prefix in [("cwa_marine_alerts", "CWA_MARINE_ALERTS"), ("cdc_dengue_cluster", "CDC_DENGUE_CLUSTER"),
                         ("motcmpb_nav_notices", "MOTCMPB_NAV_NOTICES")]:
        assert get_entry_by_name(name) is not None
        assert getattr(config, f"{prefix}_ENABLED") is False


class _FakeR2:
    def __init__(self):
        self.puts = []

    def put_json(self, key, payload, cache_control):
        self.puts.append(key)


class _Resp:
    def __init__(self, payload, headers=None):
        self._p, self.headers = payload, headers or {}

    def raise_for_status(self):
        pass

    def json(self):
        return self._p


def test_marine_collector_publishes_once_until_content_changes():
    from collectors.cwa_marine_alerts import CwaMarineAlertsCollector
    spring, swell, stations = _marine()
    payload = {"O-B0069-001": spring, "O-B0070-001": swell, "O-B0076-001": stations}
    c = object.__new__(CwaMarineAlertsCollector)
    c._r2, c._last_hash = _FakeR2(), None

    class S:
        def get(self, url, timeout):
            return _Resp({"cwaopendata": payload[url.rsplit("/", 1)[1].removesuffix(".json")]})
    c._session = S()
    r1, r2 = c.collect(), c.collect()
    assert r1["r2_published"] is True and r2["r2_published"] is False
    assert c._r2.puts.count("live/tw/cwa_tide_swell_alerts/latest.geojson") == 1
    assert len(r1["data"]) == 40 and r1["_upstream_max_at"]


def _marine_collector(payload):
    from collectors.cwa_marine_alerts import CwaMarineAlertsCollector
    c = object.__new__(CwaMarineAlertsCollector)
    c._r2, c._last_hash = _FakeR2(), None

    class S:
        def get(self, url, timeout):
            return _Resp({"cwaopendata": payload[url.rsplit("/", 1)[1].removesuffix(".json")]})
    c._session = S()
    return c


@pytest.mark.parametrize("empty", ["spring", "swell"])
def test_marine_collector_rejects_partial_dataset(empty):
    """E4：任一警戒資料集 200 但沒有監測點 → raise，不可用另一份覆寫完整快照。"""
    import copy
    spring, swell, stations = _marine()
    docs = {"spring": copy.deepcopy(spring), "swell": copy.deepcopy(swell)}
    docs[empty]["Resources"]["Resource"]["Data"]["SeaSurfaceObs"] = []
    c = _marine_collector({"O-B0069-001": docs["spring"], "O-B0070-001": docs["swell"], "O-B0076-001": stations})
    with pytest.raises(RuntimeError, match=empty):
        c.collect()
    assert c._r2.puts == []


def test_nav_collector_skips_unchanged_rounds():
    from collectors.motcmpb_nav_notices import MotcmpbNavNoticesCollector
    src = _load("nav_notices_subset_20261121.json")
    c = object.__new__(MotcmpbNavNoticesCollector)
    c._r2, c._last_hash = _FakeR2(), None

    class S:
        def get(self, url, timeout):
            return _Resp(src)
    c._session = S()
    r1, r2 = c.collect(), c.collect()
    assert "data" in r1 and "data" not in r2  # 沒變的輪次不存 9 MB 原檔
    assert c._r2.puts.count("live/tw/nav_notices/latest.geojson") == 1


def test_nav_masks_contact_names():
    src = {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [120.0, 23.0]},
           "properties": {"code": "n1", "shape_name": "A", "name": "x", "noticekind": "礙航公告", "startdate": "2026/01/01 00:00:00",
                          "enddate": "", "lifetime": "永久", "shape": "Point",
                          "info": "宏華營造股份有限公司 陳宏菖先生(07-8711188)、李小姐 04-26569811#20。三、聯絡人：辛金山 07-5711369"}}]}
    latest, _, st = T.nav_normalise(src, NOW)
    info = latest["features"][0]["properties"]["info"]
    assert "先生" not in info and "小姐" not in info and "辛金山" not in info and "宏華營造" in info
    assert st["info_name_masked_features"] == 1
