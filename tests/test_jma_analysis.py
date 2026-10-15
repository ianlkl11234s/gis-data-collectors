"""JMA 分析圖（collectors/weather_raster/analysis.py）單元測試。不打網路、不碰真 S3／R2／DB。"""
from __future__ import annotations

import io
import json
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pytest

from collectors.weather_raster import analysis as ana
from collectors.weather_raster import publish as pub

H6, W6 = ana.Z6_SHAPE
H7, W7 = H6 * 2, W6 * 2


# ───────────────────────── 假物件 ─────────────────────────

class FakeS3:
    """S3StateStore 用到的介面：get_file／head（Metadata）／list_files／s3.put_object／bucket。"""

    def __init__(self):
        self.objects: dict[str, tuple[bytes, dict]] = {}
        self.bucket = "test"
        self.s3 = self
        self.gets = 0

    def put_object(self, Bucket, Key, Body, StorageClass, ContentType, Metadata):  # noqa: N803
        self.objects[Key] = (Body, dict(Metadata))

    def get_file(self, key):
        self.gets += 1
        o = self.objects.get(key)
        return o[0] if o else None

    def head(self, key):
        o = self.objects.get(key)
        return None if o is None else {"ContentLength": len(o[0]), "Metadata": o[1]}

    def list_files(self, prefix, max_keys=1000):
        return [{"key": k} for k in sorted(self.objects) if k.startswith(prefix)]


class FakeR2:
    def __init__(self, fail=False):
        self.objects: dict[str, tuple[bytes, str, str]] = {}
        self.fail = fail

    def upload_image(self, key, data, content_type, cache_control=None):
        if self.fail:
            raise RuntimeError("R2 down")
        self.objects[key] = (data, content_type, cache_control)


def t1_bytes(shape, *, fe, vf=None, max_level=None, missing=None, count_ge_3=None) -> bytes:
    vf = fe if vf is None else vf
    arrays = {"max_level": np.zeros(shape, np.uint8) if max_level is None else max_level.astype(np.uint8),
              "missing_count": np.zeros(shape, np.uint16) if missing is None else missing.astype(np.uint16),
              "valid_frames": np.array(vf, np.uint16)}
    if count_ge_3 is not None:
        arrays["count_ge_3"] = count_ge_3.astype(np.uint16)
    meta = {"frames_expected": fe, "valid_frames": vf}
    bio = io.BytesIO()
    np.savez_compressed(bio, meta=np.array(json.dumps(meta)), **arrays)
    return bio.getvalue()


def put_t1(s3: FakeS3, product: str, day: str, data: bytes) -> None:
    s3.objects[ana.t1_key(product, day)] = (data, {})


def engine(s3, r2=None, epoch=date(2026, 10, 1), **kw):
    return ana.AnalysisEngine(ana.S3StateStore(s3), r2 if r2 is not None else FakeR2(), epoch=epoch,
                              warn=lambda m: None, **kw)


# ───────────────────────── 純函式 ─────────────────────────

def test_hours_identical_for_10min_and_30min_cadence_days():
    """risk_* 10-11 是 144 幀（10 分），之後 48 幀（30 分）：同樣 1 小時警戒要得到同樣 60 分。"""
    fam = ana.FAMILIES["risk_land"]
    c144 = np.zeros((H7, W7), np.uint16); c144[0, 0] = 6
    c48 = np.zeros((H7, W7), np.uint16); c48[0, 0] = 2
    a = ana.day_contribution(fam, t1_bytes((H7, W7), fe=144, count_ge_3=c144))
    b = ana.day_contribution(fam, t1_bytes((H7, W7), fe=48, count_ge_3=c48))
    assert a["warn_min"][0, 0] == b["warn_min"][0, 0] == 60
    assert a["obs_min"][0, 0] == b["obs_min"][0, 0] == 1440
    assert a["warn_min"].shape == ana.Z6_SHAPE


def test_extra_off_cadence_frames_shrink_frame_duration():
    """10-12 risk：expected 48、valid 77 → 每幀 1440/77 分，觀測總量仍是一整天。"""
    fam = ana.FAMILIES["risk_inund"]
    c = np.zeros((H7, W7), np.uint16); c[0, 0] = 77
    r = ana.day_contribution(fam, t1_bytes((H7, W7), fe=48, vf=77, count_ge_3=c), warn=lambda m: None)
    assert r["warn_min"][0, 0] == 1440 and r["obs_min"][0, 0] == 1440


def test_z7_pooling_event_any_and_coverage_best_subpixel():
    fam = ana.FAMILIES["rain"]
    ml = np.zeros((H7, W7), np.uint8); ml[1, 1] = 2      # 2×2 塊 (0,0) 只有一格 ≥1 mm
    ml[2:4, 2:4] = 255                                   # 塊 (1,1) 全缺測
    ml[4, 4] = 1                                         # level 1 不算雨日
    miss = np.zeros((H7, W7), np.uint16); miss[2:4, 2:4] = 48; miss[6, 6] = 48  # 塊 (3,3) 只一格缺
    r = ana.day_contribution(fam, t1_bytes((H7, W7), fe=48, max_level=ml, missing=miss))
    assert r["event"][0, 0] and not r["event"][1, 1] and not r["event"][2, 2]
    assert not r["observed"][1, 1], "4 格全缺 → 無觀測"
    assert r["observed"][3, 3], "2×2 取最佳覆蓋的子格"


def test_snow_threshold_level2_and_missing_255_not_snow():
    fam = ana.FAMILIES["snow"]
    ml = np.zeros((H6, W6), np.uint8); ml[0, 0] = 1; ml[0, 1] = 2; ml[0, 2] = 7; ml[0, 3] = 255
    miss = np.zeros((H6, W6), np.uint16); miss[0, 3] = 24
    r = ana.day_contribution(fam, t1_bytes((H6, W6), fe=24, max_level=ml, missing=miss))
    assert r["event"][0, :4].tolist() == [False, True, True, False]
    assert r["observed"][0, :4].tolist() == [True, True, True, False]


def test_count_la_clamp_transparent_and_insufficient():
    v = np.array([[0, 1, 254, 300, 5]])
    cov = np.array([[1.0, 1.0, 1.0, 1.0, 0.2]])
    L, A = pub.decode_png_la(pub.png_la(*ana.count_la(v, cov, 0.5)))
    assert L.tolist() == [[0, 1, 254, 254, 255]]
    assert A.tolist() == [[0, 255, 255, 255, 255]]


def test_period_and_day_of_season():
    snow = ana.FAMILIES["snow"]
    assert ana.period_of(snow, date(2027, 2, 3)) == ("season2026", date(2026, 10, 1))
    assert ana.period_of(snow, date(2026, 7, 1)) is None
    assert ana.day_of_season(date(2026, 10, 1)) == 1 and ana.day_of_season(date(2027, 5, 31)) == 243
    assert ana.period_of(ana.FAMILIES["rain"], date(2026, 10, 15)) == ("202610", date(2026, 10, 1))
    vt = ana.valid_time(date(2026, 10, 15))
    assert pub.r2_key("ana_rain_days", vt) == "imagery/jma/ana_rain_days/20261014/150000.png"


# ───────────────────────── 引擎 ─────────────────────────

def snow_ml(pixels) -> np.ndarray:
    ml = np.zeros((H6, W6), np.uint8)
    for (r, c), lv in pixels.items():
        ml[r, c] = lv
    return ml


def test_snow_first_and_days_accumulate_with_missing_t1_day():
    s3, r2 = FakeS3(), FakeR2()
    put_t1(s3, "snow_depth", "20261001", t1_bytes((H6, W6), fe=24, max_level=snow_ml({(0, 0): 2})))
    # 10-02 沒有 T1
    put_t1(s3, "snow_depth", "20261003", t1_bytes((H6, W6), fe=24, max_level=snow_ml({(0, 0): 3, (0, 1): 2})))
    e = engine(s3, r2)
    today = date(2026, 10, 10)
    rows = []
    for d in ana.target_days(date(2026, 10, 1), date(2026, 10, 3)):
        assert e.eligible(ana.FAMILIES["snow"], d, today)
        rows += e.publish_day(ana.FAMILIES["snow"], d)
    last = {r["product"]: r for r in rows if r["_day"] == "20261003"}
    assert {r["status"] for r in last.values()} == {"partial"}, "10-02 缺 T1 → partial"
    L, A = pub.decode_png_la(r2.objects[last["ana_snow_first"]["image_key"]][0])
    assert (L[0, 0], L[0, 1], A[0, 2]) == (1, 3, 0), "首日：(0,0)=10/1、(0,1)=10/3；無雪透明"
    L, A = pub.decode_png_la(r2.objects[last["ana_snow_days"]["image_key"]][0])
    assert (L[0, 0], L[0, 1]) == (2, 1)
    # 覆蓋 2/3 ≥ 0.5 → 不是未涵蓋；缺日沒有變成 0 也沒有變成「無雪」
    assert A[5, 5] == 0 and L[5, 5] == 0
    side = json.loads(r2.objects[last["ana_snow_days"]["image_key"][:-4] + ".json"][0])
    assert side["t1_absent_days"] == ["20261002"] and side["elapsed_days"] == 3
    assert r2.objects[last["ana_snow_days"]["image_key"]][2] == ana.CACHE_CONTROL
    assert last["ana_snow_days"]["valid_time"] == "2026-10-03T00:00:00+09:00"
    assert last["ana_snow_days"]["encoding"] == "count_la_v1"


def test_insufficient_coverage_marks_255_and_no_t1_is_missing_without_image():
    s3, r2 = FakeS3(), FakeR2()
    e = engine(s3, r2)
    fam = ana.FAMILIES["rain"]
    rows = e.publish_day(fam, date(2026, 10, 1))
    assert rows[0]["status"] == "missing" and "image_key" not in rows[0] and not r2.objects
    put_t1(s3, "rasrf", "20261003", t1_bytes((H7, W7), fe=48))
    e2 = engine(s3, r2)
    r = e2.publish_day(fam, date(2026, 10, 3))[0]
    L, A = pub.decode_png_la(r2.objects[r["image_key"]][0])
    assert (L == 255).all() and (A == 255).all(), "3 天只 1 天有觀測（1/3 < 0.5）→ 全部未涵蓋，不是 0"


def test_late_t1_invalidates_downstream_snapshots_and_republishes():
    s3, r2 = FakeS3(), FakeR2()
    fam = ana.FAMILIES["rain"]
    ml = np.zeros((H7, W7), np.uint8); ml[0, 0] = 3
    put_t1(s3, "rasrf", "20261001", t1_bytes((H7, W7), fe=48, max_level=ml))
    put_t1(s3, "rasrf", "20261003", t1_bytes((H7, W7), fe=48, max_level=ml))
    cache: dict = {}
    e = engine(s3, r2)
    for d in ana.target_days(date(2026, 10, 1), date(2026, 10, 3)):
        for row in e.publish_day(fam, d, publish_cache=cache):
            cache.setdefault(row["product"], {})[row["_day"]] = row["_pub_sig"]
    k3 = "imagery/jma/ana_rain_days/20261002/150000.png"
    assert pub.decode_png_la(r2.objects[k3][0])[0][0, 0] == 2

    # 同樣輸入再跑：全部命中快取，不讀 S3
    e2 = engine(s3, r2)
    gets = s3.gets
    assert all(not e2.publish_day(fam, d, publish_cache=cache) for d in ana.target_days(date(2026, 10, 1), date(2026, 10, 3)))
    assert s3.gets == gets

    # 10-02 的 T1 遲到：10-02、10-03 的 sig 改變 → 重算重發；10-01 不動
    put_t1(s3, "rasrf", "20261002", t1_bytes((H7, W7), fe=48, max_level=ml))
    e3 = engine(s3, r2)
    redo = [r["_day"] for d in ana.target_days(date(2026, 10, 1), date(2026, 10, 3))
            for r in e3.publish_day(fam, d, publish_cache=cache)]
    assert redo == ["20261002", "20261003"]
    L, _ = pub.decode_png_la(r2.objects[k3][0])
    assert L[0, 0] == 3
    assert e3.stats["t1_read"] == 2, "10-01 快照沿用，只讀 10-02、10-03 的 T1"


def test_recompute_from_scratch_matches_incremental():
    s3 = FakeS3()
    fam = ana.FAMILIES["risk_land"]
    for i, n in enumerate([2, 0, 5]):
        c = np.zeros((H7, W7), np.uint16); c[10, 10] = n
        put_t1(s3, "risk_land", f"2026100{i + 1}", t1_bytes((H7, W7), fe=48, count_ge_3=c))
    e = engine(s3)
    for d in ana.target_days(date(2026, 10, 1), date(2026, 10, 3)):
        e.publish_day(fam, d, dry_run=True)
    inc, _m, _s = e.ensure_state(fam, date(2026, 10, 3))
    full, _m2, _s2 = engine(s3).ensure_state(fam, date(2026, 10, 3), recompute=True)
    assert np.array_equal(inc["warn_min"], full["warn_min"]) and full["warn_min"][5, 5] == 210
    val, cov = ana.output_values("ana_risk_land_hours", full, 3)
    assert val[5, 5] == 4 and cov[5, 5] == 1.0, "210 分 → 進位 4 小時"


def test_epoch_truncation_is_partial_and_off_season_not_eligible():
    s3 = FakeS3()
    put_t1(s3, "rasrf", "20261010", t1_bytes((H7, W7), fe=48))
    e = engine(s3, epoch=date(2026, 10, 10))
    fam = ana.FAMILIES["rain"]
    assert not e.eligible(fam, date(2026, 10, 9), date(2026, 10, 20))
    r = e.publish_day(fam, date(2026, 10, 10), dry_run=True)[0]
    assert r["status"] == "partial", "EPOCH 晚於月初 → 只算到部分月份"
    assert not e.eligible(ana.FAMILIES["snow"], date(2027, 7, 1), date(2027, 7, 5))
    assert not e.eligible(fam, date(2026, 10, 12), date(2026, 10, 13)), "T1 未到且未過寬限期 → 等"
    assert e.eligible(fam, date(2026, 10, 12), date(2026, 10, 16)), "過寬限期 → 當缺日出圖"


def test_r2_failure_returns_no_row_and_raises_nothing():
    s3 = FakeS3()
    put_t1(s3, "rasrf", "20261001", t1_bytes((H7, W7), fe=48))
    e = engine(s3, FakeR2(fail=True))
    assert e.publish_day(ana.FAMILIES["rain"], date(2026, 10, 1)) == []
    assert e.stats["publish_failed"] == 1


# ───────────────────────── collector 整合 ─────────────────────────

def test_collector_analysis_rows_do_not_collide_with_frame_keys(tmp_path, monkeypatch):
    """00:00 JST＝15:00 UTC 也是 snow_depth 的整點 vt：分析列必須放在獨立 pending，不可蓋掉 frames 列。"""
    from tests.test_jma_raster import make_collector
    from collectors.global_climate import jma_raster as jr
    s3, r2 = FakeS3(), FakeR2()
    ml = np.zeros((H6, W6), np.uint8); ml[0, 0] = 2
    put_t1(s3, "snow_depth", "20261010", t1_bytes((H6, W6), fe=24, max_level=ml))
    c = make_collector(tmp_path, ["snow_depth"], s3, lambda url: (404, b""))
    c._r2 = r2
    monkeypatch.setattr("config.JAPAN_JMA_ANALYSIS_EPOCH", "2026-10-10")
    st = c._load_state("snow_depth")
    st["pending_frames"]["20261009150000"] = {"_type": "frame", "product": "snow_depth", "status": "ok"}
    c._save_state("snow_depth", st)
    now = datetime(2026, 10, 11, 3, 0, tzinfo=timezone.utc)
    out = c.run_analysis(now, deadline=float("inf"))
    assert out["rows"] == 2
    ast = c._load_analysis_state()
    assert set(ast["pending"]) == {"ana_snow_first:20261010", "ana_snow_days:20261010"}
    assert all(not k.startswith("_") or k == "_type" for r in ast["pending"].values() for k in r)
    assert c._load_state("snow_depth")["pending_frames"]["20261009150000"]["product"] == "snow_depth"
    # 再跑一輪：快取命中，不重發
    assert c.run_analysis(now, deadline=float("inf"))["rows"] == 0
    c._commit_analysis({"rows": ["ana_snow_first:20261010"]})
    assert set(c._load_analysis_state()["pending"]) == {"ana_snow_days:20261010"}


def test_writer_accepts_analysis_rows(monkeypatch):
    from storage.supabase_writer import SupabaseWriter
    w = SupabaseWriter.__new__(SupabaseWriter)
    row = {"_type": "frame", "source": "jma", "product": "ana_rain_days", "valid_time": "2026-10-10T00:00:00+09:00",
           "status": "partial", "nonzero_pixels": 3, "image_key": "imagery/jma/ana_rain_days/20261009/150000.png"}
    assert w._transform_jma_raster({"data": [row]}, datetime.now(timezone.utc)) == [row]
