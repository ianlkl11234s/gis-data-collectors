"""JMA 發布幀（collectors/weather_raster/publish.py）單元測試。

不打網路、不碰真 R2／S3／DB：R2 以 FakeR2 替換，collector 沿用 test_jma_raster／test_jma_ocean_env 的假物件。
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pytest

from collectors.weather_raster import publish as pub
from collectors.weather_raster import spool as sp
from collectors.weather_raster.decode import MISSING, pool_max
from storage.supabase_writer import SupabaseWriter


class FakeR2:
    def __init__(self, fail=False):
        self.objects: dict[str, bytes] = {}
        self.fail = fail

    def upload_image(self, key, data, content_type, cache_control=None):
        if self.fail:
            raise RuntimeError("R2 down")
        assert content_type == "image/png"
        self.objects[key] = data

    def head(self, key):
        return {"ContentLength": len(self.objects[key])} if key in self.objects else None


# ───────────────────────── 編碼 ─────────────────────────

def test_level_la_round_trip_including_none_and_missing():
    lv = np.array([[0, 1, 2], [8, 254, MISSING]], np.uint8)
    L, A = pub.decode_png_la(pub.png_la(*pub.level_la(lv)))
    assert L.tolist() == [[0, 1, 2], [8, 254, 255]]
    assert A.tolist() == [[0, 255, 255], [255, 255, 255]]


def test_sea_ice_land_transparent_nodata_missing():
    lv = np.array([[0, 2, 5, pub.ICE_LAND, MISSING]], np.uint8)
    L, A = pub.level_la(lv, pub.PUBLISH_PRODUCTS["sea_ice"]["transparent"])
    assert L.tolist() == [[0, 2, 5, 0, 255]] and A.tolist() == [[0, 255, 255, 0, 255]]


def test_pool_max_treats_255_as_missing_not_max():
    g = np.array([[255, 255, 255, 255],
                  [255, 3, 255, 255]], np.uint8)
    g = np.vstack([g, np.zeros((2, 4), np.uint8)])
    out = pool_max(g, 2)
    assert out.tolist() == [[3, 255], [0, 0]], "有一格有效 → 取有效最大；4 格全缺才 255"
    r = pub.render("radar", np.full((4, 4), 255, np.uint8), bounds_lonlat=[0, 0, 1, 1])
    L, A = pub.decode_png_la(r["png"])
    assert (r["width"], r["height"]) == (2, 2) and (L == 255).all() and (A == 255).all()


def test_sst_mapping_and_codes():
    dc = np.array([[-30, 0, 281, 331, 400, pub.SST_LAND, pub.SST_ICE, pub.SST_BAD, -29]], np.int16)
    L, A = pub.sst_la(dc)
    # -3.0℃→0→clamp 1；0℃→21；28.1℃→217.7→218；33.1℃→252.7→253；40℃→clamp 254；-2.9℃→0.7→1
    assert L.tolist() == [[1, 21, 218, 253, 254, 0, 0, 255, 1]]
    assert A.tolist() == [[255, 255, 255, 255, 255, 0, 255, 255, 255]]
    rt_L, rt_A = pub.decode_png_la(pub.png_la(L, A))
    assert (rt_L == L).all() and (rt_A == A).all()


def test_reproject_bounds_and_sizes():
    p_him = {"bounds_lonlat": [100.0, 0.0, 180.0, 60.0], "registration": "area", "step_deg": 0.1}
    g = np.zeros((600, 800), np.int16)
    g[0, :] = 1    # 最北列
    g[-1, :] = 2   # 最南列
    out, b = pub.reproject_lonlat_to_3857(g, pub.lonlat_edges(p_him))
    assert out.shape[1] == 800 and 740 <= out.shape[0] <= 770
    assert b == [100.0, 0.0, 180.0, 60.0]
    assert (out[0] == 1).all() and (out[-1] == 2).all(), "row 0＝北"
    # Mercator：高緯列被拉長 → 北半部（30–60N）佔的列數多於南半部
    rows = np.repeat(np.arange(600, dtype=np.int16)[:, None], 800, axis=1)
    src, _ = pub.reproject_lonlat_to_3857(rows, pub.lonlat_edges(p_him))
    north = int((src[:, 0] < 300).sum())
    assert north / src.shape[0] == pytest.approx(0.768 / 1.317, abs=0.01)
    # point 註冊外擴半格；寬上限 2048
    ice = {"bounds_lonlat": [115.0, 35.0, 165.0, 65.0], "registration": "point", "step_deg": 0.05}
    assert pub.lonlat_edges(ice) == [114.975, 34.975, 165.025, 65.025]
    wide, _ = pub.reproject_lonlat_to_3857(np.zeros((10, 5000), np.uint8), [0, 0, 50, 1])
    assert wide.shape[1] == 2048


def test_render_tile_products_sizes_and_bounds():
    gr = sp.georef(7, 104, 44, 4096, 3072)
    r = pub.render("radar", np.zeros((3072, 4096), np.uint8), bounds_lonlat=gr["bounds_lonlat"])
    assert (r["width"], r["height"], r["encoding"]) == (2048, 1536, "level_la_v1")
    assert r["bounds"] == pytest.approx(gr["bounds_lonlat"])
    r = pub.render("snow_depth", np.zeros((1536, 2048), np.uint8), bounds_lonlat=gr["bounds_lonlat"])
    assert (r["width"], r["height"]) == (2048, 1536)


def test_determinism_same_input_same_bytes():
    rng = np.random.default_rng(0)
    g = rng.integers(0, 9, (1536, 2048), dtype=np.uint8)
    g[:100, :100] = MISSING
    a = pub.render("snow_depth", g, bounds_lonlat=[1, 2, 3, 4])["png"]
    b = pub.render("snow_depth", g.copy(), bounds_lonlat=[1, 2, 3, 4])["png"]
    assert a == b
    assert b"tIME" not in a and b"tEXt" not in a and b"iTXt" not in a


def test_r2_key_uses_utc_valid_time():
    from datetime import timedelta
    jst0 = datetime(2026, 10, 14, 0, 0, tzinfo=timezone(timedelta(hours=9)))
    assert pub.r2_key("sst_him", jst0) == "imagery/jma/sst_him/20261013/150000.png"


# ───────────────────────── publish（副作用層）─────────────────────────

def test_publish_success_failure_and_disabled(monkeypatch):
    vt = datetime(2026, 10, 14, 1, tzinfo=timezone.utc)
    g = np.zeros((1536, 2048), np.uint8); g[5, 5] = 3
    r2 = FakeR2()
    img = pub.publish("snow_depth", vt, g, bounds_lonlat=[1, 2, 3, 4], r2=r2)
    assert img["image_key"] == "imagery/jma/snow_depth/20261014/010000.png"
    assert img["image_bytes"] == len(r2.objects[img["image_key"]]) and img["encoding"] == "level_la_v1"
    assert (img["lon_min"], img["lat_min"], img["lon_max"], img["lat_max"]) == (1.0, 2.0, 3.0, 4.0)
    assert img["published_at"] and all(type(v) in (int, float, str) for v in img.values())
    assert pub.publish("snow_depth", vt, g, bounds_lonlat=[1, 2, 3, 4], r2=FakeR2(fail=True)) is None
    assert pub.publish("risk_flood", vt, g, bounds_lonlat=[1, 2, 3, 4], r2=r2) is None
    monkeypatch.setattr(pub, "_r2_cache", {"r2": None})
    assert pub.publish("snow_depth", vt, g, bounds_lonlat=[1, 2, 3, 4]) is None, "R2 未設定 → 略過"
    monkeypatch.setattr("config.JAPAN_JMA_IMAGERY_PUBLISH", False)
    assert pub.publish("snow_depth", vt, g, bounds_lonlat=[1, 2, 3, 4], r2=r2) is None


# ───────────────────────── collector hook ─────────────────────────

def test_jma_raster_publishes_each_frame_and_r2_failure_keeps_t2(tmp_path, monkeypatch):
    from tests.test_jma_raster import FakeS3, make_collector, png_blank, png_p, snow_tt
    from collectors.global_climate import jma_raster as jr
    latest = "20261009160000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch", lambda s, path, **k: snow_tt(latest))
    idx = np.zeros((256, 256), np.uint8); idx[10, 10:20] = 3
    content = png_p(idx)
    get = lambda url: (200, content) if url.endswith("/56/24.png") else (200, png_blank())  # noqa: E731
    now = datetime(2026, 10, 9, 16, 5, tzinfo=timezone.utc)

    s3, r2 = FakeS3(), FakeR2()
    c = make_collector(tmp_path / "a", ["snow_depth"], s3, get)
    c._r2 = r2
    c.collect_product(jr.PRODUCTS["snow_depth"], now, budget_frames=10)
    st = c._load_state("snow_depth")
    assert len(r2.objects) == 4
    f = st["pending_frames"]["20261009130000"]
    assert f["image_key"] == "imagery/jma/snow_depth/20261009/130000.png" and f["image_width"] == 2048
    L, A = pub.decode_png_la(r2.objects[f["image_key"]])
    assert int((A == 255).sum()) == f["nonzero_pixels"] == 10

    # R2 故障：T2/T3 照常，frame 列無 image 欄
    s3b = FakeS3()
    c2 = make_collector(tmp_path / "b", ["snow_depth"], s3b, get)
    c2._r2 = FakeR2(fail=True)
    res = c2.collect_product(jr.PRODUCTS["snow_depth"], now, budget_frames=10)
    assert res["days_closed"] == ["20261009"]
    assert "weather-grid/jma/snow_depth/20261009/22.npz" in s3b.objects
    assert all("image_key" not in fr for fr in c2._load_state("snow_depth")["pending_frames"].values())


def test_jma_ocean_env_publishes_sst_and_uv(tmp_path):
    from tests.test_jma_ocean_env import FakeS3, him_text, make_collector
    from collectors.global_climate import jma_ocean_env as oe
    now = datetime(2026, 10, 12, 3, tzinfo=timezone.utc)

    def get(url):
        day = url[-12:-4]
        if day == "20261011":
            return 200, him_text(day), {}
        return 404, b"", {}
    c = make_collector(tmp_path, ["sst_him"], FakeS3(), get, missing_after=30)
    c._r2 = FakeR2()
    c.fetch_grid_product(oe.GRID_PRODUCTS["sst_him"], now)
    f = c._load_state("sst_him")["pending_frames"]["20261011"]
    assert f["image_key"] == "imagery/jma/sst_him/20261010/150000.png" and f["encoding"] == "sst_la_v1"
    assert f["image_width"] == 800 and (f["lon_min"], f["lat_max"]) == (100.0, 60.0)
    L, A = pub.decode_png_la(c._r2.objects[f["image_key"]])
    assert L[0, 0] == 220 and A[0, -1] == 0, "28.4℃ → 220；最東欄 999 陸地透明"


# ───────────────────────── writer ─────────────────────────

def test_writer_image_update_is_separate_and_failure_tolerant(monkeypatch):
    captured = []

    def ev(cur, sql, values, page_size=100, template=None):
        if sql.startswith("UPDATE") and fail["on"]:
            raise RuntimeError("column image_key does not exist")
        captured.append((sql, list(values), template))
    fail = {"on": False}
    monkeypatch.setattr("storage.supabase_writer.execute_values", ev)
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._pool = MagicMock(); w._pool.statement_timeout_ms = 30_000
    conn = MagicMock()
    img = {"image_key": "imagery/jma/radar/20261009/000000.png", "image_bytes": 10, "image_width": 2048,
           "image_height": 1536, "lon_min": 1.0, "lon_max": 2.0, "lat_min": 3.0, "lat_max": 4.0,
           "encoding": "level_la_v1", "published_at": "2026-10-09T00:01:00+00:00"}
    data = [{"_type": "frame", "source": "jma", "product": "radar", "valid_time": "2026-10-09T00:00:00+00:00",
             "status": "ok", **img},
            {"_type": "frame", "source": "jma", "product": "radar", "valid_time": "2026-10-09T00:05:00+00:00",
             "status": "missing"}]
    recs = w._transform_jma_raster({"data": data}, datetime.now(timezone.utc))
    w._write_to_db(conn, "jma_raster", recs, datetime.now(timezone.utc))
    upsert, update = captured
    assert "image_key" not in upsert[0], "狀態列 upsert 不帶 image 欄"
    assert update[0].startswith("UPDATE live.weather_raster_frames") and len(update[1]) == 1
    assert update[1][0][:4] == ("jma", "radar", "2026-10-09T00:00:00+00:00", img["image_key"])
    assert "::timestamptz" in update[2] and "::smallint" in update[2]
    # image UPDATE 失敗不 raise（frames 狀態已寫）
    captured.clear(); fail["on"] = True
    w._write_to_db(conn, "jma_raster", recs, datetime.now(timezone.utc))
    assert len(captured) == 1
