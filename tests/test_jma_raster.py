"""jma_raster（JMA 網格圖磚 G 型）單元測試。

不打網路、不打 DB、不碰真 S3：圖磚用 Pillow 在測試內生成（JMA 4-bit palette 版面），
targetTimes／HTTP／S3 全部以假物件替換。
"""
from __future__ import annotations

import io
import json
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
from PIL import Image

from collectors.global_climate import jma_raster as jr
from collectors.weather_raster import spool as sp
from collectors.weather_raster.decode import MISSING, DecodeTable, decode_tile, pool_max
from storage.supabase_writer import SupabaseWriter

RADAR_PLTE = [(255, 255, 255), (255, 255, 255), (242, 242, 255), (160, 210, 255), (33, 140, 255),
              (0, 65, 255), (250, 245, 0), (255, 153, 0), (255, 40, 0), (180, 0, 104)]
TRNS = bytes([0, 0] + [255] * 8)


def png_p(indices: np.ndarray, plte=RADAR_PLTE, trns=TRNS) -> bytes:
    im = Image.fromarray(indices.astype(np.uint8), "P")
    im.putpalette([c for rgb in plte for c in rgb])
    bio = io.BytesIO()
    im.save(bio, format="PNG", transparency=trns)
    return bio.getvalue()


def png_blank() -> bytes:
    bio = io.BytesIO()
    Image.new("RGBA", (256, 256), (0, 0, 0, 0)).save(bio, format="PNG")
    return bio.getvalue()


@pytest.fixture(scope="module")
def radar_table():
    return DecodeTable.load("jma", "radar")


# ───────────────────────── 解碼 ─────────────────────────

def test_decode_tables_load_and_versions():
    for name in ("radar", "rasrf", "risk_land", "risk_inund", "snow_depth"):
        t = DecodeTable.load("jma", name)
        assert t.version and t.thresholds and all(1 <= v <= 254 for v in t.levels.values())
    assert DecodeTable.load("jma", "radar").plte_verified is True
    assert DecodeTable.load("jma", "risk_land").plte_verified is False  # 暫定版


def test_decode_palette_levels_and_unknown_color(radar_table):
    idx = np.zeros((256, 256), np.uint8)
    idx[0, :10] = 2      # <1 mm/h → 1
    idx[1, :5] = 9       # ≥80 → 8
    idx[2, :3] = 10      # 表外顏色
    plte = RADAR_PLTE + [(1, 2, 3)]
    td = decode_tile(png_p(idx, plte, TRNS + b"\xff"), radar_table)
    assert (td.levels[0, :10] == 1).all() and (td.levels[1, :5] == 8).all()
    assert (td.levels[2, :3] == MISSING).all(), "表外顏色不可歸 0"
    assert td.unknown_pixels == 3 and td.unknown_colors == {"#010203": 3}
    assert td.nonzero == 15 and not td.is_blank


def test_decode_blank_placeholder_and_small_content_tile(radar_table):
    assert decode_tile(png_blank(), radar_table).is_blank
    idx = np.zeros((256, 256), np.uint8)
    idx[100, 100] = 3
    data = png_p(idx)
    assert len(data) < 334  # 有內容但 bytes 比佔位小：不能只看 bytes 判空
    td = decode_tile(data, radar_table)
    assert not td.is_blank and td.levels[100, 100] == 2


def test_decode_rgb_tolerance_for_provisional_table():
    t = DecodeTable.load("jma", "snow_depth")           # legend 黃 255,245,0；tolerance 8
    idx = np.zeros((256, 256), np.uint8)
    idx[0, 0] = 6                                        # PLTE 250,245,0
    td = decode_tile(png_p(idx), t)
    assert td.levels[0, 0] == 4 and td.unknown_pixels == 0


def test_pool_max_keeps_missing_only_when_all_missing():
    g = np.array([[1, 255, 255, 255], [3, 0, 255, 255]], np.uint8)
    assert pool_max(g, 2).tolist() == [[3, 255]]


# ───────────────────────── 時刻 ─────────────────────────

def test_latest_analysis_filters():
    rasrf = [
        {"basetime": "20261009205000", "validtime": "20261009205000", "member": "immed", "elements": ["rasrf"]},
        {"basetime": "20261009205000", "validtime": "20261009205000", "member": "none", "elements": ["sjfcstmap"]},
        {"basetime": "20261009203000", "validtime": "20261009203000", "member": "none", "elements": ["rasrf"]},
        {"basetime": "20261009200000", "validtime": "20261010110000", "member": "none", "elements": ["rasrf"]},
    ]
    assert jr.latest_analysis(jr.PRODUCTS["rasrf"], rasrf) == "20261009203000"
    risk = [{"basetime": "20261009210000", "validtime": "20261009210000", "member": "immed0"},
            {"basetime": "20261009203000", "validtime": "20261009203000", "member": "none"}]
    assert jr.latest_analysis(jr.PRODUCTS["risk_land"], risk) == "20261009203000"


def test_expected_vts_on_cadence_grid():
    p = jr.PRODUCTS["rasrf"]
    s = datetime(2026, 10, 9, 10, 1, tzinfo=timezone.utc)
    e = datetime(2026, 10, 9, 11, 30, tzinfo=timezone.utc)
    assert jr.expected_vts(p, s, e) == ["20261009103000", "20261009110000", "20261009113000"]
    assert jr.jst_date("20261009150000") == "20261010"


# ───────────────────────── spool：累加器、T2、T3 ─────────────────────────

def test_accumulator_idempotent_and_rebuild_after_crash(tmp_path):
    shape = (4, 6)
    g1 = np.zeros(shape, np.uint8); g1[0, 0] = 4; g1[1, 1] = 2; g1[3, 5] = MISSING
    g2 = np.zeros(shape, np.uint8); g2[0, 0] = 1; g2[3, 5] = MISSING
    gdir = tmp_path / "grids"
    p1, p2 = gdir / "a.npz", gdir / "b.npz"
    sp.save_grid(p1, g1); sp.save_grid(p2, g2)
    acc = sp.DayAccumulator(tmp_path / "acc", shape, [1, 2, 4])
    assert acc.apply("a", g1) and acc.apply("b", g2)
    assert not acc.apply("a", g1), "同一幀不可重複累加"
    # 重啟：重新開啟仍記得 applied
    acc2 = sp.DayAccumulator(tmp_path / "acc", shape, [1, 2, 4])
    assert acc2.applied == {"a", "b"} and not acc2.needs_rebuild()
    # 模擬累加途中崩潰：標記殘留 → 需重建，重建後結果與正常一致
    (tmp_path / "acc" / "applying").write_text("c")
    acc3 = sp.DayAccumulator(tmp_path / "acc", shape, [1, 2, 4])
    assert acc3.needs_rebuild()
    acc3.rebuild({"a": p1, "b": p2})
    out = tmp_path / "t1.npz"
    acc3.write_summary(out, valid_frames=2, meta={"x": 1})
    z = np.load(out)
    assert z["count_ge_1"][0, 0] == 2 and z["count_ge_4"][0, 0] == 1 and z["count_ge_2"][1, 1] == 1
    assert z["max_level"][0, 0] == 4 and z["max_level"][3, 5] == MISSING  # 全日缺測 ≠ 0
    assert z["missing_count"][3, 5] == 2 and int(z["valid_frames"]) == 2
    assert json.loads(str(z["meta"]))["x"] == 1


def test_stack_npz_and_day_tar_deterministic(tmp_path):
    shape = (4, 4)
    grids = []
    for i, vt in enumerate(["20261009000000", "20261009000500"]):
        p = tmp_path / f"{vt}.npz"
        sp.save_grid(p, np.full(shape, i, np.uint8))
        grids.append((vt, p))
    sp.write_stack_npz(tmp_path / "h.npz", grids, shape, {"m": 1})
    z = np.load(tmp_path / "h.npz")
    assert z["stack"].shape == (2, 4, 4) and z["stack"][1].max() == 1
    assert list(z["valid_times"]) == ["20261009000000", "20261009000500"]

    ft = tmp_path / "frames"
    sp.write_frame_tar(ft / "1.tar", [("radar/1/z6/52/22.png", b"abc")], 100)
    sp.write_frame_tar(ft / "2.tar", [("radar/2/z6/52/22.png", b"de")], 200)
    a = sp.pack_day_tar(tmp_path / "a.tar", [ft / "2.tar", ft / "1.tar"], {"product": "radar"}, 5)
    b = sp.pack_day_tar(tmp_path / "b.tar", [ft / "1.tar", ft / "2.tar"], {"product": "radar"}, 5)
    assert a["sha256"] == b["sha256"] and a["files"] == 2, "重打包必須 bytes 相同（重試靠 sha 判已上傳）"
    with tarfile.open(tmp_path / "a.tar") as tf:
        names = tf.getnames()
        man = json.loads(tf.extractfile("manifest.json").read())
    assert names[-1] == "manifest.json" and man["files"][0]["sha256"]


# ───────────────────────── collector 端到端（假 HTTP／假 S3）─────────────────────────

class FakeS3:
    def __init__(self, fail_keys=()):
        self.objects: dict[str, dict] = {}
        self.uploads: list[tuple[str, str]] = []
        self.fail_keys = set(fail_keys)

    def head(self, key):
        return self.objects.get(key)

    def upload_path(self, local, key, *, storage_class, metadata, content_type):
        self.uploads.append((key, storage_class))
        if any(key.startswith(f) for f in self.fail_keys):
            return False
        self.objects[key] = {"ContentLength": Path(local).stat().st_size,
                             "Metadata": {k: str(v) for k, v in metadata.items()},
                             "StorageClass": storage_class}
        return True


def make_collector(tmp_path, products, s3, get, lookback_min=180):
    c = jr.JmaRasterCollector.__new__(jr.JmaRasterCollector)
    c.name = "jma_raster"
    c.interval_minutes = 60
    c._tables, c._s3, c._pending_commit = {}, s3, None
    import threading
    c._lock, c._requests = threading.Lock(), 0
    c.spool_root = tmp_path / "weather_raw" / "jma"
    c.state_dir = tmp_path / "state" / "jma_raster"
    c.products = [jr.PRODUCTS[p] for p in products]
    c.max_fine_tiles, c.concurrency = 15, 2
    c.initial_lookback = timedelta(minutes=lookback_min)
    c._session = None
    c._get = get
    return c


def snow_tt(latest):
    return [{"basetime": latest, "validtime": latest, "elements": ["snowd"]}]


def test_collect_and_close_day_then_retry_without_reupload(tmp_path, monkeypatch):
    latest = "20261009160000"  # = 2026-10-10 01:00 JST
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch", lambda s, path, **k: snow_tt(latest))
    idx = np.zeros((256, 256), np.uint8); idx[10, 10:20] = 3
    content = png_p(idx)
    calls = []

    def get(url):
        calls.append(url)
        return (200, content) if url.endswith("/56/24.png") else (200, png_blank())

    s3 = FakeS3()
    c = make_collector(tmp_path, ["snow_depth"], s3, get)
    now = datetime(2026, 10, 9, 16, 5, tzinfo=timezone.utc)
    res = c.collect_product(jr.PRODUCTS["snow_depth"], now, budget_frames=10)
    assert res["fetched"] == 4 and res["days_closed"] == ["20261009"]
    assert len(calls) == 4 * 48, "snow 只抓 z6 全幅，不下鑽"
    keys = {k: sc for k, sc in s3.uploads}
    raw = "weather-raw/jma/snow_depth/2026/10/20261009.tar"
    assert keys[raw] == "DEEP_ARCHIVE"
    assert keys["weather-daily/jma/snow_depth/2026/20261009.npz"] == "STANDARD"
    assert keys["weather-grid/jma/snow_depth/20261009/22.npz"] == "STANDARD"
    assert not (c.spool_root / "snow_depth" / "20261009").exists(), "驗證成功後 spool 刪除"
    receipt = json.loads((c.spool_root / "_receipts" / "snow_depth" / "20261009.json").read_text())
    assert receipt["status"] == "verified" and receipt["raw_sha256"] == s3.objects[raw]["Metadata"]["sha256"]
    st = c._load_state("snow_depth")
    assert set(st["pending_frames"]) == {"20261009130000", "20261009140000", "20261009150000", "20261009160000"}
    assert st["pending_daily"]["20261009"]["status"] == "verified"
    f = st["pending_frames"]["20261009130000"]
    assert f["status"] == "ok" and f["tiles_requested"] == 48 and f["nonzero_pixels"] == 10
    # 再跑一輪：已關的日不重抓、不重傳
    n_up = len(s3.uploads)
    res2 = c.collect_product(jr.PRODUCTS["snow_depth"], now, budget_frames=10)
    assert res2["fetched"] == 0 and len(s3.uploads) == n_up


def test_close_day_failure_keeps_spool_and_retry_skips_existing(tmp_path, monkeypatch):
    latest = "20261009160000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch", lambda s, path, **k: snow_tt(latest))
    s3 = FakeS3(fail_keys=["weather-raw/"])
    c = make_collector(tmp_path, ["snow_depth"], s3, lambda url: (200, png_blank()))
    now = datetime(2026, 10, 9, 16, 5, tzinfo=timezone.utc)
    res = c.collect_product(jr.PRODUCTS["snow_depth"], now, 10)
    assert res["days_failed"] == ["20261009"]
    assert (c.spool_root / "snow_depth" / "20261009" / "frames").exists()
    assert c._load_state("snow_depth")["pending_daily"]["20261009"]["status"] == "failed"
    # 修好後重試：T1 已驗證過不重傳；T3 若已在 S3（sha 相符）也不重傳
    s3.fail_keys = set()
    t1_uploads = sum(1 for k, _ in s3.uploads if k.startswith("weather-daily/"))
    res2 = c.collect_product(jr.PRODUCTS["snow_depth"], now, 10)
    assert res2["days_closed"] == ["20261009"]
    assert sum(1 for k, _ in s3.uploads if k.startswith("weather-daily/")) == t1_uploads
    n = len(s3.uploads)
    raw = "weather-raw/jma/snow_depth/2026/10/20261009.tar"
    assert c._put_verified  # head 判斷：同 sha 不再 PUT
    p = tmp_path / "x.tar"; p.write_bytes(b"x")
    s3.objects["k"] = {"ContentLength": 1, "Metadata": {"sha256": "abc"}, "StorageClass": "DEEP_ARCHIVE"}
    assert c._put_verified(p, "k", "abc", "DEEP_ARCHIVE") and len(s3.uploads) == n
    assert s3.objects[raw]["StorageClass"] == "DEEP_ARCHIVE"


def test_missing_finalized_only_after_retention(tmp_path, monkeypatch):
    latest = "20261009160000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch", lambda s, path, **k: snow_tt(latest))
    c = make_collector(tmp_path, ["snow_depth"], None, lambda url: (404, b""))
    now = datetime(2026, 10, 9, 16, 5, tzinfo=timezone.utc)
    res = c.collect_product(jr.PRODUCTS["snow_depth"], now, 10)
    assert res["fetched"] == 0 and res["unavailable"] == 4 and res["missing_finalized"] == 0
    assert c._load_state("snow_depth")["pending_frames"] == {}
    # 26 小時後：這 4 幀已超出上游 24h 保留期 → 定案 missing
    later = "20261010180000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch", lambda s, path, **k: snow_tt(later))
    res2 = c.collect_product(jr.PRODUCTS["snow_depth"], now + timedelta(hours=26), 3)
    st = c._load_state("snow_depth")
    for vt in ("20261009130000", "20261009140000", "20261009150000", "20261009160000"):
        assert st["pending_frames"][vt]["status"] == "missing"
    assert res2["missing_finalized"] >= 4


def test_radar_descends_top15_within_budget(tmp_path, monkeypatch):
    latest = "20261009000000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch",
                        lambda s, path, **k: [{"basetime": latest, "validtime": latest}])
    idx = np.zeros((256, 256), np.uint8); idx[:4, :] = 4
    rich = png_p(idx)
    calls = []

    def get(url):
        calls.append(url)
        return 200, (rich if "/6/" in url or "/8/" in url else png_blank())

    c = make_collector(tmp_path, ["radar"], None, get, lookback_min=0)
    res = c.collect_product(jr.PRODUCTS["radar"], datetime(2026, 10, 9, 0, 5, tzinfo=timezone.utc), 1)
    assert res["fetched"] == 1 and res["requests"] == 48 + 15 * 16 == 288
    st = c._load_state("radar")
    ds = c._day_state(jr.PRODUCTS["radar"], "20261009")
    fr = ds["frames"][latest]
    assert len(fr["fine_tiles"]) == 15 and len(fr["fine_skipped"]) == 33 and fr["zoom_raw"] == 8
    g = sp.load_grid(c.day_dir(jr.PRODUCTS["radar"], "20261009") / "grids" / f"{latest}.npz")
    assert g.shape == (3072, 4096) and g.max() == 3
    assert st["pending_frames"][latest]["tiles_requested"] == 288
    with tarfile.open(c.day_dir(jr.PRODUCTS["radar"], "20261009") / "frames" / f"{latest}.tar") as tf:
        assert sum(1 for n in tf.getnames() if "/z8/" in n) == 240


def test_himawari_raw_only_z5(tmp_path, monkeypatch):
    latest = "20261009000000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch",
                        lambda s, path, **k: [{"basetime": latest, "validtime": latest}])
    calls = []
    c = make_collector(tmp_path, ["himawari_b13"], None,
                       lambda url: (calls.append(url) or (200, b"\xff\xd8jpg")), lookback_min=0)
    res = c.collect_product(jr.PRODUCTS["himawari_b13"], datetime(2026, 10, 9, 0, 5, tzinfo=timezone.utc), 1)
    assert res["fetched"] == 1 and len(calls) == 12 and all("/5/" in u for u in calls)
    fr = c._load_state("himawari_b13")["pending_frames"][latest]
    assert fr["nonzero_pixels"] is None and fr["raw_bytes"] == 12 * 5


def test_run_commits_pending_only_after_db_success(tmp_path, monkeypatch):
    latest = "20261009000000"
    monkeypatch.setattr("collectors.global_climate.jma_common.fetch", lambda s, path, **k: snow_tt(latest))
    c = make_collector(tmp_path, ["snow_depth"], None, lambda url: (200, png_blank()), lookback_min=0)
    c.storage = MagicMock()
    c.should_persist_local = lambda: False
    c.run_count = c.error_count = c.consecutive_errors = 0
    c.last_run = c.last_success_at = None
    monkeypatch.setattr("collectors.base.notify_error", lambda *a, **k: None)
    monkeypatch.setattr("collectors.base.notify_success", lambda *a, **k: None)
    c.supabase_writer = MagicMock()
    c.supabase_writer.write.return_value = False
    assert c.run().get("error")
    assert c._load_state("snow_depth")["pending_frames"], "DB 失敗不可清 pending"
    c.supabase_writer.write.return_value = True
    out = c.run()
    assert not out.get("error") and c._load_state("snow_depth")["pending_frames"] == {}


# ───────────────────────── writer ─────────────────────────

def test_writer_frames_status_guard_and_daily_upsert(monkeypatch):
    captured = []
    monkeypatch.setattr("storage.supabase_writer.execute_values",
                        lambda cur, sql, values, page_size=100: captured.append((sql, list(values))))
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._pool = MagicMock(); w._pool.statement_timeout_ms = 30_000
    conn = MagicMock()
    data = [
        {"_type": "frame", "source": "jma", "product": "radar", "valid_time": "2026-10-09T00:00:00+00:00",
         "status": "ok", "tiles_requested": 48},
        {"_type": "frame", "source": "jma", "product": "radar", "valid_time": None, "status": "ok"},
        {"_type": "daily", "source": "jma", "product": "radar", "obs_date": "2026-10-09", "status": "verified"},
    ]
    recs = w._transform_jma_raster({"data": data}, datetime.now(timezone.utc))
    assert len(recs) == 2
    w._write_to_db(conn, "jma_raster", recs, datetime.now(timezone.utc))
    sqls = {s.split()[2]: s for s, _ in captured}
    assert set(sqls) == {"live.weather_raster_frames", "live.weather_raster_daily"}
    assert "WHERE CASE live.weather_raster_frames.status" in sqls["live.weather_raster_frames"]
    assert "ON CONFLICT (source, product, obs_date) DO UPDATE" in sqls["live.weather_raster_daily"]
