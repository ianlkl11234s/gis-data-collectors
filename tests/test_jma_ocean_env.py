"""jma_ocean_env（C8 海溫／海冰／UV／黃砂）單元測試。

不打網路、不打 DB、不碰真 S3：ASCII 網格與 PNG 在測試內生成，HTTP／S3 以假物件替換。
"""
from __future__ import annotations

import gzip
import io
import json
import tarfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest
from PIL import Image

from collectors.global_climate import jma_ocean_env as oe
from collectors.weather_raster import spool as sp
from collectors.weather_raster.decode import MISSING, DecodeTable, decode_tile
from storage.supabase_writer import SupabaseWriter

UV_PLTE = [(255, 255, 255), (255, 255, 255), (217, 217, 255), (153, 203, 255), (255, 255, 190), (250, 250, 150),
           (250, 245, 0), (255, 200, 0), (255, 140, 0), (250, 90, 0), (255, 20, 0), (165, 0, 33), (181, 0, 91),
           (204, 0, 160), (204, 0, 204)]
UV_TRNS = bytes([0] + [204] * 14)


def png_p(idx: np.ndarray, plte=UV_PLTE, trns=UV_TRNS) -> bytes:
    im = Image.fromarray(idx.astype(np.uint8), "P")
    im.putpalette([c for rgb in plte for c in rgb])
    bio = io.BytesIO()
    im.save(bio, format="PNG", transparency=trns)
    return bio.getvalue()


SMALL_SST = oe.GridProduct("t_sst", "x", "t_sst{d}.txt", "sst", 2, 4, 3, 100.0, 60.0, 0.1, "area")
SMALL_ICE = oe.GridProduct("t_ice", "x", "t_ice{d}.txt", "ice", 2, 4, 1, 115.0, 65.0, 0.05, "point")


# ───────────────────────── ASCII 網格解析 ─────────────────────────

def test_parse_sst_codes_negative_and_unparsable():
    text = "2026  10  11\n284 -5888999\n***401 0 12 \n"
    hdr, g, st = oe.parse_grid_text(SMALL_SST, text)
    assert hdr == "20261011" and g.dtype == np.int16 and g.shape == (2, 4)
    assert g[0].tolist() == [284, -5, oe.SST_ICE, oe.SST_LAND]
    assert g[1, 0] == oe.SST_BAD and g[1, 1] == oe.SST_BAD, "無法解析／超出物理範圍 → 缺測碼，不可為 0"
    assert g[1, 2] == 0 and g[1, 3] == 12, "真的 0.0℃ 仍是 0"
    assert st["unknown"] == 2 and st["ice"] == 1 and st["land"] == 1 and st["valid"] == 4


def test_parse_sst_wrong_shape_raises():
    with pytest.raises(ValueError):
        oe.parse_grid_text(SMALL_SST, "2026  10  11\n284 -5888\n284 -5888999\n")
    with pytest.raises(ValueError):
        oe.parse_grid_text(SMALL_SST, "2026  10  11\n284 -5888999\n")


def test_parse_ice_codes_land_and_no_analysis_not_zero():
    hdr, g, st = oe.parse_grid_text(SMALL_ICE, "JMA_SEA_ICE_ANALYSIS20260215\n0257\n349x\n")
    assert hdr == "20260215" and g.dtype == np.uint8
    assert g[0].tolist() == [0, 2, 5, oe.ICE_NODATA]
    assert g[1].tolist() == [3, 4, oe.ICE_LAND, MISSING]
    assert st["unknown"] == 1 and st["no_analysis"] == 1 and st["land"] == 1 and st["ice_any"] == 4


def test_lonlat_georef_area_vs_point():
    a = oe.lonlat_georef(oe.GRID_PRODUCTS["sst_him"])
    assert a["bounds_lonlat"] == [100.0, 0.0, 180.0, 60.0] and a["first_value_lonlat"] == [100.05, 59.95]
    p = oe.lonlat_georef(oe.GRID_PRODUCTS["sea_ice"])
    assert p["bounds_lonlat"] == [115.0, 35.0, 165.0, 65.0] and p["first_value_lonlat"] == [115.0, 65.0]
    g = oe.lonlat_georef(oe.GRID_PRODUCTS["sst_mgd"])
    assert g["width"] == 1440 and g["height"] == 720 and g["first_value_lonlat"] == [0.125, 89.875]


# ───────────────────────── UV 解碼 ─────────────────────────

def test_uv_table_levels_and_unknown_color():
    t = DecodeTable.load("jma", "uv_anal")
    assert t.plte_verified and len(t.levels) == 14 and t.thresholds == [4, 7, 9, 12]
    idx = np.zeros((256, 256), np.uint8)
    idx[0, :4] = 1      # UVI 0 → level 1（不可與透明 0 混淆）
    idx[1, :3] = 14     # UVI ≥13 → 14
    idx[2, :2] = 15     # 表外顏色
    td = decode_tile(png_p(idx, UV_PLTE + [(1, 2, 3)], UV_TRNS + b"\xcc"), t)
    assert (td.levels[0, :4] == 1).all() and (td.levels[1, :3] == 14).all()
    assert (td.levels[2, :2] == MISSING).all() and td.unknown_pixels == 2 and td.unknown_colors == {"#010203": 2}
    assert td.levels[3, 0] == 0, "透明＝範圍外 → 0"


def test_uv_expected_vts_only_06_to_18_jst():
    start = datetime(2026, 10, 11, 0, tzinfo=oe.JST)
    vts = oe.uv_expected_vts(start, start + timedelta(days=1) - timedelta(seconds=1))
    assert len(vts) == 13 and vts[0] == "20261010210000" and vts[-1] == "20261011090000"


# ───────────────────────── 假 S3／collector ─────────────────────────

class FakeS3:
    def __init__(self, fail_prefix=()):
        self.objects: dict[str, dict] = {}
        self.files: dict[str, bytes] = {}
        self.uploads: list[tuple[str, str]] = []
        self.fail_prefix = tuple(fail_prefix)

    def head(self, key):
        return self.objects.get(key)

    def upload_path(self, local, key, *, storage_class, metadata, content_type):
        self.uploads.append((key, storage_class))
        if self.fail_prefix and key.startswith(self.fail_prefix):
            return False
        self.files[key] = Path(local).read_bytes()
        self.objects[key] = {"ContentLength": Path(local).stat().st_size,
                             "Metadata": {k: str(v) for k, v in metadata.items()}, "StorageClass": storage_class}
        return True


def make_collector(tmp_path, products, s3, get, seasons=None, lookback_days=3, missing_after=7):
    c = oe.JmaOceanEnvCollector.__new__(oe.JmaOceanEnvCollector)
    c.name = "jma_ocean_env"
    c.interval_minutes = 60
    c._s3, c._pending_commit, c._uv_table = s3, None, None
    c._lock, c._requests = threading.Lock(), 0
    c.spool_root = tmp_path / "weather_raw" / "jma"
    c.state_dir = tmp_path / "state" / "jma_ocean_env"
    c.products = list(products)
    c.seasons = seasons if seasons is not None else {"sea_ice": frozenset({12, 1, 2, 3, 4, 5})}
    c.lookback_days, c.missing_after_days, c.max_days_per_run = lookback_days, missing_after, 4
    c._session = None

    def counted(url):
        c._requests += 1
        return get(url)
    c._get = counted
    return c


def him_text(day: str, val: int = 284) -> bytes:
    p = oe.GRID_PRODUCTS["sst_him"]
    row = (f"{val:3d}" * (p.ncols - 1) + "999")
    return (f"{day[:4]}  {int(day[4:6]):2d}  {int(day[6:]):2d}\n" + "\n".join([row] * p.nrows) + "\n").encode()


def test_grid_product_fetch_archive_missing_and_no_refetch(tmp_path):
    now = datetime(2026, 10, 12, 3, tzinfo=timezone.utc)  # 12:00 JST
    calls = []

    def get(url):
        calls.append(url)
        day = url[-12:-4]
        if day in ("20261010", "20261011"):
            return 200, him_text(day), {"Last-Modified": "Mon, 12 Oct 2026 01:01:37 GMT"}
        return 404, b"", {}
    s3 = FakeS3()
    c = make_collector(tmp_path, ["sst_him"], s3, get, missing_after=2)
    out = c.fetch_grid_product(oe.GRID_PRODUCTS["sst_him"], now)
    # epoch = 10-09；10-09 404 且已 3 天 → missing；10-12（今天）不打
    assert out["fetched"] == 2 and out["missing"] == 1 and out["not_yet"] == 0
    assert not any(u.endswith("20261012.txt") for u in calls)
    t3 = "weather-raw/jma/sst_him/2026/10/20261011.tar.gz"
    assert s3.objects[t3]["StorageClass"] == "DEEP_ARCHIVE"
    assert s3.objects["weather-grid/jma/sst_him/2026/20261011.npz"]["StorageClass"] == "STANDARD"
    tf = tarfile.open(fileobj=io.BytesIO(gzip.decompress(s3.files[t3])))
    assert tf.extractfile("him_sst_pac_D20261011.txt").read() == him_text("20261011"), "T3 保存原檔位元組"
    with np.load(io.BytesIO(s3.files["weather-grid/jma/sst_him/2026/20261011.npz"])) as z:
        g = z["sst_dc"]
        assert g.dtype == np.int16 and g[0, 0] == 284 and g[0, -1] == oe.SST_LAND
    st = c._load_state("sst_him")
    assert st["pending_frames"]["20261009"]["status"] == "missing"
    assert st["pending_daily"]["20261011"]["status"] == "verified"
    assert st["pending_daily"]["20261011"]["raw_key"] == t3
    assert st["finalized_until"] == "20261012"
    # 下一輪：只再試 10-12，不重抓已收的日子
    calls.clear()
    c.fetch_grid_product(oe.GRID_PRODUCTS["sst_him"], now + timedelta(days=1))
    assert [u[-12:-4] for u in calls] == ["20261012"]


def test_grid_upload_failure_keeps_receipt_absent_for_retry(tmp_path):
    now = datetime(2026, 10, 12, 3, tzinfo=timezone.utc)
    s3 = FakeS3(fail_prefix=("weather-raw/",))
    c = make_collector(tmp_path, ["sst_him"], s3, lambda url: (200, him_text(url[-12:-4]), {}), lookback_days=1)
    out = c.fetch_grid_product(oe.GRID_PRODUCTS["sst_him"], now)
    assert out["fetched"] == 0 and out["failed"] >= 1
    assert not c.receipt_path("sst_him", "20261011").exists()
    assert c._load_state("sst_him")["pending_daily"] == {}


def test_header_date_mismatch_still_cold_stored_as_partial(tmp_path):
    now = datetime(2026, 10, 12, 3, tzinfo=timezone.utc)
    s3 = FakeS3()
    c = make_collector(tmp_path, ["sst_him"], s3, lambda url: (200, him_text("20200101"), {}), lookback_days=1)
    c.fetch_grid_product(oe.GRID_PRODUCTS["sst_him"], now)
    st = c._load_state("sst_him")
    assert st["pending_frames"]["20261011"]["status"] == "partial"
    assert "weather-raw/jma/sst_him/2026/10/20261011.tar.gz" in s3.objects
    assert not any(k.startswith("weather-grid/") for k in s3.objects), "解析失敗不產 T2"


def test_sea_ice_season_switch_no_requests_off_season(tmp_path):
    calls = []
    c = make_collector(tmp_path, ["sea_ice"], FakeS3(), lambda url: calls.append(url) or (404, b"", {}))
    out = c.fetch_grid_product(oe.GRID_PRODUCTS["sea_ice"], datetime(2026, 10, 12, 3, tzinfo=timezone.utc))
    assert calls == [] and out["requests"] == 0 and out["missing"] == 0, "季外不打上游、不算缺"
    c2 = make_collector(tmp_path / "b", ["sea_ice"], FakeS3(), lambda url: calls.append(url) or (404, b"", {}))
    c2.fetch_grid_product(oe.GRID_PRODUCTS["sea_ice"], datetime(2026, 12, 2, 3, tzinfo=timezone.utc))
    assert {u[-12:-4] for u in calls} == {"20261201"}, "11/29、11/30 季外；12/1 起才抓（今天 12/2 不打）"


def test_kosa_off_season_no_requests(tmp_path):
    calls = []
    c = make_collector(tmp_path, ["kosa_fcst"], FakeS3(), lambda url: calls.append(url) or (200, b"", {}),
                       seasons={"kosa_fcst": frozenset({2, 3, 4, 5})})
    out = c.collect_kosa(datetime(2026, 10, 12, 3, tzinfo=timezone.utc))
    assert calls == [] and out.get("off_season") is True


def test_season_spec_parsing_reuses_raster_parser():
    seasons = oe.parse_product_map("sea_ice=12,1,2,3,4,5,uv_anal=4,5,6", oe._months)
    assert seasons["sea_ice"] == frozenset({12, 1, 2, 3, 4, 5}) and seasons["uv_anal"] == frozenset({4, 5, 6})


# ───────────────────────── UV 流程 ─────────────────────────

def uv_get_factory(available_vts: set, calls: list):
    idx = np.ones((256, 256), np.uint8) * 6   # UVI 5
    idx[:10, :10] = 0                         # 透明
    tile = png_p(idx)

    def get(url):
        calls.append(url)
        vt = url.split("/uv_a/")[1][:14]
        return (200, tile, {}) if vt in available_vts else (404, b"", {})
    return get


def test_uv_collect_close_day_t1_t2_t3_and_missing(tmp_path):
    day_vts = oe.uv_expected_vts(datetime(2026, 10, 11, tzinfo=oe.JST), datetime(2026, 10, 11, 23, tzinfo=oe.JST))
    available = set(day_vts[1:])               # 06 JST 那幀上游沒有
    calls = []
    s3 = FakeS3()
    c = make_collector(tmp_path, ["uv_anal"], s3, uv_get_factory(available, calls))
    st = c._load_state("uv_anal")
    st["epoch"] = "20261010150000"             # 10-11 00:00 JST
    c._save_state("uv_anal", st)
    now1 = datetime(2026, 10, 11, 10, 30, tzinfo=timezone.utc)   # 19:30 JST
    out = c.collect_uv(now1)
    assert out["fetched"] == 12 and out["unavailable"] == 1 and out["days_closed"] == []
    assert out["requests"] == 12 * 6 + 1
    # 06 JST 幀超出保留期後才定案 missing，當日即可日結
    now2 = datetime(2026, 10, 12, 17, tzinfo=timezone.utc)
    out2 = c.collect_uv(now2)
    assert out2["missing"] == 1 and "20261011" in out2["days_closed"]
    st = c._load_state("uv_anal")
    row = st["pending_daily"]["20261011"]
    assert row["frames_expected"] == 13 and row["frames_ok"] == 12 and row["status"] == "verified"
    assert s3.objects["weather-raw/jma/uv_anal/2026/10/20261011.tar"]["StorageClass"] == "DEEP_ARCHIVE"
    with np.load(io.BytesIO(s3.files["weather-daily/jma/uv_anal/2026/20261011.npz"])) as z:
        assert z["max_level"][356, 612] == 6 and z["count_ge_4"][356, 612] == 12 and z["count_ge_7"][356, 612] == 0
        assert z["max_level"][0, 0] == 0, "範圍外仍為 0"
    t2 = sp.load_hour_npz(io.BytesIO(s3.files["weather-grid/jma/uv_anal/2026/20261011.npz"]))
    assert len(t2) == 13 and t2[0][1] is None and t2[1][1][356, 612] == 6 and t2[1][1][256, 512] == 0
    # 同內容幀：T3 磚級 sha256 去重只存一份
    with tarfile.open(fileobj=io.BytesIO(s3.files["weather-raw/jma/uv_anal/2026/10/20261011.tar"])) as tf:
        man = json.loads(tf.extractfile("manifest.json").read())
    assert man["dedup"]["tiles"] == 72 and man["dedup"]["unique_blobs"] == 1
    assert not c.day_dir("uv_anal", "20261011").exists()


# ───────────────────────── 黃砂 ─────────────────────────

def kosa_list(items):
    lines = ["var imgFcst = new Array();", "var i = 0;"]
    lines += [f'imgFcst[i++] = "{t}";' for t in items["fcst"]]
    lines += ["var imgAnly = new Array();", "var n = 0;"]
    lines += [f'imgAnly[n++] = "{t}";' for t in items["anly"]]
    return ("\n".join(lines) + "\n").encode()


def test_parse_kosa_list():
    items = oe.parse_kosa_list(kosa_list({"fcst": ["202610130300", "202610130000"], "anly": ["202610112100"]})
                               .decode())
    assert items == [("fcst", "202610130300"), ("fcst", "202610130000"), ("anly", "202610112100")]


def test_kosa_sha256_dedup_within_and_across_days(tmp_path):
    state = {"list": kosa_list({"fcst": ["202610130300", "202610130000"], "anly": ["202610112100"]}),
             "lm": "Sun, 11 Oct 2026 21:00:48 GMT"}
    imgs = {"202610130300": b"A" * 100, "202610130000": b"B" * 100, "202610112100": b"A" * 100,
            "202610140300": b"C" * 100}
    calls = []

    def get(url):
        calls.append(url)
        if url.endswith("s_jpjp_list.js"):
            return 200, state["list"], {"Last-Modified": state["lm"]}
        t = url.rsplit("/", 1)[1][:12]
        return 200, imgs[t], {}
    s3 = FakeS3()
    c = make_collector(tmp_path, ["kosa_fcst"], s3, get)
    out = c.collect_kosa(datetime(2026, 10, 12, 0, tzinfo=timezone.utc))
    assert out["issued"] and out["images"] == 3 and out["new_blobs"] == 3
    # list 沒變 → 只打 1 次 list
    calls.clear()
    out = c.collect_kosa(datetime(2026, 10, 12, 1, tzinfo=timezone.utc))
    assert not out["issued"] and len(calls) == 1
    # 隔日新發布：同一輪裡 10-12 的 T3 尚未驗證上傳 → 不可 same_as 指向它（review F080），
    # 202610130300（內容 A 未變）要在新一日自帶一份
    state["list"] = kosa_list({"fcst": ["202610140300", "202610130300"], "anly": []})
    state["lm"] = "Mon, 12 Oct 2026 21:00:48 GMT"
    out = c.collect_kosa(datetime(2026, 10, 12, 21, 5, tzinfo=timezone.utc))
    assert out["new_blobs"] == 2 and out["dedup_refs"] == 0 and "20261012" in out["days_closed"]
    with tarfile.open(fileobj=io.BytesIO(s3.files["weather-raw/jma/kosa_fcst/2026/10/20261012.tar"])) as tf:
        man = json.loads(tf.extractfile("manifest.json").read())
    # 10-12：list.js＋issue.json＋3 張圖，其中 A 重複 → 圖只存 2 份
    assert man["dedup"]["tiles"] == 5 and man["dedup"]["unique_blobs"] == 4
    assert man["decoded"] is False and "why_not_decoded" in man
    c.collect_kosa(datetime(2026, 10, 13, 17, tzinfo=timezone.utc))   # 10-13 日結（list 沒變）
    with tarfile.open(fileobj=io.BytesIO(s3.files["weather-raw/jma/kosa_fcst/2026/10/20261013.tar"])) as tf:
        man = json.loads(tf.extractfile("manifest.json").read())
    assert len(man["blobs"]) == 4, "list.js＋issue.json＋A＋C"
    # 再隔日：10-13 已驗證上傳 → A 才可只記引用
    state["list"] = kosa_list({"fcst": ["202610130300"], "anly": []})
    state["lm"] = "Wed, 14 Oct 2026 21:00:48 GMT"
    out = c.collect_kosa(datetime(2026, 10, 14, 21, 5, tzinfo=timezone.utc))
    assert out["new_blobs"] == 0 and out["dedup_refs"] == 1
    st = c._load_state("kosa_fcst")
    assert {r["status"] for r in st["pending_daily"].values()} == {"verified"}
    assert all(r["product"] == "kosa_fcst" for r in st["pending_frames"].values())


# ───────────────────────── DB 寫入 ─────────────────────────

def test_writer_routes_jma_ocean_env_to_weather_raster_tables(monkeypatch):
    captured = []
    monkeypatch.setattr("storage.supabase_writer.execute_values",
                        lambda cur, sql, values, page_size=100: captured.append((sql, list(values))))
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._pool = MagicMock(); w._pool.statement_timeout_ms = 30_000
    data = [oe.JmaOceanEnvCollector._frame_row("sst_him", datetime(2026, 10, 11, tzinfo=oe.JST), "ok"),
            oe.JmaOceanEnvCollector._daily_row("sst_him", "20261011", status="verified")]
    recs = SupabaseWriter.TRANSFORMERS["jma_ocean_env"](w, {"data": data}, datetime.now(timezone.utc))
    assert len(recs) == 2
    w._write_to_db(MagicMock(), "jma_ocean_env", recs, datetime.now(timezone.utc))
    assert {s.split()[2] for s, _ in captured} == {"live.weather_raster_frames", "live.weather_raster_daily"}


def test_run_commits_pending_only_after_db_success(tmp_path, monkeypatch):
    c = make_collector(tmp_path, ["sst_him"], FakeS3(), lambda url: (404, b"", {}))
    st = c._load_state("sst_him")
    st["pending_frames"]["20261011"] = {"x": 1}
    c._save_state("sst_him", st)

    def fake_run(self):
        self._pending_commit = {"sst_him": {"frames": ["20261011"], "daily": []}}
        return {"error": "db down"}
    monkeypatch.setattr(oe.BaseCollector, "run", fake_run)
    c.run()
    assert "20261011" in c._load_state("sst_him")["pending_frames"]

    def ok_run(self):
        self._pending_commit = {"sst_him": {"frames": ["20261011"], "daily": []}}
        return {}
    monkeypatch.setattr(oe.BaseCollector, "run", ok_run)
    c.run()
    assert c._load_state("sst_him")["pending_frames"] == {}


def test_kosa_list_hash_not_advanced_when_image_fetch_fails(tmp_path):
    """review F079：有圖抓取失敗不推進 last_list_sha，下輪同一份清單會重抓。"""
    lst = kosa_list({"fcst": ["202610130300", "202610130000"], "anly": []})
    fail = {"on": True}

    def get(url):
        if url.endswith("s_jpjp_list.js"):
            return 200, lst, {"Last-Modified": "Sun, 11 Oct 2026 21:00:48 GMT"}
        t = url.rsplit("/", 1)[1][:12]
        if fail["on"] and t == "202610130000":
            return 500, b"", {}
        return 200, t.encode() * 10, {}
    c = make_collector(tmp_path, ["kosa_fcst"], FakeS3(), get)
    out = c.collect_kosa(datetime(2026, 10, 12, 0, tzinfo=timezone.utc))
    assert out["failed"] == 1 and c._load_state("kosa_fcst")["last_list_sha"] is None
    fail["on"] = False
    out = c.collect_kosa(datetime(2026, 10, 12, 1, tzinfo=timezone.utc))
    assert out["issued"] and out["failed"] == 0 and c._load_state("kosa_fcst")["last_list_sha"]
    out = c.collect_kosa(datetime(2026, 10, 12, 2, tzinfo=timezone.utc))
    assert not out["issued"]
