"""日本氣象廳 海洋・環境網格收集器（C8＋附加；ADR-0021 G 型；不需新 migration）

產品（2026-10-12 實測，詳見 docs/JMA_OCEAN_ENV.md）：
  sst_him    him_sst_pac_D  每日 ASCII，0.1°，0–60N／100E–180（台灣在內），3 位數 0.1℃；888 海冰、999 陸地／無法決定
  sst_mgd    mgd_sst_glb_D  每日 ASCII .txt.gz，0.25° 全球，同上編碼
  sea_ice    man_ice_okh_D  每日 ASCII，0.05° 格點（point），35–65N／115–165E；只有 12–5 月（季節開關）
  uv_anal    UV インデックス解析圖磚，每正時 06–18 JST（13 幀／日），上游只留約前一日 06 時起（約 36h）；z5 6 磚
  kosa_fcst  黃砂解析預測圖（單張 800×580 PNG，底圖燒進圖、色票無數值刻度）→ 只冷存，不解碼

分層（與 jma_raster 同一套 key 規約與狀態表）：
  T3 冷  S3 weather-raw/jma/{product}/{YYYY}/{MM}/{YYYYMMDD}.tar[.gz]  DEEP_ARCHIVE，每日每產品 1 個
  T2 溫  S3 weather-grid/jma/{product}/{YYYY}/{YYYYMMDD}.npz           STANDARD（全桶 lifecycle 轉 IA／GIR），永久
  T1 熱  S3 weather-daily/jma/uv_anal/{YYYY}/{YYYYMMDD}.npz            只有 uv_anal（日最大 UVI、達標幀數）
  DB     live.weather_raster_frames／weather_raster_daily（source='jma'，product 如上）— 只放狀態

缺值：SST int16 0.1℃，-32768＝陸地／無法決定（999）、-32767＝海冰（888）、-32766＝無法解析；
      海冰 uint8 0/2/3/4/5＝密接度級，254＝陸地（9），255＝無解析／雲（7）或無法解析；
      UV uint8 0＝範圍外（透明），level＝UVI＋1，255＝缺測／表外顏色。皆不當 0。
"""

from __future__ import annotations

import gzip
import io
import json
import re
import shutil
import tarfile
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import numpy as np

import config
from collectors.base import BaseCollector
from collectors.global_climate.jma_common import new_jma_session
from collectors.global_climate.jma_raster import _months, parse_product_map
from collectors.weather_raster import publish as pub
from collectors.weather_raster import spool as sp
from collectors.weather_raster.decode import MISSING, DecodeTable, decode_tile

SOURCE = "jma"
JST = timezone(timedelta(hours=9))
VT_FMT = "%Y%m%d%H%M%S"
DATA_BASE = "https://www.data.jma.go.jp"
GOOS_BASE = f"{DATA_BASE}/goos/data/pub/JMA-product"
UV_BASE = f"{DATA_BASE}/env/uvindex"
KOSA_BASE = f"{DATA_BASE}/env/kosa/fcst"
LICENSE = "気象庁ホームページ（PDL1.0）"
LICENSE_DERIVED = "気象庁ホームページ（PDL1.0）を加工"

SST_LAND, SST_ICE, SST_BAD = -32768, -32767, -32766
ICE_LAND, ICE_NODATA = 254, MISSING
ICE_CLASSES = {0: "0/10（開水面）", 2: "1–3/10", 3: "4–6/10", 4: "7–8/10", 5: "9–10/10"}
GRID_PARSER_VERSION = "jma-goos-ascii-v1"

ALL_PRODUCTS = ("sst_him", "sst_mgd", "sea_ice", "uv_anal", "kosa_fcst")


# ───────────────────────── 每日 ASCII 網格 ─────────────────────────

@dataclass(frozen=True)
class GridProduct:
    name: str
    folder: str               # GOOS 目錄
    fname: str                # 檔名樣板 {d}=YYYYMMDD
    kind: str                 # sst / ice
    nrows: int
    ncols: int
    width: int                # 每值字元數
    west: float
    north: float
    step: float
    registration: str         # area（格心）/ point（格點）
    gz: bool = False
    season_months: Optional[frozenset] = None

    def url(self, d: str) -> str:
        return f"{GOOS_BASE}/{self.folder}/{d[:4]}/" + self.fname.format(d=d)


GRID_PRODUCTS: dict[str, GridProduct] = {p.name: p for p in (
    GridProduct("sst_him", "him_sst_pac_D", "him_sst_pac_D{d}.txt", "sst", 600, 800, 3, 100.0, 60.0, 0.1, "area"),
    GridProduct("sst_mgd", "mgd_sst_glb_D", "mgd_sst_glb_D{d}.txt.gz", "sst", 720, 1440, 3, 0.0, 90.0, 0.25, "area",
                gz=True),
    GridProduct("sea_ice", "man_ice_okh_D", "man_ice_okh_D{d}.txt", "ice", 601, 1001, 1, 115.0, 65.0, 0.05, "point"),
)}


def lonlat_georef(p: GridProduct) -> dict:
    """EPSG:4326 規則網格。row 0＝最北、col 0＝最西。area：值代表格心（格邊界＝bounds）；point：值在格點上。"""
    if p.registration == "area":
        bounds = [p.west, p.north - p.nrows * p.step, p.west + p.ncols * p.step, p.north]
        first = [p.west + p.step / 2, p.north - p.step / 2]
    else:
        bounds = [p.west, p.north - (p.nrows - 1) * p.step, p.west + (p.ncols - 1) * p.step, p.north]
        first = list(bounds[::3])  # [west, north]
    return {"crs": "EPSG:4326", "registration": p.registration, "step_deg": p.step,
            "width": p.ncols, "height": p.nrows, "first_value_lonlat": [round(v, 6) for v in first],
            "bounds_lonlat": [round(v, 6) for v in bounds], "pixel": "row 0 = north, col 0 = west"}


def _header_date(line: str) -> Optional[str]:
    m = re.fullmatch(r"\s*(\d{4})\s+(\d{1,2})\s+(\d{1,2})\s*", line)
    if m:
        return f"{int(m[1]):04d}{int(m[2]):02d}{int(m[3]):02d}"
    m = re.search(r"(\d{8})\s*$", line)
    return m[1] if m else None


def parse_grid_text(p: GridProduct, text: str) -> tuple[Optional[str], np.ndarray, dict]:
    """GOOS ASCII → (header 日期, 陣列, 統計)。行數或行寬不符直接丟 ValueError（不猜）。

    sst：int16 0.1℃（888→SST_ICE、999→SST_LAND、無法解析或超出 -30..400 → SST_BAD 並計數）
    ice：uint8（0/2/3/4/5 原值、9→254 陸地、7→255 無解析；其他字元 → 255 並計數）
    """
    lines = text.replace("\r", "").split("\n")
    hdr = _header_date(lines[0])
    body = lines[1:1 + p.nrows]
    if len(body) != p.nrows or any(len(l) != p.ncols * p.width for l in body):
        bad = sorted({len(l) for l in body})[:5]
        raise ValueError(f"{p.name}: 網格大小不符（rows={len(body)}，行寬={bad}，預期 {p.nrows}×{p.ncols * p.width}）")
    raw = "".join(body).encode("ascii", errors="replace")
    stats: dict = {"unknown": 0}
    if p.kind == "sst":
        cells = np.frombuffer(raw, dtype=f"S{p.width}")
        try:
            vals = cells.astype(np.int32)
            bad = np.zeros(vals.shape, bool)
        except ValueError:
            vals = np.empty(cells.shape, np.int32)
            bad = np.zeros(cells.shape, bool)
            for i, c in enumerate(cells):
                try:
                    vals[i] = int(c)
                except ValueError:
                    vals[i], bad[i] = 0, True
        ice, land = vals == 888, vals == 999
        bad |= ~(ice | land) & ((vals < -30) | (vals > 400))
        out = vals.astype(np.int16)
        out[ice], out[land], out[bad] = SST_ICE, SST_LAND, SST_BAD
        stats.update(unknown=int(bad.sum()), ice=int(ice.sum()), land=int(land.sum()),
                     valid=int(out.size - ice.sum() - land.sum() - bad.sum()))
        valid = out[(out > SST_BAD)]
        if valid.size:
            stats.update(min_dc=int(valid.min()), max_dc=int(valid.max()))
        return hdr, out.reshape(p.nrows, p.ncols), stats
    codes = np.frombuffer(raw, np.uint8) - ord("0")
    out = np.full(codes.shape, ICE_NODATA, np.uint8)
    for c in ICE_CLASSES:
        out[codes == c] = c
    out[codes == 9] = ICE_LAND
    known = np.isin(codes, list(ICE_CLASSES) + [7, 9])
    stats.update(unknown=int((~known).sum()), land=int((codes == 9).sum()), no_analysis=int((codes == 7).sum()),
                 ice_any=int(np.isin(codes, [2, 3, 4, 5]).sum()), open_water=int((codes == 0).sum()))
    return hdr, out.reshape(p.nrows, p.ncols), stats


def gzip_det(data: bytes) -> bytes:
    return gzip.compress(data, compresslevel=9, mtime=0)


def tar_bytes(members: list[tuple[str, bytes]], mtime: int) -> bytes:
    bio = io.BytesIO()
    with tarfile.open(fileobj=bio, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for name, data in members:
            tf.addfile(sp._tarinfo(name, len(data), mtime), io.BytesIO(data))
    return bio.getvalue()


# ───────────────────────── UV 圖磚 ─────────────────────────

UV_ZOOM = 5
UV_CANVAS = (26, 11, 3, 3)               # z5 x26..28、y11..13（768×768）
UV_TILES = ((28, 12), (27, 12), (27, 13), (28, 11), (28, 13), (26, 13))  # 解析範圍內的磚（其他 404＝範圍外）；第一個用來判斷幀是否存在
UV_HOURS_JST = range(6, 19)              # 06..18 JST
UV_RETENTION = timedelta(hours=34)       # 上游約保留「前一日 06 時」起；保守取 34h
UV_URL = UV_BASE + "/tile/uv/uv_a/{vt}/uvi_anal/{vt}/surf/uvi_anal/{z}/{x}/{y}.png"


def uv_expected_vts(start: datetime, end: datetime) -> list[str]:
    """[start, end] 間的 UV 解析時刻（每正時 06–18 JST），回 UTC 字串（舊→新）。"""
    out = []
    d = start.astimezone(JST).date()
    while d <= end.astimezone(JST).date():
        for h in UV_HOURS_JST:
            t = datetime(d.year, d.month, d.day, h, tzinfo=JST)
            if start <= t <= end:
                out.append(t.astimezone(timezone.utc).strftime(VT_FMT))
        d += timedelta(days=1)
    return out


def parse_vt(vt: str) -> datetime:
    return datetime.strptime(vt, VT_FMT).replace(tzinfo=timezone.utc)


def jst_day(vt: str) -> str:
    return parse_vt(vt).astimezone(JST).strftime("%Y%m%d")


# ───────────────────────── 黃砂 ─────────────────────────

KOSA_LIST = KOSA_BASE + "/list_js/s_jpjp_list.js"
KOSA_IMG = KOSA_BASE + "/img/surf/jp/{t}_kosafcst-s_jp_jp.png"
KOSA_SHA_KEEP_DAYS = 10


def parse_kosa_list(text: str) -> list[tuple[str, str]]:
    """s_jpjp_list.js → [(kind, YYYYMMDDHHNN)]；kind=fcst（予測）/anly（解析）。時刻為 JST（頁面以瀏覽器本地時間比對）。"""
    out = []
    for kind, arr in (("fcst", "imgFcst"), ("anly", "imgAnly")):
        for t in re.findall(arr + r"\[\w\+\+\]\s*=\s*\"(\d{12})\"", text):
            out.append((kind, t))
    return out


# ───────────────────────── collector ─────────────────────────

class JmaOceanEnvCollector(BaseCollector):
    name = "jma_ocean_env"
    interval_minutes = config.JAPAN_JMA_OCEAN_ENV_INTERVAL
    COLLECT_TIMEOUT = 1800
    GC_THRESHOLD_SEC = 10

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-ocean-env")
        self._s3 = None
        self._lock = threading.Lock()
        self._requests = 0
        self._pending_commit: Optional[dict] = None
        self._uv_table: Optional[DecodeTable] = None
        self.spool_root = Path(config.LOCAL_DATA_DIR) / "weather_raw" / SOURCE
        self.state_dir = Path(config.LOCAL_DATA_DIR) / "state" / "jma_ocean_env"
        names = [n.strip() for n in config.JAPAN_JMA_OCEAN_ENV_PRODUCTS.split(",") if n.strip()]
        self.products = [n for n in names if n in ALL_PRODUCTS]
        seasons = parse_product_map(config.JAPAN_JMA_OCEAN_ENV_SEASON, _months)
        self.seasons: dict[str, frozenset] = {k: v for k, v in seasons.items() if k in ALL_PRODUCTS}
        self.lookback_days = max(1, config.JAPAN_JMA_OCEAN_ENV_INITIAL_LOOKBACK_DAYS)
        self.missing_after_days = max(2, config.JAPAN_JMA_OCEAN_ENV_MISSING_AFTER_DAYS)
        self.max_days_per_run = max(1, config.JAPAN_JMA_OCEAN_ENV_MAX_DAYS_PER_RUN)

    # ───────── BaseCollector hooks ─────────
    def require_db_write(self) -> bool:
        return True

    def run(self) -> dict:
        self._pending_commit = None
        stats = super().run()
        if self._pending_commit is not None and not stats.get("error"):
            for pname, sent in self._pending_commit.items():
                st = self._load_state(pname)
                for k in sent["frames"]:
                    st["pending_frames"].pop(k, None)
                for k in sent["daily"]:
                    st["pending_daily"].pop(k, None)
                self._save_state(pname, st)
        self._pending_commit = None
        return stats

    # ───────── 共用 ─────────
    def s3(self):
        if self._s3 is None and config.S3_BUCKET:
            from storage.s3 import S3Storage
            self._s3 = S3Storage()
        return self._s3

    def uv_table(self) -> DecodeTable:
        if self._uv_table is None:
            self._uv_table = DecodeTable.load(SOURCE, "uv_anal")
        return self._uv_table

    def in_season(self, product: str, day: str) -> bool:
        ms = self.seasons.get(product)
        return ms is None or int(day[4:6]) in ms

    def _state_path(self, pname: str) -> Path:
        return self.state_dir / f"{pname}.json"

    def _load_state(self, pname: str) -> dict:
        st = sp.read_json(self._state_path(pname), None) or {}
        for k, v in (("epoch", None), ("finalized_until", None), ("pending_frames", {}), ("pending_daily", {}),
                     ("last_list_sha", None), ("recent_shas", {})):
            st.setdefault(k, v)
        return st

    def _save_state(self, pname: str, st: dict) -> None:
        sp.write_json_atomic(self._state_path(pname), st)

    def receipt_path(self, product: str, day: str) -> Path:
        return self.spool_root / "_receipts" / product / f"{day}.json"

    def day_dir(self, product: str, day: str) -> Path:
        return self.spool_root / product / day

    def _get(self, url: str) -> tuple[int, bytes, dict]:
        """回 (status, body, headers)。404 視為不存在；其他錯誤重試 2 次後回 (-1 或狀態碼, b'', {})。"""
        last = -1
        for attempt in range(3):
            with self._lock:
                self._requests += 1
            try:
                r = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return 200, r.content, dict(r.headers)
                if r.status_code == 404:
                    return 404, b"", {}
                last = r.status_code
            except Exception:  # noqa: BLE001
                last = -1
            time.sleep(1 + attempt)
        return last, b"", {}

    def _put(self, local: Path, key: str, storage_class: str) -> tuple[bool, str]:
        from storage.s3 import put_verified
        sha = sp.sha256_file(local)
        ctype = "application/x-tar" if ".tar" in key else "application/octet-stream"
        try:
            return put_verified(self.s3(), local, key, sha, storage_class, content_type=ctype), sha
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ S3 {key}: {e}")
            return False, sha

    @staticmethod
    def _frame_row(product: str, valid_time: datetime, status: str, **kw) -> dict:
        row = {"_type": "frame", "source": SOURCE, "product": product, "valid_time": valid_time.isoformat(),
               "base_time": None, "zoom_raw": None, "tiles_requested": None, "tiles_nonempty": None,
               "raw_bytes": None, "nonzero_pixels": None, "unknown_color_pixels": None, "status": status,
               "probe_mode": None, "probe_misses": None, "tiles_inferred_blank": None}
        row.update(kw)
        return row

    @staticmethod
    def _daily_row(product: str, day: str, **kw) -> dict:
        row = {"_type": "daily", "source": SOURCE, "product": product, "obs_date": f"{day[:4]}-{day[4:6]}-{day[6:]}",
               "frames_expected": None, "frames_ok": None, "raw_key": None, "raw_bytes": None, "raw_sha256": None,
               "raw_storage_class": "DEEP_ARCHIVE", "grid_objects": 0, "grid_bytes": 0, "summary_key": None,
               "decode_table_version": None, "status": "failed", "uploaded_at": None}
        row.update(kw)
        return row

    # ───────── 每日 ASCII 網格產品 ─────────
    def candidate_days(self, product: str, st: dict, today: date) -> list[str]:
        if st["epoch"] is None:
            st["epoch"] = (today - timedelta(days=self.lookback_days)).strftime("%Y%m%d")
        start = datetime.strptime(st["finalized_until"] or st["epoch"], "%Y%m%d").date()
        out, d = [], start
        while d < today:  # D 的檔約 D+1 10:00 JST 才出爐，當天不打（省 24 次 404／日）
            ds = d.strftime("%Y%m%d")
            if self.in_season(product, ds) and not self.receipt_path(product, ds).exists():
                out.append(ds)
            d += timedelta(days=1)
        return out

    def fetch_grid_product(self, p: GridProduct, now: datetime) -> dict:
        st = self._load_state(p.name)
        today = now.astimezone(JST).date()
        out = {"fetched": 0, "not_yet": 0, "missing": 0, "requests": 0, "failed": 0, "bytes_raw": 0, "bytes_t2": 0}
        done = 0
        for day in self.candidate_days(p.name, st, today):
            if done >= self.max_days_per_run:
                break
            status, body, headers = self._get(p.url(day))
            out["requests"] += 1
            age = (today - datetime.strptime(day, "%Y%m%d").date()).days
            d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=JST)
            if status == 404:
                if age >= self.missing_after_days:
                    st["pending_frames"][day] = self._frame_row(p.name, d0, "missing")
                    sp.write_json_atomic(self.receipt_path(p.name, day), {"status": "missing", "url": p.url(day)})
                    out["missing"] += 1
                else:
                    out["not_yet"] += 1
                continue
            if status != 200:
                out["failed"] += 1
                continue
            done += 1
            info = self.process_grid_file(p, day, body, headers, st)
            if info is None:
                out["failed"] += 1
                continue
            out["fetched"] += 1
            out["bytes_raw"] += info["raw_bytes"]
            out["bytes_t2"] += info["t2_bytes"]
        self._advance_finalized(p.name, st, today)
        self._save_state(p.name, st)
        return out

    def _advance_finalized(self, product: str, st: dict, today: date) -> None:
        """finalized_until 前進到第一個「季內、尚無 receipt」的日子（之前的不再回頭掃）。"""
        d = datetime.strptime(st["finalized_until"] or st["epoch"], "%Y%m%d").date()
        while d < today:
            ds = d.strftime("%Y%m%d")
            if self.in_season(product, ds) and not self.receipt_path(product, ds).exists():
                break
            d += timedelta(days=1)
        st["finalized_until"] = d.strftime("%Y%m%d")

    def process_grid_file(self, p: GridProduct, day: str, body: bytes, headers: dict, st: dict) -> Optional[dict]:
        """原始檔 → T3（DA）＋T2（npz）→ 驗證 → receipt＋DB 列。任何失敗回 None（下輪重試，原檔不刪上游）。"""
        dd = self.day_dir(p.name, day)
        dd.mkdir(parents=True, exist_ok=True)
        d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=JST)
        raw_name = p.fname.format(d=day)
        try:
            text = (gzip.decompress(body) if p.gz else body).decode("ascii", errors="replace")
            hdr, grid, stats = parse_grid_text(p, text)
            if hdr != day:
                raise ValueError(f"header 日期 {hdr} ≠ 檔名 {day}")
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ {p.name} {day} 解析失敗：{e}（原檔仍冷存）")
            grid, stats, hdr = None, {"parse_error": str(e)}, None
        if stats.get("unknown"):
            print(f"[{self.name}] ⚠ {p.name} {day} 無法解析的值 {stats['unknown']} 格 → 設為缺測，不歸 0")
        raw_sha = sp.sha256_bytes(body)
        manifest = {"source": SOURCE, "product": p.name, "date": day, "url": p.url(day), "file": raw_name,
                    "bytes": len(body), "sha256": raw_sha, "last_modified": headers.get("Last-Modified")
                    or headers.get("last-modified"), "header_date": hdr, "parser": GRID_PARSER_VERSION,
                    "stats": stats, "license": LICENSE}
        mtime = int(d0.timestamp())
        tar = tar_bytes([(raw_name, body), ("manifest.json", json.dumps(manifest, ensure_ascii=False,
                                                                       sort_keys=True).encode())], mtime)
        t3_ext = ".tar" if p.gz else ".tar.gz"
        t3_local = dd / f"{day}{t3_ext}"
        t3_local.write_bytes(tar if p.gz else gzip_det(tar))
        t3_key = f"weather-raw/{SOURCE}/{p.name}/{day[:4]}/{day[4:6]}/{day}{t3_ext}"
        ok, t3_sha = self._put(t3_local, t3_key, "DEEP_ARCHIVE")
        if not ok:
            return None
        t2_key, t2_bytes = None, 0
        if grid is not None:
            t2_local = dd / f"{day}.npz"
            meta = {"source": SOURCE, "product": p.name, "date": day, "georef": lonlat_georef(p),
                    "parser": GRID_PARSER_VERSION, "raw_key": t3_key, "raw_sha256": raw_sha, "stats": stats,
                    "license": LICENSE_DERIVED}
            if p.kind == "sst":
                meta.update(units="0.1 degC（int16，值÷10＝℃）",
                            nodata={str(SST_LAND): "陸地／無法決定（原 999）", str(SST_ICE): "海冰（原 888）",
                                    str(SST_BAD): "無法解析"})
                arrays = {"sst_dc": grid}
            else:
                meta.update(units="密接度級（uint8）", classes={str(k): v for k, v in ICE_CLASSES.items()},
                            nodata={str(ICE_LAND): "陸地（原 9）", str(ICE_NODATA): "無解析／雲（原 7）或無法解析"})
                arrays = {"ice": grid}
            tmp = t2_local.with_name(t2_local.name + ".tmp.npz")
            np.savez_compressed(tmp, meta=np.array(json.dumps(meta, ensure_ascii=False, sort_keys=True)), **arrays)
            tmp.replace(t2_local)
            t2_bytes = t2_local.stat().st_size
            t2_key = f"weather-grid/{SOURCE}/{p.name}/{day[:4]}/{day}.npz"
            ok, _ = self._put(t2_local, t2_key, "STANDARD")
            if not ok:
                return None
        # 發布幀（契約 §5：sst_him／sea_ice 每日一張，valid_time＝該日 00:00 JST）；失敗只 log，不影響 T2/T3
        image = None
        if grid is not None and p.name in pub.PUBLISH_PRODUCTS:
            image = pub.publish(p.name, d0, grid, georef=lonlat_georef(p), r2=getattr(self, "_r2", None),
                                log_prefix=f"[{self.name}]")
        nonzero = None
        if grid is not None:
            nonzero = (int(stats.get("valid", 0)) if p.kind == "sst" else int(stats.get("ice_any", 0)))
        status = "ok" if grid is not None else "partial"
        st["pending_frames"][day] = self._frame_row(
            p.name, d0, status, tiles_requested=1, tiles_nonempty=1, raw_bytes=len(body),
            nonzero_pixels=nonzero, unknown_color_pixels=int(stats.get("unknown", 0)) if grid is not None else None,
            **(image or {}))
        row = self._daily_row(p.name, day, frames_expected=1, frames_ok=1, raw_key=t3_key,
                              raw_bytes=t3_local.stat().st_size, raw_sha256=t3_sha, grid_objects=1 if t2_key else 0,
                              grid_bytes=t2_bytes, decode_table_version=GRID_PARSER_VERSION, status="verified",
                              uploaded_at=datetime.now(timezone.utc).isoformat())
        st["pending_daily"][day] = row
        sp.write_json_atomic(self.receipt_path(p.name, day), {**row, "t2_key": t2_key, "stats": stats})
        shutil.rmtree(dd, ignore_errors=True)
        return {"raw_bytes": len(body), "t3_bytes": row["raw_bytes"], "t2_bytes": t2_bytes}

    # ───────── UV 解析圖磚 ─────────
    def _uv_day_state(self, day: str) -> dict:
        ds = sp.read_json(self.day_dir("uv_anal", day) / "state.json", None) or {}
        ds.setdefault("frames", {})
        return ds

    def _uv_save_day(self, day: str, ds: dict) -> None:
        sp.write_json_atomic(self.day_dir("uv_anal", day) / "state.json", ds)

    def _uv_acc(self, day: str) -> sp.DayAccumulator:
        return sp.DayAccumulator(self.day_dir("uv_anal", day) / "acc", (UV_CANVAS[3] * 256, UV_CANVAS[2] * 256),
                                 self.uv_table().thresholds)

    def fetch_uv_frame(self, vt: str) -> Optional[dict]:
        """抓一幀（6 磚）。第一磚 404＝上游尚無此幀 → None。範圍外磚不抓，網格保持 0（＝透明）。"""
        table = self.uv_table()
        first = self._get(UV_URL.format(vt=vt, z=UV_ZOOM, x=UV_TILES[0][0], y=UV_TILES[0][1]))
        if first[0] == 404:
            return None
        res = {UV_TILES[0]: first}
        for k in UV_TILES[1:]:
            res[k] = self._get(UV_URL.format(vt=vt, z=UV_ZOOM, x=k[0], y=k[1]))
        x0, y0, nx, ny = UV_CANVAS
        grid = np.zeros((ny * 256, nx * 256), np.uint8)
        members, failed, unknown_colors = [], [], {}
        unknown = nonempty = 0
        for (x, y), (s, b, _h) in res.items():
            r0, c0 = (y - y0) * 256, (x - x0) * 256
            if s != 200:
                grid[r0:r0 + 256, c0:c0 + 256] = MISSING
                failed.append(f"z{UV_ZOOM}/{x}/{y}:{s}")
                continue
            members.append((f"uv_anal/{vt}/z{UV_ZOOM}/{x}/{y}.png", b))
            td = decode_tile(b, table)
            grid[r0:r0 + 256, c0:c0 + 256] = td.levels
            unknown += td.unknown_pixels
            for c, n in td.unknown_colors.items():
                unknown_colors[c] = unknown_colors.get(c, 0) + n
            nonempty += 0 if td.is_blank else 1
        if unknown:
            print(f"[{self.name}] ⚠ uv_anal {vt} 表外顏色 {unknown} px {unknown_colors}（解碼表 {table.version}）"
                  f"→ 設為缺測 255，不歸 0")
        day = jst_day(vt)
        dd = self.day_dir("uv_anal", day)
        sp.write_frame_tar(dd / "frames" / f"{vt}.tar", members, int(parse_vt(vt).timestamp()))
        sp.save_grid(dd / "grids" / f"{vt}.npz", grid)
        self._uv_acc(day).apply(vt, grid)
        image = pub.publish("uv_anal", parse_vt(vt), grid,
                            bounds_lonlat=sp.georef(UV_ZOOM, x0, y0, nx * 256, ny * 256)["bounds_lonlat"],
                            r2=getattr(self, "_r2", None), log_prefix=f"[{self.name}]")
        return {**(image or {}),"status": "ok" if not failed else "partial", "tiles_requested": len(res), "tiles_nonempty": nonempty,
                "raw_bytes": sum(len(b) for _, b in members),
                "nonzero_pixels": int(np.count_nonzero((grid > 0) & (grid < MISSING))),
                "unknown_color_pixels": unknown, "unknown_colors": unknown_colors, "failed_tiles": failed,
                "grid": True, "decode_table_version": table.version,
                "fetched_at": datetime.now(timezone.utc).isoformat()}

    def uv_frame_row(self, vt: str, fr: dict) -> dict:
        t = parse_vt(vt)
        if fr["status"] == "missing":
            return self._frame_row("uv_anal", t, "missing")
        return self._frame_row("uv_anal", t, fr["status"], base_time=t.isoformat(), zoom_raw=UV_ZOOM,
                               **{k: fr.get(k) for k in ("tiles_requested", "tiles_nonempty", "raw_bytes",
                                                         "nonzero_pixels", "unknown_color_pixels")},
                               **{k: fr[k] for k in pub.IMAGE_FIELDS if fr.get(k) is not None})

    def collect_uv(self, now: datetime) -> dict:
        st = self._load_state("uv_anal")
        if st["epoch"] is None:
            st["epoch"] = (now - UV_RETENTION).strftime(VT_FMT)
        epoch = parse_vt(st["epoch"])
        window_start = max(epoch, now - UV_RETENTION)
        out = {"fetched": 0, "unavailable": 0, "missing": 0, "requests": 0, "days_closed": [], "days_failed": []}
        r0 = self._requests
        # 1) 超出保留期仍沒抓到 → missing
        scan_from = parse_vt(st["finalized_until"]) if st["finalized_until"] else epoch
        if window_start > scan_from:
            for vt in uv_expected_vts(scan_from, window_start - timedelta(seconds=1)):
                day = jst_day(vt)
                if self.receipt_path("uv_anal", day).exists():
                    continue
                ds = self._uv_day_state(day)
                if vt in ds["frames"]:
                    continue
                ds["frames"][vt] = {"status": "missing"}
                self._uv_save_day(day, ds)
                st["pending_frames"][vt] = self.uv_frame_row(vt, ds["frames"][vt])
                out["missing"] += 1
            st["finalized_until"] = window_start.strftime(VT_FMT)
        # 2) 保留期內補抓
        for vt in uv_expected_vts(window_start, now):
            day = jst_day(vt)
            if self.receipt_path("uv_anal", day).exists():
                continue
            ds = self._uv_day_state(day)
            prev = ds["frames"].get(vt)
            if prev is not None and prev.get("status") != "partial":
                continue
            # 保留期內的 partial 幀（部分磚失敗）仍可重抓；日結前補齊即升級為 ok
            fr = self.fetch_uv_frame(vt)
            if fr is None:
                out["unavailable"] += 1
                continue
            ds["frames"][vt] = fr
            self._uv_save_day(day, ds)
            if prev is not None:
                # 日累積器對同一 vt 只套一次；換了網格要整日重建，T1 才不會沿用舊的 partial
                dd = self.day_dir("uv_anal", day)
                self._uv_acc(day).rebuild({v: dd / "grids" / f"{v}.npz" for v, f in ds["frames"].items()
                                           if f.get("status") in ("ok", "partial")})
                out["refetched"] = out.get("refetched", 0) + 1
            st["pending_frames"][vt] = self.uv_frame_row(vt, fr)
            out["fetched"] += 1
        # 3) 日結：該日 13 幀皆定案（抓到或 missing）
        base = self.spool_root / "uv_anal"
        if base.exists() and self.s3() is not None:
            for dd in sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit()):
                day = dd.name
                d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=JST)
                exp = uv_expected_vts(max(d0, epoch), d0 + timedelta(days=1) - timedelta(seconds=1))
                ds = self._uv_day_state(day)
                if not exp or not all(v in ds["frames"] for v in exp):
                    continue
                (out["days_closed"] if self.close_uv_day(day, ds, exp, st) else out["days_failed"]).append(day)
        out["requests"] = self._requests - r0
        self._save_state("uv_anal", st)
        return out

    def close_uv_day(self, day: str, ds: dict, exp: list[str], st: dict) -> bool:
        dd = self.day_dir("uv_anal", day)
        table = self.uv_table()
        frames = ds["frames"]
        ok_vts = sorted(v for v in exp if frames[v]["status"] in ("ok", "partial"))
        grids = {v: dd / "grids" / f"{v}.npz" for v in ok_vts}
        x0, y0, nx, ny = UV_CANVAS
        georef = sp.georef(UV_ZOOM, x0, y0, nx * 256, ny * 256)
        labels = {str(k): v for k, v in table.labels.items()}
        row = self._daily_row("uv_anal", day, frames_expected=len(exp), frames_ok=len(ok_vts),
                              decode_table_version=table.version)
        try:
            grid_objects = grid_bytes = 0
            if ok_vts:
                # T2：全日每幀網格一檔（檔內 zero／same_as_prev 去重）
                t2 = dd / "t2.npz"
                meta = {"source": SOURCE, "product": "uv_anal", "date_jst": day, "georef": georef,
                        "decode_table_version": table.version, "levels": labels,
                        "nodata": {"0": "範圍外（透明，不是 UVI 0）", "255": "缺測／表外顏色"},
                        "encoding": "level＝UVI＋1", "license": LICENSE_DERIVED}
                sp.write_hour_npz(t2, [(v, grids.get(v)) for v in exp], (ny * 256, nx * 256), meta)
                ok, _ = self._put(t2, f"weather-grid/{SOURCE}/uv_anal/{day[:4]}/{day}.npz", "STANDARD")
                if not ok:
                    raise RuntimeError("T2 上傳／驗證失敗")
                grid_objects, grid_bytes = 1, t2.stat().st_size
                # T1：日最大 UVI、達標幀數
                acc = self._uv_acc(day)
                if acc.needs_rebuild() or acc.applied != set(grids):
                    acc.rebuild(grids)
                t1 = dd / "t1.npz"
                meta1 = {**meta, "thresholds": table.thresholds, "frames_expected": len(exp),
                         "valid_frames": len(grids), "nodata": {"max_level=255": "全日缺測", "max_level=0": "範圍外"}}
                acc.write_summary(t1, valid_frames=len(grids), meta=meta1)
                key1 = f"weather-daily/{SOURCE}/uv_anal/{day[:4]}/{day}.npz"
                ok, _ = self._put(t1, key1, "STANDARD")
                if not ok:
                    raise RuntimeError("T1 上傳／驗證失敗")
                row["summary_key"] = key1
                grid_bytes += t1.stat().st_size
            row.update(grid_objects=grid_objects, grid_bytes=grid_bytes)
            # T3
            manifest = {"source": SOURCE, "product": "uv_anal", "date_jst": day, "timezone": "Asia/Tokyo",
                        "url_template": UV_URL, "zoom": UV_ZOOM, "tiles": [f"{x}/{y}" for x, y in UV_TILES],
                        "decode_table_version": table.version,
                        "frames": [{"valid_time": v, **frames[v]} for v in exp],
                        "missing": [v for v in exp if frames[v]["status"] == "missing"], "license": LICENSE}
            local = dd / f"{day}.tar"
            info = sp.pack_day_tar(local, [dd / "frames" / f"{v}.tar" for v in ok_vts], manifest,
                                   int(datetime.strptime(day, "%Y%m%d").timestamp()))
            key = f"weather-raw/{SOURCE}/uv_anal/{day[:4]}/{day[4:6]}/{day}.tar"
            ok, sha = self._put(local, key, "DEEP_ARCHIVE")
            if not ok:
                raise RuntimeError("T3 上傳／驗證失敗")
            row.update(raw_key=key, raw_bytes=info["bytes"], raw_sha256=sha, status="verified",
                       uploaded_at=datetime.now(timezone.utc).isoformat())
            sp.write_json_atomic(self.receipt_path("uv_anal", day), {**row, "dedup": {k: info[k] for k in (
                "tiles", "blank", "empty", "unique_blobs", "bytes_before", "bytes_blobs")}})
            shutil.rmtree(dd)
            st["pending_daily"][day] = row
            return True
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ uv_anal {day} 日結失敗（下輪重試）: {e}")
            st["pending_daily"][day] = row
            return False

    # ───────── 黃砂解析預測圖 ─────────
    def collect_kosa(self, now: datetime) -> dict:
        st = self._load_state("kosa_fcst")
        out = {"issued": False, "images": 0, "new_blobs": 0, "dedup_refs": 0, "requests": 0, "failed": 0,
               "days_closed": [], "days_failed": []}
        r0 = self._requests
        if self.in_season("kosa_fcst", now.astimezone(JST).strftime("%Y%m%d")):  # 季外只做日結，不打上游
            status, body, headers = self._get(KOSA_LIST)
            if status != 200:
                raise RuntimeError(f"kosa list HTTP {status}")
            list_sha = sp.sha256_bytes(body)
            if list_sha != st["last_list_sha"]:
                issue = self.fetch_kosa_issue(body, headers, now, st)
                out.update(issue)
                # 有圖抓取失敗時不推進 last_list_sha，下輪同一份清單會再抓（review F079）
                if not issue.get("failed"):
                    st["last_list_sha"] = list_sha
        else:
            out["off_season"] = True
        # 日結：發布日（JST）D 在 D+1 01:00 後
        base = self.spool_root / "kosa_fcst"
        if base.exists() and self.s3() is not None:
            for dd in sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit()):
                d0 = datetime.strptime(dd.name, "%Y%m%d").replace(tzinfo=JST)
                if now.astimezone(JST) < d0 + timedelta(days=1, hours=1):
                    continue
                (out["days_closed"] if self.close_kosa_day(dd.name, st) else out["days_failed"]).append(dd.name)
        # 跨日 sha256 索引只留最近 KOSA_SHA_KEEP_DAYS 天
        cutoff = (now.astimezone(JST) - timedelta(days=KOSA_SHA_KEEP_DAYS)).strftime("%Y%m%d")
        st["recent_shas"] = {h: v for h, v in st["recent_shas"].items() if v["day"] >= cutoff}
        out["requests"] = self._requests - r0
        self._save_state("kosa_fcst", st)
        return out

    def fetch_kosa_issue(self, list_body: bytes, headers: dict, now: datetime, st: dict) -> dict:
        """一次發布（list 內容有變）：抓清單上全部圖；與近期已存的圖 sha256 相同 → 只記引用。"""
        lm = headers.get("Last-Modified") or headers.get("last-modified")
        try:
            issued = parsedate_to_datetime(lm).astimezone(timezone.utc) if lm else now
        except (TypeError, ValueError):
            issued = now
        issued = issued.replace(second=0, microsecond=0)
        ivt = issued.strftime(VT_FMT)
        day = issued.astimezone(JST).strftime("%Y%m%d")
        items = parse_kosa_list(list_body.decode("utf-8", errors="replace"))
        members: list[tuple[str, bytes]] = [(f"kosa_fcst/{ivt}/s_jpjp_list.js", list_body)]
        entries, failed = [], []
        new_blobs = refs = 0
        for kind, t in items:
            s, b, _h = self._get(KOSA_IMG.format(t=t))
            e = {"kind": kind, "valid_jst": t, "http": s}
            if s != 200:
                failed.append(t)
                entries.append(e)
                continue
            h = sp.sha256_bytes(b)
            e.update(sha256=h, bytes=len(b))
            prev = st["recent_shas"].get(h)
            if prev and prev["day"] != day and self._kosa_sha_verified(prev):
                e["same_as"] = prev          # 內容已在另一日「已驗證上傳」的 T3 tar，此處只記引用
                refs += 1
            else:
                members.append((f"kosa_fcst/{ivt}/{kind}_{t}.png", b))
                entry = {"day": day, "issue": ivt, "member": f"{kind}_{t}.png", "verified": False}
                if prev is None or prev["day"] != day:
                    # 舊日條目尚未驗證：改指向本日（本日 tar 自帶這張圖）；本日 T3 驗證後才升級為可引用
                    st["recent_shas"][h] = entry
                new_blobs += 1
            entries.append(e)
        issue_doc = {"issued_utc": issued.isoformat(), "last_modified": lm, "images": entries,
                     "list_sha256": sp.sha256_bytes(list_body), "time_basis": "valid_jst＝JST（頁面以本地時間比對）"}
        members.append((f"kosa_fcst/{ivt}/issue.json",
                        json.dumps(issue_doc, ensure_ascii=False, sort_keys=True).encode()))
        dd = self.day_dir("kosa_fcst", day)
        sp.write_frame_tar(dd / "frames" / f"{ivt}.tar", members, int(issued.timestamp()))
        ds = sp.read_json(dd / "state.json", None) or {"issues": {}}
        ok_n = len(items) - len(failed)
        ds["issues"][ivt] = {"images": len(items), "ok": ok_n, "failed": failed, "new_blobs": new_blobs,
                             "dedup_refs": refs, "raw_bytes": sum(len(b) for _, b in members)}
        sp.write_json_atomic(dd / "state.json", ds)
        status = "ok" if not failed and items else "partial"
        st["pending_frames"][ivt] = self._frame_row(
            "kosa_fcst", issued, status, base_time=issued.isoformat(), tiles_requested=len(items) + 1,
            tiles_nonempty=ok_n, raw_bytes=ds["issues"][ivt]["raw_bytes"])
        if failed:
            print(f"[{self.name}] ⚠ kosa_fcst {ivt} {len(failed)} 張抓取失敗：{failed[:5]}")
        return {"issued": True, "issue": ivt, "images": len(items), "new_blobs": new_blobs, "dedup_refs": refs,
                "failed": len(failed)}

    def _kosa_sha_verified(self, entry: dict) -> bool:
        """same_as 只能指向已驗證上傳的 T3：新條目看 verified 旗標；
        舊版 state 沒有旗標時，該日 spool 已不在（已日結刪除）才視為已驗證。"""
        if "verified" in entry:
            return bool(entry["verified"])
        return not self.day_dir("kosa_fcst", entry["day"]).exists()

    def close_kosa_day(self, day: str, st: dict) -> bool:
        dd = self.day_dir("kosa_fcst", day)
        ds = sp.read_json(dd / "state.json", None) or {"issues": {}}
        issues = sorted(ds["issues"])
        row = self._daily_row("kosa_fcst", day, frames_expected=len(issues),
                              frames_ok=sum(1 for i in issues if not ds["issues"][i]["failed"]))
        try:
            manifest = {"source": SOURCE, "product": "kosa_fcst", "date_jst": day, "list_url": KOSA_LIST,
                        "url_template": KOSA_IMG, "issues": ds["issues"], "decoded": False,
                        "why_not_decoded": "色票只有濃／薄連續漸層、無數值刻度，且疊在地形底圖上；只冷存原圖",
                        "license": LICENSE}
            local = dd / f"{day}.tar"
            info = sp.pack_day_tar(local, sorted((dd / "frames").glob("*.tar")), manifest,
                                   int(datetime.strptime(day, "%Y%m%d").timestamp()))
            key = f"weather-raw/{SOURCE}/kosa_fcst/{day[:4]}/{day[4:6]}/{day}.tar"
            ok, sha = self._put(local, key, "DEEP_ARCHIVE")
            if not ok:
                raise RuntimeError("T3 上傳／驗證失敗")
            row.update(raw_key=key, raw_bytes=info["bytes"], raw_sha256=sha, status="verified",
                       uploaded_at=datetime.now(timezone.utc).isoformat())
            sp.write_json_atomic(self.receipt_path("kosa_fcst", day), {**row, "dedup": {k: info[k] for k in (
                "tiles", "unique_blobs", "bytes_before", "bytes_blobs")}})
            shutil.rmtree(dd)
            for v in st["recent_shas"].values():   # T3 已驗證上傳：此日的圖才可被後日 same_as 引用
                if v["day"] == day:
                    v["verified"] = True
            st["pending_daily"][day] = row
            return True
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ kosa_fcst {day} 日結失敗（下輪重試）: {e}")
            st["pending_daily"][day] = row
            return False

    # ───────── 主流程 ─────────
    def collect(self) -> dict:
        now = datetime.now(timezone.utc)
        self._requests = 0
        per: dict[str, dict] = {}
        failures: dict[str, str] = {}
        if self.s3() is None:
            raise RuntimeError("jma_ocean_env: S3 未設定（T3 冷存為必要）")
        for name in self.products:
            try:
                if name in GRID_PRODUCTS:
                    per[name] = self.fetch_grid_product(GRID_PRODUCTS[name], now)
                elif name == "uv_anal":
                    per[name] = self.collect_uv(now)
                elif name == "kosa_fcst":
                    per[name] = self.collect_kosa(now)
            except Exception as e:  # noqa: BLE001
                failures[name] = str(e)
                print(f"[{self.name}] ✗ {name}: {e}")
        rows: list[dict] = []
        commit: dict[str, dict] = {}
        for name in self.products:
            st = self._load_state(name)
            rows.extend(st["pending_frames"].values())
            rows.extend(st["pending_daily"].values())
            commit[name] = {"frames": list(st["pending_frames"]), "daily": list(st["pending_daily"])}
        self._pending_commit = commit
        result = {"data": rows, "products": per, "source_failures": failures, "http_attempts": self._requests,
                  "peak_rss_mb": sp.peak_rss_mb()}
        if self.products and len(failures) == len(self.products):
            raise RuntimeError(f"jma_ocean_env: 全部產品失敗 {failures}")
        if failures:
            result["_collector_error"] = f"jma_ocean_env: 部分產品失敗 {failures}"
        return result
