"""日本氣象廳 網格圖磚收集器（ADR-0021 G 型；gis-platform migration 437）

產品（2026-10-09/10 實測，詳見 config/decode_tables/jma/*.yaml 與 analytics docs/api-platforms/jma/）：
  radar        nowc hrpns（member none）     每 5 分、上游留 24h   z6 全幅＋回波最多 15 磚下鑽 z8 → z7 網格
  rasrf        解析雨量 1h（member none）     每 30 分、留 12h      同上
  risk_land    土砂キキクル（只收 none 定稿；immed0/1/2 是最新 3 幀暫定路徑，不收）每 10 分、留 5.5h
  risk_inund   浸水キキクル（同上）
  risk_flood   洪水キキクル：pbf 向量磚，只打包原始檔進 T3，不解碼、不產 T1/T2
  snow_depth   解析積雪深 snowd（解析值 bt==vt）每 1 小時、留 24h   z6 → z6 網格
  himawari_b13 ひまわり B13 TBB jpg：原生只到 z5（12 磚），每 1 小時 1 張，只留原始

分層（契約 jma_raster_contract.md）：
  spool  {LOCAL_DATA_DIR}/weather_raw/jma/{product}/{YYYYMMDD JST}/  上傳驗證成功即刪
  T3 冷  S3 weather-raw/jma/{product}/{YYYY}/{MM}/{YYYYMMDD}.tar   DEEP_ARCHIVE，永久
  T2 溫  S3 weather-grid/jma/{product}/{YYYYMMDD}/{HH}.npz          STANDARD，90 天（lifecycle）
  T1 熱  S3 weather-daily/jma/{product}/{YYYY}/{YYYYMMDD}.npz       STANDARD，永久

中斷補抓：每輪對每產品列出「上游保留期內、首跑 epoch 之後」所有 validtime，oldest-first 補抓
  spool 沒有的幀（每輪有上限）；超出保留期仍沒抓到才定案 missing。
DB：frame／daily 列先存 product state 的 pending，DB 寫入成功（require_db_write）才清掉（照 jma_quake）。
"""

from __future__ import annotations

import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

import config
from collectors.base import BaseCollector
from collectors.global_climate.jma_common import JMA_BOSAI_BASE, new_jma_session
from collectors.weather_raster.decode import (
    BLANK_PLACEHOLDER_BYTES, MISSING, DecodeTable, decode_tile, missing_tile, pool_max, upsample,
)
from collectors.weather_raster import spool as sp

SOURCE = "jma"
JST = timezone(timedelta(hours=9))
VT_FMT = "%Y%m%d%H%M%S"
CLOSE_AFTER = timedelta(hours=1)          # JST D+1 01:00 之後才日結
RETENTION_MARGIN = timedelta(minutes=30)  # 保留期邊界留緩衝，避免抓到正在被刪的幀


@dataclass(frozen=True)
class Product:
    name: str
    url: str                 # 相對 JMA_BOSAI_BASE；{bt}{vt}{z}{x}{y}
    target_times: str
    cadence_min: int
    retention: timedelta
    coarse_zoom: int
    fine_zoom: Optional[int] = None
    grid_zoom: Optional[int] = None    # None = 不解碼（只留原始）
    fmt: str = "png"                   # png / pbf / jpg
    analysis_filter: str = "bt_eq_vt"  # targetTimes 過濾方式

    @property
    def decodes(self) -> bool:
        return self.grid_zoom is not None


PRODUCTS: dict[str, Product] = {p.name: p for p in (
    Product("radar", "jmatile/data/nowc/{bt}/none/{vt}/surf/hrpns/{z}/{x}/{y}.png",
            "jmatile/data/nowc/targetTimes_N1.json", 5, timedelta(hours=24), 6, 8, 7),
    Product("rasrf", "jmatile/data/rasrf/{bt}/none/{vt}/surf/rasrf/{z}/{x}/{y}.png",
            "jmatile/data/rasrf/targetTimes.json", 30, timedelta(hours=12), 6, 8, 7,
            analysis_filter="rasrf_none"),
    Product("risk_land", "jmatile/data/risk/{bt}/none/{vt}/surf/land/{z}/{x}/{y}.png",
            "jmatile/data/risk/targetTimes.json", 10, timedelta(hours=5, minutes=30), 6, 8, 7,
            analysis_filter="member_none"),
    Product("risk_inund", "jmatile/data/risk/{bt}/none/{vt}/surf/inund/{z}/{x}/{y}.png",
            "jmatile/data/risk/targetTimes.json", 10, timedelta(hours=5, minutes=30), 6, 8, 7,
            analysis_filter="member_none"),
    Product("risk_flood", "jmatile/data/risk/{bt}/none/{vt}/surf/flood/{z}/{x}/{y}.pbf",
            "jmatile/data/risk/targetTimes.json", 10, timedelta(hours=5, minutes=30), 6, 8, None,
            fmt="pbf", analysis_filter="member_none"),
    Product("snow_depth", "jmatile/data/snow/{bt}/none/{vt}/surf/snowd/{z}/{x}/{y}.png",
            "jmatile/data/snow/targetTimes.json", 60, timedelta(hours=24), 6, None, 6,
            analysis_filter="snowd"),
    Product("himawari_b13", "himawari/data/satimg/{bt}/fd/{vt}/B13/TBB/{z}/{x}/{y}.jpg",
            "himawari/data/satimg/targetTimes_fd.json", 60, timedelta(hours=119), 5, None, None,
            fmt="jpg"),
)}

# 日本畫布：z6 x=52..59、y=22..27（＝z5 x26..29、y11..13）
CANVAS_Z6 = (52, 22, 8, 6)


def canvas(z: int) -> tuple[int, int, int, int]:
    """(x0, y0, nx, ny) at zoom z。"""
    x0, y0, nx, ny = CANVAS_Z6
    if z >= 6:
        f = 2 ** (z - 6)
        return x0 * f, y0 * f, nx * f, ny * f
    d = 2 ** (6 - z)
    return x0 // d, y0 // d, nx // d, ny // d


def parse_vt(vt: str) -> datetime:
    return datetime.strptime(vt, VT_FMT).replace(tzinfo=timezone.utc)


def fmt_vt(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime(VT_FMT)


def jst_date(vt: str) -> str:
    return parse_vt(vt).astimezone(JST).strftime("%Y%m%d")


def latest_analysis(product: Product, items: list) -> Optional[str]:
    """targetTimes → 最新的解析（非預報）validtime。"""
    best = None
    for e in items or []:
        if not isinstance(e, dict) or e.get("basetime") != e.get("validtime"):
            continue
        f = product.analysis_filter
        els = e.get("elements") or []
        if f == "rasrf_none" and not (e.get("member") == "none" and "rasrf" in els):
            continue
        if f == "member_none" and e.get("member") != "none":
            continue
        if f == "snowd" and "snowd" not in els:
            continue
        vt = e["validtime"]
        if best is None or vt > best:
            best = vt
    return best


def expected_vts(product: Product, start: datetime, end: datetime) -> list[str]:
    """[start, end] 之間落在 cadence 格點上的 validtime（UTC 字串，舊→新）。"""
    step = timedelta(minutes=product.cadence_min)
    epoch0 = datetime(2000, 1, 1, tzinfo=timezone.utc)
    k = -(-(start - epoch0) // step)  # ceil
    t = epoch0 + k * step
    out = []
    while t <= end:
        out.append(fmt_vt(t))
        t += step
    return out


def is_blank_bytes(data: bytes, fmt: str) -> bool:
    if fmt == "pbf":
        return len(data) == 0
    return len(data) == BLANK_PLACEHOLDER_BYTES


class JmaRasterCollector(BaseCollector):
    name = "jma_raster"
    interval_minutes = config.JAPAN_JMA_RASTER_INTERVAL
    COLLECT_TIMEOUT = 3600  # 含補抓段（預設 45 分）
    GC_THRESHOLD_SEC = 30

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-raster")
        self._tables: dict[str, DecodeTable] = {}
        self._s3 = None
        self._pending_commit: Optional[dict[str, dict]] = None
        self._lock = threading.Lock()
        self._requests = 0
        self.spool_root = Path(config.LOCAL_DATA_DIR) / "weather_raw" / SOURCE
        self.state_dir = Path(config.LOCAL_DATA_DIR) / "state" / "jma_raster"
        names = [n.strip() for n in config.JAPAN_JMA_RASTER_PRODUCTS.split(",") if n.strip()]
        self.products = [PRODUCTS[n] for n in names if n in PRODUCTS]
        self.max_fine_tiles = config.JAPAN_JMA_RASTER_MAX_FINE_TILES
        self.concurrency = max(1, min(4, config.JAPAN_JMA_RASTER_CONCURRENCY))
        self.initial_lookback = timedelta(minutes=config.JAPAN_JMA_RASTER_INITIAL_LOOKBACK_MIN)
        self.backlog_products = {n.strip() for n in config.JAPAN_JMA_RASTER_BACKLOG_PRODUCTS.split(",") if n.strip()}
        self._window: dict[str, datetime] = {}
        self._epoch: dict[str, datetime] = {}

    # ───────── BaseCollector hooks ─────────
    def require_db_write(self) -> bool:
        return True

    def run(self) -> dict:
        self._pending_commit = None
        stats = super().run()
        if self._pending_commit is not None and not stats.get("error"):
            for pname, sent in self._pending_commit.items():
                st = self._load_state(pname)
                for k in sent.get("frames", []):
                    st["pending_frames"].pop(k, None)
                for k in sent.get("daily", []):
                    st["pending_daily"].pop(k, None)
                self._save_state(pname, st)
        self._pending_commit = None
        return stats

    # ───────── 共用 ─────────
    def table(self, product: Product) -> DecodeTable:
        if product.name not in self._tables:
            self._tables[product.name] = DecodeTable.load(SOURCE, product.name)
        return self._tables[product.name]

    def s3(self):
        if self._s3 is None and config.S3_BUCKET:
            from storage.s3 import S3Storage
            self._s3 = S3Storage()
        return self._s3

    def _state_path(self, pname: str) -> Path:
        return self.state_dir / f"{pname}.json"

    def _load_state(self, pname: str) -> dict:
        st = sp.read_json(self._state_path(pname), None) or {}
        st.setdefault("epoch", None)
        st.setdefault("closed_days", [])
        st.setdefault("pending_frames", {})
        st.setdefault("pending_daily", {})
        return st

    def _save_state(self, pname: str, st: dict) -> None:
        st["closed_days"] = sorted(st["closed_days"])[-10:]
        sp.write_json_atomic(self._state_path(pname), st)

    def day_dir(self, product: Product, day: str) -> Path:
        return self.spool_root / product.name / day

    def _day_state(self, product: Product, day: str) -> dict:
        ds = sp.read_json(self.day_dir(product, day) / "state.json", None) or {}
        ds.setdefault("frames", {})
        ds.setdefault("hours", {})
        ds.setdefault("t1", None)
        ds.setdefault("t3", None)
        return ds

    def _save_day(self, product: Product, day: str, ds: dict) -> None:
        sp.write_json_atomic(self.day_dir(product, day) / "state.json", ds)

    def _get(self, url: str) -> tuple[int, bytes]:
        """回 (status, body)。404 視為「不存在」；其他錯誤重試 2 次後回 (-1, b'')。"""
        last = -1
        for attempt in range(3):
            with self._lock:
                self._requests += 1
            try:
                r = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return 200, r.content
                if r.status_code == 404:
                    return 404, b""
                last = r.status_code
            except Exception:  # noqa: BLE001
                last = -1
            time.sleep(1 + attempt)
        return last, b""

    # ───────── 抓一幀 ─────────
    def _url(self, product: Product, vt: str, z: int, x: int, y: int) -> str:
        return f"{JMA_BOSAI_BASE}/" + product.url.format(bt=vt, vt=vt, z=z, x=x, y=y)

    def fetch_frame(self, product: Product, vt: str) -> Optional[dict]:
        """抓並解碼一幀，寫 spool。上游無此幀（探測磚 404）回 None。"""
        zc = product.coarse_zoom
        x0, y0, nx, ny = canvas(zc)
        keys = [(x, y) for y in range(y0, y0 + ny) for x in range(x0, x0 + nx)]
        status, body = self._get(self._url(product, vt, zc, *keys[0]))
        if status == 404:
            return None
        coarse = {keys[0]: (status, body)}
        with ThreadPoolExecutor(self.concurrency) as ex:
            for k, res in zip(keys[1:], ex.map(lambda k: self._get(self._url(product, vt, zc, *k)), keys[1:])):
                coarse[k] = res
        req = len(keys)
        members: list[tuple[str, bytes]] = []
        mtime = int(parse_vt(vt).timestamp())
        ext = product.fmt

        def member(z, x, y):
            return f"{product.name}/{vt}/z{z}/{x}/{y}.{ext}"

        failed: list[str] = []
        for (x, y), (s, b) in coarse.items():
            if s == 200:
                members.append((member(zc, x, y), b))
            else:
                failed.append(f"z{zc}/{x}/{y}:{s}")

        table = self.table(product) if product.decodes else None
        dec: dict[tuple[int, int], object] = {}
        if table is not None:
            for k, (s, b) in coarse.items():
                dec[k] = decode_tile(b, table) if s == 200 else missing_tile()

        # 下鑽：只對「有內容」的粗層磚，依內容量排序取前 max_fine_tiles
        fine_parents: list[tuple[int, int]] = []
        if product.fine_zoom is not None:
            if table is not None:
                cand = [(dec[k].nonzero + dec[k].unknown_pixels, k) for k in keys
                        if coarse[k][0] == 200 and not dec[k].is_blank]
            else:
                cand = [(len(coarse[k][1]), k) for k in keys
                        if coarse[k][0] == 200 and not is_blank_bytes(coarse[k][1], product.fmt)]
            cand.sort(key=lambda t: (-t[0], t[1]))
            fine_parents = [k for _, k in cand[: self.max_fine_tiles]]
            skipped_parents = [k for _, k in cand[self.max_fine_tiles:]]
            skipped_echo = cand[self.max_fine_tiles:]
        else:
            skipped_parents, skipped_echo = [], []

        fine = self._fetch_fine(product, vt, fine_parents) if fine_parents else {}
        req += len(fine)
        for (x, y), (s_, b) in fine.items():
            if s_ == 200:
                members.append((member(product.fine_zoom, x, y), b))
            else:
                failed.append(f"z{product.fine_zoom}/{x}/{y}:{s_}")

        raw_bytes = sum(len(b) for _, b in members)
        nonempty = sum(1 for k in keys if coarse[k][0] == 200 and not is_blank_bytes(coarse[k][1], product.fmt))
        nonzero = unknown = 0
        unknown_colors: dict[str, int] = {}
        grid_path = None
        day = jst_date(vt)
        dd = self.day_dir(product, day)

        if table is not None:
            gz = product.grid_zoom
            gx0, gy0, gnx, gny = canvas(gz)
            grid = np.zeros((gny * 256, gnx * 256), np.uint8)
            for (x, y) in keys:
                if (x, y) in fine_parents:
                    u, cols = self._apply_fine(grid, product, (x, y), fine, table)
                else:
                    r0, c0, cpx = self._parent_rc(product, (x, y))
                    td = dec[(x, y)]
                    u, cols = td.unknown_pixels, td.unknown_colors
                    grid[r0:r0 + cpx, c0:c0 + cpx] = (td.levels if gz == zc
                                                      else upsample(td.levels, 2 ** (gz - zc)))
                unknown += u
                for c, n in cols.items():
                    unknown_colors[c] = unknown_colors.get(c, 0) + n
            grid_path = dd / "grids" / f"{vt}.npz"
            sp.save_grid(grid_path, grid)
            nonzero = int(np.count_nonzero((grid > 0) & (grid < MISSING)))
            del grid

        sp.write_frame_tar(dd / "frames" / f"{vt}.tar", members, mtime)
        status = "ok" if not failed else "partial"
        if unknown:
            print(f"[{self.name}] ⚠ {product.name} {vt} 表外顏色 {unknown} px {unknown_colors}"
                  f"（解碼表 {table.version if table else '-'}）→ 設為缺測 255，不歸 0")
        return {
            "status": status,
            "base_time": vt,
            "zoom_raw": product.fine_zoom if fine_parents else zc,
            "tiles_requested": req,
            "tiles_nonempty": nonempty,
            "raw_bytes": raw_bytes,
            "nonzero_pixels": nonzero if table is not None else None,
            "unknown_color_pixels": unknown if table is not None else None,
            "unknown_colors": unknown_colors,
            "fine_tiles": [f"z{zc}/{x}/{y}" for x, y in fine_parents],
            "fine_skipped": [f"z{zc}/{x}/{y}" for x, y in skipped_parents],
            # 補抓段（backlog）待抓清單：[tile, 回波量]；保留期內低速補抓 z8，過期未抓的移到 fine_unfetched
            "fine_pending": ([[f"z{zc}/{x}/{y}", e] for e, (x, y) in skipped_echo]
                             if product.name in self.backlog_products else []),
            "fine_backlog_done": [],
            "fine_unfetched": ([] if product.name in self.backlog_products
                               else [f"z{zc}/{x}/{y}" for x, y in skipped_parents]),
            "settled": False,
            "failed_tiles": failed,
            "grid": bool(grid_path),
            "decode_table_version": table.version if table else None,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    def _parent_rc(self, product: Product, parent: tuple[int, int]) -> tuple[int, int, int]:
        zc, gz = product.coarse_zoom, product.grid_zoom
        cpx = 256 * 2 ** (gz - zc)  # 粗層一磚在網格上的像素邊長
        cx0, cy0, _, _ = canvas(zc)
        return (parent[1] - cy0) * cpx, (parent[0] - cx0) * cpx, cpx

    def _fine_keys(self, product: Product, parent: tuple[int, int]) -> list[tuple[int, int]]:
        f = 2 ** (product.fine_zoom - product.coarse_zoom)
        px, py = parent
        return [(px * f + i, py * f + j) for j in range(f) for i in range(f)]

    def _fetch_fine(self, product: Product, vt: str, parents: list[tuple[int, int]]) -> dict:
        fkeys = [k for p in parents for k in self._fine_keys(product, p)]
        zf = product.fine_zoom
        out = {}
        with ThreadPoolExecutor(self.concurrency) as ex:
            for k, res in zip(fkeys, ex.map(lambda k: self._get(self._url(product, vt, zf, *k)), fkeys)):
                out[k] = res
        return out

    def _apply_fine(self, grid: np.ndarray, product: Product, parent: tuple[int, int], fine: dict,
                    table: DecodeTable) -> tuple[int, dict]:
        """把 parent 的細層子磚解碼、池化寫進網格對應區塊；回傳表外顏色統計。"""
        r0, c0, _ = self._parent_rc(product, parent)
        f = 2 ** (product.fine_zoom - product.coarse_zoom)
        k = 2 ** (product.fine_zoom - product.grid_zoom)
        sub = 256 // k
        unknown, cols = 0, {}
        for idx, key in enumerate(self._fine_keys(product, parent)):
            j, i = divmod(idx, f)
            s_, b = fine[key]
            td = decode_tile(b, table) if s_ == 200 else missing_tile()
            unknown += td.unknown_pixels
            for c, n in td.unknown_colors.items():
                cols[c] = cols.get(c, 0) + n
            grid[r0 + j * sub:r0 + (j + 1) * sub, c0 + i * sub:c0 + (i + 1) * sub] = pool_max(td.levels, k)
        return unknown, cols

    # ───────── 補抓段（backlog）：其餘有內容的粗層磚，保留期內低速補抓細層 ─────────
    def run_backlog(self, deadline: float) -> dict:
        stats = {"tiles_done": 0, "requests": 0, "frames_touched": 0, "pending_tiles": 0}
        rps = config.JAPAN_JMA_RASTER_BACKLOG_RPS
        if rps <= 0:
            return stats
        items = []
        for product in self.products:
            if product.name not in self.backlog_products or product.fine_zoom is None:
                continue
            ws = self._window.get(product.name)
            base = self.spool_root / product.name
            if ws is None or not base.exists():
                continue
            for dd in sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit()):
                ds = self._day_state(product, dd.name)
                for vt, fr in ds["frames"].items():
                    if fr.get("fine_pending") and not fr.get("settled") and parse_vt(vt) >= ws:
                        items.append((sum(e for _, e in fr["fine_pending"]), product, dd.name, vt))
                        stats["pending_tiles"] += len(fr["fine_pending"])
        items.sort(key=lambda t: (-t[0], t[3]))
        gap = 1.0 / rps
        next_t = time.monotonic()
        for _, product, day, vt in items:
            ds = self._day_state(product, day)
            fr = ds["frames"][vt]
            table = self.table(product) if product.decodes else None
            dd = self.day_dir(product, day)
            grid_path = dd / "grids" / f"{vt}.npz"
            grid = sp.load_grid(grid_path) if table is not None else None
            touched = False
            for tile, echo in sorted(fr["fine_pending"], key=lambda t: -t[1]):
                n_req = 2 ** (2 * (product.fine_zoom - product.coarse_zoom))
                if time.monotonic() + n_req * gap > deadline:
                    break
                _, x, y = tile.split("/")
                parent = (int(x), int(y))
                fine = {}
                for key in self._fine_keys(product, parent):
                    delay = next_t - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    next_t = max(next_t, time.monotonic()) + gap
                    fine[key] = self._get(self._url(product, vt, product.fine_zoom, *key))
                stats["requests"] += len(fine)
                new_members = []
                for (fx, fy), (s_, b) in fine.items():
                    if s_ == 200:
                        new_members.append((f"{product.name}/{vt}/z{product.fine_zoom}/{fx}/{fy}.{product.fmt}", b))
                    else:
                        fr["failed_tiles"].append(f"z{product.fine_zoom}/{fx}/{fy}:{s_}")
                        fr["status"] = "partial"
                ftar = dd / "frames" / f"{vt}.tar"
                import tarfile
                with tarfile.open(ftar) as tf:
                    old = [(m.name, tf.extractfile(m).read()) for m in tf.getmembers() if m.isfile()]
                sp.write_frame_tar(ftar, old + new_members, int(parse_vt(vt).timestamp()))
                fr["raw_bytes"] = (fr.get("raw_bytes") or 0) + sum(len(b) for _, b in new_members)
                fr["tiles_requested"] = (fr.get("tiles_requested") or 0) + len(fine)
                if grid is not None:
                    u, cols = self._apply_fine(grid, product, parent, fine, table)
                    fr["unknown_color_pixels"] = (fr.get("unknown_color_pixels") or 0) + u
                    for c, n in cols.items():
                        fr["unknown_colors"][c] = fr["unknown_colors"].get(c, 0) + n
                fr["fine_pending"] = [t for t in fr["fine_pending"] if t[0] != tile]
                fr["fine_backlog_done"].append(tile)
                fr["zoom_raw"] = product.fine_zoom
                stats["tiles_done"] += 1
                touched = True
                if grid is not None:
                    sp.save_grid(grid_path, grid)
                    fr["nonzero_pixels"] = int(np.count_nonzero((grid > 0) & (grid < MISSING)))
                self._save_day(product, day, ds)
            if touched:
                stats["frames_touched"] += 1
            del grid
            if time.monotonic() >= deadline:
                break
        return stats

    def settle(self, product: Product, st: dict) -> int:
        """幀定案：補抓清單清空，或已超出上游保留期（剩下的記 fine_unfetched）。定案後才累加 T1、送最終 DB 列。"""
        ws = self._window.get(product.name)
        base = self.spool_root / product.name
        n = 0
        if ws is None or not base.exists():
            return n
        for dd in sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit()):
            ds = self._day_state(product, dd.name)
            changed = False
            for vt in sorted(ds["frames"]):
                fr = ds["frames"][vt]
                if fr["status"] == "missing" or fr.get("settled"):
                    continue
                if fr.get("fine_pending") and parse_vt(vt) >= ws:
                    continue
                fr["fine_unfetched"] = fr.get("fine_unfetched", []) + [t for t, _ in fr.get("fine_pending", [])]
                fr["fine_pending"] = []
                fr["settled"] = True
                if fr.get("grid"):
                    acc = self._acc(product, dd.name)
                    if acc.needs_rebuild():
                        acc.rebuild({v: p for v, p in self._grids(product, dd.name, ds).items()
                                     if ds["frames"][v].get("settled")})
                    else:
                        acc.apply(vt, sp.load_grid(dd / "grids" / f"{vt}.npz"))
                st["pending_frames"][vt] = self.frame_row(product, vt, fr)
                changed = True
                n += 1
            if changed:
                self._save_day(product, dd.name, ds)
        return n

    # ───────── DB rows ─────────
    @staticmethod
    def frame_row(product: Product, vt: str, fr: dict) -> dict:
        iso = parse_vt(vt).isoformat()
        return {
            "_type": "frame", "source": SOURCE, "product": product.name, "valid_time": iso,
            "base_time": parse_vt(fr["base_time"]).isoformat() if fr.get("base_time") else None,
            "zoom_raw": fr.get("zoom_raw"), "tiles_requested": fr.get("tiles_requested"),
            "tiles_nonempty": fr.get("tiles_nonempty"), "raw_bytes": fr.get("raw_bytes"),
            "nonzero_pixels": fr.get("nonzero_pixels"), "unknown_color_pixels": fr.get("unknown_color_pixels"),
            "status": fr["status"],
        }

    # ───────── 一個產品一輪 ─────────
    def fetch_product(self, product: Product, now: datetime, budget_frames: int) -> dict:
        st = self._load_state(product.name)
        from collectors.global_climate.jma_common import fetch
        items = fetch(self._session, product.target_times)
        latest = latest_analysis(product, items)
        if latest is None:
            raise RuntimeError(f"{product.name}: targetTimes 沒有解析時刻")
        latest_t = parse_vt(latest)
        if st["epoch"] is None:
            st["epoch"] = fmt_vt(latest_t - self.initial_lookback)
        epoch = parse_vt(st["epoch"])
        window_start = max(epoch, latest_t - product.retention + RETENTION_MARGIN)
        closed = set(st["closed_days"])

        # 1) 超出保留期仍沒抓到 → missing 定案
        missing_new = 0
        if window_start > epoch:
            for vt in expected_vts(product, epoch, window_start - timedelta(seconds=1)):
                day = jst_date(vt)
                if day in closed:
                    continue
                ds = self._day_state(product, day)
                if vt in ds["frames"]:
                    continue
                ds["frames"][vt] = {"status": "missing"}
                self._save_day(product, day, ds)
                st["pending_frames"][vt] = self.frame_row(product, vt, {"status": "missing"})
                missing_new += 1

        # 2) 保留期內補抓（oldest-first、每輪上限）
        fetched = unavailable = requests = 0
        day_cache: dict[str, dict] = {}
        for vt in expected_vts(product, window_start, latest_t):
            day = jst_date(vt)
            if day in closed:
                continue
            ds = day_cache.setdefault(day, self._day_state(product, day))
            if vt in ds["frames"]:
                continue
            if fetched >= budget_frames:
                break
            fr = self.fetch_frame(product, vt)
            if fr is None:
                unavailable += 1
                requests += 1
                continue
            requests += fr["tiles_requested"]
            ds["frames"][vt] = fr
            self._save_day(product, day, ds)
            st["pending_frames"][vt] = self.frame_row(product, vt, fr)
            fetched += 1

        self._window[product.name] = window_start
        self._epoch[product.name] = epoch
        self._save_state(product.name, st)
        return {"latest": latest, "fetched": fetched, "unavailable": unavailable,
                "missing_finalized": missing_new, "requests": requests}

    def finish_product(self, product: Product, now: datetime) -> dict:
        """定案幀（累加 T1）→ 小時收尾（T2）→ 日結（T1＋T3）。"""
        st = self._load_state(product.name)
        settled = self.settle(product, st)
        closures = self.close_ready(product, st, self._epoch[product.name], self._window[product.name], now)
        self._save_state(product.name, st)
        return {"settled": settled, **closures}

    def collect_product(self, product: Product, now: datetime, budget_frames: int,
                        backlog_seconds: float = 0) -> dict:
        """單一產品完整一輪（測試與手動用）：抓取 → 補抓段 → 定案／收尾。"""
        out = self.fetch_product(product, now, budget_frames)
        if backlog_seconds:
            out["backlog"] = self.run_backlog(time.monotonic() + backlog_seconds)
        out.update(self.finish_product(product, now))
        return out

    def _acc(self, product: Product, day: str) -> sp.DayAccumulator:
        x0, y0, nx, ny = canvas(product.grid_zoom)
        return sp.DayAccumulator(self.day_dir(product, day) / "acc", (ny * 256, nx * 256),
                                 self.table(product).thresholds)

    def _grids(self, product: Product, day: str, ds: dict) -> dict[str, Path]:
        d = self.day_dir(product, day) / "grids"
        return {vt: d / f"{vt}.npz" for vt, fr in ds["frames"].items() if fr.get("grid")}

    def _georef(self, product: Product) -> dict:
        x0, y0, nx, ny = canvas(product.grid_zoom)
        return sp.georef(product.grid_zoom, x0, y0, nx * 256, ny * 256)

    # ───────── 收尾 ─────────
    def close_ready(self, product: Product, st: dict, epoch: datetime, window_start: datetime,
                    now: datetime) -> dict:
        out = {"hours_closed": 0, "days_closed": [], "days_failed": []}
        base = self.spool_root / product.name
        if not base.exists():
            return out
        for dd in sorted(p for p in base.iterdir() if p.is_dir() and p.name.isdigit()):
            day = dd.name
            ds = self._day_state(product, day)
            d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=JST)
            exp = expected_vts(product, max(d0, epoch), d0 + timedelta(days=1) - timedelta(seconds=1))
            def final(vt, ds=ds):
                fr = ds["frames"].get(vt)
                return fr is not None and (fr["status"] == "missing" or bool(fr.get("settled")))
            # 小時（JST）
            if product.decodes and self.s3() is not None:
                hours: dict[str, list[str]] = {}
                for vt in exp:
                    hours.setdefault(parse_vt(vt).astimezone(JST).strftime("%H"), []).append(vt)
                for hh, vts in sorted(hours.items()):
                    if hh in ds["hours"] or not all(final(v) for v in vts):
                        continue
                    if self.close_hour(product, day, hh, vts, ds):
                        out["hours_closed"] += 1
                    self._save_day(product, day, ds)
            if now.astimezone(JST) < d0 + timedelta(days=1) + CLOSE_AFTER:
                continue
            if not all(final(v) for v in exp):
                if now.astimezone(JST) > d0 + timedelta(days=2):
                    print(f"[{self.name}] ⚠ {product.name} {day} spool 超過 2 天仍未齊（等上游保留期到期定案 missing）")
                continue
            if self.s3() is None:
                print(f"[{self.name}] ⚠ S3 未設定，{product.name} {day} 無法日結")
                continue
            ok = self.close_day(product, day, ds, exp, st)
            (out["days_closed"] if ok else out["days_failed"]).append(day)
        return out

    def close_hour(self, product: Product, day: str, hh: str, vts: list[str], ds: dict) -> bool:
        dd = self.day_dir(product, day)
        frames = [(vt, dd / "grids" / f"{vt}.npz" if ds["frames"][vt].get("grid") else None) for vt in vts]
        if not any(p for _, p in frames):
            ds["hours"][hh] = {"key": None, "bytes": 0, "frames": 0}
            return True
        x0, y0, nx, ny = canvas(product.grid_zoom)
        local = dd / "t2" / f"{hh}.npz"
        meta = {"source": SOURCE, "product": product.name, "date_jst": day, "hour_jst": hh,
                "decode_table_version": self.table(product).version, "georef": self._georef(product),
                "levels": {str(k): v for k, v in self.table(product).labels.items()},
                "nodata": {"0": "無", "255": "缺測"}, "license": "気象庁ホームページ（PDL1.0）を加工"}
        info = sp.write_hour_npz(local, frames, (ny * 256, nx * 256), meta,
                                 compression=config.JAPAN_JMA_RASTER_GRID_COMPRESSION)
        size = info["bytes"]
        key = f"weather-grid/{SOURCE}/{product.name}/{day}/{hh}.npz"
        sha = sp.sha256_file(local)
        if not self._put_verified(local, key, sha, "STANDARD"):
            return False
        ds["hours"][hh] = {"key": key, "bytes": size, "sha256": sha,
                           **{k: info[k] for k in ("array", "zero", "same_as_prev", "missing", "raw_bytes")}}
        local.unlink(missing_ok=True)
        return True

    def _put_verified(self, local: Path, key: str, sha: str, storage_class: str) -> bool:
        """head 已存在且 sha 相符 → 跳過；否則上傳後 head 驗 ContentLength＋sha＋StorageClass。"""
        s3 = self.s3()
        size = local.stat().st_size

        def good(h):
            return (h is not None and h["ContentLength"] == size and h["Metadata"].get("sha256") == sha
                    and h["StorageClass"] == storage_class)
        try:
            if good(s3.head(key)):
                return True
            if not s3.upload_path(local, key, storage_class=storage_class,
                                  metadata={"sha256": sha, "bytes": size},
                                  content_type="application/x-tar" if key.endswith(".tar") else "application/octet-stream"):
                return False
            return good(s3.head(key))
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ S3 {key}: {e}")
            return False

    def close_day(self, product: Product, day: str, ds: dict, exp: list[str], st: dict) -> bool:
        dd = self.day_dir(product, day)
        frames = ds["frames"]
        ok_vts = sorted(v for v in exp if frames[v]["status"] in ("ok", "partial"))
        table = self.table(product) if product.decodes else None
        row = {"_type": "daily", "source": SOURCE, "product": product.name,
               "obs_date": f"{day[:4]}-{day[4:6]}-{day[6:]}", "frames_expected": len(exp),
               "frames_ok": len(ok_vts), "raw_key": None, "raw_bytes": None, "raw_sha256": None,
               "raw_storage_class": "DEEP_ARCHIVE",
               "grid_objects": sum(1 for h in ds["hours"].values() if h.get("key")),
               "grid_bytes": sum(h.get("bytes", 0) for h in ds["hours"].values()),
               "summary_key": None, "decode_table_version": table.version if table else None,
               "status": "failed", "uploaded_at": None}
        try:
            # T1
            if table is not None and ds["t1"] is None:
                acc = self._acc(product, day)
                grids = self._grids(product, day, ds)
                if acc.needs_rebuild() or acc.applied != set(grids):
                    acc.rebuild(grids)
                local = dd / "t1.npz"
                meta = {"source": SOURCE, "product": product.name, "date_jst": day,
                        "decode_table_version": table.version, "georef": self._georef(product),
                        "thresholds": table.thresholds, "frames_expected": len(exp),
                        "valid_frames": len(grids), "levels": {str(k): v for k, v in table.labels.items()},
                        "nodata": {"max_level=255": "全日缺測"}, "license": "気象庁ホームページ（PDL1.0）を加工"}
                acc.write_summary(local, valid_frames=len(grids), meta=meta)
                key = f"weather-daily/{SOURCE}/{product.name}/{day[:4]}/{day}.npz"
                sha = sp.sha256_file(local)
                if not self._put_verified(local, key, sha, "STANDARD"):
                    raise RuntimeError("T1 上傳／驗證失敗")
                ds["t1"] = {"key": key, "bytes": local.stat().st_size, "sha256": sha}
                self._save_day(product, day, ds)
            if ds["t1"]:
                row["summary_key"] = ds["t1"]["key"]
                row["grid_bytes"] += ds["t1"]["bytes"]
            # T3
            local = dd / f"{day}.tar"
            manifest = {
                "source": SOURCE, "product": product.name, "date_jst": day, "timezone": "Asia/Tokyo",
                "url_template": f"{JMA_BOSAI_BASE}/{product.url}", "cadence_min": product.cadence_min,
                "coarse_zoom": product.coarse_zoom, "fine_zoom": product.fine_zoom,
                "canvas_coarse": canvas(product.coarse_zoom),
                "decode_table_version": table.version if table else None,
                "frames": [{"valid_time": vt, **{k: v for k, v in frames[vt].items() if k != "grid"}}
                           for vt in exp],
                "missing": [vt for vt in exp if frames[vt]["status"] == "missing"],
                "license": "気象庁ホームページ（PDL1.0）",
            }
            tars = [dd / "frames" / f"{vt}.tar" for vt in ok_vts]
            info = sp.pack_day_tar(local, tars, manifest, int(datetime.strptime(day, "%Y%m%d").timestamp()))
            key = f"weather-raw/{SOURCE}/{product.name}/{day[:4]}/{day[4:6]}/{day}.tar"
            if not self._put_verified(local, key, info["sha256"], "DEEP_ARCHIVE"):
                raise RuntimeError("T3 上傳／驗證失敗")
            print(f"[{self.name}] {product.name} {day} T3 去重：{info['tiles']} 磚 {info['bytes_before']:,}B → "
                  f"{info['unique_blobs']} blob {info['bytes_blobs']:,}B（blank {info['blank']}、empty {info['empty']}）")
            row.update(raw_key=key, raw_bytes=info["bytes"], raw_sha256=info["sha256"], status="verified",
                       uploaded_at=datetime.now(timezone.utc).isoformat())
            sp.write_json_atomic(self.spool_root / "_receipts" / product.name / f"{day}.json",
                                 {**row, "files": info["files"], "dedup": {k: info[k] for k in (
                                     "tiles", "blank", "empty", "unique_blobs", "bytes_before", "bytes_blobs")},
                                  "hours": ds["hours"], "t1": ds["t1"]})
            shutil.rmtree(dd)
            st["closed_days"].append(day)
            st["pending_daily"][day] = row
            return True
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ {product.name} {day} 日結失敗（下輪重試）: {e}")
            st["pending_daily"][day] = row
            return False

    # ───────── 主流程 ─────────
    def collect(self) -> dict:
        now = datetime.now(timezone.utc)
        per: dict[str, dict] = {}
        failures: dict[str, str] = {}
        self._requests = 0
        t0 = time.monotonic()
        for product in self.products:
            budget = 2 * max(1, self.interval_minutes // product.cadence_min) + 2
            try:
                per[product.name] = self.fetch_product(product, now, budget)
            except Exception as e:  # noqa: BLE001
                failures[product.name] = str(e)
                print(f"[{self.name}] ✗ {product.name}: {e}")
        backlog = {}
        try:
            backlog = self.run_backlog(t0 + config.JAPAN_JMA_RASTER_BACKLOG_MAX_SECONDS)
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ✗ backlog: {e}")
        for product in self.products:
            if product.name in failures or product.name not in self._window:
                continue
            try:
                per[product.name].update(self.finish_product(product, now))
            except Exception as e:  # noqa: BLE001
                failures[product.name] = str(e)
                print(f"[{self.name}] ✗ {product.name} 收尾: {e}")
        rows: list[dict] = []
        commit: dict[str, dict] = {}
        for product in self.products:
            st = self._load_state(product.name)
            rows.extend(st["pending_frames"].values())
            rows.extend(st["pending_daily"].values())
            commit[product.name] = {"frames": list(st["pending_frames"]), "daily": list(st["pending_daily"])}
        self._pending_commit = commit
        result = {"data": rows, "products": per, "source_failures": failures,
                  "requests": sum(v.get("requests", 0) for v in per.values()),
                  "http_attempts": self._requests, "backlog": backlog, "peak_rss_mb": sp.peak_rss_mb()}
        if self.products and len(failures) == len(self.products):
            raise RuntimeError(f"jma_raster: 全部產品失敗 {failures}")
        if failures:
            result["_collector_error"] = f"jma_raster: 部分產品失敗 {failures}"
        return result
