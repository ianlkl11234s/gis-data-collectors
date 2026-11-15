"""雷達整合回波（O-A0058-005）→ 北北 250 m 網格雨勢分級。

搬自 mini-taiwan-pulse `scripts/preprocess/sound/radar_rain_lib.py`（純函式）與
`scripts/preprocess/build-sound-radar-rain.py` 的 decode_frame／publish（--publish 每日包），常數與運算順序不改。

dBZ 碼（uint8）：0 = 無回波（透明）、1–66 = 色塊 0–65 dBZ 再加 1、255 = 無法判斷。
每日包雜波濾波需要前、後一幀（當日第一、最後一幀不判）；即時幀看不到後一幀 → `live_clutter_flag`
把「前一幀條件成立」的格先標 255（無法判斷），所以即時值只會等於每日值或是 255，不會更有把握。
"""
from __future__ import annotations

import gzip
import hashlib
import io
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from .common import GRID, GRID_CELLS, GRID_SHA256, UNKNOWN, contract_bytes, contract_json, in_scope_mask

SCHEMA = "pulse-sound-radar-rain-v1"
PRODUCT = "O-A0058-005"
BASE_URL = "https://data.itsmigu.com/imagery/cwa"
RADAR_BBOX = (115.0, 17.75, 126.5, 29.25)
RADAR_SIZE = 3600
RADAR_DEG = (RADAR_BBOX[2] - RADAR_BBOX[0]) / RADAR_SIZE
USER_AGENT = "pulse-sound-atlas-live/1.0"

RAIN_CLASSES = contract_json("rain_classes.json")
ZR_A = float(RAIN_CLASSES["zr"]["a"])
ZR_B = float(RAIN_CLASSES["zr"]["b"])
RAIN_CLASS_EDGES_MM_H = tuple(float(x) for x in RAIN_CLASSES["edgesMmPerHour"])
RAIN_CLASS_NAMES = tuple(RAIN_CLASSES["names"]["zh"])
DBZ_BIN_OFFSET = 0.5

CLUTTER_MIN_DBZ = 45.0
CLUTTER_JUMP_DB = 20.0
CLUTTER_NEIGHBOR_CELLS = 8
CLUTTER_RULE_VERSION = "clutter-v1"

PALETTE_PATH_IN_FRONTEND = "scripts/preprocess/sound/cwa-radar-palette.json"


# ── 純函式（radar_rain_lib.py）────────────────────────────────────

def dbz_to_rate(dbz: np.ndarray) -> np.ndarray:
    z = np.power(10.0, dbz / 10.0)
    return np.power(z / ZR_A, 1.0 / ZR_B)


def rate_to_class(rate: np.ndarray) -> np.ndarray:
    return np.searchsorted(np.asarray(RAIN_CLASS_EDGES_MM_H), rate, side="right").astype(np.uint8)


def codes_to_class(codes: np.ndarray) -> np.ndarray:
    """dBZ 碼 → 分級碼（0–4、255）；同 build-sound-radar-rain.py main() 的 rate/cls 兩行"""
    codes = np.asarray(codes)
    rate = np.where((codes == 0) | (codes == UNKNOWN), 0.0, dbz_to_rate(codes.astype(float) - 1 + DBZ_BIN_OFFSET))
    cls = rate_to_class(rate)
    cls[codes == UNKNOWN] = UNKNOWN
    return cls


def _neighborhood_max(a: np.ndarray, r: int) -> np.ndarray:
    if r <= 0:
        return a.copy()
    out = a.copy()
    for k in range(1, r + 1):
        out[:, k:] = np.maximum(out[:, k:], a[:, :-k])
        out[:, :-k] = np.maximum(out[:, :-k], a[:, k:])
    tmp = out.copy()
    for k in range(1, r + 1):
        out[k:, :] = np.maximum(out[k:, :], tmp[:-k, :])
        out[:-k, :] = np.maximum(out[:-k, :], tmp[k:, :])
    return out


def _as_dbz(codes: np.ndarray) -> np.ndarray:
    return np.where((codes == 0) | (codes == UNKNOWN), -np.inf, codes.astype(float) - 1)


def clutter_mask(codes: np.ndarray, frame_ok: np.ndarray, rows: int, cols: int,
                 min_dbz: float = CLUTTER_MIN_DBZ, jump_db: float = CLUTTER_JUMP_DB,
                 radius: int = CLUTTER_NEIGHBOR_CELLS) -> np.ndarray:
    """codes: (n, rows*cols) dBZ 碼；frame_ok: (n,) bool → (n, rows*cols) bool：True = 疑似雜波（radar_rain_lib.clutter_mask）"""
    # 與原版逐幀運算相同，只是不一次建 (n, rows, cols) 的 float64 陣列（144 幀 ≈ 96 MB × 2）：
    # 每幀的 dBZ 與鄰域最大值只在用到時算、只留前後各一幀。
    n = codes.shape[0]
    flag = np.zeros((n, rows * cols), dtype=bool)
    nb_cache: dict[int, np.ndarray] = {}

    def nbmax(i: int) -> np.ndarray:
        if i not in nb_cache:
            nb_cache[i] = _neighborhood_max(_as_dbz(codes[i]).reshape(rows, cols), radius)
        return nb_cache[i]

    for i in range(1, n - 1):
        for old in [k for k in nb_cache if k < i - 1]:
            del nb_cache[old]
        if not (frame_ok[i] and frame_ok[i - 1] and frame_ok[i + 1]):
            continue
        cur = _as_dbz(codes[i]).reshape(rows, cols)
        flag[i] = ((cur >= min_dbz) & (nbmax(i - 1) <= cur - jump_db) & (nbmax(i + 1) <= cur - jump_db)).reshape(-1)
    return flag


def live_clutter_flag(cur_codes: np.ndarray, prev_codes: Optional[np.ndarray], k: int, n: int = 144,
                      rows: int = GRID["rows"], cols: int = GRID["cols"]) -> np.ndarray:
    """即時幀 k 的雜波標記（看不到 k+1）：每日規則中「前一幀條件」成立就先標（後一幀未知＝當成成立）。

    每日包對 k=0、k=n-1、前一幀缺不判 → 這裡同樣不標；k+1 缺或之後才到，每日包可能不判或判定不是雜波，
    即時值因此只會「多標 255」，不會少標。
    """
    if k <= 0 or k >= n - 1 or prev_codes is None:
        return np.zeros(rows * cols, dtype=bool)
    cur = _as_dbz(cur_codes).reshape(rows, cols)
    prev_nb = _neighborhood_max(_as_dbz(prev_codes).reshape(rows, cols), CLUTTER_NEIGHBOR_CELLS)
    return ((cur >= CLUTTER_MIN_DBZ) & (prev_nb <= cur - CLUTTER_JUMP_DB)).reshape(rows * cols)


# ── 色表、重採樣、解碼（build-sound-radar-rain.py）───────────────────

def load_palette() -> tuple[dict[int, int], dict]:
    pal = contract_json("cwa_radar_palette.json")
    lut: dict[int, int] = {}
    for e in pal["entries"]:
        r, g, b = (int(e["rgb"][i:i + 2], 16) for i in (1, 3, 5))
        lut[(r << 16) | (g << 8) | b] = int(e["dbz"]) + 1
    return lut, pal


def grid_pixel_index() -> tuple[np.ndarray, np.ndarray, tuple[int, int, int, int]]:
    cols, rows = GRID["cols"], GRID["rows"]
    lon = GRID["west"] + (np.arange(cols) + 0.5) * GRID["dLon"]
    lat = GRID["north"] - (np.arange(rows) + 0.5) * GRID["dLat"]
    pc = np.floor((lon - RADAR_BBOX[0]) / RADAR_DEG).astype(int)
    pr = np.floor((RADAR_BBOX[3] - lat) / RADAR_DEG).astype(int)
    window = (int(pr.min()), int(pr.max()) + 1, int(pc.min()), int(pc.max()) + 1)
    return pr, pc, window


class FrameDecoder:
    """PNG bytes → 網格 dBZ 碼（長度 GRID_CELLS 的 uint8）；表外顏色回傳計數（同 decode_frame）"""

    def __init__(self):
        self.lut, self.palette = load_palette()
        pr, pc, self.window = grid_pixel_index()
        r0, _r1, c0, _c1 = self.window
        self.sel_r = (pr - r0)[:, None]
        self.sel_c = (pc - c0)[None, :]

    def decode(self, png: bytes) -> tuple[np.ndarray, dict[str, int], set[int]]:
        from PIL import Image
        r0, r1, c0, c1 = self.window
        im = Image.open(io.BytesIO(png))
        # 只解碼裁切窗（PIL 對 PNG 仍會整張解，但 crop 前不 convert 可省一份 RGBA 全圖）
        a = np.asarray(im.crop((c0, r0, c1, r1)).convert("RGBA"), dtype=np.uint8)
        rgb = (a[..., 0].astype(np.uint32) << 16) | (a[..., 1].astype(np.uint32) << 8) | a[..., 2]
        alpha = a[..., 3]
        out = np.full(rgb.shape, UNKNOWN, dtype=np.uint8)
        out[alpha == 0] = 0
        opaque = alpha == 255
        bad: dict[str, int] = {}
        for val in np.unique(rgb[opaque]):
            code = self.lut.get(int(val))
            sel = opaque & (rgb == val)
            if code is None:
                bad[f"#{int(val):06x}"] = int(sel.sum())
            else:
                out[sel] = code
        partial = (alpha != 0) & (alpha != 255)
        if partial.any():
            for al in np.unique(alpha[partial]):
                bad[f"alpha={int(al)}"] = int((alpha == al).sum())
        seen = {int(x) for x in np.unique(out)}
        return out[self.sel_r, self.sel_c].reshape(-1), bad, seen


# ── 抓圖 ──────────────────────────────────────────────────────────

def slots_for_taiwan_day(day: str) -> list[datetime]:
    start = datetime.fromisoformat(f"{day}T00:00:00+08:00").astimezone(timezone.utc)
    return [start + timedelta(minutes=10 * i) for i in range(144)]


def frame_key(t: datetime) -> str:
    return f"{PRODUCT}/{t:%Y%m%d}/{t:%H%M%S}.png"


def http_get(url: str, retries: int = 3, timeout: float = 30.0) -> tuple[str, Optional[bytes]]:
    """('ok', bytes)／('missing', None)（403/404）／('fetch_error', None)；同 build-sound-radar-rain.py"""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return "ok", r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return "missing", None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(1.5 * (attempt + 1))
    return "fetch_error", None


class FrameFetcher:
    """依 key 取雷達 PNG：本機快取目錄（測試）→ 公開網址（正式）。回 (status, bytes|None)。"""

    def __init__(self, cache_dir: Optional[str] = None, base_url: Optional[str] = BASE_URL,
                 getter: Callable[[str], tuple[str, Optional[bytes]]] = http_get, write_cache: bool = False):
        self.cache = Path(cache_dir) if cache_dir else None
        self.base_url = base_url
        self.getter = getter
        self.write_cache = write_cache

    def get(self, key: str) -> tuple[str, Optional[bytes]]:
        if self.cache:
            p = self.cache / key
            if p.exists():
                return "ok", p.read_bytes()
        if not self.base_url:
            return "missing", None
        st, body = self.getter(f"{self.base_url}/{key}")
        if st == "ok" and self.cache and self.write_cache:
            p = self.cache / key
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(body)  # type: ignore[arg-type]
        return st, body


# ── 每日包（build-sound-radar-rain.py --publish）──────────────────────

def build_daily(date: str, fetcher: FrameFetcher, decoder: Optional[FrameDecoder] = None,
                parallel: int = 3, generated_at: Optional[str] = None) -> tuple[bytes, dict]:
    """回 (radar-rain-<D>.class.bin 位元組, meta dict)。缺幀整幀 255，不內插。"""
    decoder = decoder or FrameDecoder()
    slots = slots_for_taiwan_day(date)
    frames_meta = [{"utc": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "key": frame_key(t)} for t in slots]

    def fetch(i: int):
        return fetcher.get(frames_meta[i]["key"])

    n = len(slots)
    dbz = np.full((n, GRID_CELLS), UNKNOWN, dtype=np.uint8)
    seen_codes: set[int] = set()
    bad_all: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        # 分批抓，避免 144 張 PNG 同時留在記憶體
        for b0 in range(0, n, parallel * 2):
            idx = list(range(b0, min(n, b0 + parallel * 2)))
            for i, (st, body) in zip(idx, ex.map(fetch, idx)):
                frames_meta[i]["status"] = st
                if st != "ok":
                    continue
                codes, bad, seen = decoder.decode(body)  # type: ignore[arg-type]
                if bad:
                    frames_meta[i]["unknown_colors"] = bad
                    for k, v in bad.items():
                        bad_all[k] = bad_all.get(k, 0) + v
                    continue
                dbz[i] = codes
                seen_codes |= seen
    if bad_all:
        raise ValueError(f"表外顏色（未硬配）：{bad_all}")
    cls = np.empty_like(dbz)
    for i in range(n):  # 逐幀算（逐元素運算，結果與整批相同；整批的 float64 暫存陣列約 100 MB × 數個）
        cls[i] = codes_to_class(dbz[i])
    missing = [i for i, f in enumerate(frames_meta) if f["status"] != "ok"]
    return publish_daily(date, frames_meta, dbz, cls, missing, seen_codes, generated_at=generated_at)


def publish_daily(date: str, frames_meta: list[dict], dbz: np.ndarray, cls: np.ndarray, missing: list[int],
                  seen_codes: set[int], generated_at: Optional[str] = None) -> tuple[bytes, dict]:
    n = dbz.shape[0]
    rows, cols = GRID["rows"], GRID["cols"]
    in_scope = in_scope_mask()
    frame_ok = np.array([f["status"] == "ok" for f in frames_meta])
    flag = clutter_mask(dbz, frame_ok, rows, cols)
    out_cls = cls.copy()
    out_cls[flag] = UNKNOWN
    per_frame = []
    for i in range(n):
        k_all = int(flag[i].sum())
        if k_all:
            k_in = int((flag[i] & in_scope).sum())
            per_frame.append({"utc": frames_meta[i]["utc"], "cells_bbox": k_all, "cells_in_scope": k_in,
                              "max_dbz": int(dbz[i][flag[i]].max()) - 1})
    payload = gzip.compress(out_cls.tobytes(), compresslevel=9, mtime=0)
    stem = f"radar-rain-{date}"
    slot0 = slots_for_taiwan_day(date)[0]
    pal = contract_json("cwa_radar_palette.json")
    meta = {
        "schema": SCHEMA,
        "date_taiwan": date,
        "product": PRODUCT,
        "generated_at": generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "grid": {k: GRID[k] for k in ("id", "west", "north", "dLon", "dLat", "cols", "rows")} | {"sha256": GRID_SHA256},
        "payload": {"file": f"{stem}.class.bin", "encoding": "gzip", "dtype": "uint8", "layout": "frame-major, frame k = cells row·cols+col",
                    "bytes": len(payload), "raw_bytes": int(out_cls.size), "sha256": hashlib.sha256(payload).hexdigest()},
        "frames": {"count": n, "step_s": 600, "first_slot_utc": slot0.strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "slot_rule": "frame k 代表 [first_slot + k·600 s, +600 s)；雷達圖時刻＝該槽起點"},
        "frames_missing": [{"index": i, "utc": frames_meta[i]["utc"], "status": frames_meta[i]["status"]} for i in missing],
        "classes": {"version": RAIN_CLASSES["version"], "edges_mm_h": list(RAIN_CLASS_EDGES_MM_H), "names_zh": list(RAIN_CLASS_NAMES), "unknown": UNKNOWN},
        "zr": {"a": ZR_A, "b": ZR_B, "note": "Marshall-Palmer，假設"},
        "palette": {"path": PALETTE_PATH_IN_FRONTEND, "retrieved": pal["source"]["retrieved"]},
        "transparent_rule": "透明像素＝無回波（假設；官方圖例未說明）",
        "clutter": {"rule": CLUTTER_RULE_VERSION, "min_dbz": CLUTTER_MIN_DBZ, "jump_db": CLUTTER_JUMP_DB, "neighbor_cells": CLUTTER_NEIGHBOR_CELLS,
                    "description": "回波 ≥ min_dbz 且前、後一幀在 ±neighbor_cells 格內最大回波都低 jump_db 以上 → 255；前或後一幀缺（含當日首末幀）不判定",
                    "cells_flagged_bbox": int(flag.sum()), "cells_flagged_in_scope": int((flag & in_scope[None, :]).sum()), "frames": per_frame},
        "dbz_codes_seen_in_window": sorted(seen_codes),
    }
    return payload, meta


def meta_bytes(meta: dict) -> bytes:
    """同前端腳本：json.dumps(meta, ensure_ascii=False, indent=1)"""
    import json
    return json.dumps(meta, ensure_ascii=False, indent=1).encode("utf-8")


def contract_palette_sha() -> str:
    return hashlib.sha256(contract_bytes("cwa_radar_palette.json")).hexdigest()
