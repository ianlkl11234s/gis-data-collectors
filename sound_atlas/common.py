"""共用：靜態契約、時間槽、網格外框、原始檔讀取（本機 /data 目錄或 S3 archive tar，兩者內容相同）。"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import io
import json
import math
import os
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional

CONTRACT_DIR = Path(__file__).resolve().parent / "contract"
TPE = dt.timezone(dt.timedelta(hours=8))
SLOTS_PER_DAY = 144
SLOT_S = 600
UNKNOWN = 255


# ── 契約 ──────────────────────────────────────────────────────────

def contract_bytes(name: str) -> bytes:
    return (CONTRACT_DIR / name).read_bytes()


def contract_json(name: str):
    return json.loads(contract_bytes(name))


GRID = contract_json("grid_spec.json")
GRID_SHA256 = hashlib.sha256(contract_bytes("grid_spec.json")).hexdigest()
GRID_CELLS = GRID["cols"] * GRID["rows"]


def grid_bbox() -> list[float]:
    """同 build-sound-grid-sources.py grid_bbox()"""
    south = GRID["north"] - GRID["rows"] * GRID["dLat"]
    east = GRID["west"] + GRID["cols"] * GRID["dLon"]
    return [GRID["west"], round(south, 6), round(east, 6), GRID["north"]]


def mask_spans() -> list:
    return contract_json("grid_mask_spans.json")


def in_scope_mask():
    """格是否在北北範圍內（numpy bool，長度 GRID_CELLS）"""
    import numpy as np
    out = np.zeros(GRID_CELLS, dtype=bool)
    for row, rs in enumerate(mask_spans()):
        for c_a, c_b, _ in rs:
            out[row * GRID["cols"] + c_a: row * GRID["cols"] + c_b + 1] = True
    return out


def natural_static(static_dir: Optional[str]) -> Optional[dict]:
    """coast＋river_network 靜態幾何（不進 git）；驗 sha256，不符就拒用（回 None）。"""
    if not static_dir:
        return None
    p = Path(static_dir) / "natural-static-v1.json"
    if not p.exists():
        return None
    b = p.read_bytes()
    want = contract_json("manifest.json").get("natural_static", {}).get("sha256")
    if want and hashlib.sha256(b).hexdigest() != want:
        return None
    return json.loads(b)


# ── 時間 ──────────────────────────────────────────────────────────

def day_window(date: str) -> tuple[int, int]:
    """臺灣日 [00:00, 24:00) 的 unix 秒（同 build-sound-scenes.py）"""
    d = dt.date.fromisoformat(date)
    start = int(dt.datetime(d.year, d.month, d.day, tzinfo=TPE).timestamp())
    return start, start + 86_400


def shift(date: str, days: int) -> str:
    return (dt.date.fromisoformat(date) + dt.timedelta(days=days)).isoformat()


def slot_of(date: str, unix: float) -> int:
    """unix 秒落在臺灣日 date 的第幾個 10 分槽（可能 <0 或 ≥144）"""
    return int((unix - day_window(date)[0]) // SLOT_S)


def slot_start_utc(date: str, k: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(day_window(date)[0] + k * SLOT_S, dt.timezone.utc)


def taiwan_date(unix: float) -> str:
    return dt.datetime.fromtimestamp(unix, TPE).date().isoformat()


def unix_of(s: str) -> int:
    return int(dt.datetime.fromisoformat(s).timestamp())


# ── 幾何 ──────────────────────────────────────────────────────────

def in_bbox(lon: float, lat: float, b: list[float]) -> bool:
    return b[0] <= lon <= b[2] and b[1] <= lat <= b[3]


def expand_bbox(b: list[float], km: float) -> list[float]:
    """同 build-sound-scenes.py expand_bbox（含 round 5 位）"""
    lat = (b[1] + b[3]) / 2
    dlat = km / 111.32
    dlon = km / (111.32 * math.cos(math.radians(lat)))
    return [round(b[0] - dlon, 5), round(b[1] - dlat, 5), round(b[2] + dlon, 5), round(b[3] + dlat, 5)]


def bbox_hit(a: list[float], b: list[float]) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def num(v):
    """同 build-sound-scenes.py _num：非數字、NaN、inf → None"""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


# ── 原始檔來源 ────────────────────────────────────────────────────
#
# collector 每次執行存一份 <LOCAL_DATA_DIR>/<collector>/<YYYY>/<MM>/<DD>/<collector>_<HHMM>.json（臺北時間），
# tasks/archive.py 每日把整個日期目錄壓成 S3 <collector>/archives/<YYYY-MM-DD>.tar.gz（成員檔名不變）。
# 所以「本機目錄」與「S3 tar」是同一批檔案，解析程式只寫一份。

@dataclass(frozen=True)
class RawFile:
    collector: str
    date: str          # 目錄日期（臺北時間，收集當天）
    name: str          # <collector>_<HHMM>.json
    hhmm: int          # 由檔名解析，作為抵達順序與模擬抵達時間
    loader: Callable[[], bytes]

    def json(self):
        return json.loads(self.loader())

    def arrival_unix(self) -> int:
        d = dt.date.fromisoformat(self.date)
        return int(dt.datetime(d.year, d.month, d.day, self.hhmm // 100, self.hhmm % 100, tzinfo=TPE).timestamp())


def _hhmm(name: str) -> int:
    stem = name.rsplit(".", 1)[0]
    try:
        return int(stem.rsplit("_", 1)[1])
    except (IndexError, ValueError):
        return -1


class RawSource:
    """列出某 collector 某日的原始檔：先找本機目錄，沒有再找 archive tar（本機快取或 S3 下載器）。

    local_dir：collector 的 LOCAL_DATA_DIR（正式環境 /data）
    archive_dir：放 `<collector>__archives__<D>.tar.gz` 的目錄（測試用本機原始匯出）
    archive_fetch：key → bytes|None（正式環境回補用 S3；None 表示沒有）
    """

    def __init__(self, local_dir: Optional[str] = None, archive_dir: Optional[str] = None,
                 archive_fetch: Optional[Callable[[str], Optional[bytes]]] = None):
        self.local_dir = Path(local_dir) if local_dir else None
        self.archive_dir = Path(archive_dir) if archive_dir else None
        self.archive_fetch = archive_fetch

    def _local(self, collector: str, date: str) -> Optional[list[RawFile]]:
        if not self.local_dir:
            return None
        d = self.local_dir / collector / date.replace("-", "/")
        if not d.is_dir():
            return None
        files = sorted(p for p in d.glob(f"{collector}_*.json"))
        return [RawFile(collector, date, p.name, _hhmm(p.name), (lambda p=p: p.read_bytes())) for p in files]

    def archive_bytes(self, collector: str, date: str) -> Optional[bytes]:
        key = f"{collector}/archives/{date}.tar.gz"
        if self.archive_dir:
            p = self.archive_dir / key.replace("/", "__")
            if p.exists():
                return p.read_bytes()
        if self.archive_fetch:
            return self.archive_fetch(key)
        return None

    def files(self, collector: str, date: str) -> Optional[Iterator[RawFile]]:
        """依檔名（HHMM）順序逐檔產出；完全找不到 → None（≠ 沒有檔案）。

        archive 用串流模式讀（一次只留一個成員在記憶體；VD 一天 144 檔 × 1.2 MB）；
        產出的 RawFile 要在取下一個之前讀完。tasks/archive.py 依檔名排序加入 tar，
        所以 tar 成員順序＝檔名順序＝前端建檔腳本（tf.getmembers()）的處理順序。
        """
        local = self._local(collector, date)
        if local is not None:
            return iter(local)
        raw = self.archive_bytes(collector, date)
        if raw is None:
            return None

        def gen() -> Iterator[RawFile]:
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r|gz") as tf:
                for m in tf:
                    if m.isfile() and m.name.endswith(".json"):
                        b = tf.extractfile(m).read()
                        n = os.path.basename(m.name)
                        yield RawFile(collector, date, n, _hhmm(n), (lambda b=b: b))
        return gen()


def iter_json(files: Iterable[RawFile]) -> Iterator[tuple[RawFile, dict]]:
    for f in files:
        yield f, f.json()


# ── 輸出 ──────────────────────────────────────────────────────────

def dumps_compact(doc) -> bytes:
    """同前端預處理 write_json：ensure_ascii=False、緊湊分隔"""
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def gzip_bytes(b: bytes) -> bytes:
    return gzip.compress(b, compresslevel=9, mtime=0)


def now_tpe_iso() -> str:
    return dt.datetime.now(TPE).replace(microsecond=0).isoformat()


def now_utc_z() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
