"""網格圖磚 spool：原始磚、解碼網格、每日累加器、T1／T2／T3 產物（ADR-0021）。

spool 版面（每產品每 JST 日一個目錄，日結上傳驗證後整個刪除）：
  {day_dir}/state.json              日狀態（幀、小時、T1/T2/T3 進度）
  {day_dir}/frames/{vt}.tar         該幀原始磚（不壓縮、決定性 tar）
  {day_dir}/grids/{vt}.npz          解碼後 uint8 網格（累加器重建與 T2 的來源）
  {day_dir}/acc/                    np.memmap 累加器（count_ge_k uint16、max uint8、missing uint16）

所有寫檔都先寫 .tmp 再 replace（原子），讓服務中斷後可以續算。
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import tarfile
import zipfile
from pathlib import Path
from typing import Iterable, Optional

import numpy as np

from collectors.weather_raster.decode import MISSING

CHUNK_ROWS = 512


# ───────────────────────── 小工具 ─────────────────────────

def write_json_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=1), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _tarinfo(name: str, size: int, mtime: int) -> tarfile.TarInfo:
    ti = tarfile.TarInfo(name)
    ti.size, ti.mtime, ti.mode = size, mtime, 0o644
    ti.uid = ti.gid = 0
    ti.uname = ti.gname = ""
    return ti


def write_frame_tar(path: Path, members: list[tuple[str, bytes]], mtime: int) -> None:
    """決定性 tar（同內容同 bytes），讓重打包 sha256 不變、重試時可用 head 判斷已上傳。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tarfile.open(tmp, "w", format=tarfile.PAX_FORMAT) as tf:
        for name, data in sorted(members):
            tf.addfile(_tarinfo(name, len(data), mtime), io.BytesIO(data))
    tmp.replace(path)


def save_grid(path: Path, grid: np.ndarray) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp.npz")
    np.savez_compressed(tmp, g=grid)
    tmp.replace(path)
    return path.stat().st_size


def load_grid(path: Path) -> np.ndarray:
    with np.load(path) as z:
        return z["g"]


# ───────────────────────── georef ─────────────────────────

def _merc_lon(x: float, z: int) -> float:
    return x / 2 ** z * 360.0 - 180.0


def _merc_lat(y: float, z: int) -> float:
    n = math.pi - 2 * math.pi * y / 2 ** z
    return math.degrees(math.atan(math.sinh(n)))


def georef(zoom: int, tile_x0: int, tile_y0: int, width: int, height: int) -> dict:
    x1, y1 = tile_x0 + width / 256, tile_y0 + height / 256
    half = 20037508.342789244
    to_m = lambda t: t / 2 ** zoom * 2 * half - half  # noqa: E731
    return {
        "crs": "EPSG:3857", "zoom": zoom, "tile_x0": tile_x0, "tile_y0": tile_y0,
        "width": width, "height": height, "pixel": "area; row 0 = north",
        "bounds_3857": [to_m(tile_x0), half - (to_m(y1) + half), to_m(x1), half - (to_m(tile_y0) + half)],
        "bounds_lonlat": [_merc_lon(tile_x0, zoom), _merc_lat(y1, zoom), _merc_lon(x1, zoom), _merc_lat(tile_y0, zoom)],
    }


# ───────────────────────── 每日累加器（memmap，跨重啟續算）─────────────────────────

class DayAccumulator:
    """count_ge_k／max／missing 以 np.memmap 放在 spool，不吃 RAM。

    冪等：applied.json 記已累加的 validtime；累加前寫 applying 標記，若重啟時標記還在
    （中途崩潰、累加器可能半更新），以 grids/ 下的網格重建整份累加器。
    """

    def __init__(self, acc_dir: Path, shape: tuple[int, int], thresholds: list[int]):
        self.dir = acc_dir
        self.shape = shape
        self.thresholds = list(thresholds)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.applied_path = self.dir / "applied.json"
        self.marker = self.dir / "applying"
        self.applied: set[str] = set(read_json(self.applied_path, []) or [])

    def _open(self, mode: str):
        k = max(len(self.thresholds), 1)
        h, w = self.shape
        return (np.memmap(self.dir / "count.u16", np.uint16, mode, shape=(k, h, w)),
                np.memmap(self.dir / "max.u8", np.uint8, mode, shape=(h, w)),
                np.memmap(self.dir / "missing.u16", np.uint16, mode, shape=(h, w)))

    def _ensure(self) -> None:
        if not (self.dir / "count.u16").exists():
            for arr in self._open("w+"):
                arr.flush()
                del arr

    def needs_rebuild(self) -> bool:
        return self.marker.exists()

    def apply(self, vt: str, grid: np.ndarray) -> bool:
        if vt in self.applied:
            return False
        self._ensure()
        self.marker.write_text(vt)
        count, mx, miss = self._open("r+")
        for r0 in range(0, self.shape[0], CHUNK_ROWS):
            g = grid[r0:r0 + CHUNK_ROWS]
            valid = g != MISSING
            for i, k in enumerate(self.thresholds):
                count[i, r0:r0 + CHUNK_ROWS] += ((g >= k) & valid).astype(np.uint16)
            np.maximum(mx[r0:r0 + CHUNK_ROWS], np.where(valid, g, 0).astype(np.uint8),
                       out=mx[r0:r0 + CHUNK_ROWS])
            miss[r0:r0 + CHUNK_ROWS] += (~valid).astype(np.uint16)
        for arr in (count, mx, miss):
            arr.flush()
        del count, mx, miss
        self.applied.add(vt)
        write_json_atomic(self.applied_path, sorted(self.applied))
        self.marker.unlink(missing_ok=True)
        return True

    def rebuild(self, grids: dict[str, Path]) -> None:
        for name in ("count.u16", "max.u8", "missing.u16"):
            (self.dir / name).unlink(missing_ok=True)
        self.applied = set()
        write_json_atomic(self.applied_path, [])
        self.marker.unlink(missing_ok=True)
        self._ensure()
        for vt in sorted(grids):
            self.apply(vt, load_grid(grids[vt]))

    def write_summary(self, path: Path, *, valid_frames: int, meta: dict) -> int:
        """T1：count_ge_{k}（uint16）、max_level（uint8，從未有效＝255）、missing_count、valid_frames。"""
        self._ensure()
        count, mx, miss = self._open("r")
        tmp = path.with_name(path.name + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for i, k in enumerate(self.thresholds):
                with zf.open(f"count_ge_{k}.npy", "w", force_zip64=True) as f:
                    np.lib.format.write_array(f, count[i])
            with zf.open("max_level.npy", "w", force_zip64=True) as f:
                np.lib.format.write_array_header_1_0(f, {"descr": "|u1", "fortran_order": False, "shape": self.shape})
                for r0 in range(0, self.shape[0], CHUNK_ROWS):
                    m = np.array(mx[r0:r0 + CHUNK_ROWS])
                    m[np.asarray(miss[r0:r0 + CHUNK_ROWS]) >= valid_frames] = MISSING
                    f.write(m.tobytes())
            with zf.open("missing_count.npy", "w", force_zip64=True) as f:
                np.lib.format.write_array(f, miss)
            with zf.open("valid_frames.npy", "w") as f:
                np.lib.format.write_array(f, np.array(valid_frames, np.uint16))
            with zf.open("meta.npy", "w") as f:
                np.lib.format.write_array(f, np.array(json.dumps(meta, ensure_ascii=False, sort_keys=True)))
        del count, mx, miss
        tmp.replace(path)
        return path.stat().st_size


# ───────────────────────── T2：每小時 stack ─────────────────────────

def write_stack_npz(path: Path, grids: list[tuple[str, Path]], shape: tuple[int, int], meta: dict) -> int:
    """逐幀串流寫進 zip 內的 stack.npy（n,H,W），記憶體只放一幀。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        with zf.open("stack.npy", "w", force_zip64=True) as f:
            np.lib.format.write_array_header_1_0(
                f, {"descr": "|u1", "fortran_order": False, "shape": (len(grids), *shape)})
            for _vt, p in grids:
                g = load_grid(p)
                if g.shape != shape:
                    raise ValueError(f"grid shape {g.shape} != {shape}")
                f.write(np.ascontiguousarray(g, np.uint8).tobytes())
        with zf.open("valid_times.npy", "w") as f:
            np.lib.format.write_array(f, np.array([vt for vt, _ in grids]))
        with zf.open("meta.npy", "w") as f:
            np.lib.format.write_array(f, np.array(json.dumps(meta, ensure_ascii=False, sort_keys=True)))
    tmp.replace(path)
    return path.stat().st_size


# ───────────────────────── T3：每日原始 tar ─────────────────────────

def pack_day_tar(out_path: Path, frame_tars: Iterable[Path], manifest: dict, mtime: int) -> dict:
    """把各幀 tar 的成員依序併成一個不壓縮 tar，最後放 manifest.json；回傳 {sha256, bytes, files}。

    manifest['files'] 由本函式填入（每檔 name/sha256/bytes），內容決定性。
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    files: list[dict] = []
    with tarfile.open(tmp, "w", format=tarfile.PAX_FORMAT) as out:
        for ft in sorted(frame_tars):
            with tarfile.open(ft, "r") as src:
                for m in src.getmembers():
                    if not m.isfile():
                        continue
                    data = src.extractfile(m).read()
                    out.addfile(_tarinfo(m.name, len(data), m.mtime), io.BytesIO(data))
                    files.append({"name": m.name, "sha256": sha256_bytes(data), "bytes": len(data)})
        manifest = dict(manifest, files=files)
        body = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")
        out.addfile(_tarinfo("manifest.json", len(body), mtime), io.BytesIO(body))
    tmp.replace(out_path)
    return {"sha256": sha256_file(out_path), "bytes": out_path.stat().st_size, "files": len(files)}


def peak_rss_mb() -> Optional[float]:
    try:
        import resource
        import sys
        r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(r / (1 << 20) if sys.platform == "darwin" else r / 1024, 1)
    except Exception:  # noqa: BLE001
        return None


def disk_free_mb(path: Path) -> float:
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / (1 << 20)
