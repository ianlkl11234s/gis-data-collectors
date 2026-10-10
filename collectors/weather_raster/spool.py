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

    def _chunk(self, name: str, dtype, plane: int, r0: int, rows: int, mode: str) -> np.memmap:
        """只映射一段列（offset 映射），讓常駐記憶體只有一個 chunk 的大小。"""
        h, w = self.shape
        item = np.dtype(dtype).itemsize
        return np.memmap(self.dir / name, dtype, mode, offset=(plane * h + r0) * w * item, shape=(rows, w))

    def apply(self, vt: str, grid: np.ndarray) -> bool:
        if vt in self.applied:
            return False
        self._ensure()
        self.marker.write_text(vt)
        h = self.shape[0]
        for r0 in range(0, h, CHUNK_ROWS):
            rows = min(CHUNK_ROWS, h - r0)
            g = grid[r0:r0 + rows]
            valid = g != MISSING
            for i, k in enumerate(self.thresholds):
                c = self._chunk("count.u16", np.uint16, i, r0, rows, "r+")
                c += ((g >= k) & valid).astype(np.uint16)
                c.flush(); del c
            mx = self._chunk("max.u8", np.uint8, 0, r0, rows, "r+")
            np.maximum(mx, np.where(valid, g, 0).astype(np.uint8), out=mx)
            mx.flush(); del mx
            ms = self._chunk("missing.u16", np.uint16, 0, r0, rows, "r+")
            ms += (~valid).astype(np.uint16)
            ms.flush(); del ms
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
        """T1：count_ge_{k}（uint16）、max_level（uint8，從未有效＝255）、missing_count、valid_frames。
        逐 chunk 串流寫進 zip，不整份載入。"""
        self._ensure()
        h, w = self.shape
        tmp = path.with_name(path.name + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        def stream(zf, name, dtype, plane, fix=None):
            with zf.open(name, "w", force_zip64=True) as f:
                np.lib.format.write_array_header_1_0(
                    f, {"descr": np.dtype(dtype).str, "fortran_order": False, "shape": self.shape})
                for r0 in range(0, h, CHUNK_ROWS):
                    rows = min(CHUNK_ROWS, h - r0)
                    a = np.array(self._chunk(name_map[name], dtype, plane, r0, rows, "r"))
                    if fix is not None:
                        a = fix(a, r0, rows)
                    f.write(a.tobytes())

        name_map = {f"count_ge_{k}.npy": "count.u16" for k in self.thresholds}
        name_map.update({"max_level.npy": "max.u8", "missing_count.npy": "missing.u16"})

        def fix_max(a, r0, rows):
            miss = np.array(self._chunk("missing.u16", np.uint16, 0, r0, rows, "r"))
            a[miss >= valid_frames] = MISSING
            return a

        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            for i, k in enumerate(self.thresholds):
                stream(zf, f"count_ge_{k}.npy", np.uint16, i)
            stream(zf, "max_level.npy", np.uint8, 0, fix_max)
            stream(zf, "missing_count.npy", np.uint16, 0)
            with zf.open("valid_frames.npy", "w") as f:
                np.lib.format.write_array(f, np.array(valid_frames, np.uint16))
            with zf.open("meta.npy", "w") as f:
                np.lib.format.write_array(f, np.array(json.dumps(meta, ensure_ascii=False, sort_keys=True)))
        tmp.replace(path)
        return path.stat().st_size


# ───────────────────────── T2：每小時網格（每幀保留、去重）─────────────────────────

ZIP_METHODS = {"deflate": zipfile.ZIP_DEFLATED, "lzma": zipfile.ZIP_LZMA, "bzip2": zipfile.ZIP_BZIP2}


def write_hour_npz(path: Path, frames: list[tuple[str, Optional[Path]]], shape: tuple[int, int],
                   meta: dict, compression: str = "deflate") -> dict:
    """一小時一檔、每幀都留（永久層，不可只留彙總）。

    frames：[(vt, grid_path 或 None＝缺幀)]，依時間排序。檔內去重：
      kind=zero          全零幀，不存陣列
      kind=same_as_prev  與前一個有效幀完全相同，記引用
      kind=missing       缺幀
      kind=array         存 f{vt}.npy（uint8 H×W；0=無、255=缺測）
    讀取用 load_hour_npz()。np.load 也可直接讀各 f{vt} 陣列與 index。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    index: list[dict] = []
    prev: Optional[np.ndarray] = None
    prev_vt: Optional[str] = None
    stats = {"array": 0, "zero": 0, "same_as_prev": 0, "missing": 0, "raw_bytes": 0}
    with zipfile.ZipFile(tmp, "w", ZIP_METHODS[compression], allowZip64=True) as zf:
        for vt, gp in frames:
            if gp is None:
                index.append({"vt": vt, "kind": "missing"}); stats["missing"] += 1
                continue
            g = load_grid(gp)
            if g.shape != shape:
                raise ValueError(f"grid shape {g.shape} != {shape}")
            if not g.any():
                index.append({"vt": vt, "kind": "zero"}); stats["zero"] += 1
            elif prev is not None and np.array_equal(g, prev):
                index.append({"vt": vt, "kind": "same_as_prev", "ref": prev_vt}); stats["same_as_prev"] += 1
            else:
                with zf.open(f"f{vt}.npy", "w", force_zip64=True) as f:
                    np.lib.format.write_array(f, np.ascontiguousarray(g, np.uint8))
                index.append({"vt": vt, "kind": "array"}); stats["array"] += 1
                stats["raw_bytes"] += g.nbytes
            if g.any():
                prev, prev_vt = g, vt
        meta = dict(meta, shape=list(shape), dtype="uint8", compression=compression)
        for name, obj in (("index.npy", index), ("meta.npy", meta)):
            with zf.open(name, "w") as f:
                np.lib.format.write_array(f, np.array(json.dumps(obj, ensure_ascii=False, sort_keys=True)))
    tmp.replace(path)
    return {**stats, "bytes": path.stat().st_size}


def load_hour_npz(path) -> list[tuple[str, Optional[np.ndarray]]]:
    """還原 write_hour_npz：回 [(vt, grid 或 None＝缺幀)]。"""
    with np.load(path) as z:
        index = json.loads(str(z["index"]))
        meta = json.loads(str(z["meta"]))
        shape = tuple(meta["shape"])
        out, cache = [], {}
        for e in index:
            k = e["kind"]
            if k == "missing":
                out.append((e["vt"], None)); continue
            if k == "zero":
                g = np.zeros(shape, np.uint8)
            elif k == "same_as_prev":
                g = cache[e["ref"]]
            else:
                g = z[f"f{e['vt']}"]
            cache[e["vt"]] = g
            out.append((e["vt"], g))
    return out


# ───────────────────────── T3：每日原始 tar（磚級去重）─────────────────────────

def _is_blank_placeholder(data: bytes) -> bool:
    """JMA 334 byte 透明 RGBA 佔位磚（或任何全透明 PNG）。"""
    if len(data) != 334:
        return False
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(data))
        return im.mode == "RGBA" and im.getextrema()[3][1] == 0
    except Exception:  # noqa: BLE001
        return False


def pack_day_tar(out_path: Path, frame_tars: Iterable[Path], manifest: dict, mtime: int) -> dict:
    """各幀 tar 的磚依 sha256 去重，唯一內容依序串接成單一成員 blobs.pack（不壓縮）；
    334B 空白佔位與 0 byte pbf 只記在 manifest。tar 成員：blobs.pack、manifest.json。

    為什麼不用每個 blob 一個 tar 成員：JMA 磚平均 <1 KB，tar 每成員 512B header＋補齊 512，
    實測 radar 一小時 0.98 MB blob 會變 2.05 MB；串成一個 pack 只多一個 header。

    manifest.json（緊湊 JSON）新增：
      tiles: {vt: {'z{z}/{x}/{y}.{ext}': blob 序號 | -1（blank）| -2（empty）}}
      blobs: [[sha256, offset, length], ...]（offset 為 blobs.pack 內位置）
      dedup: {tiles, blank, empty, unique_blobs, bytes_before, bytes_blobs}
    還原：tiles[vt][磚] → blobs[i] → blobs.pack[offset:offset+length]；blank＝JMA 334B 透明佔位（內容雜湊見 blank_sha256）。
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    pack = out_path.with_name(out_path.name + ".blobs.tmp")
    tiles: dict[str, dict[str, int]] = {}
    blobs: list[list] = []
    blob_idx: dict[str, int] = {}
    blank_shas: set[str] = set()
    st = {"tiles": 0, "blank": 0, "empty": 0, "unique_blobs": 0, "bytes_before": 0, "bytes_blobs": 0}
    with pack.open("wb") as pf:
        for ft in sorted(frame_tars):
            with tarfile.open(ft, "r") as src:
                for m in sorted((m for m in src.getmembers() if m.isfile()), key=lambda m: m.name):
                    data = src.extractfile(m).read()
                    st["tiles"] += 1
                    st["bytes_before"] += len(data)
                    _prod, vt, rest = m.name.split("/", 2)
                    slot = tiles.setdefault(vt, {})
                    if len(data) == 0:
                        slot[rest] = -2; st["empty"] += 1
                        continue
                    h = sha256_bytes(data)
                    if h in blank_shas or _is_blank_placeholder(data):
                        blank_shas.add(h)
                        slot[rest] = -1; st["blank"] += 1
                        continue
                    if h not in blob_idx:
                        blob_idx[h] = len(blobs)
                        blobs.append([h, st["bytes_blobs"], len(data)])
                        pf.write(data)
                        st["unique_blobs"] += 1
                        st["bytes_blobs"] += len(data)
                    slot[rest] = blob_idx[h]
    with tarfile.open(tmp, "w", format=tarfile.PAX_FORMAT) as out:
        with pack.open("rb") as pf:
            out.addfile(_tarinfo("blobs.pack", st["bytes_blobs"], mtime), pf)
        manifest = dict(manifest, tiles=tiles, blobs=blobs, dedup=st, blank_sha256=sorted(blank_shas),
                        layout="blobs.pack + manifest.json（tiles[vt][z/x/y.ext]→blobs[i]=[sha256,offset,length]；-1 blank、-2 empty）")
        body = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        out.addfile(_tarinfo("manifest.json", len(body), mtime), io.BytesIO(body))
    pack.unlink(missing_ok=True)
    tmp.replace(out_path)
    return {"sha256": sha256_file(out_path), "bytes": out_path.stat().st_size, "files": st["tiles"], **st}


def read_tile_from_day_tar(tar_path: Path, name: str) -> Optional[bytes]:
    """依 manifest 取回單張磚原始 bytes；blank/empty 回 b''（blank 的原始內容見 blank_sha256）。"""
    with tarfile.open(tar_path) as tf:
        man = json.loads(tf.extractfile("manifest.json").read())
        _prod, vt, rest = name.split("/", 2)
        ref = man["tiles"].get(vt, {}).get(rest)
        if ref is None:
            return None
        if ref < 0:
            return b""
        _sha, off, n = man["blobs"][ref]
        f = tf.extractfile("blobs.pack")
        f.seek(off)
        return f.read(n)


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
