#!/usr/bin/env python3
"""歷史 analysis（f000）回填：從**自家 S3 原始檔**重建 live.global_climate_grids 的實況 row。

## 為什麼需要

live.global_climate_grids 唯一鍵是 (dataset_id, observed_at)，過去搭 ON CONFLICT
DO NOTHING：舊 cycle 的長 leadtime 預報先佔住某個 valid-time，新 cycle 的 f000 實況
被靜默拒絕 → 近 14 天 leadtime_hr=0 幾乎零筆，前端時間軸拉到過去只剩「N 天前 cycle
的 +120h 預報」冒充實況。storage/supabase_tables.py 已改 do_update，但那只讓「從今以後」
累積得起來；過去 14 天仍需回填。

好消息：**S3 上的原始檔還在，而且內容其實是對的**。GFS 每個 valid-time key 會被多個
cycle 依序覆寫，正常情況下最後一個寫入者就是該日 cycle 的 f000（實測 2026-08-12 的
gfs_wind10m_0000.grib2 內嵌 step=0），只是 DB row 的 metadata 停在第一個寫入者
（+120h）身上。本腳本重讀檔案、以**內嵌 metadata** 修正 row，再把對應的 analysis 幀
補烤進 frames manifest。

## ⚠ 判定原則（本腳本的核心約束）

S3 key 不可信：
  - GFS key 用 **valid time**，同一 key 被多個 cycle 覆寫，且上傳有間歇性失敗
    （實測 08-14 那輪 84h/102h/108h 缺席，那些 key 還留著更早 run 的內容）。
  - CAMS / CMEMS key 用 **collected_at（台北時間！）**，跟 valid time 差最多 +1 天
    （例：cams/2026/08/14/cams_forecast_0001.nc 其實是 UTC 08-13 16:01 收的，init=08-12）。

所以唯一可信依據是**檔案內嵌 metadata**：
  - GFS GRIB2  → cfgrib coords `time`(=init) / `step`(=leadtime) / `valid_time`
  - CAMS  .nc  → `forecast_reference_time`(=init) / `forecast_period` / `valid_time`
  - CMEMS .nc  → `time` 軸（無 forecast_reference_time，見下方誠實說明）

**不用**檔名、S3 LastModified、或現有 DB row 推斷 leadtime。LastModified 只在
「兩個檔案 metadata 完全等價」時當 tie-break（會印出來）。寧可缺幀也不標錯。

## CMEMS 的誠實限制

copernicusmarine subset 產出的 .nc **不帶 forecast_reference_time**，檔案本身無法證明
某個 time slice 是「同化後的 analysis」還是「短期 nowcast」。本腳本採用可由 metadata
證明的最強敘述：對目標日 D，只收「時間軸第一格 == D 00Z」的檔案（= t0 就是 D 00Z 那次
subset 的最前緣，離真實時間最近的一格），並照 cmems.py 既有慣例記 leadtime_hr=0。
這**不等於**證明它是 analysis —— 報告請照實說「nowcast，非嚴格 analysis」。

## 用法

    # 本機無 venv，用有 xarray/cfgrib/netCDF4 的 3.12
    /usr/local/bin/python3 scripts/backfill_climate_f000.py                    # dry-run（預設）
    /usr/local/bin/python3 scripts/backfill_climate_f000.py --days 14 --datasets wind,dust
    /usr/local/bin/python3 scripts/backfill_climate_f000.py --execute          # 真的寫（需 user 拍板）

預設 **dry-run**：只做唯讀 SELECT + 下載到 cache，不寫 DB、不上傳任何 S3 物件。

## 依賴

xarray + cfgrib(eccodes) + h5netcdf/netCDF4 + Pillow + numpy + boto3 + psycopg2。
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import psycopg2

import config
from collectors.base import TAIPEI_TZ
from collectors.global_climate import climate_bake as cb
from collectors.global_climate.cams import CamsCollector
from collectors.global_climate.cmems import CMEMS_DATASETS, CmemsCollector
from collectors.global_climate.noaa_gfs import GFS_VARIABLES, NoaaGfsCollector
from storage.s3 import S3Storage

WIND_VAR = next(v for v in GFS_VARIABLES if v["id"] == "gfs_wind10m")
CURRENTS_CFG = next(d for d in CMEMS_DATASETS if d["id"] == "cmems_currents")

# dataset key → (grids dataset_id, S3 collector prefix, 檔名 pattern, frames manifest 名)
DATASETS = {
    "wind":     ("gfs_wind10m",    "global_climate_noaa_gfs", "gfs_wind10m_0000.grib2", "wind10m"),
    "currents": ("cmems_currents", "global_climate_cmems",    "cmems_currents_",        "currents"),
    "dust":     ("cams_dust",      "global_climate_cams",     "cams_forecast_",         "dust"),
}


# ── S3 lazy range reader（只給 CMEMS 掃描用）────────────────────────────────
class S3BlockFile(io.RawIOBase):
    """S3 物件的 seekable read-only file object，按 block 抓 Range 並快取。

    CMEMS 單檔 65MB，掃描階段只需要 time/lat/lon 座標（幾 KB）。整包下載 14 檔 = 0.9GB，
    在慢速鏈路上不可行。h5netcdf 吃 file-like object，配這個 reader 就只抓 header 附近的
    block。**不是另寫一套 NetCDF 解析** —— 解析仍然是 xarray/h5netcdf 做的。
    """

    def __init__(self, s3c, bucket: str, key: str, size: int, blk: int = 32 * 1024):
        self._s3, self._b, self._k, self._size, self._blk = s3c, bucket, key, size, blk
        self._pos = 0
        self._cache: dict[int, bytes] = {}
        self.bytes_read = 0
        self.n_req = 0

    def readable(self) -> bool:  return True
    def seekable(self) -> bool:  return True
    def writable(self) -> bool:  return False
    def tell(self) -> int:       return self._pos

    def seek(self, off: int, whence: int = 0) -> int:
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self._size + off)
        return self._pos

    def _block(self, i: int) -> bytes:
        if i not in self._cache:
            start = i * self._blk
            end = min(start + self._blk, self._size) - 1
            body = self._s3.get_object(Bucket=self._b, Key=self._k,
                                       Range=f"bytes={start}-{end}")["Body"].read()
            self._cache[i] = body
            self.bytes_read += len(body)
            self.n_req += 1
        return self._cache[i]

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = self._size - self._pos
        n = min(n, self._size - self._pos)
        if n <= 0:
            return b""
        out, pos, rem = bytearray(), self._pos, n
        while rem > 0:
            blk = self._block(pos // self._blk)
            off = pos % self._blk
            take = min(rem, len(blk) - off)
            if take <= 0:
                break
            out += blk[off:off + take]
            pos += take
            rem -= take
        self._pos = pos
        return bytes(out)

    def readinto(self, b) -> int:
        d = self.read(len(b))
        b[:len(d)] = d
        return len(d)


# ── 掃描結果 ────────────────────────────────────────────────────────────────
@dataclass
class Source:
    """一個 S3 原始檔的判定結果。"""
    dataset: str                       # wind / currents / dust
    key: str
    size: int
    last_modified: datetime
    init_at: Optional[datetime] = None
    leadtime_hr: Optional[int] = None
    observed_at: Optional[datetime] = None
    usable: bool = False
    reason: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def s3_uri(self) -> str:
        return f"s3://{S3_BUCKET}/{self.key}"


S3_BUCKET = ""  # main() 填入


def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc)


def _np_to_dt(v) -> datetime:
    """numpy datetime64 → tz-aware UTC datetime。"""
    return datetime.fromisoformat(np.datetime_as_string(v, unit="s")).replace(tzinfo=timezone.utc)


def _day(dt: datetime) -> str:
    return _utc(dt).strftime("%Y-%m-%d")


# ── S3 掃描 ────────────────────────────────────────────────────────────────
def list_keys(s3, prefix_root: str, days: list[datetime], name_pat: str) -> list[tuple[str, int, datetime]]:
    out = []
    for d in days:
        pref = f"{prefix_root}/{d.strftime('%Y/%m/%d')}/"
        resp = s3.s3.list_objects_v2(Bucket=s3.bucket, Prefix=pref, MaxKeys=1000)
        for o in resp.get("Contents", []):
            if name_pat in o["Key"].rsplit("/", 1)[-1]:
                out.append((o["Key"], o["Size"], o["LastModified"]))
    return sorted(out)


# ── 判定：讀內嵌 metadata ───────────────────────────────────────────────────
def inspect_wind(local: Path, src: Source, gfs) -> None:
    """GRIB2：cfgrib coords time(init) / step(leadtime) / valid_time。"""
    ds = gfs._open_grib(local, WIND_VAR["filter_keys"])
    try:
        init = _np_to_dt(ds["time"].values)
        step_ns = int(np.asarray(ds["step"].values).astype("timedelta64[ns]").astype("int64"))
        lt = step_ns // 3_600_000_000_000
        valid = _np_to_dt(ds["valid_time"].values)
        src.extra["grid"] = f"{ds.sizes.get('longitude')}x{ds.sizes.get('latitude')}"
    finally:
        ds.close()
    src.init_at, src.leadtime_hr, src.observed_at = init, int(lt), valid
    if lt != 0:
        src.reason = f"step={lt}h ≠ 0（此 key 留著更早 run 的內容，該日 f000 未成功上傳）"
    elif valid.hour != 0:
        src.reason = f"valid_time {valid:%H}Z ≠ 00Z（climate_bake._plan_wind_frames 只收 00Z）"
    else:
        src.usable = True


def inspect_dust(local: Path, src: Source) -> None:
    """CAMS .nc：forecast_reference_time(init) / forecast_period / valid_time。"""
    import xarray as xr

    ds = xr.open_dataset(local)
    try:
        frt = ds.get("forecast_reference_time")
        fp = ds.get("forecast_period")
        vt = ds.get("valid_time")
        if frt is None or fp is None:
            src.reason = "缺 forecast_reference_time / forecast_period，無法判定 leadtime"
            return
        init = _np_to_dt(np.asarray(frt.values).ravel()[0])
        hours = [int(np.timedelta64(v, "h").astype(int)) for v in np.atleast_1d(fp.values)]
        src.extra["leadtimes"] = hours
        src.extra["has_duaod550"] = "duaod550" in ds.data_vars
        if 0 not in hours:
            src.reason = f"forecast_period={hours} 不含 0，此檔無 f000 slice"
            return
        idx = hours.index(0)
        valid = _np_to_dt(np.asarray(vt.values).ravel()[idx]) if vt is not None else init
        src.init_at, src.leadtime_hr, src.observed_at = init, 0, valid
        if not src.extra["has_duaod550"]:
            src.reason = "檔內無 duaod550 變數"
            return
        src.usable = True
    finally:
        ds.close()


def inspect_currents(fileobj, src: Source) -> None:
    """CMEMS .nc：無 forecast_reference_time，只能用 time 軸第一格（見模組 docstring）。"""
    import xarray as xr

    ds = xr.open_dataset(fileobj, engine="h5netcdf")
    try:
        times = [_np_to_dt(t) for t in np.atleast_1d(ds["time"].values)]
        src.extra["time_axis"] = [t.strftime("%m-%dT%HZ") for t in times]
        src.extra["n_slices"] = len(times)
        src.extra["vars"] = list(ds.data_vars)
        # 檔案自帶的 bulletin/production 線索（若上游有給就一併印出來當佐證）
        for k in ("bulletin_date", "bulletin_type", "forecast_range", "product",
                  "title", "date_created", "history"):
            if k in ds.attrs:
                src.extra[f"attr_{k}"] = str(ds.attrs[k])[:120]
        if not times:
            src.reason = "time 軸為空"
            return
        t0 = times[0]
        src.init_at, src.leadtime_hr, src.observed_at = t0, 0, t0
        if t0.hour != 0:
            src.reason = f"time[0]={t0:%H}Z ≠ 00Z（不是 daily 00Z 那次 subset）"
            return
        if "uo" not in ds.data_vars or "vo" not in ds.data_vars:
            src.reason = f"缺 uo/vo（vars={list(ds.data_vars)}）"
            return
        src.usable = True
    finally:
        ds.close()


# ── 下載 / 快取 ────────────────────────────────────────────────────────────
def cached_download(s3, key: str, cache_dir: Path) -> Path:
    dest = cache_dir / key.replace("/", "_")
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    s3.s3.download_file(s3.bucket, key, str(tmp))
    tmp.rename(dest)
    return dest


# ── 規劃 ────────────────────────────────────────────────────────────────────
def scan(s3, which: str, days: list[datetime], cache_dir: Path, workers: int) -> list[Source]:
    """列 S3 → 平行下載 → **序列**讀 metadata。

    ⚠ 解析一定要序列：eccodes / HDF5 在多執行緒下不保證安全，平行開檔會偶發 segfault。
    慢的是下載（實測跨區鏈路 ~40 KB/s 上限），那段才平行。
    """
    _, prefix, pat, _ = DATASETS[which]
    listed = list_keys(s3, prefix, days, pat)
    srcs = [Source(dataset=which, key=k, size=sz, last_modified=lm) for k, sz, lm in listed]
    if not srcs:
        return srcs

    if which == "currents":
        # 65MB/檔 → 只用 Range 讀 header（實測 2 個 request / 55KB / 3s），不整包下載
        for src in srcs:
            try:
                f = S3BlockFile(s3.s3, s3.bucket, src.key, src.size)
                inspect_currents(f, src)
                src.extra["scan_bytes"] = f.bytes_read
                src.extra["scan_reqs"] = f.n_req
            except Exception as e:
                src.reason = f"讀檔失敗: {type(e).__name__} {str(e)[:120]}"
        return srcs

    def fetch(src: Source):
        try:
            src.extra["local"] = str(cached_download(s3, src.key, cache_dir))
        except Exception as e:
            src.reason = f"下載失敗: {type(e).__name__} {str(e)[:120]}"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(fetch, srcs))

    gfs = object.__new__(NoaaGfsCollector) if which == "wind" else None
    for src in srcs:
        if "local" not in src.extra:
            continue
        try:
            local = Path(src.extra["local"])
            if which == "wind":
                inspect_wind(local, src, gfs)
            else:
                inspect_dust(local, src)
        except Exception as e:
            src.reason = f"讀檔失敗: {type(e).__name__} {str(e)[:120]}"
    return srcs


def pick_per_day(srcs: list[Source], window: tuple[datetime, datetime]) -> dict[str, Source]:
    """每個 UTC 日挑一個可用來源；同日多檔以 LastModified 較新者勝（會印出來）。"""
    lo, hi = window
    best: dict[str, Source] = {}
    for s in srcs:
        if not s.usable or s.observed_at is None:
            continue
        if not (lo <= s.observed_at <= hi):
            s.reason = s.reason or f"observed_at {s.observed_at:%Y-%m-%d %HZ} 落在回填窗口外"
            continue
        d = _day(s.observed_at)
        prev = best.get(d)
        if prev is None or s.last_modified > prev.last_modified:
            if prev is not None:
                s.extra["tiebreak_over"] = prev.key
            best[d] = s
    return best


def fetch_db_rows(dataset_ids: list[str], lo: datetime, hi: datetime) -> dict[tuple[str, str], dict]:
    """唯讀：抓窗口內既有 row，供 dry-run diff。key = (dataset_id, iso observed_at)。"""
    out: dict[tuple[str, str], dict] = {}
    with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset_id, observed_at, init_at, leadtime_hr, s3_uri, collected_at
              FROM live.global_climate_grids
             WHERE dataset_id = ANY(%s) AND observed_at BETWEEN %s AND %s
            """,
            (dataset_ids, lo, hi),
        )
        for ds, obs, init, lt, uri, ca in cur.fetchall():
            out[(ds, _utc(obs).isoformat())] = {
                "observed_at": obs, "init_at": init, "leadtime_hr": lt,
                "s3_uri": uri, "collected_at": ca,
            }
    return out


# ── 寫入（--execute）────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO live.global_climate_grids
  (dataset_id, observed_at, init_at, leadtime_hr, digest, s3_uri, raw_size_bytes, collected_at)
VALUES (%(dataset_id)s, %(observed_at)s, %(init_at)s, %(leadtime_hr)s,
        %(digest)s::jsonb, %(s3_uri)s, %(raw_size_bytes)s, %(collected_at)s)
ON CONFLICT (dataset_id, observed_at) DO UPDATE SET
  init_at        = EXCLUDED.init_at,
  leadtime_hr    = EXCLUDED.leadtime_hr,
  digest         = COALESCE(EXCLUDED.digest, live.global_climate_grids.digest),
  s3_uri         = EXCLUDED.s3_uri,
  raw_size_bytes = EXCLUDED.raw_size_bytes,
  collected_at   = EXCLUDED.collected_at
"""
# ⚠ bbox 刻意不在 UPDATE SET 內：既有 row 的 bbox 由 collector 常數寫入，回填不該動它。
#   （若某日真的沒有既有 row，INSERT 會留 bbox NULL —— 目前窗口內每個 valid-time 都已被
#     舊預報佔住，實務上走的都是 UPDATE 分支；dry-run 報告會標出 INSERT 的筆數。）


def compute_digest(which: str, local: Path, src: Source) -> Optional[str]:
    """用 collector 既有的 digest code path 重算（不另寫一套）。"""
    if which == "wind":
        gfs = object.__new__(NoaaGfsCollector)
        d = gfs._compute_digest(local, WIND_VAR)
        return json.dumps(d, ensure_ascii=False) if d else None
    if which == "dust":
        rows = CamsCollector._compute_digest_and_rows(
            None, local, src.s3_uri, src.init_at, datetime.now(TAIPEI_TZ))
        for r in rows:
            if r["dataset_id"] == "cams_dust" and r["leadtime_hr"] == 0:
                return r["digest"]
        return None
    rows = CmemsCollector._compute_digest_and_rows(
        None, CURRENTS_CFG, local, src.s3_uri, datetime.now(TAIPEI_TZ))
    target = _utc(src.observed_at).isoformat()
    for r in rows:
        if r["observed_at"].replace("Z", "+00:00") == target:
            return r["digest"]
    return rows[0]["digest"] if rows else None


def spec_of(which: str, src: Source) -> dict:
    """組成 climate_bake._bake_*_frame 認得的 spec。"""
    spec = {
        "t": src.observed_at, "s3_uri": src.s3_uri, "kind": "analysis",
        "init_at": src.init_at, "leadtime": src.leadtime_hr or 0,
    }
    if which == "currents":
        spec["slice_time"] = src.observed_at
    return spec


def load_raw_manifest(s3) -> dict:
    key = f"{cb.S3_FRAMES_PREFIX}/manifest.json"
    try:
        obj = s3.s3.get_object(Bucket=s3.bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"version": 1, "datasets": {}}


def execute(s3, plan: dict[str, dict[str, Source]], cache_dir: Path, do_bake: bool) -> None:
    bake = object.__new__(cb.ClimateBakeCollector)
    bake._s3 = s3
    now_tpe = datetime.now(TAIPEI_TZ)
    manifest = load_raw_manifest(s3)
    manifest.setdefault("datasets", {})
    touched_frames = 0

    with tempfile.TemporaryDirectory(prefix="bf_f000_") as td:
        tmp = Path(td)
        conn = psycopg2.connect(config.SUPABASE_DB_URL)
        try:
            for which, per_day in plan.items():
                grids_id, _, _, frame_ds = DATASETS[which]
                new_entries: dict[str, dict] = {}
                for day in sorted(per_day):
                    src = per_day[day]
                    local = cached_download(s3, src.key, cache_dir)   # currents 這時才整包抓
                    digest = compute_digest(which, local, src)
                    with conn.cursor() as cur:
                        cur.execute(UPSERT_SQL, {
                            "dataset_id":     grids_id,
                            "observed_at":    src.observed_at,
                            "init_at":        src.init_at,
                            "leadtime_hr":    src.leadtime_hr,
                            "digest":         digest,
                            "s3_uri":         src.s3_uri,
                            "raw_size_bytes": src.size,
                            "collected_at":   now_tpe,
                        })
                    conn.commit()
                    print(f"  [db] {grids_id} {day} ← {src.key}")

                    if not do_bake:
                        continue
                    spec = spec_of(which, src)
                    stamp = bake._stamp(spec["t"])
                    png_rel = f"{frame_ds}/{stamp}.png"
                    baker = {"wind": bake._bake_wind_frame,
                             "currents": bake._bake_currents_frame,
                             "dust": bake._bake_dust_frame}[which]
                    try:
                        new_entries[stamp] = baker(spec, tmp, {src.s3_uri: local}, png_rel)
                        touched_frames += 1
                        print(f"  [bake] {frame_ds}/{stamp}.png")
                    except Exception as e:
                        print(f"  [bake] ✗ {frame_ds}/{stamp}: {type(e).__name__} {str(e)[:160]}")

                if do_bake and new_entries:
                    blk = manifest["datasets"].setdefault(frame_ds, {"frames": []})
                    merged = {f["png"].rsplit("/", 1)[-1].replace(".png", ""): f
                              for f in blk.get("frames", [])}
                    merged.update(new_entries)          # 回填幀覆蓋同 stamp 的舊 entry
                    blk["frames"] = sorted(merged.values(), key=lambda e: e["t"])
                    if which == "dust":
                        blk.update(cb.DUST_SCALE_META)  # 固定 sqrt 色階說明
        finally:
            conn.close()

        if do_bake and touched_frames:
            bake._upload_manifest(manifest["datasets"])
    print(f"\n完成：DB {sum(len(v) for v in plan.values())} 筆、frames {touched_frames} 張")


# ── 報告 ────────────────────────────────────────────────────────────────────
def _evidence(s: Source) -> str:
    """單檔的內嵌 metadata 判定證據（給報告 / --evidence 用）。"""
    if s.init_at is None:
        return f"讀不出 metadata（{s.reason or 'n/a'}）"
    parts = [f"init={_utc(s.init_at):%Y-%m-%d %HZ}",
             f"leadtime={s.leadtime_hr}h",
             f"valid={_utc(s.observed_at):%Y-%m-%d %HZ}" if s.observed_at else "valid=?"]
    for k in ("leadtimes", "n_slices", "grid", "time_axis"):
        if k in s.extra:
            parts.append(f"{k}={s.extra[k]}")
    return "  ".join(parts)


def dump_evidence(all_srcs: dict[str, list[Source]]) -> None:
    print(f"\n{'=' * 78}\n【逐檔證據】每個 S3 原始檔實際讀到的內嵌 metadata\n{'=' * 78}")
    for which, srcs in all_srcs.items():
        print(f"\n-- {which} --")
        for s in sorted(srcs, key=lambda x: x.key):
            mark = "✓" if s.usable else "✗"
            print(f" {mark} {s.key}")
            print(f"     size={s.size / 1e6:.2f}MB  LastModified={_utc(s.last_modified):%Y-%m-%d %H:%M:%S}Z"
                  f"（僅供參考，非判定依據）")
            print(f"     {_evidence(s)}")
            for k, v in s.extra.items():
                if k.startswith("attr_"):
                    print(f"     nc attr {k[5:]} = {v}")
            if not s.usable:
                print(f"     ✗ {s.reason}")


def report(plan: dict[str, dict[str, Source]], all_srcs: dict[str, list[Source]],
           db: dict[tuple[str, str], dict], window: tuple[datetime, datetime],
           manifest: dict) -> None:
    lo, hi = window
    want_days = []
    d = lo
    while d <= hi:
        want_days.append(_day(d))
        d += timedelta(days=1)

    for which, per_day in plan.items():
        grids_id, _, _, frame_ds = DATASETS[which]
        print(f"\n{'=' * 78}\n【{which}】→ live.global_climate_grids dataset_id='{grids_id}'"
              f"   frames/{frame_ds}/\n{'=' * 78}")
        srcs = all_srcs[which]
        print(f"S3 掃描 {len(srcs)} 檔，判定可用 {sum(1 for s in srcs if s.usable)} 檔，"
              f"落在窗口內 {len(per_day)} 天\n")
        print(f"{'目標日(UTC)':<12} {'動作':<7} {'新 init':<17} {'lt':>3}  "
              f"{'DB 現況 (init / lt)':<26} 來源 key")
        print("-" * 118)
        n_ins = n_upd = n_same = 0
        for day in want_days:
            src = per_day.get(day)
            if src is None:
                continue
            cur = db.get((grids_id, _utc(src.observed_at).isoformat()))
            if cur is None:
                action, n_ins = "INSERT", n_ins + 1
                old = "(無 row)"
            elif cur["leadtime_hr"] == src.leadtime_hr and cur["s3_uri"] == src.s3_uri \
                    and cur["init_at"] and _utc(cur["init_at"]) == _utc(src.init_at):
                action, n_same = "已正確", n_same + 1
                old = f"{_utc(cur['init_at']):%m-%d %HZ} / lt={cur['leadtime_hr']}"
            else:
                action, n_upd = "UPDATE", n_upd + 1
                oi = f"{_utc(cur['init_at']):%m-%d %HZ}" if cur["init_at"] else "None"
                old = f"{oi} / lt={cur['leadtime_hr']}"
            print(f"{day:<12} {action:<7} {_utc(src.init_at):%m-%d %HZ}      "
                  f"{src.leadtime_hr:>3}  {old:<26} {src.key.rsplit('/', 2)[-2]}/"
                  f"{src.key.rsplit('/', 1)[-1]}")
            if "tiebreak_over" in src.extra:
                print(f"{'':<12} ↳ 同日多檔，以 LastModified 較新者勝（敗方 {src.extra['tiebreak_over']}）")
        print(f"\n  → INSERT {n_ins} / UPDATE {n_upd} / 已正確 {n_same}")

        missing = [d for d in want_days if d not in per_day]
        if missing:
            print(f"\n  ✗ 無可用 f000 的日子（{len(missing)} 天）：")
            for d in missing:
                print(f"     {d}")
                hit = [s for s in srcs if s.observed_at and _day(s.observed_at) == d]
                same_prefix = [s for s in srcs if f"/{d.replace('-', '/')}/" in s.key]
                if hit:
                    for c in hit:      # 有檔對到這天，但判定不可用
                        print(f"        {c.key.rsplit('/', 1)[-1]}  不可用: {c.reason or 'n/a'}")
                        print(f"          證據 {_evidence(c)}")
                elif same_prefix:      # 該日 prefix 有檔，但內嵌 metadata 指向別天
                    print(f"        該日 prefix 有 {len(same_prefix)} 檔，但內嵌 metadata 都不是這天的 f000：")
                    for c in same_prefix:
                        print(f"          {c.key.rsplit('/', 1)[-1]} → {_evidence(c)}")
                else:
                    print("        S3 該日 prefix 無檔案，且無其他檔的 metadata 指向這天"
                          "（collector 該輪未成功上傳）")

        # 幀影響
        existing = {f["png"].rsplit("/", 1)[-1].replace(".png", ""): f
                    for f in manifest.get("datasets", {}).get(frame_ds, {}).get("frames", [])}
        will_new = will_over = 0
        for src in per_day.values():
            stamp = _utc(src.observed_at).strftime("%Y%m%dT%H%MZ")
            if stamp in existing:
                will_over += 1
            else:
                will_new += 1
        print(f"\n  幀：manifest 現有 {len(existing)} 張 → 回填會新增 {will_new} 張、"
              f"覆蓋 {will_over} 張（frames/{frame_ds}/<stamp>.png）")

        dl = sum(s.size for s in per_day.values()) / 1e6
        print(f"  --execute 需下載 {dl:.1f} MB（{len(per_day)} 檔；cache 命中則 0）")


def main() -> int:
    global S3_BUCKET
    ap = argparse.ArgumentParser(description="從自家 S3 原始檔回填 f000 analysis row + frames")
    ap.add_argument("--days", type=int, default=14, help="回補過去幾天（預設 14）")
    ap.add_argument("--datasets", default="wind,currents,dust",
                    help="逗號分隔：wind,currents,dust")
    ap.add_argument("--execute", action="store_true",
                    help="真的寫 DB + 上傳 frames（預設 dry-run 什麼都不寫）")
    ap.add_argument("--no-bake", action="store_true", help="--execute 時只修 DB，不烤幀")
    ap.add_argument("--cache-dir", default=None, help="下載快取目錄（預設系統暫存）")
    ap.add_argument("--workers", type=int, default=8, help="平行下載/讀檔數")
    ap.add_argument("--margin-days", type=int, default=2,
                    help="S3 prefix 掃描前後多抓幾天（CAMS/CMEMS key 用台北時間，會錯位）")
    ap.add_argument("--evidence", action="store_true",
                    help="逐檔印出讀到的內嵌 metadata（init / leadtime / valid）")
    args = ap.parse_args()

    which_list = [w.strip() for w in args.datasets.split(",") if w.strip()]
    bad = [w for w in which_list if w not in DATASETS]
    if bad:
        print(f"未知 dataset: {bad}；可用 {list(DATASETS)}")
        return 2

    s3 = S3Storage()
    S3_BUCKET = s3.bucket
    cache_dir = Path(args.cache_dir) if args.cache_dir else Path(tempfile.gettempdir()) / "climate_f000_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    lo, hi = today - timedelta(days=args.days), today
    window_days = [lo + timedelta(days=i) for i in range(args.days + 1)]
    # GFS key 的日期 = UTC valid date，不需要 margin（多抓一天 = 多下載 2MB）；
    # CAMS/CMEMS key 的日期 = 台北時區的 collected_at，跟 valid date 會差 ±1 天。
    margin_days = [lo - timedelta(days=args.margin_days) + timedelta(days=i)
                   for i in range(args.days + 2 * args.margin_days + 1)]

    print(f"bucket = {s3.bucket} ({config.S3_REGION})")
    print(f"回填窗口（UTC valid-time）: {_day(lo)} 00Z ~ {_day(hi)} 00Z（{args.days + 1} 天）")
    print(f"S3 掃描 prefix: wind={_day(window_days[0])}~{_day(window_days[-1])}（UTC valid date）、"
          f"currents/dust={_day(margin_days[0])}~{_day(margin_days[-1])}（±{args.margin_days} 天，key 用台北時間）")
    print(f"cache = {cache_dir}")
    print(f"模式 = {'⚠ EXECUTE（會寫 DB / 上傳 S3）' if args.execute else 'dry-run（唯讀）'}\n")

    all_srcs: dict[str, list[Source]] = {}
    plan: dict[str, dict[str, Source]] = {}
    for which in which_list:
        t0 = time.time()
        scan_days = window_days if which == "wind" else margin_days
        srcs = scan(s3, which, scan_days, cache_dir, args.workers)
        all_srcs[which] = srcs
        plan[which] = pick_per_day(srcs, (lo, hi))
        print(f"[scan] {which}: {len(srcs)} 檔 / {time.time() - t0:.0f}s")

    db = fetch_db_rows([DATASETS[w][0] for w in which_list], lo, hi)
    manifest = load_raw_manifest(s3)
    report(plan, all_srcs, db, (lo, hi), manifest)
    if args.evidence:
        dump_evidence(all_srcs)

    if not args.execute:
        total = sum(len(v) for v in plan.values())
        print(f"\n{'=' * 78}\n[dry-run] 未寫入任何東西。加 --execute 才會 upsert {total} 筆 + 烤幀。")
        return 0

    print(f"\n{'=' * 78}\n⚠ EXECUTE 開始")
    print("⚠ 這支腳本會重寫 frames/manifest.json 整包。排程的 global_climate_bake"
          f"（每 {config.GLOBAL_CLIMATE_BAKE_INTERVAL} 分鐘一輪）同時在跑的話會互相覆蓋，"
          "請挑兩輪 bake 之間執行。\n")
    execute(s3, plan, cache_dir, do_bake=not args.no_bake)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
