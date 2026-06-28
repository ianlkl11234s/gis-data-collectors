"""NOAA GFS 全球風場 daily 收集器

資料來源：NOAA NCEP via AWS Open Data S3（無認證）
  Bucket：s3://noaa-gfs-bdp-pds/
  Cycle：每日 4 個（00/06/12/18 UTC），約 4 hr 延遲完整
  Path：gfs.{YYYYMMDD}/{HH}/atmos/gfs.t{HH}z.pgrb2.0p25.f{FFF}

策略：
  1. 抓 .idx 找變數 byte offset
  2. HTTP Range request 只拉需要的變數（一檔 500MB → 4MB 5 變數）
  3. cfgrib + xarray 讀

變數：
  - PRMSL (mean sea level pressure)
  - UGRD/VGRD @ 10m above ground
  - UGRD/VGRD @ 250 mb (噴流)

寫入：realtime.global_climate_grids（每 leadtime × 每變數一筆 row）
"""

from __future__ import annotations

import json
import math
import re
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.s3 import S3Storage

GFS_BASE = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"

# 要拉的變數 + GRIB2 level pattern
GFS_VARIABLES = [
    {
        "id":           "gfs_prmsl",
        "pattern":      r"PRMSL:mean sea level",
        "filter_keys":  {"typeOfLevel": "meanSea"},
        "var_name":     "prmsl",
        "unit":         "Pa",
    },
    {
        "id":           "gfs_wind10m",
        "pattern":      r"(UGRD|VGRD):10 m above ground",
        "filter_keys":  {"typeOfLevel": "heightAboveGround", "level": 10},
        "var_name":     ["u10", "v10"],
        "unit":         "m/s",
    },
    {
        "id":           "gfs_wind250hpa",
        "pattern":      r"(UGRD|VGRD):250 mb",
        "filter_keys":  {"typeOfLevel": "isobaricInhPa", "level": 250},
        "var_name":     ["u", "v"],
        "unit":         "m/s",
    },
]
GFS_LEADTIMES = [0, 24, 48, 72, 96, 120]


def _to_number(v) -> Optional[float]:
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _pick_cycle(now_utc: datetime) -> tuple[str, str]:
    """挑最新可用 cycle（往前回 4 小時為延遲緩衝）。
    Returns (date_str=YYYYMMDD, cycle_hh)
    """
    candidate = now_utc - timedelta(hours=4)
    cycle_hour = (candidate.hour // 6) * 6
    cycle_time = candidate.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    return cycle_time.strftime("%Y%m%d"), f"{cycle_hour:02d}"


class NoaaGfsCollector(BaseCollector):
    """NOAA GFS 風場 daily 收集器（無認證，AWS Open Data .idx Range pull）。"""

    name = "global_climate_noaa_gfs"
    interval_minutes = config.GLOBAL_CLIMATE_NOAA_GFS_INTERVAL
    COLLECT_TIMEOUT = 1800

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        try:
            self._s3 = S3Storage()
        except Exception:
            self._s3 = None

    def _fetch_idx(self, date_str: str, cycle: str, leadtime: int) -> Optional[list[tuple[int, str]]]:
        """抓 .idx 回傳 [(offset, line), ...]"""
        url = f"{GFS_BASE}/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f{leadtime:03d}.idx"
        try:
            resp = self._session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception:
            return None
        out = []
        for line in resp.text.splitlines():
            parts = line.split(":")
            if len(parts) < 3:
                continue
            try:
                offset = int(parts[1])
                out.append((offset, line))
            except ValueError:
                continue
        return out

    def _range_pull(self, date_str: str, cycle: str, leadtime: int,
                    idx_lines: list[tuple[int, str]], patterns: list[str],
                    out_file: Path) -> bool:
        """根據 patterns 找 byte ranges、HTTP Range pull → 寫到 out_file。"""
        url = f"{GFS_BASE}/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f{leadtime:03d}"
        ranges = []  # [(start, end_inclusive)]
        for i, (offset, line) in enumerate(idx_lines):
            if any(re.search(p, line) for p in patterns):
                # end = 下一筆 offset - 1（最後一筆無法知道，省略）
                if i + 1 < len(idx_lines):
                    end = idx_lines[i + 1][0] - 1
                else:
                    end = offset + 5_000_000  # 估計上限 5MB
                ranges.append((offset, end))

        if not ranges:
            return False
        with open(out_file, "wb") as f:
            for start, end in ranges:
                resp = self._session.get(url, headers={"Range": f"bytes={start}-{end}"}, timeout=60)
                if resp.status_code in (200, 206):
                    f.write(resp.content)
        return out_file.stat().st_size > 0

    def _open_grib(self, grib_file: Path, filter_keys: dict):
        import xarray as xr
        return xr.open_dataset(grib_file, engine="cfgrib",
                                backend_kwargs={"filter_by_keys": filter_keys, "indexpath": ""})

    def _compute_digest(self, grib_file: Path, var_cfg: dict) -> Optional[dict]:
        try:
            ds = self._open_grib(grib_file, var_cfg["filter_keys"])
        except Exception as e:
            print(f"[gfs] open {var_cfg['id']} 失敗: {str(e)[:200]}")
            return None
        digest = {}
        try:
            var_names = var_cfg["var_name"]
            if isinstance(var_names, str):
                var_names = [var_names]
            for v in var_names:
                if v not in ds:
                    continue
                arr = ds[v].values
                valid = arr[~np.isnan(arr)]
                if valid.size == 0:
                    continue
                digest[v] = {
                    "max":   _to_number(np.max(valid)),
                    "min":   _to_number(np.min(valid)),
                    "avg":   _to_number(np.mean(valid)),
                    "unit":  var_cfg["unit"],
                    "count": int(valid.size),
                }
        finally:
            ds.close()
        return digest or None

    def _upload_s3(self, grib_file: Path, dataset_id: str, valid_at: datetime) -> Optional[str]:
        if not self._s3:
            return None
        date_str = valid_at.strftime("%Y/%m/%d")
        key = f"{self.name}/{date_str}/{dataset_id}_{valid_at.strftime('%H%M')}.grib2"
        try:
            self._s3.upload_file(grib_file, key)
            return f"s3://{self._s3.bucket}/{key}"
        except Exception as e:
            print(f"[gfs] S3 upload 失敗 {dataset_id}: {e}")
            return None

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        now_utc = now.astimezone(timezone.utc)
        date_str, cycle = _pick_cycle(now_utc)
        cycle_init = datetime.strptime(f"{date_str}{cycle}", "%Y%m%d%H").replace(tzinfo=timezone.utc)

        all_rows: list[dict] = []
        bbox = {"min_lon": -180, "max_lon": 180, "min_lat": -90, "max_lat": 90}

        with tempfile.TemporaryDirectory(prefix="gfs_") as tmpdir:
            tmp = Path(tmpdir)
            for leadtime in GFS_LEADTIMES:
                idx = self._fetch_idx(date_str, cycle, leadtime)
                if not idx:
                    continue
                valid_at = cycle_init + timedelta(hours=leadtime)

                for var_cfg in GFS_VARIABLES:
                    grib_file = tmp / f"{var_cfg['id']}_f{leadtime:03d}.grib2"
                    ok = self._range_pull(
                        date_str, cycle, leadtime, idx,
                        [var_cfg["pattern"]], grib_file,
                    )
                    if not ok:
                        continue
                    digest = self._compute_digest(grib_file, var_cfg)
                    if not digest:
                        continue
                    s3_uri = self._upload_s3(grib_file, var_cfg["id"], valid_at)
                    all_rows.append({
                        "dataset_id":     var_cfg["id"],
                        "observed_at":    valid_at.isoformat(),
                        "init_at":        cycle_init.isoformat(),
                        "leadtime_hr":    leadtime,
                        "bbox_min_lon":   bbox["min_lon"],
                        "bbox_max_lon":   bbox["max_lon"],
                        "bbox_min_lat":   bbox["min_lat"],
                        "bbox_max_lat":   bbox["max_lat"],
                        "digest":         json.dumps(digest, ensure_ascii=False),
                        "s3_uri":         s3_uri,
                        "pmtiles_uri":    None,
                        "raw_size_bytes": grib_file.stat().st_size,
                        "collected_at":   now.isoformat(),
                    })

        return {
            "data":         all_rows,
            "row_count":    len(all_rows),
            "cycle":        f"{date_str}/{cycle}",
            "collected_at": now.isoformat(),
        }
