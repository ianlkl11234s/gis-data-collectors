"""CMEMS 海洋模式 daily 收集器

資料來源：Copernicus Marine Service / Mercator Ocean GLO12（1/12°）
  端點：copernicusmarine subset SDK
  Dataset：
    - cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i (海流 uo/vo)
    - cmems_mod_glo_phy-thetao_anfc_0.083deg_PT6H-i (海溫 thetao)
    - cmems_mod_glo_wav_anfc_0.083deg_PT3H-i (波浪 VHM0)
  範圍：台灣 bbox 117-126°E × 19-27°N，surface only

寫入：
  - live.global_climate_grids (一個 NetCDF time slice = 一筆 row)
  - 原檔上傳 S3 → s3_uri
"""

from __future__ import annotations

import io
import json
import math
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.s3 import S3Storage

# Dataset 設定
CMEMS_DATASETS = [
    {
        "id":            "cmems_currents",
        "dataset_id":    "cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i",
        "variables":     ["uo", "vo"],
        "depth_range":   (0, 1),
    },
    {
        "id":            "cmems_sst",
        "dataset_id":    "cmems_mod_glo_phy-thetao_anfc_0.083deg_PT6H-i",
        "variables":     ["thetao"],
        "depth_range":   (0, 1),
    },
    {
        "id":            "cmems_waves",
        "dataset_id":    "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
        "variables":     ["VHM0"],
        "depth_range":   None,
    },
]

BBOX_TAIWAN = {"min_lon": 90, "max_lon": 180, "min_lat": -15, "max_lat": 55}  # 廣域西太+東南亞+中太 90°×70°（前為西太 100-160/0-45；再前台灣 117-126/19-27）


def _to_number(v) -> Optional[float]:
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


class CmemsCollector(BaseCollector):
    """CMEMS 海洋模式 daily 收集器（CMEMS account_only）。"""

    name = "global_climate_cmems"
    interval_minutes = config.GLOBAL_CLIMATE_CMEMS_INTERVAL
    COLLECT_TIMEOUT = 1800  # subset 可能 10+ 分鐘

    def __init__(self):
        super().__init__()
        if not (config.COPERNICUSMARINE_SERVICE_USERNAME and config.COPERNICUSMARINE_SERVICE_PASSWORD):
            raise ValueError(
                "CMEMS 需要 COPERNICUSMARINE_SERVICE_USERNAME + _PASSWORD env"
            )
        os.environ.setdefault("COPERNICUSMARINE_SERVICE_USERNAME", config.COPERNICUSMARINE_SERVICE_USERNAME)
        os.environ.setdefault("COPERNICUSMARINE_SERVICE_PASSWORD", config.COPERNICUSMARINE_SERVICE_PASSWORD)
        try:
            self._s3 = S3Storage()
        except Exception:
            self._s3 = None

    def _subset(self, ds_cfg: dict, out_dir: Path) -> Optional[Path]:
        """copernicusmarine subset → NetCDF；回傳檔案路徑。"""
        out_file = out_dir / f"{ds_cfg['id']}.nc"
        # ⚠ 時間範圍必帶：不帶會抓整段 anfc 時間軸（多年 analysis + 10 天 forecast），
        # 60°×45° bbox 下單檔爆到 18.7GB（2026-07-02 事故）。只取今日 00Z 起 +48h。
        t0 = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        t1 = t0 + timedelta(hours=48)
        cmd = [
            "copernicusmarine", "subset",
            "--dataset-id", ds_cfg["dataset_id"],
            "--minimum-longitude", str(BBOX_TAIWAN["min_lon"]),
            "--maximum-longitude", str(BBOX_TAIWAN["max_lon"]),
            "--minimum-latitude", str(BBOX_TAIWAN["min_lat"]),
            "--maximum-latitude", str(BBOX_TAIWAN["max_lat"]),
            "--start-datetime", t0.strftime("%Y-%m-%dT%H:%M:%S"),
            "--end-datetime", t1.strftime("%Y-%m-%dT%H:%M:%S"),
            "-o", str(out_dir),
            "--output-filename", out_file.name,
            "--force-download",
        ]
        for v in ds_cfg["variables"]:
            cmd += ["--variable", v]
        if ds_cfg["depth_range"]:
            cmd += [
                "--minimum-depth", str(ds_cfg["depth_range"][0]),
                "--maximum-depth", str(ds_cfg["depth_range"][1]),
            ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=900)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            stderr = getattr(e, "stderr", b"")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            print(f"[cmems] subset 失敗 {ds_cfg['id']}: {stderr[:300]}")
            return None
        return out_file if out_file.exists() else None

    def _compute_digest_and_rows(self, ds_cfg: dict, nc_file: Path, s3_uri: str,
                                  collected_at: datetime) -> list[dict]:
        """xarray 開 NetCDF，每個 time slice 一筆 digest row。"""
        import xarray as xr  # 延遲 import 避免 collector 載入時 fail
        rows: list[dict] = []
        ds = xr.open_dataset(nc_file)
        try:
            times = ds["time"].values
            for t in times:
                t_iso = np.datetime_as_string(t, unit="s") + "Z"
                slice_ds = ds.sel(time=t)
                digest = {}
                for var in ds_cfg["variables"]:
                    if var not in slice_ds:
                        continue
                    arr = slice_ds[var].values
                    if arr.ndim > 2:
                        arr = arr.reshape(-1)
                    valid = arr[~np.isnan(arr)] if np.issubdtype(arr.dtype, np.floating) else arr
                    if valid.size == 0:
                        digest[var] = None
                        continue
                    digest[var] = {
                        "max":   _to_number(np.max(valid)),
                        "min":   _to_number(np.min(valid)),
                        "avg":   _to_number(np.mean(valid)),
                        "count": int(valid.size),
                        "nan_count": int(arr.size - valid.size),
                    }
                rows.append({
                    "dataset_id":     ds_cfg["id"],
                    "observed_at":    t_iso,
                    "init_at":        t_iso,         # CMEMS analysis = observed_at
                    "leadtime_hr":    0,
                    "bbox_min_lon":   BBOX_TAIWAN["min_lon"],
                    "bbox_max_lon":   BBOX_TAIWAN["max_lon"],
                    "bbox_min_lat":   BBOX_TAIWAN["min_lat"],
                    "bbox_max_lat":   BBOX_TAIWAN["max_lat"],
                    "digest":         json.dumps(digest, ensure_ascii=False),
                    "s3_uri":         s3_uri,
                    "pmtiles_uri":    None,
                    "raw_size_bytes": nc_file.stat().st_size,
                    "collected_at":   collected_at.isoformat(),
                })
        finally:
            ds.close()
        return rows

    def _upload_s3(self, nc_file: Path, ds_cfg: dict, collected_at: datetime) -> Optional[str]:
        if not self._s3:
            return None
        date_str = collected_at.strftime("%Y/%m/%d")
        key = f"{self.name}/{date_str}/{ds_cfg['id']}_{collected_at.strftime('%H%M')}.nc"
        try:
            self._s3.upload_file(nc_file, key)
            return f"s3://{self._s3.bucket}/{key}"
        except Exception as e:
            print(f"[cmems] S3 upload 失敗 {ds_cfg['id']}: {e}")
            return None

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        all_rows: list[dict] = []
        details = []

        with tempfile.TemporaryDirectory(prefix="cmems_") as tmpdir:
            tmp = Path(tmpdir)
            for ds_cfg in CMEMS_DATASETS:
                nc_file = self._subset(ds_cfg, tmp)
                if not nc_file:
                    details.append({"dataset": ds_cfg["id"], "status": "skip"})
                    continue
                s3_uri = self._upload_s3(nc_file, ds_cfg, now)
                rows = self._compute_digest_and_rows(ds_cfg, nc_file, s3_uri, now)
                all_rows.extend(rows)
                details.append({
                    "dataset": ds_cfg["id"],
                    "rows":    len(rows),
                    "s3":      s3_uri,
                    "size":    nc_file.stat().st_size,
                })

        return {
            "data":         all_rows,
            "datasets":     details,
            "row_count":    len(all_rows),
            "collected_at": now.isoformat(),
        }
