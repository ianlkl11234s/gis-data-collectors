"""CAMS 大氣化學 daily 收集器

資料來源：Copernicus Atmosphere Monitoring Service / ECMWF (ADS)
  端點：cdsapi.Client(url='https://ads.atmosphere.copernicus.eu/api', key=CAMS_API_KEY)
  Dataset：cams-global-atmospheric-composition-forecasts
  變數：pm2p5, pm10, duaod550（沙塵 AOD）
  範圍：東亞 5-50°N × 100-145°E

寫入：realtime.global_climate_grids（leadtime 多筆 row）
  - 原檔上傳 S3
  - ⚠ 排隊延遲 5-30 min（cdsapi 內部 poll）
"""

from __future__ import annotations

import io
import json
import math
import os
import tempfile
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.s3 import S3Storage

CAMS_VARIABLES = [
    "particulate_matter_2.5um",
    "particulate_matter_10um",
    "dust_aerosol_optical_depth_550nm",
]
CAMS_LEADTIMES = ["0", "24", "48", "72", "96", "120"]
BBOX_EASTASIA = {"north": 50, "south": 5, "west": 100, "east": 145}

# 變數名 → dataset_id 名（在 global_climate_grids 表）
VAR_TO_DATASET_ID = {
    "pm2p5":    "cams_pm25",
    "pm10":     "cams_pm10",
    "duaod550": "cams_dust",
}


def _to_number(v) -> Optional[float]:
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


class CamsCollector(BaseCollector):
    """CAMS 大氣化學 daily 收集器（CAMS_API_KEY，每日 1 次）。"""

    name = "global_climate_cams"
    interval_minutes = config.GLOBAL_CLIMATE_CAMS_INTERVAL
    COLLECT_TIMEOUT = 2400  # 排隊可能 30+ 分鐘

    def __init__(self):
        super().__init__()
        if not config.CAMS_API_KEY:
            raise ValueError("CAMS 需要 CAMS_API_KEY env")
        try:
            self._s3 = S3Storage()
        except Exception:
            self._s3 = None

    def _build_client(self):
        import cdsapi
        return cdsapi.Client(
            url="https://ads.atmosphere.copernicus.eu/api",
            key=config.CAMS_API_KEY,
        )

    def _retrieve(self, out_dir: Path, init_date: datetime) -> Optional[Path]:
        """ADS retrieve → netcdf_zip → 解開回傳 .nc 路徑。"""
        client = self._build_client()
        zip_file = out_dir / "cams.zip"
        date_str = init_date.strftime("%Y-%m-%d")
        try:
            client.retrieve(
                "cams-global-atmospheric-composition-forecasts",
                {
                    "variable":      CAMS_VARIABLES,
                    "date":          f"{date_str}/{date_str}",
                    "time":          "00:00",
                    "leadtime_hour": CAMS_LEADTIMES,
                    "type":          "forecast",
                    "data_format":   "netcdf_zip",
                    "area": [
                        BBOX_EASTASIA["north"],
                        BBOX_EASTASIA["west"],
                        BBOX_EASTASIA["south"],
                        BBOX_EASTASIA["east"],
                    ],
                },
                str(zip_file),
            )
        except Exception as e:
            print(f"[cams] retrieve 失敗: {str(e)[:300]}")
            return None

        # 解 zip
        try:
            with zipfile.ZipFile(zip_file) as z:
                z.extractall(out_dir)
            nc_files = list(out_dir.glob("*.nc"))
            return nc_files[0] if nc_files else None
        except Exception as e:
            print(f"[cams] zip 解壓失敗: {e}")
            return None

    def _compute_digest_and_rows(self, nc_file: Path, s3_uri: str,
                                  init_date: datetime, collected_at: datetime) -> list[dict]:
        """xarray 開 NetCDF → 每變數 × 每 leadtime 一筆 row。"""
        import xarray as xr
        rows: list[dict] = []
        ds = xr.open_dataset(nc_file)
        try:
            # CAMS NetCDF schema: forecast_period (leadtime hours) + forecast_reference_time + lat + lon
            leadtimes = ds.get("forecast_period", None)
            if leadtimes is None:
                return rows
            init_time = ds.get("forecast_reference_time")
            init_at = (np.datetime_as_string(init_time.values[0], unit="s") + "Z"
                       if init_time is not None and init_time.size > 0 else init_date.isoformat())

            for var in ds.data_vars:
                dataset_id = VAR_TO_DATASET_ID.get(var)
                if not dataset_id:
                    continue
                for lt_idx, lt in enumerate(leadtimes.values):
                    # lt 是 timedelta64[ns]
                    lt_hours = int(np.timedelta64(lt, "h").astype(int))
                    valid_time = ds.get("valid_time")
                    if valid_time is not None:
                        vt = valid_time.values.flatten()[lt_idx]
                        observed_iso = np.datetime_as_string(vt, unit="s") + "Z"
                    else:
                        # fallback
                        observed_iso = (init_date + timedelta(hours=lt_hours)).isoformat()

                    arr = ds[var].isel(forecast_period=lt_idx).values
                    valid = arr[~np.isnan(arr)]
                    if valid.size == 0:
                        continue
                    # PM 單位 kg/m³ → μg/m³（×1e9）
                    if var in ("pm2p5", "pm10"):
                        scale = 1e9
                    else:
                        scale = 1.0
                    digest = {
                        var: {
                            "max":   _to_number(np.max(valid) * scale),
                            "min":   _to_number(np.min(valid) * scale),
                            "avg":   _to_number(np.mean(valid) * scale),
                            "unit":  "μg/m³" if scale != 1 else "1",
                            "count": int(valid.size),
                            "nan_count": int(arr.size - valid.size),
                        }
                    }
                    rows.append({
                        "dataset_id":     dataset_id,
                        "observed_at":    observed_iso,
                        "init_at":        init_at,
                        "leadtime_hr":    lt_hours,
                        "bbox_min_lon":   BBOX_EASTASIA["west"],
                        "bbox_max_lon":   BBOX_EASTASIA["east"],
                        "bbox_min_lat":   BBOX_EASTASIA["south"],
                        "bbox_max_lat":   BBOX_EASTASIA["north"],
                        "digest":         json.dumps(digest, ensure_ascii=False),
                        "s3_uri":         s3_uri,
                        "pmtiles_uri":    None,
                        "raw_size_bytes": nc_file.stat().st_size,
                        "collected_at":   collected_at.isoformat(),
                    })
        finally:
            ds.close()
        return rows

    def _upload_s3(self, nc_file: Path, collected_at: datetime) -> Optional[str]:
        if not self._s3:
            return None
        date_str = collected_at.strftime("%Y/%m/%d")
        key = f"{self.name}/{date_str}/cams_forecast_{collected_at.strftime('%H%M')}.nc"
        try:
            self._s3.upload_file(nc_file, key)
            return f"s3://{self._s3.bucket}/{key}"
        except Exception as e:
            print(f"[cams] S3 upload 失敗: {e}")
            return None

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        # 拉前一天 00 UTC init（保證 forecast 完整）
        init_date = (now.astimezone(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        all_rows: list[dict] = []
        s3_uri = None
        size = 0

        with tempfile.TemporaryDirectory(prefix="cams_") as tmpdir:
            tmp = Path(tmpdir)
            nc_file = self._retrieve(tmp, init_date)
            if nc_file:
                size = nc_file.stat().st_size
                s3_uri = self._upload_s3(nc_file, now)
                all_rows = self._compute_digest_and_rows(nc_file, s3_uri, init_date, now)

        return {
            "data":           all_rows,
            "row_count":      len(all_rows),
            "init_at":        init_date.isoformat(),
            "s3_uri":         s3_uri,
            "raw_size_bytes": size,
            "collected_at":   now.isoformat(),
        }
