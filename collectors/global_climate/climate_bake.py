"""全球氣候 texture 烤圖 collector（GC-2）。

把 GFS 風場 GRIB2 / CMEMS 海流 NetCDF / CAMS 沙塵 NetCDF 的最新「實況場」
（newest cycle 的 f000 analysis）萃取成前端粒子動畫 / raster 用的 RGBA PNG + meta JSON，
上傳到 s3://<bucket>/deploy-assets/climate/{name}_latest.{png,json}。

與 mini-taiwan-pulse/scripts/preprocess/extract_climate_uv.py 同一套編碼約定
（前端 climateParticleLineLayer.ts / useDustForecastLayer.ts 解碼），差異：
- 選檔改「最新 cycle 的 f000」而非「observed_at 最遠」→ 顯示當下實況，非 +120h 預報
- 不再鏡像到 public/climate/（container 內無此路徑），只上傳 S3 deploy-assets

PNG 編碼（風場 / 海流）：R=u、G=v、B=0、A=255 valid / 0 無效；min/max 在同名 .json。
沙塵：預烤棕色色階（與前端 climateRamps.ts DUST_BAKE_STOPS 同值）+ alpha=強度。

⚠ 依賴：xarray + cfgrib（GRIB2）+ netCDF4（.nc）+ Pillow + numpy（見 requirements.txt）。
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.s3 import S3Storage

S3_DEPLOY_PREFIX = "deploy-assets/climate"

# 沙塵 AOD 棕色色階控制點（t, r, g, b）— 與前端 climateRamps.ts DUST_BAKE_STOPS 同值
DUST_STOPS = [
    (0.00, 218, 197, 168),
    (0.20, 180, 140, 90),
    (0.45, 132, 86, 46),
    (0.75, 82, 46, 22),
    (1.00, 40, 20, 10),
]


class ClimateBakeCollector(BaseCollector):
    """全球氣候 texture 烤圖（GFS 風場 / CMEMS 海流 / CAMS 沙塵）。"""

    name = "global_climate_bake"
    interval_minutes = config.GLOBAL_CLIMATE_BAKE_INTERVAL
    COLLECT_TIMEOUT = 600  # 下載 + xarray 解析較久

    def __init__(self):
        super().__init__()
        try:
            self._s3 = S3Storage()
        except Exception as e:
            print(f"[climate_bake] S3Storage 初始化失敗（無 S3 無法烤圖）: {e}")
            self._s3 = None

    # ── Supabase：取最新 cycle 的 f000 analysis ──
    def _latest_analysis(self, dataset_id: str) -> Optional[tuple[str, str]]:
        """回傳 (s3_uri, valid_at_iso)；挑 newest init_at + 最小 leadtime = 當下實況。"""
        import psycopg2

        with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT s3_uri, observed_at
                  FROM realtime.global_climate_grids
                 WHERE dataset_id = %s AND s3_uri IS NOT NULL
                 ORDER BY init_at DESC NULLS LAST, leadtime_hr ASC NULLS LAST
                 LIMIT 1
                """,
                (dataset_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return row[0], row[1].isoformat()

    def _download(self, s3_uri: str, dest: Path) -> None:
        assert s3_uri.startswith("s3://")
        bucket, key = s3_uri[len("s3://"):].split("/", 1)
        self._s3.s3.download_file(bucket, key, str(dest))

    def _upload(self, png_path: Path, json_path: Path, base_name: str) -> None:
        for path, name, ctype in [
            (png_path, f"{base_name}.png", "image/png"),
            (json_path, f"{base_name}.json", "application/json"),
        ]:
            key = f"{S3_DEPLOY_PREFIX}/{name}"
            self._s3.s3.upload_file(
                str(path), self._s3.bucket, key,
                ExtraArgs={"ContentType": ctype, "CacheControl": "public, max-age=3600"},
            )
            print(f"[climate_bake] ↑ s3://{self._s3.bucket}/{key}")

    # ── UV → RGBA PNG（風場 / 海流共用）──
    @staticmethod
    def _encode_uv(u: np.ndarray, v: np.ndarray, out_png: Path) -> dict:
        from PIL import Image

        u_arr = np.asarray(u, dtype=np.float32)
        v_arr = np.asarray(v, dtype=np.float32)
        mask = np.isfinite(u_arr) & np.isfinite(v_arr)
        u_min = float(np.nanmin(u_arr)) if mask.any() else -1.0
        u_max = float(np.nanmax(u_arr)) if mask.any() else 1.0
        v_min = float(np.nanmin(v_arr)) if mask.any() else -1.0
        v_max = float(np.nanmax(v_arr)) if mask.any() else 1.0

        def enc(arr, lo, hi):
            if hi - lo < 1e-9:
                return np.full(arr.shape, 128, dtype=np.uint8)
            return np.clip((arr - lo) / (hi - lo) * 255.0, 0, 255).astype(np.uint8)

        r = enc(np.where(mask, u_arr, 0), u_min, u_max)
        g = enc(np.where(mask, v_arr, 0), v_min, v_max)
        b = np.zeros_like(r)
        a = np.where(mask, 255, 0).astype(np.uint8)
        Image.fromarray(np.stack([r, g, b, a], axis=-1), "RGBA").save(out_png, format="PNG", optimize=True)
        h, w = u_arr.shape
        return {
            "width": w, "height": h,
            "u_min": u_min, "u_max": u_max, "v_min": v_min, "v_max": v_max,
            "valid_pct": float(mask.sum()) / mask.size * 100,
        }

    @staticmethod
    def _lon_to_180(arr: np.ndarray, lons: np.ndarray):
        """0..360 經度重排成 -180..180。回傳 (arr, lons)。"""
        if lons.max() > 180:
            shift = int(np.searchsorted(lons, 180.0))
            arr = np.concatenate([arr[:, shift:], arr[:, :shift]], axis=1)
            lons = np.concatenate([lons[shift:] - 360, lons[:shift]])
        return arr, lons

    # ── 三個 dataset 的處理 ──
    def _bake_wind(self, tmp: Path) -> dict:
        import xarray as xr

        sel = self._latest_analysis("gfs_wind10m")
        if not sel:
            raise RuntimeError("no s3_uri for gfs_wind10m")
        s3_uri, valid_at = sel
        local = tmp / "wind10m.grib2"
        self._download(s3_uri, local)
        ds = xr.open_dataset(
            local, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 10}},
        )
        u, v = ds["u10"].values, ds["v10"].values
        lats, lons = ds["latitude"].values, ds["longitude"].values
        u, lons2 = self._lon_to_180(u, lons)
        v, _ = self._lon_to_180(v, lons)
        lons = lons2
        bbox = [float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())]
        if lats[0] < lats[-1]:
            u, v = np.flipud(u), np.flipud(v)
        out_png, out_json = tmp / "wind10m.png", tmp / "wind10m.json"
        meta = self._encode_uv(u, v, out_png)
        meta.update({"dataset": "gfs_wind10m", "valid_at": valid_at, "source_s3": s3_uri, "bbox": bbox})
        out_json.write_text(json.dumps(meta, indent=2))
        self._upload(out_png, out_json, "wind10m_latest")
        return {"dataset": "gfs_wind10m", "valid_at": valid_at, "valid_pct": round(meta["valid_pct"], 1)}

    def _bake_currents(self, tmp: Path) -> dict:
        import xarray as xr

        sel = self._latest_analysis("cmems_currents")
        if not sel:
            raise RuntimeError("no s3_uri for cmems_currents")
        s3_uri, valid_at = sel
        local = tmp / "currents.nc"
        self._download(s3_uri, local)
        ds = xr.open_dataset(local)
        uo, vo = ds["uo"], ds["vo"]
        for dim in ("depth", "time"):
            if dim in uo.dims:
                uo, vo = uo.isel({dim: 0}), vo.isel({dim: 0})
        u, v = uo.values, vo.values
        lats = ds["latitude"].values if "latitude" in ds else ds["lat"].values
        lons = ds["longitude"].values if "longitude" in ds else ds["lon"].values
        bbox = [float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())]
        if lats[0] < lats[-1]:
            u, v = np.flipud(u), np.flipud(v)
        out_png, out_json = tmp / "currents.png", tmp / "currents.json"
        meta = self._encode_uv(u, v, out_png)
        meta.update({"dataset": "cmems_currents", "valid_at": valid_at, "source_s3": s3_uri, "bbox": bbox})
        out_json.write_text(json.dumps(meta, indent=2))
        self._upload(out_png, out_json, "currents_latest")
        return {"dataset": "cmems_currents", "valid_at": valid_at, "valid_pct": round(meta["valid_pct"], 1)}

    def _bake_dust(self, tmp: Path) -> dict:
        import xarray as xr
        from PIL import Image

        sel = self._latest_analysis("cams_dust")
        if not sel:
            raise RuntimeError("no s3_uri for cams_dust")
        s3_uri, valid_at = sel
        local = tmp / "dust.nc"
        self._download(s3_uri, local)
        ds = xr.open_dataset(local)
        da = ds["duaod550"]
        for dim in ("forecast_reference_time", "forecast_period", "time"):
            if dim in da.dims:
                da = da.isel({dim: 0})
        arr = np.asarray(da.values, dtype=np.float32)
        lats = ds["latitude"].values if "latitude" in ds else ds["lat"].values
        lons = ds["longitude"].values if "longitude" in ds else ds["lon"].values
        arr, lons = self._lon_to_180(arr, lons)
        bbox = [float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())]
        if lats[0] < lats[-1]:
            arr = np.flipud(arr)

        mask = np.isfinite(arr)
        valid = arr[mask]
        dust_min = float(valid.min()) if mask.any() else 0.0
        dust_max = float(valid.max()) if mask.any() else 1.0
        t = np.where(mask, np.clip((arr - dust_min) / max(dust_max - dust_min, 1e-9), 0, 1), 0)

        rgb = np.zeros((*t.shape, 3), dtype=np.uint8)
        for i in range(len(DUST_STOPS) - 1):
            t0, r0, g0, b0 = DUST_STOPS[i]
            t1, r1, g1, b1 = DUST_STOPS[i + 1]
            band = (t >= t0) & (t <= t1)
            if not band.any():
                continue
            lt = np.clip((t - t0) / max(t1 - t0, 1e-9), 0, 1)
            rgb[..., 0] = np.where(band, r0 + (r1 - r0) * lt, rgb[..., 0])
            rgb[..., 1] = np.where(band, g0 + (g1 - g0) * lt, rgb[..., 1])
            rgb[..., 2] = np.where(band, b0 + (b1 - b0) * lt, rgb[..., 2])
        alpha = (np.where(mask, np.clip(t * 1.4, 0, 1), 0) * 255).astype(np.uint8)
        rgba = np.concatenate([rgb, alpha[..., None]], axis=-1)

        out_png, out_json = tmp / "dust.png", tmp / "dust.json"
        Image.fromarray(rgba, "RGBA").save(out_png, format="PNG", optimize=True)
        meta = {
            "dataset": "cams_dust", "valid_at": valid_at, "source_s3": s3_uri, "bbox": bbox,
            "width": int(arr.shape[1]), "height": int(arr.shape[0]),
            "dust_min": dust_min, "dust_max": dust_max,
        }
        out_json.write_text(json.dumps(meta, indent=2))
        self._upload(out_png, out_json, "dust_latest")
        return {"dataset": "cams_dust", "valid_at": valid_at, "dust_max": round(dust_max, 3)}

    def collect(self) -> dict:
        if not self._s3:
            raise RuntimeError("S3Storage 未初始化")
        if not config.SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL 未設定")

        baked, failed = [], []
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            for label, fn in [("wind", self._bake_wind), ("currents", self._bake_currents), ("dust", self._bake_dust)]:
                try:
                    baked.append(fn(tmp))
                    print(f"[climate_bake] ✓ {label}")
                except Exception as e:
                    failed.append({"dataset": label, "error": str(e)})
                    print(f"[climate_bake] ✗ {label}: {e}")

        # 全失敗才算這輪失敗（讓 base.run 記 error）；部分成功照常回報
        if not baked:
            raise RuntimeError(f"全部烤圖失敗: {failed}")
        # 不含 'data' key → base.run() 不會寫 storage / Supabase（本 collector 只產 deploy-assets）
        return {"baked": baked, "failed": failed, "ok_count": len(baked)}
