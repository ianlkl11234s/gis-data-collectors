"""全球氣候 texture 烤圖 collector（GC-2）。

把 GFS 風場 GRIB2 / CMEMS 海流 NetCDF / CAMS 沙塵 NetCDF 的最新「實況場」
（newest cycle 的 f000 analysis）萃取成前端粒子動畫 / raster 用的 RGBA PNG + meta JSON，
上傳到 s3://<bucket>/deploy-assets/climate/{name}_latest.{png,json}。

與 mini-taiwan-pulse/scripts/preprocess/extract_climate_uv.py 同一套編碼約定
（前端 climateParticleLineLayer.ts / useDustForecastLayer.ts 解碼），差異：
- 選檔改「最新 cycle 的 f000」而非「observed_at 最遠」→ 顯示當下實況，非 +120h 預報
- 不再鏡像到 public/climate/（container 內無此路徑），只上傳 S3 deploy-assets

PNG 編碼（風場 / 海流）：R=u、G=v、B=0、A=255 valid / 0 無效；min/max 在同名 .json。
沙塵：預烤棕色色階（與前端 climateRamps.ts DUST_BAKE_STOPS 同值）+ alpha=強度；
      映射**固定**為 sqrt(aod / DUST_AOD_MAX)（非 per-file normalize）才能跨幀比較。

⚠ 依賴：xarray + cfgrib（GRIB2）+ netCDF4（.nc）+ Pillow + numpy（見 requirements.txt）。
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.s3 import S3Storage

S3_DEPLOY_PREFIX = "deploy-assets/climate"
# 多幀時間軸（frames）— 契約見任務說明；PNG 路徑 manifest 內相對於 climate/frames/
S3_FRAMES_PREFIX = "deploy-assets/climate/frames"
FRAME_PAST_DAYS = 14  # 過去 N 天 analysis（daily 00Z）

# 沙塵 AOD 棕色色階控制點（t, r, g, b）— 與前端 climateRamps.ts DUST_BAKE_STOPS 同值
DUST_STOPS = [
    (0.00, 218, 197, 168),
    (0.20, 180, 140, 90),
    (0.45, 132, 86, 46),
    (0.75, 82, 46, 22),
    (1.00, 40, 20, 10),
]

# ── 沙塵色階：固定值域 + sqrt（⚠ 不可改回 per-file normalize）──
# duaod550（550nm 沙塵光學厚度，無因次）。每張自己算 min/max 的話，
# 每一幀的最大值都會被染成同一個最深棕 → 跨幀完全不可比，時間軸播起來
# 會讓人以為濃度天天一樣（或誤判某天特別嚴重）。固定映射才有可比性。
#
# 映射：t = sqrt(clip(aod, 0, MAX) / MAX)
#   - 上限 1.0：CAMS / WMO SDS-WAS 沙塵 AOD 圖慣例的「重度沙塵」量級。
#     實測 DB 51 筆 cams_dust digest 全域單幀最大值 0.11~0.84（中位 0.34），
#     夏季不截頂；春季蒙古／塔克拉瑪干事件超過 1.0 時頂端飽和 = 誠實的「≥1.0」。
#   - sqrt 而非線性：AOD 場是重尾分佈（實測 mean 0.006 vs max 0.085），線性固定
#     色階會讓整層幾乎全透明（實測可見像素僅 0.1~2.5%、mean alpha 2~4/255，
#     再乘前端 0.7 opacity 等於看不見）。sqrt 後可見像素 30~37%、mean alpha 23~26。
#     非線性正是 WMO SDS-WAS 沙塵 AOD 色階的慣例（刻度約略倍增），不是特例。
#   - ⚠ 動 MAX 或 GAMMA 等於改變所有既有幀的語意，必須手動清掉 S3 frames/dust/
#     讓全部重烤（_bake_frames 靠 stamp+init_at 判斷重用，不會偵測色階變更）
DUST_AOD_MIN = 0.0
DUST_AOD_MAX = 1.0
DUST_AOD_GAMMA = 0.5  # t = (aod / MAX) ** GAMMA

# CAMS 一個 .nc 檔含這 6 個 leadtime（見 cams.py CAMS_LEADTIMES），daily
DUST_LEADTIMES = (0, 24, 48, 72, 96, 120)

# 寫進 manifest datasets.dust 的色階說明（前端 normalizeManifest 只讀 .frames，
# 同層其他 key 會被忽略 → 安全的擴充點，供圖例日後顯示實際 AOD 數值）
DUST_SCALE_META = {
    "scale": {
        "var": "duaod550",
        "unit": "1",
        "kind": "fixed-sqrt",       # t = ((aod - min) / (max - min)) ** gamma
        "min": DUST_AOD_MIN,
        "max": DUST_AOD_MAX,
        "gamma": DUST_AOD_GAMMA,    # 反推圖例刻度：aod = min + (max - min) * t ** (1 / gamma)
    }
}


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
                  FROM live.global_climate_grids
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

    # ── 沙塵（CAMS duaod550）：一個 .nc 檔含 6 個 leadtime，latest 與 frames 共用下面三個 helper ──
    def _latest_dust_source(self) -> Optional[tuple[str, datetime]]:
        """最新 init 的 CAMS 檔 → (s3_uri, init_at)。

        ⚠ 刻意不用 _latest_analysis()（它靠 row 的 leadtime_hr 挑最小值）：
        live.global_climate_grids 唯一鍵是 (dataset_id, observed_at)，過去用
        ON CONFLICT DO NOTHING，舊 cycle 的 f120 先佔住 valid-time 讓新 cycle 的
        f000 寫不進來 → 近 14 天 leadtime_hr=0 幾乎零筆（2026-08-14 實測：51 筆
        cams_dust 只有 06-27、07-01 兩筆 lt=0）。照 row 選會挑到「5 天前 cycle 的
        +120h 預報」卻標成實況。改成只認最新 init_at，f000 直接從檔案裡取 slice。
        """
        import psycopg2

        with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT s3_uri, init_at
                  FROM live.global_climate_grids
                 WHERE dataset_id='cams_dust' AND s3_uri IS NOT NULL AND init_at IS NOT NULL
                 ORDER BY init_at DESC, collected_at DESC
                 LIMIT 1
                """
            )
            row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def _open_dust_slice(self, nc_path: Path, leadtime_hr: int) -> tuple[np.ndarray, list, Optional[str]]:
        """CAMS .nc → 指定 leadtime 的 duaod550 2D 場。回傳 (arr, bbox, valid_at_iso|None)。

        arr 已做 0..360→-180..180 重排 + 南北翻正（與 latest 單張同一套，
        保證 frames 與 dust_latest.png 同 bbox / 同 grid → 前端換幀不重設 coordinates）。
        """
        import xarray as xr

        ds = xr.open_dataset(nc_path)
        try:
            da = ds["duaod550"]
            if "forecast_reference_time" in da.dims:
                da = da.isel(forecast_reference_time=0)
            idx = self._dust_leadtime_index(ds, leadtime_hr)
            for dim in ("forecast_period", "time"):
                if dim in da.dims:
                    da = da.isel({dim: idx})
                    break
            # 其餘殘留維度（除了 lat/lon）一律取第 0 片
            for dim in list(da.dims):
                if dim not in ("latitude", "lat", "longitude", "lon"):
                    da = da.isel({dim: 0})
            arr = np.asarray(da.values, dtype=np.float32)
            lats = ds["latitude"].values if "latitude" in ds else ds["lat"].values
            lons = ds["longitude"].values if "longitude" in ds else ds["lon"].values
            valid_at = self._dust_valid_at(ds, idx)
        finally:
            ds.close()
        arr, lons = self._lon_to_180(arr, lons)
        bbox = [float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())]
        if lats[0] < lats[-1]:
            arr = np.flipud(arr)
        return arr, bbox, valid_at

    @staticmethod
    def _dust_leadtime_index(ds, leadtime_hr: int) -> int:
        """forecast_period（timedelta64）座標裡找 leadtime_hr 的 index；找不到退回 lt//24。"""
        fp = ds.get("forecast_period")
        if fp is not None and fp.size:
            try:
                hours = [int(np.timedelta64(v, "h").astype(int)) for v in np.atleast_1d(fp.values)]
                if leadtime_hr in hours:
                    return hours.index(leadtime_hr)
            except Exception:
                pass
        return max(int(leadtime_hr) // 24, 0)

    @staticmethod
    def _dust_valid_at(ds, idx: int) -> Optional[str]:
        vt = ds.get("valid_time")
        if vt is None or vt.size <= idx:
            return None
        try:
            return np.datetime_as_string(np.asarray(vt.values).flatten()[idx], unit="s") + "Z"
        except Exception:
            return None

    @staticmethod
    def _dust_rgba(arr: np.ndarray) -> tuple[np.ndarray, dict]:
        """AOD 2D 場 → 預烤棕色色階 RGBA + meta。

        ⚠ 映射固定為 t = ((aod - MIN) / (MAX - MIN)) ** GAMMA，**不做 per-file normalize**
        —— 理由見該組常數註解（每幀各自 normalize 會讓每幀最大值都染成同一深棕，跨幀不可比）。
        """
        mask = np.isfinite(arr)
        span = max(DUST_AOD_MAX - DUST_AOD_MIN, 1e-9)
        t = np.where(mask, np.clip((arr - DUST_AOD_MIN) / span, 0, 1) ** DUST_AOD_GAMMA, 0)

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

        valid = arr[mask]
        n = max(int(mask.sum()), 1)
        meta = {
            "width": int(arr.shape[1]), "height": int(arr.shape[0]),
            # ⚠ dust_min/dust_max 語意已改為「色階兩端對應的 AOD 值」（固定），
            # 不再是這張圖的實測 min/max —— 那兩個改放 dust_obs_*。
            # 前端 useDustForecastLayer 對這兩個欄位呼叫 .toFixed(3)，缺值會 throw。
            "dust_min": DUST_AOD_MIN, "dust_max": DUST_AOD_MAX,
            "dust_scale": "fixed-sqrt", "dust_gamma": DUST_AOD_GAMMA,
            "dust_obs_min": float(valid.min()) if mask.any() else 0.0,
            "dust_obs_max": float(valid.max()) if mask.any() else 0.0,
            "dust_clipped_pct": float((valid > DUST_AOD_MAX).sum()) / n * 100 if mask.any() else 0.0,
        }
        return rgba, meta

    def _bake_dust(self, tmp: Path) -> dict:
        from PIL import Image

        sel = self._latest_dust_source()
        if not sel:
            raise RuntimeError("no s3_uri for cams_dust")
        s3_uri, init_at = sel
        local = tmp / "dust.nc"
        self._download(s3_uri, local)
        # f000 = 這個 cycle 的實況場（舊寫法是拿 DB row 的 observed_at 當 valid_at，
        # 而那筆 row 近期都是 +120h → 畫 f000 卻標成 5 天後，時間標示是錯的）
        arr, bbox, valid_at = self._open_dust_slice(local, 0)
        valid_at = valid_at or self._iso_z(init_at)
        rgba, meta = self._dust_rgba(arr)

        out_png, out_json = tmp / "dust.png", tmp / "dust.json"
        Image.fromarray(rgba, "RGBA").save(out_png, format="PNG", optimize=True)
        meta.update({
            "dataset": "cams_dust", "valid_at": valid_at,
            "init_at": self._iso_z(init_at), "source_s3": s3_uri, "bbox": bbox,
        })
        out_json.write_text(json.dumps(meta, indent=2))
        self._upload(out_png, out_json, "dust_latest")
        return {"dataset": "cams_dust", "valid_at": valid_at,
                "dust_obs_max": round(meta["dust_obs_max"], 3)}

    # ── 多幀時間軸（frames）──
    @staticmethod
    def _iso_z(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _stamp(dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")

    def _frames_load_manifest(self) -> dict:
        """讀既有 manifest → {dataset: {stamp: frame_entry}}；不存在回 {}。"""
        key = f"{S3_FRAMES_PREFIX}/manifest.json"
        try:
            obj = self._s3.s3.get_object(Bucket=self._s3.bucket, Key=key)
            data = json.loads(obj["Body"].read())
        except Exception:
            return {}
        out: dict = {}
        for ds, blk in data.get("datasets", {}).items():
            out[ds] = {}
            for fr in blk.get("frames", []):
                stamp = fr["png"].rsplit("/", 1)[-1].replace(".png", "")
                out[ds][stamp] = fr
        return out

    def _frame_png_exists(self, png_rel: str) -> bool:
        try:
            self._s3.s3.head_object(Bucket=self._s3.bucket, Key=f"{S3_FRAMES_PREFIX}/{png_rel}")
            return True
        except Exception:
            return False

    def _upload_frame_png(self, local: Path, png_rel: str) -> None:
        self._s3.s3.upload_file(
            str(local), self._s3.bucket, f"{S3_FRAMES_PREFIX}/{png_rel}",
            ExtraArgs={"ContentType": "image/png", "CacheControl": "public, max-age=86400"},
        )

    def _upload_manifest(self, datasets: dict) -> None:
        manifest = {
            "version": 1,
            "generated_at": self._iso_z(datetime.now(timezone.utc)),
            "datasets": datasets,
        }
        key = f"{S3_FRAMES_PREFIX}/manifest.json"
        body = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        self._s3.s3.put_object(
            Bucket=self._s3.bucket, Key=key, Body=body,
            ContentType="application/json", CacheControl="public, max-age=300",
        )
        n = sum(len(v["frames"]) for v in datasets.values())
        print(f"[climate_bake] ↑ manifest {n} frames → s3://{self._s3.bucket}/{key}")

    @staticmethod
    def _frame_entry(spec: dict, png_rel: str, meta: dict) -> dict:
        return {
            "t": ClimateBakeCollector._iso_z(spec["t"]),
            "png": png_rel,
            "u_min": meta["u_min"], "u_max": meta["u_max"],
            "v_min": meta["v_min"], "v_max": meta["v_max"],
            "kind": spec["kind"],
            "init_at": ClimateBakeCollector._iso_z(spec["init_at"]),
        }

    @staticmethod
    def _scalar_frame_entry(spec: dict, png_rel: str) -> dict:
        """scalar 場（PNG 已預烤色階，前端不解 UV）的 entry — 不帶 u/v 值域。

        ⚠ u_min/u_max/v_min/v_max 必須「四個全給」或「一個都不給」：前端
        normalizeManifest 對 uvPresent ∉ {0, 4} 的 entry 會整筆 continue 丟棄。
        """
        return {
            "t": ClimateBakeCollector._iso_z(spec["t"]),
            "png": png_rel,
            "kind": spec["kind"],
            "init_at": ClimateBakeCollector._iso_z(spec["init_at"]),
        }

    def _plan_wind_frames(self) -> list[dict]:
        """gfs_wind10m：過去 14 天 daily 00Z f000（analysis）＋最新 cycle 0~+120h（forecast）。"""
        import psycopg2

        frames: dict[str, dict] = {}
        with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT s3_uri, observed_at, init_at
                  FROM live.global_climate_grids
                 WHERE dataset_id='gfs_wind10m' AND leadtime_hr=0 AND s3_uri IS NOT NULL
                   AND observed_at >= now() - make_interval(days => %s)
                   AND EXTRACT(hour FROM observed_at AT TIME ZONE 'UTC') = 0
                 ORDER BY observed_at
                """,
                (FRAME_PAST_DAYS,),
            )
            for s3_uri, obs, init in cur.fetchall():
                frames[self._stamp(obs)] = {
                    "t": obs, "s3_uri": s3_uri, "kind": "analysis",
                    "init_at": init, "leadtime": 0,
                }
            # forecast：最新 cycle 起 +120h 窗口，每個未來 valid-time 取「最新 init」那筆。
            # grids 唯一鍵是 (dataset_id, observed_at) + ON CONFLICT DO NOTHING，
            # 舊 cycle 的 f120 會先佔住某些 valid-time → 新 cycle 同 observed_at 被拒；
            # 用 DISTINCT ON (observed_at) ORDER BY init_at DESC 補滿整條 6h 序列（取最鮮）。
            cur.execute("SELECT max(init_at) FROM live.global_climate_grids WHERE dataset_id='gfs_wind10m'")
            latest_init = cur.fetchone()[0]
            if latest_init:
                cur.execute(
                    """
                    SELECT DISTINCT ON (observed_at) s3_uri, observed_at, init_at, leadtime_hr
                      FROM live.global_climate_grids
                     WHERE dataset_id='gfs_wind10m' AND s3_uri IS NOT NULL
                       AND observed_at >= %s
                       AND observed_at <= %s + make_interval(hours => 120)
                     ORDER BY observed_at, init_at DESC
                    """,
                    (latest_init, latest_init),
                )
                for s3_uri, obs, init, lt in cur.fetchall():
                    frames[self._stamp(obs)] = {  # forecast 覆蓋同 stamp 的 analysis
                        "t": obs, "s3_uri": s3_uri,
                        "kind": "analysis" if lt == 0 else "forecast",
                        "init_at": init, "leadtime": lt,
                    }
        return sorted(frames.values(), key=lambda f: f["t"])

    def _plan_currents_frames(self) -> list[dict]:
        """cmems_currents：過去 14 天 daily 00Z（analysis）＋今日起 6h step（forecast）。"""
        import psycopg2

        with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (observed_at) observed_at, s3_uri, init_at
                  FROM live.global_climate_grids
                 WHERE dataset_id='cmems_currents' AND s3_uri IS NOT NULL
                   AND observed_at >= now() - make_interval(days => %s)
                 ORDER BY observed_at, collected_at DESC
                """,
                (FRAME_PAST_DAYS,),
            )
            rows = cur.fetchall()
        now = datetime.now(timezone.utc)
        today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        frames: dict[str, dict] = {}
        for obs, s3_uri, init in rows:
            # 過去只保留 daily 00Z；今日起（未來 6h step）全收
            if obs < today0 and obs.hour != 0:
                continue
            frames[self._stamp(obs)] = {
                "t": obs, "s3_uri": s3_uri,
                "kind": "analysis" if obs <= now else "forecast",
                "init_at": obs, "leadtime": 0, "slice_time": obs,
            }
        return sorted(frames.values(), key=lambda f: f["t"])

    def _plan_dust_frames(self) -> list[dict]:
        """cams_dust：過去 14 天 f000（analysis）＋最新 init 的 0/24/48/72/96/120h。

        CAMS 一個 .nc 就含全部 6 個 leadtime，所以最新 cycle 只要下載一個檔就烤得出 6 幀
        （download cache 以 s3_uri 為 key，自然去重）。

        ⚠ 這裡刻意**不**套 wind 那招 `DISTINCT ON (observed_at) ORDER BY init_at DESC`：
        dust 近期每個 valid-time 都只剩舊 cycle 的 f120 殘留（見 _latest_dust_source
        註解），那樣挑會把「5 天前 cycle 的 +120h 預報」當成實況幀 —— 正是這次要修的病。
        過去 analysis 只認 leadtime_hr=0 的真 f000，寧可幀數少也不拿預報冒充實況；
        upsert 改 do_update 後 f000 會每天補進來一筆，窗口自然長回 14 天。
        """
        import psycopg2

        frames: dict[str, dict] = {}
        with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (observed_at) s3_uri, observed_at, init_at
                  FROM live.global_climate_grids
                 WHERE dataset_id='cams_dust' AND leadtime_hr=0 AND s3_uri IS NOT NULL
                   AND observed_at >= now() - make_interval(days => %s)
                 ORDER BY observed_at, init_at DESC
                """,
                (FRAME_PAST_DAYS,),
            )
            for s3_uri, obs, init in cur.fetchall():
                frames[self._stamp(obs)] = {
                    "t": obs, "s3_uri": s3_uri, "kind": "analysis",
                    "init_at": init or obs, "leadtime": 0,
                }
            cur.execute(
                """
                SELECT s3_uri, init_at
                  FROM live.global_climate_grids
                 WHERE dataset_id='cams_dust' AND s3_uri IS NOT NULL AND init_at IS NOT NULL
                 ORDER BY init_at DESC, collected_at DESC
                 LIMIT 1
                """
            )
            row = cur.fetchone()

        if row:
            s3_uri, init_at = row
            now = datetime.now(timezone.utc)
            for lt in DUST_LEADTIMES:
                t = init_at + timedelta(hours=lt)
                frames[self._stamp(t)] = {  # 最新 cycle 覆蓋同 stamp 的舊 analysis
                    "t": t, "s3_uri": s3_uri,
                    "kind": "analysis" if t <= now else "forecast",
                    "init_at": init_at, "leadtime": lt,
                }
        return sorted(frames.values(), key=lambda f: f["t"])

    def _bake_wind_frame(self, spec: dict, tmp: Path, cache: dict, png_rel: str) -> dict:
        import xarray as xr

        local = cache.get(spec["s3_uri"])
        if local is None:
            local = tmp / f"src_{len(cache)}.grib2"
            self._download(spec["s3_uri"], local)
            cache[spec["s3_uri"]] = local
        ds = xr.open_dataset(
            local, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 10}, "indexpath": ""},
        )
        try:
            u, v = ds["u10"].values, ds["v10"].values
            lats, lons = ds["latitude"].values, ds["longitude"].values
        finally:
            ds.close()
        u, _ = self._lon_to_180(u, lons)
        v, _ = self._lon_to_180(v, lons)
        if lats[0] < lats[-1]:
            u, v = np.flipud(u), np.flipud(v)
        out_png = tmp / f"wind_{spec['leadtime']:03d}_{self._stamp(spec['t'])}.png"
        meta = self._encode_uv(u, v, out_png)
        self._upload_frame_png(out_png, png_rel)
        return self._frame_entry(spec, png_rel, meta)

    def _bake_currents_frame(self, spec: dict, tmp: Path, cache: dict, png_rel: str) -> dict:
        import xarray as xr

        local = cache.get(spec["s3_uri"])
        if local is None:
            local = tmp / f"src_{len(cache)}.nc"
            self._download(spec["s3_uri"], local)
            cache[spec["s3_uri"]] = local
        ds = xr.open_dataset(local)
        try:
            uo, vo = ds["uo"], ds["vo"]
            if "depth" in uo.dims:
                uo, vo = uo.isel(depth=0), vo.isel(depth=0)
            if "time" in uo.dims:
                t64 = np.datetime64(spec["slice_time"].replace(tzinfo=None))
                uo, vo = uo.sel(time=t64, method="nearest"), vo.sel(time=t64, method="nearest")
            u, v = uo.values, vo.values
            lats = ds["latitude"].values if "latitude" in ds else ds["lat"].values
        finally:
            ds.close()
        if lats[0] < lats[-1]:
            u, v = np.flipud(u), np.flipud(v)
        out_png = tmp / f"cur_{self._stamp(spec['t'])}.png"
        meta = self._encode_uv(u, v, out_png)
        self._upload_frame_png(out_png, png_rel)
        return self._frame_entry(spec, png_rel, meta)

    def _bake_dust_frame(self, spec: dict, tmp: Path, cache: dict, png_rel: str) -> dict:
        from PIL import Image

        local = cache.get(spec["s3_uri"])
        if local is None:
            local = tmp / f"src_{len(cache)}.nc"
            self._download(spec["s3_uri"], local)
            cache[spec["s3_uri"]] = local
        arr, _bbox, _valid_at = self._open_dust_slice(local, spec["leadtime"])
        rgba, _meta = self._dust_rgba(arr)
        out_png = tmp / f"dust_{spec['leadtime']:03d}_{self._stamp(spec['t'])}.png"
        Image.fromarray(rgba, "RGBA").save(out_png, format="PNG", optimize=True)
        self._upload_frame_png(out_png, png_rel)
        return self._scalar_frame_entry(spec, png_rel)

    def _bake_frames(self, tmp: Path) -> dict:
        """建 frames manifest：已存在同 stamp+init_at 的 PNG 跳過重烤；先傳所有 PNG 再傳 manifest。"""
        existing = self._frames_load_manifest()
        cache: dict = {}
        datasets: dict = {}
        for dataset, planner, baker, extra in [
            ("wind10m", self._plan_wind_frames, self._bake_wind_frame, None),
            ("currents", self._plan_currents_frames, self._bake_currents_frame, None),
            ("dust", self._plan_dust_frames, self._bake_dust_frame, DUST_SCALE_META),
        ]:
            specs = planner()
            entries, baked_n, reused_n = [], 0, 0
            for spec in specs:
                stamp = self._stamp(spec["t"])
                png_rel = f"{dataset}/{stamp}.png"
                prev = existing.get(dataset, {}).get(stamp)
                if prev and prev.get("init_at") == self._iso_z(spec["init_at"]) and self._frame_png_exists(png_rel):
                    entries.append(prev)
                    reused_n += 1
                    continue
                try:
                    entries.append(baker(spec, tmp, cache, png_rel))
                    baked_n += 1
                except Exception as e:
                    print(f"[climate_bake] frame ✗ {dataset} {stamp}: {str(e)[:160]}")
            entries.sort(key=lambda e: e["t"])
            datasets[dataset] = {"frames": entries, **(extra or {})}
            print(f"[climate_bake] frames {dataset}: baked={baked_n} reused={reused_n} total={len(entries)}")
        self._upload_manifest(datasets)
        return {d: len(v["frames"]) for d, v in datasets.items()}

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

            # 多幀時間軸（frames）：風場 + 海流 + 沙塵 → frames/manifest.json
            frame_counts = None
            try:
                frame_counts = self._bake_frames(tmp)
                print(f"[climate_bake] ✓ frames {frame_counts}")
            except Exception as e:
                failed.append({"dataset": "frames", "error": str(e)})
                print(f"[climate_bake] ✗ frames: {e}")

        # 全失敗才算這輪失敗（讓 base.run 記 error）；部分成功照常回報
        if not baked:
            raise RuntimeError(f"全部烤圖失敗: {failed}")
        # 不含 'data' key → base.run() 不會寫 storage / Supabase（本 collector 只產 deploy-assets）
        return {"baked": baked, "failed": failed, "ok_count": len(baked), "frames": frame_counts}
