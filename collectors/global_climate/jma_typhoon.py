"""JMA RSMC Tokyo 颱風位置收集器

資料來源：日本氣象廳 bosai/typhoon（免認證、WMO 西北太平洋官方颱風機構）
  端點：
    https://www.jma.go.jp/bosai/typhoon/data/targetTc.json
    https://www.jma.go.jp/bosai/typhoon/data/{tc_id}/forecast.json
  特性：
    - targetTc.json 列出當前所有 active TC，空陣列代表無颱風 idle
    - forecast.json 含 preTyphoon + typhoon + forecast 段，每段是 [[lat, lon], ...]
    - 同一 advisory 拉到後展開過去軌跡 + 現在 + 預報為多筆 typhoon_position row

寫入：realtime.typhoon_positions (source='jma')
  - UNIQUE(storm_id, source, valid_at, point_type, advisory_number)
  - ON CONFLICT DO NOTHING
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

URL_JMA_TARGET = "https://www.jma.go.jp/bosai/typhoon/data/targetTc.json"
URL_JMA_FORECAST = "https://www.jma.go.jp/bosai/typhoon/data/{tc_id}/forecast.json"


class JmaTyphoonCollector(BaseCollector):
    """JMA 西北太平洋颱風位置（time-point decomposed）收集器。
    無颱風時 idle 不寫；有颱風時展開為多筆 row。"""

    name = "global_climate_jma_typhoon"
    interval_minutes = config.GLOBAL_CLIMATE_JMA_TYPHOON_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; global-climate-jma-typhoon)",
            "Accept": "application/json",
        })

    def _fetch_json(self, url: str) -> Optional[dict | list]:
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def _decompose_forecast(self, storm_id: str, forecast_data: list, collected_at: datetime) -> list[dict]:
        """把 forecast.json 內容展開為 N 筆 typhoon_position row。

        forecast.json 結構：
          [
            {"part":"title", "issue":{...}, "typhoonNumber":"2607",
             "name":{"jp":"...", "en":"..."}},
            {"part":{"jp":"実況", "en":"Analysis"},
             "advancedHours":0, "validtime":{"UTC":"..."},
             "track":{"preTyphoon":[[lat,lon],...], "typhoon":[[lat,lon],...]},
             "elements":{"centerPressure":..., "maxWindSpeed":..., ...}},
            ...更多 part 段（72h forecast / 120h forecast）
          ]
        """
        rows: list[dict] = []

        # 從第一段（title part）取 metadata
        title_part = next((p for p in forecast_data if p.get("part") == "title"), None)
        if not title_part:
            return rows
        name_local = (title_part.get("name") or {}).get("jp")
        name_en = (title_part.get("name") or {}).get("en")
        advisory_issued_at = None
        issue = title_part.get("issue") or {}
        if issue.get("UTC"):
            try:
                advisory_issued_at = datetime.fromisoformat(issue["UTC"].replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass

        # 用 advisory_issued_at hashing 當 advisory_number（時間+0 / +24 / +72…）
        # 找不到時用 epoch 秒
        if advisory_issued_at:
            advisory_number = int(advisory_issued_at.timestamp() // 60)  # 分鐘級唯一
        else:
            advisory_number = int(collected_at.timestamp() // 60)

        collected_iso = collected_at.isoformat()
        advisory_iso = advisory_issued_at.isoformat() if advisory_issued_at else None

        for part in forecast_data:
            part_meta = part.get("part")
            if not isinstance(part_meta, dict):
                continue  # title part 已處理

            # validtime
            vt = part.get("validtime") or {}
            valid_at_str = vt.get("UTC")
            if not valid_at_str:
                continue
            try:
                valid_at = datetime.fromisoformat(valid_at_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            valid_iso = valid_at.isoformat()

            # observed (advancedHours=0) vs forecast (advancedHours>0)
            advanced_hr = part.get("advancedHours", 0)
            point_type = "observed" if advanced_hr == 0 else "forecast"

            # 取 center 位置（JMA bosai 的 center 是 [lat, lon] list；elements 可能 None）
            center = part.get("center")
            elements = part.get("elements") or {}

            def _safe_get(d, *keys):
                for k in keys:
                    if not isinstance(d, dict):
                        return None
                    d = d.get(k)
                    if d is None:
                        return None
                return d

            center_pressure  = _safe_get(elements, "centerPressure", "value")
            max_wind_kt      = _safe_get(elements, "maxWindSpeed", "value")
            gale_radius_km   = _safe_get(elements, "windAreas", "gale", "longRadius")
            storm_radius_km  = _safe_get(elements, "windAreas", "storm", "longRadius")

            lat = lon = None
            if isinstance(center, list) and len(center) >= 2:
                lat, lon = center[0], center[1]

            # 如果 part 沒 center，從 track 段補（fallback）
            if lat is None or lon is None:
                track = part.get("track") or {}
                last_point = None
                for key in ("typhoon", "preTyphoon"):
                    arr = track.get(key) or []
                    if arr and isinstance(arr[-1], list) and len(arr[-1]) >= 2:
                        last_point = arr[-1]
                        break
                if last_point:
                    lat, lon = last_point[0], last_point[1]

            if lat is None or lon is None:
                continue

            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except (TypeError, ValueError):
                continue

            rows.append({
                "storm_id":            storm_id,
                "source":              "jma",
                "valid_at":            valid_iso,
                "point_type":          point_type,
                "advisory_number":     advisory_number,
                "advisory_issued_at":  advisory_iso,
                "name_local":          name_local,
                "name_en":             name_en,
                "center_lat":          lat_f,
                "center_lon":          lon_f,
                "center_pressure_hpa": center_pressure,
                "max_wind_kt":         max_wind_kt,
                "gale_radius_km":      gale_radius_km,
                "storm_radius_km":     storm_radius_km,
                "lon":                 lon_f,  # 給 transformer 組 geom 用
                "lat":                 lat_f,
                "raw_json":            json.dumps(part, ensure_ascii=False),
                "collected_at":        collected_iso,
            })

            # 同 part 內若有 preTyphoon / typhoon 歷史軌跡，展開為過去 observed 點
            if advanced_hr == 0:
                track = part.get("track") or {}
                for key in ("preTyphoon", "typhoon"):
                    arr = track.get(key) or []
                    n = len(arr)
                    if n < 2:
                        continue
                    # 倒推時間：假設每點間距 3 hr（JMA 預設）
                    for i, point in enumerate(arr[:-1]):
                        if not (isinstance(point, list) and len(point) >= 2):
                            continue
                        try:
                            hist_lat = float(point[0])
                            hist_lon = float(point[1])
                        except (TypeError, ValueError):
                            continue
                        # 第 i 個點 (從 0) 對應 valid_at - (n - 1 - i) * 3h
                        offset_hr = (n - 1 - i) * 3
                        hist_valid = valid_at - timedelta(hours=offset_hr)
                        hist_iso = hist_valid.isoformat()
                        rows.append({
                            "storm_id":            storm_id,
                            "source":              "jma",
                            "valid_at":            hist_iso,
                            "point_type":          "observed",
                            "advisory_number":     advisory_number,
                            "advisory_issued_at":  advisory_iso,
                            "name_local":          name_local,
                            "name_en":             name_en,
                            "center_lat":          hist_lat,
                            "center_lon":          hist_lon,
                            "center_pressure_hpa": None,
                            "max_wind_kt":         None,
                            "gale_radius_km":      None,
                            "storm_radius_km":     None,
                            "lon":                 hist_lon,
                            "lat":                 hist_lat,
                            "raw_json":            None,
                            "collected_at":        collected_iso,
                        })

        return rows

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        target = self._fetch_json(URL_JMA_TARGET) or []

        all_rows: list[dict] = []
        storms = []
        for entry in target:
            tc_id = entry.get("tropicalCyclone")
            if not tc_id:
                continue
            storms.append(tc_id)
            forecast = self._fetch_json(URL_JMA_FORECAST.format(tc_id=tc_id))
            if not forecast:
                continue
            rows = self._decompose_forecast(tc_id, forecast, now)
            all_rows.extend(rows)

        return {
            "data":          all_rows,
            "storm_count":   len(storms),
            "active_storms": storms,
            "point_count":   len(all_rows),
            "collected_at":  now.isoformat(),
        }


if __name__ == "__main__":
    c = JmaTyphoonCollector.__new__(JmaTyphoonCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (jma-typhoon-test)", "Accept": "application/json"})
    out = c.collect()
    print(f"storms: {out['storm_count']}, points: {out['point_count']}")
    print(f"active: {out['active_storms']}")
    if out["data"]:
        print(f"sample: {out['data'][0]}")
