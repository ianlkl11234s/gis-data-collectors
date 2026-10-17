"""JMA RSMC Tokyo 颱風位置收集器

資料來源：日本氣象廳 bosai/typhoon（免認證、WMO 西北太平洋官方颱風機構）
  端點：
    https://www.jma.go.jp/bosai/typhoon/data/targetTc.json
    https://www.jma.go.jp/bosai/typhoon/data/{tc_id}/forecast.json
    https://www.jma.go.jp/bosai/typhoon/data/{tc_id}/specifications.json
  特性：
    - targetTc.json 列出當前所有 active TC，空陣列代表無颱風 idle
    - forecast.json 含 preTyphoon + typhoon + forecast 段，每段是 [[lat, lon], ...]
    - 同一 advisory 拉到後展開過去軌跡 + 現在 + 預報為多筆 typhoon_position row
    - forecast.json 只有位置與幾何（沒有 elements）；氣壓、風速、風圈、預報圓在
      specifications.json（2026-10-17 W0 查核：261 起 jma 列的氣壓／風速／風圈全 NULL 即因讀錯檔）。
      兩檔以 advancedHours 對齊（不用 index）。
    - specifications 的 galeWarning（強風域 ≥15 m/s）、stormWarning（暴風域 ≥25 m/s）
      只有實況段（advancedHours=0）是當下半徑 → 填 gale_radius_km／storm_radius_km；
      預報段的 stormWarning 是「暴風警戒域」（含預報圓的包絡），只放 wind_radii.storm_warning_area。
      非對稱時可能有多個 area（未實測）：單欄取最大值（長半徑），全部 area 存 wind_radii.*.areas。
      probabilityCircleRadius.km（予報円＝70% 機率圈）→ prob70_radius_km。
    - 錯誤處理：targetTc 抓取失敗 raise；個別颱風檔非 404 錯誤 → _collector_error（已解析的列照寫，
      本輪記失敗）。targetTc 空陣列 → status=no_active_storm、不帶 data（不寫列）。
      歷史列不回填（DO NOTHING；舊 raw_json 是 forecast.json 的段落，沒有數值）。

寫入：live.typhoon_positions (source='jma')
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
from collectors.global_climate.typhoon_common import storm_key, to_float, valid_position, valid_time

URL_JMA_TARGET = "https://www.jma.go.jp/bosai/typhoon/data/targetTc.json"
URL_JMA_FORECAST = "https://www.jma.go.jp/bosai/typhoon/data/{tc_id}/forecast.json"
URL_JMA_SPECS = "https://www.jma.go.jp/bosai/typhoon/data/{tc_id}/specifications.json"


class JmaFetchError(RuntimeError):
    """非 404 的抓取／解析錯誤（不可當成「沒有颱風」）。"""


def _warning_ranges(items) -> tuple[Optional[float], Optional[dict]]:
    """galeWarning／stormWarning list → (單一半徑 km＝各 area 最大值, {'all'| 'areas'})。"""
    areas = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        km = to_float((it.get("range") or {}).get("km"))
        if km is None:
            continue
        area = it.get("area") or {}
        areas.append({"area": area.get("en") or area.get("jp"), "km": km})
    if not areas:
        return None, None
    longest = max(a["km"] for a in areas)
    if len(areas) == 1 and str(areas[0]["area"] or "").lower() in ("all", "全域"):
        return longest, {"all": longest}
    return longest, {"all": longest, "areas": areas}


def parse_jma_specifications(specs) -> dict[int, dict]:
    """specifications.json → {advancedHours: 數值欄位 dict}。"""
    out: dict[int, dict] = {}
    for part in specs or []:
        if not isinstance(part, dict) or not isinstance(part.get("part"), dict):
            continue
        try:
            hr = int(part.get("advancedHours", 0))
        except (TypeError, ValueError):
            continue
        mw = part.get("maximumWind") or {}
        sustained = mw.get("sustained") or {}
        gust = mw.get("gust") or {}
        gale_r, gale = _warning_ranges(part.get("galeWarning"))
        storm_r, storm = _warning_ranges(part.get("stormWarning"))
        wr: dict = {"unit": "km", "threshold": {"gale": "15 m/s", "storm": "25 m/s"}}
        if hr == 0:
            if gale:
                wr["gale"] = gale
            if storm:
                wr["storm"] = storm
        elif storm:
            wr["storm_warning_area"] = storm
        out[hr] = {
            "center_pressure_hpa": to_float(part.get("pressure")),
            "max_wind_kt":         to_float(sustained.get("kt")),
            "max_wind_ms":         to_float(sustained.get("m/s")),
            "max_gust_ms":         to_float(gust.get("m/s")),
            "gale_radius_km":      gale_r if hr == 0 else None,
            "storm_radius_km":     storm_r if hr == 0 else None,
            "prob70_radius_km":    to_float((part.get("probabilityCircleRadius") or {}).get("km")),
            "wind_radii":          json.dumps(wr, ensure_ascii=False) if len(wr) > 2 else None,
        }
    return out


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
        """404 → None（該颱風檔已撤下）；其他 HTTP／JSON 錯誤 → JmaFetchError（不可當成沒颱風）。"""
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise JmaFetchError(f"{url}: {type(e).__name__}: {e}") from e

    def _decompose_forecast(self, storm_id: str, forecast_data: list, collected_at: datetime,
                            specs: Optional[dict[int, dict]] = None) -> list[dict]:
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
        # 年份：國際編號 typhoonNumber（2630 → 2026）優先，其次 storm_id TC{YY}{NN}
        year = None
        tn = str(title_part.get("typhoonNumber") or "")
        if len(tn) == 4 and tn.isdigit():
            year = 2000 + int(tn[:2])
        elif len(storm_id) == 6 and storm_id.startswith("TC") and storm_id[2:].isdigit():
            year = 2000 + int(storm_id[2:4])
        skey = storm_key(year, name_en)
        specs = specs or {}
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

            # 數值一律取 specifications.json（forecast.json 沒有 elements；保留舊鍵當後備）
            spec = specs.get(int(advanced_hr or 0)) or {}
            center_pressure  = spec.get("center_pressure_hpa", _safe_get(elements, "centerPressure", "value"))
            max_wind_kt      = spec.get("max_wind_kt", _safe_get(elements, "maxWindSpeed", "value"))
            gale_radius_km   = spec.get("gale_radius_km", _safe_get(elements, "windAreas", "gale", "longRadius"))
            storm_radius_km  = spec.get("storm_radius_km", _safe_get(elements, "windAreas", "storm", "longRadius"))

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
            if not valid_position(lat_f, lon_f) or not valid_time(
                    point_type, valid_at, collected_at, advisory_issued_at):
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
                "max_wind_ms":         spec.get("max_wind_ms"),
                "max_gust_ms":         spec.get("max_gust_ms"),
                "prob70_radius_km":    spec.get("prob70_radius_km"),
                "wind_radii":          spec.get("wind_radii"),
                "storm_key":           skey,
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
                            "storm_key":           skey,
                            "lon":                 hist_lon,
                            "lat":                 hist_lat,
                            "raw_json":            None,
                            "collected_at":        collected_iso,
                        })

        return rows

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        target = self._fetch_json(URL_JMA_TARGET)  # 失敗 raise JmaFetchError
        if target is None or not isinstance(target, list):
            raise JmaFetchError(f"targetTc.json 回應異常：{type(target).__name__}")

        all_rows: list[dict] = []
        storms: list[str] = []
        errors: list[str] = []
        for entry in target:
            tc_id = entry.get("tropicalCyclone") if isinstance(entry, dict) else None
            if not tc_id:
                continue
            storms.append(tc_id)
            try:
                forecast = self._fetch_json(URL_JMA_FORECAST.format(tc_id=tc_id))
                specs_raw = self._fetch_json(URL_JMA_SPECS.format(tc_id=tc_id))
            except JmaFetchError as e:
                errors.append(str(e))
                continue
            if not forecast:
                continue  # 404：颱風檔已撤下
            if specs_raw is None:
                errors.append(f"{tc_id} specifications.json 404（位置照寫、數值欄 NULL）")
            rows = self._decompose_forecast(tc_id, forecast, now, parse_jma_specifications(specs_raw))
            all_rows.extend(rows)

        base = {
            "storm_count":   len(storms),
            "active_storms": storms,
            "collected_at":  now.isoformat(),
        }
        if not storms:
            # 刻意不帶 'data'：不存檔、不寫 DB；status 讓日誌分得出 idle 與故障
            return {**base, "status": "no_active_storm", "point_count": 0}
        result = {**base, "status": "ok", "data": all_rows, "point_count": len(all_rows)}
        if errors:
            result["_collector_error"] = "; ".join(errors)[:1000]
        return result

if __name__ == "__main__":
    c = JmaTyphoonCollector.__new__(JmaTyphoonCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (jma-typhoon-test)", "Accept": "application/json"})
    out = c.collect()
    print(f"storms: {out['storm_count']}, points: {out['point_count']}")
    print(f"active: {out['active_storms']}")
    if out["data"]:
        print(f"sample: {out['data'][0]}")
