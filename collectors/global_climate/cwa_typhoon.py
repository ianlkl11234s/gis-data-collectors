"""氣象署熱帶氣旋路徑收集器（W-C0034-005）

資料來源：交通部中央氣象署開放資料 datastore W-C0034-005（需 CWA_API_KEY）
  https://opendata.cwa.gov.tw/api/v1/rest/datastore/W-C0034-005?format=JSON
  特性（2026-10-17 實測，颱風薔琵 CHAMPI）：
    - records.TropicalCyclones.TropicalCyclone[]：每顆 Year、TyphoonName（英文）、
      CwaTyphoonName（中文）、CwaTdNo、CwaTyNo；沒有國際編號
    - AnalysisData.Fix[]：每 6 小時，DateTime 帶 +08:00；MaxWindSpeed／MaxGustSpeed（m/s，10 分鐘平均）、
      Pressure（hPa）、MovingSpeed（km/h）、Circle15ms／Circle25ms（7／10 級風圈 km）＋ QuadrantRadii（NE/SE/SW/NW）
      早期點沒有風圈（缺就 NULL，不寫 0）
    - ForecastData.Fix[]：+6…+120h，沒有 DateTime，用 InitialTime + ForecastHour；
      風圈只有單一半徑，另有 Radius70PercentProbability（70% 機率圈 km）
    - 值多為字串；單顆時 TropicalCyclone 可能是 dict 而不是 list

寫入：live.typhoon_positions（source='cwa'，gis-platform migration 476）
  storm_id = 'cwa' + YY + CwaTdNo（兩位），例 cwa2633（不可用 TC 開頭：RPC 以 TC% 判 JMA）
  advisory_number = InitialTime 的分鐘 epoch；advisory_issued_at = InitialTime
  UNIQUE(storm_id, source, valid_at, point_type, advisory_number) → ON CONFLICT DO NOTHING
  storm_key = Year + 英文名（跨 JMA／JTWC 對應，見 typhoon_common）
  max_wind_kt = m/s × 1.943844（10 分鐘平均，與 JTWC 1 分鐘口徑不同）；原值在 max_wind_ms 與 raw_json

沒有颱風：回傳沒有 'data' 的 dict（status='no_active_storm'），base 不存檔、不寫 DB；
HTTP／JSON／結構錯誤一律 raise，絕不當成「沒有颱風」。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.typhoon_common import (
    ms_to_kt, storm_key, to_float, valid_position, valid_time,
)

DATASET_ID = "W-C0034-005"
QUADRANTS = ("NE", "SE", "SW", "NW")


def _as_list(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _children(node, key: str) -> list:
    """XML 轉 JSON 的容器節點：{key: x|[..]} 取子項；容器本身就是 list／單筆時照收；其他形狀回空。"""
    if isinstance(node, dict) and key in node:
        return _as_list(node.get(key))
    if isinstance(node, list):
        return node
    return []


def _parse_dt(s) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).strip())
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=TAIPEI_TZ)


def _circle(c) -> tuple[Optional[float], Optional[dict]]:
    """Circle15ms / Circle25ms → (單一半徑, {'all':..,'ne':..} 或 None)。"""
    if not isinstance(c, dict):
        return None, None
    radius = to_float(c.get("Radius"))
    out: dict = {}
    if radius is not None:
        out["all"] = radius
    for q in _children(c.get("QuadrantRadii"), "Radius"):
        if not isinstance(q, dict):
            continue
        d = str(q.get("dir") or "").upper()
        v = to_float(q.get("value"))
        if d in QUADRANTS and v is not None:
            out[d.lower()] = v
    return radius, (out or None)


def _wind_radii(fix: dict) -> Optional[dict]:
    _, gale = _circle(fix.get("Circle15ms"))
    _, storm = _circle(fix.get("Circle25ms"))
    if not gale and not storm:
        return None
    wr: dict = {"unit": "km", "threshold": {"gale": "15 m/s", "storm": "25 m/s"}}
    if gale:
        wr["gale"] = gale
    if storm:
        wr["storm"] = storm
    return wr


def parse_cwa_typhoons(payload: dict, now: datetime, collected_at: Optional[datetime] = None) -> dict:
    """純函式：W-C0034-005 JSON → {'rows', 'storms', 'rejected', 'no_active'}。

    結構不符（缺 records、success 非 true）直接 raise ValueError。
    """
    if not isinstance(payload, dict):
        raise ValueError("W-C0034-005 回應不是 JSON 物件")
    if str(payload.get("success", "")).lower() != "true":
        raise ValueError(f"W-C0034-005 success={payload.get('success')!r}")
    records = payload.get("records")
    if not isinstance(records, dict):
        raise ValueError("W-C0034-005 缺 records")

    # 容器缺失或型別不符＝上游 schema 異常，不可當「無颱風」靜默 idle（review F193）；
    # 只有 TropicalCyclones 是 dict 且（空 dict 或 TropicalCyclone 為空）才算顯式無颱風。
    if "TropicalCyclones" not in records or not isinstance(records["TropicalCyclones"], dict):
        raise ValueError("W-C0034-005 缺 records.TropicalCyclones 或型別不符")
    tcs_node = records["TropicalCyclones"]
    if tcs_node and "TropicalCyclone" not in tcs_node:
        raise ValueError("W-C0034-005 TropicalCyclones 缺 TropicalCyclone 欄位")
    tc_value = tcs_node.get("TropicalCyclone")
    if tc_value is not None and not isinstance(tc_value, (list, dict)):
        raise ValueError("W-C0034-005 TropicalCyclone 型別不符")
    tcs = _as_list(tc_value)
    collected_iso = (collected_at or now).isoformat()

    rows: list[dict] = []
    storms: list[str] = []
    rejected = 0
    for tc in tcs:
        if not isinstance(tc, dict):
            continue
        year = int(to_float(tc.get("Year")) or 0) or None
        td_no = str(tc.get("CwaTdNo") or "").strip()
        name_en_raw = (tc.get("TyphoonName") or "").strip()
        name_en = name_en_raw.title() if name_en_raw else None
        name_local = (tc.get("CwaTyphoonName") or "").strip() or None
        if year is None:
            raise ValueError(f"颱風缺 Year：{name_en_raw or td_no}")
        if td_no.isdigit():
            sid = f"cwa{year % 100:02d}{int(td_no):02d}"
        elif name_en_raw:
            # 沒有 TD 編號時退用名稱（未實測到，保守處理；仍不以 TC 開頭）
            sid = f"cwa{year % 100:02d}n{name_en_raw.lower()}"
        else:
            raise ValueError("颱風同時缺 CwaTdNo 與 TyphoonName")
        skey = storm_key(year, name_en_raw)
        storms.append(sid)

        analysis = _children(tc.get("AnalysisData"), "Fix")
        forecast = _children(tc.get("ForecastData"), "Fix")

        init_time = None
        for f in forecast:
            init_time = _parse_dt(f.get("InitialTime")) if isinstance(f, dict) else None
            if init_time:
                break
        if init_time is None:
            # 沒有預報（例如剛生成）時以最新分析時刻當版次
            times = [_parse_dt(a.get("DateTime")) for a in analysis if isinstance(a, dict)]
            times = [t for t in times if t]
            init_time = max(times) if times else None
        if init_time is None:
            raise ValueError(f"{sid} 沒有 InitialTime 也沒有分析時刻")
        advisory_number = int(init_time.timestamp() // 60)
        advisory_iso = init_time.isoformat()

        def _row(fix: dict, point_type: str, valid_at: datetime) -> Optional[dict]:
            lat = to_float(fix.get("CoordinateLatitude"))
            lon = to_float(fix.get("CoordinateLongitude"))
            if not valid_position(lat, lon) or not valid_time(point_type, valid_at, now, init_time):
                return None
            wind_ms = to_float(fix.get("MaxWindSpeed"))
            gale_r, _ = _circle(fix.get("Circle15ms"))
            storm_r, _ = _circle(fix.get("Circle25ms"))
            wr = _wind_radii(fix)
            return {
                "storm_id":            sid,
                "source":              "cwa",
                "valid_at":            valid_at.isoformat(),
                "point_type":          point_type,
                "advisory_number":     advisory_number,
                "advisory_issued_at":  advisory_iso,
                "name_local":          name_local,
                "name_en":             name_en,
                "center_lat":          lat,
                "center_lon":          lon,
                "center_pressure_hpa": to_float(fix.get("Pressure")),
                "max_wind_kt":         ms_to_kt(wind_ms),
                "max_wind_ms":         wind_ms,
                "max_gust_ms":         to_float(fix.get("MaxGustSpeed")),
                "gale_radius_km":      gale_r,
                "storm_radius_km":     storm_r,
                "prob70_radius_km":    to_float(fix.get("Radius70PercentProbability")),
                "wind_radii":          json.dumps(wr, ensure_ascii=False) if wr else None,
                "storm_key":           skey,
                "lon":                 lon,
                "lat":                 lat,
                "raw_json":            json.dumps(fix, ensure_ascii=False),
                "collected_at":        collected_iso,
            }

        for fix in analysis:
            if not isinstance(fix, dict):
                continue
            vt = _parse_dt(fix.get("DateTime"))
            r = _row(fix, "observed", vt) if vt else None
            if r:
                rows.append(r)
            else:
                rejected += 1
        for fix in forecast:
            if not isinstance(fix, dict):
                continue
            it = _parse_dt(fix.get("InitialTime")) or init_time
            hr = to_float(fix.get("ForecastHour"))
            r = _row(fix, "forecast", it + timedelta(hours=hr)) if hr is not None else None
            if r:
                rows.append(r)
            else:
                rejected += 1

    return {"rows": rows, "storms": storms, "rejected": rejected, "no_active": not tcs}


class CwaTyphoonCollector(BaseCollector):
    """氣象署颱風路徑＋7／10 級風圈＋70% 機率圈。無颱風時 idle（不寫列、status=no_active_storm）。"""

    name = "global_climate_cwa_typhoon"
    interval_minutes = config.GLOBAL_CLIMATE_CWA_TYPHOON_INTERVAL

    def __init__(self):
        super().__init__()
        self.api_key = config.CWA_API_KEY
        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; global-climate-cwa-typhoon)",
            "Accept": "application/json",
        })

    def _fetch(self) -> dict:
        resp = self._session.get(
            f"{config.CWA_API_BASE}/v1/rest/datastore/{DATASET_ID}",
            params={"Authorization": self.api_key, "format": "JSON"},
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        parsed = parse_cwa_typhoons(self._fetch(), now)
        base = {
            "storm_count":   len(parsed["storms"]),
            "active_storms": parsed["storms"],
            "rejected_rows": parsed["rejected"],
            "collected_at":  now.isoformat(),
        }
        if parsed["no_active"]:
            # 刻意不帶 'data'：base.run() 不存檔、不寫 DB；status 讓日誌與 webhook 分得出 idle 與故障
            return {**base, "status": "no_active_storm", "point_count": 0}
        result = {**base, "status": "ok", "data": parsed["rows"], "point_count": len(parsed["rows"])}
        if not parsed["rows"]:
            result["_collector_error"] = (
                f"有 {len(parsed['storms'])} 顆颱風但 0 列通過守門（rejected={parsed['rejected']}）"
            )
        return result
