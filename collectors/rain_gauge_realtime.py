"""
CWA 即時雨量站收集器

資料來源：中央氣象署 CWA OpenData（每 10 分鐘更新）
  - 自動雨量站-雨量觀測資料  O-A0002-001
  - 需要 CWA_API_KEY

寫入：
  - realtime.rain_gauge_readings  (時序，ON CONFLICT DO NOTHING)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

CWA_RAIN_URL = f"{config.CWA_API_BASE}/v1/rest/datastore/O-A0002-001"


def _flt(v) -> Optional[float]:
    try:
        f = float(v)
        return None if f < -990 else f  # CWA 用 -99x 表示無效值
    except (TypeError, ValueError):
        return None


def _parse_dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s.replace("+08:00", ""), fmt.replace("%z", ""))
            return dt.replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


class RainGaugeRealtimeCollector(BaseCollector):
    """中央氣象署自動雨量站即時資料收集器（每 10 分鐘）"""

    name = "rain_gauge_realtime"
    interval_minutes = config.RAIN_GAUGE_REALTIME_INTERVAL

    def __init__(self):
        super().__init__()
        if not config.CWA_API_KEY:
            raise ValueError("CWA_API_KEY 未設定")
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (rain-gauge-realtime)",
            "Accept": "application/json",
        })

    def _fetch(self) -> list[dict]:
        resp = self._session.get(
            CWA_RAIN_URL,
            params={"Authorization": config.CWA_API_KEY, "format": "JSON"},
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") not in ("true", True):
            raise ValueError(f"CWA API 回傳失敗: {data.get('message', data)}")
        return data.get("records", {}).get("Station", [])

    def _normalize(self, s: dict, collected_at: datetime) -> dict | None:
        station_id = s.get("StationId", "").strip()
        if not station_id:
            return None

        geo = s.get("GeoInfo", {})
        coords = geo.get("Coordinates", [])
        coord = coords[0] if coords else {}
        lat = _flt(coord.get("StationLatitude"))
        lng = _flt(coord.get("StationLongitude"))

        # O-A0002-001 可能用 RainfallElement 或 WeatherElement
        rain = s.get("RainfallElement") or s.get("WeatherElement", {})

        obs_time_str = (
            rain.get("ObservationTime")
            or s.get("ObsTime", {}).get("DateTime")
        )
        observed_at = _parse_dt(obs_time_str)
        if not observed_at:
            observed_at = collected_at

        def _precip(key: str) -> Optional[float]:
            return _flt(rain.get(key, {}).get("Precipitation") if isinstance(rain.get(key), dict)
                        else rain.get(key))

        return {
            "station_id":          station_id,
            "station_name":        s.get("StationName"),
            "county":              geo.get("CountyName"),
            "town":                geo.get("TownName"),
            "lat":                 lat,
            "lng":                 lng,
            "precipitation_10min": _precip("Now") or _precip("Past10min"),
            "precipitation_1hr":   _precip("Past1hr"),
            "precipitation_3hr":   _precip("Past3hr"),
            "precipitation_6hr":   _precip("Past6hr"),
            "precipitation_12hr":  _precip("Past12hr"),
            "precipitation_24hr":  _precip("Past24hr"),
            "observed_at":         observed_at,
            "collected_at":        collected_at,
            "geom":                f"SRID=4326;POINT({lng} {lat})" if lat and lng else None,
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        raw = self._fetch()
        rows = [
            row for s in raw
            if (row := self._normalize(s, now)) is not None
        ]
        return {
            "data":          rows,
            "station_count": len(rows),
            "collected_at":  now.isoformat(),
        }
