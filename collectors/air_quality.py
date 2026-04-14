"""
環境部 77 站即時空氣品質觀測收集器

資料集: data.moenv.gov.tw AQX_P_432 (空氣品質指標 AQI)
    每站每小時一筆，含 AQI + PM2.5/PM10/O3/O3_8hr/NO2/SO2/CO/NOx/風向風速

寫入:
    - realtime.air_quality_observations (分區表)
    - realtime.air_quality_current (最新快照)

註：站點靜態資料（名稱、類型、座標）由同 API 回傳，因此不需另一個 collector；
但 reference.stations 的 system='air_quality' 由 migration 初次匯入，
之後若有新站需另外維護。本 collector 只負責時序觀測。
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector

TAIPEI_TZ = timezone(timedelta(hours=8))

API_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_432"


NUMERIC_FIELDS = [
    "aqi", "so2", "co", "co_8hr", "o3", "o3_8hr",
    "pm10", "pm2.5", "pm10_avg", "pm2.5_avg", "so2_avg",
    "no2", "nox", "no", "wind_speed", "wind_direc",
    "longitude", "latitude",
]


def _flt(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


class AirQualityCollector(BaseCollector):
    """環境部 AQX_P_432 即時觀測收集器"""

    name = "air_quality"
    interval_minutes = config.AIR_QUALITY_INTERVAL

    def __init__(self):
        super().__init__()
        if not config.MOENV_API_KEY:
            raise ValueError("MOENV_API_KEY 未設定，無法使用 air_quality collector")
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (air-quality)",
        })

    def _fetch_all(self) -> list[dict]:
        """MOENV API v2 分頁拉完全部站點。"""
        offset = 0
        page = 1000
        out: list[dict] = []
        while True:
            resp = self._session.get(API_URL, params={
                "api_key": config.MOENV_API_KEY,
                "limit": page,
                "offset": offset,
            }, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            chunk = resp.json()
            if not isinstance(chunk, list) or not chunk:
                break
            out.extend(chunk)
            if len(chunk) < page:
                break
            offset += page
        return out

    def _normalize(self, rec: dict) -> dict:
        """字串 → float，把 pm2.5 → pm25 以對齊 DB 欄位。"""
        cleaned = dict(rec)
        for f in NUMERIC_FIELDS:
            if f in cleaned:
                cleaned[f] = _flt(cleaned[f])
        if "pm2.5" in cleaned:
            cleaned["pm25"] = cleaned.pop("pm2.5")
        if "pm2.5_avg" in cleaned:
            cleaned["pm25_avg"] = cleaned.pop("pm2.5_avg")
        return cleaned

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)
        raw = self._fetch_all()
        records = [self._normalize(r) for r in raw]

        # 取第一筆 publishtime 作為全體觀測時間
        observed_at = None
        for r in raw:
            pt = r.get("publishtime")
            if pt:
                try:
                    dt = datetime.strptime(pt, "%Y/%m/%d %H:%M:%S")
                    observed_at = dt.replace(tzinfo=TAIPEI_TZ).isoformat()
                    break
                except ValueError:
                    pass
        if observed_at is None:
            observed_at = fetch_time.replace(minute=0, second=0, microsecond=0).isoformat()

        # 統計
        aqi_vals = [r["aqi"] for r in records if r.get("aqi") is not None]
        pm25_vals = [r["pm25"] for r in records if r.get("pm25") is not None]
        county_stats: dict[str, int] = {}
        for r in records:
            c = r.get("county") or "-"
            county_stats[c] = county_stats.get(c, 0) + 1

        print(f"[{self.name}]   ✓ {len(records)} 站  obs@{observed_at[11:16]}")
        if aqi_vals:
            print(f"[{self.name}]     AQI {min(aqi_vals):.0f}~{max(aqi_vals):.0f}, "
                  f"PM25 {min(pm25_vals):.0f}~{max(pm25_vals):.0f}")

        return {
            "fetch_time": fetch_time.isoformat(),
            "observed_at": observed_at,
            "total_stations": len(records),
            "aqi_range": [min(aqi_vals), max(aqi_vals)] if aqi_vals else None,
            "by_county": {c: n for c, n in sorted(county_stats.items(), key=lambda x: -x[1])[:10]},
            "data": records,
        }
