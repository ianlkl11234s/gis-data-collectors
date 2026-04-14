"""
LASS AirBox 微型感測器資料收集器

LASS (Location Aware Sensing System) 是中研院主導的 PM2.5 微型感測開源社群，
與訊舟 AirBox 合作，每 ~5 分鐘更新 ~500 個活躍感測點（校園、社區、公共場所），
資料開放、免 API key。

端點: https://pm25.lass-net.org/data/last-all-airbox.json

欄位:
    s_d0  PM2.5 (μg/m³)
    s_d1  PM10
    s_d2  PM1.0
    s_t0  溫度
    s_h0  濕度
    gps_lat/gps_lon
    device_id
    SiteName / name
    area  (縣市英文名)

寫入: realtime.micro_sensor_readings，source='lass_airbox'

擴充：環境部微型感測物聯網 (10k+ 點) 原走 Civil IoT SensorThings API，
目前端點 sta.ci.taiwan.gov.tw 無法從公網連線；待確認後可加入 fetch_moenv_iot()
走同一 source='moenv_iot' 寫入同一張表。
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector

TAIPEI_TZ = timezone(timedelta(hours=8))

LASS_URL = "https://pm25.lass-net.org/data/last-all-airbox.json"


def _flt(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "", "N/A", "-") else None
    except (TypeError, ValueError):
        return None


class AirQualityMicroSensorCollector(BaseCollector):
    """LASS AirBox 微型感測器資料收集器"""

    name = "air_quality_microsensors"
    interval_minutes = config.AIR_QUALITY_MICROSENSORS_INTERVAL

    def __init__(self):
        super().__init__()
        self.outlier_pm25 = config.AIR_QUALITY_MICROSENSORS_PM25_OUTLIER
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (air-quality-microsensors)",
        })

    def _fetch_lass(self) -> list[dict]:
        resp = self._session.get(LASS_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        feeds = data.get("feeds") if isinstance(data, dict) else data
        if not isinstance(feeds, list):
            raise RuntimeError(f"LASS 回傳格式異常: {type(data).__name__}")
        return feeds

    def _normalize(self, rec: dict) -> Optional[dict]:
        lat = _flt(rec.get("gps_lat"))
        lon = _flt(rec.get("gps_lon"))
        # 台灣 bbox 粗過濾
        if lat is None or lon is None:
            return None
        if not (21.5 <= lat <= 26.5 and 119.0 <= lon <= 122.5):
            return None

        pm25 = _flt(rec.get("s_d0"))
        if pm25 is not None and pm25 > self.outlier_pm25:
            return None  # 離群點剔除

        return {
            "device_id": rec.get("device_id"),
            "source": "lass_airbox",
            "site_name": rec.get("SiteName") or rec.get("name"),
            "area": rec.get("area"),
            "app": rec.get("app"),
            "latitude": lat,
            "longitude": lon,
            "pm25": pm25,
            "pm10": _flt(rec.get("s_d1")),
            "pm1": _flt(rec.get("s_d2")),
            "temperature": _flt(rec.get("s_t0")),
            "humidity": _flt(rec.get("s_h0")),
            "observed_at": rec.get("timestamp"),
        }

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)
        raw = self._fetch_lass()

        records: list[dict] = []
        for r in raw:
            n = self._normalize(r)
            if n is not None and n.get("device_id"):
                records.append(n)

        pm25_vals = [r["pm25"] for r in records if r["pm25"] is not None]
        area_stats: dict[str, int] = {}
        for r in records:
            a = r.get("area") or "-"
            area_stats[a] = area_stats.get(a, 0) + 1

        print(f"[{self.name}]   ✓ {len(records)} 點 (raw {len(raw)})")
        if pm25_vals:
            print(f"[{self.name}]     PM25 min={min(pm25_vals):.1f} "
                  f"max={max(pm25_vals):.1f}")

        return {
            "fetch_time": fetch_time.isoformat(),
            "total_sensors": len(records),
            "raw_count": len(raw),
            "by_area": {a: n for a, n in sorted(area_stats.items(), key=lambda x: -x[1])[:8]},
            "data": records,
        }
