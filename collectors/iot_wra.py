"""
水利署 IoT 水文感測收集器

資料來源：經濟部水利署 IoT 水文感測平台（每小時更新）
  https://iot.wra.gov.tw/

涵蓋 7 種站點類型：
  - river              河川/區排水位站（1,634 站）
  - groundwater        地下水位監測站（765 站）
  - cumulativeflow     累計流量（671 站）
  - watergate          閘門（232 站）
  - erosiondepth       沖刷深度（228 站）
  - damstructure       堤防結構安全（44 站）
  - dustemission       揚塵（8 站）

寫入：
  - public.iot_wra_stations       （站點 metadata，upsert）
  - realtime.iot_wra_measurements （時序讀值，ON CONFLICT DO NOTHING）

備註：
  - 免授權（Swagger 聲稱需 Bearer，實際端點免授權回應）
  - 與現有 river_water_level.py / groundwater_level.py 的舊版 OpenData 並存；
    iot.wra 覆蓋更多站、統一 UUID，可與舊版比對交叉校驗
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Iterable

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ


IOT_WRA_BASE_URL = "https://iot.wra.gov.tw"

STATION_TYPES: list[tuple[str, str]] = [
    # (station_type, endpoint path)
    ("river",          "/river/stations"),
    # groundwater 跟舊版 groundwater_level.py 完全重複（同一批井，500m 內 95% 配對）。
    # 舊版多了 voltage（運維健康度）、address、metadata jsonb，資訊更豐富。
    # 改為只跑舊版，iot 已收的歷史資料保留在 DB。
    # ("groundwater",    "/groundwaterlevel/stations"),
    ("cumulativeflow", "/cumulativeflow/stations"),
    ("watergate",      "/watergate/stations"),
    ("erosiondepth",   "/erosiondepth/stations"),
    ("damstructure",   "/damstructure/stations"),
    ("dustemission",   "/dustemission/stations"),
]


def _flt(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def _parse_dt(s: str | None) -> Optional[datetime]:
    """解析 iot.wra 的 ISO8601 時間字串（含 +08:00 offset）。"""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Fallback: 嘗試其他格式
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s[:19], fmt)
                return dt.replace(tzinfo=TAIPEI_TZ)
            except ValueError:
                continue
        return None


class IotWraCollector(BaseCollector):
    """水利署 IoT 水文感測收集器（每 60 分鐘）"""

    name = "iot_wra"
    interval_minutes = config.IOT_WRA_INTERVAL
    # 7 個端點串行呼叫，預設 ~15s，留 2 倍餘裕
    COLLECT_TIMEOUT: int = 120

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (iot-wra)",
            "Accept": "application/json",
        })

    def _fetch(self, endpoint: str) -> list[dict]:
        resp = self._session.get(
            f"{IOT_WRA_BASE_URL}{endpoint}",
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def _normalize_station(self, r: dict, station_type: str, collected_at: datetime) -> dict | None:
        iow_id = r.get("IoWStationId")
        if not iow_id:
            return None
        lat = _flt(r.get("Latitude"))
        lng = _flt(r.get("Longtiude") or r.get("Longitude"))
        return {
            "iow_station_id": iow_id,
            "station_id":     r.get("StationId") or "",
            "station_type":   station_type,
            "name":           r.get("Name") or "",
            "county_code":    r.get("CountyCode") or "",
            "county_name":    r.get("CountyName") or "",
            "town_code":      r.get("TownCode") or "",
            "town_name":      r.get("TownName") or "",
            "basin_code":     r.get("BasinCode") or "",
            "basin_name":     r.get("BasinName") or "",
            "admin_name":     r.get("AdminName") or "",
            "hydro_station_type": r.get("HydroStationType"),
            "lat":            lat,
            "lng":            lng,
            "updated_at":     collected_at.isoformat(),
        }

    def _normalize_measurements(
        self,
        r: dict,
        station_type: str,
        collected_at: datetime,
    ) -> Iterable[dict]:
        iow_id = r.get("IoWStationId")
        if not iow_id:
            return
        for m in r.get("Measurements") or []:
            pq_id = m.get("IoWPhysicalQuantityId")
            ts    = _parse_dt(m.get("TimeStamp"))
            value = _flt(m.get("Value"))
            if not pq_id or not ts or value is None:
                continue
            yield {
                "iow_station_id":       iow_id,
                "physical_quantity_id": pq_id,
                "station_type":         station_type,
                "observed_at":          ts.isoformat(),
                "name":                 m.get("Name") or "",
                "full_name":            m.get("FullName") or "",
                "si_unit":              m.get("SIUnit") or "",
                "value":                value,
                "collected_at":         collected_at.isoformat(),
            }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        stations: list[dict] = []
        measurements: list[dict] = []
        per_type_counts: dict[str, dict] = {}

        for station_type, endpoint in STATION_TYPES:
            try:
                raw = self._fetch(endpoint)
            except Exception as e:
                print(f"[iot_wra] {station_type} 擷取失敗：{e}")
                per_type_counts[station_type] = {"stations": 0, "measurements": 0, "error": str(e)[:100]}
                continue

            s_count = 0
            m_count = 0
            for r in raw:
                s = self._normalize_station(r, station_type, now)
                if s:
                    stations.append(s)
                    s_count += 1
                for m in self._normalize_measurements(r, station_type, now):
                    measurements.append(m)
                    m_count += 1
            per_type_counts[station_type] = {"stations": s_count, "measurements": m_count}

        # 靜態 metadata：直接 upsert 到 public.iot_wra_stations（不進分區歷史表）
        if stations and self.supabase_writer:
            try:
                self.supabase_writer._upsert_iot_wra_stations(stations)
            except Exception as e:
                print(f"[iot_wra] 站點 metadata upsert 失敗：{e}")

        return {
            "data":               measurements,   # 時序讀值 → realtime.iot_wra_measurements
            "per_type":           per_type_counts,
            "total_stations":     len(stations),
            "total_measurements": len(measurements),
            "collected_at":       now.isoformat(),
        }
