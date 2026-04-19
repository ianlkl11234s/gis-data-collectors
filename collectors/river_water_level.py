"""
河川水位即時收集器

資料來源：經濟部水利署 WRA OpenData（每 10 分鐘更新）
  - 即時水位資料（data.gov.tw nid=25768）
  - UUID: 73c4c3de-4045-4765-abeb-89f9f9cd5ff0
  - 無需 API Key，公開免費

寫入：
  - realtime.river_water_level  (時序，ON CONFLICT DO NOTHING)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

WRA_RIVER_LEVEL_URL = (
    "https://opendata.wra.gov.tw/api/v2/"
    "73c4c3de-4045-4765-abeb-89f9f9cd5ff0"
    "?sort=_importdate+desc&format=JSON"
)


def _flt(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def _parse_dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


class RiverWaterLevelCollector(BaseCollector):
    """水利署即時河川水位收集器（每 10 分鐘）"""

    name = "river_water_level"
    interval_minutes = config.RIVER_WATER_LEVEL_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (river-water-level)",
            "Accept": "application/json",
        })

    def _fetch(self) -> list[dict]:
        resp = self._session.get(WRA_RIVER_LEVEL_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def _normalize(self, r: dict, collected_at: datetime) -> dict | None:
        station_id = str(r.get("stationid", "")).strip()
        if not station_id:
            return None
        observed_at = _parse_dt(r.get("datetime"))
        if not observed_at:
            return None
        water_level = _flt(r.get("waterlevel"))
        if water_level is None:
            return None
        try:
            check_result = int(r.get("checkresult", 1))
        except (TypeError, ValueError):
            check_result = None
        return {
            "station_id":    station_id,
            "observed_at":   observed_at,
            "water_level_m": water_level,
            "check_result":  check_result,
            "collected_at":  collected_at,
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        raw = self._fetch()
        rows = [
            row for r in raw
            if (row := self._normalize(r, now)) is not None
        ]
        return {
            "data":         rows,
            "station_count": len(set(r["station_id"] for r in rows)),
            "collected_at": now.isoformat(),
        }
