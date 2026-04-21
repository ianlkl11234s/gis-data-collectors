"""
水庫每日營運狀況 collector（WRA OpenData 41568）

資料來源：經濟部水利署
  - UUID: 51023e88-4c76-4dbc-bbb9-470da690d539
  - 更新頻率：每日 09:30 前各水庫管理單位輸入
  - 欄位：reservoiridentifier / reservoirname / datetime / capacity / dwl /
         nwlmax / basinrainfall / inflow / crossflow / outflowdischarge /
         outflowtotal / regulatorydischarge / outflow
  - 單位：容量類為萬立方公尺，雨量 mm

寫入：
  - realtime.reservoir_daily_ops  (時序，ON CONFLICT (reservoir_id, observed_at) DO NOTHING)

與 water_reservoir（45501 每小時）互補：本 collector 取每日統計量
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

WRA_DAILY_OPS_URL = (
    "https://opendata.wra.gov.tw/api/v2/"
    "51023e88-4c76-4dbc-bbb9-470da690d539"
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
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


class WaterReservoirDailyOpsCollector(BaseCollector):
    """水利署水庫每日營運收集器（預設每日一次，1440 分）"""

    name = "water_reservoir_daily_ops"
    interval_minutes = config.WATER_RESERVOIR_DAILY_OPS_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (water-reservoir-daily-ops)",
            "Accept": "application/json",
        })

    def _fetch(self) -> list[dict]:
        resp = self._session.get(WRA_DAILY_OPS_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def _normalize(self, r: dict, collected_at: datetime) -> dict | None:
        rid = str(r.get("reservoiridentifier", "")).strip()
        if not rid:
            return None
        observed_at = _parse_dt(r.get("datetime"))
        if not observed_at:
            return None
        return {
            "reservoir_id":             rid,
            "reservoir_name":           str(r.get("reservoirname", "")).strip() or None,
            "observed_at":              observed_at,
            "effective_capacity_wan":   _flt(r.get("capacity")),
            "dead_water_level_m":       _flt(r.get("dwl")),
            "normal_water_level_max":   _flt(r.get("nwlmax")),
            "basin_rainfall_mm":        _flt(r.get("basinrainfall")),
            "inflow_wan_m3":            _flt(r.get("inflow")),
            "crossflow_wan_m3":         _flt(r.get("crossflow")),
            "outflow_discharge_wan":    _flt(r.get("outflowdischarge")),
            "outflow_total_wan":        _flt(r.get("outflowtotal")),
            "regulatory_discharge_wan": _flt(r.get("regulatorydischarge")),
            "outflow_wan":              _flt(r.get("outflow")),
            "collected_at":             collected_at,
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        raw = self._fetch()
        rows = [
            row for r in raw
            if (row := self._normalize(r, now)) is not None
        ]
        return {
            "data":             rows,
            "reservoir_count":  len(set(r["reservoir_id"] for r in rows)),
            "collected_at":     now.isoformat(),
        }
