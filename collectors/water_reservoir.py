"""
水情體系收集器（Layer 1+2）

資料來源：經濟部水利署 WRA OpenData
  - 水庫水情資料（全台統一）: 每小時，~40 座水庫
  - 水庫基本資料: 靜態，每次啟動同步一次

寫入：
  - public.water_reservoirs       (靜態，UPSERT)
  - realtime.reservoir_status     (時序，INSERT ON CONFLICT DO NOTHING)

API 文件：https://opendata.wra.gov.tw/openapi/swagger/index.html
無需 API Key，公開免費。
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# ---------------------------------------------------------------------------
# WRA API endpoints
# ---------------------------------------------------------------------------

WRA_STATUS_URL = (
    "https://opendata.wra.gov.tw/api/v2/"
    "2be9044c-6e44-4856-aad5-dd108c2e6679"
    "?sort=_importdate+desc&format=JSON"
)
WRA_BASIC_URL = (
    "https://opendata.wra.gov.tw/api/v2/"
    "708a43b0-24dc-40b7-9ed2-fca6a291e7ae"
    "?format=JSON"
)

# ---------------------------------------------------------------------------
# 主要水庫靜態座標（WGS84，資料來源：水利署 GIS + 地圖比對）
# Key = reservoiridentifier (string)
# ---------------------------------------------------------------------------

RESERVOIR_COORDS: dict[str, tuple[float, float]] = {
    "10101": (24.944, 121.601),   # 翡翠水庫
    "10201": (24.843, 121.241),   # 石門水庫
    "10204": (25.115, 121.726),   # 新山水庫
    "10301": (24.789, 120.981),   # 寶山水庫
    "10302": (24.797, 120.989),   # 寶山第二水庫
    "10401": (24.944, 121.368),   # 桂山水庫
    "20101": (24.594, 120.825),   # 明德水庫
    "20201": (24.621, 120.837),   # 永和山水庫
    "20301": (24.192, 120.769),   # 鯉魚潭水庫（苗栗）
    "20401": (24.259, 121.056),   # 德基水庫
    "20402": (24.305, 120.853),   # 石岡壩
    "20501": (23.864, 120.909),   # 日月潭
    "20502": (24.047, 121.016),   # 霧社水庫
    "20503": (23.885, 120.867),   # 頭社水庫
    "20601": (23.818, 120.643),   # 集集攔河堰
    "20701": (23.666, 120.485),   # 湖山水庫
    "30101": (22.671, 120.337),   # 澄清湖
    "30201": (22.641, 120.367),   # 鳳山水庫
    "30301": (23.130, 120.617),   # 南化水庫
    "30401": (23.168, 120.389),   # 烏山頭水庫
    "30501": (23.363, 120.443),   # 白河水庫
    "30502": (23.253, 120.521),   # 曾文水庫
    "30601": (23.474, 120.503),   # 蘭潭水庫
    "30701": (23.445, 120.492),   # 仁義潭水庫
    "30801": (23.214, 120.315),   # 虎頭埤水庫
    "30901": (22.760, 120.380),   # 阿公店水庫
    "40101": (22.148, 120.793),   # 牡丹水庫
    "40201": (23.974, 121.563),   # 鯉魚潭（花蓮）
    "40301": (24.097, 121.568),   # 新城攔河堰
    "10503": (25.049, 121.432),   # 直潭壩
    "10502": (25.041, 121.423),   # 青潭堰
    "10203": (24.867, 121.260),   # 義興堰
}

# ---------------------------------------------------------------------------
# 輔助
# ---------------------------------------------------------------------------

def _flt(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    try:
        return int(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def _parse_dt(s: str | None) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%dT%H%M%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class WaterReservoirCollector(BaseCollector):
    """水利署水庫水情收集器"""

    name = "water_reservoir"
    interval_minutes = config.WATER_RESERVOIR_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (water-reservoir)",
            "Accept": "application/json",
        })
        self._basic_synced = False  # 每次程式啟動只同步一次靜態資料

    # ------------------------------------------------------------------
    # 靜態水庫基本資料（每次啟動同步一次）
    # ------------------------------------------------------------------

    def _fetch_basic(self) -> list[dict]:
        resp = self._session.get(WRA_BASIC_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def _upsert_basic(self, records: list[dict]) -> int:
        if not self.supabase_writer:
            return 0
        rows = []
        for r in records:
            rid = str(r.get("水庫代碼", "")).strip()
            if not rid:
                continue
            lat, lng = RESERVOIR_COORDS.get(rid, (None, None))
            # region: 10x=北, 20x=中, 30x=南, 40x=東, 50x=離島
            prefix = rid[:2] if len(rid) >= 2 else ""
            region_map = {"10": "北區", "20": "中區", "30": "南區", "40": "東區", "50": "離島"}
            region = region_map.get(prefix, r.get("地區別", ""))

            cap_str = str(r.get("設計有效容量", "") or "").replace(",", "")
            cur_cap_str = str(r.get("目前有效容量", "") or "").replace(",", "")
            catch_str = str(r.get("集水面積", "") or "").replace(",", "")

            rows.append({
                "id":                     rid,
                "name":                   r.get("水庫名稱", ""),
                "region":                 region,
                "river_name":             r.get("河川名稱", ""),
                "lat":                    lat,
                "lng":                    lng,
                "township":               r.get("鄉鎮市區名稱", ""),
                "dam_type":               r.get("型式", ""),
                "design_capacity_wan":    _flt(r.get("設計總容量", "").replace(",", "") if r.get("設計總容量") else None),
                "effective_capacity_wan": _flt(cap_str) if cap_str else None,
                "current_capacity_wan":   _flt(cur_cap_str) if cur_cap_str else None,
                "catchment_area_km2":     _flt(catch_str) if catch_str else None,
                "function_type":          r.get("功能", ""),
                "agency":                 r.get("機關名稱", ""),
                "updated_at":             datetime.now(tz=TAIPEI_TZ),
            })

        if rows:
            self.supabase_writer._upsert_water_reservoirs(rows)
        return len(rows)

    # ------------------------------------------------------------------
    # 即時水情（每小時）
    # ------------------------------------------------------------------

    def _fetch_status(self) -> list[dict]:
        resp = self._session.get(WRA_STATUS_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def _normalize_status(self, r: dict, collected_at: datetime) -> dict | None:
        rid = str(r.get("reservoiridentifier", "")).strip()
        if not rid:
            return None
        snapshot_at = _parse_dt(r.get("observationtime"))
        if not snapshot_at:
            return None
        return {
            "reservoir_id":              rid,
            "snapshot_at":               snapshot_at,
            "water_level_m":             _flt(r.get("waterlevel")),
            "effective_storage_wan_m3":  _flt(r.get("effectivewaterstoragecapacity")),
            "inflow_cms":                _flt(r.get("inflowdischarge")),
            "total_outflow_cms":         _flt(r.get("totaloutflow")),
            "spillway_outflow_cms":      _flt(r.get("spillwayoutflow")),
            "basin_rainfall_mm":         _flt(r.get("accumulaterainfallincatchment")),
            "hourly_rainfall_mm":        _flt(r.get("precipitationhourly")),
            "status_type":               _int(r.get("statustype")),
            "collected_at":              collected_at,
        }

    # ------------------------------------------------------------------
    # BaseCollector 介面
    # ------------------------------------------------------------------

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)

        # 靜態基本資料（每次啟動同步一次）
        basic_count = 0
        if not self._basic_synced:
            try:
                basic_data = self._fetch_basic()
                basic_count = self._upsert_basic(basic_data)
                self._basic_synced = True
            except Exception as e:
                print(f"[water_reservoir] 水庫基本資料同步失敗（非致命）: {e}")

        # 即時水情
        raw = self._fetch_status()
        status_rows = [
            row for r in raw
            if (row := self._normalize_status(r, now)) is not None
        ]

        return {
            "status_rows":   status_rows,
            "basic_synced":  basic_count,
            "collected_at":  now.isoformat(),
        }
