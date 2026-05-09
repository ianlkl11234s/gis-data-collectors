"""TDX RoadEvent 預告事件收集器（Event/City 系列）

1 個端點 × 10 縣市，每 12 小時抓一次：
  - /v1/Traffic/RoadEvent/Event/City/{City}

預告型事件（活動、施工排程）：
  - 80% 有 Geometry MULTIPOLYGON（活動範圍）
  - 69% 有 Positions POINT
  - 100% 有 ExpireTime（活動預定結束時間）
"""

from __future__ import annotations

from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


class RoadEventPlannedCollector(BaseCollector):
    """TDX RoadEvent Event/City — 預告 / 規劃中事件"""

    name = "road_event_planned"
    interval_minutes = config.ROAD_EVENT_PLANNED_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_city(self, city: str) -> list[dict]:
        url = f"{config.TDX_API_BASE}/v1/Traffic/RoadEvent/Event/City/{city}"
        try:
            resp = self._session.get(
                url, headers=self.auth.get_auth_header(),
                params={"$format": "JSON"},
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("Events", [])
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 404):
                return []
            raise

    def collect(self) -> dict:
        """收集預告事件（不做 Section 富化，預告事件多為活動範圍 polygon）"""
        events: list[dict] = []
        per_city: dict[str, int] = {}

        for city in config.ROAD_EVENT_CITIES:
            ev_list = self._fetch_city(city)
            per_city[city] = len(ev_list)
            for ev in ev_list:
                ev["_source"] = "event_city"
                ev["_city"] = city
                ev["_enrich"] = {"enrich_status": "pos_only"}
                events.append(ev)

        print(f"   ✓ event_city × {len(config.ROAD_EVENT_CITIES)}: total={len(events)}")
        for c, n in sorted(per_city.items(), key=lambda x: -x[1]):
            if n:
                print(f"     {c}: {n}")

        return {
            "fetch_time": datetime.now().isoformat(),
            "total": len(events),
            "per_city": per_city,
            "data": events,
        }
