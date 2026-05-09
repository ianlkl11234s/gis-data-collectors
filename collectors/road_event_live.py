"""TDX RoadEvent 即時事件收集器（Live 系列）

3 個端點，5 分鐘抓一次：
  - /v1/Traffic/RoadEvent/LiveEvent/Freeway
  - /v1/Traffic/RoadEvent/LiveEvent/Highway
  - /v1/Traffic/RoadEvent/LiveEvent/City/{City}（10 縣市輪詢）

啟動時載入 Section 快取（freeway+highway），每筆事件做 normalize match 富化。
寫入 realtime.road_events（partition history） + realtime.road_events_current（snapshot）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import requests

import config
from utils.auth import TDXAuth
from utils.section_enricher import SectionEnricher
from utils.tdx_session import TDXSession
from .base import BaseCollector

# 已驗證有資料的 10 縣市（Phase 1 EDA）
DEFAULT_CITIES = [
    "Taipei", "NewTaipei", "Taoyuan", "Taichung", "Tainan",
    "Kaohsiung", "Keelung", "ChiayiCounty", "YilanCounty", "KinmenCounty",
]


class RoadEventLiveCollector(BaseCollector):
    """TDX RoadEvent LiveEvent — Freeway / Highway / City"""

    name = "road_event_live"
    interval_minutes = config.ROAD_EVENT_LIVE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)
        # 共享 enricher singleton（避免每個 collector 重抓 Section）
        self.enricher = SectionEnricher.get_instance(self._session, self.auth)

    def _fetch(self, path: str) -> list[dict]:
        url = f"{config.TDX_API_BASE}{path}"
        resp = self._session.get(
            url, headers=self.auth.get_auth_header(),
            params={"$format": "JSON"},
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("LiveEvents", [])

    def _fetch_with_skip_400(self, path: str, label: str) -> list[dict]:
        """City 系列：HTTP 400/404 視為該縣市未上架，skip 不報錯"""
        try:
            return self._fetch(path)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 404):
                return []
            raise

    def collect(self) -> dict:
        """收集所有 Live 事件並富化"""
        # 1. 確保 Section 快取已載入
        self.enricher.refresh()

        events: list[dict] = []
        per_source_count = {"live_freeway": 0, "live_highway": 0, "live_city": 0}

        # 2. Freeway
        for ev in self._fetch("/v1/Traffic/RoadEvent/LiveEvent/Freeway"):
            ev["_source"] = "live_freeway"
            events.append(ev)
        per_source_count["live_freeway"] = sum(1 for e in events if e["_source"] == "live_freeway")

        # 3. Highway（省道 + 快速公路）
        for ev in self._fetch("/v1/Traffic/RoadEvent/LiveEvent/Highway"):
            ev["_source"] = "live_highway"
            events.append(ev)
        per_source_count["live_highway"] = sum(1 for e in events if e["_source"] == "live_highway")

        # 4. City × N
        cities = config.ROAD_EVENT_CITIES
        for city in cities:
            ev_list = self._fetch_with_skip_400(
                f"/v1/Traffic/RoadEvent/LiveEvent/City/{city}", city
            )
            for ev in ev_list:
                ev["_source"] = "live_city"
                ev["_city"] = city
                events.append(ev)
        per_source_count["live_city"] = sum(1 for e in events if e["_source"] == "live_city")

        # 5. 富化 (collector 端執行 normalize+match)
        enrich_stats = {
            "matched": 0, "multi_match": 0, "km_out": 0,
            "no_section": 0, "no_road": 0, "no_km": 0,
            "pos_only": 0,
        }
        for ev in events:
            if ev["_source"] == "live_city":
                # 縣市事件無結構化路段欄位，標 pos_only 不做 join
                ev["_enrich"] = {"enrich_status": "pos_only"}
                enrich_stats["pos_only"] += 1
                continue
            feh = ((ev.get("Location") or {}).get("FreeExpressHighway")) or {}
            r = self.enricher.match(
                feh.get("Road"), feh.get("Direction"),
                feh.get("StartKM"), feh.get("EndKM"),
            )
            ev["_enrich"] = r
            enrich_stats[r["enrich_status"]] = enrich_stats.get(r["enrich_status"], 0) + 1

        total = len(events)
        matched = enrich_stats["matched"] + enrich_stats["multi_match"]
        non_city = total - per_source_count["live_city"]
        hit_rate = matched / non_city * 100 if non_city else 0

        print(f"   ✓ Freeway/Highway/City: {per_source_count}")
        print(f"   ✓ 富化命中率（非 city）: {hit_rate:.1f}% ({matched}/{non_city})")

        return {
            "fetch_time": datetime.now().isoformat(),
            "total": total,
            "per_source": per_source_count,
            "enrich_stats": enrich_stats,
            "data": events,
        }
