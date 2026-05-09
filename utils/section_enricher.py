"""TDX RoadEvent → Section 富化工具

把事件的 (Road, Direction, StartKM) 對到 freeway_sections / highway_sections 的 SectionID，
讓前端能 join LineString 高亮整段路。

設計：
- 啟動時載入 Section 快取（freeway + highway），每 24 hr refresh
- 共享 TDX session（不會吃 freeway_vd 的 4 req/s budget）
- 命中率（Phase 1 驗證）：freeway 84.6%、highway 72.6%
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Optional

import config
from utils.tdx_session import TDXSession

logger = logging.getLogger(__name__)

# Normalize 規則
_CN2NUM = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
           "六": "6", "七": "7", "八": "8", "九": "9", "十": "10"}
_DIR_MAP = {"北": "N", "南": "S", "東": "E", "西": "W",
            "北向": "N", "南向": "S", "東向": "E", "西向": "W", "雙向": "B"}

_KM_RE = re.compile(r"(\d+)K?\+(\d+)")

# Section 快取 refresh 週期（24 hr，TDX SrcUpdateInterval=86400）
_REFRESH_SEC = 24 * 3600


def norm_road(r: Optional[str]) -> str:
    if not r:
        return ""
    out = r
    for cn, num in _CN2NUM.items():
        out = out.replace(cn, num)
    return out.replace("號", "").replace("線", "").strip()


def norm_dir(d: Optional[str]) -> str:
    if not d:
        return ""
    return _DIR_MAP.get(d.strip(), d.strip().upper())


def parse_km(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = _KM_RE.match(s.strip())
    return float(m.group(1)) + float(m.group(2)) / 1000 if m else None


class SectionEnricher:
    """Section 快取 + match 邏輯。Thread-safe singleton。"""

    _instance: "Optional[SectionEnricher]" = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, session: TDXSession, auth) -> "SectionEnricher":
        with cls._lock:
            if cls._instance is None:
                cls._instance = SectionEnricher(session, auth)
            return cls._instance

    def __init__(self, session: TDXSession, auth):
        self._session = session
        self._auth = auth
        self._index: dict[tuple[str, str], list[tuple[float, float, dict]]] = {}
        self._last_refresh = 0.0
        self._refresh_lock = threading.Lock()

    def _fetch_sections(self, path: str) -> list[dict]:
        url = f"{config.TDX_API_BASE}{path}"
        resp = self._session.get(
            url, headers=self._auth.get_auth_header(),
            params={"$format": "JSON"},
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json().get("Sections", [])

    def refresh(self, force: bool = False) -> None:
        """重抓 Section 快取（每 24 hr 一次）"""
        now = time.monotonic()
        with self._refresh_lock:
            if not force and (now - self._last_refresh) < _REFRESH_SEC and self._index:
                return
            try:
                fw = self._fetch_sections("/v2/Road/Traffic/Section/Freeway")
                hw = self._fetch_sections("/v2/Road/Traffic/Section/Highway")
            except Exception as e:
                logger.warning(f"[section_enricher] refresh failed: {e}")
                return

            idx: dict[tuple[str, str], list[tuple[float, float, dict]]] = {}
            for s in fw + hw:
                rn = norm_road(s.get("RoadName"))
                rd = norm_dir(s.get("RoadDirection"))
                sm = s.get("SectionMile") or {}
                sk = parse_km(sm.get("StartKM"))
                ek = parse_km(sm.get("EndKM"))
                if not rn or sk is None or ek is None:
                    continue
                idx.setdefault((rn, rd), []).append(
                    (min(sk, ek), max(sk, ek), s)
                )
            self._index = idx
            self._last_refresh = now
            logger.info(
                f"[section_enricher] refreshed: "
                f"{len(fw)} freeway + {len(hw)} highway sections, "
                f"{len(idx)} (road,dir) groups"
            )

    def match(self, road: Optional[str], direction: Optional[str],
              start_km_str: Optional[str], end_km_str: Optional[str] = None) -> dict:
        """
        回傳 {enrich_status, matched_section_id?, matched_section_name?,
              matched_road_id?, start_km?, end_km?}
        """
        # Refresh 檢查
        if (time.monotonic() - self._last_refresh) > _REFRESH_SEC or not self._index:
            self.refresh()

        if not road:
            return {"enrich_status": "no_road"}

        rn = norm_road(road)
        rd = norm_dir(direction)
        sk = parse_km(start_km_str)
        ek = parse_km(end_km_str)

        if not rn:
            return {"enrich_status": "no_road"}
        if sk is None:
            return {"enrich_status": "no_km", "start_km": sk, "end_km": ek}

        # 候選池
        if rd == "B" or not rd:
            # 雙向 / 無方向 → 不限方向
            candidates = [c for (r2, _d), lst in self._index.items() if r2 == rn for c in lst]
        else:
            candidates = self._index.get((rn, rd), [])
            if not candidates:
                # fallback：找其他方向
                candidates = [c for (r2, _d), lst in self._index.items() if r2 == rn for c in lst]

        if not candidates:
            return {"enrich_status": "no_section",
                    "start_km": sk, "end_km": ek}

        matches = [c for c in candidates if c[0] <= sk <= c[1]]
        if not matches:
            return {"enrich_status": "km_out",
                    "start_km": sk, "end_km": ek}

        sec = matches[0][2]
        return {
            "enrich_status": "matched" if len(matches) == 1 else "multi_match",
            "matched_section_id": sec.get("SectionID"),
            "matched_section_name": sec.get("SectionName"),
            "matched_road_id": sec.get("RoadID"),
            "start_km": sk,
            "end_km": ek,
        }
