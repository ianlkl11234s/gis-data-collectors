"""警政署即時交通事故 A1 收集器（24 小時內死亡）

資料來源：opdadm.moi.gov.tw（data.gov.tw nid 57023）
  端點：GET https://opdadm.moi.gov.tw/api/v1/no-auth/resource/api/dataset/{ds}/resource/{rid}/download
  回傳：{success, result:{total, 資料提供日期, 事故類別, records:[...]}}

  ⚠ 累積快照 — 每日全年累積（截至「資料提供日期」），不是 event push。
     dedup by (發生日期+時間+地點+當事者順位) hash → UNIQUE(dedup_hash) ON CONFLICT DO NOTHING
  ⚠ A2 量太大（~50 萬 row ZIP）暫不做。
  ⚠ 每筆事故 N 個當事者 → N row（party_order=1..N）

寫入：
  - realtime.traffic_accidents_a1 (UNIQUE dedup_hash, ON CONFLICT DO NOTHING)
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

A1_URL = (
    "https://opdadm.moi.gov.tw/api/v1/no-auth/resource/"
    "api/dataset/F4077949-50CC-4640-8114-79958CC8BBEA/"
    "resource/A3C4A73F-3119-41F9-AFF3-5E128847B68C/download"
)

LON_MIN, LON_MAX = 118.0, 122.5
LAT_MIN, LAT_MAX = 21.5, 26.5


def _int(v) -> Optional[int]:
    try: return int(str(v).strip().replace(",", ""))
    except (TypeError, ValueError): return None


def _float(v) -> Optional[float]:
    try: return float(str(v).strip())
    except (TypeError, ValueError): return None


def _parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """日期 20260101 + 時間 053700 → datetime"""
    if not date_str or not time_str:
        return None
    m_d = re.match(r"^(\d{4})(\d{2})(\d{2})$", date_str.strip())
    m_t = re.match(r"^(\d{2})(\d{2})(\d{2})$", time_str.strip().zfill(6))
    if not (m_d and m_t):
        return None
    try:
        return datetime(int(m_d.group(1)), int(m_d.group(2)), int(m_d.group(3)),
                        int(m_t.group(1)), int(m_t.group(2)), int(m_t.group(3)),
                        tzinfo=TAIPEI_TZ)
    except ValueError:
        return None


def _hash(date: str, time: str, location: str, party_order: str) -> str:
    s = f"{date}|{time}|{location}|{party_order}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


class NpaTrafficAccidentA1Collector(BaseCollector):
    """警政署即時 A1 交通事故收集器（每日 1-2 次足夠，cumulative dedup）"""

    name = "npa_traffic_accident_a1"
    interval_minutes = config.NPA_TRAFFIC_ACCIDENT_A1_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (npa-traffic-accident-a1)",
            "Accept": "application/json",
        })
        self._session.verify = False

    def _fetch(self) -> dict:
        resp = self._session.get(A1_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        payload = self._fetch()
        result = payload.get("result", {})
        records_raw = result.get("records", []) or []

        out: list[dict] = []
        skipped = 0
        for r in records_raw:
            date_s = (r.get("發生日期") or "").strip()
            time_s = (r.get("發生時間") or "").strip()
            location = (r.get("發生地點") or "").strip()
            party_order = (r.get("當事者順位") or "").strip()
            if not (date_s and time_s and location and party_order):
                skipped += 1; continue

            occurred = _parse_datetime(date_s, time_s)
            if occurred is None:
                skipped += 1; continue

            lat = _float(r.get("緯度"))
            lon = _float(r.get("經度"))
            if lat is not None and lon is not None and \
               not (LON_MIN <= lon <= LON_MAX and LAT_MIN <= lat <= LAT_MAX):
                lat, lon = None, None  # 超範圍視為缺失，但仍保留事故 row

            geom = f"SRID=4326;POINT({lon} {lat})" if (lat and lon) else None

            out.append({
                "accident_class":      "A1",
                "occurred_at":         occurred.isoformat(),
                "agency":              (r.get("處理單位名稱警局層") or "").strip() or None,
                "location":            location,
                "lat":                 lat,
                "lon":                 lon,
                "weather":             (r.get("天候名稱") or "").strip() or None,
                "light":               (r.get("光線名稱") or "").strip() or None,
                "road_type":           (r.get("道路類別-第1當事者-名稱") or "").strip() or None,
                "speed_limit":         _int(r.get("速限-第1當事者")),
                "accident_type_major": (r.get("事故類型及型態大類別名稱") or "").strip() or None,
                "accident_type_sub":   (r.get("事故類型及型態子類別名稱") or "").strip() or None,
                "cause_main_major":    (r.get("肇因研判大類別名稱-主要") or "").strip() or None,
                "cause_main_sub":      (r.get("肇因研判子類別名稱-主要") or "").strip() or None,
                "death_injury":        (r.get("死亡受傷人數") or "").strip() or None,
                "party_order":         _int(party_order),
                "party_type_major":    (r.get("當事者區分-類別-大類別名稱-車種") or "").strip() or None,
                "party_type_sub":      (r.get("當事者區分-類別-子類別名稱-車種") or "").strip() or None,
                "party_gender":        (r.get("當事者屬-性-別名稱") or "").strip() or None,
                "party_age":           _int(r.get("當事者事故發生時年齡")),
                "is_hit_and_run":      (r.get("肇事逃逸類別名稱-是否肇逃") or "").strip() or None,
                "dedup_hash":          _hash(date_s, time_s, location, party_order),
                "geom":                geom,
                "collected_at":        now.isoformat(),
            })

        return {
            "data":         out,
            "row_count":    len(out),
            "skipped":      skipped,
            "total_upstream": result.get("total"),
            "source_date":  result.get("資料提供日期"),
            "collected_at": now.isoformat(),
        }
