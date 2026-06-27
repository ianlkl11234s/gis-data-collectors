"""移民署機場入出境 APIS 收集器（免金鑰）

資料來源：opendata.immigration.gov.tw/APIS/{code}
  端點清單（探勘後實際 active 6 個）：
    TPE1   桃園機場入境
    TPE5   桃園機場出境
    TPE51  桃園機場 T2 入境（細分航廈）
    TPE52  桃園機場 T2 出境
    RMQ5   臺中機場出境
    TSA1   松山機場入境

  ⚠ 來源是「當下細格快照」— 每細格 (航廈×in_out×性別×國籍×年齡段) 對應人數 paxCnt。
  ⚠ 來源不提供時間戳，整批 snapshot 用 collected_at 標記。
  ⚠ 4 個港口端點（TWKEL1/5、TWKHH5、TWHUN5）schema 完全不同（船班粒度+時間戳），
     不在本 collector 範圍，後續若做改建獨立 collector。

  港口端點 (TWKEL1 等) catalog: docs/api-platforms/immigration/endpoints.yaml
  data.gov.tw nid 對應：88851 / 88856 / 167537 / 167549 / 88857 / 88769

寫入：
  - realtime.border_airport_snapshot (append-only by collected_at)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APIS_BASE = "https://opendata.immigration.gov.tw/APIS"

# 6 個 active 端點（探勘 2026-06-28 確認 HTTP 200）
ENDPOINTS = [
    {"code": "TPE1",  "airport": "TPE", "terminal": None, "in_out": "in"},
    {"code": "TPE5",  "airport": "TPE", "terminal": None, "in_out": "out"},
    {"code": "TPE51", "airport": "TPE", "terminal": "1",  "in_out": "in"},
    {"code": "TPE52", "airport": "TPE", "terminal": "2",  "in_out": "out"},
    {"code": "RMQ5",  "airport": "RMQ", "terminal": None, "in_out": "out"},
    {"code": "TSA1",  "airport": "TSA", "terminal": None, "in_out": "in"},
]


def _int(v) -> Optional[int]:
    try: return int(str(v).strip().replace(",", ""))
    except (TypeError, ValueError): return None


class ImmigrationApisAirportCollector(BaseCollector):
    """機場入出境 demographic snapshot（每小時一次足夠，來源每日聚合）"""

    name = "immigration_apis_airport"
    interval_minutes = config.IMMIGRATION_APIS_AIRPORT_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (immigration-apis-airport)",
            "Accept": "application/json",
        })
        self._session.verify = False

    def _fetch(self, code: str) -> list[dict]:
        resp = self._session.get(f"{APIS_BASE}/{code}", timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        try:
            return resp.json() or []
        except Exception:
            return []

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        records: list[dict] = []
        endpoint_stats: dict[str, int] = {}
        failed: list[str] = []

        for ep in ENDPOINTS:
            code = ep["code"]
            try:
                rows = self._fetch(code)
            except requests.RequestException as e:
                failed.append(code)
                print(f"[{self.name}] ⚠ {code} 抓取失敗: {e}")
                continue

            for r in rows:
                pax = _int(r.get("paxCnt"))
                if pax is None or pax <= 0:
                    continue
                # 來源 inOutTransit: 1=入境 5=出境 其他=轉機
                io_code = (r.get("inOutTransit") or "").strip()
                io_label = "in" if io_code == "1" else ("out" if io_code == "5" else "transit")
                records.append({
                    "airport":       (r.get("airport") or ep["airport"] or "").strip() or None,
                    "terminal":      (r.get("terminal") or ep["terminal"] or "").strip() or None,
                    "in_out":        io_label,
                    "in_out_code":   io_code or None,
                    "gender":        (r.get("gender") or "").strip() or None,
                    "nationality":   (r.get("nationality") or "").strip() or None,
                    "age_band":      (r.get("age") or "").strip() or None,
                    "pax_count":     pax,
                    "endpoint_code": code,
                    "collected_at":  now.isoformat(),
                })
            endpoint_stats[code] = sum(1 for r in rows if _int(r.get("paxCnt", 0)))

        return {
            "data":           records,
            "row_count":      len(records),
            "endpoint_stats": endpoint_stats,
            "failed":         failed,
            "collected_at":   now.isoformat(),
        }
