"""
重度級急救責任醫院急診即時訊息收集器

資料來源：衛生福利部中央健康保險署（NHI）即時 API
  頁面：https://info.nhi.gov.tw/INAE4000/INAE4001S01
  端點：
    GET  /api/inae4000/inae4001s01/SQL0001  → 縣市清單
    GET  /api/inae4000/inae4001s01/SQL0003  → 醫院層級清單
    POST /api/inae4000/inae4001s01/SQL0002  {AREA_NO, CONT_TYPE} → 該縣市該層級的急診即時量能

  來源「整點」更新一次（sysdate 形如 2026-06-02 16:00），全台同一時刻。
  API 不提供歷史，只回當下快照 → 時序資料庫從上線起累積。
  全台約 59 家重度級 / 兒童急救責任醫院。

  ⚠ NHI 憑證缺 Subject Key Identifier，Python TLS 預設驗證會失敗，需 verify=False。

寫入：
  - realtime.er_hospital_status   (時序，UNIQUE(hosp_id, observed_at)，ON CONFLICT DO NOTHING)
  - realtime.er_hospital_current  (最新狀態，UPSERT by hosp_id)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

NHI_ER_BASE = "https://info.nhi.gov.tw/api/inae4000/inae4001s01"

# NHI 憑證缺 SKI，verify=False 後關閉警告噪音
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _int(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _parse_sysdate(s: str | None) -> Optional[datetime]:
    """sysdate 形如 '2026-06-02 16:00'（整點，無秒、無時區）"""
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


class ERHospitalRealtimeCollector(BaseCollector):
    """健保署重度級急救責任醫院急診即時訊息收集器（每 15 分鐘，對齊來源更新頻率）"""

    name = "er_hospital_realtime"
    interval_minutes = config.ER_HOSPITAL_REALTIME_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (er-hospital-realtime)",
            "Accept": "application/json",
            "Content-Type": "application/json;charset=UTF-8",
        })
        self._session.verify = False  # NHI 憑證缺 SKI

    def _get(self, path: str) -> list[dict]:
        resp = self._session.get(f"{NHI_ER_BASE}{path}", timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict) -> dict:
        resp = self._session.post(f"{NHI_ER_BASE}{path}", json=payload,
                                  timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _normalize(self, row: dict, area_name: str, level_name: str,
                   observed_at: datetime, collected_at: datetime) -> dict | None:
        hosp_id = (row.get("hosP_ID") or "").strip()
        if not hosp_id:
            return None
        return {
            "hosp_id":          hosp_id,
            "hosp_name":        row.get("hosP_NAME"),
            "area_no":          row.get("areA_NO_N"),
            "area_name":        area_name,
            "cont_type":        row.get("conT_TYPE"),
            "level_name":       level_name,
            "inform":           row.get("inform"),
            "wait_see_cnt":     _int(row.get("waiT_SEE_CNT")),      # 待看診
            "wait_bed_cnt":     _int(row.get("waiT_BED_CNT")),      # 待住院
            "wait_general_cnt": _int(row.get("waiT_GENERAL_CNT")),  # 等一般病床
            "wait_icu_cnt":     _int(row.get("waiT_ICU_CNT")),      # 等加護病床
            "source_url":       row.get("url"),
            "observed_at":      observed_at,
            "collected_at":     collected_at,
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)

        areas = self._get("/SQL0001")   # [{key:縣市, value:代碼, ...}]
        types = self._get("/SQL0003")   # [{value:代碼, key:層級名}]
        level_map = {t["value"]: t["key"] for t in types}

        hosp: dict[str, dict] = {}      # hosp_id -> row（去重，後寫覆蓋）
        combos_with_data = 0
        for a in areas:
            area_code = (a.get("value") or "").strip()
            if not area_code:           # 跳過「請選擇」
                continue
            area_name = a.get("key")
            for t in types:
                level_code = t["value"]
                try:
                    r = self._post("/SQL0002",
                                   {"AREA_NO": area_code, "CONT_TYPE": level_code})
                except requests.RequestException as e:
                    print(f"[{self.name}] ⚠ {area_name}/{level_map.get(level_code)} 抓取失敗: {e}")
                    continue
                observed_at = _parse_sysdate(r.get("sysdate")) or now
                rows = r.get("data", []) or []
                if rows:
                    combos_with_data += 1
                for row in rows:
                    n = self._normalize(row, area_name, level_map.get(level_code, level_code),
                                        observed_at, now)
                    if n:
                        hosp[n["hosp_id"]] = n

        rows = list(hosp.values())
        return {
            "data":           rows,
            "hospital_count": len(rows),
            "combo_count":    combos_with_data,
            "collected_at":   now.isoformat(),
        }
