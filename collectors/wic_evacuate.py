"""
臺北市疏散門即時監測收集器

資料來源：臺北市政府工務局水利工程處 OpenData
  端點：GET https://wic.gov.taipei/OpenData/API/Evacuate/Get
  query：stationNo=&loginId=watergate&dataKey=44D76DA6
  認證：無金鑰
  頻率：10 分鐘

⚠️ catalog 給的 host wic.heo.taipei 會 timeout，改用 wic.gov.taipei 同 path 即可。

覆蓋：臺北市 35 站河川疏散門
欄位語意：
  - fo* = 全開（full open）  + / -
  - fc* = 全閉（full close） + / -
  - flt* = 故障（fault）     + / -
  - gateNum = 1 → 只有 *01 欄位有值；gateNum = 2 → *02 才有值

寫入：
  - public.taipei_evacuate_stations    （站點 metadata，upsert）
  - realtime.taipei_evacuate_status    （時序狀態，ON CONFLICT DO NOTHING）
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# 政府憑證缺 SKI（同 NHI ER / USWG 坑）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_LOGIN_ID = "watergate"
DEFAULT_DATA_KEY = "44D76DA6"
ENDPOINT = "https://wic.gov.taipei/OpenData/API/Evacuate/Get"


def _parse_rec_time(s: Optional[str]) -> Optional[datetime]:
    """Evacuate recTime = YYYYMMDDHHMMSS (14 chars) → tz-aware datetime"""
    if not s or len(s) < 12:
        return None
    try:
        dt = datetime.strptime(s[:14] if len(s) >= 14 else s[:12], "%Y%m%d%H%M%S" if len(s) >= 14 else "%Y%m%d%H%M")
        return dt.replace(tzinfo=TAIPEI_TZ)
    except ValueError:
        return None


def _norm_flag(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s in ("+", "-") else None


class WicEvacuateCollector(BaseCollector):
    """臺北市疏散門即時收集器"""

    name = "wic_evacuate"
    interval_minutes = getattr(config, "WIC_EVACUATE_INTERVAL", 10)
    COLLECT_TIMEOUT: int = 30

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (wic_evacuate)",
            "Accept": "application/json",
        })
        self._login_id = os.environ.get("WIC_TAIPEI_EVAC_LOGIN_ID") or DEFAULT_LOGIN_ID
        self._data_key = os.environ.get("WIC_TAIPEI_EVAC_DATA_KEY") or DEFAULT_DATA_KEY

    def _fetch(self) -> list[dict]:
        resp = self._session.get(
            ENDPOINT,
            params={"stationNo": "", "loginId": self._login_id, "dataKey": self._data_key},
            timeout=20,
            verify=False,
        )
        resp.raise_for_status()
        return resp.json().get("data") or []

    def _normalize_station(self, r: dict) -> Optional[dict]:
        no = r.get("stationNo")
        if not no:
            return None
        return {
            "station_no":   no,
            "station_name": r.get("stationName") or "",
            "gate_num":     r.get("gateNum"),
        }

    def _normalize_status(self, r: dict, collected_at: datetime) -> Optional[dict]:
        no = r.get("stationNo")
        ts = _parse_rec_time(r.get("recTime"))
        if not no or not ts:
            return None
        return {
            "station_no":   no,
            "observed_at":  ts.isoformat(),
            "fo01":  _norm_flag(r.get("fo01")),
            "fc01":  _norm_flag(r.get("fc01")),
            "flt01": _norm_flag(r.get("flt01")),
            "fo02":  _norm_flag(r.get("fo02")),
            "fc02":  _norm_flag(r.get("fc02")),
            "flt02": _norm_flag(r.get("flt02")),
            "collected_at": collected_at.isoformat(),
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        try:
            raw = self._fetch()
        except Exception as e:
            print(f"[{self.name}] 擷取失敗：{e}")
            return {
                "data": [],
                "total_stations": 0,
                "total_status": 0,
                "error": str(e)[:200],
                "collected_at": now.isoformat(),
            }

        stations, status_rows = [], []
        for r in raw:
            s = self._normalize_station(r)
            if s:
                stations.append(s)
            m = self._normalize_status(r, now)
            if m:
                status_rows.append(m)

        if stations and self.supabase_writer:
            try:
                self.supabase_writer._upsert_taipei_evacuate_stations(stations)
            except Exception as e:
                print(f"[{self.name}] 站點 metadata upsert 失敗：{e}")

        return {
            "data": status_rows,
            "total_stations": len(stations),
            "total_status": len(status_rows),
            "collected_at": now.isoformat(),
        }


def _dry_run() -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    print("=" * 60)
    print("WicEvacuate Collector — DRY RUN（不寫 DB）")
    print("=" * 60)
    coll = WicEvacuateCollector()
    coll.supabase_writer = None

    t0 = time.time()
    print(f"\n[1/3] Fetch {ENDPOINT} …")
    try:
        raw = coll._fetch()
        print(f"      ✅ {len(raw)} 站")
    except Exception as e:
        print(f"      ❌ {e}")
        return 1

    now = datetime.now(tz=TAIPEI_TZ)
    stations = [s for s in (coll._normalize_station(r) for r in raw) if s]
    status_rows = [m for m in (coll._normalize_status(r, now) for r in raw) if m]
    print(f"\n[2/3] Parsed: stations={len(stations)} status={len(status_rows)}")

    # 統計閘門狀態
    from collections import Counter
    fo_cnt = Counter()
    fc_cnt = Counter()
    flt_cnt = Counter()
    for m in status_rows:
        for k, c in (("fo01", fo_cnt), ("fc01", fc_cnt), ("flt01", flt_cnt)):
            v = m.get(k)
            if v:
                c[v] += 1
    print(f"      gate1: fo={dict(fo_cnt)}  fc={dict(fc_cnt)}  flt={dict(flt_cnt)}")

    print(f"\n[3/3] Sample first record:")
    if status_rows:
        import json
        print(json.dumps(status_rows[0], ensure_ascii=False, indent=2))

    print(f"\n[done] 耗時 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(_dry_run())
    print("WicEvacuate Collector module. Use --dry-run to test fetch+parse without DB write.")
    sys.exit(0)
