"""
臺北市雨水下水道水位站即時資料收集器

資料來源：臺北市政府工務局水利工程處 OpenData
  端點：GET https://wic.gov.taipei/OpenData/API/Sewer/Get
  query：stationNo=&loginId=sewer01&dataKey=BD3E513A（公開於 data.taipei）
  認證：無金鑰（query 帶固定 loginId+dataKey）
  頻率：10 分鐘

覆蓋：臺北市 233 站雨水下水道水位
寫入：
  - public.taipei_sewer_stations         （站點 metadata，upsert）
  - live.taipei_sewer_measurements   （時序讀值，ON CONFLICT DO NOTHING）

Standalone usage（dry-run，不寫 DB）：
  cd data-collectors
  python3 -m collectors.wic_sewer --dry-run
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

DEFAULT_LOGIN_ID = "sewer01"
DEFAULT_DATA_KEY = "BD3E513A"
ENDPOINT = "https://wic.gov.taipei/OpenData/API/Sewer/Get"


def _flt(v) -> Optional[float]:
    if v is None or v == "" or v == "-":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_rec_time(s: Optional[str]) -> Optional[datetime]:
    """Sewer recTime = YYYYMMDDHHMM (12 chars, 無秒) → tz-aware datetime"""
    if not s or len(s) < 12:
        return None
    try:
        dt = datetime.strptime(s[:12], "%Y%m%d%H%M")
        return dt.replace(tzinfo=TAIPEI_TZ)
    except ValueError:
        return None


class WicSewerCollector(BaseCollector):
    """臺北市雨水下水道水位收集器"""

    name = "wic_sewer"
    interval_minutes = getattr(config, "WIC_SEWER_INTERVAL", 10)
    COLLECT_TIMEOUT: int = 30

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (wic_sewer)",
            "Accept": "application/json",
        })
        self._login_id = os.environ.get("WIC_TAIPEI_SEWER_LOGIN_ID") or DEFAULT_LOGIN_ID
        self._data_key = os.environ.get("WIC_TAIPEI_SEWER_DATA_KEY") or DEFAULT_DATA_KEY

    def _fetch(self) -> list[dict]:
        resp = self._session.get(
            ENDPOINT,
            params={"stationNo": "", "loginId": self._login_id, "dataKey": self._data_key},
            timeout=20,
            verify=False,
        )
        resp.raise_for_status()
        j = resp.json()
        return j.get("data") or []

    def _normalize_station(self, r: dict) -> Optional[dict]:
        no = r.get("stationNo")
        if not no:
            return None
        return {
            "station_no":   no,
            "station_name": r.get("stationName") or "",
        }

    def _normalize_measurement(self, r: dict, collected_at: datetime) -> Optional[dict]:
        no = r.get("stationNo")
        ts = _parse_rec_time(r.get("recTime"))
        if not no or not ts:
            return None
        return {
            "station_no":   no,
            "observed_at":  ts.isoformat(),
            "level_out":    _flt(r.get("levelOut")),
            "ground_far":   _flt(r.get("groundFar")),
            "voltage":      _flt(r.get("voltage")),
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
                "total_measurements": 0,
                "error": str(e)[:200],
                "collected_at": now.isoformat(),
            }

        stations, measurements = [], []
        for r in raw:
            s = self._normalize_station(r)
            if s:
                stations.append(s)
            m = self._normalize_measurement(r, now)
            if m:
                measurements.append(m)

        if stations and self.supabase_writer:
            try:
                self.supabase_writer._upsert_taipei_sewer_stations(stations)
            except Exception as e:
                print(f"[{self.name}] 站點 metadata upsert 失敗：{e}")

        return {
            "data": measurements,
            "total_stations": len(stations),
            "total_measurements": len(measurements),
            "collected_at": now.isoformat(),
        }


def _dry_run() -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    print("=" * 60)
    print("WicSewer Collector — DRY RUN（不寫 DB）")
    print("=" * 60)
    coll = WicSewerCollector()
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
    measurements = [m for m in (coll._normalize_measurement(r, now) for r in raw) if m]
    print(f"\n[2/3] Parsed: stations={len(stations)}  measurements={len(measurements)}")

    if measurements:
        ts = [m["observed_at"] for m in measurements]
        print(f"      observed_at: earliest={min(ts)}  latest={max(ts)}")
        lv = [m["level_out"] for m in measurements if m["level_out"] is not None]
        if lv:
            print(f"      level_out: min={min(lv):.2f} max={max(lv):.2f} median={sorted(lv)[len(lv)//2]:.2f}")

    print(f"\n[3/3] Sample first record:")
    if measurements:
        import json
        print(json.dumps(measurements[0], ensure_ascii=False, indent=2))

    print(f"\n[done] 耗時 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(_dry_run())
    print("WicSewer Collector module. Use --dry-run to test fetch+parse without DB write.")
    sys.exit(0)
