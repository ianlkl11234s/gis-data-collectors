"""
臺北市抽水站運轉狀態即時收集器

資料來源：臺北市政府水情整合 OpenAPI（heopublic）
  端點：GET https://heopublic.gov.taipei/taipei-heo-api/openapi/pumb/latest
  認證：無
  頻率：10 分鐘（實測每 5 分鐘上游就更新一次）

覆蓋：臺北市 97 站抽水站
即時 feed 已含 lat/lon、警戒線 max_allowable_water_level，故 stations 表完整。

寫入：
  - public.taipei_pumb_stations  （站點 + 警戒線，upsert）
  - realtime.taipei_pumb_status  （時序狀態，ON CONFLICT DO NOTHING）

衍生 RPC：public.get_taipei_pumb_latest()
  → 含 risk_ratio = inner_value / max_allowable，>0.8 即將淹水
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

ENDPOINT = "https://heopublic.gov.taipei/taipei-heo-api/openapi/pumb/latest"


def _flt(v) -> Optional[float]:
    if v is None or v == "" or v == "-":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_obs_time(s: Optional[str]) -> Optional[datetime]:
    """Pumb obs_time = 'YYYY-MM-DD HH:MM:SS' → tz-aware datetime"""
    if not s:
        return None
    try:
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=TAIPEI_TZ)
    except ValueError:
        return None


class WicPumbCollector(BaseCollector):
    """臺北市抽水站運轉狀態收集器"""

    name = "wic_pumb"
    interval_minutes = getattr(config, "WIC_PUMB_INTERVAL", 10)
    COLLECT_TIMEOUT: int = 30

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (wic_pumb)",
            "Accept": "application/json",
        })

    def _fetch(self) -> list[dict]:
        resp = self._session.get(ENDPOINT, timeout=20, verify=False)
        resp.raise_for_status()
        j = resp.json()
        return j if isinstance(j, list) else []

    def _normalize_station(self, r: dict) -> Optional[dict]:
        sid = r.get("stn_id")
        if not sid:
            return None
        return {
            "stn_id":   sid,
            "stn_name": r.get("stn_name") or "",
            "lat":      _flt(r.get("lat")),
            "lng":      _flt(r.get("lon")),
            "pumb_num": r.get("pumb_num"),
            "door_num": r.get("door_num"),
            "max_allowable_water_level": _flt(r.get("max_allowable_water_level")),
        }

    def _normalize_status(self, r: dict, collected_at: datetime) -> Optional[dict]:
        sid = r.get("stn_id")
        ts = _parse_obs_time(r.get("obs_time"))
        if not sid or not ts:
            return None
        return {
            "stn_id":       sid,
            "observed_at":  ts.isoformat(),
            "inner_value":  _flt(r.get("inner_value")),
            "outer_value":  _flt(r.get("outer_value")),
            "pumb_status":  r.get("pumb_status"),
            "door_status":  r.get("door_status"),
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
                self.supabase_writer._upsert_taipei_pumb_stations(stations)
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
    print("WicPumb Collector — DRY RUN（不寫 DB）")
    print("=" * 60)
    coll = WicPumbCollector()
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
    with_coord = sum(1 for s in stations if s["lat"] and s["lng"])
    print(f"      with lat/lng: {with_coord} / {len(stations)}")

    from collections import Counter
    pcnt = Counter(m["pumb_status"] for m in status_rows if m.get("pumb_status"))
    dcnt = Counter(m["door_status"] for m in status_rows if m.get("door_status"))
    print(f"      pumb_status: {dict(pcnt)}")
    print(f"      door_status: {dict(dcnt)}")

    # 警戒分布
    risky = 0
    for m, s in zip(status_rows, stations):
        if m.get("inner_value") and s.get("max_allowable_water_level"):
            if m["inner_value"] / s["max_allowable_water_level"] > 0.8:
                risky += 1
    print(f"      risk_ratio > 0.8: {risky} 站")

    print(f"\n[3/3] Sample first record:")
    if status_rows:
        import json
        print(json.dumps(status_rows[0], ensure_ascii=False, indent=2))

    print(f"\n[done] 耗時 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(_dry_run())
    print("WicPumb Collector module. Use --dry-run to test fetch+parse without DB write.")
    sys.exit(0)
