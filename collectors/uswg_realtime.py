"""
都市淹水感知器（USWG = Urban Storm Water Gauge）即時讀值收集器

資料來源：經濟部水利署 IoT 水資源物聯網
  https://iot.wra.gov.tw/
  Swagger: https://iot.wra.gov.tw/swagger/v1/swagger.json
  端點：GET /uswg/stations  （單端點同時回 station + 即時值）
  認證：OAuth2 client_credentials → Bearer JWT（30 min TTL）

覆蓋：全國約 1,999 站（22 縣市）
  臺中 449 / 嘉義縣 252 / 雲林 198 / 臺南 192 / 桃園 171 / 高雄 132 /
  新北 129 / 彰化 97 / 宜蘭 80 / 新竹市 77 / ...

寫入：
  - public.uswg_stations       （站點 metadata，upsert）
  - realtime.uswg_measurements （時序讀值，ON CONFLICT DO NOTHING）

備註：
  - 憑證缺 SKI → verify=False（同 NHI ER、iot_wra 同站不同分支已 ack）
  - 欄位 typo：官方 schema 寫 "Longtiude" 而非 "Longitude"
  - 與 iot_wra（7 類水利基礎設施）並存：USWG 是縣市政府部署的「都市淹水」獨立站系

Standalone usage（dry-run，不寫 DB）：
  cd data-collectors
  export IOW_CLIENT_ID=...
  export IOW_CLIENT_SECRET=...
  python3 -m collectors.uswg_realtime --dry-run
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from typing import Optional, Iterable

import requests
import urllib3

import config
from collectors.base import BaseCollector

# 政府憑證缺 SKI（同 NHI ER 坑）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TAIPEI_TZ = config.TAIPEI_TZ if hasattr(config, "TAIPEI_TZ") else None
USWG_BASE_URL  = "https://iot.wra.gov.tw"
USWG_TOKEN_URL = f"{USWG_BASE_URL}/Oauth2/token"
USWG_ENDPOINT  = "/uswg/stations"
TOKEN_TTL_SEC  = 25 * 60   # 30min 官方，提前 5min 刷新


def _flt(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:19], fmt)
            except ValueError:
                continue
        return None


class UswgCollector(BaseCollector):
    """都市淹水感知器即時讀值收集器（每 10 分鐘）"""

    name = "uswg"
    interval_minutes = getattr(config, "USWG_INTERVAL", 10)
    COLLECT_TIMEOUT: int = 60

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (uswg)",
            "Accept": "application/json",
        })
        self._token: Optional[str] = None
        self._token_obtained_at: float = 0.0

        self._client_id     = os.environ.get("IOW_CLIENT_ID")
        self._client_secret = os.environ.get("IOW_CLIENT_SECRET")
        if not self._client_id or not self._client_secret:
            print("[uswg] ⚠ 缺 IOW_CLIENT_ID / IOW_CLIENT_SECRET，將無法 fetch")

    # ------------------------------------------------------------
    # OAuth2 token 管理
    # ------------------------------------------------------------
    def _get_token(self) -> str:
        """換 token；若 cache 仍有效（<25min）直接重用"""
        if self._token and (time.time() - self._token_obtained_at) < TOKEN_TTL_SEC:
            return self._token

        resp = requests.post(
            USWG_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=15,
            verify=False,
        )
        resp.raise_for_status()
        tok = resp.json().get("access_token")
        if not tok:
            raise RuntimeError(f"USWG token response missing access_token: {resp.text[:200]}")
        self._token = tok
        self._token_obtained_at = time.time()
        return tok

    def _fetch_stations(self) -> list[dict]:
        token = self._get_token()
        resp = self._session.get(
            f"{USWG_BASE_URL}{USWG_ENDPOINT}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=config.REQUEST_TIMEOUT if hasattr(config, "REQUEST_TIMEOUT") else 30,
            verify=False,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    # ------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------
    def _normalize_station(self, r: dict, collected_at: datetime) -> Optional[dict]:
        iow_id = r.get("IoWStationId")
        if not iow_id:
            return None
        lat = _flt(r.get("Latitude"))
        # ⚠ 官方 typo：Longtiude（多了一個 i）
        lng = _flt(r.get("Longtiude") or r.get("Longitude"))
        return {
            "iow_station_id":     iow_id,
            "station_id":         r.get("StationId") or "",
            "name":               r.get("Name") or "",
            "county_code":        r.get("CountyCode") or "",
            "county_name":        r.get("CountyName") or "",
            "town_code":          r.get("TownCode") or "",
            "town_name":          r.get("TownName") or "",
            "admin_name":         r.get("AdminName") or "",
            "hydro_station_type": r.get("HydroStationType"),
            "lat":                lat,
            "lng":                lng,
            "updated_at":         collected_at.isoformat(),
        }

    def _normalize_measurements(self, r: dict, collected_at: datetime) -> Iterable[dict]:
        iow_id = r.get("IoWStationId")
        if not iow_id:
            return
        for m in r.get("Measurements") or []:
            pq_id = m.get("IoWPhysicalQuantityId")
            ts    = _parse_dt(m.get("TimeStamp"))
            value = _flt(m.get("Value"))
            if not pq_id or not ts or value is None:
                continue
            yield {
                "iow_station_id":       iow_id,
                "physical_quantity_id": pq_id,
                "observed_at":          ts.isoformat(),
                "name":                 m.get("Name") or "",
                "si_unit":              m.get("SIUnit") or "",
                "value":                value,
                "collected_at":         collected_at.isoformat(),
            }

    # ------------------------------------------------------------
    # collect() — BaseCollector contract
    # ------------------------------------------------------------
    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ) if TAIPEI_TZ else datetime.now()
        stations: list[dict] = []
        measurements: list[dict] = []

        try:
            raw = self._fetch_stations()
        except Exception as e:
            print(f"[uswg] 擷取失敗：{e}")
            return {
                "data":               [],
                "total_stations":     0,
                "total_measurements": 0,
                "error":              str(e)[:200],
                "collected_at":       now.isoformat(),
            }

        for r in raw:
            s = self._normalize_station(r, now)
            if s:
                stations.append(s)
            for m in self._normalize_measurements(r, now):
                measurements.append(m)

        # 靜態 metadata 走 upsert（不進歷史時序表）
        if stations and self.supabase_writer:
            try:
                self.supabase_writer._upsert_uswg_stations(stations)
            except Exception as e:
                print(f"[uswg] 站點 metadata upsert 失敗：{e}")

        return {
            "data":               measurements,   # → realtime.uswg_measurements
            "total_stations":     len(stations),
            "total_measurements": len(measurements),
            "collected_at":       now.isoformat(),
        }


# ============================================================
# Standalone dry-run mode
# ============================================================
def _dry_run() -> int:
    """獨立執行：fetch → parse → 統計 → 不寫 DB"""
    from collections import Counter

    # 確保 .env 被讀進來（CLI 模式）
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # 重新讀 env（dotenv 載入後）
    os.environ.setdefault("IOW_CLIENT_ID", os.environ.get("IOW_CLIENT_ID", ""))
    os.environ.setdefault("IOW_CLIENT_SECRET", os.environ.get("IOW_CLIENT_SECRET", ""))

    print("=" * 60)
    print("USWG Collector — DRY RUN（不寫 DB）")
    print("=" * 60)

    coll = UswgCollector()
    # 強制 dry-run：不論 supabase_writer 是否存在，都不進 collect() 內的 upsert
    coll.supabase_writer = None

    t0 = time.time()
    print(f"\n[1/4] OAuth2 換 token …")
    try:
        tok = coll._get_token()
        print(f"      ✅ token: {tok[:24]}... ({len(tok)} chars)")
    except Exception as e:
        print(f"      ❌ {e}")
        return 1

    print(f"\n[2/4] Fetch {USWG_BASE_URL}{USWG_ENDPOINT} …")
    try:
        raw = coll._fetch_stations()
        print(f"      ✅ {len(raw)} 站")
    except Exception as e:
        print(f"      ❌ {e}")
        return 1

    print(f"\n[3/4] Parse stations + measurements …")
    now = datetime.now(tz=TAIPEI_TZ) if TAIPEI_TZ else datetime.now()
    stations, measurements = [], []
    station_skipped = 0
    for r in raw:
        s = coll._normalize_station(r, now)
        if s:
            stations.append(s)
        else:
            station_skipped += 1
        for m in coll._normalize_measurements(r, now):
            measurements.append(m)

    print(f"      Stations parsed:     {len(stations)}  (skipped: {station_skipped})")
    print(f"      Measurements parsed: {len(measurements)}")

    # 座標健康度
    with_coord = sum(1 for s in stations if s["lat"] and s["lng"])
    print(f"      With lat/lon:        {with_coord} / {len(stations)}")

    # 縣市分布
    county_cnt = Counter(s["county_name"] or "(no county)" for s in stations)
    print(f"\n      縣市分布 (top 10):")
    for c, n in county_cnt.most_common(10):
        print(f"        {c:24s} {n:>5}")

    # measurements 時間範圍
    if measurements:
        obs = [m["observed_at"] for m in measurements]
        print(f"\n      Observation time range:")
        print(f"        earliest: {min(obs)}")
        print(f"        latest:   {max(obs)}")

    # 淹水深度分布
    values = [m["value"] for m in measurements]
    nonzero = [v for v in values if v > 0]
    print(f"\n      淹水深度分布:")
    print(f"        =0 cm:  {len(values) - len(nonzero)} 筆（無淹水）")
    print(f"        >0 cm:  {len(nonzero)} 筆")
    if nonzero:
        print(f"        max:    {max(nonzero):.1f} cm")
        print(f"        median: {sorted(nonzero)[len(nonzero)//2]:.1f} cm")

    # SI Unit 一致性檢查
    unit_cnt = Counter(m["si_unit"] for m in measurements)
    print(f"\n      SI Unit 分布: {dict(unit_cnt)}")

    # Sample dump
    print(f"\n[4/4] Sample (前 1 筆 station + 1 筆 measurement)")
    if stations:
        import json
        print("  Station:")
        print(json.dumps(stations[0], ensure_ascii=False, indent=4))
    if measurements:
        import json
        print("  Measurement:")
        print(json.dumps(measurements[0], ensure_ascii=False, indent=4))

    print(f"\n[done] 耗時 {time.time() - t0:.1f}s")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(_dry_run())
    print("USWG Collector module. Use --dry-run to test fetch+parse without DB write.")
    sys.exit(0)
