"""
OpenAQ v3 空品測站收集器（菲律賓 PH C098 起，國家清單可擴充）

來源：https://api.openaq.org/v3（header `X-API-Key`，環境變數 OPENAQ_API_KEY）
限流：60 次/分、2,000 次/時（帳號層級）。

每輪呼叫：
  1. 每個國家 1 次 `GET /locations?iso=XX&limit=1000`（測站 metadata、授權、sensors、datetimeLast）
  2. 對「datetimeLast 比上次看到的新、且在 OPENAQ_MAX_AGE_HOURS 內」的測站各 1 次
     `GET /locations/{id}/latest`（回應只有 sensorsId／value／datetime，參數與單位由 1. 的 sensors 對回）
  `/parameters/{id}/latest` 不支援國家篩選（2026-11-20 實測 iso、countries_id 都被忽略、回全球 2.1 萬筆），不用。
  PH 實測：116 站、近 7 天有更新 85 站 → 每小時約 86 次呼叫。呼叫間隔至少 OPENAQ_MIN_INTERVAL_SEC 秒。

寫入：
  - live.openaq_measurements：每 sensor 最新值，自然鍵 (observed_at, sensor_id)，ON CONFLICT DO NOTHING
  - live.openaq_locations：測站 metadata（provider、isMonitor、授權與署名、座標），有變才 UPDATE
  gis-platform migration 561。

資料語意：
  - isMonitor=false ＝ 低價感測器（PH 幾乎全部是 Clarity、AirGradient），前端要標示。
  - 缺值（None、NaN、-999 這類哨兵）一律存 NULL，不當 0。
  - 超過 OPENAQ_MAX_AGE_HOURS 的「最新值」不寫（例：某站 pm10 停在 2025-01），
    避免被保留期刪掉後每輪又寫回。
"""

import math
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

import requests

import config
from collectors.base import BaseCollector

API_BASE = "https://api.openaq.org/v3"
LOCATIONS_PAGE_LIMIT = 1000
LOCATIONS_MAX_PAGES = 10
RATE_LIMIT_MAX_WAIT_SEC = 60
# OpenAQ 低價感測器偶見的哨兵值（缺值），視同 NULL
_SENTINEL_FLOOR = -999.0


def _parse_ts(value) -> Optional[datetime]:
    """OpenAQ 時間（'2026-11-20T05:06:49Z' 或 {'utc': ...}）→ aware UTC datetime。"""
    if isinstance(value, dict):
        value = value.get("utc")
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _clean_value(v) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f) or f <= _SENTINEL_FLOOR:
        return None
    return f


def _flt(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def normalize_location(loc: dict, country: str) -> Optional[dict]:
    """/locations 的一筆 → live.openaq_locations 一列（欄位名與 migration 561 一致）。"""
    loc_id = loc.get("id")
    if loc_id is None:
        return None
    coords = loc.get("coordinates") or {}
    lat, lon = _flt(coords.get("latitude")), _flt(coords.get("longitude"))
    if lat is None or lon is None:
        lat = lon = None
    provider = loc.get("provider") or {}
    owner = loc.get("owner") or {}
    licenses = loc.get("licenses")
    sensors = []
    for s in loc.get("sensors") or []:
        p = s.get("parameter") or {}
        if s.get("id") is None or not p.get("name"):
            continue
        sensors.append({"id": s["id"], "parameter": p["name"], "units": p.get("units")})
    first_attr = ((licenses[0] or {}).get("attribution") or {}) if licenses else {}
    license_names = " / ".join(dict.fromkeys(
        l.get("name") for l in (licenses or []) if l.get("name"))) or None
    country_code = ((loc.get("country") or {}).get("code") or country or "").upper() or None
    return {
        "location_id": int(loc_id),
        "name": loc.get("name"),
        "locality": loc.get("locality"),
        "country_code": country_code,
        "timezone": loc.get("timezone"),
        "provider_id": provider.get("id"),
        "provider_name": provider.get("name"),
        "owner_name": owner.get("name"),
        "is_monitor": loc.get("isMonitor"),
        "is_mobile": loc.get("isMobile"),
        "instruments": [i.get("name") for i in (loc.get("instruments") or []) if i.get("name")] or None,
        "parameters": sorted({s["parameter"] for s in sensors}) or None,
        "sensors": sensors or None,
        "license_names": license_names,
        "attribution_name": first_attr.get("name"),
        "attribution_url": first_attr.get("url"),
        "licenses": licenses if licenses else None,
        "lon": lon,
        "lat": lat,
        "datetime_first": _iso(_parse_ts(loc.get("datetimeFirst"))),
        "datetime_last": _iso(_parse_ts(loc.get("datetimeLast"))),
    }


def normalize_latest(results: list[dict], station: dict, cutoff: datetime,
                     collected_at: str) -> tuple[list[dict], int, int]:
    """/locations/{id}/latest 的 results → 量測列。

    Returns: (rows, 過舊丟棄數, 對不回 sensor 的筆數)
    """
    sensor_map = {s["id"]: s for s in station.get("sensors") or []}
    rows, stale, unknown = [], 0, 0
    for r in results or []:
        sid = r.get("sensorsId")
        observed = _parse_ts(r.get("datetime"))
        sensor = sensor_map.get(sid)
        if sensor is None:
            unknown += 1
            continue
        if observed is None or observed < cutoff:
            stale += 1
            continue
        rows.append({
            "observed_at": observed.isoformat(),
            "sensor_id": int(sid),
            "location_id": station["location_id"],
            "parameter": sensor["parameter"],
            "units": sensor.get("units"),
            "value": _clean_value(r.get("value")),
            "collected_at": collected_at,
        })
    return rows, stale, unknown


class OpenAQCollector(BaseCollector):
    """OpenAQ v3 多國空品測站（預設 PH）"""

    name = "openaq"
    interval_minutes = config.OPENAQ_INTERVAL
    # PH 約 86 次呼叫 × ≥1.1 秒間隔 ≈ 1.6 分鐘；國家變多時會變長
    COLLECT_TIMEOUT = 900

    def __init__(self, sleep: Callable[[float], None] = time.sleep,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        super().__init__()
        self.countries = [c.strip().upper() for c in (config.OPENAQ_COUNTRIES or "").split(",") if c.strip()]
        self.max_age = timedelta(hours=config.OPENAQ_MAX_AGE_HOURS)
        self.min_interval = config.OPENAQ_MIN_INTERVAL_SEC
        self._sleep = sleep
        self._now = now
        self._last_call = 0.0
        self.calls = 0
        # location_id → 上次成功抓 /latest 時看到的 datetimeLast（沒前進就不打）
        self._polled_last: dict[int, str] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (openaq)",
            "Accept": "application/json",
        })
        if config.OPENAQ_API_KEY:
            self._session.headers["X-API-Key"] = config.OPENAQ_API_KEY

    # ── HTTP ────────────────────────────────────────────
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """節流＋429 重試一次。錯誤訊息不帶 header（key 在 header，不在 URL）。"""
        for attempt in (1, 2):
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                self._sleep(wait)
            self._last_call = time.monotonic()
            self.calls += 1
            resp = self._session.get(f"{API_BASE}{path}", params=params, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code == 429 and attempt == 1:
                reset = resp.headers.get("x-ratelimit-reset") or resp.headers.get("Retry-After") or "60"
                try:
                    delay = float(reset)
                except ValueError:
                    delay = RATE_LIMIT_MAX_WAIT_SEC
                self._sleep(min(max(delay, 1.0), RATE_LIMIT_MAX_WAIT_SEC))
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError("unreachable")

    def _fetch_locations(self, country: str) -> list[dict]:
        out: list[dict] = []
        for page in range(1, LOCATIONS_MAX_PAGES + 1):
            body = self._get("/locations", {"iso": country, "limit": LOCATIONS_PAGE_LIMIT, "page": page})
            results = body.get("results")
            if not isinstance(results, list):
                raise RuntimeError(f"OpenAQ /locations?iso={country} 回傳格式異常")
            out.extend(results)
            if len(results) < LOCATIONS_PAGE_LIMIT:
                break
        return out

    # ── 主流程 ──────────────────────────────────────────
    def collect(self) -> dict:
        if not config.OPENAQ_API_KEY:
            raise RuntimeError("OPENAQ_API_KEY 未設定")
        if not self.countries:
            raise RuntimeError("OPENAQ_COUNTRIES 為空")

        self.calls = 0
        now = self._now()
        cutoff = now - self.max_age
        collected_at = now.isoformat()

        stations: list[dict] = []
        for country in self.countries:
            for loc in self._fetch_locations(country):
                st = normalize_location(loc, country)
                if st:
                    stations.append(st)

        rows: list[dict] = []
        polled = skipped_unchanged = skipped_inactive = failed = 0
        stale_dropped = unknown_sensor = 0
        for st in stations:
            last = _parse_ts(st["datetime_last"])
            if last is None or last < cutoff:
                skipped_inactive += 1
                continue
            if self._polled_last.get(st["location_id"]) == st["datetime_last"]:
                skipped_unchanged += 1
                continue
            try:
                body = self._get(f"/locations/{st['location_id']}/latest")
            except Exception as e:  # 單站失敗不讓整輪失敗；全部失敗才算失敗（見下方）
                failed += 1
                print(f"[{self.name}]   ⚠ location {st['location_id']} latest 失敗: {type(e).__name__}")
                continue
            polled += 1
            r, s, u = normalize_latest(body.get("results") or [], st, cutoff, collected_at)
            rows.extend(r)
            stale_dropped += s
            unknown_sensor += u
            self._polled_last[st["location_id"]] = st["datetime_last"]

        if failed and polled == 0:
            raise RuntimeError(f"OpenAQ /latest 全部失敗（{failed} 站）")

        # 測站 metadata：有變才 UPDATE（iot_wra 同模式）；失敗不擋量測寫入
        if stations and self.supabase_writer:
            try:
                self.supabase_writer._upsert_openaq_locations(stations)
            except Exception as e:
                print(f"[{self.name}] ⚠ 測站 metadata 寫入失敗: {type(e).__name__}: {e}")

        low_cost = sum(1 for s in stations if s.get("is_monitor") is False)
        print(f"[{self.name}]   ✓ {len(stations)} 站（低價感測器 {low_cost}），"
              f"打 latest {polled} 站、沒變略過 {skipped_unchanged}、停更 {skipped_inactive}、失敗 {failed}；"
              f"{len(rows)} 筆量測（過舊丟棄 {stale_dropped}），API {self.calls} 次")

        return {
            "fetch_time": collected_at,
            "countries": self.countries,
            "total_locations": len(stations),
            "low_cost_locations": low_cost,
            "polled_locations": polled,
            "skipped_unchanged": skipped_unchanged,
            "skipped_inactive": skipped_inactive,
            "failed_locations": failed,
            "stale_readings_dropped": stale_dropped,
            "unknown_sensor_readings": unknown_sensor,
            "api_calls": self.calls,
            "total_readings": len(rows),
            # S3 歸檔正本也要有測站 metadata 與授權（不只數值）
            "stations": stations,
            "data": rows,
        }

