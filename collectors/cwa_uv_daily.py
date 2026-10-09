"""
CWA 每日紫外線指數最大值收集器

資料來源：中央氣象署開放資料（需 CWA_API_KEY）
  - O-A0005-001（datagov 9039）：前一日各站紫外線指數最大值，約 30 站
  - O-A0003-001：署屬有人站觀測（只取 GeoInfo 補座標/站名/縣市）

特性：
  - 上游只給一個 Date（2026-10-02 拉到 2026-10-01）→ 每日 2 次 cron（720 分）即可
  - UVIndex = -99 為缺值 → uv_index NULL、uv_raw 保留原值，不可當 0
  - 同日重抓 UPSERT（站號＋日期）；重抓不是失敗，抓到 0 站才是

寫入：live.cwa_uv_daily UNIQUE(station_id, obs_date) UPSERT
"""

from __future__ import annotations

from datetime import datetime, date
from typing import Optional

import requests
from urllib3.util.retry import Retry

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.cwa_marine_observation import _CwaTlsAdapter  # CWA 憑證缺 SKI：關 VERIFY_X509_STRICT（仍驗證鏈）

CWA_BASE = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
UV_ENDPOINT = "O-A0005-001"
STATION_ENDPOINT = "O-A0003-001"


def _num(v) -> Optional[float]:
    s = str(v or "").strip()
    if s in ("", "-", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_station_meta(payload: dict) -> dict[str, dict]:
    meta: dict[str, dict] = {}
    for st in ((payload or {}).get("records") or {}).get("Station", []) or []:
        sid = st.get("StationId")
        if not sid:
            continue
        geo = st.get("GeoInfo") or {}
        lon = lat = None
        for c in geo.get("Coordinates") or []:
            if c.get("CoordinateName") == "WGS84":
                lon, lat = _num(c.get("StationLongitude")), _num(c.get("StationLatitude"))
        meta[sid] = {"station_name": st.get("StationName"), "county": geo.get("CountyName"),
                     "lon": lon, "lat": lat}
    return meta


def parse_uv(payload: dict, meta: dict[str, dict], collected_at: datetime) -> list[dict]:
    we = (((payload or {}).get("records") or {}).get("weatherElement")) or {}
    d_raw = (we.get("Date") or "").strip()
    try:
        obs_date = date.fromisoformat(d_raw[:10])
    except ValueError:
        return []
    observed_at = datetime(obs_date.year, obs_date.month, obs_date.day, tzinfo=TAIPEI_TZ)
    out: list[dict] = []
    seen: set[str] = set()
    for loc in we.get("location") or []:
        sid = (loc.get("StationID") or "").strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        raw = str(loc.get("UVIndex") if loc.get("UVIndex") is not None else "").strip()
        val = _num(raw)
        if val is not None and val < 0:   # -99 等缺值碼
            val = None
        m = meta.get(sid, {})
        out.append({
            "station_id":   sid,
            "station_name": m.get("station_name"),
            "county":       m.get("county"),
            "obs_date":     obs_date.isoformat(),
            "observed_at":  observed_at.isoformat(),
            "uv_index":     val,
            "uv_raw":       raw or None,
            "lon":          m.get("lon"),
            "lat":          m.get("lat"),
            "collected_at": collected_at.isoformat(),
        })
    return out


class CwaUvDailyCollector(BaseCollector):
    """CWA 每日紫外線指數最大值（720 分鐘 cron）"""

    name = "cwa_uv_daily"
    interval_minutes = config.CWA_UV_DAILY_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; cwa-uv)"})
        adapter = _CwaTlsAdapter()
        # 有限次、僅暫時性錯誤重試（429/5xx/連線逾時）；最終錯誤仍走 _get 的 sanitized 處理
        adapter.max_retries = Retry(
            total=2, connect=2, read=2, backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(("GET",)),
            raise_on_status=False,
        )
        self._session.mount("https://", adapter)

    def require_db_write(self) -> bool:
        return True

    def _get(self, endpoint: str) -> dict:
        try:
            resp = self._session.get(f"{CWA_BASE}/{endpoint}",
                                     params={"Authorization": config.CWA_API_KEY, "format": "JSON"},
                                     timeout=config.REQUEST_TIMEOUT * 2)
        except requests.RequestException as e:
            # 不把含 Authorization 的 URL 帶進錯誤訊息
            raise RuntimeError(f"CWA {endpoint} request failed: {type(e).__name__}") from None
        if resp.status_code != 200:
            raise RuntimeError(f"CWA {endpoint} HTTP {resp.status_code}")
        return resp.json()

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        uv_payload = self._get(UV_ENDPOINT)
        meta_error = None
        try:
            meta = parse_station_meta(self._get(STATION_ENDPOINT))
        except Exception as e:   # 座標補不到不擋寫入（lon/lat 留 NULL）
            meta, meta_error = {}, str(e)
        rows = parse_uv(uv_payload, meta, now)
        if not rows:
            raise RuntimeError("CWA UV parsed 0 stations (upstream empty or schema changed)")
        result = {
            "data":           rows,
            "station_count":  len(rows),
            "obs_date":       rows[0]["obs_date"],
            "missing_value_count": sum(1 for r in rows if r["uv_index"] is None),
            "no_coord_count": sum(1 for r in rows if r["lon"] is None),
            "collected_at":   now.isoformat(),
        }
        if meta_error:
            result["station_meta_error"] = meta_error
        return result


if __name__ == "__main__":
    c = CwaUvDailyCollector.__new__(CwaUvDailyCollector)
    c._session = requests.Session()
    c._session.mount("https://", _CwaTlsAdapter())
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
    print("sample:", out["data"][0])
