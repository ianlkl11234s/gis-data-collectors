"""
核安會 全國環境輻射即時監測收集器

資料來源：核能安全委員會 輻射偵測中心（免金鑰）
  端點：https://www.nusc.gov.tw/open/gammamonitor_u.csv
  datagov：119233（即時值）；站位 136065

特性：
  - 63 站全國一般環境站（含離島），上游 5 分更新；本 collector 15 分 cron
  - CSV UTF-8 BOM，header：監測站, 監測站(英文), 監測值(微西弗/時), 時間, GPS經度, GPS緯度
  - 時間 'YYYY-MM-DD HH:MM'（台北時區）；座標 WGS84
  - 上游無站碼欄 → station_id 用英文站名（2026-10-02 實測 63 站唯一）

⚠ 與 nuclear_radiation（台電 42326，核設施周界 51 站）是不同網路，不可距離合併。

寫入：
  - live.nusc_gamma_measurements  UNIQUE(station_id, observed_at) DO NOTHING
  - live.nusc_gamma_stations      PK=station_id UPSERT（updated_at touch＝心跳）
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from collectors.base import BaseCollector, TAIPEI_TZ

URL_GAMMA = "https://www.nusc.gov.tw/open/gammamonitor_u.csv"
STALE_THRESHOLD = timedelta(minutes=30)
MIN_EXPECTED_STATIONS = 30  # 上游 63 站；低於此數視為上游異常（仍寫入，但回報錯誤）


def _num(v) -> Optional[float]:
    s = str(v or "").strip()
    if s in ("", "-", "--", "N/A", "NA"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_time(s: str | None) -> Optional[datetime]:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


def parse_gamma_csv(text: str, collected_at: datetime) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    out: list[dict] = []
    seen: set[tuple] = set()
    for row in reader:
        clean = {(k or "").strip().lstrip("﻿"): (v or "").strip() for k, v in row.items()}
        name_en = clean.get("監測站(英文)") or None
        name_zh = clean.get("監測站") or None
        obs = _parse_time(clean.get("時間"))
        if not name_en or obs is None:
            continue
        key = (name_en, obs)
        if key in seen:
            continue
        seen.add(key)
        lon = _num(clean.get("GPS經度"))
        lat = _num(clean.get("GPS緯度"))
        # 座標合理範圍（台澎金馬＋東沙/南沙不在此 feed）；超出視為無座標，不合成
        if lon is None or lat is None or not (116 <= lon <= 123.5 and 20 <= lat <= 27):
            lon = lat = None
        out.append({
            "station_id":   name_en,
            "station_name": name_zh,
            "dose_usvh":    _num(clean.get("監測值(微西弗/時)")),
            "observed_at":  obs.isoformat(),
            "lon":          lon,
            "lat":          lat,
            "is_stale":     (collected_at - obs) > STALE_THRESHOLD,
            "collected_at": collected_at.isoformat(),
        })
    return out


class NuscGammaRadiationCollector(BaseCollector):
    """核安會全國環境輻射即時監測（15 分鐘 cron）"""

    name = "nusc_gamma_radiation"
    interval_minutes = config.NUSC_GAMMA_RADIATION_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; nusc-gamma)",
        })
        # 有限次、僅暫時性錯誤重試（429/5xx/連線逾時）
        self._session.mount("https://", HTTPAdapter(max_retries=Retry(
            total=2, connect=2, read=2, backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(("GET",)),
            raise_on_status=False,
        )))

    def require_db_write(self) -> bool:
        return True

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        resp = self._session.get(URL_GAMMA, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        rows = parse_gamma_csv(resp.content.decode("utf-8-sig"), now)
        if not rows:
            # 0 筆不是成功：讓 BaseCollector 走錯誤告警
            raise RuntimeError("nusc gamma CSV parsed 0 rows (upstream empty or schema changed)")
        result = {
            "data":          rows,
            "station_count": len(rows),
            "stale_count":   sum(1 for r in rows if r["is_stale"]),
            "no_coord_count": sum(1 for r in rows if r["lon"] is None),
            "collected_at":  now.isoformat(),
        }
        if len(rows) < MIN_EXPECTED_STATIONS:
            result["_collector_error"] = f"nusc gamma only {len(rows)} stations (< {MIN_EXPECTED_STATIONS})"
        return result


if __name__ == "__main__":
    c = NuscGammaRadiationCollector.__new__(NuscGammaRadiationCollector)
    c._session = requests.Session()
    out = c.collect()
    print(f"stations: {out['station_count']} stale: {out['stale_count']} no_coord: {out['no_coord_count']}")
    print("sample:", out["data"][0])
