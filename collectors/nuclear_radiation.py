"""
核設施環境輻射劑量即時收集器

資料來源：台灣電力公司 開放資料平台（service.taipower.com.tw，免金鑰）
  端點：https://service.taipower.com.tw/data/opendata/apply/file/d525001/001.csv
  catalog nid：42326

特性：
  - 51 站全國覆蓋（核一/核二/核三週邊 + 蘭嶼貯存場 + 恆春外圍）
  - 5 分鐘上游更新（本 collector 15 分 cron 即可）
  - CSV UTF-8 BOM，6 欄 header：[站名, 站號, 劑量率(微西弗/小時), 日期時間, 經度, 緯度]
  - 日期時間 'YYYYMMDDTHHMMSS'（無 timezone，台北時區）
  - 經緯度 WGS84 十進位（直接可用）
  - 站點離線會保留最後讀值 → staleness：now - observed_at > 30min 標 stale

  ⚠ 台電憑證缺 Subject Key Identifier，verify=False
  ⚠ 正常背景 0.039–0.072 µSv/h，異常 >10× 可做核安告警 layer

寫入（schema=realtime）：
  - live.nuclear_radiation_measurements  (UNIQUE(station_id, observed_at)，DO NOTHING)
  - live.nuclear_radiation_stations      (PK=station_id，UPSERT)
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

URL_NUCLEAR = "https://service.taipower.com.tw/data/opendata/apply/file/d525001/001.csv"

STALE_THRESHOLD = timedelta(minutes=30)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _num(v) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "-", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_obs(s: str | None) -> Optional[datetime]:
    """'YYYYMMDDTHHMMSS' → tz-aware datetime（台北時區）"""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y%m%dT%H%M%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


class NuclearRadiationCollector(BaseCollector):
    """核設施環境輻射劑量收集器（15 分鐘 cron）"""

    name = "nuclear_radiation"
    interval_minutes = config.NUCLEAR_RADIATION_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; nuclear-radiation)",
        })
        self._session.verify = False  # 台電憑證缺 SKI

    def _fetch_csv(self) -> str:
        resp = self._session.get(URL_NUCLEAR, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        # ⚠ UTF-8 BOM → utf-8-sig（非預想的 BIG5）
        return resp.content.decode("utf-8-sig")

    def _parse_rows(self, text: str, collected_at: datetime) -> list[dict]:
        reader = csv.DictReader(io.StringIO(text))
        collected_iso = collected_at.isoformat()
        out: list[dict] = []
        seen_keys: set[tuple] = set()

        for row in reader:
            # 欄名/值可能殘留 BOM → strip
            clean = {(k or "").strip().lstrip("﻿"): (v or "").strip().lstrip("﻿")
                     for k, v in row.items()}

            station_id = clean.get("站號") or None
            station_name = clean.get("站名") or None
            dose = _num(clean.get("劑量率(微西弗/小時)"))
            obs_dt = _parse_obs(clean.get("日期時間"))
            lon = _num(clean.get("經度"))
            lat = _num(clean.get("緯度"))

            if not station_id or obs_dt is None:
                continue

            key = (station_id, obs_dt.isoformat())
            if key in seen_keys:
                continue
            seen_keys.add(key)

            is_stale = (collected_at - obs_dt) > STALE_THRESHOLD

            out.append({
                "station_id":   station_id,
                "station_name": station_name,
                "dose_usvh":    dose,
                "observed_at":  obs_dt.isoformat(),
                "lon":          lon,
                "lat":          lat,
                "is_stale":     is_stale,
                # geom 由 writer 端組 ST_SetSRID(ST_MakePoint(lon,lat),4326)
                "collected_at": collected_iso,
            })
        return out

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        text = self._fetch_csv()
        measurements = self._parse_rows(text, now)

        return {
            "data":              measurements,    # base.py 需 'data' 才寫入 → 同時當 history
            "measurements":      measurements,    # 顯式別名
            "station_count":     len(measurements),
            "stale_count":       sum(1 for r in measurements if r["is_stale"]),
            "collected_at":      now.isoformat(),
        }


if __name__ == "__main__":
    # 離線試跑：python3 -m collectors.nuclear_radiation
    c = NuclearRadiationCollector.__new__(NuclearRadiationCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (nuclear-radiation-test)"})
    c._session.verify = False
    out = c.collect()
    print(f"stations: {out['station_count']}  stale: {out['stale_count']}")
    if out["data"]:
        print("sample:", out["data"][0])
