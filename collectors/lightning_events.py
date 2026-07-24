"""
台電落雷即時資料收集器

資料來源：台灣電力公司 開放資料平台（service.taipower.com.tw，免金鑰）
  端點：https://service.taipower.com.tw/data/opendata/apply/file/d546005/001.csv
  catalog nid：61139

特性：
  - 1 分鐘 snapshot 模式（每分鐘整檔覆寫，非累積）
  - 單批 ~60 筆，全國覆蓋（含台灣海峽周邊）
  - 雲中閃 ~95% / 雲對地閃 ~5%（物理常識吻合）
  - CSV 無 header，UTF-8 BOM
  - 6 欄：[timestamp, event_id, lon, lat, intensity_ka, strike_type]
    - timestamp 形如 '2026-06-14 23:21:14.00'（本地時間）
    - event_id 整數（疑似 ns 時間戳衍生），作為去重主鍵
    - intensity_ka 可能為 '-'（type=1 多無強度）
    - strike_type 0=雲對地 / 1=雲中

  ⚠ 台電憑證缺 Subject Key Identifier，Python TLS 預設驗證會失敗 → verify=False

寫入（schema=realtime）：
  - live.lightning_events  (event_id UNIQUE, dedup_hash UNIQUE 兩鍵保險)
    ON CONFLICT DO NOTHING
"""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

URL_LIGHTNING = "https://service.taipower.com.tw/data/opendata/apply/file/d546005/001.csv"

# 台電端點憑證缺 Subject Key Identifier，verify=False 並關閉警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _num(v) -> Optional[float]:
    """解析數值：'-' / 空值 → None"""
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _int(v) -> Optional[int]:
    n = _num(v)
    return int(n) if n is not None else None


def _parse_strike_time(s: str | None) -> Optional[datetime]:
    """解析落雷時間 '2026-06-14 23:21:14.00' → tz-aware datetime（台北時區）"""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


def _make_dedup_hash(strike_time_iso: str, lon: float, lat: float, strike_type: Optional[int]) -> str:
    """保險去重 hash：md5(strike_time||lon||lat||strike_type)
    上游 event_id 若重置或衝突，dedup_hash 可兜底擋下塌列。"""
    raw = f"{strike_time_iso}|{lon:.5f}|{lat:.5f}|{strike_type if strike_type is not None else ''}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


class LightningEventsCollector(BaseCollector):
    """台電落雷即時事件收集器（5 分鐘 cron，snapshot 去重累積）"""

    name = "lightning_events"
    interval_minutes = config.LIGHTNING_EVENTS_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; lightning-events)",
        })
        self._session.verify = False  # 台電憑證缺 SKI

    def _fetch_csv(self) -> str:
        resp = self._session.get(URL_LIGHTNING, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        # CSV 開頭 UTF-8 BOM → 用 utf-8-sig decode 自動去除
        return resp.content.decode("utf-8-sig")

    def _parse_rows(self, text: str, collected_at: datetime) -> list[dict]:
        """解析 CSV → 列出去重後的事件 dict 清單。
        CSV 無 header，6 欄固定順序：
          col0: timestamp '2026-06-14 23:21:14.00'
          col1: event_id   整數
          col2: lon
          col3: lat
          col4: intensity_ka  可能 '-'
          col5: strike_type   0/1
        """
        reader = csv.reader(io.StringIO(text))
        collected_iso = collected_at.isoformat()
        rows: list[dict] = []
        seen_event_ids: set[int] = set()

        for row in reader:
            if not row or len(row) < 6:
                continue
            # 同檔內去重保險：上游偶見同 event_id 重複
            strike_time = _parse_strike_time(row[0])
            event_id = _int(row[1])
            lon = _num(row[2])
            lat = _num(row[3])
            intensity = _num(row[4])
            stype = _int(row[5])

            if event_id is None or strike_time is None or lon is None or lat is None:
                continue
            if event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)

            strike_iso = strike_time.isoformat()
            rows.append({
                "event_id":     event_id,
                "strike_time":  strike_iso,
                "lon":          lon,
                "lat":          lat,
                "intensity_ka": intensity,
                "strike_type":  stype,
                "dedup_hash":   _make_dedup_hash(strike_iso, lon, lat, stype),
                # geom 由 writer 端用 ST_SetSRID(ST_MakePoint(lon,lat),4326) 組
                "observed_at":  strike_iso,  # = strike_time，作為時序欄位
                "collected_at": collected_iso,
            })
        return rows

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        text = self._fetch_csv()
        events = self._parse_rows(text, now)

        return {
            "data":         events,
            "event_count":  len(events),
            "collected_at": now.isoformat(),
        }


if __name__ == "__main__":
    # 離線試跑：python3 -m collectors.lightning_events
    c = LightningEventsCollector.__new__(LightningEventsCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (lightning-events-test)"})
    c._session.verify = False
    out = c.collect()
    print(f"events: {out['event_count']}")
    if out["data"]:
        print("sample:", out["data"][0])
