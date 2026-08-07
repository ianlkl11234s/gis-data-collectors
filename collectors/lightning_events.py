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
  - live.lightning_events  (source='taipower'；UNIQUE 是 (source,event_id) / (source,dedup_hash))
    ON CONFLICT DO NOTHING

## ⚠️ 上游自 2026-07-10 起靜默斷供

端點還活著（HTTP 200）但內容永遠只有 BOM + 標題行。實證：S3 archive 6/15~7/09
在 52KB~53MB 劇烈變化、7/10 起固定 52KB；2026-08-06 連續監看 10 分鐘 24 次全 0 筆，
而同時段氣象署偵測到 179 次閃電。替代源見 collectors/lightning_cwa.py。

因此 interval 從 1 分鐘放寬到 30 分鐘（config.py）—— 反正沒東西可漏。

⚠️ **上游恢復時必須立刻把 LIGHTNING_EVENTS_INTERVAL 調回 1**：這個端點是
「1 分鐘整檔覆寫」，每批落雷只在檔案裡存在 60 秒，30 分鐘跑一次會漏掉 29/30。
本 collector 偵測到恢復會發 Telegram 提醒（見 _maybe_notify_recovery）。
"""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import datetime, timedelta
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.db import connect_supabase
from utils.notify import send_telegram

URL_LIGHTNING = "https://service.taipower.com.tw/data/opendata/apply/file/d546005/001.csv"

# 「上游恢復」的判準：DB 上一筆台電落雷距今超過這麼久，又突然有資料。
# ⚠ 不能用「這輪有資料」當判準 —— 沒有雷雨時本來就是 0 筆，那樣每場雷雨都會誤報。
RECOVERY_GAP_DAYS = 3

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
        # 同一進程只提醒一次；寫入後 DB 的 max(strike_time) 也會前進，自然不再觸發
        self._recovery_notified = False

    def _maybe_notify_recovery(self, n_events: int) -> None:
        """上游斷供後首次又有資料 → 發 Telegram 提醒把頻率調回 1 分鐘。

        判準是「DB 上一筆台電落雷距今 > RECOVERY_GAP_DAYS」而不是「這輪有資料」——
        後者會在每一場雷雨都誤報，因為沒雷雨時本來就是 0 筆。
        """
        if self._recovery_notified:
            return
        try:
            conn = connect_supabase(autocommit=True, statement_timeout_ms=30_000)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT max(strike_time) FROM live.lightning_events "
                        "WHERE source = 'taipower'")
                    last = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as e:
            print(f"[{self.name}] ⚠ 恢復偵測查詢失敗（不影響本輪收集）: {e}")
            return

        now = datetime.now(tz=TAIPEI_TZ)
        if last is not None and (now - last) < timedelta(days=RECOVERY_GAP_DAYS):
            return  # 只是一般雷雨，不是斷供後恢復

        when = "DB 無任何台電歷史" if last is None else f"上一筆是 {last.astimezone(TAIPEI_TZ):%Y-%m-%d %H:%M}"
        send_telegram(
            "⚡ *台電落雷資料源恢復供資*\n\n"
            f"本輪抓到 *{n_events}* 筆（{when}）。\n\n"
            "⚠️ 請立刻把 Zeabur 的 `LIGHTNING_EVENTS_INTERVAL` 調回 `1` 並 restart —— "
            "上游是 1 分鐘整檔覆寫，每批落雷只在檔案裡存在 60 秒，"
            "目前的 30 分鐘設定會漏掉 29/30。"
        )
        self._recovery_notified = True
        print(f"[{self.name}] ⚡ 偵測到上游恢復供資（{n_events} 筆），已發 Telegram")

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
                # 雙源標記（migration 338）—— 另一源見 collectors/lightning_cwa.py
                "source":       "taipower",
                # geom 由 writer 端用 ST_SetSRID(ST_MakePoint(lon,lat),4326) 組
                "observed_at":  strike_iso,  # = strike_time，作為時序欄位
                "collected_at": collected_iso,
            })
        return rows

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        text = self._fetch_csv()
        events = self._parse_rows(text, now)

        if events:
            self._maybe_notify_recovery(len(events))

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
