"""中央氣象署閃電落雷即時觀測 — 落雷第二資料源

端點：https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0039-001（KMZ，每 5 分更新）
寫入：live.lightning_events（`source='cwa'`，與台電同表，migration 338 加的 source 欄位）

## 為什麼要第二源

台電落雷（`lightning_events` collector）自 2026-07-10 起**端點活著但內容永遠是空的**
（只有 BOM + 標題行）。實證：S3 archive 6/15~7/09 在 52KB~53MB 劇烈變化、7/10 起固定
52KB；2026-08-06 連續監看 10 分鐘 24 次全 0 筆，而同時段氣象署偵測到 179 次閃電、
CWA 還發了 9 則雷雨警特報。collector 每分鐘正常跑、端點沒換 —— 是上游停供。

兩源並行後，「一邊有資料一邊沒有」會立刻現形；這次那種單邊靜默斷供拖了 28 天沒人發現。

## 與台電源的差異（不是等價替代）

| | 台電 | 本 collector（CWA） |
|---|---|---|
| 時間精度 | 奈秒 | **分鐘** |
| 座標精度 | 小數 5 位 | 小數 2~3 位（約 100m） |
| 電流強度 kA | 有 | **無** → `intensity_ka=None` |
| event_id | 上游提供 | **無** → 本檔以 hash 合成 |
| 供應方式 | 1 分鐘 snapshot 整檔覆寫 | **滾動 1 小時視窗**，每 5 分更新 |

滾動視窗是本源的優勢：cron 慢個幾分鐘不會漏，台電那種只要錯過一分鐘就永久遺失。
代價是同一筆會被重複抓到約 12 次，靠 `(source, dedup_hash)` UNIQUE 擋掉。

⚠️ 分鐘級時間 + 3 位座標讓 dedup 的解析度比台電粗，同分鐘同座標同類型會塌成一筆。
   2026-08-06 的 179 筆樣本實測**零碰撞**，但雷雨極密集時仍可能少算。

⚠️ 端點會回 **302 重導**。requests 預設跟隨，curl 手測必須加 `-L`，否則拿到 0 bytes
   會誤判成「沒資料」。
"""
from __future__ import annotations

import io
import re
import zipfile
from datetime import datetime, timezone
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.lightning_events import _make_dedup_hash

URL_CWA_LIGHTNING = (
    "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0039-001"
    "?Authorization={key}&format=KMZ"
)

# KML Placemark：種類在 description、時間取 <TimeStamp><when>（帶 Z，比 description
# 的台北時間少一層時區推斷）、座標取 <Point><coordinates>
_RE_PLACEMARK = re.compile(r"<Placemark>(.*?)</Placemark>", re.S)
_RE_KIND = re.compile(r"閃電種類:\s*(\S+)")
# ⚠ 上游收尾標籤實際長這樣：`</when >`（多一個空格）—— 寫死 `</when>` 會靜默抓不到
_RE_WHEN = re.compile(r"<when>\s*([\dT:\-]+)Z\s*</when\s*>")
_RE_COORD = re.compile(r"<coordinates>\s*([-\d.]+),([-\d.]+)")

# 對齊台電語意：0=雲對地 / 1=雲中
_KIND_TO_TYPE = {"對地": 0, "雲間": 1}

SOURCE = "cwa"


def _synth_event_id(dedup_hash: str) -> int:
    """CWA 不給 event_id，但表欄位是 NOT NULL 且前端型別為 number（非 nullable）。

    取 dedup_hash 前 15 hex（< 2^60，bigint 安全）當合成 id —— 同一筆閃電永遠得到
    同一個值，重複抓到時 ON CONFLICT 擋得住。與台電的上游整數 id 不會撞，
    且 UNIQUE 已改成 (source, event_id)。
    """
    return int(dedup_hash[:15], 16)


def _parse_kmz(blob: bytes) -> list[dict]:
    """KMZ → 逐筆閃電 dict（不含 source / collected_at，由呼叫端補）。"""
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        names = z.namelist()
        if not names:
            return []
        kml = z.read(names[0]).decode("utf-8", "ignore")

    out: list[dict] = []
    for pm in _RE_PLACEMARK.findall(kml):
        m_when = _RE_WHEN.search(pm)
        m_coord = _RE_COORD.search(pm)
        if not m_when or not m_coord:
            continue
        m_kind = _RE_KIND.search(pm)
        kind = m_kind.group(1) if m_kind else ""
        stype: Optional[int] = _KIND_TO_TYPE.get(kind)

        try:
            # '2026-08-06T09:05' + Z → UTC；上游只到分鐘
            strike_time = datetime.strptime(m_when.group(1), "%Y-%m-%dT%H:%M").replace(
                tzinfo=timezone.utc)
        except ValueError:
            continue
        lon = float(m_coord.group(1))
        lat = float(m_coord.group(2))

        strike_iso = strike_time.isoformat()
        dedup = _make_dedup_hash(strike_iso, lon, lat, stype)
        out.append({
            "event_id":     _synth_event_id(dedup),
            "strike_time":  strike_iso,
            "lon":          lon,
            "lat":          lat,
            "intensity_ka": None,      # CWA 不提供
            "strike_type":  stype,
            "dedup_hash":   dedup,
            "source":       SOURCE,
            "observed_at":  strike_iso,
        })
    return out


class LightningCwaCollector(BaseCollector):
    """氣象署落雷收集器（滾動 1 小時視窗，靠 dedup 累積）"""

    name = "lightning_cwa"
    interval_minutes = config.LIGHTNING_CWA_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; lightning-cwa)",
        })

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        if not config.CWA_API_KEY:
            raise RuntimeError("CWA_API_KEY 未設定")

        resp = self._session.get(
            URL_CWA_LIGHTNING.format(key=config.CWA_API_KEY),
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        events = _parse_kmz(resp.content)
        collected_iso = now.isoformat()
        for e in events:
            e["collected_at"] = collected_iso

        # 同批去重（滾動視窗理論上不會有，但上游若重覆列出就會撞 ON CONFLICT）
        seen: set[str] = set()
        deduped = []
        for e in events:
            if e["dedup_hash"] in seen:
                continue
            seen.add(e["dedup_hash"])
            deduped.append(e)

        return {
            "data":         deduped,
            "event_count":  len(deduped),
            "raw_count":    len(events),
            "collected_at": collected_iso,
        }


if __name__ == "__main__":
    # 離線試跑：python3 -m collectors.lightning_cwa
    c = LightningCwaCollector.__new__(LightningCwaCollector)
    c._session = requests.Session()
    out = c.collect()
    print(f"events: {out['event_count']} (raw {out['raw_count']})")
    if out["data"]:
        print("sample:", out["data"][0])
