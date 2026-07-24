"""
TWSE 加權指數即時 ticker 收集器

資料來源：證交所 MIS（mis.twse.com.tw）即時報價端點
  端點：GET /stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw|tse_IX0001.tw&json=1&delay=0
  Header：Referer: https://mis.twse.com.tw/stock/index.jsp（必帶）

  盤中 09:00-13:30 每 5 秒更新一筆（userDelay=5000）；
  盤後 / 週末 / 國定假日回傳上一交易日收盤值（rtcode 仍 0000，靠 d 欄判斷 stale）。

寫入：
  - live.market_index_tick    （append-only，UNIQUE(index_code, observed_at) DO NOTHING）
  - live.market_index_current （UPSERT by index_code）
"""

from __future__ import annotations

from datetime import datetime, time
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

TWSE_MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
INDEX_CHANNELS = "tse_t00.tw|tse_IX0001.tw"


def _num(v) -> Optional[float]:
    try:
        if v is None or v == "" or v == "-":
            return None
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    try:
        if v is None or v == "" or v == "-":
            return None
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def _parse_observed_at(d: str | None, t: str | None) -> Optional[datetime]:
    """組合 d='20260612' + t='13:33:00' → datetime in TAIPEI_TZ"""
    if not d or not t:
        return None
    try:
        return datetime.strptime(f"{d} {t}", "%Y%m%d %H:%M:%S").replace(tzinfo=TAIPEI_TZ)
    except ValueError:
        return None


def _is_market_open(observed_at: datetime | None) -> bool:
    if observed_at is None:
        return False
    # 週一到週五 09:00-13:30 視為盤中
    if observed_at.weekday() >= 5:
        return False
    return time(9, 0) <= observed_at.time() <= time(13, 30)


class TwseMarketIndexCollector(BaseCollector):
    """TWSE 加權指數即時 ticker（5 秒 polling，盤中才有 fresh data）"""

    name = "twse_market_index"
    interval_minutes = config.TWSE_MARKET_INDEX_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (twse-market-index)",
            "Accept": "application/json",
            "Referer": "https://mis.twse.com.tw/stock/index.jsp",
        })

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        params = {"ex_ch": INDEX_CHANNELS, "json": "1", "delay": "0"}
        resp = self._session.get(TWSE_MIS_URL, params=params,
                                 timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()

        rows = []
        for entry in payload.get("msgArray", []) or []:
            code = (entry.get("@") or entry.get("ch") or "").strip()
            if not code:
                continue
            observed_at = _parse_observed_at(entry.get("d"), entry.get("t"))
            row = {
                "index_code":      code,
                "index_name":      entry.get("n"),
                "current_value":   _num(entry.get("z")),
                "prev_close":      _num(entry.get("y")),
                "open_value":      _num(entry.get("o")),
                "high_value":      _num(entry.get("h")),
                "low_value":       _num(entry.get("l")),
                "volume_lots":     _int(entry.get("r")),
                "value_thousands": _int(entry.get("m")),
                "is_market_open":  _is_market_open(observed_at),
                "observed_at":     observed_at,
                "collected_at":    now,
            }
            # 缺 current_value 或 observed_at 的不寫
            if row["current_value"] is None or row["observed_at"] is None:
                continue
            rows.append(row)

        return {
            "data":         rows,
            "index_count":  len(rows),
            "collected_at": now.isoformat(),
        }
