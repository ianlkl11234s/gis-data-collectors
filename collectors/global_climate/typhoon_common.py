"""颱風共用工具：跨來源颱風鍵、風速換算、時空守門。

storm_key 規則與 gis-platform migration 476 的
``public.typhoon_normalize_storm_key(int, text)`` 一致，改一邊要同步改另一邊：
  'YYYY:' + 英文名去掉非英數字後轉大寫（2026:CHAMPI、2026:CHOIWAN）；
  熱帶性低氣壓階段的序數名（ONE…NINETEEN、TWENTY…FIFTY 及拼接，如 TWENTYNINE）、
  INVEST、空名稱 → None（不硬配，交給 DB／前端的 500 km 空間規則）。
年份取各家 storm_id 或 CWA Year 欄，不用 valid_at（跨 1/1 的颱風才不會斷成兩顆）。
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional

MS_TO_KT = 1.943844

_ORDINAL_UNITS = "ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE"
_RE_ORDINAL = re.compile(
    rf"^(?:(?:{_ORDINAL_UNITS}|TEN|ELEVEN|TWELVE|THIRTEEN|FOURTEEN|FIFTEEN|SIXTEEN|SEVENTEEN|EIGHTEEN|NINETEEN)"
    rf"|(?:TWENTY|THIRTY|FORTY|FIFTY)(?:{_ORDINAL_UNITS})?)$"
)
_RE_PLACEHOLDER = re.compile(r"^(?:INVEST|TD|TS|TY|STY|NONAME|UNNAMED|[0-9]+)$")
_RE_NON_ALNUM = re.compile(r"[^A-Za-z0-9]")


def storm_key(year: Optional[int], name: Optional[str]) -> Optional[str]:
    if year is None or not (1950 <= int(year) <= 2100):
        return None
    n = _RE_NON_ALNUM.sub("", name or "").upper()
    if not n or _RE_ORDINAL.match(n) or _RE_PLACEHOLDER.match(n):
        return None
    return f"{int(year)}:{n}"


def to_float(v) -> Optional[float]:
    """上游數值多為字串；空字串、'-'、None、非數字 → None（缺值不當 0）。"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s in {"-", "--", "NA", "N/A"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ms_to_kt(ms: Optional[float]) -> Optional[float]:
    return round(ms * MS_TO_KT, 1) if ms is not None else None


def valid_position(lat: Optional[float], lon: Optional[float]) -> bool:
    return lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 360


def valid_time(point_type: str, valid_at: datetime, now: datetime,
               issued_at: Optional[datetime] = None) -> bool:
    """擋壞時間：觀測點不可晚於現在 3 小時、不早於 60 天前；預報點不超過發布後 168 小時。
    （JTWC 曾出現 valid_at 落在三週後的壞列，見 W0 查核。）"""
    if point_type == "observed":
        return now - timedelta(days=60) <= valid_at <= now + timedelta(hours=3)
    base = issued_at or now
    return base - timedelta(hours=12) <= valid_at <= base + timedelta(hours=168)
