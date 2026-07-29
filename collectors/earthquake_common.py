"""地震系列 collector 的共用小工具（非 collector，不進 registry）

放這裡的東西必須是「多支地震 collector 都要用」的：
  - 震度階文字 → 數值（CWA 2020 新制，5/6 級分弱強）
  - CWA JSON 的單元素塌成 dict 的防呆（XML→JSON 轉出來的檔常見）
  - 事件自然鍵組法（小區域地震 EarthquakeNo 一律 115000 不唯一，必須複合）
"""

from __future__ import annotations

from typing import Any, Optional

# CWA 震度階（2020 新制）→ 數值。5/6 級分弱強，用 .0/.5 表達順序。
# 舊制的 '5級'/'6級' 一併吃下，避免歷史報告解析成 None。
INTENSITY_SCALE = {
    '0級': 0.0,
    '1級': 1.0,
    '2級': 2.0,
    '3級': 3.0,
    '4級': 4.0,
    '5級': 5.0,   # 舊制
    '5弱': 5.0,
    '5強': 5.5,
    '6級': 6.0,   # 舊制
    '6弱': 6.0,
    '6強': 6.5,
    '7級': 7.0,
}


def intensity_to_value(text: Optional[str]) -> Optional[float]:
    """'5弱' → 5.0；未知字串回 None（不猜）"""
    if not text:
        return None
    return INTENSITY_SCALE.get(text.strip())


def as_list(obj: Any) -> list:
    """CWA 的 XML→JSON 檔在只有一筆時會把 list 塌成 dict，統一補回 list。"""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    return [obj]


def safe_float(val: Any) -> Optional[float]:
    try:
        if val is None or val == '':
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def safe_int(val: Any) -> Optional[int]:
    try:
        if val is None or val == '':
            return None
        return int(float(val))
    except (TypeError, ValueError):
        return None


def compact_time(origin_time: str) -> str:
    """'2026-07-25T02:04:53+08:00' → '20260725020453'（只留數字，取前 14 碼）"""
    digits = ''.join(ch for ch in (origin_time or '') if ch.isdigit())
    return digits[:14]


def make_event_id(earthquake_no: Any, origin_time: str, source_type: str) -> str:
    """事件自然鍵。

    顯著有感地震（E-A0015-001）EarthquakeNo 逐次遞增（115050/115051/…）→ 沿用
    純編號，維持與 2026-07 前既有 live.earthquake_events 資料相容。

    小區域有感地震（E-A0016-001）EarthquakeNo 一律 115000（不編號），全部會被
    壓成同一筆 → 改用 {EarthquakeNo}-{YYYYMMDDHHMMSS} 複合鍵。
    """
    no = str(earthquake_no) if earthquake_no not in (None, '') else 'NA'
    if source_type == 'significant' and no != 'NA':
        return no
    return f"{no}-{compact_time(origin_time)}"
