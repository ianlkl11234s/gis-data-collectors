"""日本氣象廳 bosai JSON 共用工具（jma_amedas / jma_warnings / jma_quake）。

⚠ bosai JSON 是非公式 API（無官方文件，結構可能不預告變更）；時間一律 JST(+09:00)
  或 UTC(Z)，一律轉成帶時區 ISO 字串交給 timestamptz。
⚠ 授權 PDL1.0：前端顯示需標「気象庁ホームページ」並註明加工。
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

import config

JMA_BOSAI_BASE = "https://www.jma.go.jp/bosai"


class JmaFetchError(RuntimeError):
    pass


def new_jma_session(tag: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; {tag})",
        "Accept": "application/json, text/plain, */*",
    })
    return s


def mount_bounded_retry(session: requests.Session) -> requests.Session:
    """有限次、僅暫時性錯誤（429/5xx/連線逾時）重試，並忽略伺服器指定的 Retry-After
    （避免長睡眠繞過單輪時限）。"""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    adapter = HTTPAdapter(max_retries=Retry(
        total=2, connect=2, read=2, backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET",)),
        respect_retry_after_header=False,
        raise_on_status=False,
    ))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch(session: requests.Session, path: str, *, as_text: bool = False,
          timeout: Optional[int] = None, retries: int = 1) -> Any:
    """GET bosai 路徑（相對 JMA_BOSAI_BASE 或完整 URL）。失敗丟 JmaFetchError。"""
    url = path if path.startswith("http") else f"{JMA_BOSAI_BASE}/{path.lstrip('/')}"
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout or config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text if as_text else resp.json()
        except Exception as e:  # noqa: BLE001 — 統一包成 JmaFetchError
            last_err = e
            if attempt < retries:
                time.sleep(2)
    raise JmaFetchError(f"{url}: {last_err}")


def parse_dt(value: Any) -> Optional[datetime]:
    """'2026-10-09T15:40:00+09:00' / '2026-10-09T06:25:28Z' → aware datetime；無法解析回 None。"""
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None  # 不猜時區
    return dt


def iso(value: Any) -> Optional[str]:
    dt = parse_dt(value)
    return dt.isoformat() if dt else None


def age_minutes(dt: Optional[datetime], now: Optional[datetime] = None) -> Optional[float]:
    if dt is None:
        return None
    now = now or datetime.now(timezone.utc)
    return (now - dt).total_seconds() / 60.0


def to_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f
