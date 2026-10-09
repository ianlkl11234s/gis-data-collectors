"""日本氣象廳 AMeDAS 10 分鐘全國觀測收集器（約 1,286 站）

資料來源：JMA bosai（免認證、非公式 JSON）
  https://www.jma.go.jp/bosai/amedas/data/latest_time.txt        → '2026-10-09T15:40:00+09:00'
  https://www.jma.go.jp/bosai/amedas/data/map/{YYYYMMDDHHmm00}.json → {station_id: {elem: [值, 品質碼]}}
  https://www.jma.go.jp/bosai/amedas/const/amedastable.json       → 站表（lat/lon 為 [度, 分]）

寫入（gis-platform migration 435，is_multi_table）：
  live.jma_amedas_current       PK station_id，每輪全量 upsert（updated_at=now()）
  live.jma_amedas_observations  PK (station_id, observed_at)，只寫整點（分鐘=00），DO NOTHING；保留 90 天

缺值語意（docs/api-platforms/jma/gotchas.md）：
  - 值為 [值, 品質碼]；品質碼 None = 缺值 → NULL（不補 0）
  - 無積雪計的站（站表 elems 第 6 碼 != '1'，約 950 站）也會回 snow1h: [0, None] → snow* 一律 NULL
整點保證：若某輪 latest_time 不是整點（例如排程漂移跳過 :00），會補抓最近整點那一份 map，
  避免 observations 漏小時（同一 process 內每個整點只補一次；DB 端 DO NOTHING 兜底重複）。
新鮮度：latest_time 落後現在 > 40 分 → print stale 警告（result['stale']=True）。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import (
    JmaFetchError, age_minutes, fetch, new_jma_session, parse_dt, to_float,
)

URL_LATEST_TIME = "amedas/data/latest_time.txt"
URL_MAP = "amedas/data/map/{stamp}.json"
URL_TABLE = "amedas/const/amedastable.json"

STALE_MINUTES = 40
TABLE_REFRESH = timedelta(hours=24)

# JSON 鍵 → 表欄位
ELEMENT_FIELDS = {
    "temp": "temp",
    "precipitation10m": "precip10m",
    "precipitation1h": "precip1h",
    "precipitation3h": "precip3h",
    "precipitation24h": "precip24h",
    "wind": "wind",
    "windDirection": "wind_dir",
    "gust": "gust",
    "humidity": "humidity",
    "pressure": "pressure",          # 現地気圧（normalPressure 海面気圧不在契約內）
    "sun1h": "sun1h",
    "snow": "snow",
    "snow1h": "snow1h",
    "snow6h": "snow6h",
    "snow12h": "snow12h",
    "snow24h": "snow24h",
}
SNOW_FIELDS = ("snow", "snow1h", "snow6h", "snow12h", "snow24h")

ROW_FIELDS = (
    "station_id", "station_name", "station_name_en", "lat", "lon", "alt_m",
    "observed_at", *ELEMENT_FIELDS.values(), "has_snow_gauge", "source_time", "collected_at",
)


def _deg_min(value: Any) -> Optional[float]:
    """[度, 分] → 十進位度。"""
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        d, m = to_float(value[0]), to_float(value[1])
        if d is None or m is None:
            return None
        return round(d + m / 60.0, 6)
    return to_float(value)


def build_station_index(table: dict) -> dict[str, dict]:
    """amedastable.json → {station_id: {name, name_en, lat, lon, alt_m, has_snow_gauge}}"""
    out: dict[str, dict] = {}
    for sid, st in (table or {}).items():
        if not isinstance(st, dict):
            continue
        elems = str(st.get("elems") or "")
        out[str(sid)] = {
            "station_name": st.get("kjName"),
            "station_name_en": st.get("enName"),
            "lat": _deg_min(st.get("lat")),
            "lon": _deg_min(st.get("lon")),
            "alt_m": to_float(st.get("alt")),
            "has_snow_gauge": len(elems) > 5 and elems[5] == "1",
        }
    return out


def _value(pair: Any) -> Optional[float]:
    """[值, 品質碼] → 值；品質碼 None 或格式不符 → None。"""
    if not isinstance(pair, (list, tuple)) or len(pair) < 2:
        return None
    if pair[1] is None:
        return None
    return to_float(pair[0])


def parse_amedas_map(data: dict, stations: dict[str, dict], *, observed_at: str,
                     source_time: str, collected_at: str) -> list[dict]:
    """map/{stamp}.json → 每站一列（欄位同 ROW_FIELDS）。不在站表的站仍保留（座標 NULL）。"""
    rows: list[dict] = []
    for sid, obs in (data or {}).items():
        if not isinstance(obs, dict):
            continue
        st = stations.get(str(sid)) or {}
        has_snow = bool(st.get("has_snow_gauge"))
        row: dict[str, Any] = {
            "station_id": str(sid),
            "station_name": st.get("station_name"),
            "station_name_en": st.get("station_name_en"),
            "lat": st.get("lat"),
            "lon": st.get("lon"),
            "alt_m": st.get("alt_m"),
            "observed_at": observed_at,
            "has_snow_gauge": has_snow,
            "source_time": source_time,
            "collected_at": collected_at,
        }
        for key, field in ELEMENT_FIELDS.items():
            row[field] = _value(obs.get(key))
        if row["wind_dir"] is not None:
            row["wind_dir"] = int(row["wind_dir"])
        if not has_snow:
            for f in SNOW_FIELDS:
                row[f] = None
        rows.append(row)
    return rows


def _stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M00")


class JmaAmedasCollector(BaseCollector):
    """AMeDAS 10 分鐘全量 → current upsert + 整點 observations。"""

    name = "jma_amedas"
    interval_minutes = config.JAPAN_JMA_AMEDAS_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-amedas")
        self._stations: dict[str, dict] = {}
        self._stations_loaded_at: Optional[datetime] = None
        self._hours_done: set[str] = set()

    def _station_index(self) -> dict[str, dict]:
        now = datetime.now(timezone.utc)
        if (not self._stations or self._stations_loaded_at is None
                or now - self._stations_loaded_at > TABLE_REFRESH):
            try:
                self._stations = build_station_index(fetch(self._session, URL_TABLE))
                self._stations_loaded_at = now
            except JmaFetchError as e:
                if not self._stations:
                    raise
                print(f"[{self.name}] ⚠ 站表更新失敗，沿用舊表: {e}")
        return self._stations

    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        latest_raw = str(fetch(self._session, URL_LATEST_TIME, as_text=True)).strip()
        latest = parse_dt(latest_raw)
        if latest is None:
            raise RuntimeError(f"jma_amedas: latest_time.txt 無法解析: {latest_raw[:60]!r}")
        source_time = latest.isoformat()

        stations = self._station_index()
        data = fetch(self._session, URL_MAP.format(stamp=_stamp(latest)))
        rows = parse_amedas_map(data, stations, observed_at=source_time,
                                source_time=source_time, collected_at=collected_at)
        if not rows:
            raise RuntimeError("jma_amedas: map JSON 0 站")

        records = [{**r, "_type": "current"} for r in rows]

        # 整點 observations：latest 本身是整點就直接用；否則補抓最近整點那份
        hour = latest.replace(minute=0, second=0, microsecond=0)
        hour_key = hour.isoformat()
        hourly_count = 0
        if latest.minute == 0:
            records.extend({**r, "_type": "observation"} for r in rows)
            hourly_count = len(rows)
            self._hours_done.add(hour_key)
        elif hour_key not in self._hours_done:
            try:
                hdata = fetch(self._session, URL_MAP.format(stamp=_stamp(hour)))
                hrows = parse_amedas_map(hdata, stations, observed_at=hour_key,
                                         source_time=source_time, collected_at=collected_at)
                records.extend({**r, "_type": "observation"} for r in hrows)
                hourly_count = len(hrows)
                self._hours_done.add(hour_key)
            except JmaFetchError as e:
                print(f"[{self.name}] ⚠ 補抓整點 {hour_key} 失敗（下輪再試）: {e}")
        # 只留最近 48 個整點，避免 set 無限長
        if len(self._hours_done) > 48:
            self._hours_done = set(sorted(self._hours_done)[-48:])

        lag = age_minutes(latest)
        stale = lag is not None and lag > STALE_MINUTES
        if stale:
            print(f"[{self.name}] ⚠ stale: latest_time={source_time} 已落後 {lag:.0f} 分（>{STALE_MINUTES}）")

        return {
            "data": records,
            "station_count": len(rows),
            "hourly_count": hourly_count,
            "latest_time": source_time,
            "lag_minutes": round(lag, 1) if lag is not None else None,
            "stale": stale,
            "collected_at": collected_at,
        }


if __name__ == "__main__":
    c = JmaAmedasCollector.__new__(JmaAmedasCollector)
    c.name = "jma_amedas"
    c._session = new_jma_session("jma-amedas-test")
    c._stations, c._stations_loaded_at, c._hours_done = {}, None, set()
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
    print(out["data"][0])
