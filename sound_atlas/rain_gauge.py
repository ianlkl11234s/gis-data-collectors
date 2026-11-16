"""雨量站逐時列（rain-gauge-<D>.json，schema pulse-sound-rain-gauge-v1）。

每日檔組裝搬自 mini-taiwan-pulse `scripts/preprocess/build-sound-grid-sources.py build_rain_gauge`；
原本的輸入是 RPC get_rain_gauge_day 的匯出 CSV，這裡改讀 collector 原始檔（rain_gauge_realtime 本機 JSON／S3 archive），
用同一套規則在 Python 重現 RPC：
  - live.rain_gauge_readings 以 (station_id, observed_at) ON CONFLICT DO NOTHING → 同鍵「先到的」為準；
  - get_rain_gauge_day：observed_at 在臺灣日內、lat/lng 非空，DISTINCT ON (站, 整點) 取 observed_at 最晚的一筆。
"""
from __future__ import annotations

import datetime as dt
from typing import Iterable, Optional

from .common import TPE, day_window, expand_bbox, in_bbox, now_tpe_iso, num

SCHEMA = "pulse-sound-rain-gauge-v1"
RAIN_PAD_KM = 6.0


def _tpe_iso(s: str) -> str:
    return dt.datetime.fromisoformat(s).astimezone(TPE).isoformat()


def reading_key(r: dict) -> Optional[tuple[str, int]]:
    if not r.get("station_id") or not r.get("observed_at"):
        return None
    return r["station_id"], int(dt.datetime.fromisoformat(r["observed_at"]).timestamp())


def normalize(r: dict) -> Optional[dict]:
    """原始讀數 → 每日檔的列格式（bucket_at＝整點，臺北時間 ISO）；lat/lng 缺 → None（RPC 同樣排除）"""
    lat, lng = num(r.get("lat")), num(r.get("lng"))
    if lat is None or lng is None:
        return None
    obs = dt.datetime.fromisoformat(r["observed_at"]).astimezone(TPE)
    bucket = obs.replace(minute=0, second=0, microsecond=0)
    return {
        "station_id": r["station_id"],
        "station_name": r.get("station_name") or None,
        "county": r.get("county") or None,
        "town": r.get("town") or None,
        "lat": lat,
        "lng": lng,
        "bucket_at": bucket.isoformat(),
        "observed_at": obs.isoformat(),
        "precipitation_10min": num(r.get("precipitation_10min")),
        "precipitation_1hr": num(r.get("precipitation_1hr")),
        "precipitation_3hr": num(r.get("precipitation_3hr")),
        "precipitation_24hr": num(r.get("precipitation_24hr")),
    }


def first_seen_readings(docs: Iterable[dict], date: str, bbox: list[float], seen: Optional[set] = None) -> list[dict]:
    """依抵達順序掃原始檔，回「第一次看到」的讀數（已正規化、在臺灣日 date 內、在 bbox 內）。

    seen：跨呼叫共用的已見鍵集合（即時段用；每日段每次新建）。DB 的 ON CONFLICT DO NOTHING 是對全台去重，
    所以鍵不限 bbox；bbox 只決定要不要輸出。
    """
    seen = set() if seen is None else seen
    start, end = day_window(date)
    out = []
    for doc in docs:
        for r in doc.get("data", []) or []:
            k = reading_key(r)
            if k is None or k in seen:
                continue
            seen.add(k)
            if not (start <= k[1] < end):
                continue
            row = normalize(r)
            if row is None or not in_bbox(row["lng"], row["lat"], bbox):
                continue
            out.append(row)
    return out


def hourly_latest(rows: Iterable[dict]) -> list[dict]:
    """DISTINCT ON (站, bucket_at) 取 observed_at 最晚（同 RPC）；回傳依 (站, bucket_at) 排序"""
    best: dict[tuple[str, str], dict] = {}
    for r in rows:
        k = (r["station_id"], r["bucket_at"])
        cur = best.get(k)
        if cur is None or dt.datetime.fromisoformat(r["observed_at"]) > dt.datetime.fromisoformat(cur["observed_at"]):
            best[k] = r
    return sorted(best.values(), key=lambda x: (x["station_id"], x["bucket_at"]))


def station_bbox(grid_bbox: list[float]) -> list[float]:
    return expand_bbox(grid_bbox, RAIN_PAD_KM)


def build_daily(date: str, docs: Iterable[dict], grid_bbox: list[float], origin_note: str, generated_at: Optional[str] = None) -> dict:
    sb = station_bbox(grid_bbox)
    rows = hourly_latest(first_seen_readings(docs, date, sb))
    per_station: dict[str, int] = {}
    for r in rows:
        per_station[r["station_id"]] = per_station.get(r["station_id"], 0) + 1
    counts = sorted(per_station.values())
    return {
        "schema": SCHEMA,
        "date_taiwan": date,
        "generated_at": generated_at or now_tpe_iso(),
        "origin": origin_note,
        "health": "ok" if rows else "empty",
        "station_extent": grid_bbox,
        "notes_zh": [
            f"收北北外擴 {RAIN_PAD_KM:g} km 內 {len(per_station)} 站、{len(rows)} 列（每站列數 {counts[0] if counts else 0}–{counts[-1] if counts else 0}）。",
            "bucket_at／observed_at 為臺北時間；precipitation_1hr 是 observed_at 前 60 分的累積。",
            "來源：rain_gauge_realtime 原始檔（同 live.rain_gauge_readings；同鍵先到為準），每站每整點取 observed_at 最晚一筆（同 get_rain_gauge_day）。",
        ],
        "rows": rows,
    }
