"""USGS 全球地震即時收集器

資料來源：USGS Earthquake Hazards Program（美國地質調查局，免認證、免 key）
  端點：https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
  特性：
    - GeoJSON FeatureCollection，每個 feature 一個地震事件
    - feed 約 1-5 min 更新（內含過去 1 hr 內所有地震）
    - 跟 CWA 本地地震 API 互補（CWA 對台灣準、USGS 對海域/國外更全）
    - properties.time 是 unix ms 不是秒
    - geometry.coordinates 是 [lon, lat, depth_km] 不是 [lat, lon]

自然鍵：event_id = feature.id（USGS 全球唯一）
保險：dedup_hash = md5(event_id||observed_at)

寫入：realtime.earthquakes_global（migration 261）
  - UNIQUE(event_id) + UNIQUE(dedup_hash) 雙保險
  - ON CONFLICT DO NOTHING
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

URL_USGS_HOUR = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"


def _make_dedup_hash(event_id: str, observed_at_iso: str) -> str:
    raw = f"{event_id}|{observed_at_iso}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


class UsgsEarthquakeCollector(BaseCollector):
    """USGS 全球地震 hourly feed 收集器。"""

    name = "global_climate_usgs_earthquake"
    interval_minutes = config.GLOBAL_CLIMATE_USGS_EARTHQUAKE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; global-climate-usgs-earthquake)",
            "Accept": "application/geo+json",
        })

    def _fetch_geojson(self) -> dict:
        resp = self._session.get(URL_USGS_HOUR, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _parse_features(self, geojson: dict) -> list[dict]:
        rows: list[dict] = []
        for feat in geojson.get("features", []):
            event_id = feat.get("id")
            props = feat.get("properties") or {}
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or []

            if not event_id or len(coords) < 2:
                continue
            try:
                lon = float(coords[0])
                lat = float(coords[1])
                depth_km = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None
            except (TypeError, ValueError):
                continue

            time_ms = props.get("time")
            if time_ms is None:
                continue
            try:
                observed_at = datetime.fromtimestamp(int(time_ms) / 1000, tz=timezone.utc)
            except (TypeError, ValueError):
                continue
            observed_iso = observed_at.isoformat()

            rows.append({
                "event_id":     event_id,
                "mag":          props.get("mag"),
                "place":        props.get("place"),
                "observed_at":  observed_iso,
                "depth_km":     depth_km,
                "lon":          lon,
                "lat":          lat,
                "raw_json":     json.dumps(props, ensure_ascii=False),
                "dedup_hash":   _make_dedup_hash(event_id, observed_iso),
            })
        return rows

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        gj = self._fetch_geojson()
        events = self._parse_features(gj)

        return {
            "data":         events,
            "event_count":  len(events),
            "collected_at": now.isoformat(),
        }


if __name__ == "__main__":
    # 離線試跑：cd data-collectors && python3 -m collectors.global_climate.usgs_earthquake
    c = UsgsEarthquakeCollector.__new__(UsgsEarthquakeCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (usgs-eq-test)", "Accept": "application/geo+json"})
    out = c.collect()
    print(f"events: {out['event_count']}")
    if out["data"]:
        print("sample:", out["data"][0])
