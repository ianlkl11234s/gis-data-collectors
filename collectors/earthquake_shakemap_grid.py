"""
地震網格 shakemap 收集器（NCDR EQ1，全台 2.5km 網格）

端點（⚠️ 免金鑰）：
    https://dataapi.ncdr.nat.gov.tw/NCDRAPI/OpenData/NCDR/EQ1?DataFormat=json

特性（2026-07-29 實測）：
  - 回傳裸 JSON array，4,377 筆 = 經度 73 格 × 緯度 123 格（0.025° ≈ 2.5km）
  - 只有「最新一次」地震，下一次地震整檔覆寫 → 歷史只能靠本表存
  - 每列欄位：EventName / EventDateTime / Magnitude / EQ_WGS84_Lon / EQ_WGS84_Lat
              / Depth / WGS84_Lon / WGS84_Lat / PGA / PGV / Intensity
  - EventDateTime 無時區（如 2026-07-27T10:15:00），實際為台灣時間，且通常是
    CWA 發震時間取整分（CWA 10:14:49 → NCDR 10:15:00）→ 不能拿來精確 join
  - Intensity 已是數值（0.0 ~ 7.0），不是 '5弱' 這種文字階
  - 保留全部 4,377 格（含 PGA=0 的外海格），前端要畫完整面

效率守門（必要）：先問 DB 這個 event_time 是否已入庫，已入庫直接結束，
否則每 15 分鐘會做 4,377 列的 no-op conflict insert。

事件檔（2026-11-09，ADR-0026；前端改讀 R2，Supabase 只留短期）：
  每個事件寫一份 JSON：R2 events/shakemap/<UTC YYYYMMDDTHHMMSSZ>.json（immutable，
  公開網址 https://data.itsmigu.com/events/shakemap/…）＋同 key 進 S3 正本；
  另維護 R2 events/shakemap/index.json（事件清單，前端用來對應地震，短快取）。
  事件檔已存在＝這次地震處理過，整輪略過（DB 保留期限刪掉舊網格後，NCDR 仍回同一事件，
  不會被重寫回 DB）。R2／S3 未設定或失敗不影響 DB 寫入（best-effort，下一輪會補）。
"""

import json
from datetime import datetime, timedelta, timezone

import requests

import config
from storage.r2 import IMMUTABLE_CACHE_CONTROL, get_r2_storage
from .base import BaseCollector
from .earthquake_common import safe_float

NCDR_EQ1_URL = "https://dataapi.ncdr.nat.gov.tw/NCDRAPI/OpenData/NCDR/EQ1?DataFormat=json"

# NCDR EventDateTime 無時區，實際為台灣時間
TAIPEI_OFFSET = "+08:00"

# R2／S3 事件檔
EVENT_PREFIX = "events/shakemap"
INDEX_KEY = f"{EVENT_PREFIX}/index.json"
INDEX_CACHE_CONTROL = "public, max-age=300"
EVENT_COLUMNS = ["lon", "lat", "pga", "pgv", "intensity"]


def _event_dt_utc(event_time: str) -> datetime:
    dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    if dt.tzinfo is None:  # NCDR 無時區＝台灣時間
        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return dt.astimezone(timezone.utc)


def shakemap_event_key(event_time: str) -> str:
    """'2026-07-27T10:15:00+08:00' → 'events/shakemap/20260727T021500Z.json'（時間一律 UTC）。"""
    return f"{EVENT_PREFIX}/{_event_dt_utc(event_time).strftime('%Y%m%dT%H%M%SZ')}.json"


def build_event_payload(rows: list) -> dict:
    """同一事件的網格列 → 事件檔（欄式 cells，含 intensity 0 的外海格）。"""
    first = rows[0]
    return {
        "event_name": first["event_name"],
        "event_time": _event_dt_utc(first["event_time"]).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "magnitude": first["magnitude"],
        "eq_lon": first["eq_lon"],
        "eq_lat": first["eq_lat"],
        "depth": first["depth"],
        "source": "NCDR EQ1（dataapi.ncdr.nat.gov.tw）",
        "columns": EVENT_COLUMNS,
        "cells": [[r[c] for c in EVENT_COLUMNS] for r in rows],
    }


def index_entry(payload: dict, key: str) -> dict:
    return {
        "event_time": payload["event_time"],
        "event_name": payload["event_name"],
        "magnitude": payload["magnitude"],
        "eq_lon": payload["eq_lon"],
        "eq_lat": payload["eq_lat"],
        "key": key,
        "cell_count": len(payload["cells"]),
        "felt_cell_count": sum(1 for c in payload["cells"] if (c[4] or 0) > 0),
    }


def merge_index(index: dict | None, entry: dict) -> dict:
    events = [e for e in (index or {}).get("events", []) if e.get("key") != entry["key"]]
    events.append(entry)
    events.sort(key=lambda e: e["event_time"], reverse=True)
    return {"updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "events": events}


def publish_event(r2, s3, rows: list) -> str | None:
    """寫事件檔到 R2＋S3、更新 index。回傳 key；R2 未設定回 None。失敗會 raise（呼叫端吞掉）。"""
    if r2 is None or not rows:
        return None
    payload = build_event_payload(rows)
    key = shakemap_event_key(rows[0]["event_time"])
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if s3 is not None and s3.head(key) is None:
        if not s3.upload_snapshot(key, body):
            raise RuntimeError(f"S3 上傳失敗 {key}")
    if r2.head(key) is None:
        r2.put_json(key, payload, IMMUTABLE_CACHE_CONTROL)
    r2.put_json(INDEX_KEY, merge_index(r2.get_json(INDEX_KEY), index_entry(payload, key)), INDEX_CACHE_CONTROL)
    return key


class EarthquakeShakemapGridCollector(BaseCollector):
    """NCDR 地震 2.5km 網格 shakemap 收集器"""

    name = "earthquake_shakemap_grid"
    interval_minutes = config.EARTHQUAKE_SHAKEMAP_GRID_INTERVAL

    # 單次最多寫 4,377 列 + 下載 ~1MB，比預設 300s 保守放寬
    COLLECT_TIMEOUT = 600

    def __init__(self):
        super().__init__()
        self._r2 = get_r2_storage()
        self._s3 = None
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (earthquake-shakemap-grid)",
            "Accept": "application/json",
        })

    def _fetch(self) -> list:
        resp = self._session.get(NCDR_EQ1_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = json.loads(resp.content.decode('utf-8-sig'))
        if not isinstance(data, list):
            raise ValueError(f"NCDR EQ1 回傳非陣列: {type(data).__name__}")
        return data

    @staticmethod
    def _to_tz(dt_str: str) -> str:
        """'2026-07-27T10:15:00' → '2026-07-27T10:15:00+08:00'（已帶時區則原樣回傳）"""
        s = (dt_str or '').strip()
        if not s:
            return ''
        if s.endswith('Z') or '+' in s[10:] or s[10:].count('-') > 0:
            return s
        return f"{s}{TAIPEI_OFFSET}"

    def _s3_storage(self):
        if self._s3 is None and config.S3_BUCKET:
            from storage.s3 import S3Storage
            self._s3 = S3Storage()
        return self._s3

    def _event_published(self, event_time: str) -> bool:
        """R2 已有事件檔＝處理過。R2 未設定或查詢失敗回 False（退回 DB 守門）。"""
        if self._r2 is None:
            return False
        try:
            return self._r2.head(shakemap_event_key(event_time)) is not None
        except Exception as e:
            print(f"   [shakemap_grid] R2 事件檔查詢失敗（改用 DB 守門）: {e}")
            return False

    def _already_stored(self, event_time: str) -> bool:
        if not self.supabase_writer:
            return False
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM live.earthquake_shakemap_grid "
                        "WHERE event_time = %s LIMIT 1",
                        (event_time,),
                    )
                    return cur.fetchone() is not None
        except Exception as e:
            print(f"   [shakemap_grid] 既有事件查詢失敗（改為照常寫入）: {e}")
            return False

    def collect(self) -> dict:
        fetch_time = datetime.now()
        raw = self._fetch()

        if not raw:
            print("   NCDR EQ1 無資料")
            return {
                'fetch_time': fetch_time.isoformat(),
                'event_time': None,
                'grid_count': 0,
                'skipped': True,
                'data': [],
            }

        first = raw[0]
        event_time = self._to_tz(first.get('EventDateTime', ''))
        event_name = first.get('EventName', '')

        if not event_time:
            print("   來源無 EventDateTime，略過")
            return {
                'fetch_time': fetch_time.isoformat(),
                'event_time': None,
                'grid_count': 0,
                'skipped': True,
                'data': [],
            }

        if self._event_published(event_time):
            print(f"   {event_name} {event_time} 的事件檔已在 R2 → 略過 {len(raw)} 格")
            return {
                'fetch_time': fetch_time.isoformat(),
                'event_time': event_time,
                'event_name': event_name,
                'grid_count': 0,
                'skipped': True,
                'data': [],
            }

        rows = []
        for r in raw:
            lon = safe_float(r.get('WGS84_Lon'))
            lat = safe_float(r.get('WGS84_Lat'))
            if lon is None or lat is None:
                continue
            rows.append({
                'event_name': r.get('EventName', ''),
                'event_time': self._to_tz(r.get('EventDateTime', '')),
                'magnitude': safe_float(r.get('Magnitude')),
                'eq_lon': safe_float(r.get('EQ_WGS84_Lon')),
                'eq_lat': safe_float(r.get('EQ_WGS84_Lat')),
                'depth': safe_float(r.get('Depth')),
                'lon': lon,
                'lat': lat,
                'pga': safe_float(r.get('PGA')),
                'pgv': safe_float(r.get('PGV')),
                'intensity': safe_float(r.get('Intensity')),
            })

        felt = sum(1 for r in rows if (r['intensity'] or 0) > 0)
        print(f"   {event_name} {event_time}：{len(rows)} 格（有感 {felt} 格）")

        # 事件檔（R2＋S3）：best-effort，失敗不影響 DB；下一輪 R2 仍無檔會再試
        if rows:
            try:
                key = publish_event(self._r2, self._s3_storage(), rows)
                if key:
                    print(f"   事件檔已發布 {key}")
            except Exception as e:
                print(f"   [shakemap_grid] ⚠️ 事件檔發布失敗（DB 照寫）: {e}")

        if self._already_stored(event_time):
            print(f"   {event_name} {event_time} 的網格已入庫 → 不重寫 DB")
            return {
                'fetch_time': fetch_time.isoformat(),
                'event_time': event_time,
                'event_name': event_name,
                'grid_count': 0,
                'skipped': True,
                'data': [],
            }

        return {
            'fetch_time': fetch_time.isoformat(),
            'event_time': event_time,
            'event_name': event_name,
            'magnitude': safe_float(first.get('Magnitude')),
            'grid_count': len(rows),
            'felt_grid_count': felt,
            'skipped': False,
            'data': rows,
        }
