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
"""

import json
from datetime import datetime

import requests

import config
from .base import BaseCollector
from .earthquake_common import safe_float

NCDR_EQ1_URL = "https://dataapi.ncdr.nat.gov.tw/NCDRAPI/OpenData/NCDR/EQ1?DataFormat=json"

# NCDR EventDateTime 無時區，實際為台灣時間
TAIPEI_OFFSET = "+08:00"


class EarthquakeShakemapGridCollector(BaseCollector):
    """NCDR 地震 2.5km 網格 shakemap 收集器"""

    name = "earthquake_shakemap_grid"
    interval_minutes = config.EARTHQUAKE_SHAKEMAP_GRID_INTERVAL

    # 單次最多寫 4,377 列 + 下載 ~1MB，比預設 300s 保守放寬
    COLLECT_TIMEOUT = 600

    def __init__(self):
        super().__init__()
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

        if self._already_stored(event_time):
            print(f"   {event_name} {event_time} 的網格已入庫 → 略過 {len(raw)} 格")
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
