"""
鄉鎮震度收集器（CWA E-A0015-005，全台 368 鄉鎮逐區震度）

端點（⚠️ 免金鑰，S3 直抓，不走 opendata API）：
    https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Earthquake/E-A0015-005.json

特性：
  - 只保留「最新一次」有感地震的鄉鎮震度，下一次地震就被整檔覆寫
    → 歷史只能靠 live.earthquake_town_intensity 自己存
  - 一次 22 縣市 × 共 368 鄉鎮，含 TownCode（行政區代碼）與鄉鎮代表點座標
  - 檔內沒有 EarthquakeNo，只有 identifier（如 CWA-TPIR1150727021449a）
    → earthquake_no 欄位留 NULL，靠 origin_time 跟 earthquake_events 對齊

效率守門：先用 origin_time 問 DB 這場地震是否已入庫，已入庫直接結束，
避免每 15 分鐘重打 368 列 no-op。
"""

import json
from datetime import datetime

import requests

import config
from .base import BaseCollector
from .earthquake_common import as_list, intensity_to_value, safe_float

TOWN_INTENSITY_URL = (
    "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Earthquake/E-A0015-005.json"
)


class EarthquakeTownIntensityCollector(BaseCollector):
    """全台 368 鄉鎮逐區震度收集器"""

    name = "earthquake_town_intensity"
    interval_minutes = config.EARTHQUAKE_TOWN_INTENSITY_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (earthquake-town-intensity)",
            "Accept": "application/json",
        })

    def _fetch(self) -> dict:
        resp = self._session.get(TOWN_INTENSITY_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        # S3 上的檔可能帶 UTF-8 BOM，用 resp.content 自行解碼比 resp.json() 保險
        return json.loads(resp.content.decode('utf-8-sig'))

    def _already_stored(self, origin_time: str) -> bool:
        """該場地震的鄉鎮震度是否已入庫"""
        if not self.supabase_writer:
            return False
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM live.earthquake_town_intensity "
                        "WHERE origin_time = %s LIMIT 1",
                        (origin_time,),
                    )
                    return cur.fetchone() is not None
        except Exception as e:
            print(f"   [town_intensity] 既有事件查詢失敗（改為照常寫入）: {e}")
            return False

    def collect(self) -> dict:
        fetch_time = datetime.now()
        payload = self._fetch()

        root = payload.get('cwaopendata', {}) or {}
        eq = root.get('Earthquake', {}) or {}
        origin_time = eq.get('OriginTime', '')

        if not origin_time:
            print("   來源無 OriginTime，略過")
            return {
                'fetch_time': fetch_time.isoformat(),
                'origin_time': None,
                'town_count': 0,
                'skipped': True,
                'data': [],
            }

        if self._already_stored(origin_time):
            print(f"   {origin_time} 的鄉鎮震度已入庫 → 略過")
            return {
                'fetch_time': fetch_time.isoformat(),
                'origin_time': origin_time,
                'town_count': 0,
                'skipped': True,
                'data': [],
            }

        magnitude = safe_float((eq.get('Magnitude', {}) or {}).get('MagnitudeValue'))
        epicenter_lat = safe_float(eq.get('EpicenterLatitude'))
        epicenter_lon = safe_float(eq.get('EpicenterLongitude'))
        depth_km = safe_float(eq.get('FocalDepth'))
        report_id = root.get('identifier', '')

        rows = []
        for county in as_list((eq.get('Intensity', {}) or {}).get('County')):
            county_name = county.get('CountyName', '')
            county_code = county.get('CountyCode', '')
            county_max = county.get('CountyMaxIntensity', '')
            for town in as_list(county.get('Town')):
                town_code = town.get('TownCode')
                if not town_code:
                    continue
                intensity_txt = town.get('StationIntensity', '')
                rows.append({
                    'origin_time': origin_time,
                    'report_id': report_id,
                    'earthquake_no': None,  # 來源不提供
                    'magnitude': magnitude,
                    'depth_km': depth_km,
                    'epicenter_lat': epicenter_lat,
                    'epicenter_lon': epicenter_lon,
                    'county_name': county_name,
                    'county_code': county_code,
                    'county_max_intensity': county_max,
                    'town_name': town.get('TownName', ''),
                    'town_code': town_code,
                    'lat': safe_float(town.get('StationLatitude')),
                    'lon': safe_float(town.get('StationLongitude')),
                    'intensity': intensity_txt,
                    'intensity_value': intensity_to_value(intensity_txt),
                })

        print(f"   {origin_time} M{magnitude}：{len(rows)} 個鄉鎮震度")

        return {
            'fetch_time': fetch_time.isoformat(),
            'origin_time': origin_time,
            'report_id': report_id,
            'magnitude': magnitude,
            'town_count': len(rows),
            'skipped': False,
            'data': rows,
        }
