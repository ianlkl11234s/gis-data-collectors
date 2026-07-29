"""
完整地震目錄收集器（CWA E-A0073-001，含無感地震）

原本混在 earthquake collector 裡跟即時報告一起抓；2026-07 拆出來獨立跑，理由：
  - 目錄是「本年度正式地震目錄」，上游約半年才更新一批，不需要跟 15 分鐘的
    即時報告同頻
  - 每次回 1,000+ 筆，混在即時 collector 裡等於每輪重打整份目錄

端點走 fileapi（⚠️ 不是 opendata/api/）：
    https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/E-A0073-001

寫入 live.earthquake_events，report_type='catalog'，event_id 沿用歷史格式
`cat_{OriginTime}_{lat}_{lon}`（改格式會讓既有資料重複），ON CONFLICT DO NOTHING。

效率守門：抓完先問 DB「這份目錄的時間範圍內已經有幾筆 catalog」，筆數沒少就
直接結束，log 一行不寫入。
"""

from datetime import datetime

import requests

import config
from .base import BaseCollector
from .earthquake_common import safe_float


class EarthquakeCatalogCollector(BaseCollector):
    """CWA 完整地震目錄（含無感）收集器 — 每日檢查一次，有新資料才寫"""

    name = "earthquake_catalog"
    interval_minutes = config.EARTHQUAKE_CATALOG_INTERVAL

    CATALOG_URL = f"{config.CWA_FILE_API_BASE}/E-A0073-001"

    def __init__(self):
        super().__init__()
        self.api_key = config.CWA_API_KEY
        self._session = requests.Session()

        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")

    def _fetch_catalog(self) -> list:
        """從 CWA fileapi 取得完整地震目錄（含無感地震）"""
        params = {
            'Authorization': self.api_key,
            'downloadType': 'WEB',
            'format': 'JSON',
        }

        response = self._session.get(
            self.CATALOG_URL,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        data = response.json()
        catalog = data.get('cwaopendata', {}).get('Dataset', {}).get('Catalog', {})
        return catalog.get('EarthquakeInfo', []) or []

    @staticmethod
    def _parse_entry(eq: dict) -> dict:
        lat = safe_float(eq.get('EpicenterLatitude'))
        lon = safe_float(eq.get('EpicenterLongitude'))
        origin_time = eq.get('OriginTime', '')
        return {
            # 沿用 2026-07 前既有格式，改了會讓歷史 row 全部重新插入一次
            'event_id': f"cat_{origin_time}_{lat}_{lon}",
            'origin_time': origin_time,
            'latitude': lat,
            'longitude': lon,
            'focal_depth_km': safe_float(eq.get('FocalDepth')),
            'local_magnitude': safe_float(eq.get('LocalMagnitude')),
            'station_number': eq.get('StationNumber'),
            'quality': eq.get('Quality', ''),
            'review_status': eq.get('ReviewStatus', ''),
        }

    def _existing_count(self, min_time: str, max_time: str) -> int:
        """DB 內該時間範圍已有幾筆 catalog row；查不到回 -1（代表無法判斷 → 照寫）"""
        if not self.supabase_writer:
            return -1
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT count(*) FROM live.earthquake_events "
                        "WHERE report_type = 'catalog' "
                        "  AND occurred_at >= %s AND occurred_at <= %s",
                        (min_time, max_time),
                    )
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
        except Exception as e:
            print(f"   [catalog] 既有筆數查詢失敗（改為全量寫入）: {e}")
            return -1

    def collect(self) -> dict:
        fetch_time = datetime.now()

        print("   正在取得完整地震目錄 (E-A0073-001)...")
        raw = self._fetch_catalog()
        entries = [self._parse_entry(eq) for eq in raw if eq.get('OriginTime')]
        entries.sort(key=lambda x: x['origin_time'], reverse=True)

        if not entries:
            print("   目錄為空，略過")
            return {
                'fetch_time': fetch_time.isoformat(),
                'catalog_count': 0,
                'new_data': False,
                'data': [],
            }

        max_time = entries[0]['origin_time']
        min_time = entries[-1]['origin_time']
        catalog_range = f"{min_time[:10]} ~ {max_time[:10]}"

        existing = self._existing_count(min_time, max_time)
        if existing >= len(entries):
            print(f"   目錄 {len(entries)} 筆（{catalog_range}），DB 已有 {existing} 筆 → 無新資料，略過寫入")
            return {
                'fetch_time': fetch_time.isoformat(),
                'catalog_count': len(entries),
                'existing_count': existing,
                'catalog_range': catalog_range,
                'new_data': False,
                'data': [],
            }

        print(f"   目錄 {len(entries)} 筆（{catalog_range}），DB 已有 {existing} 筆 → 寫入")
        return {
            'fetch_time': fetch_time.isoformat(),
            'catalog_count': len(entries),
            'existing_count': existing,
            'catalog_range': catalog_range,
            'new_data': True,
            'data': entries,
        }
