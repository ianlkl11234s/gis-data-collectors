"""
省道 + 市區路況即時收集器

從 TDX Road Traffic Live 取得省道（全國 1 endpoint）與市區（縣市 N）路段即時車速 / 旅行時間 / 壅塞等級：
  /v2/Road/Traffic/Live/Highway        — 省道全國
  /v2/Road/Traffic/Live/City/{City}    — 市區（預設 5 縣市實測堪用）

實測（2026-06-19）22 縣市市區資料品質參差，僅 5 縣市有及時資料：
  Taoyuan / Taichung / Tainan / Keelung / YilanCounty
（北市 sample 顯示 6/16 三天前停滯，其他多回 0 段）

線型 geometry 走 reference.road_sections_geometry（靜態，由 import_road_sections.py 一次性載入）。
"""

import time
from datetime import datetime
from typing import Optional

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


# 等級 ID 對應數值（CongestionLevel 欄位）
# TDX 慣例：1=順暢 2=車多 3=略壅塞 4=壅塞，-99=無資料
def _parse_level(v) -> Optional[int]:
    try:
        n = int(v)
        return n if n in (-99, 0, 1, 2, 3, 4) else None
    except (TypeError, ValueError):
        return None


class RoadCongestionCollector(BaseCollector):
    """省道 + 市區即時路況收集器"""

    name = "road_congestion"
    interval_minutes = config.ROAD_CONGESTION_INTERVAL

    def __init__(self, cities: list = None):
        super().__init__()
        self.cities = cities if cities is not None else config.ROAD_CONGESTION_CITIES
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch(self, path: str) -> tuple:
        url = f"{config.TDX_API_BASE}{path}"
        headers = self.auth.get_auth_header()
        response = self._session.get(
            url, headers=headers, params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data.get('LiveTraffics', []), data.get('AuthorityCode')
        return [], None

    @staticmethod
    def _parse_section(s: dict, source: str, city: Optional[str], authority: Optional[str], fetch_time: datetime) -> dict:
        sid = s.get('SectionID', '')
        return {
            'section_uid': f"{source}_{sid}",
            'section_id': sid,
            'source': source,
            'city': city,
            'authority_code': authority,
            'travel_time': s.get('TravelTime'),
            'travel_speed': s.get('TravelSpeed'),
            'congestion_level': _parse_level(s.get('CongestionLevel')),
            'congestion_level_id': s.get('CongestionLevelID'),
            'data_sources': s.get('DataSources'),
            'data_collect_time': s.get('DataCollectTime'),
            '_fetch_time': fetch_time.isoformat(),
        }

    def _fetch_variant(self, label: str, path: str, source: str, city: Optional[str], results: list, stats: dict, fetch_time: datetime):
        try:
            sections, authority = self._fetch(path)
            parsed = [self._parse_section(s, source, city, authority, fetch_time) for s in sections]
            results.extend(parsed)
            valid = sum(1 for p in parsed if (p['travel_time'] or -99) >= 0)
            stats[label] = {
                'sections': len(parsed),
                'valid': valid,
                'authority': authority,
            }
            print(f"   ✓ {label}: {len(parsed)} 段 ({valid} 有及時資料)")
            time.sleep(config.REQUEST_INTERVAL)
        except requests.exceptions.HTTPError as e:
            stats[label] = {'error': f'HTTP {e.response.status_code}'}
            print(f"   ✗ {label}: HTTP {e.response.status_code}")
        except Exception as e:
            stats[label] = {'error': str(e)}
            print(f"   ✗ {label}: {e}")

    def collect(self) -> dict:
        fetch_time = datetime.now()
        all_sections = []
        stats = {}

        # 省道（全國 1 endpoint）
        self._fetch_variant(
            'highway', '/v2/Road/Traffic/Live/Highway',
            'highway', None, all_sections, stats, fetch_time,
        )

        # 市區（縣市迴圈）
        for city in self.cities:
            self._fetch_variant(
                f'city/{city}', f'/v2/Road/Traffic/Live/City/{city}',
                'city', city, all_sections, stats, fetch_time,
            )

        total = len(all_sections)
        valid = sum(1 for s in all_sections if (s['travel_time'] or -99) >= 0)
        print(f"\n   📊 路況總計: {total} 段 ({valid} 有及時資料)")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_sections': total,
            'valid_sections': valid,
            'by_variant': stats,
            'data': all_sections,
        }
