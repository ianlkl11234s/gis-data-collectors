"""
公車即時位置收集器

從 TDX API 取得公車即時定頻資料 (RealTimeByFrequency / A1)。
僅保留執勤中 (DutyStatus=1) 且正常營運 (BusStatus=0) 的車輛。
"""

from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from .base import BaseCollector


class BusCollector(BaseCollector):
    """公車即時位置收集器"""

    name = "bus"
    interval_minutes = config.BUS_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self.auth = TDXAuth(session=self._session)

    def _fetch_realtime_by_frequency(self, city: str) -> list:
        """取得指定城市的公車即時定頻資料 (A1)"""
        url = f"{config.TDX_API_BASE}/v2/Bus/RealTimeByFrequency/City/{city}"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _filter_active(buses: list) -> list:
        """過濾出執勤中且正常營運的車輛

        - DutyStatus 1 = 執勤中
        - BusStatus 0 = 正常
        """
        return [
            b for b in buses
            if b.get('DutyStatus') == 1 and b.get('BusStatus') == 0
        ]

    def collect(self) -> dict:
        """收集各城市公車即時位置資料"""
        fetch_time = datetime.now()
        all_buses = []
        city_stats = {}

        for city in config.BUS_CITIES:
            try:
                raw = self._fetch_realtime_by_frequency(city)
                active = self._filter_active(raw)

                # 標記城市與抓取時間
                for bus in active:
                    bus['_city'] = city
                    bus['_fetch_time'] = fetch_time.isoformat()

                city_stats[city] = {
                    'raw': len(raw),
                    'active': len(active),
                }
                all_buses.extend(active)

                print(f"   {city}: {len(active)}/{len(raw)} 台執勤中")

            except requests.exceptions.HTTPError as e:
                print(f"   {city}: HTTP 錯誤 {e.response.status_code}")
                city_stats[city] = {'error': str(e)}
            except Exception as e:
                print(f"   {city}: 錯誤 {e}")
                city_stats[city] = {'error': str(e)}

        total = len(all_buses)
        print(f"   ✓ 合計: {total} 台公車")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_active': total,
            'by_city': city_stats,
            'data': all_buses,
        }
