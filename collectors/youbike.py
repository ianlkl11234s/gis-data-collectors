"""
YouBike 即時車位資料收集器

從 TDX API 取得公共自行車即時車位資料。
"""

import time
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


# 城市代碼對照
CITY_NAMES = {
    'Taipei': '臺北市',
    'NewTaipei': '新北市',
    'Taoyuan': '桃園市',
    'Taichung': '臺中市',
    'Tainan': '臺南市',
    'Kaohsiung': '高雄市',
    'Hsinchu': '新竹市',
    'Chiayi': '嘉義市',
}


class YouBikeCollector(BaseCollector):
    """YouBike 即時車位資料收集器（使用 Session 重用連線）"""

    name = "youbike"
    interval_minutes = config.YOUBIKE_INTERVAL

    def __init__(self, cities: list = None):
        super().__init__()
        self.cities = cities or config.YOUBIKE_CITIES
        # 建立共用 Session（TDXSession 會自動通過全域 TDX rate limiter）
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_city(self, city: str) -> list:
        """取得單一城市的即時資料"""
        url = f"{config.TDX_API_BASE}/v2/Bike/Availability/City/{city}"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        return data if isinstance(data, list) else data.get('BikeAvailabilities', [])

    def collect(self) -> dict:
        """收集所有城市的即時車位資料"""
        fetch_time = datetime.now()
        all_stations = []
        city_stats = {}

        for city in self.cities:
            city_name = CITY_NAMES.get(city, city)
            try:
                stations = self._fetch_city(city)

                # 加入城市標記
                for station in stations:
                    station['_city'] = city
                    station['_fetch_time'] = fetch_time.isoformat()

                all_stations.extend(stations)

                # 統計
                bikes = sum(s.get('AvailableRentBikes', 0) for s in stations)
                spaces = sum(s.get('AvailableReturnBikes', 0) for s in stations)
                city_stats[city] = {
                    'name': city_name,
                    'stations': len(stations),
                    'bikes': bikes,
                    'spaces': spaces
                }

                print(f"   ✓ {city_name}: {len(stations)} 站 | "
                      f"可借 {bikes:,} | 空位 {spaces:,}")

                time.sleep(config.REQUEST_INTERVAL)

            except requests.exceptions.HTTPError as e:
                print(f"   ✗ {city_name}: HTTP 錯誤 {e.response.status_code}")
                city_stats[city] = {'name': city_name, 'error': str(e)}

            except Exception as e:
                print(f"   ✗ {city_name}: {e}")
                city_stats[city] = {'name': city_name, 'error': str(e)}

        # 總計
        total_stations = len(all_stations)
        total_bikes = sum(s.get('AvailableRentBikes', 0) for s in all_stations)
        total_spaces = sum(s.get('AvailableReturnBikes', 0) for s in all_stations)

        print(f"\n   📊 總計: {total_stations} 站 | "
              f"可借 {total_bikes:,} | 空位 {total_spaces:,}")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_stations': total_stations,
            'total_bikes': total_bikes,
            'total_spaces': total_spaces,
            'by_city': city_stats,
            'data': all_stations
        }
