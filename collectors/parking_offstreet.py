"""
路外停車場（OffStreet）即時可用性收集器

從 TDX OffStreet ParkingAvailability 三變體取得即時剩餘車位：
  (1) /v1/Parking/OffStreet/ParkingAvailability/City/{City}  — 縣市路外停車場
  (2) /v1/Parking/OffStreet/ParkingAvailability/Road/Freeway/ServiceArea — 國道服務區
  (3) /v1/Parking/OffStreet/ParkingAvailability/Tourism — 觀光景點停車場

三變體 response schema 一致（ParkingAvailabilities[] + CarParkID/CarParkName...），
unified 寫入 live.parking_lots_availability 與 _current。
"""

import time
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


CITY_NAMES = {
    'Taipei': '臺北市', 'NewTaipei': '新北市', 'Taoyuan': '桃園市',
    'Taichung': '臺中市', 'Tainan': '臺南市', 'Kaohsiung': '高雄市',
    'Keelung': '基隆市', 'Hsinchu': '新竹市', 'HsinchuCounty': '新竹縣',
    'MiaoliCounty': '苗栗縣', 'Chiayi': '嘉義市', 'ChiayiCounty': '嘉義縣',
    'Changhua': '彰化縣', 'NantouCounty': '南投縣', 'Yunlin': '雲林縣',
    'YilanCounty': '宜蘭縣', 'HualienCounty': '花蓮縣', 'TaitungCounty': '臺東縣',
    'Pingtung': '屏東縣', 'PenghuCounty': '澎湖縣',
    'KinmenCounty': '金門縣', 'LienchiangCounty': '連江縣',
}


class ParkingOffStreetCollector(BaseCollector):
    """路外停車場即時可用性收集器（3 變體）"""

    name = "parking_offstreet"
    interval_minutes = config.PARKING_OFFSTREET_INTERVAL

    def __init__(self, cities: list = None):
        super().__init__()
        self.cities = cities or config.PARKING_OFFSTREET_CITIES
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch(self, path: str) -> list:
        url = f"{config.TDX_API_BASE}{path}"
        headers = self.auth.get_auth_header()
        response = self._session.get(
            url, headers=headers, params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            return data.get('ParkingAvailabilities', []), data.get('AuthorityCode')
        return [], None

    @staticmethod
    def _parse_lot(lot: dict, source_category: str, authority: str, sub_category: str, fetch_time: datetime) -> dict:
        name = lot.get('CarParkName') or {}
        car_park_id = lot.get('CarParkID', '')
        authority_safe = authority or 'UNK'
        return {
            'car_park_uid': f"{authority_safe}_{car_park_id}",
            'car_park_id': car_park_id,
            'car_park_name': name.get('Zh_tw', '') if isinstance(name, dict) else str(name),
            'source_category': source_category,
            'authority_code': authority,
            'sub_category': sub_category,
            'total_spaces': lot.get('TotalSpaces'),
            'available_spaces': lot.get('AvailableSpaces'),
            'full_status': lot.get('FullStatus'),
            'service_status': lot.get('ServiceStatus'),
            'charge_status': lot.get('ChargeStatus'),
            'space_types': lot.get('Availabilities'),
            'data_collect_time': lot.get('DataCollectTime'),
            '_fetch_time': fetch_time.isoformat(),
        }

    def _fetch_variant(self, label: str, path: str, source_category: str, sub_category: str, results: list, stats: dict):
        try:
            lots, authority = self._fetch(path)
            parsed = [self._parse_lot(l, source_category, authority, sub_category, self._fetch_time) for l in lots]
            results.extend(parsed)
            stats[label] = {
                'lots': len(parsed),
                'authority': authority,
                'available_spaces': sum(l['available_spaces'] for l in parsed if (l['available_spaces'] or 0) >= 0),
                'total_spaces': sum(l['total_spaces'] for l in parsed if (l['total_spaces'] or 0) >= 0),
            }
            print(f"   ✓ {label}: {len(parsed)} 場館")
            time.sleep(config.REQUEST_INTERVAL)
        except requests.exceptions.HTTPError as e:
            stats[label] = {'error': f'HTTP {e.response.status_code}'}
            print(f"   ✗ {label}: HTTP {e.response.status_code}")
        except Exception as e:
            stats[label] = {'error': str(e)}
            print(f"   ✗ {label}: {e}")

    def collect(self) -> dict:
        self._fetch_time = datetime.now()
        all_lots = []
        stats = {}

        # (1) City × N
        for city in self.cities:
            label = f"city/{city}"
            path = f"/v1/Parking/OffStreet/ParkingAvailability/City/{city}"
            self._fetch_variant(label, path, 'city', city, all_lots, stats)

        # (2) 國道服務區
        self._fetch_variant(
            'freeway_service_area',
            '/v1/Parking/OffStreet/ParkingAvailability/Road/Freeway/ServiceArea',
            'freeway_service_area', None, all_lots, stats,
        )

        # (3) 觀光景點
        self._fetch_variant(
            'tourism',
            '/v1/Parking/OffStreet/ParkingAvailability/Tourism',
            'tourism', None, all_lots, stats,
        )

        total = len(all_lots)
        print(f"\n   📊 OffStreet 總計: {total} 場館")

        return {
            'fetch_time': self._fetch_time.isoformat(),
            'total_lots': total,
            'by_variant': stats,
            'data': all_lots,
        }
