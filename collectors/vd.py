"""
VD 車輛偵測器即時車流量收集器

從 TDX API 取得車輛偵測器 (Vehicle Detector) 的即時交通資料。
資料每 5 分鐘更新一次。
"""

import time
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from .base import BaseCollector


# 城市代碼對照
CITY_NAMES = {
    'Taipei': '台北市',
    'NewTaipei': '新北市',
}

# 車種對照
VEHICLE_TYPES = {
    'M': '機車',
    'S': '小型車',
    'L': '大型車',
    'T': '聯結車',
}


class VDCollector(BaseCollector):
    """VD 車輛偵測器即時車流量收集器"""

    name = "vd"
    interval_minutes = config.VD_INTERVAL

    def __init__(self, cities: list = None):
        super().__init__()
        self.cities = cities or config.VD_CITIES
        self._session = requests.Session()
        self.auth = TDXAuth(session=self._session)

    def _fetch_city(self, city: str) -> tuple:
        """取得單一城市的 VD 即時資料

        Args:
            city: 城市代碼

        Returns:
            (VD 資料列表, 更新時間)
        """
        url = f"{config.TDX_API_BASE}/v2/Road/Traffic/Live/VD/City/{city}"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        update_time = data.get('UpdateTime')
        vd_lives = data.get('VDLives', [])

        return vd_lives, update_time

    def _parse_vd_live(self, vd_live: dict) -> dict:
        """解析單一 VD 即時資料

        Args:
            vd_live: VD 即時資料

        Returns:
            解析後的資料
        """
        vd_id = vd_live.get('VDID', '')
        collect_time = vd_live.get('DataCollectTime', '')
        status = vd_live.get('Status', -1)

        # 解析車流資料
        total_volume = {'M': 0, 'S': 0, 'L': 0, 'T': 0}
        speeds = []
        occupancies = []
        lane_count = 0

        for link_flow in vd_live.get('LinkFlows', []):
            for lane in link_flow.get('Lanes', []):
                lane_count += 1

                speed = lane.get('Speed', 0)
                occupancy = lane.get('Occupancy', 0)

                if speed > 0:
                    speeds.append(speed)
                if occupancy >= 0:
                    occupancies.append(occupancy)

                for vehicle in lane.get('Vehicles', []):
                    v_type = vehicle.get('VehicleType', '')
                    volume = vehicle.get('Volume', 0)
                    if volume > 0 and v_type in total_volume:
                        total_volume[v_type] += volume

        # 計算統計值
        avg_speed = sum(speeds) / len(speeds) if speeds else 0
        avg_occupancy = sum(occupancies) / len(occupancies) if occupancies else 0
        total_all = sum(total_volume.values())

        return {
            'VDID': vd_id,
            'DataCollectTime': collect_time,
            'Status': status,
            'LaneCount': lane_count,
            'AvgSpeed': round(avg_speed, 1),
            'AvgOccupancy': round(avg_occupancy, 1),
            'TotalVolume': total_all,
            'VolumeMotorcycle': total_volume['M'],
            'VolumeSmallCar': total_volume['S'],
            'VolumeLargeCar': total_volume['L'],
            'VolumeTrailer': total_volume['T'],
        }

    def collect(self) -> dict:
        """收集所有城市的 VD 即時車流量資料"""
        fetch_time = datetime.now()
        all_vd_data = []
        city_stats = {}
        api_update_time = None

        for city in self.cities:
            city_name = CITY_NAMES.get(city, city)
            try:
                vd_lives, update_time = self._fetch_city(city)
                api_update_time = update_time or api_update_time

                # 解析每個 VD 資料
                city_vd_data = []
                for vd_live in vd_lives:
                    parsed = self._parse_vd_live(vd_live)
                    parsed['City'] = city
                    parsed['CityName'] = city_name
                    parsed['_fetch_time'] = fetch_time.isoformat()
                    city_vd_data.append(parsed)

                all_vd_data.extend(city_vd_data)

                # 統計
                total_volume = sum(d['TotalVolume'] for d in city_vd_data)
                valid_speeds = [d['AvgSpeed'] for d in city_vd_data if d['AvgSpeed'] > 0]
                avg_speed = sum(valid_speeds) / len(valid_speeds) if valid_speeds else 0

                city_stats[city] = {
                    'name': city_name,
                    'vd_count': len(city_vd_data),
                    'total_volume': total_volume,
                    'avg_speed': round(avg_speed, 1)
                }

                print(f"   ✓ {city_name}: {len(city_vd_data)} VD | "
                      f"車流 {total_volume:,} 輛 | 均速 {avg_speed:.1f} km/h")

                time.sleep(config.REQUEST_INTERVAL)

            except requests.exceptions.HTTPError as e:
                print(f"   ✗ {city_name}: HTTP 錯誤 {e.response.status_code}")
                city_stats[city] = {'name': city_name, 'error': str(e)}

            except Exception as e:
                print(f"   ✗ {city_name}: {e}")
                city_stats[city] = {'name': city_name, 'error': str(e)}

        # 總計
        total_vd = len(all_vd_data)
        total_volume = sum(d['TotalVolume'] for d in all_vd_data)
        valid_speeds = [d['AvgSpeed'] for d in all_vd_data if d['AvgSpeed'] > 0]
        avg_speed = sum(valid_speeds) / len(valid_speeds) if valid_speeds else 0

        # 車種統計
        type_totals = {
            'motorcycle': sum(d['VolumeMotorcycle'] for d in all_vd_data),
            'small_car': sum(d['VolumeSmallCar'] for d in all_vd_data),
            'large_car': sum(d['VolumeLargeCar'] for d in all_vd_data),
            'trailer': sum(d['VolumeTrailer'] for d in all_vd_data),
        }

        print(f"\n   📊 總計: {total_vd} VD | "
              f"車流 {total_volume:,} 輛/5分 | 均速 {avg_speed:.1f} km/h")

        return {
            'fetch_time': fetch_time.isoformat(),
            'api_update_time': api_update_time,
            'total_vd': total_vd,
            'total_volume': total_volume,
            'avg_speed': round(avg_speed, 1),
            'by_vehicle_type': type_totals,
            'by_city': city_stats,
            'data': all_vd_data
        }
