"""
氣象觀測資料收集器

從中央氣象署 (CWA) API 取得全台氣象站即時觀測資料。
API 端點: O-A0001-001 (氣象觀測站-全測站逐時氣象資料)
"""

from datetime import datetime

import requests

import config
from .base import BaseCollector
from storage.run_receipt import max_upstream_iso


class WeatherCollector(BaseCollector):
    """氣象觀測資料收集器（使用 Session 重用連線）"""

    name = "weather"
    interval_minutes = config.WEATHER_INTERVAL

    # CWA API 端點
    ENDPOINT = "O-A0001-001"

    def __init__(self, stations: list = None):
        """
        初始化氣象收集器

        Args:
            stations: 指定收集的測站 ID 列表，None 表示收集全部
        """
        super().__init__()
        self.stations = stations or config.WEATHER_STATIONS
        self.api_key = config.CWA_API_KEY
        # 建立共用 Session，重用 TCP 連線以節省記憶體
        self._session = requests.Session()

        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")

    def _fetch_observations(self) -> dict:
        """從 CWA API 取得觀測資料"""
        url = f"{config.CWA_API_BASE}/v1/rest/datastore/{self.ENDPOINT}"

        params = {
            'Authorization': self.api_key,
            'format': 'JSON',
        }

        # 如果有指定測站，加入篩選條件
        if self.stations:
            params['StationId'] = ','.join(self.stations)

        response = self._session.get(
            url,
            params=params,
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()

        if data.get('success') != 'true':
            raise ValueError(f"API 回傳失敗: {data}")

        return data

    def _parse_station(self, station: dict, fetch_time: datetime) -> dict:
        """解析單一測站資料"""
        geo_info = station.get('GeoInfo', {})
        weather = station.get('WeatherElement', {})
        obs_time = station.get('ObsTime', {})

        # 取得座標
        coords = geo_info.get('Coordinates', [{}])
        if coords:
            coord = coords[0] if isinstance(coords, list) else coords
            lat = coord.get('StationLatitude')
            lon = coord.get('StationLongitude')
        else:
            lat, lon = None, None

        # 解析天氣要素
        parsed = {
            # 基本資訊
            'station_id': station.get('StationId'),
            'station_name': station.get('StationName'),
            'obs_time': obs_time.get('DateTime'),

            # 位置資訊
            'latitude': lat,
            'longitude': lon,
            'altitude': geo_info.get('StationAltitude'),
            'county': geo_info.get('CountyName'),
            'town': geo_info.get('TownName'),

            # 天氣觀測
            'temperature': self._safe_float(weather.get('AirTemperature')),
            'humidity': self._safe_float(weather.get('RelativeHumidity')),
            'pressure': self._safe_float(weather.get('AirPressure')),
            'wind_speed': self._safe_float(weather.get('WindSpeed')),
            'wind_direction': self._safe_float(weather.get('WindDirection')),
            'gust_speed': self._safe_float(weather.get('GustInfo', {}).get('PeakGustSpeed')),

            # 降雨資料
            'precipitation_now': self._safe_float(weather.get('Now', {}).get('Precipitation')),
            'precipitation_1hr': self._safe_float(weather.get('Past1hr', {}).get('Precipitation')),
            'precipitation_3hr': self._safe_float(weather.get('Past3hr', {}).get('Precipitation')),
            'precipitation_6hr': self._safe_float(weather.get('Past6hr', {}).get('Precipitation')),
            'precipitation_12hr': self._safe_float(weather.get('Past12hr', {}).get('Precipitation')),
            'precipitation_24hr': self._safe_float(weather.get('Past24hr', {}).get('Precipitation')),

            # 溫度極值
            'temp_max': self._safe_float(weather.get('DailyExtreme', {}).get('DailyHigh', {}).get('TemperatureInfo', {}).get('AirTemperature')),
            'temp_min': self._safe_float(weather.get('DailyExtreme', {}).get('DailyLow', {}).get('TemperatureInfo', {}).get('AirTemperature')),

            # 其他
            'visibility': self._safe_float(weather.get('VisibilityDescription')),
            'sunshine_duration': self._safe_float(weather.get('SunshineDuration')),
            'uv_index': self._safe_float(weather.get('UVIndex')),

            # 中繼資料
            '_fetch_time': fetch_time.isoformat(),
        }

        return parsed

    def _safe_float(self, value) -> float | None:
        """安全轉換為浮點數，處理無效值 (-99)"""
        if value is None:
            return None
        try:
            val = float(value)
            # CWA 使用 -99 或 -998 表示無效值
            if val <= -99:
                return None
            return val
        except (ValueError, TypeError):
            return None

    def collect(self) -> dict:
        """收集氣象觀測資料"""
        fetch_time = datetime.now()

        print(f"   正在從 CWA API 取得資料...")

        # 取得原始資料
        raw_data = self._fetch_observations()
        stations_data = raw_data.get('records', {}).get('Station', [])

        # 解析每個測站
        parsed_stations = []
        county_stats = {}

        for station in stations_data:
            parsed = self._parse_station(station, fetch_time)

            # 跳過無效座標
            if parsed['latitude'] is None or parsed['longitude'] is None:
                continue

            parsed_stations.append(parsed)

            # 按縣市統計
            county = parsed['county'] or '未知'
            if county not in county_stats:
                county_stats[county] = {
                    'count': 0,
                    'temps': [],
                    'humids': [],
                }
            county_stats[county]['count'] += 1
            if parsed['temperature'] is not None:
                county_stats[county]['temps'].append(parsed['temperature'])
            if parsed['humidity'] is not None:
                county_stats[county]['humids'].append(parsed['humidity'])

        # 計算統計
        total_stations = len(parsed_stations)
        temps = [s['temperature'] for s in parsed_stations if s['temperature'] is not None]
        humids = [s['humidity'] for s in parsed_stations if s['humidity'] is not None]

        avg_temp = round(sum(temps) / len(temps), 1) if temps else None
        avg_humid = round(sum(humids) / len(humids), 1) if humids else None
        min_temp = round(min(temps), 1) if temps else None
        max_temp = round(max(temps), 1) if temps else None

        # 輸出統計
        print(f"   ✓ 取得 {total_stations} 個測站資料")
        if avg_temp is not None:
            print(f"   📊 平均溫度: {avg_temp}°C (範圍: {min_temp}~{max_temp}°C)")
        if avg_humid is not None:
            print(f"   📊 平均濕度: {avg_humid}%")

        # 輸出縣市分布 (前 5 個)
        sorted_counties = sorted(county_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:5]
        print(f"   📍 測站分布: " + ", ".join([f"{c}: {s['count']}站" for c, s in sorted_counties]))

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_stations': total_stations,
            'avg_temperature': avg_temp,
            'avg_humidity': avg_humid,
            'temp_range': {'min': min_temp, 'max': max_temp},
            'by_county': {
                county: {
                    'count': stats['count'],
                    'avg_temp': round(sum(stats['temps']) / len(stats['temps']), 1) if stats['temps'] else None,
                    'avg_humid': round(sum(stats['humids']) / len(stats['humids']), 1) if stats['humids'] else None,
                }
                for county, stats in county_stats.items()
            },
            # 執行收據：上游觀測時間（weather_observations 的時間欄是收集時間，看不出上游停更）
            '_upstream_max_at': max_upstream_iso(s.get('obs_time') for s in parsed_stations),
            'data': parsed_stations
        }
