"""
台鐵靜態資料收集器

從 TDX API 取得台鐵靜態資料：車站、路線、站序、車種、當日時刻表。
"""

import time
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


class TRAStaticCollector(BaseCollector):
    """台鐵靜態資料收集器（每日更新）"""

    name = "tra_static"
    interval_minutes = config.TRA_STATIC_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_api(self, endpoint: str, description: str) -> list:
        """通用 API 呼叫"""
        url = f"{config.TDX_API_BASE}{endpoint}"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()

        # 處理不同 API 的回傳格式
        if isinstance(data, list):
            result = data
        elif 'Stations' in data:
            result = data['Stations']
        elif 'Lines' in data:
            result = data['Lines']
        elif 'StationOfLines' in data:
            result = data['StationOfLines']
        elif 'TrainTypes' in data:
            result = data['TrainTypes']
        elif 'TrainTimetables' in data:
            result = data['TrainTimetables']
        elif 'Shapes' in data:
            result = data['Shapes']
        else:
            result = data

        print(f"   ✓ {description}: {len(result) if isinstance(result, list) else 'OK'}")
        return result

    def collect(self) -> dict:
        """收集所有靜態資料"""
        fetch_time = datetime.now()
        results = {}

        try:
            # 1. 車站資料
            stations = self._fetch_api('/v2/Rail/TRA/Station', '車站資料')
            results['stations'] = {
                'count': len(stations),
                'data': stations
            }
            time.sleep(config.REQUEST_INTERVAL)

            # 2. 路線資料
            lines = self._fetch_api('/v2/Rail/TRA/Line', '路線資料')
            results['lines'] = {
                'count': len(lines),
                'data': lines
            }
            time.sleep(config.REQUEST_INTERVAL)

            # 3. 路線站序
            station_of_lines = self._fetch_api('/v2/Rail/TRA/StationOfLine', '路線站序')
            results['station_of_lines'] = {
                'count': len(station_of_lines),
                'data': station_of_lines
            }
            time.sleep(config.REQUEST_INTERVAL)

            # 4. 車種資料
            train_types = self._fetch_api('/v2/Rail/TRA/TrainType', '車種資料')
            results['train_types'] = {
                'count': len(train_types),
                'data': train_types
            }
            time.sleep(config.REQUEST_INTERVAL)

            # 5. 當日時刻表
            timetables = self._fetch_api('/v3/Rail/TRA/DailyTrainTimetable/Today', '當日時刻表')
            results['daily_timetable'] = {
                'count': len(timetables),
                'data': timetables
            }
            time.sleep(config.REQUEST_INTERVAL)

            # 6. 軌道幾何（可能沒有資料）
            try:
                shapes = self._fetch_api('/v3/Rail/TRA/Shape', '軌道幾何')
                results['shapes'] = {
                    'count': len(shapes) if isinstance(shapes, list) else 0,
                    'data': shapes
                }
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"   ⚠️ 軌道幾何: API 不存在或無資料")
                    results['shapes'] = {'count': 0, 'data': [], 'error': 'API not available'}
                else:
                    raise

            # 統計摘要
            print(f"\n   📊 靜態資料摘要:")
            print(f"     - 車站: {results['stations']['count']} 站")
            print(f"     - 路線: {results['lines']['count']} 條")
            print(f"     - 車種: {results['train_types']['count']} 種")
            print(f"     - 當日班次: {results['daily_timetable']['count']} 班")

            return {
                'fetch_time': fetch_time.isoformat(),
                'data': results
            }

        except requests.exceptions.HTTPError as e:
            print(f"   ✗ HTTP 錯誤: {e.response.status_code}")
            raise

        except Exception as e:
            print(f"   ✗ 錯誤: {e}")
            raise
