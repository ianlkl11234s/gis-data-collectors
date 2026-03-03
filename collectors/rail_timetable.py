"""
台鐵 + 高鐵每日時刻表歸檔收集器

每天收集一次 TDX 的 DailyTimetable，保存完整的當日時刻表。
TDX 每日時刻表含停駛/加班車標記，與定期時刻表有 10-18% 差異，
且歷史資料僅保留約 90 天，因此需要每日歸檔。

API endpoints:
- 台鐵: /v3/Rail/TRA/DailyTrainTimetable/Today
- 高鐵: /v2/Rail/THSR/DailyTimetable/Today

注意：tra_static 收集器也會抓台鐵當日時刻表，但它是跟所有靜態資料
混在一起儲存。本收集器專門做時刻表歸檔，輸出結構更乾淨。
"""

import time
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from .base import BaseCollector


class RailTimetableCollector(BaseCollector):
    """台鐵 + 高鐵每日時刻表歸檔收集器"""

    name = "rail_timetable"
    interval_minutes = config.RAIL_TIMETABLE_INTERVAL

    # TDX API endpoints
    TRA_DAILY_URL = "/v3/Rail/TRA/DailyTrainTimetable/Today"
    THSR_DAILY_URL = "/v2/Rail/THSR/DailyTimetable/Today"

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self.auth = TDXAuth(session=self._session)

    def _fetch_api(self, endpoint: str, description: str) -> list:
        """呼叫 TDX API 取得資料"""
        url = f"{config.TDX_API_BASE}{endpoint}"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=60,  # 時刻表資料量大，給長一點
        )
        response.raise_for_status()

        data = response.json()

        # TRA v3 回傳 {"TrainTimetables": [...]}
        if isinstance(data, dict) and 'TrainTimetables' in data:
            result = data['TrainTimetables']
        # THSR v2 回傳 [...]
        elif isinstance(data, list):
            result = data
        else:
            result = data

        count = len(result) if isinstance(result, list) else '?'
        print(f"   ✓ {description}: {count} 班")
        return result

    def collect(self) -> dict:
        """收集台鐵 + 高鐵當日時刻表"""
        fetch_time = datetime.now()
        today = fetch_time.strftime("%Y-%m-%d")

        results = {}

        # 1. 台鐵每日時刻表
        print(f"   📅 收集 {today} 時刻表...")
        tra_timetable = self._fetch_api(self.TRA_DAILY_URL, '台鐵每日時刻表')
        results['tra'] = {
            'train_count': len(tra_timetable),
            'data': tra_timetable,
        }

        time.sleep(config.REQUEST_INTERVAL)

        # 2. 高鐵每日時刻表
        thsr_timetable = self._fetch_api(self.THSR_DAILY_URL, '高鐵每日時刻表')
        results['thsr'] = {
            'train_count': len(thsr_timetable),
            'data': thsr_timetable,
        }

        print(f"   📊 {today}: 台鐵 {results['tra']['train_count']} 班, "
              f"高鐵 {results['thsr']['train_count']} 班")

        return {
            'fetch_time': fetch_time.isoformat(),
            'date': today,
            'tra_train_count': results['tra']['train_count'],
            'thsr_train_count': results['thsr']['train_count'],
            'data': results,
        }
