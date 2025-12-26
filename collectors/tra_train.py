"""
台鐵即時列車位置收集器

從 TDX API 取得台鐵列車即時動態資料 (TrainLiveBoard)。
"""

from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from .base import BaseCollector


class TRATrainCollector(BaseCollector):
    """台鐵即時列車位置收集器"""

    name = "tra_train"
    interval_minutes = config.TRA_TRAIN_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self.auth = TDXAuth(session=self._session)

    def _fetch_train_live_board(self) -> list:
        """取得即時列車動態"""
        url = f"{config.TDX_API_BASE}/v3/Rail/TRA/TrainLiveBoard"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        return data.get('TrainLiveBoards', [])

    def collect(self) -> dict:
        """收集即時列車位置資料"""
        fetch_time = datetime.now()

        try:
            trains = self._fetch_train_live_board()

            # 統計各車種數量
            train_type_stats = {}
            for train in trains:
                train_type = train.get('TrainTypeName', {}).get('Zh_tw', '未知')
                train_type_stats[train_type] = train_type_stats.get(train_type, 0) + 1

            # 加入抓取時間標記
            for train in trains:
                train['_fetch_time'] = fetch_time.isoformat()

            total_trains = len(trains)

            print(f"   ✓ 即時列車: {total_trains} 班次")
            for train_type, count in sorted(train_type_stats.items(), key=lambda x: -x[1]):
                print(f"     - {train_type}: {count}")

            return {
                'fetch_time': fetch_time.isoformat(),
                'train_count': total_trains,
                'by_train_type': train_type_stats,
                'data': trains
            }

        except requests.exceptions.HTTPError as e:
            print(f"   ✗ HTTP 錯誤: {e.response.status_code}")
            raise

        except Exception as e:
            print(f"   ✗ 錯誤: {e}")
            raise
