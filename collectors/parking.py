"""
路邊停車即時可用性收集器

從 TDX API 取得台北、新北、台中路邊停車即時資料。

資料特性:
    - 更新頻率: 每 15 分鐘
    - 支援城市: 台北市、新北市、台中市
    - 注意: 高雄市不在 TDX 路邊停車 API 支援範圍內

API 資訊:
    - 端點: /v1/Parking/OnStreet/ParkingSegmentAvailability/City/{City}
    - 認證: TDX OAuth Token
    - 回傳: JSON

重要欄位說明:
    - TotalSpaces: 總車位數
    - AvailableSpaces: 剩餘車位數（-1 表示無資料）
    - Occupancy: 使用率（0.5 = 50%）
    - FullStatus: 滿載狀態（0:有空位, 1:已滿, -1:無資料）
    - DataCollectTime: 資料收集時間
"""

import time
from datetime import datetime
from typing import Optional

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


# 城市代碼對照
CITY_NAMES = {
    'Taipei': '臺北市',
    'NewTaipei': '新北市',
    'Taichung': '臺中市',
}


class ParkingCollector(BaseCollector):
    """路邊停車即時可用性收集器"""

    name = "parking"
    interval_minutes = config.PARKING_INTERVAL

    def __init__(self, cities: list = None):
        """初始化路邊停車收集器

        Args:
            cities: 要收集的城市列表，None 表示使用設定檔預設值
        """
        super().__init__()
        self.cities = cities or config.PARKING_CITIES
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_city(self, city: str) -> dict:
        """取得單一城市的路邊停車資料

        Args:
            city: 城市代碼 (Taipei, NewTaipei, Taichung)

        Returns:
            API 回傳的 JSON 資料

        Raises:
            requests.HTTPError: API 請求失敗
        """
        url = f"{config.TDX_API_BASE}/v1/Parking/OnStreet/ParkingSegmentAvailability/City/{city}"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()

    def _parse_segment(self, segment: dict, city: str, fetch_time: datetime) -> dict:
        """解析單一路段資料

        Args:
            segment: 原始路段資料
            city: 城市代碼
            fetch_time: 擷取時間

        Returns:
            解析後的路段資料
        """
        total = segment.get('TotalSpaces', 0)
        available = segment.get('AvailableSpaces', -1)

        # 計算使用率
        if total > 0 and available >= 0:
            occupancy = round(1 - (available / total), 3)
        else:
            occupancy = None

        # 解析車位類型資訊
        availabilities = segment.get('Availabilities', [])
        space_types = []
        for avail in availabilities:
            space_types.append({
                'type': avail.get('SpaceType'),
                'total': avail.get('NumberOfSpaces', 0),
                'available': avail.get('AvailableSpaces', -1),
                'occupancy': self._safe_float(avail.get('Occupancy'))
            })

        return {
            'segment_id': segment.get('ParkingSegmentID'),
            'segment_name': segment.get('ParkingSegmentName', {}).get('Zh_tw'),
            'total_spaces': total,
            'available_spaces': available,
            'occupancy': occupancy,
            'full_status': segment.get('FullStatus'),  # 0:有空位, 1:已滿, -1:無資料
            'service_status': segment.get('ServiceStatus'),
            'charge_status': segment.get('ChargeStatus'),
            'space_types': space_types if space_types else None,
            'data_collect_time': segment.get('DataCollectTime'),
            '_city': city,
            '_city_name': CITY_NAMES.get(city, city),
            '_fetch_time': fetch_time.isoformat()
        }

    def _safe_float(self, value) -> Optional[float]:
        """安全轉換為浮點數

        Args:
            value: 要轉換的值

        Returns:
            浮點數或 None
        """
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def collect(self) -> dict:
        """收集所有城市的路邊停車資料

        Returns:
            收集結果，包含:
            - fetch_time: 擷取時間
            - total_segments: 總路段數
            - total_spaces: 總車位數
            - total_available: 總空位數
            - by_city: 各城市統計
            - data: 所有路段資料
        """
        fetch_time = datetime.now()
        all_segments = []
        city_stats = {}

        for city in self.cities:
            city_name = CITY_NAMES.get(city, city)
            try:
                data = self._fetch_city(city)
                segments = data.get('CurbParkingSegmentAvailabilities', [])

                # 解析每個路段
                parsed_segments = [
                    self._parse_segment(seg, city, fetch_time)
                    for seg in segments
                ]
                all_segments.extend(parsed_segments)

                # 統計計算
                total_spaces = sum(s['total_spaces'] for s in parsed_segments)
                available_spaces = sum(
                    s['available_spaces'] for s in parsed_segments
                    if s['available_spaces'] >= 0
                )
                full_segments = sum(
                    1 for s in parsed_segments
                    if s['full_status'] == 1
                )

                # 計算平均使用率 (只計算有效資料)
                valid_segments = [
                    s for s in parsed_segments
                    if s['occupancy'] is not None
                ]
                avg_occupancy = (
                    round(sum(s['occupancy'] for s in valid_segments) / len(valid_segments), 3)
                    if valid_segments else None
                )

                # 緊張路段數 (剩餘車位 < 10%)
                tight_segments = sum(
                    1 for s in parsed_segments
                    if s['total_spaces'] > 0 and s['available_spaces'] >= 0
                    and (s['available_spaces'] / s['total_spaces']) < 0.1
                )

                city_stats[city] = {
                    'name': city_name,
                    'segments': len(parsed_segments),
                    'total_spaces': total_spaces,
                    'available_spaces': available_spaces,
                    'full_segments': full_segments,
                    'tight_segments': tight_segments,
                    'avg_occupancy': avg_occupancy,
                    'update_time': data.get('SrcUpdateTime')
                }

                # 輸出統計
                occ_pct = f"{avg_occupancy * 100:.1f}%" if avg_occupancy else "N/A"
                print(f"   ✓ {city_name}: {len(parsed_segments)} 路段 | "
                      f"車位 {available_spaces:,}/{total_spaces:,} | "
                      f"使用率 {occ_pct}")

                time.sleep(config.REQUEST_INTERVAL)

            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP 錯誤 {e.response.status_code}"
                print(f"   ✗ {city_name}: {error_msg}")
                city_stats[city] = {'name': city_name, 'error': error_msg}

            except Exception as e:
                error_msg = str(e)
                print(f"   ✗ {city_name}: {error_msg}")
                city_stats[city] = {'name': city_name, 'error': error_msg}

        # 總計
        total_segments = len(all_segments)
        total_spaces = sum(
            s.get('total_spaces', 0)
            for s in city_stats.values()
            if 'error' not in s
        )
        total_available = sum(
            s.get('available_spaces', 0)
            for s in city_stats.values()
            if 'error' not in s
        )
        total_full = sum(
            s.get('full_segments', 0)
            for s in city_stats.values()
            if 'error' not in s
        )

        # 計算總體使用率
        if total_spaces > 0:
            overall_occupancy = round(1 - (total_available / total_spaces), 3)
        else:
            overall_occupancy = None

        occ_str = f"{overall_occupancy * 100:.1f}%" if overall_occupancy else "N/A"
        print(f"\n   📊 總計: {total_segments} 路段 | "
              f"空位 {total_available:,}/{total_spaces:,} | "
              f"使用率 {occ_str} | 滿載 {total_full} 路段")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_segments': total_segments,
            'total_spaces': total_spaces,
            'total_available': total_available,
            'total_full_segments': total_full,
            'overall_occupancy': overall_occupancy,
            'by_city': city_stats,
            'data': all_segments
        }
