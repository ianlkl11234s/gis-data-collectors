"""
台灣好行公車即時位置收集器（TouristShuttle / TaiwanTrip）

從 TDX API 取得「台灣好行」觀光公車 A1 即時定頻資料。
單一 endpoint 涵蓋全國 ~70 條路線，不需逐城市呼叫。
"""

from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


class TouristShuttleCollector(BaseCollector):
    """台灣好行即時位置收集器（A1 全國單一端點）"""

    name = "tourist_shuttle"
    interval_minutes = config.TOURIST_SHUTTLE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_realtime(self) -> list:
        url = f"{config.TDX_API_BASE}/v2/Tourism/Bus/RealTimeByFrequency/TaiwanTrip"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json() if isinstance(response.json(), list) else []

    @staticmethod
    def _has_gps(buses: list) -> list:
        """保留有經緯度的車輛（DutyStatus/BusStatus 各業者填寫不一，改以 GPS 為準）"""
        result = []
        for b in buses:
            pos = b.get('BusPosition') or {}
            if isinstance(pos, dict) and pos.get('PositionLat') and pos.get('PositionLon'):
                result.append(b)
        return result

    def collect(self) -> dict:
        fetch_time = datetime.now()

        raw = self._fetch_realtime()
        active = self._has_gps(raw)

        # 按台灣好行路線統計
        line_stats = {}
        for bus in active:
            line = (bus.get('TaiwanTripName') or {}).get('Zh_tw') or 'unknown'
            line_stats[line] = line_stats.get(line, 0) + 1

        total = len(active)
        print(f"   ✓ 台灣好行: {total}/{len(raw)} 台有 GPS 回報，{len(line_stats)} 條路線在跑")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_active': total,
            'total_raw': len(raw),
            'by_line': line_stats,
            'data': active,
        }
