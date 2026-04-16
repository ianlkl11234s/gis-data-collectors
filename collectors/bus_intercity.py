"""
公路客運即時位置收集器

從 TDX API 取得跨縣市公路客運 / 國道客運即時定頻資料 (RealTimeByFrequency / A1)。
單一 endpoint 涵蓋全台，不需逐城市呼叫。
僅保留執勤中 (DutyStatus=1) 且正常營運 (BusStatus=0) 的車輛。
"""

from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


class BusIntercityCollector(BaseCollector):
    """公路客運 / 國道客運即時位置收集器"""

    name = "bus_intercity"
    interval_minutes = config.BUS_INTERCITY_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_realtime_intercity(self) -> list:
        """取得全台公路客運即時定頻資料 (A1)"""
        url = f"{config.TDX_API_BASE}/v2/Bus/RealTimeByFrequency/InterCity"
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
    def _has_gps(buses: list) -> list:
        """保留有經緯度的車輛。各客運業者 DutyStatus/BusStatus 填寫習慣不一致，改以 GPS 為準。"""
        result = []
        for b in buses:
            pos = b.get('BusPosition') or {}
            if isinstance(pos, dict) and pos.get('PositionLat') and pos.get('PositionLon'):
                result.append(b)
        return result

    def collect(self) -> dict:
        """收集公路客運即時位置資料"""
        fetch_time = datetime.now()

        raw = self._fetch_realtime_intercity()
        active = self._has_gps(raw)

        # city 欄位：優先用 SubAuthorityID（通常為 None），fallback 到 OperatorID（業者代號），再 fallback 到 'InterCity'
        for bus in active:
            operator = bus.get('SubAuthorityID') or bus.get('OperatorID') or 'InterCity'
            bus['_city'] = str(operator)
            bus['_fetch_time'] = fetch_time.isoformat()

        total = len(active)
        print(f"   ✓ 合計: {total}/{len(raw)} 台公路客運有 GPS 回報")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_active': total,
            'total_raw': len(raw),
            'data': active,
        }
