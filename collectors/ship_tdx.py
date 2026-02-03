"""
TDX 國內航線船位收集器

從 TDX API 取得國內航線的即時船舶位置資料。
資料來源：TDX /v3/Ship/LivePosition
"""

from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from .base import BaseCollector


class ShipTDXCollector(BaseCollector):
    """TDX 國內航線船位收集器"""

    name = "ship_tdx"
    interval_minutes = config.SHIP_TDX_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self.auth = TDXAuth(session=self._session)

    def _fetch_live_position(self) -> list:
        """取得即時船位資料"""
        url = f"{config.TDX_API_BASE}/v3/Ship/LivePosition"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        # TDX Ship API 回傳格式可能是陣列或物件
        if isinstance(data, list):
            return data
        return data.get('ShipLivePositions', data.get('Data', []))

    def _fetch_routes(self) -> list:
        """取得航線資料（用於補充資訊）"""
        url = f"{config.TDX_API_BASE}/v3/Ship/Route"
        headers = self.auth.get_auth_header()

        response = self._session.get(
            url,
            headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        data = response.json()
        if isinstance(data, list):
            return data
        return data.get('Routes', data.get('Data', []))

    def collect(self) -> dict:
        """收集即時船位資料"""
        fetch_time = datetime.now()

        try:
            ships = self._fetch_live_position()

            # 統計船舶類型
            vessel_type_stats = {}
            nav_status_stats = {}

            for ship in ships:
                # 船舶類型統計
                vessel_type = ship.get('VesselType', 0)
                vessel_type_name = self._get_vessel_type_name(vessel_type)
                vessel_type_stats[vessel_type_name] = vessel_type_stats.get(vessel_type_name, 0) + 1

                # 航行狀態統計
                nav_status = ship.get('NAVSTAT', -1)
                nav_status_name = self._get_nav_status_name(nav_status)
                nav_status_stats[nav_status_name] = nav_status_stats.get(nav_status_name, 0) + 1

                # 加入抓取時間標記
                ship['_fetch_time'] = fetch_time.isoformat()

                # 標準化座標欄位
                if 'VesselPosition' in ship:
                    pos = ship['VesselPosition']
                    ship['_lat'] = pos.get('PositionLat')
                    ship['_lon'] = pos.get('PositionLon')

            total_ships = len(ships)

            print(f"   ✓ 即時船位: {total_ships} 艘船舶")
            for vessel_type, count in sorted(vessel_type_stats.items(), key=lambda x: -x[1]):
                print(f"     - {vessel_type}: {count}")

            return {
                'fetch_time': fetch_time.isoformat(),
                'ship_count': total_ships,
                'by_vessel_type': vessel_type_stats,
                'by_nav_status': nav_status_stats,
                'data': ships
            }

        except requests.exceptions.HTTPError as e:
            print(f"   ✗ HTTP 錯誤: {e.response.status_code}")
            raise

        except Exception as e:
            print(f"   ✗ 錯誤: {e}")
            raise

    @staticmethod
    def _get_vessel_type_name(vessel_type: int) -> str:
        """將船舶類型代碼轉換為名稱"""
        vessel_types = {
            0: '未指定',
            20: '地效翼船',
            21: '地效翼船(A類)',
            22: '地效翼船(B類)',
            23: '地效翼船(C類)',
            24: '地效翼船(D類)',
            30: '漁船',
            31: '拖船',
            32: '拖船(長度>200m)',
            33: '疏浚船',
            34: '潛水作業船',
            35: '軍艦',
            36: '帆船',
            37: '遊艇',
            40: '高速船',
            50: '引水船',
            51: '搜救船',
            52: '拖船',
            53: '港口小艇',
            54: '防污船',
            55: '執法船',
            60: '客輪',
            61: '客輪(A類)',
            62: '客輪(B類)',
            63: '客輪(C類)',
            64: '客輪(D類)',
            70: '貨船',
            71: '貨船(A類)',
            72: '貨船(B類)',
            73: '貨船(C類)',
            74: '貨船(D類)',
            80: '油輪',
            81: '油輪(A類)',
            82: '油輪(B類)',
            83: '油輪(C類)',
            84: '油輪(D類)',
            90: '其他',
        }
        return vessel_types.get(vessel_type, f'未知({vessel_type})')

    @staticmethod
    def _get_nav_status_name(nav_status: int) -> str:
        """將航行狀態代碼轉換為名稱"""
        nav_statuses = {
            0: '航行中',
            1: '錨泊中',
            2: '失去控制',
            3: '操縱受限',
            4: '受吃水限制',
            5: '繫泊中',
            6: '擱淺',
            7: '從事捕魚',
            8: '帆船航行',
            9: '保留',
            10: '保留',
            11: '拖曳中',
            12: '推頂中',
            13: '保留',
            14: 'AIS-SART',
            15: '未定義',
        }
        return nav_statuses.get(nav_status, f'未知({nav_status})')
