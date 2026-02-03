"""
航港局 AIS 即時船位收集器

從航港局「臺灣海域船舶即時資訊系統」取得全台灣海域的 AIS 船位資料。
資料來源：https://mpbais.motcmpb.gov.tw/aismpb/
API 端點：geojsonais.ashx

此 API 涵蓋所有船型（漁船、貨船、油輪、客輪等），
資料量遠大於 TDX 的國內航線船位。
"""

from datetime import datetime

import requests

import config
from .base import BaseCollector


class ShipAISCollector(BaseCollector):
    """航港局 AIS 即時船位收集器"""

    name = "ship_ais"
    interval_minutes = config.SHIP_AIS_INTERVAL

    # 航港局 AIS API 端點
    AIS_BASE_URL = "https://mpbais.motcmpb.gov.tw/aismpb/tools"
    AIS_REALTIME_ENDPOINT = f"{AIS_BASE_URL}/geojsonais.ashx"

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        # 設定 User-Agent 避免被擋
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Referer': 'https://mpbais.motcmpb.gov.tw/aismpb/',
        })

    def _fetch_ais_realtime(self) -> dict:
        """取得即時 AIS 資料"""
        response = self._session.get(
            self.AIS_REALTIME_ENDPOINT,
            timeout=config.REQUEST_TIMEOUT
        )
        response.raise_for_status()

        return response.json()

    def collect(self) -> dict:
        """收集即時 AIS 船位資料"""
        fetch_time = datetime.now()

        try:
            geojson = self._fetch_ais_realtime()

            features = geojson.get('features', [])

            # 統計
            vessel_type_stats = {}
            nav_status_stats = {}
            flag_stats = {}  # 船旗國統計
            taiwan_count = 0

            processed_ships = []

            for feature in features:
                props = feature.get('properties', {})
                geom = feature.get('geometry', {})
                coords = geom.get('coordinates', [None, None])

                # 船舶類型統計
                vessel_type = props.get('Ship_and_Cargo_Type', 0)
                vessel_type_name = self._get_vessel_type_name(vessel_type)
                vessel_type_stats[vessel_type_name] = vessel_type_stats.get(vessel_type_name, 0) + 1

                # 航行狀態統計
                nav_status = props.get('Navigational_Status', -1)
                nav_status_name = self._get_nav_status_name(nav_status)
                nav_status_stats[nav_status_name] = nav_status_stats.get(nav_status_name, 0) + 1

                # 船旗國統計（從 MMSI 前三碼判斷）
                mmsi = str(props.get('MMSI', ''))
                if len(mmsi) >= 3:
                    mid = mmsi[:3]
                    country = self._get_country_from_mid(mid)
                    flag_stats[country] = flag_stats.get(country, 0) + 1
                    if mid in ('416',):  # 台灣 MID
                        taiwan_count += 1

                # 整理為統一格式
                ship = {
                    'mmsi': props.get('MMSI'),
                    'imo': props.get('IMO_Number'),
                    'ship_name': props.get('ShipName'),
                    'call_sign': props.get('Call_Sign'),
                    'vessel_type': vessel_type,
                    'vessel_type_name': vessel_type_name,
                    'nav_status': nav_status,
                    'nav_status_name': nav_status_name,
                    'lon': coords[0],
                    'lat': coords[1],
                    'sog': props.get('SOG'),  # 對地速度 (節)
                    'cog': props.get('COG'),  # 對地航向 (度)
                    'heading': props.get('True_Heading'),  # 真航向
                    'rot': props.get('Rate_of_Turn'),  # 轉向率
                    'length': props.get('Overall_Length'),
                    'width': props.get('Breadth'),
                    'draught': props.get('Draught'),
                    'destination': props.get('Destination'),
                    'eta': props.get('ETA'),
                    'record_time': props.get('Record_Time'),
                    '_fetch_time': fetch_time.isoformat(),
                }
                processed_ships.append(ship)

            total_ships = len(processed_ships)

            print(f"   ✓ AIS 船位: {total_ships} 艘船舶")
            print(f"     - 台灣籍: {taiwan_count}")

            # 顯示前 5 大船型
            sorted_types = sorted(vessel_type_stats.items(), key=lambda x: -x[1])[:5]
            for vessel_type, count in sorted_types:
                print(f"     - {vessel_type}: {count}")

            return {
                'fetch_time': fetch_time.isoformat(),
                'ship_count': total_ships,
                'taiwan_ship_count': taiwan_count,
                'by_vessel_type': vessel_type_stats,
                'by_nav_status': nav_status_stats,
                'by_flag': flag_stats,
                'data': processed_ships
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
        # AIS 船舶類型代碼
        vessel_types = {
            0: '未指定',
            20: '地效翼船',
            30: '漁船',
            31: '拖船',
            32: '拖船(大型)',
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
            70: '貨船',
            80: '油輪',
            90: '其他',
        }
        # 處理子類別 (61-64, 71-74, 81-84)
        if 60 <= vessel_type <= 69:
            return '客輪'
        elif 70 <= vessel_type <= 79:
            return '貨船'
        elif 80 <= vessel_type <= 89:
            return '油輪'
        return vessel_types.get(vessel_type, f'其他({vessel_type})')

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
            11: '拖曳中',
            12: '推頂中',
            14: 'AIS-SART',
            15: '未定義',
        }
        return nav_statuses.get(nav_status, f'未知({nav_status})')

    @staticmethod
    def _get_country_from_mid(mid: str) -> str:
        """從 MID (Maritime Identification Digits) 取得船旗國"""
        # 常見的 MID 對照表
        mid_countries = {
            '416': '台灣',
            '412': '中國',
            '413': '中國',
            '414': '中國',
            '431': '日本',
            '432': '日本',
            '440': '韓國',
            '441': '韓國',
            '351': '巴拿馬',
            '352': '巴拿馬',
            '353': '巴拿馬',
            '354': '巴拿馬',
            '355': '巴拿馬',
            '356': '巴拿馬',
            '357': '巴拿馬',
            '370': '巴拿馬',
            '371': '巴拿馬',
            '372': '巴拿馬',
            '373': '巴拿馬',
            '374': '巴拿馬',
            '636': '賴比瑞亞',
            '637': '賴比瑞亞',
            '477': '香港',
            '563': '新加坡',
            '564': '新加坡',
            '565': '新加坡',
            '566': '新加坡',
            '533': '巴哈馬',
            '538': '馬紹爾群島',
            '244': '荷蘭',
            '245': '荷蘭',
            '246': '荷蘭',
            '518': '紐西蘭',
            '503': '澳大利亞',
            '525': '印尼',
            '548': '菲律賓',
            '574': '越南',
        }
        return mid_countries.get(mid, f'其他({mid})')
