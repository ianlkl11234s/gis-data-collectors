"""
溫度網格資料收集器

從中央氣象署 File API 取得全台溫度網格資料 (O-A0038-003)。

資料特性:
    - 更新頻率: 每小時
    - 解析度: 0.03 度 (約 3.3 公里)
    - 覆蓋範圍: 全台灣
    - 資料格式: CSV 格式的網格數據

API 資訊:
    - 端點: https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0038-003
    - 認證: CWA API Key
    - 回傳: JSON (包含 CSV 格式的網格數據)
"""

from datetime import datetime
from typing import Optional

import requests

import config
from .base import BaseCollector


class TemperatureGridCollector(BaseCollector):
    """溫度網格收集器"""

    name = "temperature"
    interval_minutes = config.TEMPERATURE_INTERVAL

    def __init__(self):
        """初始化溫度網格收集器"""
        super().__init__()
        self.api_key = config.CWA_API_KEY
        self._session = requests.Session()

        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")

    def _fetch_grid_data(self) -> dict:
        """從 CWA File API 取得網格資料

        Returns:
            原始 JSON 資料

        Raises:
            requests.HTTPError: API 請求失敗
        """
        url = f"{config.CWA_FILE_API_BASE}/{config.TEMPERATURE_DATASET}"
        params = {
            'Authorization': self.api_key,
            'format': 'JSON'
        }

        response = self._session.get(
            url,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
            allow_redirects=True  # API 會 302 重導向到 S3
        )
        response.raise_for_status()
        return response.json()

    def _parse_grid(self, data: dict) -> tuple:
        """解析網格資料

        Args:
            data: 原始 JSON 資料

        Returns:
            (grid_data, geo_info, observation_time)
            - grid_data: 二維陣列，每個元素為溫度值或 None
            - geo_info: 地理範圍資訊
            - observation_time: 觀測時間字串
        """
        cwa = data['cwaopendata']
        dataset = cwa['dataset']

        geo_info = dataset['GeoInfo']
        datetime_str = dataset['DataTime']['DateTime']
        content = dataset['Resource']['Content']

        # 解析 CSV 格式的網格數據
        rows = content.strip().split('\n')
        grid_data = []

        for row in rows:
            values = []
            for v in row.split(','):
                try:
                    val = float(v.strip())
                    # CWA 使用 -999 或類似負值表示無效資料
                    values.append(val if val > -900 else None)
                except ValueError:
                    values.append(None)
            grid_data.append(values)

        return grid_data, geo_info, datetime_str

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
            val = float(value)
            return val if val > -900 else None
        except (ValueError, TypeError):
            return None

    def _calculate_stats(self, grid: list) -> dict:
        """計算統計值

        Args:
            grid: 二維網格資料

        Returns:
            統計資訊字典
        """
        valid_temps = [v for row in grid for v in row if v is not None]

        if not valid_temps:
            return {'valid_points': 0}

        return {
            'valid_points': len(valid_temps),
            'min_temp': round(min(valid_temps), 1),
            'max_temp': round(max(valid_temps), 1),
            'avg_temp': round(sum(valid_temps) / len(valid_temps), 1),
            'std_temp': round(self._std(valid_temps), 2)
        }

    def _std(self, values: list) -> float:
        """計算標準差

        Args:
            values: 數值列表

        Returns:
            標準差
        """
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5

    def collect(self) -> dict:
        """收集溫度網格資料

        Returns:
            收集結果，包含:
            - fetch_time: 擷取時間
            - observation_time: 觀測時間
            - geo_info: 地理範圍資訊
            - grid_size: 網格大小
            - valid_points: 有效格點數
            - min_temp, max_temp, avg_temp: 溫度統計
            - data: 二維網格資料
        """
        fetch_time = datetime.now()

        print("   正在從 CWA File API 取得溫度網格...")

        # 取得原始資料
        raw_data = self._fetch_grid_data()

        # 解析網格
        grid, geo_info, obs_time = self._parse_grid(raw_data)

        # 計算統計
        stats = self._calculate_stats(grid)

        # 網格大小
        rows = len(grid)
        cols = len(grid[0]) if grid else 0

        print(f"   ✓ 網格大小: {rows} x {cols}")
        print(f"   📊 有效格點: {stats.get('valid_points', 0):,}")

        if stats.get('avg_temp'):
            print(f"   🌡️ 溫度範圍: {stats['min_temp']}°C ~ {stats['max_temp']}°C")
            print(f"   🌡️ 平均溫度: {stats['avg_temp']}°C (標準差: {stats.get('std_temp', 0)})")

        return {
            'fetch_time': fetch_time.isoformat(),
            'observation_time': obs_time,
            'geo_info': {
                'bottom_left_lon': self._safe_float(geo_info.get('BottomLeftLongitude')),
                'bottom_left_lat': self._safe_float(geo_info.get('BottomLeftLatitude')),
                'top_right_lon': self._safe_float(geo_info.get('TopRightLongitude')),
                'top_right_lat': self._safe_float(geo_info.get('TopRightLatitude')),
                'resolution_deg': 0.03,
                'resolution_km': 3.3,
            },
            'grid_size': {
                'rows': rows,
                'cols': cols
            },
            **stats,
            'data': grid  # 二維陣列
        }
