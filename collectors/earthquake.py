"""
地震報告資料收集器

從中央氣象署 (CWA) API 取得有感地震報告，每日收集一次。
API 端點:
    E-A0015-001 (顯著有感地震報告)
    E-A0016-001 (小區域有感地震報告)
"""

from datetime import datetime

import requests

import config
from .base import BaseCollector


class EarthquakeCollector(BaseCollector):
    """地震報告收集器（每日打包一次）"""

    name = "earthquake"
    interval_minutes = config.EARTHQUAKE_INTERVAL

    # CWA API 端點
    ENDPOINTS = {
        'significant': 'E-A0015-001',  # 顯著有感地震報告
        'local': 'E-A0016-001',        # 小區域有感地震報告
    }

    def __init__(self):
        super().__init__()
        self.api_key = config.CWA_API_KEY
        self._session = requests.Session()

        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")

    def _fetch_reports(self, endpoint_id: str, limit: int = 30) -> list:
        """從 CWA API 取得地震報告"""
        url = f"{config.CWA_API_BASE}/v1/rest/datastore/{endpoint_id}"

        params = {
            'Authorization': self.api_key,
            'format': 'JSON',
            'limit': limit,
        }

        response = self._session.get(
            url,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        data = response.json()

        if data.get('success') != 'true':
            raise ValueError(f"API 回傳失敗: {data}")

        return data.get('records', {}).get('Earthquake', [])

    def _parse_earthquake(self, eq: dict) -> dict:
        """解析單筆地震報告"""
        info = eq.get('EarthquakeInfo', {})
        epicenter = info.get('Epicenter', {})
        magnitude = info.get('EarthquakeMagnitude', {})
        intensity = eq.get('Intensity', {})
        shaking_areas = intensity.get('ShakingArea', [])

        # 解析各測站震度
        station_details = []
        for area in shaking_areas:
            for station in area.get('EqStation', []):
                station_details.append({
                    'area_name': area.get('AreaName', ''),
                    'area_intensity': area.get('AreaIntensity', ''),
                    'county': area.get('CountyName', ''),
                    'station_name': station.get('StationName', ''),
                    'station_id': station.get('StationID', ''),
                    'intensity': station.get('SeismicIntensity', ''),
                    'latitude': station.get('StationLatitude'),
                    'longitude': station.get('StationLongitude'),
                })

        return {
            'earthquake_no': eq.get('EarthquakeNo'),
            'report_type': eq.get('ReportType', ''),
            'origin_time': info.get('OriginTime', ''),
            'focal_depth_km': info.get('FocalDepth'),
            'epicenter_location': epicenter.get('Location', ''),
            'epicenter_latitude': epicenter.get('EpicenterLatitude'),
            'epicenter_longitude': epicenter.get('EpicenterLongitude'),
            'magnitude_type': magnitude.get('MagnitudeType', ''),
            'magnitude_value': magnitude.get('MagnitudeValue'),
            'max_intensity': shaking_areas[0].get('AreaIntensity', '') if shaking_areas else '',
            'station_count': len(station_details),
            'stations': station_details,
            'report_content': eq.get('ReportContent', ''),
            'report_image_uri': eq.get('ReportImageURI', ''),
        }

    def collect(self) -> dict:
        """收集地震報告（顯著有感 + 小區域有感）"""
        fetch_time = datetime.now()

        print(f"   正在從 CWA API 取得地震報告...")

        all_reports = []

        for eq_type, endpoint_id in self.ENDPOINTS.items():
            try:
                reports = self._fetch_reports(endpoint_id, limit=30)
                print(f"   [{eq_type}] 取得 {len(reports)} 筆")
                for eq in reports:
                    parsed = self._parse_earthquake(eq)
                    parsed['source_type'] = eq_type
                    all_reports.append(parsed)
            except Exception as e:
                print(f"   [{eq_type}] 取得失敗: {e}")

        # 用 earthquake_no 去重
        seen = set()
        unique_reports = []
        for report in all_reports:
            eq_no = report['earthquake_no']
            if eq_no and eq_no not in seen:
                seen.add(eq_no)
                unique_reports.append(report)

        # 按時間排序（新到舊）
        unique_reports.sort(key=lambda x: x['origin_time'], reverse=True)

        # 篩選今日的報告
        today_str = fetch_time.strftime('%Y-%m-%d')
        today_reports = [r for r in unique_reports if r['origin_time'].startswith(today_str)]

        # 統計
        total = len(unique_reports)
        today_count = len(today_reports)
        magnitudes = [r['magnitude_value'] for r in unique_reports if r['magnitude_value']]
        max_mag = max(magnitudes) if magnitudes else None

        print(f"   合計: {total} 筆不重複報告 (今日: {today_count} 筆)")
        if max_mag:
            print(f"   最大規模: {max_mag}")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_reports': total,
            'today_reports': today_count,
            'max_magnitude': max_mag,
            'magnitude_range': {
                'min': min(magnitudes) if magnitudes else None,
                'max': max_mag,
            },
            'data': unique_reports,
        }
