"""
地震報告 + 海嘯資訊收集器（CWA datastore API）

端點：
    E-A0015-001 顯著有感地震報告  → live.earthquake_events + live.earthquake_station_obs
    E-A0016-001 小區域有感地震報告 → 同上
    E-A0014-001 海嘯資訊          → live.tsunami_alerts

interval 15 分鐘（2026-07 前為 1440，事件驅動的地震報告延遲一天才進庫沒有意義）。

⚠️ 去重坑（2026-07 修）：
    E-A0016-001 的 EarthquakeNo 全部是 115000（小區域地震不編號），舊版用
    earthquake_no 當去重鍵 → 所有小區域地震被壓成一筆。現在自然鍵改由
    earthquake_common.make_event_id() 產生：顯著地震沿用純編號（維持與歷史
    資料相容），小區域走 {EarthquakeNo}-{YYYYMMDDHHMMSS} 複合鍵。

完整地震目錄（E-A0073-001，含無感）已搬到獨立的 earthquake_catalog collector，
本檔不再抓——它半年才更新一批，沒必要跟 15 分鐘的即時報告綁在一起。
"""

from datetime import datetime

import requests

import config
from .base import BaseCollector
from .earthquake_common import (
    as_list,
    intensity_to_value,
    make_event_id,
    safe_float,
    safe_int,
)


class EarthquakeCollector(BaseCollector):
    """地震報告 + 逐站觀測 + 海嘯資訊收集器"""

    name = "earthquake"
    interval_minutes = config.EARTHQUAKE_INTERVAL

    # CWA datastore API 端點（有感地震報告）
    ENDPOINTS = {
        'significant': 'E-A0015-001',  # 顯著有感地震報告
        'local': 'E-A0016-001',        # 小區域有感地震報告
    }

    # 海嘯資訊（同 datastore API；平時回的是歷史報告清單，不是空的）
    TSUNAMI_ENDPOINT = 'E-A0014-001'

    def __init__(self):
        super().__init__()
        self.api_key = config.CWA_API_KEY
        self._session = requests.Session()

        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")

    # ------------------------------------------------------------------
    # 有感地震報告
    # ------------------------------------------------------------------
    def _fetch_records(self, endpoint_id: str, node: str, limit: int = 30) -> list:
        """從 CWA datastore API 取得資料（node = records 底下的陣列名）"""
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

        return as_list(data.get('records', {}).get(node, []))

    def _parse_earthquake(self, eq: dict, source_type: str) -> dict:
        """解析單筆地震報告（事件層級；逐站資料另由 _extract_stations 展開）"""
        info = eq.get('EarthquakeInfo', {}) or {}
        epicenter = info.get('Epicenter', {}) or {}
        magnitude = info.get('EarthquakeMagnitude', {}) or {}
        shaking_areas = as_list((eq.get('Intensity', {}) or {}).get('ShakingArea', []))

        origin_time = info.get('OriginTime', '')
        earthquake_no = eq.get('EarthquakeNo')

        station_count = sum(len(as_list(a.get('EqStation'))) for a in shaking_areas)

        # 最大震度：ShakingArea 同時含「觀測分區」與「最大震度N級地區」摘要列，
        # 首筆不保證最大 → 取所有分區的數值最大者。
        max_intensity = ''
        max_value = None
        for area in shaking_areas:
            txt = area.get('AreaIntensity', '')
            val = intensity_to_value(txt)
            if val is not None and (max_value is None or val > max_value):
                max_value, max_intensity = val, txt

        return {
            'event_id': make_event_id(earthquake_no, origin_time, source_type),
            'earthquake_no': earthquake_no,
            'source_type': source_type,
            'report_type': eq.get('ReportType', ''),
            'origin_time': origin_time,
            'focal_depth_km': safe_float(info.get('FocalDepth')),
            'epicenter_location': epicenter.get('Location', ''),
            'epicenter_latitude': safe_float(epicenter.get('EpicenterLatitude')),
            'epicenter_longitude': safe_float(epicenter.get('EpicenterLongitude')),
            'magnitude_type': magnitude.get('MagnitudeType', ''),
            'magnitude_value': safe_float(magnitude.get('MagnitudeValue')),
            'max_intensity': max_intensity,
            'max_intensity_value': max_value,
            'station_count': station_count,
            'report_content': eq.get('ReportContent', ''),
            'report_image_uri': eq.get('ReportImageURI', ''),
            'shakemap_image_uri': eq.get('ShakemapImageURI', ''),
            'issue_time': eq.get('IssueTime', ''),
            'web': eq.get('Web', ''),
        }

    @staticmethod
    def _extract_stations(eq: dict, parsed: dict) -> list:
        """展開一筆地震報告的逐測站觀測（震度 + PGA/PGV 三分量）"""
        rows = []
        seen = set()
        for area in as_list((eq.get('Intensity', {}) or {}).get('ShakingArea', [])):
            for st in as_list(area.get('EqStation')):
                station_id = st.get('StationID')
                if not station_id or station_id in seen:
                    continue
                seen.add(station_id)

                pga = st.get('pga', {}) or {}
                pgv = st.get('pgv', {}) or {}
                intensity_txt = st.get('SeismicIntensity', '')

                rows.append({
                    'event_id': parsed['event_id'],
                    'earthquake_no': safe_int(parsed['earthquake_no']),
                    'origin_time': parsed['origin_time'],
                    'source_type': parsed['source_type'],
                    'station_id': station_id,
                    'station_name': st.get('StationName', ''),
                    'county_name': area.get('CountyName', ''),
                    'area_desc': area.get('AreaDesc', ''),
                    'area_intensity': area.get('AreaIntensity', ''),
                    'lat': safe_float(st.get('StationLatitude')),
                    'lon': safe_float(st.get('StationLongitude')),
                    'epicenter_distance_km': safe_float(st.get('EpicenterDistance')),
                    'back_azimuth': safe_float(st.get('BackAzimuth')),
                    'seismic_intensity': intensity_txt,
                    'intensity_value': intensity_to_value(intensity_txt),
                    'pga_ew': safe_float(pga.get('EWComponent')),
                    'pga_ns': safe_float(pga.get('NSComponent')),
                    'pga_v': safe_float(pga.get('VComponent')),
                    'pga_int': safe_float(pga.get('IntScaleValue')),
                    'pgv_ew': safe_float(pgv.get('EWComponent')),
                    'pgv_ns': safe_float(pgv.get('NSComponent')),
                    'pgv_v': safe_float(pgv.get('VComponent')),
                    'pgv_int': safe_float(pgv.get('IntScaleValue')),
                    'wave_image_uri': st.get('WaveImageURI', ''),
                })
        return rows

    def _existing_station_events(self, event_ids: list) -> set:
        """查 live.earthquake_station_obs 已有哪些 event_id（避免每 15 分鐘重寫 1,100+ 列）

        查不到 / 沒有 writer → 回空 set（照寫，靠 ON CONFLICT DO NOTHING 兜底）。
        """
        if not event_ids or not self.supabase_writer:
            return set()
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT DISTINCT event_id FROM live.earthquake_station_obs "
                        "WHERE event_id = ANY(%s)",
                        (list(event_ids),),
                    )
                    return {row[0] for row in cur.fetchall()}
        except Exception as e:
            print(f"   [station_obs] 既有事件查詢失敗（改為全量寫入）: {e}")
            return set()

    # ------------------------------------------------------------------
    # 海嘯資訊
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_tsunami(t: dict) -> dict:
        info = t.get('EarthquakeInfo', {}) or {}
        epicenter = info.get('Epicenter', {}) or {}
        magnitude = info.get('EarthquakeMagnitude', {}) or {}
        valid = t.get('ValidTime', {}) or {}
        wave = t.get('TsunamiWave')

        return {
            'tsunami_no': safe_int(t.get('TsunamiNo')),
            'report_no': t.get('ReportNo', ''),
            'report_type': t.get('ReportType', ''),
            'report_color': t.get('ReportColor', ''),
            'report_content': t.get('ReportContent', ''),
            'issued_at': t.get('IssueTime', ''),
            'valid_end_at': valid.get('EndTime') or None,
            'origin_time': info.get('OriginTime') or None,
            'source': info.get('Source', ''),
            'epicenter_location': epicenter.get('Location', ''),
            'epicenter_lat': safe_float(epicenter.get('EpicenterLatitude')),
            'epicenter_lon': safe_float(epicenter.get('EpicenterLongitude')),
            'focal_depth_km': safe_float(info.get('FocalDepth')),
            'magnitude': safe_float(magnitude.get('MagnitudeValue')),
            'web_url': t.get('Web', ''),
            # TsunamiWave 全文（WarningArea 各沿海分區預估到時/波高 + TsuStation 潮位站實測）
            'station_details': wave,
            'raw': t,
        }

    # ------------------------------------------------------------------
    def collect(self) -> dict:
        """收集有感地震報告 + 逐站觀測 + 海嘯資訊"""
        fetch_time = datetime.now()

        # === 1. 有感地震報告 + 逐站觀測 ===
        print("   正在從 CWA API 取得有感地震報告...")

        parsed_by_id = {}       # event_id → 事件 dict
        stations_by_id = {}     # event_id → 逐站 list
        for source_type, endpoint_id in self.ENDPOINTS.items():
            try:
                reports = self._fetch_records(endpoint_id, 'Earthquake', limit=30)
                print(f"   [{source_type}] 取得 {len(reports)} 筆")
                for eq in reports:
                    parsed = self._parse_earthquake(eq, source_type)
                    eid = parsed['event_id']
                    if eid in parsed_by_id:
                        continue
                    parsed_by_id[eid] = parsed
                    stations_by_id[eid] = self._extract_stations(eq, parsed)
            except Exception as e:
                print(f"   [{source_type}] 取得失敗: {e}")

        unique_reports = sorted(
            parsed_by_id.values(), key=lambda x: x['origin_time'], reverse=True
        )

        # 逐站只寫「庫內還沒有的事件」，避免每輪重打 1,100+ 列的 no-op
        known = self._existing_station_events(list(stations_by_id.keys()))
        station_obs = [
            row
            for eid, rows in stations_by_id.items()
            if eid not in known
            for row in rows
        ]

        # 統計
        today_str = fetch_time.strftime('%Y-%m-%d')
        today_reports = [r for r in unique_reports if r['origin_time'].startswith(today_str)]
        magnitudes = [r['magnitude_value'] for r in unique_reports if r['magnitude_value']]
        max_mag = max(magnitudes) if magnitudes else None

        print(f"   有感報告: {len(unique_reports)} 筆 (今日: {len(today_reports)} 筆)"
              f"；逐站新寫 {len(station_obs)} 列（已入庫事件 {len(known)} 個略過）")

        # === 2. 海嘯資訊 ===
        tsunami = []
        try:
            # limit=100：上游目前公布 80 則（2023-01 起），一次抓齊當歷史回填；
            # 之後每輪重打靠 ON CONFLICT DO NOTHING 擋掉，成本可忽略
            raw_tsunami = self._fetch_records(self.TSUNAMI_ENDPOINT, 'Tsunami', limit=100)
            tsunami = [self._parse_tsunami(t) for t in raw_tsunami]
            print(f"   海嘯報告: {len(tsunami)} 筆")
        except Exception as e:
            print(f"   海嘯資訊取得失敗: {e}")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total_reports': len(unique_reports),
            'today_reports': len(today_reports),
            'max_magnitude': max_mag,
            'magnitude_range': {
                'min': min(magnitudes) if magnitudes else None,
                'max': max_mag,
            },
            'station_obs_count': len(station_obs),
            'tsunami_count': len(tsunami),
            'data': {
                'felt_reports': unique_reports,
                'station_obs': station_obs,
                'tsunami': tsunami,
            },
        }
