"""
Supabase 即時資料寫入模組

主路徑寫 DB（分區表 + current 表），失敗時暫存 buffer，定期重試。
使用 psycopg2 + Supavisor 連線池 (port 6543)。
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

import config

logger = logging.getLogger(__name__)

BUFFER_DIR = config.LOCAL_DATA_DIR / 'buffer'


class SupabaseWriter:
    """統一的 Supabase 寫入介面"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None
        self._connect()
        BUFFER_DIR.mkdir(parents=True, exist_ok=True)

    def _connect(self):
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.conn.autocommit = True
            logger.info("Supabase 連線成功")
        except Exception as e:
            logger.warning(f"Supabase 連線失敗: {e}")
            self.conn = None

    def _ensure_conn(self):
        if not self.conn or self.conn.closed:
            self._connect()
        if not self.conn:
            raise ConnectionError("Supabase 連線不可用")

    # ============================================================
    # 主要寫入介面
    # ============================================================

    def write(self, collector_name: str, result: dict, timestamp: datetime):
        """主路徑寫 DB，失敗時暫存 buffer"""
        try:
            records = self._transform(collector_name, result, timestamp)
            if not records:
                return

            self._write_to_db(collector_name, records, timestamp)

            # 衛星：額外更新 TLE 參數表（供前端 satellite.js 使用）
            if collector_name == 'satellite':
                self._write_satellite_tle(result, timestamp)

            self._report_heartbeat(collector_name, True, len(records))

        except Exception as e:
            logger.warning(f"[{collector_name}] DB 寫入失敗，暫存 buffer: {e}")
            self._write_to_buffer(collector_name, result, timestamp)
            self._report_heartbeat(collector_name, False, 0, str(e))

    def flush_buffer(self):
        """重試 buffer 中的資料"""
        buffer_files = sorted(BUFFER_DIR.glob("*.json"))
        if not buffer_files:
            return

        logger.info(f"Buffer 重試：{len(buffer_files)} 個待補寫檔案")
        for f in buffer_files:
            try:
                payload = json.loads(f.read_text())
                ts = datetime.fromisoformat(payload['timestamp'])
                records = self._transform(payload['collector'], payload['result'], ts)
                if records:
                    self._write_to_db(payload['collector'], records, ts)
                f.unlink()
                logger.info(f"Buffer 補寫成功：{f.name}")
            except Exception as e:
                logger.warning(f"Buffer 重試失敗：{f.name}: {e}")
                break  # DB 仍不可用，等下次

    # ============================================================
    # 資料轉換：Collector 原始格式 → DB 欄位
    # ============================================================

    def _transform(self, collector_name: str, result: dict, timestamp: datetime) -> list[dict]:
        """將 collector 回傳的 result 轉換為 DB records"""
        transformer = self.TRANSFORMERS.get(collector_name)
        if not transformer:
            logger.debug(f"[{collector_name}] 無對應 transformer，跳過")
            return []
        return transformer(self, result, timestamp)

    def _transform_youbike(self, result: dict, ts: datetime) -> list[dict]:
        records = []
        for r in result.get('data', []):
            rent = r.get('AvailableRentBikes', 0) or 0
            ret = r.get('AvailableReturnBikes', 0) or 0
            records.append({
                'station_uid': str(r.get('StationUID', '')),
                'city': r.get('_city', ''),
                'available_rent': rent,
                'available_return': ret,
                'total': rent + ret,
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_bus(self, result: dict, ts: datetime) -> list[dict]:
        records = []
        for r in result.get('data', []):
            pos = r.get('BusPosition', {})
            route = r.get('RouteName', {})
            records.append({
                'plate_numb': r.get('PlateNumb', ''),
                'route_uid': r.get('RouteUID', ''),
                'route_name': route.get('Zh_tw', '') if isinstance(route, dict) else str(route),
                'direction': r.get('Direction', 0),
                'bus_lat': pos.get('PositionLat', None) if isinstance(pos, dict) else None,
                'bus_lng': pos.get('PositionLon', None) if isinstance(pos, dict) else None,
                'speed': r.get('Speed', None),
                'city': r.get('_city', ''),
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_weather(self, result: dict, ts: datetime) -> list[dict]:
        records = []
        for r in result.get('data', []):
            try:
                lat = float(r.get('latitude')) if r.get('latitude') else None
                lng = float(r.get('longitude')) if r.get('longitude') else None
            except (ValueError, TypeError):
                lat, lng = None, None
            records.append({
                'station_id': r.get('station_id', ''),
                'station_name': r.get('station_name', ''),
                'temperature': r.get('temperature'),
                'humidity': r.get('humidity'),
                'pressure': r.get('pressure'),
                'wind_speed': r.get('wind_speed'),
                'wind_direction': r.get('wind_direction'),
                'rainfall': r.get('precipitation_now'),
                'observed_at': r.get('obs_time', ts.isoformat()),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
            })
        return records

    def _transform_temperature(self, result: dict, ts: datetime) -> list[dict]:
        """溫度網格：二維陣列展開為 row"""
        grid_data = result.get('data', [])
        geo = result.get('geo_info', {})

        if not grid_data or not geo:
            return []

        lat_start = geo.get('bottom_left_lat', geo.get('lat_start', 0))
        lng_start = geo.get('bottom_left_lon', geo.get('lng_start', 0))
        lat_step = geo.get('resolution_deg', geo.get('lat_step', 0.03))
        lng_step = geo.get('resolution_deg', geo.get('lng_step', 0.03))
        obs_time = result.get('observation_time', ts.isoformat())

        records = []
        for row_idx, row in enumerate(grid_data):
            if not isinstance(row, list):
                continue
            lat = lat_start + row_idx * lat_step
            for col_idx, temp in enumerate(row):
                if temp is None:
                    continue
                lng = lng_start + col_idx * lng_step
                records.append({
                    'grid_lat': round(lat, 4),
                    'grid_lng': round(lng, 4),
                    'temperature': temp,
                    'observed_at': obs_time,
                    'collected_at': ts.isoformat(),
                })
        return records

    def _transform_tra_train(self, result: dict, ts: datetime) -> list[dict]:
        records = []
        for r in result.get('data', []):
            train_type = r.get('TrainTypeName', {})
            records.append({
                'train_no': r.get('TrainNo', ''),
                'train_type': train_type.get('Zh_tw', '') if isinstance(train_type, dict) else str(train_type),
                'station_id': r.get('StationID', ''),
                'delay_minutes': r.get('DelayTime', 0),
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_ship_ais(self, result: dict, ts: datetime) -> list[dict]:
        records = []
        for r in result.get('data', []):
            lat = r.get('lat')
            lng = r.get('lon')
            records.append({
                'mmsi': str(r.get('mmsi', '')) if r.get('mmsi') else None,
                'ship_name': r.get('ship_name', ''),
                'ship_type': r.get('vessel_type_name', ''),
                'lat': lat,
                'lng': lng,
                'speed': r.get('sog'),
                'heading': r.get('heading'),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
            })
        return records

    def _transform_earthquake(self, result: dict, ts: datetime) -> list[dict]:
        """地震：合併 felt_reports + catalog"""
        data = result.get('data', {})
        if isinstance(data, list):
            return []

        records = []
        # 有感地震
        for r in data.get('felt_reports', []):
            records.append({
                'event_id': str(r.get('earthquake_no', '')),
                'magnitude': r.get('magnitude_value'),
                'depth_km': r.get('focal_depth_km'),
                'epicenter_lat': r.get('epicenter_latitude'),
                'epicenter_lng': r.get('epicenter_longitude'),
                'location_desc': r.get('epicenter_location', ''),
                'occurred_at': r.get('origin_time', ts.isoformat()),
                'report_type': r.get('source_type', 'felt'),
                'geom': f"SRID=4326;POINT({r.get('epicenter_longitude')} {r.get('epicenter_latitude')})" if r.get('epicenter_latitude') and r.get('epicenter_longitude') else None,
                'raw_data': json.dumps(r, ensure_ascii=False, default=str),
            })
        # 完整目錄
        for r in data.get('catalog', []):
            lat = r.get('latitude')
            lng = r.get('longitude')
            records.append({
                'event_id': f"cat_{r.get('origin_time', '')}_{lat}_{lng}",
                'magnitude': r.get('local_magnitude'),
                'depth_km': r.get('focal_depth_km'),
                'epicenter_lat': lat,
                'epicenter_lng': lng,
                'location_desc': '',
                'occurred_at': r.get('origin_time', ts.isoformat()),
                'report_type': 'catalog',
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
                'raw_data': json.dumps(r, ensure_ascii=False, default=str),
            })
        return records

    def _transform_rail_timetable(self, result: dict, ts: datetime) -> list[dict]:
        """時刻表：寫入 reference.daily_schedules（非 realtime）"""
        data = result.get('data', {})
        if isinstance(data, list):
            return []

        today = ts.strftime('%Y-%m-%d')
        records = []
        for system in ('tra', 'thsr'):
            sys_data = data.get(system, {})
            if sys_data and sys_data.get('data'):
                records.append({
                    '_system': system,
                    '_schedule_date': today,
                    '_train_count': sys_data.get('train_count', len(sys_data['data'])),
                    '_data': json.dumps(sys_data['data'], ensure_ascii=False, default=str),
                })
        return records

    def _transform_flight_fr24(self, result: dict, ts: datetime) -> list[dict]:
        """FR24 航班：含 trail 軌跡 → 寫入 flight_trails 表"""
        records = []
        for r in result.get('data', []):
            if not isinstance(r, dict) or not r:
                continue
            trail = r.get('trail', [])
            if not trail or not isinstance(trail, list):
                continue

            # 從 trail 建立 LineString
            coords = []
            for pt in trail:
                if isinstance(pt, dict):
                    plat, plng = pt.get('lat'), pt.get('lng', pt.get('lon'))
                elif isinstance(pt, list) and len(pt) >= 2:
                    plat, plng = pt[0], pt[1]
                else:
                    continue
                if plat and plng:
                    coords.append((float(plng), float(plat)))

            geom = None
            if len(coords) >= 2:
                coord_str = ','.join(f'{lng} {lat}' for lng, lat in coords)
                geom = f'SRID=4326;LINESTRING({coord_str})'

            records.append({
                '_type': 'trail',
                'flight_id': r.get('fr24_id', r.get('flight_id', '')),
                'callsign': r.get('callsign', ''),
                'aircraft_type': r.get('aircraft_type', ''),
                'registration': r.get('registration', ''),
                'origin': r.get('origin_icao', r.get('origin_iata', '')),
                'destination': r.get('dest_icao', r.get('dest_iata', '')),
                'status': r.get('status', ''),
                'trail': json.dumps(trail, default=str),
                'trail_points': len(trail),
                'geom': geom,
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_flight_fr24_zone(self, result: dict, ts: datetime) -> list[dict]:
        """FR24 Zone 空域快照"""
        records = []
        for r in result.get('data', []):
            if not isinstance(r, dict):
                continue
            lat = r.get('latitude')
            lng = r.get('longitude')
            if not lat or not lng:
                continue
            records.append({
                'flight_id': r.get('fr24_id', r.get('icao24', '')),
                'callsign': r.get('callsign', ''),
                'aircraft_type': r.get('aircraft_type', ''),
                'origin': r.get('origin_iata', ''),
                'destination': r.get('destination_iata', ''),
                'lat': float(lat),
                'lng': float(lng),
                'altitude': r.get('altitude_ft'),
                'speed': r.get('speed_kts'),
                'heading': r.get('track'),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})',
            })
        return records

    def _transform_flight_opensky(self, result: dict, ts: datetime) -> list[dict]:
        """OpenSky 空域快照"""
        records = []
        for r in result.get('data', []):
            if not isinstance(r, dict):
                continue
            lat = r.get('latitude')
            lng = r.get('longitude')
            if not lat or not lng:
                continue
            records.append({
                'flight_id': r.get('icao24', ''),
                'callsign': (r.get('callsign') or '').strip(),
                'aircraft_type': '',
                'origin': r.get('origin_country', ''),
                'destination': '',
                'lat': float(lat),
                'lng': float(lng),
                'altitude': r.get('baro_altitude') or r.get('geo_altitude'),
                'speed': r.get('velocity'),
                'heading': r.get('true_track'),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})',
            })
        return records

    def _transform_freeway_vd(self, result: dict, ts: datetime) -> list[dict]:
        """國道壅塞 + VD 車流：回傳特殊格式，由 _write_to_db 分別處理"""
        data = result.get('data', {})
        if isinstance(data, list):
            return []

        records = []
        # sections（壅塞路段）
        for r in data.get('sections', []):
            records.append({
                '_type': 'section',
                'section_id': r.get('SectionID', ''),
                'travel_speed': r.get('TravelSpeed'),
                'travel_time': r.get('TravelTime'),
                'congestion_level': r.get('CongestionLevel'),
                'collected_at': ts.isoformat(),
            })
        # vd（車流偵測器）
        for r in data.get('vd', []):
            records.append({
                '_type': 'vd',
                'vd_id': r.get('VDID', ''),
                'total_volume': r.get('TotalVolume'),
                'avg_speed': r.get('AvgSpeed'),
                'avg_occupancy': r.get('AvgOccupancy'),
                'volume_small_car': r.get('VolumeSmallCar'),
                'volume_large_car': r.get('VolumeLargeCar'),
                'volume_trailer': r.get('VolumeTrailer'),
                'lane_count': r.get('LaneCount'),
                'status': r.get('Status'),
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_satellite(self, result: dict, ts: datetime) -> list[dict]:
        """衛星位置：GP + SGP4 計算結果"""
        records = []
        for r in result.get('data', []):
            lat = r.get('lat')
            lng = r.get('lng')
            records.append({
                'norad_id': r.get('norad_id'),
                'name': r.get('name', ''),
                'constellation': r.get('constellation', ''),
                'orbit_type': r.get('orbit_type', ''),
                'lat': lat,
                'lng': lng,
                'altitude_km': r.get('altitude_km'),
                'velocity_kms': r.get('velocity_kms'),
                'inclination': r.get('inclination'),
                'period_min': r.get('period_min'),
                'tle_epoch': r.get('tle_epoch', ''),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
            })
        return records

    def _transform_launch(self, result: dict, ts: datetime) -> list[dict]:
        """太空發射：launches + pads + events 三合一"""
        data = result.get('data', {})
        if isinstance(data, list):
            return []

        records = []

        # launches
        for r in data.get('launches', []):
            lat = r.get('pad_latitude')
            lng = r.get('pad_longitude')
            records.append({
                '_type': 'launch',
                'id': r.get('id', ''),
                'name': r.get('name', ''),
                'slug': r.get('slug', ''),
                'net': r.get('net'),
                'window_start': r.get('window_start'),
                'window_end': r.get('window_end'),
                'status': r.get('status', ''),
                'status_name': r.get('status_name', ''),
                'rocket_name': r.get('rocket_name', ''),
                'rocket_family': r.get('rocket_family', ''),
                'rocket_full_name': r.get('rocket_full_name', ''),
                'mission_name': r.get('mission_name', ''),
                'mission_type': r.get('mission_type', ''),
                'mission_description': r.get('mission_description', ''),
                'orbit_name': r.get('orbit_name', ''),
                'orbit_abbrev': r.get('orbit_abbrev', ''),
                'agency_name': r.get('agency_name', ''),
                'agency_type': r.get('agency_type', ''),
                'pad_id': r.get('pad_id'),
                'pad_name': r.get('pad_name', ''),
                'location_name': r.get('location_name', ''),
                'country_code': r.get('country_code', ''),
                'probability': r.get('probability'),
                'weather_concerns': r.get('weather_concerns', ''),
                'webcast_live': r.get('webcast_live', False),
                'image_url': r.get('image_url', ''),
                'infographic_url': r.get('infographic_url', ''),
                'program_names': r.get('program_names', ''),
                'last_updated': r.get('last_updated'),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
            })

        # pads
        for r in data.get('pads', []):
            lat = r.get('latitude')
            lng = r.get('longitude')
            records.append({
                '_type': 'pad',
                'id': r.get('id', ''),
                'name': r.get('name', ''),
                'latitude': lat,
                'longitude': lng,
                'location_name': r.get('location_name', ''),
                'country_code': r.get('country_code', ''),
                'total_launch_count': r.get('total_launch_count', 0),
                'orbital_launch_attempt_count': r.get('orbital_launch_attempt_count', 0),
                'map_url': r.get('map_url', ''),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
            })

        # events
        for r in data.get('events', []):
            records.append({
                '_type': 'event',
                'id': r.get('id', ''),
                'name': r.get('name', ''),
                'description': r.get('description', ''),
                'type_name': r.get('type_name', ''),
                'date': r.get('date'),
                'location': r.get('location', ''),
                'news_url': r.get('news_url', ''),
                'video_url': r.get('video_url', ''),
                'image_url': r.get('image_url', ''),
                'program_names': r.get('program_names', ''),
                'launch_ids': r.get('launch_ids', ''),
                'last_updated': r.get('last_updated'),
                'collected_at': ts.isoformat(),
            })

        return records

    TRANSFORMERS = {
        'youbike': _transform_youbike,
        'bus': _transform_bus,
        'weather': _transform_weather,
        'temperature': _transform_temperature,
        'tra_train': _transform_tra_train,
        'ship_ais': _transform_ship_ais,
        'earthquake': _transform_earthquake,
        'rail_timetable': _transform_rail_timetable,
        'flight_fr24': _transform_flight_fr24,
        'flight_fr24_zone': _transform_flight_fr24_zone,
        'flight_opensky': _transform_flight_opensky,
        'freeway_vd': _transform_freeway_vd,
        'satellite': _transform_satellite,
        'launch': _transform_launch,
    }

    # ============================================================
    # DB 寫入：分區表 + current 表
    # ============================================================

    # collector_name → (history_table, current_table, current_key, columns)
    TABLE_MAP = {
        'youbike': {
            'history': 'realtime.youbike_snapshots',
            'current': 'realtime.youbike_current',
            'current_key': 'station_uid',
            'columns': ['station_uid', 'city', 'available_rent', 'available_return', 'total', 'collected_at'],
        },
        'bus': {
            'history': 'realtime.bus_positions',
            'current': 'realtime.bus_current',
            'current_key': 'plate_numb',
            'columns': ['plate_numb', 'route_uid', 'route_name', 'direction', 'bus_lat', 'bus_lng', 'speed', 'city', 'collected_at'],
        },
        'weather': {
            'history': 'realtime.weather_observations',
            'current': 'realtime.weather_current',
            'current_key': 'station_id',
            'columns': ['station_id', 'station_name', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'rainfall', 'observed_at', 'collected_at', 'geom'],
            'current_columns': ['station_id', 'station_name', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'rainfall', 'observed_at', 'geom'],
        },
        'temperature': {
            'history': 'realtime.temperature_grids',
            'columns': ['grid_lat', 'grid_lng', 'temperature', 'observed_at', 'collected_at'],
        },
        'tra_train': {
            'history': 'realtime.train_positions',
            'columns': ['train_no', 'train_type', 'station_id', 'delay_minutes', 'collected_at'],
        },
        'ship_ais': {
            'history': 'realtime.ship_positions',
            'current': 'realtime.ship_current',
            'current_key': 'mmsi',
            'columns': ['mmsi', 'ship_name', 'ship_type', 'lat', 'lng', 'speed', 'heading', 'collected_at', 'geom'],
            'current_columns': ['mmsi', 'ship_name', 'ship_type', 'lat', 'lng', 'speed', 'heading', 'collected_at', 'geom'],
        },
        'earthquake': {
            'history': 'realtime.earthquake_events',
            'columns': ['event_id', 'magnitude', 'depth_km', 'epicenter_lat', 'epicenter_lng', 'location_desc', 'occurred_at', 'report_type', 'geom', 'raw_data'],
            'upsert_key': 'event_id',
        },
        'rail_timetable': {
            'is_reference': True,
        },
        'flight_fr24': {
            'is_multi_table': True,  # 特殊處理：trail 寫入 flight_trails
        },
        'flight_fr24_zone': {
            'history': 'realtime.flight_positions',
            'columns': ['flight_id', 'callsign', 'aircraft_type', 'origin', 'destination', 'lat', 'lng', 'altitude', 'speed', 'heading', 'collected_at', 'geom'],
        },
        'flight_opensky': {
            'history': 'realtime.flight_positions',
            'columns': ['flight_id', 'callsign', 'aircraft_type', 'origin', 'destination', 'lat', 'lng', 'altitude', 'speed', 'heading', 'collected_at', 'geom'],
        },
        'freeway_vd': {
            'is_multi_table': True,  # 特殊處理：sections + vd 分兩張表
        },
        'satellite': {
            'history': 'realtime.satellite_positions',
            'current': 'realtime.satellite_current',
            'current_key': 'norad_id',
            'columns': ['norad_id', 'name', 'constellation', 'orbit_type', 'lat', 'lng', 'altitude_km', 'velocity_kms', 'inclination', 'period_min', 'tle_epoch', 'collected_at', 'geom'],
            'current_columns': ['norad_id', 'name', 'constellation', 'orbit_type', 'lat', 'lng', 'altitude_km', 'velocity_kms', 'inclination', 'period_min', 'tle_epoch', 'collected_at', 'geom'],
        },
        'launch': {
            'is_multi_table': True,  # 特殊處理：launches + pads + events 分三張表
        },
    }

    def _write_to_db(self, collector_name: str, records: list[dict], timestamp: datetime):
        self._ensure_conn()

        table_config = self.TABLE_MAP.get(collector_name)
        if not table_config:
            return

        # 特殊處理：時刻表寫入 reference schema
        if table_config.get('is_reference'):
            self._write_schedules(records)
            return

        # 特殊處理：多表寫入（freeway_vd, flight_fr24）
        if table_config.get('is_multi_table'):
            self._write_multi_table(collector_name, records)
            return

        columns = table_config['columns']

        with self.conn.cursor() as cur:
            # 1. INSERT INTO 分區表（歷史）
            values = []
            for r in records:
                row = tuple(r.get(c) for c in columns)
                values.append(row)

            if values:
                placeholders = ','.join(['%s'] * len(columns))
                col_names = ','.join(columns)

                # 地震用 UPSERT（避免重複）
                if table_config.get('upsert_key'):
                    key = table_config['upsert_key']
                    update_cols = [c for c in columns if c != key]
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                    sql = f"INSERT INTO {table_config['history']} ({col_names}) VALUES %s ON CONFLICT ({key}) DO UPDATE SET {update_set}"
                else:
                    sql = f"INSERT INTO {table_config['history']} ({col_names}) VALUES %s"

                execute_values(cur, sql, values, page_size=1000)

            # 2. UPSERT INTO current 表（最新狀態）
            # 同一批次內可能有重複 PK（例如同一輛公車出現兩次），
            # ON CONFLICT 無法在同一 INSERT 中更新同一行兩次，因此先去重（保留最後一筆）
            if 'current' in table_config:
                current_cols = table_config.get('current_columns', columns)
                key = table_config['current_key']
                key_idx = current_cols.index(key) if key in current_cols else 0
                update_cols = [c for c in current_cols if c != key]
                update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                col_names = ','.join(current_cols)

                # 去重：同一 key 只保留最後出現的那筆
                seen = {}
                for r in records:
                    k = r.get(key)
                    if k is not None:
                        seen[k] = tuple(r.get(c) for c in current_cols)
                current_values = list(seen.values())

                if current_values:
                    sql = f"INSERT INTO {table_config['current']} ({col_names}) VALUES %s ON CONFLICT ({key}) DO UPDATE SET {update_set}"
                    execute_values(cur, sql, current_values, page_size=1000)

        record_count = len(records)
        logger.info(f"[{collector_name}] ✓ DB 寫入 {record_count} 筆")

    def _write_multi_table(self, collector_name: str, records: list[dict]):
        """freeway_vd 和 flight_fr24 的多表寫入"""
        self._ensure_conn()

        if collector_name == 'freeway_vd':
            sections = [r for r in records if r.get('_type') == 'section']
            vds = [r for r in records if r.get('_type') == 'vd']

            with self.conn.cursor() as cur:
                if sections:
                    cols = ['section_id', 'travel_speed', 'travel_time', 'congestion_level', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in sections]
                    execute_values(cur, f"INSERT INTO realtime.freeway_sections ({','.join(cols)}) VALUES %s", values, page_size=1000)
                    # current 表
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in cols if c != 'section_id')
                    execute_values(cur, f"INSERT INTO realtime.freeway_sections_current ({','.join(cols)}) VALUES %s ON CONFLICT (section_id) DO UPDATE SET {update_set}", values, page_size=1000)

                if vds:
                    cols = ['vd_id', 'total_volume', 'avg_speed', 'avg_occupancy', 'volume_small_car', 'volume_large_car', 'volume_trailer', 'lane_count', 'status', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in vds]
                    execute_values(cur, f"INSERT INTO realtime.freeway_vd_traffic ({','.join(cols)}) VALUES %s", values, page_size=1000)

            logger.info(f"[freeway_vd] ✓ sections {len(sections)} + vd {len(vds)} 筆寫入")

        elif collector_name == 'flight_fr24':
            trails = [r for r in records if r.get('_type') == 'trail']
            with self.conn.cursor() as cur:
                if trails:
                    cols = ['flight_id', 'callsign', 'aircraft_type', 'registration', 'origin', 'destination', 'status', 'trail', 'trail_points', 'geom', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in trails]
                    execute_values(cur, f"INSERT INTO realtime.flight_trails ({','.join(cols)}) VALUES %s", values, page_size=100)
            logger.info(f"[flight_fr24] ✓ {len(trails)} 筆航跡寫入")

        elif collector_name == 'launch':
            launches = [r for r in records if r.get('_type') == 'launch']
            pads = [r for r in records if r.get('_type') == 'pad']
            events = [r for r in records if r.get('_type') == 'event']

            with self.conn.cursor() as cur:
                # launches — UPSERT（id 為 PK）
                if launches:
                    cols = ['id', 'name', 'slug', 'net', 'window_start', 'window_end',
                            'status', 'status_name', 'rocket_name', 'rocket_family', 'rocket_full_name',
                            'mission_name', 'mission_type', 'mission_description',
                            'orbit_name', 'orbit_abbrev', 'agency_name', 'agency_type',
                            'pad_id', 'pad_name', 'location_name', 'country_code',
                            'probability', 'weather_concerns', 'webcast_live',
                            'image_url', 'infographic_url', 'program_names',
                            'last_updated', 'collected_at', 'geom']
                    values = [tuple(r.get(c) for c in cols) for r in launches]
                    update_cols = [c for c in cols if c != 'id']
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                    execute_values(cur,
                        f"INSERT INTO realtime.launches ({','.join(cols)}) VALUES %s "
                        f"ON CONFLICT (id) DO UPDATE SET {update_set}",
                        values, page_size=500)

                # pads — UPSERT（id 為 PK）
                if pads:
                    cols = ['id', 'name', 'latitude', 'longitude', 'location_name',
                            'country_code', 'total_launch_count', 'orbital_launch_attempt_count',
                            'map_url', 'collected_at', 'geom']
                    values = [tuple(r.get(c) for c in cols) for r in pads]
                    update_cols = [c for c in cols if c != 'id']
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                    execute_values(cur,
                        f"INSERT INTO realtime.launch_pads ({','.join(cols)}) VALUES %s "
                        f"ON CONFLICT (id) DO UPDATE SET {update_set}",
                        values, page_size=500)

                # events — UPSERT（id 為 PK）
                if events:
                    cols = ['id', 'name', 'description', 'type_name', 'date',
                            'location', 'news_url', 'video_url', 'image_url',
                            'program_names', 'launch_ids', 'last_updated', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in events]
                    update_cols = [c for c in cols if c != 'id']
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                    execute_values(cur,
                        f"INSERT INTO realtime.launch_events ({','.join(cols)}) VALUES %s "
                        f"ON CONFLICT (id) DO UPDATE SET {update_set}",
                        values, page_size=500)

            logger.info(f"[launch] ✓ {len(launches)} launches + {len(pads)} pads + {len(events)} events 寫入")

    def _write_satellite_tle(self, result: dict, timestamp: datetime):
        """更新衛星 TLE 參數表（全量 UPSERT，供前�� SGP4 計算用）"""
        self._ensure_conn()
        data = result.get('data', [])
        if not data:
            return

        cols = ['norad_id', 'name', 'intl_designator', 'constellation', 'orbit_type',
                'tle_line1', 'tle_line2', 'tle_epoch', 'inclination', 'eccentricity', 'period_min', 'updated_at']
        update_cols = [c for c in cols if c != 'norad_id']
        update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)

        values = []
        for r in data:
            if not r.get('tle_line1') or not r.get('tle_line2'):
                continue
            values.append((
                r.get('norad_id'),
                r.get('name', ''),
                r.get('intl_designator', ''),
                r.get('constellation', ''),
                r.get('orbit_type', ''),
                r['tle_line1'],
                r['tle_line2'],
                r.get('tle_epoch', ''),
                r.get('inclination'),
                r.get('eccentricity'),
                r.get('period_min'),
                timestamp.isoformat(),
            ))

        if values:
            with self.conn.cursor() as cur:
                sql = f"INSERT INTO realtime.satellite_tle ({','.join(cols)}) VALUES %s ON CONFLICT (norad_id) DO UPDATE SET {update_set}"
                execute_values(cur, sql, values, page_size=1000)
            logger.info(f"[satellite] ✓ TLE 表已更新 {len(values)} 筆")

    def _write_schedules(self, records: list[dict]):
        """寫入每日時刻表到 reference.daily_schedules"""
        self._ensure_conn()
        with self.conn.cursor() as cur:
            for r in records:
                cur.execute(
                    """INSERT INTO reference.daily_schedules (system, schedule_date, train_count, data)
                       VALUES (%s, %s, %s, %s::jsonb)
                       ON CONFLICT (system, schedule_date) DO UPDATE SET
                       train_count = EXCLUDED.train_count, data = EXCLUDED.data""",
                    (r['_system'], r['_schedule_date'], r['_train_count'], r['_data'])
                )
        logger.info(f"[rail_timetable] ✓ 時刻表已寫入")

    # ============================================================
    # Buffer（失敗安全網）
    # ============================================================

    def _write_to_buffer(self, collector_name: str, result: dict, timestamp: datetime):
        ts_str = timestamp.strftime('%Y%m%d_%H%M%S')
        buffer_file = BUFFER_DIR / f"{collector_name}_{ts_str}.json"
        buffer_file.write_text(json.dumps({
            'collector': collector_name,
            'timestamp': timestamp.isoformat(),
            'result': result,
        }, ensure_ascii=False, default=str))
        logger.info(f"[{collector_name}] 已暫存 buffer: {buffer_file.name}")

    # ============================================================
    # 心跳回報
    # ============================================================

    def _report_heartbeat(self, collector_name: str, success: bool, records: int = 0, error: str = None):
        try:
            self._ensure_conn()
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT report_collector_heartbeat(%s, %s, %s, %s)",
                    (collector_name, success, records, error)
                )
        except Exception as e:
            logger.debug(f"心跳回報失敗: {e}")
