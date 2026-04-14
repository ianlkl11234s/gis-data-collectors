"""
Supabase 即時資料寫入模組

主路徑寫 DB（分區表 + current 表），失敗時暫存 buffer，定期重試。
使用 psycopg2 + Supavisor 連線池 (port 6543)。
"""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2 import Binary as PgBinary
from psycopg2.extras import execute_values

import config
from utils.notify import send_telegram, _escape_md, _instance_tag
from tasks.mini_taipei_publish import (
    build_track_index,
    convert_tra_timetable,
    convert_thsr_timetable,
)

logger = logging.getLogger(__name__)

BUFFER_DIR = config.LOCAL_DATA_DIR / 'buffer'


class SupabaseWriter:
    """統一的 Supabase 寫入介面"""

    # DB 寫入連續錯誤追蹤（跨 collector 共用）
    _db_consecutive_errors: dict[str, int] = {}
    _DB_ERROR_ALERT_THRESHOLD = 3  # 連續 N 次失敗才告警（避免瞬時錯誤洗版）

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None
        # 跨 thread 序列化 DB 操作（psycopg2 連線非 thread-safe）
        # 給背景 thread collector（flight_fr24）與主 schedule thread 共用 writer
        self._lock = threading.RLock()
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
        """主路徑寫 DB，失敗時暫存 buffer（thread-safe）"""
        with self._lock:
            return self._write_locked(collector_name, result, timestamp)

    def _write_locked(self, collector_name: str, result: dict, timestamp: datetime):
        try:
            records = self._transform(collector_name, result, timestamp)
            if not records:
                return

            self._write_to_db(collector_name, records, timestamp)

            # 衛星：額外更新 TLE 參數表（供前端 satellite.js 使用）
            if collector_name == 'satellite':
                self._write_satellite_tle(result, timestamp)

            self._report_heartbeat(collector_name, True, len(records))

            # 寫入成功：重置連續錯誤計數，恢復時通知
            prev_errors = self._db_consecutive_errors.get(collector_name, 0)
            if prev_errors >= self._DB_ERROR_ALERT_THRESHOLD:
                tag = _instance_tag()
                send_telegram(
                    f"✅ *DB 寫入恢復*{tag}\n\n"
                    f"收集器: `{collector_name}`\n"
                    f"之前連續失敗: {prev_errors} 次"
                )
            self._db_consecutive_errors[collector_name] = 0

        except Exception as e:
            logger.warning(f"[{collector_name}] DB 寫入失敗，暫存 buffer: {e}")
            self._write_to_buffer(collector_name, result, timestamp)
            self._report_heartbeat(collector_name, False, 0, str(e))

            # DB 寫入連續錯誤追蹤 + Telegram 告警
            self._db_consecutive_errors[collector_name] = self._db_consecutive_errors.get(collector_name, 0) + 1
            count = self._db_consecutive_errors[collector_name]
            if count == self._DB_ERROR_ALERT_THRESHOLD:
                tag = _instance_tag()
                tg_msg = (
                    f"🗄️ *DB 寫入連續失敗*{tag}\n\n"
                    f"收集器: `{collector_name}`\n"
                    f"連續失敗: *{count} 次*\n"
                    f"錯誤: {_escape_md(str(e)[:200])}\n\n"
                    f"資料已暫存 buffer，待問題修復後自動補回"
                )
                send_telegram(tg_msg)

    def flush_buffer(self):
        """重試 buffer 中的資料（thread-safe）"""
        with self._lock:
            return self._flush_buffer_locked()

    # Buffer 檔最大保留天數：超過則直接丟棄
    # 因為分區表 retention 會自動刪舊分區，過期 buffer 已無處可寫，且會永久卡住其他檔案
    BUFFER_MAX_AGE_DAYS = 3

    # 連續失敗多少筆後判定 DB 仍不可用、放棄本輪
    BUFFER_FAIL_THRESHOLD = 5

    def _flush_buffer_locked(self):
        from datetime import timezone, timedelta as _td

        buffer_files = sorted(BUFFER_DIR.glob("*.json"))
        if not buffer_files:
            return

        logger.info(f"Buffer 重試：{len(buffer_files)} 個待補寫檔案")
        now = datetime.now(timezone.utc)
        max_age = _td(days=self.BUFFER_MAX_AGE_DAYS)

        success = 0
        skipped_old = 0
        consecutive_failures = 0

        for f in buffer_files:
            try:
                payload = json.loads(f.read_text())
                ts = datetime.fromisoformat(payload['timestamp'])
                if ts.tzinfo is None:
                    ts_cmp = ts.replace(tzinfo=timezone.utc)
                else:
                    ts_cmp = ts

                # 過期 buffer 直接丟棄（分區可能已被 retention 清掉）
                if now - ts_cmp > max_age:
                    f.unlink()
                    skipped_old += 1
                    logger.info(f"Buffer 過期丟棄：{f.name} (age={now - ts_cmp})")
                    continue

                records = self._transform(payload['collector'], payload['result'], ts)
                if records:
                    self._write_to_db(payload['collector'], records, ts)
                f.unlink()
                success += 1
                consecutive_failures = 0
                logger.info(f"Buffer 補寫成功：{f.name}")
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Buffer 重試失敗：{f.name}: {e}")
                # 不再 break — 改為連續多筆失敗才放棄，避免單一爛檔卡住其他
                if consecutive_failures >= self.BUFFER_FAIL_THRESHOLD:
                    logger.warning(f"Buffer 連續 {consecutive_failures} 筆失敗，放棄本輪重試")
                    break

        if success or skipped_old:
            logger.info(f"Buffer 重試完成：補寫 {success} 筆 / 過期丟棄 {skipped_old} 筆")

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

    def _transform_bus_intercity(self, result: dict, ts: datetime) -> list[dict]:
        # 欄位結構與 _transform_bus 一致，僅資料來源不同（InterCity API）
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

    # OD progress 快取（類別層級，所有實例共用）
    _od_progress_cache = None
    _track_index_cache = None

    def _load_od_progress(self):
        """載入 OD station progress（帶快取）"""
        if SupabaseWriter._od_progress_cache is not None:
            return SupabaseWriter._od_progress_cache, SupabaseWriter._track_index_cache

        # 嘗試從 S3 載入
        s3_prefix = getattr(config, 'MINI_TAIPEI_S3_PREFIX', 'mini-taipei')
        s3_key = f"{s3_prefix}/tra/od_station_progress.json"
        try:
            from storage.s3 import S3Storage
            s3 = S3Storage()
            data = s3.get_json(s3_key)
            if data:
                SupabaseWriter._od_progress_cache = data
                SupabaseWriter._track_index_cache = build_track_index(data)
                logger.info(f"從 S3 載入 od_station_progress: {len(data)} 條軌道")
                return data, SupabaseWriter._track_index_cache
        except Exception as e:
            logger.warning(f"從 S3 載入 od_station_progress 失敗: {e}")

        # 嘗試從本地 cache 載入
        cache_path = config.LOCAL_DATA_DIR / 'mini_taipei_cache' / 'od_station_progress.json'
        if cache_path.exists():
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            SupabaseWriter._od_progress_cache = data
            SupabaseWriter._track_index_cache = build_track_index(data)
            logger.info(f"從本地 cache 載入 od_station_progress: {len(data)} 條軌道")
            return data, SupabaseWriter._track_index_cache

        raise RuntimeError(
            f"找不到 od_station_progress.json。"
            f"請上傳到 S3: {s3_key}，"
            f"或放置到: {cache_path}"
        )

    def _transform_rail_timetable(self, result: dict, ts: datetime) -> list[dict]:
        """時刻表：轉換為 mini-taipei 格式後寫入 reference.daily_schedules"""
        data = result.get('data', {})
        if isinstance(data, list):
            return []

        today = ts.strftime('%Y-%m-%d')
        records = []

        # --- TRA：轉換為 mini-taipei 格式 ---
        tra_data = data.get('tra', {})
        if tra_data and tra_data.get('data'):
            tra_raw = tra_data['data']
            try:
                od_progress, track_index = self._load_od_progress()
                tra_output, _coverage = convert_tra_timetable(
                    tra_raw, today, track_index, od_progress
                )
                records.append({
                    '_system': 'tra_daily',
                    '_schedule_date': today,
                    '_train_count': tra_output['metadata']['total_trains'],
                    '_data': json.dumps(tra_output, ensure_ascii=False, default=str),
                })
                logger.info(
                    f"[rail_timetable] TRA 轉換成功: "
                    f"{tra_output['metadata']['total_trains']} 班 "
                    f"(失敗 {tra_output['metadata']['failed']})"
                )
            except Exception as e:
                logger.warning(f"[rail_timetable] TRA 轉換失敗，fallback 原始格式: {e}")
                records.append({
                    '_system': 'tra',
                    '_schedule_date': today,
                    '_train_count': tra_data.get('train_count', len(tra_raw)),
                    '_data': json.dumps(tra_raw, ensure_ascii=False, default=str),
                })

        # --- THSR：轉換為 mini-taipei 格式 ---
        thsr_data = data.get('thsr', {})
        if thsr_data and thsr_data.get('data'):
            thsr_raw = thsr_data['data']
            try:
                thsr_output = convert_thsr_timetable(thsr_raw, today)
                records.append({
                    '_system': 'thsr_daily',
                    '_schedule_date': today,
                    '_train_count': thsr_output['_metadata']['total_trains'],
                    '_data': json.dumps(thsr_output, ensure_ascii=False, default=str),
                })
                logger.info(
                    f"[rail_timetable] THSR 轉換成功: "
                    f"{thsr_output['_metadata']['total_trains']} 班"
                )
            except Exception as e:
                logger.warning(f"[rail_timetable] THSR 轉換失敗，fallback 原始格式: {e}")
                records.append({
                    '_system': 'thsr',
                    '_schedule_date': today,
                    '_train_count': thsr_data.get('train_count', len(thsr_raw)),
                    '_data': json.dumps(thsr_raw, ensure_ascii=False, default=str),
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

    def _transform_ncdr_alerts(self, result: dict, ts: datetime) -> list[dict]:
        """NCDR 災害示警：直接展平，identifier 為 PK"""
        records = []
        for r in result.get('data', []):
            records.append({
                'identifier': r.get('identifier'),
                'sender': r.get('sender'),
                'sender_name': r.get('sender_name'),
                'author': r.get('author'),
                'category': r.get('category'),
                'event': r.get('event'),
                'event_term': r.get('event_term'),
                'urgency': r.get('urgency'),
                'severity': r.get('severity'),
                'certainty': r.get('certainty'),
                'status': r.get('status'),
                'msg_type': r.get('msg_type'),
                'scope': r.get('scope'),
                'headline': r.get('headline'),
                'description': r.get('description'),
                'instruction': r.get('instruction'),
                'area_desc': r.get('area_desc'),
                'geocodes': r.get('geocodes'),
                'sent': r.get('sent'),
                'effective': r.get('effective'),
                'onset': r.get('onset'),
                'expires': r.get('expires'),
                'cap_url': r.get('cap_url'),
                'feed_title': r.get('feed_title'),
                'feed_summary': r.get('feed_summary'),
                'geom': r.get('geom'),
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_cwa_satellite(self, result: dict, ts: datetime) -> list[dict]:
        """CWA 衛星雲圖 / 雷達影像
        每筆 record = 一張影像。collector 用 base64 傳輸（JSON-safe），
        這邊 decode 回 bytes。PRIMARY KEY (dataset_id, observed_at) 天然去重。
        """
        import base64 as _b64
        records = []
        for f in result.get('data', []):
            b64 = f.get('image_b64')
            if not b64:
                continue
            png = _b64.b64decode(b64)
            records.append({
                'dataset_id': f.get('dataset_id'),
                'observed_at': f.get('observed_at'),
                'image_bytes': PgBinary(png),
                'mime_type': f.get('mime_type', 'image/png'),
                'lon_min': f.get('lon_min'),
                'lon_max': f.get('lon_max'),
                'lat_min': f.get('lat_min'),
                'lat_max': f.get('lat_max'),
                'width': f.get('width'),
                'height': f.get('height'),
                'image_size': f.get('image_size'),
                'product_url': f.get('product_url'),
                'resource_desc': f.get('resource_desc'),
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_foursquare_poi(self, result: dict, ts: datetime) -> list[dict]:
        """Foursquare OS Places POI（collect 已完成清洗，直接映射欄位）"""
        records = []
        for r in result.get('data', []):
            lat = r.get('latitude')
            lng = r.get('longitude')
            geom = f'SRID=4326;POINT({lng} {lat})' if lat and lng else None

            # fsq_category_ids 轉 PostgreSQL array 格式
            cat_ids = r.get('fsq_category_ids', [])
            pg_array = '{' + ','.join(f'"{c}"' for c in cat_ids) + '}' if cat_ids else None

            props = r.get('properties', {})
            props_json = json.dumps(props, ensure_ascii=False) if props else '{}'

            records.append({
                'fsq_place_id': r['fsq_place_id'],
                'name': r.get('name'),
                'category': r.get('category', '其他'),
                'subcategory': r.get('subcategory'),
                'city': r.get('city'),
                'district': r.get('district'),
                'address': r.get('address'),
                'geom': geom,
                'tel': r.get('tel'),
                'website': r.get('website'),
                'fsq_category_ids': pg_array,
                'date_refreshed': r.get('date_refreshed'),
                'date_closed': r.get('date_closed'),
                'properties': props_json,
                'imported_at': ts.isoformat(),
            })
        return records

    TRANSFORMERS = {
        'youbike': _transform_youbike,
        'bus': _transform_bus,
        'bus_intercity': _transform_bus_intercity,
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
        'ncdr_alerts': _transform_ncdr_alerts,
        'cwa_satellite': _transform_cwa_satellite,
        'foursquare_poi': _transform_foursquare_poi,
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
        'bus_intercity': {
            'history': 'realtime.bus_intercity_positions',
            'current': 'realtime.bus_intercity_current',
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
        'ncdr_alerts': {
            'history': 'realtime.disaster_alerts',
            'columns': [
                'identifier', 'sender', 'sender_name', 'author', 'category', 'event', 'event_term',
                'urgency', 'severity', 'certainty', 'status', 'msg_type', 'scope',
                'headline', 'description', 'instruction', 'area_desc', 'geocodes',
                'sent', 'effective', 'onset', 'expires', 'cap_url',
                'feed_title', 'feed_summary', 'geom', 'collected_at',
            ],
            'upsert_key': 'identifier',
        },
        'cwa_satellite': {
            'history': 'realtime.cwa_imagery_frames',
            'columns': [
                'dataset_id', 'observed_at', 'image_bytes', 'mime_type',
                'lon_min', 'lon_max', 'lat_min', 'lat_max',
                'width', 'height', 'image_size', 'product_url', 'resource_desc',
                'collected_at',
            ],
            # 複合 PK (dataset_id, observed_at)：用 ON CONFLICT DO NOTHING
            # 因為同一觀測時間的 PNG 內容固定，重複抓不需更新
            'upsert_key': 'dataset_id,observed_at',
            'upsert_strategy': 'do_nothing',
        },
        'foursquare_poi': {
            'history': 'reference.foursquare_poi',
            'columns': [
                'fsq_place_id', 'name', 'category', 'subcategory',
                'city', 'district', 'address', 'geom',
                'tel', 'website', 'fsq_category_ids',
                'date_refreshed', 'date_closed', 'properties', 'imported_at',
            ],
            'upsert_key': 'fsq_place_id',
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
                    key = table_config['upsert_key']  # 支援複合鍵 'a,b'
                    if table_config.get('upsert_strategy') == 'do_nothing':
                        sql = f"INSERT INTO {table_config['history']} ({col_names}) VALUES %s ON CONFLICT ({key}) DO NOTHING"
                    else:
                        key_set = {k.strip() for k in key.split(',')}
                        update_cols = [c for c in columns if c not in key_set]
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

            # 同步寫入 TLE 歷史表（用於變軌偵測）
            self._write_satellite_tle_history(values, timestamp)

    def _write_satellite_tle_history(self, tle_values: list, timestamp: datetime):
        """追加 TLE 歷史紀錄，同一 norad_id + tle_epoch 不重複寫入"""
        hist_cols = ['norad_id', 'name', 'constellation', 'orbit_type',
                     'tle_line1', 'tle_line2', 'tle_epoch',
                     'inclination', 'eccentricity', 'period_min', 'fetched_at']

        # 從 tle_values 重新組合（原始順序：norad_id, name, intl_designator, constellation, orbit_type,
        #   tle_line1, tle_line2, tle_epoch, inclination, eccentricity, period_min, updated_at）
        hist_values = [
            (v[0], v[1], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], timestamp.isoformat())
            for v in tle_values
        ]

        try:
            with self.conn.cursor() as cur:
                sql = (f"INSERT INTO realtime.satellite_tle_history ({','.join(hist_cols)}) "
                       f"VALUES %s ON CONFLICT (norad_id, tle_epoch) DO NOTHING")
                execute_values(cur, sql, hist_values, page_size=1000)
            logger.info(f"[satellite] ✓ TLE 歷史已追加（新 epoch 才寫入）")
        except Exception as e:
            logger.warning(f"[satellite] TLE 歷史寫入失敗（表可能尚未建立）: {e}")

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
