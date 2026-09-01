"""
Supabase 即時資料寫入模組

主路徑寫 DB（分區表 + current 表），失敗時暫存 buffer，定期重試。
使用 psycopg2 連線池（SupabaseConnectionPool），每個 collector 借/還連線，
死連線只影響當下借它的 collector，其他人不受波及。

歷史背景（事故 2026-06-26）：
舊版用「單條 conn + RLock」，連線 wedge（TCP 沒斷但 server 不回）時所有
collector 在 RLock 後排隊 3 小時。改 pool + borrow timeout 後，借不到就
fall-back 到 buffer，collector 繼續收。
"""

import json
import logging
import threading
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from psycopg2 import Binary as PgBinary
from psycopg2.extras import Json, execute_values

import config
from storage.db import SupabaseConnectionPool, PoolBorrowTimeout, PoolBreakerOpen
from storage.supabase_tables import TABLE_MAP
from utils.notify import send_telegram, _escape_md, _instance_tag
from tasks.mini_taipei_publish import (
    build_track_index,
    convert_tra_timetable,
    convert_thsr_timetable,
)

logger = logging.getLogger(__name__)

BUFFER_DIR = config.LOCAL_DATA_DIR / 'buffer'


def _taipei_today() -> date:
    """目前台北（Asia/Taipei，UTC+8，全年無 DST）日期。

    獨立成函式方便測試 monkeypatch。History dedup 的每日 heartbeat
    刻意用純時間比較判斷、不查 DB。
    """
    from datetime import timedelta, timezone
    return datetime.now(timezone(timedelta(hours=8))).date()


class SupabaseWriter:
    """統一的 Supabase 寫入介面（連線池版本）。

    架構：
    - self._pool: SupabaseConnectionPool — 借/還連線，含 borrow timeout + 斷路器
    - self._err_lock: threading.Lock — 只保護 _db_consecutive_errors dict，短臨界區
    - 沒有共用 RLock；每個 write/flush 各自借自己的 conn，互不卡

    對 collector 的契約：
    - 寫 DB 失敗（含 borrow timeout、breaker open）→ 自動 fallback 到 buffer
    - collector.run() 不會被 DB 問題卡住，永遠能繼續 collect/save local/buffer
    """

    # DB 寫入連續錯誤追蹤（跨 collector 共用）
    _db_consecutive_errors: dict[str, int] = {}
    _DB_ERROR_ALERT_THRESHOLD = 3  # 連續 N 次失敗才告警（避免瞬時錯誤洗版）

    # History dedup 的每日 heartbeat：collector_name -> 上次全量寫入 history 的
    # 台北日期。同一台北日只需第一輪全量，其餘走正常 dedup；跨到新的一天
    # 再全量一次 —— 保證「長期不變的 row」不會因為 dedup + 有限 lookback
    # 窗補不到值，讓下游整天顯示 '-'。collector 重啟會清空這個 dict，
    # 下一輪多做一次全量寫，無害。
    _history_dedup_heartbeat_date: dict[str, date] = {}

    def __init__(self, database_url: str):
        self.database_url = database_url
        # 連線池：取代舊單條 conn + RLock。borrow timeout / 斷路器都在 pool 內。
        self._pool = SupabaseConnectionPool()
        # 只保護 _db_consecutive_errors dict 的 increment/reset 短臨界區
        self._err_lock = threading.Lock()
        BUFFER_DIR.mkdir(parents=True, exist_ok=True)

    def health_snapshot(self) -> dict:
        """回傳連線健康狀態（無副作用，不借連線）。供 /health 端點查詢。

        保留舊欄位名稱 (connect_failures / breaker_open) 不破壞 API 相容。
        """
        snap = self._pool.snapshot()
        return {
            "connected": snap["pool_initialized"] and not snap["breaker_open"],
            "connect_failures": snap["connect_failures"],
            "breaker_open": snap["breaker_open"],
            "pool_min": snap["minconn"],
            "pool_max": snap["maxconn"],
        }

    def with_conn(self, timeout: Optional[float] = None):
        """Public API：借一條連線，with 區塊結束自動歸還。

        Usage:
            with writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(...)

        供 collector 直接寫自訂 SQL 用（例：news_events 的 dedup query、
        wra_drought_alert 的查詢、collect() 階段需要查 reference 表的情境）。
        取代以前直接讀 self.supabase_writer.conn 的 thread-unsafe 寫法。

        Raises:
            PoolBorrowTimeout: 借不到（pool 全 busy）
            PoolBreakerOpen: 斷路器開啟中
        """
        return self._pool.borrow(timeout=timeout)

    @contextmanager
    def _txn(self, conn):
        """開一個顯式 transaction，第一件事就是 SET LOCAL statement_timeout，
        再 yield cursor 給呼叫端做寫入，成功 commit / 失敗 rollback。

        為什麼不靠連線 startup options / session-level SET
        （AR-06 實測 2026-07-02，連的是 Supavisor transaction mode pooler，port 6543）：
        - startup `options=-c statement_timeout` 會被 pooler 丟棄 → SHOW = 0、
          pg_sleep(35) 完整跑完不被砍（保護形同虛設）。
        - session-level SET 只在 backend 沒被換掉時有效；transaction mode 下每個
          transaction 可能落到不同 backend，SET 會遺失 → 不可靠。
        - 唯一可靠：SET LOCAL（set_config is_local=true）與工作語句在【同一個
          transaction】，pooler 保證同一 backend 直到 commit → timeout 一定生效。

        每個寫入單元各開一個 _txn（對齊原本每個 `with conn.cursor()` 區塊的粒度），
        因為 SET LOCAL 在 commit 後即失效，必須逐 transaction 重下。
        """
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                # set_config(name, value, is_local=true) 等同 SET LOCAL；
                # statement_timeout 吃裸整數 = 毫秒。
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(self._pool.statement_timeout_ms),),
                )
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            # 還原給 pool / 其它 best-effort 讀取用（borrow 也會再設一次）
            conn.autocommit = True

    # ============================================================
    # 主要寫入介面
    # ============================================================

    def write(self, collector_name: str, result: dict, timestamp: datetime):
        """主路徑寫 DB，失敗時暫存 buffer。

        Thread-safe：transformer 是純函數；DB 寫入透過 pool 借連線；錯誤計數
        用短 lock 保護。沒有阻塞點。
        """
        try:
            # Transform 是純函數，不需要 conn / lock
            records = self._transform(collector_name, result, timestamp)
            if not records:
                return True

            # 借一條連線，把所有 DB 動作（含衛星 TLE 額外表）跑完才還
            with self._pool.borrow() as conn:
                self._write_to_db(conn, collector_name, records, timestamp)
                if collector_name == 'satellite':
                    self._write_satellite_tle(conn, result, timestamp)

                # heartbeat 共用主寫入這條 conn，省一次 borrow + pre-ping
                # （每次成功寫入原本要借兩條連線，尖峰時是 pool 壓力來源之一）。
                # 主寫入的 _txn 已各自 commit，這裡開的是【獨立 transaction】：
                # 心跳失敗只會 rollback 心跳本身，絕不影響已寫入的資料；
                # 例外在 _report_heartbeat 內吞掉（best-effort 語義不變）。
                self._report_heartbeat(collector_name, True, len(records), conn=conn)

            # 連續錯誤計數重置 + 恢復通知
            with self._err_lock:
                prev_errors = self._db_consecutive_errors.get(collector_name, 0)
                self._db_consecutive_errors[collector_name] = 0
            if prev_errors >= self._DB_ERROR_ALERT_THRESHOLD:
                tag = _instance_tag()
                send_telegram(
                    f"✅ *DB 寫入恢復*{tag}\n\n"
                    f"收集器: `{collector_name}`\n"
                    f"之前連續失敗: {prev_errors} 次"
                )
            return True

        except (PoolBorrowTimeout, PoolBreakerOpen) as e:
            # 池滿 / 斷路器 — 都是「DB 暫時不可用」的訊號。不是 bug，不要 Telegram 洗版。
            logger.warning(f"[{collector_name}] DB 暫時不可用，暫存 buffer: {e}")
            if collector_name != 'gfw_vessel_presence' or config.GFW_RAW_ARCHIVE_ENABLED:
                self._write_to_buffer(collector_name, result, timestamp)
            self._report_heartbeat(collector_name, False, 0, str(e))
            self._record_db_error(collector_name, e)
            return False

        except Exception as e:
            logger.warning(f"[{collector_name}] DB 寫入失敗，暫存 buffer: {e}")
            if collector_name != 'gfw_vessel_presence' or config.GFW_RAW_ARCHIVE_ENABLED:
                self._write_to_buffer(collector_name, result, timestamp)
            self._report_heartbeat(collector_name, False, 0, str(e))
            self._record_db_error(collector_name, e)
            return False

    def _record_db_error(self, collector_name: str, err: Exception) -> None:
        """累計連續錯誤，到達閾值才發 Telegram（避免洗版）。"""
        with self._err_lock:
            self._db_consecutive_errors[collector_name] = self._db_consecutive_errors.get(collector_name, 0) + 1
            count = self._db_consecutive_errors[collector_name]
        if count == self._DB_ERROR_ALERT_THRESHOLD:
            tag = _instance_tag()
            send_telegram(
                f"🗄️ *DB 寫入連續失敗*{tag}\n\n"
                f"收集器: `{collector_name}`\n"
                f"連續失敗: *{count} 次*\n"
                f"錯誤: {_escape_md(str(err)[:200])}\n\n"
                f"資料已暫存 buffer，待問題修復後自動補回"
            )

    def flush_buffer(self):
        """重試 buffer 中的資料。一次借一條連線跑完整批，期間其他 collector 用別條 conn 不受影響。"""
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

        try:
            with self._pool.borrow() as conn:
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
                            self._write_to_db(conn, payload['collector'], records, ts)
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
        except (PoolBorrowTimeout, PoolBreakerOpen) as e:
            logger.warning(f"Buffer flush 跳過：DB 暫時不可用 ({e})")
            return

        if success or skipped_old:
            logger.info(f"Buffer 重試完成：補寫 {success} 筆 / 過期丟棄 {skipped_old} 筆")

    # Buffer 檔最大保留天數：超過則直接丟棄
    # 因為分區表 retention 會自動刪舊分區，過期 buffer 已無處可寫，且會永久卡住其他檔案
    BUFFER_MAX_AGE_DAYS = 3

    # 連續失敗多少筆後判定 DB 仍不可用、放棄本輪
    BUFFER_FAIL_THRESHOLD = 5

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

    def _transform_marine_observation(self, result: dict, ts: datetime) -> list[dict]:
        """Collectors already enforce the canonical long-form marine contract."""
        return [dict(row) for row in result.get("data", []) if isinstance(row, dict)]

    def _transform_internet_health(self, result: dict, ts: datetime) -> list[dict]:
        """Provider collectors already emit the shared canonical contract."""
        return [dict(row) for row in result.get("data", []) if isinstance(row, dict)]

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

    def _transform_road_congestion(self, result: dict, ts: datetime) -> list[dict]:
        """省道+市區路況 → live.road_sections_live/_current"""
        import json
        records = []
        for r in result.get('data', []):
            ds = r.get('data_sources')
            if ds is not None and not isinstance(ds, str):
                ds = json.dumps(ds, ensure_ascii=False)
            records.append({
                'section_uid': r.get('section_uid', ''),
                'section_id': r.get('section_id', ''),
                'source': r.get('source'),
                'city': r.get('city'),
                'authority_code': r.get('authority_code'),
                'travel_time': r.get('travel_time'),
                'travel_speed': r.get('travel_speed'),
                'congestion_level': r.get('congestion_level'),
                'congestion_level_id': r.get('congestion_level_id'),
                'data_sources': ds,
                'data_collect_time': r.get('data_collect_time'),
                'collected_at': ts.isoformat(),
            })
        seen = {}
        for r in records:
            if r['section_uid']:
                seen[r['section_uid']] = r
        return list(seen.values())

    def _transform_parking(self, result: dict, ts: datetime) -> list[dict]:
        """OnStreet 路邊停車 → live.parking_segments_availability/_current
        資料來自 collectors/parking.py 的 _parse_segment（已 normalize），這邊只做 JSONB 化 + 去重。
        """
        import json
        records = []
        for r in result.get('data', []):
            space_types = r.get('space_types')
            if space_types is not None and not isinstance(space_types, str):
                space_types = json.dumps(space_types, ensure_ascii=False)
            records.append({
                'segment_id': r.get('segment_id', ''),
                'segment_name': r.get('segment_name', ''),
                'city': r.get('_city', ''),
                'total_spaces': r.get('total_spaces'),
                'available_spaces': r.get('available_spaces'),
                'occupancy': r.get('occupancy'),
                'full_status': r.get('full_status'),
                'service_status': r.get('service_status'),
                'charge_status': r.get('charge_status'),
                'space_types': space_types,
                'data_collect_time': r.get('data_collect_time'),
                'collected_at': ts.isoformat(),
            })
        # 去重：segment_id 同批次內若重複保留最後
        seen = {}
        for r in records:
            if r['segment_id']:
                seen[r['segment_id']] = r
        return list(seen.values())

    def _transform_parking_offstreet(self, result: dict, ts: datetime) -> list[dict]:
        """OffStreet 路外場館 → live.parking_lots_availability/_current"""
        import json
        records = []
        for r in result.get('data', []):
            space_types = r.get('space_types')
            if space_types is not None and not isinstance(space_types, str):
                space_types = json.dumps(space_types, ensure_ascii=False)
            records.append({
                'car_park_uid': r.get('car_park_uid', ''),
                'car_park_id': r.get('car_park_id', ''),
                'car_park_name': r.get('car_park_name', ''),
                'source_category': r.get('source_category'),
                'authority_code': r.get('authority_code'),
                'sub_category': r.get('sub_category'),
                'total_spaces': r.get('total_spaces'),
                'available_spaces': r.get('available_spaces'),
                'full_status': r.get('full_status'),
                'service_status': r.get('service_status'),
                'charge_status': r.get('charge_status'),
                'space_types': space_types,
                'data_collect_time': r.get('data_collect_time'),
                'collected_at': ts.isoformat(),
            })
        seen = {}
        for r in records:
            if r['car_park_uid']:
                seen[r['car_park_uid']] = r
        return list(seen.values())

    def _transform_tourist_shuttle(self, result: dict, ts: datetime) -> list[dict]:
        """台灣好行 A1 → live.tourist_shuttle_positions/_current"""
        records = []
        for r in result.get('data', []):
            pos = r.get('BusPosition') or {}
            sub_route = r.get('SubRouteName') or {}
            taiwan_trip = r.get('TaiwanTripName') or {}
            records.append({
                'plate_numb': r.get('PlateNumb', ''),
                'operator_id': str(r.get('OperatorID', '')),
                'route_uid': r.get('RouteUID', ''),
                'sub_route_uid': r.get('SubRouteUID', ''),
                'sub_route_name': sub_route.get('Zh_tw', '') if isinstance(sub_route, dict) else str(sub_route),
                'taiwan_trip_name': taiwan_trip.get('Zh_tw', '') if isinstance(taiwan_trip, dict) else str(taiwan_trip),
                'direction': r.get('Direction', 0),
                'lat': pos.get('PositionLat') if isinstance(pos, dict) else None,
                'lng': pos.get('PositionLon') if isinstance(pos, dict) else None,
                'speed': r.get('Speed'),
                'azimuth': r.get('Azimuth'),
                'gps_time': r.get('GPSTime'),
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

    def _transform_gfw_vessel_presence(self, result: dict, ts: datetime) -> list[dict]:
        """GFW collector 已完成 defensive normalization；僅保留 multi-table records。"""
        records = []
        for row in result.get('data', []):
            if not isinstance(row, dict) or row.get('_type') not in ('run', 'snapshot'):
                continue
            records.append(row)
        return records

    def _transform_earthquake(self, result: dict, ts: datetime) -> list[dict]:
        """地震：事件 + 逐站觀測 + 海嘯，攤平為單一 list 由 _write_multi_table 依 _type 分派

        相容性：2026-07 之前的 buffer 檔還帶 'catalog' key（目錄那時混在本 collector），
        補寫時仍照舊寫進 earthquake_events（report_type='catalog'）。
        """
        data = result.get('data', {})
        if isinstance(data, list):
            return []

        records = []

        # 1) 有感地震事件
        for r in data.get('felt_reports', []):
            lat = r.get('epicenter_latitude')
            lng = r.get('epicenter_longitude')
            records.append({
                '_type': 'event',
                # 舊 buffer 沒有 event_id → 退回 earthquake_no（維持可補寫）
                'event_id': r.get('event_id') or str(r.get('earthquake_no', '')),
                'magnitude': r.get('magnitude_value'),
                'depth_km': r.get('focal_depth_km'),
                'epicenter_lat': lat,
                'epicenter_lng': lng,
                'location_desc': r.get('epicenter_location', ''),
                'occurred_at': r.get('origin_time', ts.isoformat()),
                'report_type': r.get('source_type', 'felt'),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
                'raw_data': json.dumps(r, ensure_ascii=False, default=str),
            })

        # 2) 逐測站觀測
        for r in data.get('station_obs', []):
            lat = r.get('lat')
            lon = r.get('lon')
            records.append({
                '_type': 'station',
                'event_id': r.get('event_id'),
                'earthquake_no': r.get('earthquake_no'),
                'origin_time': r.get('origin_time'),
                'source_type': r.get('source_type'),
                'station_id': r.get('station_id'),
                'station_name': r.get('station_name'),
                'county_name': r.get('county_name'),
                'area_desc': r.get('area_desc'),
                'area_intensity': r.get('area_intensity'),
                'lat': lat,
                'lon': lon,
                'epicenter_distance_km': r.get('epicenter_distance_km'),
                'back_azimuth': r.get('back_azimuth'),
                'seismic_intensity': r.get('seismic_intensity'),
                'intensity_value': r.get('intensity_value'),
                'pga_ew': r.get('pga_ew'), 'pga_ns': r.get('pga_ns'),
                'pga_v': r.get('pga_v'), 'pga_int': r.get('pga_int'),
                'pgv_ew': r.get('pgv_ew'), 'pgv_ns': r.get('pgv_ns'),
                'pgv_v': r.get('pgv_v'), 'pgv_int': r.get('pgv_int'),
                'wave_image_uri': r.get('wave_image_uri') or None,
                'geom': f'SRID=4326;POINT({lon} {lat})' if lat is not None and lon is not None else None,
                'collected_at': ts.isoformat(),
            })

        # 3) 海嘯報告
        for r in data.get('tsunami', []):
            lat = r.get('epicenter_lat')
            lon = r.get('epicenter_lon')
            records.append({
                '_type': 'tsunami',
                'tsunami_no': r.get('tsunami_no'),
                'report_no': r.get('report_no'),
                'report_type': r.get('report_type'),
                'report_color': r.get('report_color'),
                'report_content': r.get('report_content'),
                'issued_at': r.get('issued_at'),
                'valid_end_at': r.get('valid_end_at'),
                'origin_time': r.get('origin_time'),
                'source': r.get('source'),
                'epicenter_location': r.get('epicenter_location'),
                'epicenter_lat': lat,
                'epicenter_lon': lon,
                'focal_depth_km': r.get('focal_depth_km'),
                'magnitude': r.get('magnitude'),
                'web_url': r.get('web_url'),
                'station_details': (
                    json.dumps(r['station_details'], ensure_ascii=False, default=str)
                    if r.get('station_details') is not None else None
                ),
                'raw': json.dumps(r.get('raw'), ensure_ascii=False, default=str),
                'geom': f'SRID=4326;POINT({lon} {lat})' if lat is not None and lon is not None else None,
                'collected_at': ts.isoformat(),
            })

        # 4) 舊格式 buffer 的完整目錄（新流程已搬到 earthquake_catalog collector）
        for r in data.get('catalog', []):
            lat = r.get('latitude')
            lng = r.get('longitude')
            records.append({
                '_type': 'event',
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

    def _transform_earthquake_catalog(self, result: dict, ts: datetime) -> list[dict]:
        """完整地震目錄（含無感）→ live.earthquake_events，report_type='catalog'"""
        records: list[dict] = []
        for r in result.get('data', []):
            lat = r.get('latitude')
            lng = r.get('longitude')
            records.append({
                'event_id': r.get('event_id') or f"cat_{r.get('origin_time', '')}_{lat}_{lng}",
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

    def _transform_earthquake_town_intensity(self, result: dict, ts: datetime) -> list[dict]:
        """鄉鎮震度：collector 已產出 JSON-safe dict；補 geom WKT。"""
        records: list[dict] = []
        for r in result.get('data', []):
            lat = r.get('lat')
            lon = r.get('lon')
            records.append({
                'origin_time':          r.get('origin_time'),
                'report_id':            r.get('report_id'),
                'earthquake_no':        r.get('earthquake_no'),
                'magnitude':            r.get('magnitude'),
                'depth_km':             r.get('depth_km'),
                'epicenter_lat':        r.get('epicenter_lat'),
                'epicenter_lon':        r.get('epicenter_lon'),
                'county_name':          r.get('county_name'),
                'county_code':          r.get('county_code'),
                'county_max_intensity': r.get('county_max_intensity'),
                'town_name':            r.get('town_name'),
                'town_code':            r.get('town_code'),
                'lat':                  lat,
                'lon':                  lon,
                'intensity':            r.get('intensity'),
                'intensity_value':      r.get('intensity_value'),
                'geom': f'SRID=4326;POINT({lon} {lat})' if lat is not None and lon is not None else None,
                'collected_at':         ts.isoformat(),
            })
        return records

    def _transform_earthquake_shakemap_grid(self, result: dict, ts: datetime) -> list[dict]:
        """NCDR 2.5km 網格 shakemap：補 geom WKT（含 PGA=0 的外海格，前端要完整面）。"""
        records: list[dict] = []
        for r in result.get('data', []):
            lat = r.get('lat')
            lon = r.get('lon')
            if lat is None or lon is None:
                continue
            records.append({
                'event_name':   r.get('event_name'),
                'event_time':   r.get('event_time'),
                'magnitude':    r.get('magnitude'),
                'eq_lon':       r.get('eq_lon'),
                'eq_lat':       r.get('eq_lat'),
                'depth':        r.get('depth'),
                'lon':          lon,
                'lat':          lat,
                'pga':          r.get('pga'),
                'pgv':          r.get('pgv'),
                'intensity':    r.get('intensity'),
                'geom':         f'SRID=4326;POINT({lon} {lat})',
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_earthquake_moment_tensor(self, result: dict, ts: datetime) -> list[dict]:
        """AutoBATS 震源機制解：tensor / raw 已是 JSON 字串；補 geom WKT。"""
        records: list[dict] = []
        for r in result.get('data', []):
            lat = r.get('lat')
            lon = r.get('lon')
            records.append({
                'origin_time_utc':   r.get('origin_time_utc'),
                'origin_time_local': r.get('origin_time_local'),
                'event_id':          r.get('event_id'),
                'lat':               lat,
                'lon':               lon,
                'ml':                r.get('ml'),
                'mw':                r.get('mw'),
                'm0':                r.get('m0'),
                'strike1':           r.get('strike1'),
                'dip1':              r.get('dip1'),
                'rake1':             r.get('rake1'),
                'strike2':           r.get('strike2'),
                'dip2':              r.get('dip2'),
                'rake2':             r.get('rake2'),
                'centroid_depth':    r.get('centroid_depth'),
                'cwb_depth':         r.get('cwb_depth'),
                'clvd_pct':          r.get('clvd_pct'),
                'iso_pct':           r.get('iso_pct'),
                'misfit':            r.get('misfit'),
                'gap':               r.get('gap'),
                'nsta':              r.get('nsta'),
                'quality':           r.get('quality'),
                'tensor':            r.get('tensor'),
                'solution_type':     r.get('solution_type'),
                'beachball_url':     r.get('beachball_url'),
                'raw':               r.get('raw'),
                'geom': f'SRID=4326;POINT({lon} {lat})' if lat is not None and lon is not None else None,
                'collected_at':      ts.isoformat(),
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
                # include_raw_stops=True：DB 這條路徑是統計用途，需要未經
                # station_id 映射／軌道過濾的完整停靠序列（stations_raw）。
                # S3 給 3D 前端的那條路徑不傳，輸出保持不變。
                tra_output, _coverage = convert_tra_timetable(
                    tra_raw, today, track_index, od_progress, include_raw_stops=True
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

    def _transform_road_event(self, result: dict, ts: datetime) -> list[dict]:
        """TDX RoadEvent → road_events 列（共用於 live + planned）

        Schema 對齊 gis-platform/migrations/078_road_events.sql。
        幾何處理：優先用 Geometry（多邊形/活動範圍），否則用 Positions（POINT）。
        """
        from shapely import wkt as shp_wkt
        from shapely.errors import GEOSException
        import json

        records = []
        for ev in result.get('data', []):
            # 幾何 — Positions (POINT) / Geometry (MULTIPOLYGON) 二選一
            wkt_str = ev.get('Geometry') or ev.get('Positions')
            geom = None
            if wkt_str:
                try:
                    g = shp_wkt.loads(wkt_str)
                    if g and not g.is_empty:
                        geom = f'SRID=4326;{g.wkt}'
                except (GEOSException, Exception):
                    geom = None

            # Location 拆解
            loc = ev.get('Location') or {}
            feh = loc.get('FreeExpressHighway') or {}
            # Impact (僅 live_freeway 有完整)
            impact = ev.get('Impact') or {}
            regulations = impact.get('Regulations')
            if regulations is not None and not isinstance(regulations, str):
                regulations = json.dumps(regulations, ensure_ascii=False)

            # Enrich 結果（collector 端寫進 _enrich）
            enrich = ev.get('_enrich') or {}

            records.append({
                'event_id': ev.get('EventID'),
                'source': ev.get('_source'),
                'event_type': ev.get('EventType'),
                'event_subtype': ev.get('EventSubType'),
                'event_step': ev.get('EventStep'),
                'severity': impact.get('Severity'),
                'road_name': feh.get('Road'),
                'road_class': ev.get('LocationType'),
                'direction': feh.get('Direction'),
                'start_km': enrich.get('start_km'),
                'end_km': enrich.get('end_km'),
                'blocked_lanes': impact.get('BlockedLanes'),
                'regulations': regulations,
                'block_way': impact.get('BlockWay'),
                'impact_description': impact.get('Description'),
                'title': ev.get('EventTitle'),
                'description': ev.get('Description'),
                'location_other': loc.get('Other'),
                'geom': geom,
                'effective_time': ev.get('EffectiveTime') or None,
                'expire_time': ev.get('ExpireTime') or None,
                'published_at': ev.get('PublishTime') or None,
                'last_updated': ev.get('LastUpdateTime') or None,
                'collected_at': ts.isoformat(),
                'matched_section_id': enrich.get('matched_section_id'),
                'matched_section_name': enrich.get('matched_section_name'),
                'matched_road_id': enrich.get('matched_road_id'),
                'enrich_status': enrich.get('enrich_status'),
                'raw_json': json.dumps(ev, ensure_ascii=False),
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
                'image_key': f.get('image_key'),  # R2 CDN key（雙寫失敗/未設 → None）
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

    def _transform_air_quality_imagery(self, result: dict, ts: datetime) -> list[dict]:
        """airtw 空氣品質色階圖 PNG
        每筆 record = 一張影像；collector 用 base64 傳輸 JSON-safe。
        PRIMARY KEY (product_type, observed_at) 天然去重。
        """
        import base64 as _b64
        records = []
        for f in result.get('data', []):
            b64 = f.get('image_b64')
            if not b64:
                continue
            png = _b64.b64decode(b64)
            records.append({
                'product_type': f.get('product_type'),
                'observed_at': f.get('observed_at'),
                'image_bytes': PgBinary(png),
                'mime_type': f.get('mime_type', 'image/png'),
                'image_size': f.get('image_size'),
                'product_url': f.get('product_url'),
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_air_quality(self, result: dict, ts: datetime) -> list[dict]:
        """環境部 77 站即時空氣品質 (AQX_P_432)。"""
        records = []
        observed_at = result.get('observed_at') or ts.isoformat()
        for r in result.get('data', []):
            try:
                lat = float(r['latitude']) if r.get('latitude') is not None else None
                lng = float(r['longitude']) if r.get('longitude') is not None else None
            except (ValueError, TypeError):
                lat, lng = None, None
            station_id = r.get('siteid')
            if not station_id:
                continue
            # AQI 轉 smallint
            try:
                aqi = int(r['aqi']) if r.get('aqi') is not None else None
            except (ValueError, TypeError):
                aqi = None
            records.append({
                'station_id': str(station_id),
                'station_name': r.get('sitename'),
                'county': r.get('county'),
                'aqi': aqi,
                'pollutant': r.get('pollutant') or None,
                'status': r.get('status') or None,
                'pm25': r.get('pm25'),
                'pm10': r.get('pm10'),
                'o3': r.get('o3'),
                'o3_8hr': r.get('o3_8hr'),
                'no2': r.get('no2'),
                'so2': r.get('so2'),
                'co': r.get('co'),
                'co_8hr': r.get('co_8hr'),
                'nox': r.get('nox'),
                'no': r.get('no'),
                'pm25_avg': r.get('pm25_avg'),
                'pm10_avg': r.get('pm10_avg'),
                'so2_avg': r.get('so2_avg'),
                'wind_speed': r.get('wind_speed'),
                'wind_direction': r.get('wind_direc'),
                'observed_at': observed_at,
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})' if lat and lng else None,
            })
        return records

    def _transform_water_reservoir(self, result: dict, ts: datetime) -> list[dict]:
        """WRA 水庫即時水情。"""
        return result.get('data', [])

    def _transform_river_water_level(self, result: dict, ts: datetime) -> list[dict]:
        """WRA 河川即時水位（每 10 分鐘）。"""
        return result.get('data', [])

    def _transform_rain_gauge_realtime(self, result: dict, ts: datetime) -> list[dict]:
        """CWA 即時雨量站讀值（O-A0002-001，每 10 分鐘）。"""
        return result.get('data', [])

    def _transform_groundwater_level(self, result: dict, ts: datetime) -> list[dict]:
        """WRA 即時地下水水位（每 60 分鐘）。"""
        return result.get('data', [])

    def _transform_water_reservoir_daily_ops(self, result: dict, ts: datetime) -> list[dict]:
        """WRA 水庫每日營運狀況（41568，每日）。"""
        return result.get('data', [])

    def _transform_wra_drought_alert(self, result: dict, ts: datetime) -> list[dict]:
        """水情燈號 daily（HTML scrape，hash 去重後上來）。

        若 collector 偵測到上游未變動，records 會是空 list → 自然 skip。
        """
        return result.get('data', [])

    def _transform_iot_wra(self, result: dict, ts: datetime) -> list[dict]:
        """水利署 IoT 7 類站點即時感測讀值（每 60 分鐘）。

        collector 已在 collect() 結束前呼叫 _upsert_iot_wra_stations()
        將靜態 metadata 寫入 public.iot_wra_stations，
        這裡只回傳 measurements 以寫入 live.iot_wra_measurements。
        """
        return result.get('data', [])

    def _upsert_iot_wra_stations(self, rows: list[dict]) -> None:
        """upsert 靜態站點 metadata 到 public.iot_wra_stations。

        iot.wra 的 7 類站點共用同一張表，同一 UUID 可能出現在多種 station_type
        （例如河川站兼具閘門），因此 PK 為 (iow_station_id, station_type)。
        同批內去重防 ON CONFLICT DO UPDATE 的「單一命令不得影響同一行多次」錯誤。
        """
        if not rows:
            return
        cols = [
            'iow_station_id', 'station_id', 'station_type', 'name',
            'county_code', 'county_name', 'town_code', 'town_name',
            'basin_code', 'basin_name', 'admin_name',
            'hydro_station_type', 'lat', 'lng', 'updated_at',
        ]

        # 同批去重：以複合 PK 為 key，保留最後一筆
        dedup: dict[tuple, dict] = {}
        for r in rows:
            key = (r.get('iow_station_id'), r.get('station_type'))
            if key[0] and key[1]:
                dedup[key] = r

        values = [tuple(r.get(c) for c in cols) for r in dedup.values()]
        update_set = ', '.join(
            f"{c} = EXCLUDED.{c}"
            for c in cols
            if c not in ('iow_station_id', 'station_type')
        )
        sql = (
            f"INSERT INTO public.iot_wra_stations ({','.join(cols)}) VALUES %s "
            f"ON CONFLICT (iow_station_id, station_type) DO UPDATE SET {update_set}"
        )
        with self._pool.borrow() as conn:
            with self._txn(conn) as cur:
                execute_values(cur, sql, values, page_size=500)

    def _transform_precipitation_raster(self, result: dict, ts: datetime) -> list[dict]:
        """水利署累積雨量柵格圖（PNG → bytea 經 base64 transit）

        PK (cumulative_hours, observed_at) 天然去重；同 ts × ch 不重寫。
        Empty raster（API 未生成）仍存一筆 metadata，image_bytes = NULL。
        """
        import base64 as _b64
        records = []
        for f in result.get('data', []):
            b64 = f.get('image_bytes_b64')
            png = _b64.b64decode(b64) if b64 else None
            records.append({
                'cumulative_hours': f.get('cumulative_hours'),
                'observed_at':      f.get('observed_at'),
                'image_bytes':      PgBinary(png) if png else None,
                'mime_type':        'image/png',
                'image_size':       f.get('image_size'),
                'ul_lat':           f.get('ul_lat'),
                'ul_lng':           f.get('ul_lng'),
                'br_lat':           f.get('br_lat'),
                'br_lng':           f.get('br_lng'),
                'width_m':          f.get('width_m'),
                'height_m':         f.get('height_m'),
                'is_empty':         f.get('is_empty', False),
                'source_url':       f.get('source_url'),
                'collected_at':     ts.isoformat(),
            })
        return records

    def _transform_uswg(self, result: dict, ts: datetime) -> list[dict]:
        """都市淹水感知器即時讀值（每 10 分鐘）。

        collector 已在 collect() 結束前呼叫 _upsert_uswg_stations()
        將靜態 metadata 寫入 public.uswg_stations，
        這裡只回傳 measurements 以寫入 live.uswg_measurements。
        """
        return result.get('data', [])

    def _upsert_uswg_stations(self, rows: list[dict]) -> None:
        """upsert 靜態站點 metadata 到 public.uswg_stations。

        USWG PK 為單一 iow_station_id（不像 iot_wra 還有 station_type）。
        """
        if not rows:
            return
        cols = [
            'iow_station_id', 'station_id', 'name',
            'county_code', 'county_name', 'town_code', 'town_name',
            'admin_name', 'hydro_station_type', 'lat', 'lng', 'updated_at',
        ]

        # 同批去重
        dedup: dict[str, dict] = {}
        for r in rows:
            sid = r.get('iow_station_id')
            if sid:
                dedup[sid] = r

        values = [tuple(r.get(c) for c in cols) for r in dedup.values()]
        update_set = ', '.join(
            f"{c} = EXCLUDED.{c}"
            for c in cols
            if c != 'iow_station_id'
        )
        sql = (
            f"INSERT INTO public.uswg_stations ({','.join(cols)}) VALUES %s "
            f"ON CONFLICT (iow_station_id) DO UPDATE SET {update_set}"
        )
        with self._pool.borrow() as conn:
            with self._txn(conn) as cur:
                execute_values(cur, sql, values, page_size=500)

    # ------------------------------------------------------------
    # 北市水利處三本柱（wic_taipei platform）
    # ------------------------------------------------------------
    def _transform_wic_sewer(self, result: dict, ts: datetime) -> list[dict]:
        """北市雨水下水道水位即時讀值；collector 已 upsert stations metadata。"""
        return result.get('data', [])

    def _transform_wic_evacuate(self, result: dict, ts: datetime) -> list[dict]:
        """北市疏散門即時狀態；collector 已 upsert stations metadata。"""
        return result.get('data', [])

    def _transform_wic_pumb(self, result: dict, ts: datetime) -> list[dict]:
        """北市抽水站即時運轉狀態；collector 已 upsert stations metadata。"""
        return result.get('data', [])

    def _upsert_taipei_sewer_stations(self, rows: list[dict]) -> None:
        if not rows:
            return
        cols = ['station_no', 'station_name']
        dedup = {r['station_no']: r for r in rows if r.get('station_no')}
        values = [tuple(r.get(c) for c in cols) for r in dedup.values()]
        sql = (
            "INSERT INTO public.taipei_sewer_stations (station_no, station_name) VALUES %s "
            "ON CONFLICT (station_no) DO UPDATE SET "
            "station_name = EXCLUDED.station_name, "
            "updated_at = now()"
        )
        with self._pool.borrow() as conn:
            with self._txn(conn) as cur:
                execute_values(cur, sql, values, page_size=500)

    def _upsert_taipei_evacuate_stations(self, rows: list[dict]) -> None:
        if not rows:
            return
        cols = ['station_no', 'station_name', 'gate_num']
        dedup = {r['station_no']: r for r in rows if r.get('station_no')}
        values = [tuple(r.get(c) for c in cols) for r in dedup.values()]
        sql = (
            "INSERT INTO public.taipei_evacuate_stations (station_no, station_name, gate_num) VALUES %s "
            "ON CONFLICT (station_no) DO UPDATE SET "
            "station_name = EXCLUDED.station_name, "
            "gate_num = EXCLUDED.gate_num, "
            "updated_at = now()"
        )
        with self._pool.borrow() as conn:
            with self._txn(conn) as cur:
                execute_values(cur, sql, values, page_size=500)

    def _upsert_taipei_pumb_stations(self, rows: list[dict]) -> None:
        if not rows:
            return
        cols = ['stn_id', 'stn_name', 'lat', 'lng', 'pumb_num', 'door_num', 'max_allowable_water_level']
        dedup = {r['stn_id']: r for r in rows if r.get('stn_id')}
        values = [tuple(r.get(c) for c in cols) for r in dedup.values()]
        sql = (
            "INSERT INTO public.taipei_pumb_stations "
            "(stn_id, stn_name, lat, lng, pumb_num, door_num, max_allowable_water_level) VALUES %s "
            "ON CONFLICT (stn_id) DO UPDATE SET "
            "stn_name = EXCLUDED.stn_name, "
            "lat = EXCLUDED.lat, lng = EXCLUDED.lng, "
            "pumb_num = EXCLUDED.pumb_num, door_num = EXCLUDED.door_num, "
            "max_allowable_water_level = EXCLUDED.max_allowable_water_level, "
            "updated_at = now()"
        )
        with self._pool.borrow() as conn:
            with self._txn(conn) as cur:
                execute_values(cur, sql, values, page_size=500)

    def _upsert_water_reservoirs(self, rows: list[dict]) -> None:
        """upsert 靜態水庫基本資料到 public.water_reservoirs

        ⚠️ lat/lng 刻意不在此 upsert：由 reference.reservoir_geometry
        （gis-platform migration 048）權威提供，避免原硬編碼字典的 id 錯位。
        upsert 完成後自動 JOIN reference 表同步 lat/lng。
        """
        if not rows:
            return
        cols = [
            'id', 'name', 'region', 'river_name', 'township',
            'dam_type', 'design_capacity_wan', 'effective_capacity_wan',
            'current_capacity_wan', 'catchment_area_km2', 'function_type',
            'agency', 'updated_at',
        ]
        values = [tuple(r.get(c) for c in cols) for r in rows]
        update_set = ', '.join(
            f"{c} = EXCLUDED.{c}" for c in cols if c != 'id'
        )
        sql = (
            f"INSERT INTO public.water_reservoirs ({','.join(cols)}) VALUES %s "
            f"ON CONFLICT (id) DO UPDATE SET {update_set}"
        )
        sync_lat_lng_sql = r"""
            UPDATE public.water_reservoirs AS w
            SET lat = g.lat, lng = g.lng, updated_at = now()
            FROM reference.reservoir_geometry g
            WHERE w.id ~ '^\d+$'
              AND w.id::INTEGER = g.compare_id
              AND g.compare_id > 0
              AND (w.lat IS DISTINCT FROM g.lat OR w.lng IS DISTINCT FROM g.lng)
        """
        with self._pool.borrow() as conn:
            with self._txn(conn) as cur:
                execute_values(cur, sql, values, page_size=100)
                cur.execute(sync_lat_lng_sql)

    def _transform_waste_positions(self, result: dict, ts: datetime) -> list[dict]:
        """垃圾車即時 GPS（高雄 / 新北 / 台南，by waste_positions collector）

        collector 已正規化為統一 dict（lat/lng/vehicle_no/route_id/status/observed_at），
        這裡只做 PostGIS WKT 字串組裝與欄位過濾。
        無座標或 vehicle_no 為空者跳過。
        """
        records = []
        for r in result.get('data', []):
            lat = r.get('lat')
            lng = r.get('lng')
            vehicle_no = (r.get('vehicle_no') or '').strip()
            if lat is None or lng is None or not vehicle_no:
                continue
            records.append({
                'city': r.get('city'),
                'vehicle_no': vehicle_no,
                'route_id': r.get('route_id'),
                'status': r.get('status') or 'unknown',
                'geometry': f'SRID=4326;POINT({lng} {lat})',
                'observed_at': r.get('observed_at'),
                'source_url': r.get('source_url'),
            })
        return records

    def _transform_air_quality_microsensors(self, result: dict, ts: datetime) -> list[dict]:
        """LASS AirBox / 微型感測器讀值。"""
        records = []
        for r in result.get('data', []):
            lat = r.get('latitude')
            lng = r.get('longitude')
            if not (lat and lng):
                continue
            device_id = r.get('device_id')
            if not device_id:
                continue
            records.append({
                'device_id': str(device_id),
                'source': r.get('source', 'lass_airbox'),
                'site_name': r.get('site_name'),
                'area': r.get('area'),
                'app': r.get('app'),
                'pm25': r.get('pm25'),
                'pm10': r.get('pm10'),
                'pm1': r.get('pm1'),
                'temperature': r.get('temperature'),
                'humidity': r.get('humidity'),
                'observed_at': r.get('observed_at') or ts.isoformat(),
                'collected_at': ts.isoformat(),
                'geom': f'SRID=4326;POINT({lng} {lat})',
            })
        return records

    def _transform_er_hospital_realtime(self, result: dict, ts: datetime) -> list[dict]:
        """健保署重度級急救責任醫院急診即時量能。

        collector 的 _normalize 已產出與 TABLE_MAP columns 同名的 dict，
        唯 observed_at / collected_at 為 datetime（TAIPEI_TZ）物件 → 序列化為 isoformat。
        buffer fallback（json.dumps default=str）round-trip 後這兩欄會是字串，故兩者皆容忍。
        observed_at 屬 upsert_key（hosp_id,observed_at），缺值跳過。
        """
        def _iso(v):
            if v is None:
                return None
            return v.isoformat() if isinstance(v, datetime) else v

        records = []
        for r in result.get('data', []):
            hosp_id = r.get('hosp_id')
            observed_at = _iso(r.get('observed_at'))
            if not hosp_id or not observed_at:
                continue
            records.append({
                'hosp_id':          hosp_id,
                'hosp_name':        r.get('hosp_name'),
                'area_no':          r.get('area_no'),
                'area_name':        r.get('area_name'),
                'cont_type':        r.get('cont_type'),
                'level_name':       r.get('level_name'),
                'inform':           r.get('inform'),
                'wait_see_cnt':     r.get('wait_see_cnt'),
                'wait_bed_cnt':     r.get('wait_bed_cnt'),
                'wait_general_cnt': r.get('wait_general_cnt'),
                'wait_icu_cnt':     r.get('wait_icu_cnt'),
                'source_url':       r.get('source_url'),
                'observed_at':      observed_at,
                'collected_at':     _iso(r.get('collected_at')) or ts.isoformat(),
            })
        return records

    def _transform_tpml_seat(self, result: dict, ts: datetime) -> list[dict]:
        """北市圖座位即時狀態。

        collector 的 _normalize 已產出與 TABLE_MAP columns 同名的 dict，
        observed_at / collected_at 通常已是 isoformat 字串，datetime 物件亦容忍
        （照 er_hospital 慣例，buffer fallback round-trip 後為字串）。
        observed_at 屬 upsert_key（area_id,observed_at），缺值跳過。
        """
        def _iso(v):
            if v is None:
                return None
            return v.isoformat() if isinstance(v, datetime) else v

        records = []
        for r in result.get('data', []):
            area_id = r.get('area_id')
            observed_at = _iso(r.get('observed_at'))
            if area_id is None or not observed_at:
                continue
            records.append({
                'area_id':      area_id,
                'branch_name':  r.get('branch_name'),
                'floor_name':   r.get('floor_name'),
                'area_name':    r.get('area_name'),
                'free_count':   r.get('free_count'),
                'total_count':  r.get('total_count'),
                'is_closed':    bool(r.get('is_closed', False)),
                'observed_at':  observed_at,
                'collected_at': _iso(r.get('collected_at')) or ts.isoformat(),
            })
        return records

    def _transform_npa_traffic_accident_a1(self, result: dict, ts: datetime) -> list[dict]:
        """警政署即時 A1 交通事故。collector 已產出 isoformat 字串 + dedup_hash + geom WKT。
        dedup_hash 必填（UNIQUE key），缺值跳過。"""
        records = []
        for r in result.get('data', []):
            if not r.get('dedup_hash') or not r.get('occurred_at'):
                continue
            records.append(dict(r))
        return records

    def _transform_immigration_apis_airport(self, result: dict, ts: datetime) -> list[dict]:
        """移民署機場入出境 demographic snapshot（每細格一 row）。
        collector 已產出 isoformat 字串；append-only，無 upsert key。
        """
        records = []
        for r in result.get('data', []):
            if r.get('pax_count') is None or r.get('airport') is None:
                continue
            records.append({
                'airport':       r.get('airport'),
                'terminal':      r.get('terminal'),
                'in_out':        r.get('in_out'),
                'in_out_code':   r.get('in_out_code'),
                'gender':        r.get('gender'),
                'nationality':   r.get('nationality'),
                'age_band':      r.get('age_band'),
                'pax_count':     r.get('pax_count'),
                'endpoint_code': r.get('endpoint_code'),
                'collected_at':  r.get('collected_at') or ts.isoformat(),
            })
        return records

    def _transform_correctional_daily_snapshot(self, result: dict, ts: datetime) -> list[dict]:
        """矯正機關每日收容動態（全國總計）。
        collector 已產出 isoformat 字串；observed_date 必填（PRIMARY KEY）。
        """
        records = []
        for r in result.get('data', []):
            if not r.get('observed_date'):
                continue
            records.append({
                'observed_date':     r.get('observed_date'),
                'total_inmates':     r.get('total_inmates'),
                'male_inmates':      r.get('male_inmates'),
                'female_inmates':    r.get('female_inmates'),
                'approved_capacity': r.get('approved_capacity'),
                'over_capacity_pct': r.get('over_capacity_pct'),
                'new_in_count':      r.get('new_in_count'),
                'new_out_count':     r.get('new_out_count'),
                'collected_at':      r.get('collected_at') or ts.isoformat(),
            })
        return records

    def _transform_animal_adoption(self, result: dict, ts: datetime) -> list[dict]:
        """待認領養完整快照。

        即使 HTTP/完整性 gate 失敗仍回傳 run ledger，讓資料品質可稽核；只有
        ``is_complete`` 的 run 才會附動物列，writer 才能呼叫 finalize 更新 current/daily。
        """
        run_id = result.get('run_id')
        if not run_id:
            return []
        run = {
            '_type': 'run',
            'run_id': run_id,
            'run_status': result.get('run_status') or 'failed',
            'is_complete': bool(result.get('is_complete')),
            'snapshot_date': result.get('snapshot_date'),
            'row_count': result.get('row_count', 0),
            'collected_at': result.get('collected_at') or ts.isoformat(),
            'source_dataset_id': result.get('source_dataset_id'),
            'source_observed_at': result.get('source_observed_at'),
            'payload_sha256': result.get('payload_sha256'),
            'quality_note': result.get('quality_note'),
        }
        if not run['is_complete']:
            return [run]
        animals = [dict(row) for row in result.get('data', []) if row.get('source_record_key')]
        return [run, *animals]

    def _transform_animal_shelter_monthly(self, result: dict, ts: datetime) -> list[dict]:
        """保留月報 collector 已完成驗證的 run＋observation contract。

        兩個月報 source 共用同一個 live staging/finalizer；transformer 的職責只
        是把已標記 ``_type`` 的資料送進 multi-table writer，不重新計算 grain
        或 revision，避免 collector 與 DB 契約產生兩套真相。
        """
        records = [dict(row) for row in result.get('data', []) if isinstance(row, dict)]
        runs = [row for row in records if row.get('_type') == 'run']
        if len(runs) != 1 or not runs[0].get('run_id'):
            return []
        return records

    def _transform_animal_welfare_points(self, result: dict, ts: datetime) -> list[dict]:
        """獸醫／寵物業完整快照；raw_payload 僅由 LocalStorage/archive 保存。"""
        records = [dict(row) for row in result.get('data', []) if isinstance(row, dict)]
        runs = [row for row in records if row.get('_type') == 'run']
        return records if len(runs) == 1 and runs[0].get('run_id') else []

    def _transform_news_events(self, result: dict, ts: datetime) -> list[dict]:
        """新聞事件：collector 已產出與 TABLE_MAP columns 同名的 dict（JSON-safe），
        published_ts 為 isoformat 字串、title_simhash 為 signed 64-bit int。
        url_norm 缺值跳過（UNIQUE key 不可為 NULL 重複）。"""
        return [r for r in result.get('data', []) if r.get('url_norm')]

    def _transform_power_taipower(self, result: dict, ts: datetime) -> list[dict]:
        """台電即時電力供需：3 表攤平為單一 records list，由 _write_multi_table 依 _type 分派。

        collector 已在 collect() 把 observed_at / collected_at 序列化為 isoformat，
        並完成單位換算（萬瓩→MW）與 _num 解析，這裡只貼 _type 標籤。
        """
        records: list[dict] = []
        for r in result.get('system_status', []) or result.get('data', []):
            records.append({'_type': 'system', **r})
        for r in result.get('generation_units', []):
            records.append({'_type': 'unit', **r})
        for r in result.get('region_demand', []):
            records.append({'_type': 'region', **r})
        return records

    def _transform_global_climate_typhoon_positions(self, result: dict, ts: datetime) -> list[dict]:
        """颱風 time-point decomposed（JMA + JTWC 共用）：補 geom WKT。"""
        records: list[dict] = []
        for r in result.get('data', []):
            lon = r.get('lon') or r.get('center_lon')
            lat = r.get('lat') or r.get('center_lat')
            if lon is None or lat is None:
                continue
            records.append({
                'storm_id':            r.get('storm_id'),
                'source':              r.get('source'),
                'valid_at':            r.get('valid_at'),
                'point_type':          r.get('point_type'),
                'advisory_number':     r.get('advisory_number'),
                'advisory_issued_at':  r.get('advisory_issued_at'),
                'name_local':          r.get('name_local'),
                'name_en':             r.get('name_en'),
                'center_lat':          r.get('center_lat'),
                'center_lon':          r.get('center_lon'),
                'center_pressure_hpa': r.get('center_pressure_hpa'),
                'max_wind_kt':         r.get('max_wind_kt'),
                'gale_radius_km':      r.get('gale_radius_km'),
                'storm_radius_km':     r.get('storm_radius_km'),
                'geom':                f'SRID=4326;POINT({lon} {lat})',
                'raw_json':            r.get('raw_json'),
                'collected_at':        ts.isoformat(),
            })
        return records

    def _transform_global_climate_grids(self, result: dict, ts: datetime) -> list[dict]:
        """CMEMS/CAMS/GFS digest 共用：bbox 4 數字 → Polygon WKT。"""
        records: list[dict] = []
        for r in result.get('data', []):
            min_lon = r.get('bbox_min_lon')
            max_lon = r.get('bbox_max_lon')
            min_lat = r.get('bbox_min_lat')
            max_lat = r.get('bbox_max_lat')
            if None not in (min_lon, max_lon, min_lat, max_lat):
                bbox_wkt = (
                    f'SRID=4326;POLYGON(('
                    f'{min_lon} {min_lat},{max_lon} {min_lat},'
                    f'{max_lon} {max_lat},{min_lon} {max_lat},'
                    f'{min_lon} {min_lat}))'
                )
            else:
                bbox_wkt = None
            records.append({
                'dataset_id':     r.get('dataset_id'),
                'observed_at':    r.get('observed_at'),
                'init_at':        r.get('init_at'),
                'leadtime_hr':    r.get('leadtime_hr'),
                'bbox':           bbox_wkt,
                'digest':         r.get('digest'),
                's3_uri':         r.get('s3_uri'),
                'pmtiles_uri':    r.get('pmtiles_uri'),
                'raw_size_bytes': r.get('raw_size_bytes'),
                'collected_at':   ts.isoformat(),
            })
        return records

    def _transform_global_climate_usgs_earthquake(self, result: dict, ts: datetime) -> list[dict]:
        """USGS 全球地震：collector 已產出 JSON-safe dict；補 geom WKT。"""
        records: list[dict] = []
        for r in result.get('data', []):
            lon = r.get('lon')
            lat = r.get('lat')
            if lon is None or lat is None:
                continue
            records.append({
                'event_id':     r.get('event_id'),
                'mag':          r.get('mag'),
                'place':        r.get('place'),
                'observed_at':  r.get('observed_at'),
                'depth_km':     r.get('depth_km'),
                'raw_json':     r.get('raw_json'),
                'dedup_hash':   r.get('dedup_hash'),
                'geom':         f'SRID=4326;POINT({lon} {lat})',
                'collected_at': ts.isoformat(),
            })
        return records

    def _transform_lightning_events(self, result: dict, ts: datetime) -> list[dict]:
        """落雷事件：collector 已產出 JSON-safe dict；補 geom WKT。

        台電 / 氣象署兩源共用（migration 338 起 live.lightning_events 有 source 欄位，
        UNIQUE 也改成 (source, event_id) / (source, dedup_hash) —— 兩源各自去重）。
        source 由 collector 在 record 裡帶，沒帶就當台電（本欄位加入前的舊行為）。
        """
        records: list[dict] = []
        for r in result.get('data', []):
            lon = r.get('lon')
            lat = r.get('lat')
            if lon is None or lat is None:
                continue
            records.append({
                'event_id':     r.get('event_id'),
                'strike_time':  r.get('strike_time'),
                'lon':          lon,
                'lat':          lat,
                'intensity_ka': r.get('intensity_ka'),
                'strike_type':  r.get('strike_type'),
                'dedup_hash':   r.get('dedup_hash'),
                'source':       r.get('source', 'taipower'),
                'geom':         f'SRID=4326;POINT({lon} {lat})',
                'observed_at':  r.get('observed_at'),
                'collected_at': r.get('collected_at') or ts.isoformat(),
            })
        return records

    def _transform_nuclear_radiation(self, result: dict, ts: datetime) -> list[dict]:
        """核安輻射：補 geom WKT；lon/lat 缺值時 geom=None。"""
        records: list[dict] = []
        for r in result.get('data', []) or result.get('measurements', []):
            lon = r.get('lon')
            lat = r.get('lat')
            geom = f'SRID=4326;POINT({lon} {lat})' if (lon is not None and lat is not None) else None
            records.append({
                'station_id':   r.get('station_id'),
                'station_name': r.get('station_name'),
                'dose_usvh':    r.get('dose_usvh'),
                'observed_at':  r.get('observed_at'),
                'lon':          lon,
                'lat':          lat,
                'is_stale':     r.get('is_stale'),
                'geom':         geom,
                'collected_at': r.get('collected_at') or ts.isoformat(),
            })
        return records

    def _transform_food_prices(self, result: dict, ts: datetime) -> list[dict]:
        """食品價格：collector 已產出與 TABLE_MAP 同名 dict。
        trade_date 為 date 物件、collected_at 為 datetime → 序列化為 ISO 字串。
        price_avg 缺值或 <=0 的列不寫（「休市」在 collector 端已轉 None）。"""
        def _iso(v):
            if v is None:
                return None
            return v.isoformat() if isinstance(v, (datetime, date)) else v

        records = []
        for r in result.get('data', []):
            trade_date = _iso(r.get('trade_date'))
            price = r.get('price_avg')
            if not trade_date or price is None or price <= 0:
                continue
            if not r.get('item_name') or not r.get('market_name'):
                continue
            records.append({
                'trade_date':  trade_date,
                'category':    r.get('category'),
                'item_code':   r.get('item_code'),
                'item_name':   r.get('item_name'),
                'market_name': r.get('market_name'),
                'price_avg':   price,
                'price_high':  r.get('price_high'),
                'price_mid':   r.get('price_mid'),
                'price_low':   r.get('price_low'),
                'quantity':    r.get('quantity'),
                'unit':        r.get('unit'),
                'source':      r.get('source'),
                'collected_at': _iso(r.get('collected_at')) or ts.isoformat(),
            })
        return records

    def _transform_twse_market_index(self, result: dict, ts: datetime) -> list[dict]:
        """TWSE 加權指數：collector 已產出與 TABLE_MAP 同名 dict，observed_at/collected_at 為 datetime
        物件 → 序列化。"""
        def _iso(v):
            if v is None:
                return None
            return v.isoformat() if isinstance(v, datetime) else v

        records = []
        for r in result.get('data', []):
            observed_at = _iso(r.get('observed_at'))
            if not r.get('index_code') or not observed_at or r.get('current_value') is None:
                continue
            records.append({
                'index_code':      r.get('index_code'),
                'index_name':      r.get('index_name'),
                'current_value':   r.get('current_value'),
                'prev_close':      r.get('prev_close'),
                'open_value':      r.get('open_value'),
                'high_value':      r.get('high_value'),
                'low_value':       r.get('low_value'),
                'volume_lots':     r.get('volume_lots'),
                'value_thousands': r.get('value_thousands'),
                'is_market_open':  r.get('is_market_open'),
                'observed_at':     observed_at,
                'collected_at':    _iso(r.get('collected_at')) or ts.isoformat(),
            })
        return records

    def _transform_pla_activity_daily(self, result: dict, ts: datetime) -> list[dict]:
        """共機每日通報：collector 已產出 JSON-safe dict（report_date 為 ISO str）。
        report_date 為 PK，缺值跳過。"""
        records = []
        for r in result.get('data', []):
            if not r.get('report_date'):
                continue
            records.append({
                'report_date':             r.get('report_date'),
                'period_start':            r.get('period_start'),
                'period_end':              r.get('period_end'),
                'aircraft_sorties':        r.get('aircraft_sorties'),
                'crossed_median_line_cnt': r.get('crossed_median_line_cnt'),
                'plan_vessels':            r.get('plan_vessels'),
                'official_ships':          r.get('official_ships'),
                'adiz_north':              r.get('adiz_north'),
                'adiz_central':            r.get('adiz_central'),
                'adiz_southwestern':       r.get('adiz_southwestern'),
                'adiz_eastern':            r.get('adiz_eastern'),
                'raw_text':                r.get('raw_text'),
                'track_chart_url':         r.get('track_chart_url'),
                'activity_chart_url':      r.get('activity_chart_url'),
                'source_lang':             r.get('source_lang'),
                'source_url':              r.get('source_url'),
                'collected_at':            r.get('collected_at') or ts.isoformat(),
            })
        return records

    def _transform_pla_tracks_vectorize(self, result: dict, ts: datetime) -> list[dict]:
        """航跡向量化：collector 已產出 DB-ready records（帶 _type 分派標記），直接透傳。

        不在此補 collected_at —— 三張表都以 report_date 為時間軸，
        ledger 的 run_at 由 DB default / ON CONFLICT 時的 now() 給。
        """
        return result.get('data', [])

    def _transform_cdc_public_health_weekly(self, result: dict, ts: datetime) -> list[dict]:
        """CDC 公衛週報：collector 已產出與 TABLE_MAP 同名 dict。
        UNIQUE 含 township_code/gender/age_group/is_imported；rods 類別這些欄位用空字串/NULL 佔位。"""
        records = []
        for r in result.get('data', []):
            if not r.get('disease_code') or r.get('iso_year') is None or r.get('iso_week') is None:
                continue
            records.append({
                'disease_code':    r.get('disease_code'),
                'iso_year':        r.get('iso_year'),
                'iso_week':        r.get('iso_week'),
                'county_code':     r.get('county_code') or '',
                'county_name':     r.get('county_name'),
                'township_code':   r.get('township_code') or '',
                'township_name':   r.get('township_name'),
                'age_group':       r.get('age_group') or '',
                'gender':          r.get('gender') or '',
                'is_imported':     r.get('is_imported'),
                'metric_value':    r.get('metric_value'),
                'source_dataset':  r.get('source_dataset'),
                'collected_at':    ts.isoformat(),
            })
        return records

    def _transform_yt_live_video_resolver(self, result: dict, ts: datetime) -> list[dict]:
        """YouTube 直播 videoId 解析：collector 已產出與 TABLE_MAP 同名 dict，
        handle 為 current 表 PK，缺值跳過；is_live 預設 False。"""
        records = []
        for r in result.get('data', []):
            handle = r.get('handle')
            if not handle:
                continue
            records.append({
                'handle':       handle,
                'channel_id':   r.get('channel_id'),
                'video_id':     r.get('video_id'),
                'title':        r.get('title'),
                'is_live':      bool(r.get('is_live', False)),
                'view_count':   r.get('view_count'),
                'last_error':   r.get('last_error'),
                'observed_at':  r.get('observed_at') or ts.isoformat(),
                'collected_at': r.get('collected_at') or ts.isoformat(),
            })
        return records

    TRANSFORMERS = {
        'food_prices': _transform_food_prices,
        'groundwater_level': _transform_groundwater_level,
        'water_reservoir_daily_ops': _transform_water_reservoir_daily_ops,
        'youbike': _transform_youbike,
        'bus': _transform_bus,
        'bus_intercity': _transform_bus_intercity,
        'weather': _transform_weather,
        'temperature': _transform_temperature,
        'tra_train': _transform_tra_train,
        'tourist_shuttle': _transform_tourist_shuttle,
        'parking': _transform_parking,
        'parking_offstreet': _transform_parking_offstreet,
        'road_congestion': _transform_road_congestion,
        'ship_ais': _transform_ship_ais,
        'gfw_vessel_presence': _transform_gfw_vessel_presence,
        'earthquake': _transform_earthquake,
        'earthquake_catalog': _transform_earthquake_catalog,
        'earthquake_town_intensity': _transform_earthquake_town_intensity,
        'earthquake_shakemap_grid': _transform_earthquake_shakemap_grid,
        'earthquake_moment_tensor': _transform_earthquake_moment_tensor,
        'rail_timetable': _transform_rail_timetable,
        'flight_fr24': _transform_flight_fr24,
        'flight_fr24_zone': _transform_flight_fr24_zone,
        'flight_opensky': _transform_flight_opensky,
        'freeway_vd': _transform_freeway_vd,
        'satellite': _transform_satellite,
        'launch': _transform_launch,
        'ncdr_alerts': _transform_ncdr_alerts,
        'cloudflare_radar': _transform_internet_health,
        'ioda_internet_health': _transform_internet_health,
        'ripe_atlas_internet_health': _transform_internet_health,
        'ripe_ris_live': _transform_internet_health,
        'news_events': _transform_news_events,
        'cwa_satellite': _transform_cwa_satellite,
        'cwa_marine_observation': _transform_marine_observation,
        'isohe_port_marine': _transform_marine_observation,
        'foursquare_poi': _transform_foursquare_poi,
        'air_quality_imagery': _transform_air_quality_imagery,
        'air_quality': _transform_air_quality,
        'air_quality_microsensors': _transform_air_quality_microsensors,
        'waste_positions': _transform_waste_positions,
        'water_reservoir': _transform_water_reservoir,
        'river_water_level': _transform_river_water_level,
        'rain_gauge_realtime': _transform_rain_gauge_realtime,
        'er_hospital_realtime': _transform_er_hospital_realtime,
        'tpml_seat': _transform_tpml_seat,
        'correctional_daily_snapshot': _transform_correctional_daily_snapshot,
        'animal_adoption': _transform_animal_adoption,
        'animal_shelter_outcomes': _transform_animal_shelter_monthly,
        'animal_shelter_pressure': _transform_animal_shelter_monthly,
        'animal_veterinary_clinics': _transform_animal_welfare_points,
        'animal_licensed_pet_businesses': _transform_animal_welfare_points,
        'animal_protection_offices': _transform_animal_welfare_points,
        'immigration_apis_airport': _transform_immigration_apis_airport,
        'npa_traffic_accident_a1': _transform_npa_traffic_accident_a1,
        'twse_market_index': _transform_twse_market_index,
        'pla_activity_daily': _transform_pla_activity_daily,
        'pla_tracks_vectorize': _transform_pla_tracks_vectorize,
        'cdc_public_health_weekly': _transform_cdc_public_health_weekly,
        'iot_wra': _transform_iot_wra,
        'uswg': _transform_uswg,
        'precipitation_raster': _transform_precipitation_raster,
        'road_event_live': _transform_road_event,
        'road_event_planned': _transform_road_event,
        'wra_drought_alert': _transform_wra_drought_alert,
        'power_taipower': _transform_power_taipower,
        'lightning_events': _transform_lightning_events,
        'lightning_cwa': _transform_lightning_events,  # 同一張表，source 由 collector 帶
        'global_climate_usgs_earthquake': _transform_global_climate_usgs_earthquake,
        'global_climate_jma_typhoon': _transform_global_climate_typhoon_positions,
        'global_climate_jtwc': _transform_global_climate_typhoon_positions,
        'global_climate_cmems': _transform_global_climate_grids,
        'global_climate_cams': _transform_global_climate_grids,
        'global_climate_noaa_gfs': _transform_global_climate_grids,
        'nuclear_radiation': _transform_nuclear_radiation,
        'wic_sewer': _transform_wic_sewer,
        'wic_evacuate': _transform_wic_evacuate,
        'wic_pumb': _transform_wic_pumb,
        'yt_live_video_resolver': _transform_yt_live_video_resolver,
    }

    # ============================================================
    # DB 寫入：分區表 + current 表
    # 表對應設定改由 storage/supabase_tables.py 集中管理（見 module top import）
    # ============================================================

    def _write_to_db(self, conn, collector_name: str, records: list[dict], timestamp: datetime):
        table_config = TABLE_MAP.get(collector_name)
        if not table_config:
            return

        # 特殊處理：時刻表寫入 reference schema
        if table_config.get('is_reference'):
            self._write_schedules(conn, records)
            return

        # 特殊處理：多表寫入（freeway_vd, flight_fr24）
        if table_config.get('is_multi_table'):
            self._write_multi_table(conn, collector_name, records)
            return

        columns = table_config['columns']

        with self._txn(conn) as cur:
            # 0. History dedup（可選，見 table_config['history_dedup_cols']）：
            #    只有指定欄位變動、或該 current_key 首次出現，才寫 history；
            #    current 仍每輪全量 upsert，行為不變。
            #    ⚠️ 順序關鍵：必須先撈 current 現況再 upsert current（見下方 2.），
            #    否則撈到的會是本輪剛寫入的值，比對永遠相同、dedup 形同失效。
            def _dedup_norm(v):
                # REAL/float4 欄位（如 road_congestion 的 travel_speed/travel_time）
                # 存進 DB 再讀回來會有 float4 精度誤差（約 1e-5~1e-6），
                # 不 round 的話同一個值會被誤判成「變動」、dedup 形同失效。
                if isinstance(v, float):
                    return round(v, 2)
                return v

            dedup_cols = table_config.get('history_dedup_cols')
            history_records = records
            dedup_skipped = 0
            is_heartbeat = False
            if dedup_cols and table_config.get('current') and table_config.get('current_key'):
                # 每日 heartbeat：同一台北日曆日的第一次寫入 bypass dedup，
                # 全量寫入 history 當作當天的完整快照。純時間判斷、不查 DB。
                # 任何有限 lookback 的下游聚合都補不到「連續多天沒變」的
                # row，靠這個保證每天至少有一張全量快照能被回溯窗撈到。
                today = _taipei_today()
                is_heartbeat = self._history_dedup_heartbeat_date.get(collector_name) != today

                if is_heartbeat:
                    self._history_dedup_heartbeat_date[collector_name] = today
                    history_records = records
                else:
                    current_key = table_config['current_key']
                    keys = list({r.get(current_key) for r in records if r.get(current_key) is not None})
                    prev_state = {}
                    if keys:
                        cur.execute(
                            f"SELECT {current_key}, {','.join(dedup_cols)} "
                            f"FROM {table_config['current']} WHERE {current_key} = ANY(%s)",
                            (keys,),
                        )
                        prev_state = {
                            row[0]: tuple(_dedup_norm(v) for v in row[1:])
                            for row in cur.fetchall()
                        }

                    history_records = []
                    for r in records:
                        prev = prev_state.get(r.get(current_key))
                        cur_vals = tuple(_dedup_norm(r.get(c)) for c in dedup_cols)
                        if prev is None or cur_vals != prev:
                            history_records.append(r)
                        else:
                            dedup_skipped += 1

            # 1. INSERT INTO 分區表（歷史）
            values = []
            for r in history_records:
                row = tuple(r.get(c) for c in columns)
                values.append(row)

            if values:
                placeholders = ','.join(['%s'] * len(columns))
                col_names = ','.join(columns)

                # 地震用 UPSERT（避免重複）
                if table_config.get('upsert_key'):
                    key = table_config['upsert_key']  # 支援複合鍵 'a,b'
                    if table_config.get('upsert_strategy') == 'do_nothing':
                        # 無目標 ON CONFLICT：表上任一 unique index 撞到都跳過該列。
                        # 指定 ({key}) 只護一個 index — lightning_events 有
                        # uk_eventid + uk_dedup 雙 unique，feed 用新 event_id 重發
                        # 同一筆落雷時 dedup_hash 撞上第二個 index 會炸掉整批
                        # （2026-07-03 事故：寫入連續失敗、資料卡 buffer）。
                        sql = f"INSERT INTO {table_config['history']} ({col_names}) VALUES %s ON CONFLICT DO NOTHING"
                    else:
                        key_set = {k.strip() for k in key.split(',')}
                        update_cols = [c for c in columns if c not in key_set]
                        update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                        sql = f"INSERT INTO {table_config['history']} ({col_names}) VALUES %s ON CONFLICT ({key}) DO UPDATE SET {update_set}"
                else:
                    sql = f"INSERT INTO {table_config['history']} ({col_names}) VALUES %s"

                execute_values(cur, sql, values, page_size=1000)

            if dedup_cols:
                if is_heartbeat:
                    logger.info(
                        f"[{collector_name}] history dedup: 每日 heartbeat 全量寫入 {len(values)} 筆"
                    )
                else:
                    logger.info(
                        f"[{collector_name}] history dedup: 寫入 {len(values)} 筆 / 略過 {dedup_skipped} 筆未變動"
                    )

            # 2. UPSERT INTO current 表（最新狀態）
            # 同一批次內可能有重複 PK（例如同一輛公車出現兩次），
            # ON CONFLICT 無法在同一 INSERT 中更新同一行兩次，因此先去重（保留最後一筆）
            if 'current' in table_config:
                current_cols = table_config.get('current_columns', columns)
                key = table_config['current_key']
                key_idx = current_cols.index(key) if key in current_cols else 0
                update_cols = [c for c in current_cols if c != key]
                update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                if table_config.get('current_touch_updated_at'):
                    update_set += ',updated_at=now()'
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

                # 清除 stale rows：本批次沒 upsert 到、欄位值 < 本次 timestamp 的 row
                # （同 transaction 內 DELETE，讀端不會看到空表）
                prune_col = table_config.get('current_prune_by')
                if prune_col and current_values:
                    cur.execute(
                        f"DELETE FROM {table_config['current']} WHERE {prune_col} < %s",
                        (timestamp,),
                    )
                    if cur.rowcount:
                        logger.info(f"[{collector_name}] ✓ current 表清除 {cur.rowcount} 筆 stale rows")

        record_count = len(records)
        logger.info(f"[{collector_name}] ✓ DB 寫入 {record_count} 筆")

    def _write_multi_table(self, conn, collector_name: str, records: list[dict]):
        """Write collector-specific multi-table contracts atomically."""
        if collector_name in (
            'cloudflare_radar', 'ioda_internet_health',
            'ripe_atlas_internet_health', 'ripe_ris_live',
        ):
            runs = [r for r in records if r.get('_type') == 'source_run']
            observations = [r for r in records if r.get('_type') == 'observation']
            incidents = [r for r in records if r.get('_type') == 'incident']
            if len(runs) != 1:
                raise ValueError(f'{collector_name} requires exactly one source_run ledger')
            with self._txn(conn) as cur:
                run_cols = [
                    'run_id', 'source', 'started_at', 'finished_at', 'status',
                    'requested_from', 'requested_to', 'source_updated_at',
                    'records_received', 'records_written', 'records_rejected',
                    'error_code', 'error_message', 'metadata',
                ]
                execute_values(
                    cur,
                    f"INSERT INTO live.internet_health_source_runs ({','.join(run_cols)}) VALUES %s "
                    "ON CONFLICT (run_id) DO UPDATE SET "
                    "finished_at=EXCLUDED.finished_at,status=EXCLUDED.status,"
                    "source_updated_at=EXCLUDED.source_updated_at,records_received=EXCLUDED.records_received,"
                    "records_written=EXCLUDED.records_written,records_rejected=EXCLUDED.records_rejected,"
                    "error_code=EXCLUDED.error_code,error_message=EXCLUDED.error_message,metadata=EXCLUDED.metadata",
                    [tuple(Json(row.get(c) or {}) if c == 'metadata' else row.get(c) for c in run_cols) for row in runs],
                )

                if observations:
                    observation_cols = [
                        'run_id', 'source', 'evidence_family', 'source_observation_id',
                        'entity_type', 'entity_id', 'entity_name', 'signal', 'observed_at',
                        'window_start', 'window_end', 'value', 'unit', 'baseline_value',
                        'change_ratio', 'reported_status', 'incident_kind', 'confidence',
                        'sample_count', 'stale_after_seconds', 'source_updated_at',
                        'collected_at', 'quality_flags', 'metadata',
                    ]
                    values = [tuple(
                        Json(row.get(c) or {}) if c in ('quality_flags', 'metadata') else row.get(c)
                        for c in observation_cols
                    ) for row in observations]
                    execute_values(
                        cur,
                        f"INSERT INTO live.internet_health_observations ({','.join(observation_cols)}) VALUES %s "
                        "ON CONFLICT (source,entity_type,entity_id,signal,observed_at) DO UPDATE SET "
                        "run_id=EXCLUDED.run_id,evidence_family=EXCLUDED.evidence_family,"
                        "source_observation_id=EXCLUDED.source_observation_id,entity_name=EXCLUDED.entity_name,"
                        "window_start=EXCLUDED.window_start,window_end=EXCLUDED.window_end,value=EXCLUDED.value,"
                        "unit=EXCLUDED.unit,baseline_value=EXCLUDED.baseline_value,change_ratio=EXCLUDED.change_ratio,"
                        "reported_status=EXCLUDED.reported_status,incident_kind=EXCLUDED.incident_kind,"
                        "confidence=EXCLUDED.confidence,sample_count=EXCLUDED.sample_count,"
                        "stale_after_seconds=EXCLUDED.stale_after_seconds,source_updated_at=EXCLUDED.source_updated_at,"
                        "collected_at=EXCLUDED.collected_at,quality_flags=EXCLUDED.quality_flags,metadata=EXCLUDED.metadata",
                        values,
                        page_size=1000,
                    )
                if incidents:
                    incident_cols = [
                        'incident_id', 'fingerprint', 'entity_type', 'entity_id', 'entity_name',
                        'incident_kind', 'severity', 'status', 'first_detected_at',
                        'last_detected_at', 'resolved_at', 'confidence', 'detector_version',
                        'first_observation_id', 'latest_observation_id', 'evidence', 'summary', 'metadata',
                    ]
                    incident_values = [tuple(
                        Json(row.get(c) or []) if c == 'evidence'
                        else Json(row.get(c) or {}) if c == 'metadata'
                        else row.get(c)
                        for c in incident_cols
                    ) for row in incidents]
                    execute_values(
                        cur,
                        f"INSERT INTO live.internet_health_incidents ({','.join(incident_cols)}) VALUES %s "
                        "ON CONFLICT (incident_id) DO UPDATE SET "
                        "entity_name=EXCLUDED.entity_name,incident_kind=EXCLUDED.incident_kind,"
                        "severity=EXCLUDED.severity,status=EXCLUDED.status,last_detected_at=EXCLUDED.last_detected_at,"
                        "resolved_at=EXCLUDED.resolved_at,confidence=EXCLUDED.confidence,"
                        "latest_observation_id=EXCLUDED.latest_observation_id,evidence=EXCLUDED.evidence,"
                        "summary=EXCLUDED.summary,metadata=EXCLUDED.metadata",
                        incident_values,
                        page_size=500,
                    )
            logger.info(
                f"[{collector_name}] internet health transaction runs={len(runs)} "
                f"observations={len(observations)} incidents={len(incidents)}"
            )
            return
        if collector_name in ('cwa_marine_observation', 'isohe_port_marine'):
            stations = [r for r in records if r.get('_type') == 'station']
            readings = [r for r in records if r.get('_type') == 'reading']
            quarantines = [r for r in records if r.get('_type') == 'quarantine']
            with self._txn(conn) as cur:
                if stations:
                    cols = ['station_uid', 'source_network', 'source_station_id', 'origin_org', 'distribution_org',
                            'station_type', 'name_zh', 'aliases', 'longitude', 'latitude', 'geom',
                            'observed_elements', 'source_status', 'source_url', 'license', 'provenance',
                            'first_seen_at', 'last_seen_at']
                    values = []
                    for row in stations:
                        lon, lat = row.get('longitude'), row.get('latitude')
                        geom = f'POINT({lon} {lat})' if lon is not None and lat is not None else None
                        values.append(tuple(Json(row.get(c) or []) if c in ('aliases', 'observed_elements')
                                            else Json(row.get(c) or {}) if c == 'provenance'
                                            else geom if c == 'geom' else row.get(c) for c in cols))
                    template = '(' + ','.join(['%s'] * 10 + ['ST_GeomFromText(%s,4326)'] + ['%s'] * 7) + ')'
                    updates = ','.join(f'{c}=EXCLUDED.{c}' for c in cols if c != 'station_uid')
                    execute_values(cur, f"INSERT INTO reference.marine_observation_stations ({','.join(cols)}) VALUES %s "
                                       f"ON CONFLICT (station_uid) DO UPDATE SET {updates}", values, template=template, page_size=500)
                if readings:
                    cols = ['station_uid', 'source_network', 'source_station_id', 'observed_at', 'metric_code', 'depth_key',
                            'value_raw', 'value_numeric', 'unit_source', 'unit_canonical', 'vertical_datum',
                            'is_missing', 'is_valid', 'missing_reason', 'source_status', 'quality_flags',
                            'payload_sha256', 'collected_at', 'geom_at_observation']
                    values = []
                    for row in readings:
                        lon, lat = row.get('longitude'), row.get('latitude')
                        geom = f'POINT({lon} {lat})' if lon is not None and lat is not None else None
                        values.append(tuple(Json(row.get(c) or {}) if c == 'quality_flags' else geom if c == 'geom_at_observation' else row.get(c) for c in cols))
                    template = '(' + ','.join(['%s'] * 18 + ['ST_GeomFromText(%s,4326)']) + ')'
                    execute_values(cur, f"INSERT INTO live.marine_observation_readings ({','.join(cols)}) VALUES %s "
                                       "ON CONFLICT (station_uid,observed_at,metric_code,depth_key) DO UPDATE SET "
                                       "value_raw=EXCLUDED.value_raw,value_numeric=EXCLUDED.value_numeric,unit_source=EXCLUDED.unit_source,"
                                       "unit_canonical=EXCLUDED.unit_canonical,vertical_datum=EXCLUDED.vertical_datum,"
                                       "is_missing=EXCLUDED.is_missing,is_valid=EXCLUDED.is_valid,missing_reason=EXCLUDED.missing_reason,"
                                       "source_status=EXCLUDED.source_status,quality_flags=EXCLUDED.quality_flags,"
                                       "payload_sha256=EXCLUDED.payload_sha256,collected_at=EXCLUDED.collected_at,geom_at_observation=EXCLUDED.geom_at_observation",
                                       values, template=template, page_size=1000)
                    current_cols = [
                        'station_uid', 'metric_code', 'depth_key', 'observed_at', 'value_raw', 'value_numeric',
                        'unit_source', 'unit_canonical', 'vertical_datum', 'is_missing', 'is_valid', 'source_status',
                        'quality_flags', 'payload_sha256', 'collected_at', 'geom_at_observation',
                    ]
                    latest = {}
                    for row in (r for r in readings if r.get('is_valid') and not r.get('is_missing') and r.get('value_numeric') is not None):
                        key = (row['station_uid'], row['metric_code'], row['depth_key'])
                        previous = latest.get(key)
                        if previous is None or str(row['observed_at']) >= str(previous['observed_at']):
                            latest[key] = row
                    current_values = []
                    for row in latest.values():
                        lon, lat = row.get('longitude'), row.get('latitude')
                        geom = f'POINT({lon} {lat})' if lon is not None and lat is not None else None
                        current_values.append(tuple(Json(row.get(c) or {}) if c == 'quality_flags' else geom if c == 'geom_at_observation' else row.get(c) for c in current_cols))
                    if current_values:
                        template = '(' + ','.join(['%s'] * (len(current_cols) - 1) + ['ST_GeomFromText(%s,4326)']) + ')'
                        updates = ','.join(f'{c}=EXCLUDED.{c}' for c in current_cols if c not in ('station_uid', 'metric_code', 'depth_key'))
                        execute_values(cur, f"INSERT INTO live.marine_observation_current ({','.join(current_cols)}) VALUES %s "
                                           f"ON CONFLICT (station_uid,metric_code,depth_key) DO UPDATE SET {updates} "
                                           "WHERE live.marine_observation_current.observed_at <= EXCLUDED.observed_at",
                                           current_values, template=template, page_size=1000)
                if quarantines:
                    cols = ['source_network', 'source_station_id', 'reason', 'row_count', 'collected_at']
                    execute_values(cur, f"INSERT INTO live.marine_observation_quarantine ({','.join(cols)}) VALUES %s "
                                       "ON CONFLICT (source_network,source_station_id,reason,collected_at) DO NOTHING",
                                       [tuple(r.get(c) for c in cols) for r in quarantines])
            logger.info(f"[{collector_name}] marine transaction stations={len(stations)} readings={len(readings)} quarantine={len(quarantines)}")
            return
        if collector_name == 'gfw_vessel_presence':
            runs = [r for r in records if r.get('_type') == 'run']
            if len(runs) != 1:
                raise ValueError('gfw_vessel_presence requires exactly one run ledger')
            run = runs[0]
            submitted_snapshots = [r for r in records if r.get('_type') == 'snapshot']
            snapshots = submitted_snapshots if run.get('status') == 'succeeded' else []
            if run.get('status') == 'succeeded' and len(snapshots) != run.get('result_count'):
                raise ValueError('GFW result_count does not match snapshots')
            with self._txn(conn) as cur:
                if snapshots:
                    cur.execute(
                        "SELECT live.create_gfw_vessel_presence_snapshot_partition(%s::date)",
                        (run.get('snapshot_date'),),
                    )
                run_cols = [
                    'run_id', 'provider', 'source_dataset_id', 'resolved_dataset_version', 'snapshot_date',
                    'status', 'started_at', 'completed_at', 'source_window_start', 'source_window_end',
                    'query_parameters', 'result_count', 'duplicate_count', 'rejected_count', 'response_sha256',
                    'archive_verified_at', 'quality_summary', 'error_message',
                ]
                execute_values(cur, f"INSERT INTO live.gfw_vessel_presence_runs ({','.join(run_cols)}) VALUES %s ON CONFLICT (run_id) DO NOTHING", [tuple(
                    Json(run.get(c)) if c in ('query_parameters', 'quality_summary') else run.get(c) for c in run_cols
                )])
                if snapshots:
                    snapshot_cols = [
                        'snapshot_date', 'run_id', 'provider', 'source_dataset_id', 'vessel_id', 'mmsi',
                        'observed_at', 'received_at', 'source_event_key', 'record_hash', 'ship_name',
                        'vessel_type', 'flag', 'longitude', 'latitude', 'geom', 'presence_quality',
                        'quality_flags', 'source_properties', 'raw_archive_key',
                    ]
                    values = []
                    for row in snapshots:
                        lon, lat = row.get('longitude'), row.get('latitude')
                        wkt = f'POINT({lon} {lat})' if lon is not None and lat is not None else None
                        values.append(tuple(
                            Json(row.get(c) or []) if c == 'quality_flags'
                            else Json(row.get(c) or {}) if c == 'source_properties'
                            else wkt if c == 'geom'
                            else row.get(c) if c != 'provider' else run.get('provider')
                            for c in snapshot_cols
                        ))
                    snapshot_template = '(' + ','.join(['%s'] * 15 + ['ST_GeomFromText(%s,4326)'] + ['%s'] * 4) + ')'
                    execute_values(cur,
                        f"INSERT INTO live.gfw_vessel_presence_snapshots ({','.join(snapshot_cols)}) VALUES %s ON CONFLICT DO NOTHING",
                        values, template=snapshot_template, page_size=500)

                    current_cols = [
                        'provider', 'vessel_id', 'source_dataset_id', 'source_snapshot_date', 'observed_at',
                        'received_at', 'source_event_key', 'record_hash', 'mmsi', 'ship_name', 'vessel_type',
                        'flag', 'longitude', 'latitude', 'geom', 'presence_quality', 'quality_flags',
                        'source_run_id', 'updated_at',
                    ]
                    # Same vessel can appear in multiple corridors; keep the latest row per vessel in this run.
                    latest = {}
                    for row in snapshots:
                        latest[row['vessel_id']] = row
                    current_values = []
                    for row in latest.values():
                        lon, lat = row.get('longitude'), row.get('latitude')
                        wkt = f'POINT({lon} {lat})' if lon is not None and lat is not None else None
                        current_values.append(tuple(
                            Json(row.get(c) or []) if c == 'quality_flags'
                            else wkt if c == 'geom'
                            else run.get('provider') if c == 'provider'
                            else run.get('source_dataset_id') if c == 'source_dataset_id'
                            else row.get('snapshot_date') if c == 'source_snapshot_date'
                            else row.get('run_id') if c == 'source_run_id'
                            else datetime.now().isoformat() if c == 'updated_at'
                            else row.get(c)
                            for c in current_cols
                        ))
                    current_template = '(' + ','.join(['%s'] * 14 + ['ST_GeomFromText(%s,4326)'] + ['%s'] * 4) + ')'
                    update_cols = [c for c in current_cols if c not in ('provider', 'vessel_id')]
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)
                    execute_values(cur,
                        f"INSERT INTO live.gfw_vessel_presence_current ({','.join(current_cols)}) VALUES %s "
                        f"ON CONFLICT (provider,vessel_id) DO UPDATE SET {update_set} "
                        "WHERE live.gfw_vessel_presence_current.source_snapshot_date < EXCLUDED.source_snapshot_date "
                        "OR (live.gfw_vessel_presence_current.source_snapshot_date = EXCLUDED.source_snapshot_date "
                        "AND live.gfw_vessel_presence_current.observed_at <= EXCLUDED.observed_at)",
                        current_values, template=current_template, page_size=500)
            logger.info(
                f"[gfw_vessel_presence] ✓ run {run['run_id']} status={run.get('status')} "
                f"snapshots={len(snapshots)} submitted={len(submitted_snapshots)}"
            )
            return

        if collector_name in ('animal_veterinary_clinics', 'animal_licensed_pet_businesses', 'animal_protection_offices'):
            runs = [r for r in records if r.get('_type') == 'run']
            if len(runs) != 1:
                raise ValueError(f'{collector_name} requires exactly one run ledger')
            run = runs[0]
            snapshots = [r for r in records if r.get('_type') == 'snapshot']
            if run['is_complete'] and len(snapshots) != run['row_count']:
                raise ValueError(f'{collector_name} row_count does not match snapshots')
            with self._txn(conn) as cur:
                run_cols = ['run_id', 'status', 'started_at', 'completed_at', 'snapshot_date',
                            'row_count', 'payload_sha256', 'is_complete', 'source_dataset_id',
                            'source_observed_at', 'quality_note']
                execute_values(cur,
                    f"INSERT INTO live.animal_welfare_point_runs ({','.join(run_cols)}) VALUES %s",
                    [(run['run_id'], 'running' if run['is_complete'] else 'failed', run['collected_at'],
                      None if run['is_complete'] else run['collected_at'], run.get('snapshot_date'),
                      run.get('row_count'), run.get('payload_sha256'), run['is_complete'],
                      run.get('source_dataset_id'), run.get('source_observed_at'), run.get('quality_note'))])
                if run['is_complete']:
                    cols = ['snapshot_date', 'run_id', 'source_dataset_id', 'source_record_key', 'canonical_entity_key',
                            'point_type', 'service_tags', 'name', 'county_code', 'county_name', 'address',
                            'phone', 'status_norm', 'status_raw', 'valid_from', 'valid_to', 'longitude', 'latitude', 'geom',
                            'geocode_method', 'geocode_confidence', 'details', 'record_hash', 'quality_flags',
                            'collected_at', 'source_observed_at']
                    values = [tuple(Json(r.get(c) or []) if c == 'quality_flags'
                                    else Json(r.get(c) or {}) if c == 'details' else r.get(c) for c in cols)
                              for r in snapshots]
                    execute_values(cur,
                        f"INSERT INTO live.animal_welfare_point_snapshots ({','.join(cols)}) VALUES %s",
                        values, page_size=500)
                    cur.execute("SELECT live.finalize_animal_welfare_point_run(%s::uuid)", (run['run_id'],))
            logger.info(f"[{collector_name}] ✓ run {run['run_id']} status={'succeeded' if run['is_complete'] else 'failed'} snapshots={len(snapshots)}")
            return
        if collector_name in ('animal_shelter_outcomes', 'animal_shelter_pressure'):
            # 41236/73396 都是完整歷史月報；run 與 immutable rows 必須同 transaction。
            # 失敗/部分回應只插入 failed ledger，絕不清空或覆寫既有月報。
            runs = [r for r in records if r.get('_type') == 'run']
            if len(runs) != 1:
                raise ValueError(f'{collector_name} requires exactly one run ledger')
            run = runs[0]
            outcomes = [r for r in records if r.get('_type') == 'outcome']
            if run['is_complete'] and len(outcomes) != run['row_count']:
                raise ValueError(f'{collector_name} row_count does not match outcomes')

            with self._txn(conn) as cur:
                run_cols = [
                    'run_id', 'status', 'started_at', 'completed_at', 'snapshot_date',
                    'row_count', 'payload_sha256', 'is_complete', 'source_dataset_id',
                    'source_observed_at', 'quality_note',
                ]
                initial_status = 'running' if run['is_complete'] else 'failed'
                run_values = [(
                    run['run_id'], initial_status, run['collected_at'],
                    run['collected_at'] if not run['is_complete'] else None,
                    run.get('snapshot_date'), run.get('row_count'), run.get('payload_sha256'),
                    run['is_complete'], run.get('source_dataset_id'),
                    run.get('source_observed_at'), run.get('quality_note'),
                )]
                execute_values(
                    cur,
                    f"INSERT INTO live.animal_shelter_outcome_runs ({','.join(run_cols)}) VALUES %s "
                    "ON CONFLICT (run_id) DO NOTHING",
                    run_values,
                )
                if run['is_complete']:
                    cols = [
                        'snapshot_date', 'run_id', 'source_dataset_id', 'source_record_key',
                        'source_id', 'report_year', 'source_report_year', 'report_month',
                        'county_code', 'county_name', 'report_grain_key', 'revision_no',
                        'duplicate_grain_count', 'metrics', 'record_hash', 'collected_at',
                        'quality_flags',
                    ]
                    values = [
                        tuple(
                            Json(r.get(c) or []) if c == 'quality_flags'
                            else Json(r.get(c) or {}) if c == 'metrics'
                            else r.get(c)
                            for c in cols
                        )
                        for r in outcomes
                    ]
                    execute_values(
                        cur,
                        f"INSERT INTO live.animal_shelter_outcomes ({','.join(cols)}) VALUES %s "
                        "ON CONFLICT DO NOTHING",
                        values,
                        page_size=500,
                    )
                    # Finalizer validates source-specific invariants and atomically projects
                    # live rows to analytics.*_monthly; it also stamps completed/finalized_at.
                    cur.execute(
                        "SELECT live.finalize_animal_shelter_outcome_run(%s::uuid)",
                        (run['run_id'],),
                    )
            logger.info(
                f"[{collector_name}] ✓ run {run['run_id']} "
                f"status={'succeeded' if run['is_complete'] else 'failed'} outcomes={len(outcomes)}"
            )

        elif collector_name == 'animal_adoption':
            # Contract owned with gis-platform migration. A failed/partial response only
            # writes its ledger; it must never prune current or manufacture a daily zero.
            runs = [r for r in records if r.get('_type') == 'run']
            if len(runs) != 1:
                raise ValueError('animal_adoption requires exactly one run ledger')
            run = runs[0]
            animals = [r for r in records if r.get('_type') == 'animal']

            with self._txn(conn) as cur:
                run_cols = [
                    'run_id', 'status', 'started_at', 'completed_at', 'snapshot_date',
                    'row_count', 'payload_sha256', 'is_complete', 'source_dataset_id',
                    'source_observed_at', 'quality_note',
                ]
                # 完整名單也先是 running；只有 snapshots 全插入且 finalize 成功後
                # platform function 才會把它標成 succeeded。
                initial_status = 'running' if run['is_complete'] else run['run_status']
                run_values = [(
                    run['run_id'], initial_status, run['collected_at'], run['collected_at'],
                    run.get('snapshot_date'), run.get('row_count'), run.get('payload_sha256'),
                    run['is_complete'], run.get('source_dataset_id'), run.get('source_observed_at'),
                    run.get('quality_note'),
                )]
                execute_values(
                    cur,
                    f"INSERT INTO live.animal_adoption_snapshot_runs ({','.join(run_cols)}) VALUES %s "
                    "ON CONFLICT (run_id) DO NOTHING",
                    run_values,
                )

                if run['is_complete']:
                    if not animals:
                        raise ValueError('complete animal_adoption run has no snapshots')
                    snapshot_cols = [
                        'collected_at', 'run_id', 'snapshot_date', 'source_dataset_id',
                        'source_record_key', 'animal_id_raw', 'animal_subid_raw',
                        'shelter_id', 'shelter_name', 'county_code', 'animal_kind',
                        'animal_sex', 'animal_age', 'animal_colour', 'animal_breed',
                        'animal_bodytype', 'animal_sterilization', 'animal_bacterin',
                        'animal_foundplace', 'animal_place', 'animal_title', 'animal_opendate',
                        'animal_closeddate', 'animal_remark', 'animal_caption', 'image_url',
                        'shelter_address', 'shelter_tel', 'source_status', 'source_observed_at',
                        'record_hash', 'quality_flags',
                    ]
                    values = [
                        tuple(
                            Json(r.get(c) or []) if c == 'quality_flags'
                            else (run['collected_at'] if c == 'collected_at' else r.get(c))
                            for c in snapshot_cols
                        )
                        for r in animals
                    ]
                    execute_values(
                        cur,
                        f"INSERT INTO live.animal_adoption_snapshots ({','.join(snapshot_cols)}) VALUES %s "
                        "ON CONFLICT (snapshot_date, run_id, source_record_key) DO NOTHING",
                        values,
                        page_size=500,
                    )
                    # Platform function handles missing_once / second-run not_listed,
                    # current upserts and daily grain atomically with this ledger.
                    cur.execute(
                        "SELECT live.finalize_animal_adoption_snapshot(%s::uuid)",
                        (run['run_id'],),
                    )
            logger.info(
                f"[animal_adoption] ✓ run {run['run_id']} "
                f"status={initial_status} snapshots={len(animals)}"
            )

        elif collector_name == 'freeway_vd':
            sections = [r for r in records if r.get('_type') == 'section']
            vds = [r for r in records if r.get('_type') == 'vd']

            with self._txn(conn) as cur:
                if sections:
                    cols = ['section_id', 'travel_speed', 'travel_time', 'congestion_level', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in sections]
                    execute_values(cur, f"INSERT INTO live.freeway_sections ({','.join(cols)}) VALUES %s", values, page_size=1000)
                    # current 表
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in cols if c != 'section_id')
                    execute_values(cur, f"INSERT INTO live.freeway_sections_current ({','.join(cols)}) VALUES %s ON CONFLICT (section_id) DO UPDATE SET {update_set}", values, page_size=1000)

                if vds:
                    cols = ['vd_id', 'total_volume', 'avg_speed', 'avg_occupancy', 'volume_small_car', 'volume_large_car', 'volume_trailer', 'lane_count', 'status', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in vds]
                    execute_values(cur, f"INSERT INTO live.freeway_vd_traffic ({','.join(cols)}) VALUES %s", values, page_size=1000)

            logger.info(f"[freeway_vd] ✓ sections {len(sections)} + vd {len(vds)} 筆寫入")

        elif collector_name == 'flight_fr24':
            trails = [r for r in records if r.get('_type') == 'trail']
            with self._txn(conn) as cur:
                if trails:
                    cols = ['flight_id', 'callsign', 'aircraft_type', 'registration', 'origin', 'destination', 'status', 'trail', 'trail_points', 'geom', 'collected_at']
                    values = [tuple(r.get(c) for c in cols) for r in trails]
                    execute_values(cur, f"INSERT INTO live.flight_trails ({','.join(cols)}) VALUES %s", values, page_size=100)
            logger.info(f"[flight_fr24] ✓ {len(trails)} 筆航跡寫入")

        elif collector_name in ('road_event_live', 'road_event_planned'):
            # TDX RoadEvent：同表 history append + current upsert (PK: event_id, source)
            #
            # Dedup（方案 B）：history 只寫 LastUpdateTime 變動的事件。
            # 大部分事件持續多輪抓取（道路施工常常 1-3 個月），如果每 5 min 都
            # append 一筆完全相同的 row → history 會暴增 ~95%+。
            # 改：寫入前先撈 current.last_updated，過濾未變動的事件不寫 history；
            # current 仍全部 UPSERT 維持最新狀態。
            cols = [
                'event_id', 'source', 'event_type', 'event_subtype', 'event_step', 'severity',
                'road_name', 'road_class', 'direction', 'start_km', 'end_km',
                'blocked_lanes', 'regulations', 'block_way', 'impact_description',
                'title', 'description', 'location_other', 'geom',
                'effective_time', 'expire_time', 'published_at', 'last_updated', 'collected_at',
                'matched_section_id', 'matched_section_name', 'matched_road_id',
                'enrich_status', 'raw_json',
            ]
            valid = [r for r in records if r.get('event_id') and r.get('source')]
            if not valid:
                logger.info(f"[{collector_name}] ✓ 0 筆寫入")
                return

            # 同批次去重：(event_id, source) 取最後一筆（避免 ON CONFLICT 撞同列兩次）
            seen: dict = {}
            for r in valid:
                seen[(r['event_id'], r['source'])] = r
            dedup = list(seen.values())

            update_cols = [c for c in cols if c not in ('event_id', 'source')]
            update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in update_cols)

            with self._txn(conn) as cur:
                # 1. 撈 current 實質欄位，過濾「內容未變」的事件
                #
                # 注意：TDX LastUpdateTime 每 5 min 都會刷新（即使事件實質沒變），
                # 實測 71% 事件 last_updated 變但 description 不變。所以 dedup
                # 必須比對「實質內容欄位」而非 last_updated。
                CONTENT_COLS = (
                    'description', 'event_step', 'severity',
                    'blocked_lanes', 'impact_description',
                )
                ev_ids = list({r['event_id'] for r in dedup})
                src_set = list({r['source'] for r in dedup})
                cur.execute(
                    f"SELECT event_id, source, {','.join(CONTENT_COLS)} "
                    f"FROM live.road_events_current "
                    f"WHERE event_id = ANY(%s) AND source = ANY(%s)",
                    (ev_ids, src_set),
                )
                prev_content: dict = {
                    (row[0], row[1]): row[2:] for row in cur.fetchall()
                }

                new_records = []
                for r in dedup:
                    key = (r['event_id'], r['source'])
                    cur_content = tuple(r.get(c) for c in CONTENT_COLS)
                    prev = prev_content.get(key)
                    # 首次出現 OR 實質內容變動 → 寫 history
                    if prev is None or cur_content != prev:
                        new_records.append(r)

                # 2. history 只寫變動的（首次出現 / LastUpdateTime 變過）
                if new_records:
                    new_values = [tuple(r.get(c) for c in cols) for r in new_records]
                    execute_values(
                        cur,
                        f"INSERT INTO live.road_events ({','.join(cols)}) VALUES %s",
                        new_values, page_size=500,
                    )

                # 3. current 全部 UPSERT（即使欄位未變也 noop，無副作用）
                all_values = [tuple(r.get(c) for c in cols) for r in dedup]
                execute_values(
                    cur,
                    f"INSERT INTO live.road_events_current ({','.join(cols)}) VALUES %s "
                    f"ON CONFLICT (event_id, source) DO UPDATE SET {update_set}",
                    all_values, page_size=500,
                )

                # 4. cleanup current — 過期事件移除
                cur.execute(
                    "DELETE FROM live.road_events_current "
                    "WHERE expire_time IS NOT NULL AND expire_time < now()"
                )
                expired = cur.rowcount

            skipped = len(dedup) - len(new_records)
            logger.info(
                f"[{collector_name}] ✓ history {len(new_records)} 新 / skip {skipped} 未變 "
                f"/ current {len(dedup)} / cleanup {expired} 過期"
            )

        elif collector_name == 'launch':
            launches = [r for r in records if r.get('_type') == 'launch']
            pads = [r for r in records if r.get('_type') == 'pad']
            events = [r for r in records if r.get('_type') == 'event']

            with self._txn(conn) as cur:
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
                        f"INSERT INTO live.launches ({','.join(cols)}) VALUES %s "
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
                        f"INSERT INTO live.launch_pads ({','.join(cols)}) VALUES %s "
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
                        f"INSERT INTO live.launch_events ({','.join(cols)}) VALUES %s "
                        f"ON CONFLICT (id) DO UPDATE SET {update_set}",
                        values, page_size=500)

            logger.info(f"[launch] ✓ {len(launches)} launches + {len(pads)} pads + {len(events)} events 寫入")

        elif collector_name == 'earthquake':
            # 地震：事件 / 逐站觀測 / 海嘯 三張表（gis-platform migration 321）
            #   live.earthquake_events      UNIQUE(event_id) → DO UPDATE（報告會修訂）
            #   live.earthquake_station_obs UNIQUE(event_id, station_id) → DO NOTHING
            #   live.tsunami_alerts         UNIQUE(tsunami_no, report_no, issued_at) → DO NOTHING
            events = [r for r in records if r.get('_type') == 'event']
            stations = [r for r in records if r.get('_type') == 'station']
            tsunami = [r for r in records if r.get('_type') == 'tsunami']

            with self._txn(conn) as cur:
                if events:
                    cols = ['event_id', 'magnitude', 'depth_km', 'epicenter_lat', 'epicenter_lng',
                            'location_desc', 'occurred_at', 'report_type', 'geom', 'raw_data']
                    # 同批去重：同 event_id 取最後一筆（ON CONFLICT 不能在同一 INSERT 更新同列兩次）
                    seen = {r['event_id']: tuple(r.get(c) for c in cols)
                            for r in events if r.get('event_id')}
                    values = list(seen.values())
                    if values:
                        update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in cols if c != 'event_id')
                        execute_values(
                            cur,
                            f"INSERT INTO live.earthquake_events ({','.join(cols)}) VALUES %s "
                            f"ON CONFLICT (event_id) DO UPDATE SET {update_set}",
                            values, page_size=500,
                        )

                if stations:
                    cols = ['event_id', 'earthquake_no', 'origin_time', 'source_type',
                            'station_id', 'station_name', 'county_name', 'area_desc',
                            'area_intensity', 'lat', 'lon', 'epicenter_distance_km',
                            'back_azimuth', 'seismic_intensity', 'intensity_value',
                            'pga_ew', 'pga_ns', 'pga_v', 'pga_int',
                            'pgv_ew', 'pgv_ns', 'pgv_v', 'pgv_int',
                            'wave_image_uri', 'geom', 'collected_at']
                    seen = {(r.get('event_id'), r.get('station_id')): tuple(r.get(c) for c in cols)
                            for r in stations if r.get('event_id') and r.get('station_id')}
                    values = list(seen.values())
                    if values:
                        execute_values(
                            cur,
                            f"INSERT INTO live.earthquake_station_obs ({','.join(cols)}) VALUES %s "
                            f"ON CONFLICT (event_id, station_id) DO NOTHING",
                            values, page_size=1000,
                        )

                if tsunami:
                    cols = ['tsunami_no', 'report_no', 'report_type', 'report_color',
                            'report_content', 'issued_at', 'valid_end_at', 'origin_time',
                            'source', 'epicenter_location', 'epicenter_lat', 'epicenter_lon',
                            'focal_depth_km', 'magnitude', 'web_url', 'station_details',
                            'raw', 'geom', 'collected_at']
                    seen = {(r.get('tsunami_no'), r.get('report_no'), r.get('issued_at')):
                            tuple(r.get(c) for c in cols)
                            for r in tsunami if r.get('tsunami_no') and r.get('issued_at')}
                    values = list(seen.values())
                    if values:
                        execute_values(
                            cur,
                            f"INSERT INTO live.tsunami_alerts ({','.join(cols)}) VALUES %s "
                            f"ON CONFLICT (tsunami_no, report_no, issued_at) DO NOTHING",
                            values, page_size=200,
                        )

            logger.info(
                f"[earthquake] ✓ events {len(events)} + stations {len(stations)} "
                f"+ tsunami {len(tsunami)} 筆寫入"
            )

        elif collector_name == 'power_taipower':
            # 台電即時電力供需：單一 collector 寫 3 張表，皆 ON CONFLICT DO NOTHING
            system = [r for r in records if r.get('_type') == 'system']
            units = [r for r in records if r.get('_type') == 'unit']
            regions = [r for r in records if r.get('_type') == 'region']

            with self._txn(conn) as cur:
                # 1) 系統供需 — UNIQUE(observed_at)
                if system:
                    cols = [
                        'observed_at', 'curr_load_mw', 'curr_util_rate',
                        'fore_maxi_sply_capacity_mw', 'fore_peak_dema_load_mw',
                        'fore_peak_resv_capacity_mw', 'fore_peak_resv_rate',
                        'fore_peak_resv_indicator', 'fore_peak_hour_range',
                        'yday_peak_resv_rate', 'yday_peak_resv_indicator',
                        'real_hr_maxi_sply_capacity_mw', 'real_hr_peak_time',
                        'publish_time', 'collected_at',
                    ]
                    # 同批去重：同 observed_at 取最後一筆
                    seen = {r.get('observed_at'): tuple(r.get(c) for c in cols)
                            for r in system if r.get('observed_at')}
                    values = list(seen.values())
                    if values:
                        execute_values(
                            cur,
                            f"INSERT INTO live.power_system_status ({','.join(cols)}) VALUES %s "
                            f"ON CONFLICT (observed_at) DO NOTHING",
                            values, page_size=100,
                        )

                # 2) 各機組 — UNIQUE(fuel_type, unit_name, observed_at)
                #    通用桶名（如 '其它台電自有'）會跨燃料別重複，自然鍵必含 fuel_type 否則塌列
                if units:
                    cols = [
                        'observed_at', 'fuel_type', 'unit_name', 'capacity_mw',
                        'net_gen_mw', 'util_rate', 'note', 'collected_at',
                    ]
                    seen = {(r.get('fuel_type'), r.get('unit_name'), r.get('observed_at')): tuple(r.get(c) for c in cols)
                            for r in units if r.get('unit_name') and r.get('observed_at')}
                    values = list(seen.values())
                    if values:
                        execute_values(
                            cur,
                            f"INSERT INTO live.power_generation_unit ({','.join(cols)}) VALUES %s "
                            f"ON CONFLICT (fuel_type, unit_name, observed_at) DO NOTHING",
                            values, page_size=1000,
                        )

                # 3) 區域用電 — UNIQUE(region, observed_at)
                if regions:
                    cols = [
                        'observed_at', 'region', 'generation_mw',
                        'consumption_mw', 'collected_at',
                    ]
                    seen = {(r.get('region'), r.get('observed_at')): tuple(r.get(c) for c in cols)
                            for r in regions if r.get('region') and r.get('observed_at')}
                    values = list(seen.values())
                    if values:
                        execute_values(
                            cur,
                            f"INSERT INTO live.power_region_demand ({','.join(cols)}) VALUES %s "
                            f"ON CONFLICT (region, observed_at) DO NOTHING",
                            values, page_size=100,
                        )

            logger.info(
                f"[power_taipower] ✓ system {len(system)} + units {len(units)} "
                f"+ regions {len(regions)} 筆寫入"
            )

        elif collector_name == 'pla_tracks_vectorize':
            # 共機航跡向量化：形狀 + 表格項次 + ledger 三張表
            tracks = [r for r in records if r.get('_type') == 'track']
            items = [r for r in records if r.get('_type') == 'item']
            runs = [r for r in records if r.get('_type') == 'run']
            days = sorted({r['report_date'] for r in runs})

            with self._txn(conn) as cur:
                # 1) 活動區多邊形 — 先清該日再寫。
                #    UPSERT 不夠：重跑若抽出較少形狀，舊的多餘 shape_no 會殘留成幽靈多邊形。
                if days:
                    cur.execute("DELETE FROM spatial.pla_tracks WHERE report_date = ANY(%s::date[])",
                                (days,))
                if tracks:
                    cols = [
                        'report_date', 'shape_no', 'geom', 'shape_kind', 'vertices',
                        'table_items', 'balloon_items', 'needs_review', 'guided',
                        'edge_precision', 'red_recall',
                    ]
                    values = [tuple(r.get(c) for c in cols) for r in tracks]
                    execute_values(
                        cur,
                        f"INSERT INTO spatial.pla_tracks ({','.join(cols)}) VALUES %s",
                        values, page_size=500,
                    )
                    # chart_url 是 daily 表的欄位，不在 GeoJSON 重複存 → 寫完回填
                    cur.execute(
                        "UPDATE spatial.pla_tracks t SET chart_url = d.track_chart_url "
                        "FROM live.pla_activity_daily d "
                        "WHERE d.report_date = t.report_date AND t.report_date = ANY(%s::date[])",
                        (days,),
                    )

                # 2) 表格項次（機型／架次／時段）— PK (report_date, item_no)
                if items:
                    cols = ['report_date', 'item_no', 'sorties', 'kinds',
                            'is_balloon', 'balloon_count', 'time_window', 'ocr_text']
                    values = [tuple(r.get(c) for c in cols) for r in items]
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in cols[2:])
                    execute_values(
                        cur,
                        f"INSERT INTO live.pla_activity_items ({','.join(cols)}) VALUES %s "
                        f"ON CONFLICT (report_date, item_no) DO UPDATE SET {update_set}",
                        values, page_size=500,
                    )

                # 3) ledger — 每個處理過的日子一列，是本 collector 真正的心跳
                #    （0 形狀是合法結果，光看 pla_tracks 分不出「沒共機」與「沒跑」）
                if runs:
                    cols = [
                        'report_date', 'plate_ok', 'plate_size', 'georef_dev',
                        'expected', 'extracted', 'balloon_items', 'guided', 'ok',
                        'edge_precision', 'red_recall', 'chart_s3_key', 'error',
                    ]
                    values = [tuple(r.get(c) for c in cols) for r in runs]
                    update_set = ','.join(f'{c}=EXCLUDED.{c}' for c in cols[1:])
                    execute_values(
                        cur,
                        f"INSERT INTO spatial.pla_tracks_runs ({','.join(cols)}) VALUES %s "
                        f"ON CONFLICT (report_date) DO UPDATE SET {update_set}, run_at = now()",
                        values, page_size=100,
                    )

            logger.info(
                f"[pla_tracks_vectorize] ✓ {len(days)} 天 / 形狀 {len(tracks)} "
                f"+ 項次 {len(items)} 筆寫入"
            )

    def _write_satellite_tle(self, conn, result: dict, timestamp: datetime):
        """更新衛星 TLE 參數表（全量 UPSERT，供前端 SGP4 計算用）"""
        # Space-Track 版：data_all 含全部衛星（活躍+失效）；舊格式 fallback 用 data
        data = result.get('data_all') or result.get('data', [])
        if not data:
            return

        cols = ['norad_id', 'name', 'intl_designator', 'constellation', 'orbit_type',
                'tle_line1', 'tle_line2', 'tle_epoch', 'inclination', 'eccentricity', 'period_min',
                'decay_date', 'is_decayed', 'object_type', 'updated_at']
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
                r.get('decay_date'),
                bool(r.get('is_decayed', False)),
                r.get('object_type') or None,
                timestamp.isoformat(),
            ))

        if values:
            with self._txn(conn) as cur:
                sql = f"INSERT INTO live.satellite_tle ({','.join(cols)}) VALUES %s ON CONFLICT (norad_id) DO UPDATE SET {update_set}"
                execute_values(cur, sql, values, page_size=1000)
            decayed = sum(1 for v in values if v[12])
            logger.info(f"[satellite] ✓ TLE 表已更新 {len(values)} 筆（其中失效 {decayed} 筆）")

            # 同步寫入 TLE 歷史表（用於變軌偵測）— 共用同一條 conn
            self._write_satellite_tle_history(conn, values, timestamp)

    def _write_satellite_tle_history(self, conn, tle_values: list, timestamp: datetime):
        """追加 TLE 歷史紀錄，同一 norad_id + tle_epoch 不重複寫入"""
        hist_cols = ['norad_id', 'name', 'constellation', 'orbit_type',
                     'tle_line1', 'tle_line2', 'tle_epoch',
                     'inclination', 'eccentricity', 'period_min',
                     'decay_date', 'is_decayed', 'object_type', 'fetched_at']

        # tle_values 欄位順序：norad_id(0), name(1), intl_des(2), constellation(3), orbit_type(4),
        #   tle_line1(5), tle_line2(6), tle_epoch(7), inclination(8), eccentricity(9),
        #   period_min(10), decay_date(11), is_decayed(12), object_type(13), updated_at(14)
        hist_values = [
            (v[0], v[1], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10],
             v[11], v[12], v[13], timestamp.isoformat())
            for v in tle_values
        ]

        try:
            with self._txn(conn) as cur:
                sql = (f"INSERT INTO live.satellite_tle_history ({','.join(hist_cols)}) "
                       f"VALUES %s ON CONFLICT (norad_id, tle_epoch) DO NOTHING")
                execute_values(cur, sql, hist_values, page_size=1000)
            logger.info(f"[satellite] ✓ TLE 歷史已追加（新 epoch 才寫入）")
        except Exception as e:
            logger.warning(f"[satellite] TLE 歷史寫入失敗（表可能尚未建立）: {e}")

    def _write_schedules(self, conn, records: list[dict]):
        """寫入每日時刻表到 reference.daily_schedules"""
        with self._txn(conn) as cur:
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
        # 容量上限（比照 vm_buffer.MAX_BUFFER_FILES 模式）：超量先刪最舊再寫新檔，
        # 防 DB 長期不可用時塞爆 volume。檔名開頭是 collector 名、字典序≠時間序，
        # 故以 mtime 判定最舊（vm_buffer 單 collector 一目錄才可用檔名排序）。
        try:
            existing = sorted(BUFFER_DIR.glob('*.json'), key=lambda p: p.stat().st_mtime)
            overflow = len(existing) - (config.SUPABASE_BUFFER_MAX_FILES - 1)
            if overflow > 0:
                for old in existing[:overflow]:
                    old.unlink(missing_ok=True)
                logger.warning(
                    f"Buffer 達上限 {config.SUPABASE_BUFFER_MAX_FILES} 檔，"
                    f"丟棄最舊 {overflow} 檔"
                )
        except Exception as e:
            logger.warning(f"Buffer 容量檢查失敗（仍繼續寫入）: {e}")

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

    def _report_heartbeat(self, collector_name: str, success: bool, records: int = 0,
                          error: str = None, conn=None):
        """Best-effort 心跳回報。借不到連線或失敗都靜默 — 不能影響主寫入路徑。

        conn 給定時（write() 成功路徑）：直接在該連線上以獨立 transaction 跑，
        不另外 borrow — 主寫入已 commit，心跳失敗只 rollback 心跳自己。
        conn 未給定時（失敗路徑）：維持原樣自行借一條短 timeout 連線。
        """
        try:
            if conn is not None:
                self._exec_heartbeat(conn, collector_name, success, records, error)
            else:
                # 短 timeout：心跳只是觀測，不該為了它等很久
                with self._pool.borrow(timeout=1) as borrowed:
                    self._exec_heartbeat(borrowed, collector_name, success, records, error)
        except Exception as e:
            logger.debug(f"心跳回報失敗: {e}")

    def _exec_heartbeat(self, conn, collector_name: str, success: bool, records: int, error: str):
        """在給定連線上執行心跳 SQL（獨立 transaction，失敗 raise 由呼叫端吞）。"""
        with self._txn(conn) as cur:
            cur.execute(
                "SELECT report_collector_heartbeat(%s, %s, %s, %s)",
                (collector_name, success, records, error)
            )
