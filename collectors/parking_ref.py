"""
停車靜態座標參考收集器（半靜態，月更）

即時可用性端點（parking / parking_offstreet）不帶座標 → 比照 youbike「即時 join
靜態站點 ref」模式，本 collector 從 TDX v1 靜態端點抓座標，直接 upsert 進
spatial.parking_segments_ref + spatial.parking_lots_ref（不走 supabase_writer，
直接用 SUPABASE_DB_URL psycopg2 連線）。前端走 DEFINER RPC join 即時表。

資料來源（注意 v1 非 v2）：
  路邊 OnStreet：/v1/Parking/OnStreet/ParkingSegment/City/{City}
    - 台北有 Geometry(WKT POLYGON 街廓) + ParkingSegmentPosition(代表點)
    - 新北/台中僅代表點、無 Geometry
  場外 OffStreet：
    /v1/Parking/OffStreet/CarPark/City/{City}
    /v1/Parking/OffStreet/CarPark/Road/Freeway/ServiceArea
    /v1/Parking/OffStreet/CarPark/Tourism

uid 對齊（關鍵）：car_park_uid = <頂層 AuthorityCode>_<CarParkID>，
    與 live.parking_lots_current.car_park_uid 同構（例：TPE_TPE0032 / NFB_01F0502 /
    TBROC_SWC-02001）。頂層 AuthorityCode 對 ServiceArea=NFB、Tourism=TBROC，
    即使個別 record 的 CityCode 是縣市碼也一律用頂層碼（與即時收集器一致）。

更新頻率：半靜態，預設每 30 天（PARKING_REF_INTERVAL=43200），預設關閉手動觸發。
"""

from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

import config
from storage.db import connect_supabase
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector


_SEG_UPSERT = """
INSERT INTO spatial.parking_segments_ref
    (segment_id, city, geom, lon, lat, segment_name,
     fare_description, has_charging_point, updated_at)
VALUES %s
ON CONFLICT (segment_id) DO UPDATE SET
    city               = EXCLUDED.city,
    geom               = EXCLUDED.geom,
    lon                = EXCLUDED.lon,
    lat                = EXCLUDED.lat,
    segment_name       = EXCLUDED.segment_name,
    fare_description   = EXCLUDED.fare_description,
    has_charging_point = EXCLUDED.has_charging_point,
    updated_at         = now()
"""
_SEG_TEMPLATE = "(%s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, now())"

_LOT_UPSERT = """
INSERT INTO spatial.parking_lots_ref
    (car_park_uid, car_park_id, authority_code, city, lon, lat,
     car_park_name, address, car_park_type, is_public, ev_charging, updated_at)
VALUES %s
ON CONFLICT (car_park_uid) DO UPDATE SET
    car_park_id    = EXCLUDED.car_park_id,
    authority_code = EXCLUDED.authority_code,
    city           = EXCLUDED.city,
    lon            = EXCLUDED.lon,
    lat            = EXCLUDED.lat,
    car_park_name  = EXCLUDED.car_park_name,
    address        = EXCLUDED.address,
    car_park_type  = EXCLUDED.car_park_type,
    is_public      = EXCLUDED.is_public,
    ev_charging    = EXCLUDED.ev_charging,
    updated_at     = now()
"""
_LOT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())"


def _bool(v):
    """TDX 用 int 0/1 表示布林，缺值回 None。"""
    if v is None:
        return None
    return bool(v)


class ParkingRefCollector(BaseCollector):
    """停車靜態座標 ref 收集器（月更，直接 SQL 寫 spatial.*_ref）"""

    name = "parking_ref"
    interval_minutes = config.PARKING_REF_INTERVAL
    # TDX 抓 3 段路邊 + 8 城場外 + SA + Tourism，加上 upsert，給 10 分鐘 budget
    COLLECT_TIMEOUT = 600

    def __init__(self):
        super().__init__()
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch(self, path: str):
        """回傳 (records, 頂層 AuthorityCode)。"""
        url = f"{config.TDX_API_BASE}{path}"
        resp = self._session.get(
            url, headers=self.auth.get_auth_header(),
            params={'$format': 'JSON'}, timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            return [], None
        # OnStreet → ParkingSegments；OffStreet → CarParks
        records = data.get('ParkingSegments') or data.get('CarParks') or []
        return records, data.get('AuthorityCode')

    # ---- OnStreet ParkingSegment ----
    @staticmethod
    def _parse_segment(seg: dict) -> tuple:
        pos = seg.get('ParkingSegmentPosition') or {}
        name = seg.get('ParkingSegmentName') or {}
        return (
            seg.get('ParkingSegmentID'),
            seg.get('City'),
            seg.get('Geometry'),               # WKT，僅台北有；None → geom NULL
            pos.get('PositionLon'),
            pos.get('PositionLat'),
            name.get('Zh_tw') if isinstance(name, dict) else str(name),
            seg.get('FareDescription'),
            _bool(seg.get('HasChargingPoint')),
        )

    # ---- OffStreet CarPark ----
    @staticmethod
    def _parse_lot(lot: dict, authority: str, source_category: str) -> tuple:
        car_park_id = lot.get('CarParkID')
        pos = lot.get('CarParkPosition') or {}
        name = lot.get('CarParkName') or {}
        auth = authority or 'UNK'
        return (
            f"{auth}_{car_park_id}",            # car_park_uid（對齊即時表）
            car_park_id,
            authority,
            lot.get('City'),
            pos.get('PositionLon'),
            pos.get('PositionLat'),
            name.get('Zh_tw') if isinstance(name, dict) else str(name),
            lot.get('Address'),
            source_category,                    # car_park_type：city / freeway_service_area / tourism
            _bool(lot.get('IsPublic')),
            _bool(lot.get('EVRechargingAvailable')),
        )

    def _collect_segments(self, stats: dict) -> list:
        """抓路邊 OnStreet 三城，回傳 dedup 後 rows（by segment_id，last wins）。"""
        rows = {}
        stats['segments_by_city'] = {}
        stats['segments_with_geom'] = 0
        for city in config.PARKING_REF_ONSTREET_CITIES:
            try:
                recs, authority = self._fetch(
                    f"/v1/Parking/OnStreet/ParkingSegment/City/{city}")
                with_geom = 0
                for seg in recs:
                    sid = seg.get('ParkingSegmentID')
                    if not sid:
                        continue
                    row = self._parse_segment(seg)
                    rows[sid] = row
                    if row[2]:  # Geometry WKT present
                        with_geom += 1
                stats['segments_by_city'][city] = {
                    'authority': authority, 'fetched': len(recs), 'with_geom': with_geom}
                stats['segments_with_geom'] += with_geom
                print(f"   ✓ OnStreet {city}: {len(recs)} 段（geom {with_geom}）")
            except Exception as e:
                stats['segments_by_city'][city] = {'error': str(e)}
                print(f"   ✗ OnStreet {city}: {e}")
        return list(rows.values())

    def _collect_lots(self, stats: dict) -> list:
        """抓場外 OffStreet（城市 + 國道服務區 + 觀光），回傳 dedup 後 rows（by uid）。"""
        rows = {}
        stats['lots_by_source'] = {}

        def _add(label, path, source_category):
            try:
                recs, authority = self._fetch(path)
                added = 0
                for lot in recs:
                    if not lot.get('CarParkID'):
                        continue
                    row = self._parse_lot(lot, authority, source_category)
                    rows[row[0]] = row
                    added += 1
                stats['lots_by_source'][label] = {
                    'authority': authority, 'fetched': len(recs), 'kept': added}
                print(f"   ✓ OffStreet {label}: {len(recs)} 場（authority={authority}）")
            except Exception as e:
                stats['lots_by_source'][label] = {'error': str(e)}
                print(f"   ✗ OffStreet {label}: {e}")

        for city in config.PARKING_REF_OFFSTREET_CITIES:
            _add(f"city/{city}",
                 f"/v1/Parking/OffStreet/CarPark/City/{city}", 'city')
        _add('freeway_service_area',
             "/v1/Parking/OffStreet/CarPark/Road/Freeway/ServiceArea",
             'freeway_service_area')
        _add('tourism',
             "/v1/Parking/OffStreet/CarPark/Tourism", 'tourism')
        return list(rows.values())

    def collect(self) -> dict:
        if not config.SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL 未設定，無法寫入 spatial.*_ref")

        fetch_time = datetime.now()
        stats = {}

        seg_rows = self._collect_segments(stats)
        lot_rows = self._collect_lots(stats)

        # 交易性寫入：先在記憶體備妥所有 rows，再開連線 back-to-back upsert
        # （中間無 Python idle → 不會落入 idle_in_transaction_session_timeout）
        conn = connect_supabase(autocommit=False, statement_timeout_ms=120_000)
        try:
            with conn.cursor() as cur:
                if seg_rows:
                    execute_values(cur, _SEG_UPSERT, seg_rows,
                                   template=_SEG_TEMPLATE, page_size=500)
                if lot_rows:
                    execute_values(cur, _LOT_UPSERT, lot_rows,
                                   template=_LOT_TEMPLATE, page_size=500)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        stats['segments_upserted'] = len(seg_rows)
        stats['lots_upserted'] = len(lot_rows)
        print(f"\n   📊 parking_ref: segments {len(seg_rows)} | lots {len(lot_rows)} "
              f"| 台北 geom {stats.get('segments_with_geom', 0)}")

        return {
            'fetch_time': fetch_time.isoformat(),
            'data': [],  # 不走 supabase_writer，留空避免被當作未知表寫入
            **stats,
        }
