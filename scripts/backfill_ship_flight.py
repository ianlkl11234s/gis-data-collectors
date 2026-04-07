#!/usr/bin/env python3
"""
從 S3 歷史歸檔回填 ship_positions / flight_positions 到 Supabase。
解壓每天的 tar.gz，解析 JSON，批次寫入。

用法：
    # 回填特定日期（船舶 + 航班）
    python3 scripts/backfill_ship_flight.py 2026-04-04 2026-04-05

    # 只回填船舶
    python3 scripts/backfill_ship_flight.py --ship-only 2026-04-05

    # 只回填航班
    python3 scripts/backfill_ship_flight.py --flight-only 2026-04-04

    # 試跑（不寫入 DB）
    python3 scripts/backfill_ship_flight.py --dry-run 2026-04-05
"""
import sys
import os
import json
import tarfile
import logging
from io import BytesIO
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.s3 import S3Storage

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ── DB URL ──
# 優先用 SUPABASE_DB_URL，fallback 到 gis-platform/.env 的 DATABASE_POOL_URL
DB_URL = (
    os.getenv('SUPABASE_DB_URL')
    or os.getenv('DATABASE_POOL_URL')
    or os.getenv('DATABASE_URL')
)

# ── 表定義 ──

SHIP_COLS = ['mmsi', 'ship_name', 'ship_type', 'lat', 'lng', 'speed', 'heading', 'collected_at', 'geom']
SHIP_SQL = f"INSERT INTO realtime.ship_positions ({','.join(SHIP_COLS)}) VALUES %s ON CONFLICT DO NOTHING"

FLIGHT_COLS = ['flight_id', 'callsign', 'aircraft_type', 'origin', 'destination',
               'lat', 'lng', 'altitude', 'speed', 'heading', 'collected_at', 'geom']
FLIGHT_SQL = f"INSERT INTO realtime.flight_positions ({','.join(FLIGHT_COLS)}) VALUES %s ON CONFLICT DO NOTHING"


# ── Transform ──

def transform_ship(record: dict, collected_at: str) -> tuple | None:
    """JSON record → ship_positions tuple"""
    lat = record.get('lat')
    lng = record.get('lon')
    if not lat or not lng:
        return None
    mmsi = str(record.get('mmsi', '')) if record.get('mmsi') else None
    if not mmsi:
        return None
    return (
        mmsi,
        (record.get('ship_name') or '').strip(),
        record.get('vessel_type_name', ''),
        lat,
        lng,
        record.get('sog'),
        record.get('heading'),
        collected_at,
        f'SRID=4326;POINT({lng} {lat})',
    )


def transform_flight(record: dict, collected_at: str) -> tuple | None:
    """JSON record → flight_positions tuple"""
    if not isinstance(record, dict):
        return None
    lat = record.get('latitude')
    lng = record.get('longitude')
    if not lat or not lng:
        return None
    return (
        record.get('icao24', ''),
        (record.get('callsign') or '').strip(),
        '',  # aircraft_type not available from OpenSky
        record.get('origin_country', ''),
        '',  # destination not available from OpenSky
        float(lat),
        float(lng),
        record.get('baro_altitude') or record.get('geo_altitude'),
        record.get('velocity'),
        record.get('true_track'),
        collected_at,
        f'SRID=4326;POINT({lng} {lat})',
    )


# ── Archive parsing ──

def parse_ship_archive(archive_bytes: bytes, date_str: str) -> list[list[tuple]]:
    """解壓 ship_ais tar.gz → 批次 tuple 列表"""
    batches = []
    with tarfile.open(fileobj=BytesIO(archive_bytes), mode='r:gz') as tar:
        members = sorted(tar.getmembers(), key=lambda m: m.name)
        for member in members:
            if not member.name.endswith('.json'):
                continue
            f = tar.extractfile(member)
            if not f:
                continue
            try:
                data = json.loads(f.read())
            except json.JSONDecodeError:
                logger.warning(f"  跳過無法解析: {member.name}")
                continue

            # _fetch_time 是台灣時間（無時區後綴），需加 +08:00 讓 PostgreSQL 正確轉 UTC
            fetch_time = data.get('fetch_time') or data.get('_fetch_time')
            if not fetch_time:
                time_part = member.name.replace('ship_ais_', '').replace('.json', '')
                try:
                    fetch_time = f"{date_str}T{time_part[:2]}:{time_part[2:]}:00+08:00"
                except (ValueError, IndexError):
                    fetch_time = f"{date_str}T00:00:00+08:00"
            elif '+' not in fetch_time and 'Z' not in fetch_time:
                fetch_time = fetch_time + '+08:00'

            records = data.get('data', [])
            values = []
            for r in records:
                row = transform_ship(r, fetch_time)
                if row:
                    values.append(row)

            if values:
                batches.append(values)
                logger.debug(f"  {member.name}: {len(values)} ships")

    return batches


def parse_flight_archive(archive_bytes: bytes, date_str: str) -> list[list[tuple]]:
    """解壓 flight_opensky tar.gz → 批次 tuple 列表"""
    batches = []
    with tarfile.open(fileobj=BytesIO(archive_bytes), mode='r:gz') as tar:
        members = sorted(tar.getmembers(), key=lambda m: m.name)
        for member in members:
            if not member.name.endswith('.json'):
                continue
            f = tar.extractfile(member)
            if not f:
                continue
            try:
                data = json.loads(f.read())
            except json.JSONDecodeError:
                logger.warning(f"  跳過無法解析: {member.name}")
                continue

            # flight 的 fetch_time 是 UTC（不同於 ship 的 _fetch_time 是台灣時間）
            # 證據：fetch_time '15:58' 對應檔名 '2358' = 台灣 23:58 = UTC 15:58
            fetch_time = data.get('fetch_time')
            if not fetch_time:
                # fallback 用 api_time（unix epoch）或檔名推算
                api_time = data.get('api_time')
                if api_time:
                    fetch_time = datetime.fromtimestamp(api_time, tz=None).isoformat() + '+00:00'
                else:
                    fetch_time = f"{date_str}T00:00:00+00:00"
            elif '+' not in fetch_time and 'Z' not in fetch_time:
                # 加上 UTC 後綴
                fetch_time = fetch_time + '+00:00'

            records = data.get('data', [])
            values = []
            for r in records:
                row = transform_flight(r, fetch_time)
                if row:
                    values.append(row)

            if values:
                batches.append(values)
                logger.debug(f"  {member.name}: {len(values)} flights")

    return batches


# ── Backfill ──

def backfill_ships(s3: S3Storage, conn, date_str: str, dry_run: bool = False) -> int:
    """回填單日船舶資料"""
    logger.info(f"[Ship] 下載 {date_str} 歸檔...")
    archive_bytes = s3.get_archive('ship_ais', date_str)
    if not archive_bytes:
        logger.warning(f"[Ship] 找不到 {date_str} 的歸檔")
        return 0

    batches = parse_ship_archive(archive_bytes, date_str)
    total = sum(len(b) for b in batches)
    logger.info(f"[Ship] {date_str}: {len(batches)} 檔, {total} 筆")

    if dry_run:
        logger.info(f"[Ship] (dry-run) 跳過寫入")
        return total

    with conn.cursor() as cur:
        for batch in batches:
            execute_values(cur, SHIP_SQL, batch, page_size=2000)
    conn.commit()
    logger.info(f"[Ship] {date_str}: {total} 筆已寫入")
    return total


def backfill_flights(s3: S3Storage, conn, date_str: str, dry_run: bool = False) -> int:
    """回填單日航班資料"""
    logger.info(f"[Flight] 下載 {date_str} 歸檔...")
    archive_bytes = s3.get_archive('flight_opensky', date_str)
    if not archive_bytes:
        logger.warning(f"[Flight] 找不到 {date_str} 的歸檔")
        return 0

    batches = parse_flight_archive(archive_bytes, date_str)
    total = sum(len(b) for b in batches)
    logger.info(f"[Flight] {date_str}: {len(batches)} 檔, {total} 筆")

    if dry_run:
        logger.info(f"[Flight] (dry-run) 跳過寫入")
        return total

    with conn.cursor() as cur:
        for batch in batches:
            execute_values(cur, FLIGHT_SQL, batch, page_size=2000)
    conn.commit()
    logger.info(f"[Flight] {date_str}: {total} 筆已寫入")
    return total


def main():
    import argparse
    parser = argparse.ArgumentParser(description='從 S3 歸檔回填 ship/flight 到 Supabase')
    parser.add_argument('dates', nargs='+', help='要回填的日期 (YYYY-MM-DD)')
    parser.add_argument('--ship-only', action='store_true', help='只回填船舶')
    parser.add_argument('--flight-only', action='store_true', help='只回填航班')
    parser.add_argument('--dry-run', action='store_true', help='試跑，不寫入 DB')
    parser.add_argument('--db-url', help='Supabase DB URL (覆蓋環境變數)')
    args = parser.parse_args()

    db_url = args.db_url or DB_URL
    if not db_url and not args.dry_run:
        logger.error("需要 DB 連線。設定環境變數 SUPABASE_DB_URL 或用 --db-url")
        sys.exit(1)

    do_ships = not args.flight_only
    do_flights = not args.ship_only

    s3 = S3Storage()

    if args.dry_run:
        logger.info("=== DRY RUN 模式 ===")
        conn = None
    else:
        conn = psycopg2.connect(db_url)
        logger.info(f"已連線 Supabase")

    ship_total = 0
    flight_total = 0

    for date_str in sorted(args.dates):
        try:
            if do_ships:
                ship_total += backfill_ships(s3, conn, date_str, args.dry_run)
            if do_flights:
                flight_total += backfill_flights(s3, conn, date_str, args.dry_run)
        except Exception as e:
            logger.error(f"{date_str} 失敗: {e}")
            if conn:
                conn.rollback()

    if conn:
        # 刷新 materialized view
        if ship_total > 0 or flight_total > 0:
            logger.info("刷新 materialized views...")
            with conn.cursor() as cur:
                if ship_total > 0:
                    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_ship_dates")
                if flight_total > 0:
                    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_flight_dates")
            conn.commit()
            logger.info("Materialized views 已更新")

        conn.close()

    logger.info(f"\n{'='*50}")
    logger.info(f"回填完成！Ship: {ship_total} 筆, Flight: {flight_total} 筆")
    if args.dry_run:
        logger.info("(dry-run 模式，未實際寫入)")


if __name__ == '__main__':
    main()
