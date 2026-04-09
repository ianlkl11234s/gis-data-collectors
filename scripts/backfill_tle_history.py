#!/usr/bin/env python3
"""
從 S3 歷史歸檔回填 satellite_tle_history 表。
解壓每天的 tar.gz，解析每個 satellite_HHMM.json，
提取 TLE 資料寫入歷史表。

用法：
    python3 scripts/backfill_tle_history.py           # 回填所有日期
    python3 scripts/backfill_tle_history.py 2026-03-28 # 回填指定日期
"""
import sys
import os
import json
import tarfile
import tempfile
import logging
from io import BytesIO
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.s3 import S3Storage
from config import SUPABASE_DB_URL

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

HIST_COLS = ['norad_id', 'name', 'constellation', 'orbit_type',
             'tle_line1', 'tle_line2', 'tle_epoch',
             'inclination', 'eccentricity', 'period_min', 'fetched_at']

def parse_archive(archive_bytes: bytes, date_str: str) -> list[list[tuple]]:
    """解壓 tar.gz，回傳每個 JSON 檔的 TLE 資料列表"""
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

            # 從檔名推算 fetched_at (satellite_HHMM.json)
            basename = os.path.basename(member.name)
            time_part = basename.replace('satellite_', '').replace('.json', '')
            try:
                fetched_at = datetime.strptime(
                    f"{date_str} {time_part[:2]}:{time_part[2:]}", "%Y-%m-%d %H:%M"
                ).isoformat()
            except ValueError:
                fetched_at = f"{date_str}T00:00:00"

            satellites = data.get('data', data) if isinstance(data, dict) else data
            if not isinstance(satellites, list):
                continue

            values = []
            for sat in satellites:
                if not isinstance(sat, dict):
                    continue
                tle1 = sat.get('tle_line1')
                tle2 = sat.get('tle_line2')
                if not tle1 or not tle2:
                    continue
                values.append((
                    sat.get('norad_id'),
                    sat.get('name', ''),
                    sat.get('constellation', ''),
                    sat.get('orbit_type', ''),
                    tle1, tle2,
                    sat.get('tle_epoch', ''),
                    sat.get('inclination'),
                    sat.get('eccentricity'),
                    sat.get('period_min'),
                    fetched_at,
                ))

            if values:
                batches.append(values)
                logger.info(f"  {basename}: {len(values)} 筆衛星")

    return batches


def backfill_date(s3: S3Storage, conn, date_str: str):
    """回填指定日期"""
    logger.info(f"下載 {date_str} 歸檔...")
    archive_bytes = s3.get_archive('satellite', date_str)
    if not archive_bytes:
        logger.warning(f"  找不到 {date_str} 的歸檔")
        return 0

    batches = parse_archive(archive_bytes, date_str)
    total = 0

    sql = (f"INSERT INTO realtime.satellite_tle_history ({','.join(HIST_COLS)}) "
           f"VALUES %s ON CONFLICT (norad_id, tle_epoch) DO NOTHING")

    with conn.cursor() as cur:
        for values in batches:
            execute_values(cur, sql, values, page_size=1000)
            total += len(values)

    conn.commit()
    logger.info(f"  {date_str} 完成: {total} 筆已處理（重複 epoch 自動跳過）")
    return total


def main():
    s3 = S3Storage()
    conn = psycopg2.connect(SUPABASE_DB_URL)

    # 決定要回填的日期
    if len(sys.argv) > 1:
        dates = [sys.argv[1]]
    else:
        dates = s3.list_dates('satellite')
        logger.info(f"找到 {len(dates)} 天歷史資料")

    grand_total = 0
    for date_str in sorted(dates):
        try:
            count = backfill_date(s3, conn, date_str)
            grand_total += count
        except Exception as e:
            logger.error(f"  {date_str} 失敗: {e}")
            conn.rollback()

    # 統計結果
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT norad_id) FROM realtime.satellite_tle_history")
        total_rows, unique_sats = cur.fetchone()

    conn.close()
    logger.info(f"\n回填完成！共 {grand_total} 筆處理")
    logger.info(f"歷史表現有: {total_rows} 筆紀錄，涵蓋 {unique_sats} 顆衛星")


if __name__ == '__main__':
    main()
