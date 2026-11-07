#!/usr/bin/env python3
"""
從 DB 影像表產生 R2 每日 manifest（一次性 bootstrap；可重跑、冪等）。

collector 上線後只會寫「之後」的幀，前端要能回放過去幾天，得先把 DB 裡現有的幀
補進 R2 manifest。契約見 docs/IMAGERY_R2_MANIFEST.md。

來源：
- cwa    live.cwa_imagery_frames          已有 image_key（AR-11 回填過）→ 只寫 manifest
- aqi    live.aqi_imagery_frames          已有 image_key → 只寫 manifest
                                          （image_key 為 NULL 的舊列先跑 backfill_imagery_r2.py --table aqi）
- precip live.precipitation_raster_frames 沒有 image_key 欄 → 先把 bytea 上傳 R2，再寫 manifest
                                          （key 規則同 collector，重傳同一張不會產生新物件）

用法：
    python3 scripts/bootstrap_imagery_manifest.py --source cwa --dry-run
    python3 scripts/bootstrap_imagery_manifest.py --source all --days 14
    python3 scripts/bootstrap_imagery_manifest.py --source precip --days 3

需環境變數：SUPABASE_DB_URL + R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT_URL / R2_BUCKET。
--dry-run 只讀 DB、印出每日幀數與範例 key，不寫 R2。
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SUPABASE_DB_URL  # noqa: E402
from collectors.air_quality_imagery import AQI_IMAGERY_BBOX  # noqa: E402
from collectors.precipitation_raster import imagery_r2_key as precip_r2_key  # noqa: E402
from storage.imagery_manifest import frame_entry, group_by_day, publish_frames  # noqa: E402
from storage.r2 import get_r2_storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SOURCES = ('cwa', 'aqi', 'precip')


def _day_windows(days: int):
    """由舊到新，每次一個 UTC 日（短查詢，pooler 友善）。"""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(days, -1, -1):
        start = today - timedelta(days=i)
        yield start, start + timedelta(days=1)


def _query(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.commit()
    return rows


def entries_cwa(conn, start, end):
    rows = _query(conn, """
        SELECT dataset_id, observed_at, image_key, mime_type, image_size,
               lon_min, lat_min, lon_max, lat_max
          FROM live.cwa_imagery_frames
         WHERE image_key IS NOT NULL AND observed_at >= %s AND observed_at < %s
         ORDER BY dataset_id, observed_at
    """, (start, end))
    return [frame_entry(r[0], r[1], r[2], r[3], r[4], (r[5], r[6], r[7], r[8])) for r in rows], 0


def entries_aqi(conn, start, end):
    rows = _query(conn, """
        SELECT product_type, observed_at, image_key, COALESCE(mime_type, 'image/png'), image_size
          FROM live.aqi_imagery_frames
         WHERE image_key IS NOT NULL AND observed_at >= %s AND observed_at < %s
         ORDER BY product_type, observed_at
    """, (start, end))
    return [frame_entry(r[0], r[1], r[2], r[3], r[4], AQI_IMAGERY_BBOX) for r in rows], 0


def entries_precip(conn, start, end, r2, dry_run):
    rows = _query(conn, """
        SELECT cumulative_hours, observed_at, image_bytes, image_size,
               ul_lat, ul_lng, br_lat, br_lng
          FROM live.precipitation_raster_frames
         WHERE is_empty = FALSE AND image_bytes IS NOT NULL
           AND observed_at >= %s AND observed_at < %s
         ORDER BY cumulative_hours, observed_at
    """, (start, end))
    entries, failed = [], 0
    for ch, obs, data, size, ul_lat, ul_lng, br_lat, br_lng in rows:
        if None in (ul_lat, ul_lng, br_lat, br_lng):
            continue
        key = precip_r2_key(ch, obs)
        if not dry_run:
            try:
                r2.upload_image(key, bytes(data), 'image/png')
            except Exception as e:  # noqa: BLE001
                failed += 1
                logger.warning(f'  上傳失敗 {key}: {e}')
                continue
        bbox = (min(ul_lng, br_lng), min(ul_lat, br_lat), max(ul_lng, br_lng), max(ul_lat, br_lat))
        entries.append(frame_entry(f'ch{ch}', obs, key, 'image/png', size, bbox))
    return entries, failed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', choices=SOURCES + ('all',), default='all')
    ap.add_argument('--days', type=int, default=14, help='往回補幾個 UTC 日（含今天），預設 14')
    ap.add_argument('--dry-run', action='store_true', help='只讀 DB 並印統計，不寫 R2')
    args = ap.parse_args()

    if not SUPABASE_DB_URL:
        logger.error('SUPABASE_DB_URL 未設定')
        sys.exit(1)
    r2 = None
    if not args.dry_run:
        r2 = get_r2_storage()
        if r2 is None:
            logger.error('R2 憑證未設定（需 R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT_URL / R2_BUCKET）')
            sys.exit(1)

    sources = SOURCES if args.source == 'all' else (args.source,)
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        for source in sources:
            total = fail_upload = fail_manifest = 0
            for start, end in _day_windows(args.days):
                if source == 'cwa':
                    entries, failed = entries_cwa(conn, start, end)
                elif source == 'aqi':
                    entries, failed = entries_aqi(conn, start, end)
                else:
                    entries, failed = entries_precip(conn, start, end, r2, args.dry_run)
                fail_upload += failed
                if not entries:
                    continue
                total += len(entries)
                for day, day_entries in group_by_day(entries).items():
                    logger.info(f'{source} {day}: {len(day_entries)} 幀（例 {day_entries[0]["key"]}）')
                if not args.dry_run and not publish_frames(r2, source, entries, log_prefix=f'[{source}] '):
                    fail_manifest += 1
            logger.info(
                f'DONE {source} dry_run={args.dry_run} frames={total} '
                f'upload_fail={fail_upload} manifest_fail_days={fail_manifest}'
            )
    finally:
        conn.close()


if __name__ == '__main__':
    main()
