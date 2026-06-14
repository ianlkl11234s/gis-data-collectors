"""
中國軍偵衛星每日通過台灣彙總

每日跑一次，補算「昨天 + 前天」（前天用來蓋掉昨天跑時 TLE 還沒到齊的）：
1. 從 realtime.satellite_tle_history 撈出當天每顆中國軍偵衛星最新 TLE
2. SGP4 採樣 30s 一整天，偵測進出台灣 bbox → 寫進 realtime.satellite_passes
3. UPDATE counties[] = 對 spatial.county_boundaries 做 ST_Intersects
4. 重算 realtime.satellite_passes_daily 那兩天的彙總

這個 collector 不寫 supabase_writer 表（直接用 SQL），run() 回 dict 給 base 統計用。
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2

import config
from collectors.base import BaseCollector

# 重用 scripts/ 內的核心函數，避免邏輯重複
SCRIPT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
from backfill_satellite_passes import (  # noqa: E402
    fetch_cn_tles_for_day, insert_passes, update_counties_for_range,
    rebuild_daily, detect_passes_for_day,
)
from sgp4.api import Satrec  # noqa: E402

logger = logging.getLogger(__name__)


class SatellitePassesDailyCollector(BaseCollector):
    """每日中國軍偵衛星通過彙總（補昨天 + 前天）"""

    name = "satellite_passes_daily"
    interval_minutes = config.SATELLITE_PASSES_DAILY_INTERVAL

    def collect(self) -> dict:
        if not config.SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL 未設定")

        # 算昨天 + 前天兩天，蓋掉前一日跑時 TLE 不齊的 row
        today_utc = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0)
        days = [today_utc - timedelta(days=2), today_utc - timedelta(days=1)]

        conn = psycopg2.connect(config.SUPABASE_DB_URL)
        try:
            stats = {
                'days_processed': [],
                'total_passes_inserted': 0,
                'counties_updated': 0,
                'daily_rows_rebuilt': 0,
                'sgp4_errors': 0,
            }

            for day in days:
                sats = fetch_cn_tles_for_day(conn, day, groups=None)
                if not sats:
                    logger.warning(f"[{self.name}] {day.date()} 無 TLE 可用")
                    continue

                day_passes = []
                bad = 0
                for s in sats:
                    try:
                        sr = Satrec.twoline2rv(s['tle_line1'], s['tle_line2'])
                    except Exception:
                        bad += 1
                        continue
                    day_passes.extend(detect_passes_for_day(sr, day, s))

                inserted = insert_passes(conn, day_passes)
                stats['days_processed'].append({
                    'date': day.date().isoformat(),
                    'sats': len(sats),
                    'passes_computed': len(day_passes),
                    'passes_inserted': inserted,
                    'sgp4_errors': bad,
                })
                stats['total_passes_inserted'] += inserted
                stats['sgp4_errors'] += bad
                logger.info(
                    f"[{self.name}] {day.date()} sats={len(sats)} "
                    f"passes={len(day_passes)} inserted={inserted}")

            # 補 counties[] + 重建 daily
            stats['counties_updated'] = update_counties_for_range(
                conn, days[0], days[-1])
            stats['daily_rows_rebuilt'] = rebuild_daily(conn, days[0], days[-1])
            logger.info(
                f"[{self.name}] counties updated={stats['counties_updated']} "
                f"daily_rebuilt={stats['daily_rows_rebuilt']}")
        finally:
            conn.close()

        return {
            'fetch_time': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
            'data': [],  # 不走 supabase_writer，這裡留空避免被當作未知 table 寫入
            **stats,
        }
