#!/usr/bin/env python3
"""
backfill_vessel_zone.py —— 回補 live.vessel_watch_positions 的
dist_24nm_nm / zone / zone_region 三欄（Vessel Zone Watch VW-9 P1 資料層）。

搭配 gis-platform/migrations/354~357_vessel_zone_*.sql。設計說明見
mini-taiwan-pulse/docs/proposal/vessel-zone-watch.md §3/§4.2。

為什麼要跑這支腳本：354 的 BEFORE INSERT trigger 只覆蓋「本 migration
套用之後」新寫入的列。套用當下已存在的 627,306 筆歷史資料三欄全部是
NULL，需要本腳本一次性 UPDATE 補齊。

為什麼按月分批、每批獨立 commit（不用單一 transaction 或單一 DO block）：
  單一大 transaction 對 62.7 萬筆是不必要的鎖定範圍；按月分批讓任何
  一批失敗都不影響已完成的批次，且可以中斷後從任一月重跑（冪等）。

為什麼冪等：WHERE zone IS NULL AND geom IS NOT NULL —— 已分類過的列
（zone 有值）不會被重算，中斷後直接重跑同一個指令即可從斷點接續，
不需要額外的 watermark 狀態。

效能量測（2026-08-20，套用 357 segmentize 修正後）：
  最小月份（2026-02，3,036 筆）實測 4.15 秒 ≈ 1.37ms/筆。
  全表 627,306 筆推算約 14~15 分鐘（357 之前的 354 版本用未簡化幾何，
  同樣規模的量測是 42ms/筆，推算 7.3 小時——差距來自 357 的
  ST_Segmentize(ST_SimplifyPreserveTopology(line_geom, 0.0002), 0.05)
  把 twmain 頂點數從 6,503 壓到 365，其餘三個 region 壓到 139~288。

用法：
    # 全量回補（依 collected_at 月份自動分批）
    python3 scripts/backfill_vessel_zone.py

    # 指定月份範圍
    python3 scripts/backfill_vessel_zone.py --since 2026-02 --until 2026-08

    # 試跑（不寫 DB，只印每月待處理筆數）
    python3 scripts/backfill_vessel_zone.py --dry-run

⚠️ 分類邏輯唯一真相是 SQL 函數 live.classify_vessel_zone()——本腳本
   刻意不在 Python 端重寫任何一版五帶判定規則，只呼叫該函數，避免
   兩份規則日後悄悄分歧（vessel-zone-watch.md 設計原則）。

需環境變數：SUPABASE_DB_URL
"""
import logging
import os
import sys
import time
from datetime import date

import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_URL = os.getenv('SUPABASE_DB_URL')

# 表的實際資料範圍是 2026-02-27 ~ 迄今，抓寬一點的月份表沒關係——
# 空月份的 UPDATE 是 0 rows，不是錯誤。
DEFAULT_SINCE = '2026-02'
DEFAULT_UNTIL_TODAY = True  # 沒指定 --until 時，抓到本月

BATCH_UPDATE_SQL = """
UPDATE live.vessel_watch_positions p
SET (dist_24nm_nm, zone, zone_region) =
    (SELECT dist_nm, zone, zone_region FROM live.classify_vessel_zone(p.geom))
WHERE p.zone IS NULL AND p.geom IS NOT NULL
  AND p.collected_at >= %(start)s AND p.collected_at < %(end)s
"""

COUNT_PENDING_SQL = """
SELECT count(*) FROM live.vessel_watch_positions
WHERE zone IS NULL AND geom IS NOT NULL
  AND collected_at >= %(start)s AND collected_at < %(end)s
"""


def month_range(since_ym: str, until_ym: str):
    """列出 [since_ym, until_ym] 區間（含頭尾）的每月第一天，供批次邊界用。"""
    y, m = (int(x) for x in since_ym.split('-'))
    uy, um = (int(x) for x in until_ym.split('-'))
    months = []
    while (y, m) <= (uy, um):
        months.append(date(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def next_month(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def main() -> int:
    import argparse
    p = argparse.ArgumentParser(description='回補 live.vessel_watch_positions 的 zone 三欄')
    p.add_argument('--since', default=DEFAULT_SINCE, help='起始月 YYYY-MM（預設 2026-02）')
    p.add_argument('--until', default=None, help='結束月 YYYY-MM（預設本月）')
    p.add_argument('--sleep', type=float, default=1.0, help='每批之間 sleep 秒數（預設 1）')
    p.add_argument('--dry-run', action='store_true', help='只印每月待處理筆數，不寫 DB')
    args = p.parse_args()

    until_ym = args.until or date.today().strftime('%Y-%m')
    months = month_range(args.since, until_ym)

    if not DB_URL:
        logger.error('缺 SUPABASE_DB_URL')
        return 1

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    total_updated = 0
    t_start_all = time.monotonic()

    try:
        for start in months:
            end = next_month(start)
            ym = start.strftime('%Y-%m')

            if args.dry_run:
                with conn.cursor() as cur:
                    cur.execute(COUNT_PENDING_SQL, {'start': start, 'end': end})
                    (n,) = cur.fetchone()
                logger.info(f'{ym}: 待處理 {n} 筆［試跑，未寫入］')
                continue

            t0 = time.monotonic()
            with conn.cursor() as cur:
                cur.execute(BATCH_UPDATE_SQL, {'start': start, 'end': end})
                n_updated = cur.rowcount
            conn.commit()
            elapsed = time.monotonic() - t0
            total_updated += n_updated
            logger.info(f'{ym}: 更新 {n_updated:6d} 筆，耗時 {elapsed:6.2f} 秒'
                        + (f'（{elapsed / n_updated * 1000:.2f} ms/筆）' if n_updated else ''))

            # 若單批耗時遠超預期（>3 分鐘），停下來回報，不要硬跑到底
            if elapsed > 180:
                logger.error(f'{ym} 批次耗時 {elapsed:.1f} 秒，遠超預期量級——停止，回報後再決定')
                return 1

            time.sleep(args.sleep)
    finally:
        conn.close()

    elapsed_all = time.monotonic() - t_start_all
    mode = '［試跑，未寫入］' if args.dry_run else ''
    logger.info(f'完成{mode}：{len(months)} 個月批次，共更新 {total_updated} 筆，'
                f'總耗時 {elapsed_all:.1f} 秒（{elapsed_all / 60:.1f} 分鐘）')
    return 0


if __name__ == '__main__':
    sys.exit(main())
