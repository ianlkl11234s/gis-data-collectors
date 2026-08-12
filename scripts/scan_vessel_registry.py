#!/usr/bin/env python3
"""
每週掃描：更新特殊船舶名冊 live.vessel_watch_registry，並印出「待你人工確認」的船。

搭配 gis-platform/migrations/339_vessel_watch.sql 與 backfill_vessel_watch.py。
這支是**手動執行**的（用戶 2026-08-12：「我可以每週都去掃一次」），不進排程。

它做三件事：
  1. 掃最近 N 天的 S3 raw → upsert registry 的**規則欄位**
     （imo / call_sign / 船名 / 尺寸 / rule_class / first_seen / last_seen）
  2. 補掃 DB 母表最近幾天（S3 archive 延遲約 6 天，這段只有 DB 有）
  3. 印出待審清單：規則認不出的、以及本週新出現的船

⚠️ 它**永遠不會**覆寫 confirmed_class / note / is_excluded —— 你標過的分類不會被洗掉。
   （這個保證來自 backfill_vessel_watch.REGISTRY_SQL 的 ON CONFLICT DO UPDATE 欄位清單。）

用法：
    python3 scripts/scan_vessel_registry.py                 # 掃最近 14 天（涵蓋 archive 延遲）
    python3 scripts/scan_vessel_registry.py --days 30
    python3 scripts/scan_vessel_registry.py --report-only   # 不掃，只印目前待審清單

審完怎麼標（psql）：
    -- 確認是中國海警
    UPDATE live.vessel_watch_registry
       SET confirmed_class = '中國海警', confirmed_at = now()
     WHERE mmsi = '413xxxxxx';

    -- 確認是誤收的民船（留列不刪，否則下週掃描又會提示一次）
    UPDATE live.vessel_watch_registry
       SET is_excluded = true, note = '台灣民間拖船', confirmed_at = now()
     WHERE mmsi = '416xxxxxx';
"""
import sys
import os
import logging
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from storage.s3 import S3Storage
from backfill_vessel_watch import DB_URL, collect_day, write_day

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# S3 archive 延遲約 6 天，預設掃 14 天確保無縫
DEFAULT_DAYS = 14

# 母表補掃：S3 還沒打包的那幾天，改用 DB 端 sweep（同一份 SQL 收錄條件）
SWEEP_SQL = "SELECT live.sweep_vessel_watch(%s::interval)"

# 母表 → registry 的補掃。DB 沒有 imo/call_sign/尺寸，只能補身分與時序，
# 但至少讓最近幾天新出現的船不會漏進名冊。
REGISTRY_FROM_DB_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (mmsi) mmsi, ship_name, ship_type, collected_at
    FROM live.ship_positions
    WHERE collected_at >= now() - %s::interval
      AND mmsi IS NOT NULL
      AND live.is_watch_candidate(mmsi, ship_name, ship_type)
    ORDER BY mmsi, collected_at DESC
),
span AS (
    SELECT mmsi, min(collected_at) f, max(collected_at) l
    FROM live.ship_positions
    WHERE collected_at >= now() - %s::interval
      AND mmsi IN (SELECT mmsi FROM latest)
    GROUP BY mmsi
)
INSERT INTO live.vessel_watch_registry
    (mmsi, names_seen, rule_class, rule_flag, matched_by, first_seen, last_seen, last_scan_at)
SELECT l.mmsi,
       CASE WHEN coalesce(l.ship_name,'') = '' THEN '{}'::text[] ELSE ARRAY[l.ship_name] END,
       c.vessel_class, c.flag, c.matched_by, s.f, s.l, now()
FROM latest l
JOIN span s USING (mmsi)
CROSS JOIN LATERAL live.classify_vessel(l.mmsi, l.ship_name, l.ship_type) AS c
ON CONFLICT (mmsi) DO UPDATE SET
    -- ⚠️ COALESCE 不可省，理由同 backfill_vessel_watch.REGISTRY_SQL
    names_seen = COALESCE((SELECT array_agg(DISTINCT x)
                           FROM unnest(live.vessel_watch_registry.names_seen || EXCLUDED.names_seen) x
                           WHERE x IS NOT NULL AND x <> ''), '{}'),
    rule_class = EXCLUDED.rule_class,
    rule_flag  = EXCLUDED.rule_flag,
    matched_by = EXCLUDED.matched_by,
    first_seen = LEAST(COALESCE(live.vessel_watch_registry.first_seen, EXCLUDED.first_seen),
                       EXCLUDED.first_seen),
    last_seen  = GREATEST(COALESCE(live.vessel_watch_registry.last_seen, EXCLUDED.last_seen),
                          EXCLUDED.last_seen),
    last_scan_at = now()
"""

SUMMARY_SQL = """
SELECT COALESCE(effective_class, '（規則認不出）') AS cls,
       count(*) AS n,
       count(*) FILTER (WHERE confirmed_class IS NOT NULL) AS confirmed
FROM live.vessel_watch_registry
WHERE NOT is_excluded
GROUP BY 1 ORDER BY 2 DESC
"""

PENDING_SQL = """
SELECT mmsi,
       COALESCE(imo, '-')                          AS imo,
       COALESCE(call_sign, '-')                    AS call_sign,
       COALESCE(array_to_string(names_seen, ' / '), '-') AS names,
       COALESCE(rule_class, '—')                   AS rule_class,
       COALESCE(matched_by, '-')                   AS matched_by,
       to_char(last_seen AT TIME ZONE 'Asia/Taipei', 'MM-DD HH24:MI') AS last_seen
FROM live.vessel_watch_registry
WHERE confirmed_class IS NULL
  AND NOT is_excluded
  AND (rule_class IS NULL OR last_scan_at >= now() - %s::interval)
ORDER BY (rule_class IS NULL) DESC, last_seen DESC NULLS LAST
LIMIT 200
"""


def print_table(rows, headers):
    if not rows:
        print('  （無）')
        return
    cols = list(zip(*([headers] + [[str(c) for c in r] for r in rows])))
    w = [min(max(len(x) for x in col), 46) for col in cols]
    line = '  ' + '  '.join(h[:w[i]].ljust(w[i]) for i, h in enumerate(headers))
    print(line)
    print('  ' + '  '.join('-' * x for x in w))
    for r in rows:
        print('  ' + '  '.join(str(c)[:w[i]].ljust(w[i]) for i, c in enumerate(r)))


def main():
    import argparse
    p = argparse.ArgumentParser(description='每週掃描更新特殊船舶名冊')
    p.add_argument('--days', type=int, default=DEFAULT_DAYS, help=f'回看天數（預設 {DEFAULT_DAYS}）')
    p.add_argument('--report-only', action='store_true', help='不掃描，只印目前待審清單')
    args = p.parse_args()

    if not DB_URL:
        logger.error('缺 SUPABASE_DB_URL')
        return 1

    conn = psycopg2.connect(DB_URL)
    window = f'{args.days} days'

    try:
        if not args.report_only:
            # ── 1. S3 掃描（有 imo / call_sign / 尺寸）──
            s3 = S3Storage()
            today = date.today()
            n_ship = 0
            for i in range(args.days, 0, -1):
                d = today - timedelta(days=i)
                positions, registry = collect_day(s3, d)
                if not registry:
                    continue
                # 週掃只更新名冊；軌跡由 DB 端 sweep cron 每小時負責
                _, nr = write_day(conn, positions, registry, registry_only=True)
                n_ship += nr
                logger.info(f'  S3 {d}: {nr} 艘')
            logger.info(f'S3 掃描完成，累計 {n_ship} 艘次')

            # ── 2. 母表補掃（S3 尚未打包的最近幾天）──
            with conn.cursor() as cur:
                cur.execute(SWEEP_SQL, (window,))
                swept = cur.fetchone()[0]
                cur.execute(REGISTRY_FROM_DB_SQL, (window, window))
            conn.commit()
            logger.info(f'母表補掃：軌跡 +{swept} 筆，名冊已同步')

        # ── 3. 報告 ──
        with conn.cursor() as cur:
            cur.execute(SUMMARY_SQL)
            summary = cur.fetchall()
            cur.execute(PENDING_SQL, (window,))
            pending = cur.fetchall()

        print('\n═══ 名冊現況（已排除人工標記的誤收）═══')
        print_table(summary, ['分類', '艘數', '已人工確認'])

        print(f'\n═══ 待你確認（規則認不出，或最近 {args.days} 天內有活動）═══')
        print_table(pending, ['MMSI', 'IMO', '呼號', '看過的船名', '規則判定', '依據', '最後出現'])
        if pending:
            print(f'\n  共 {len(pending)} 艘待審。標記方式見本檔檔頭 docstring。')
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
