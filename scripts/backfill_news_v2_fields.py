#!/usr/bin/env python3
"""
補跑 live.news_events 的 v2 三維度欄位（gis_relevance / severity / is_event）。

migration 164 加欄位之前累積的舊資料這三欄都是 NULL，
RPC v2（migration 165）的 NULL 寬鬆放行讓「重要」級會吃到本來不該進的政策／非事件。
此腳本重跑 LLM 把 NULL 補滿。

用法：
    python3 scripts/backfill_news_v2_fields.py --count            # 只看有多少要補
    python3 scripts/backfill_news_v2_fields.py --dry-run --limit 20
    python3 scripts/backfill_news_v2_fields.py --limit 500
    python3 scripts/backfill_news_v2_fields.py                    # 全跑
"""
import os
import sys
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_batch

import config
from collectors.news_events import NewsEventsCollector, LLM_BATCH_SIZE

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)

SELECT_NULL_SQL = """
    SELECT id, title, COALESCE(summary, '') AS summary, county
    FROM live.news_events
    WHERE gis_relevance IS NULL
       OR severity      IS NULL
       OR is_event      IS NULL
    ORDER BY published_ts DESC
    LIMIT %s
"""

COUNT_NULL_SQL = """
    SELECT COUNT(*) FROM live.news_events
    WHERE gis_relevance IS NULL OR severity IS NULL OR is_event IS NULL
"""

UPDATE_SQL = """
    UPDATE live.news_events
    SET gis_relevance = %s,
        severity      = %s,
        is_event      = %s
    WHERE id = %s
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=100_000, help='最多處理幾筆（預設全跑）')
    ap.add_argument('--dry-run', action='store_true', help='不寫回 DB，只跑 LLM 看結果')
    ap.add_argument('--count', action='store_true', help='只印 NULL 筆數就離開')
    args = ap.parse_args()

    if not config.SUPABASE_DB_URL:
        sys.exit('SUPABASE_DB_URL 未設定')

    conn = psycopg2.connect(config.SUPABASE_DB_URL)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute(COUNT_NULL_SQL)
        total_null = cur.fetchone()[0]
    logger.info(f'目前 NULL 筆數：{total_null}')
    if args.count:
        conn.close()
        return

    with conn.cursor() as cur:
        cur.execute(SELECT_NULL_SQL, (args.limit,))
        rows = cur.fetchall()
    logger.info(f'本次處理 {len(rows)} 筆（limit={args.limit}）')
    if not rows:
        conn.close()
        return

    # 重用 collector 的 LLM + gazetteer 邏輯
    collector = NewsEventsCollector()
    gaz = collector._load_gazetteer()  # noqa: SLF001
    if gaz.is_empty():
        sys.exit('gazetteer 為空（township_boundaries 讀不到），中止')

    items = [
        {'_db_id': r[0], 'title': r[1], 'summary': r[2], 'county_hint': r[3]}
        for r in rows
    ]

    updates: list[tuple] = []
    usage = {'input': 0, 'output': 0, 'cached': 0, 'batches': 0, 'failed_batches': 0}
    import time

    for start in range(0, len(items), LLM_BATCH_SIZE):
        batch = items[start:start + LLM_BATCH_SIZE]
        if usage['batches'] > 0:
            time.sleep(0.5)
        try:
            anns, u = collector._llm_extract_batch(batch, gaz)  # noqa: SLF001
            usage['batches'] += 1
            for k in ('input', 'output', 'cached'):
                usage[k] += u[k]
        except Exception as e:
            usage['failed_batches'] += 1
            logger.warning(f'batch {start} 失敗：{e}')
            anns = {}

        def _int_in_range(v, lo, hi):
            try:
                n = int(v)
                return n if lo <= n <= hi else None
            except (TypeError, ValueError):
                return None

        for i, it in enumerate(batch):
            ann = anns.get(i) or {}
            gr = _int_in_range(ann.get('gis_relevance'), 0, 3)
            sv = _int_in_range(ann.get('severity'), 0, 3)
            raw_event = ann.get('is_event')
            ev = bool(raw_event) if isinstance(raw_event, bool) else None
            updates.append((gr, sv, ev, it['_db_id']))

        if (usage['batches'] % 10) == 0:
            logger.info(
                f"  進度 {start + len(batch)}/{len(items)}  "
                f"batches={usage['batches']} failed={usage['failed_batches']} "
                f"tokens in={usage['input']} out={usage['output']} cached={usage['cached']}"
            )

    logger.info(
        f"LLM 完成：batches={usage['batches']} failed={usage['failed_batches']} "
        f"tokens in={usage['input']} out={usage['output']} cached={usage['cached']}"
    )

    if args.dry_run:
        non_null = sum(1 for u in updates if u[0] is not None)
        logger.info(f'[dry-run] 將更新 {len(updates)} 筆，其中 gis_relevance 非 NULL = {non_null}')
        conn.close()
        return

    with conn.cursor() as cur:
        execute_batch(cur, UPDATE_SQL, updates, page_size=200)
    conn.commit()
    logger.info(f'已寫回 {len(updates)} 筆。剩餘 NULL 用 --count 重看；下一輪 cron 會自動 refresh news_events_daily。')
    conn.close()


if __name__ == '__main__':
    main()
