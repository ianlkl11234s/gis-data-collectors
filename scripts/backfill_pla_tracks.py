#!/usr/bin/env python3
"""從 S3 原圖補算共機航跡向量化（spatial.pla_tracks / live.pla_activity_items / ledger）。

為什麼需要：每日 collector 只重跑 ledger 的「error 非空」列；版型不認得
（plate_ok=False）的列 error 是 NULL，被當成跑過了，新增版型後不會自動重跑。
另外每日流程只看 30 天回看窗。

做法：一天一張依序處理 —— 從 S3 `pla/track_charts/YYYY/MM/{date}.jpg`（ledger 的
chart_s3_key）下載原圖，**不重新下載國防部網站**，跑與每日 collector 相同的
`PlaTracksVectorizeCollector.vectorize_records()`，再走 SupabaseWriter 既有的
`pla_tracks_vectorize` 寫入路徑（該日形狀先 DELETE 再 INSERT、項次與 ledger UPSERT）。

用法（在 collector 容器內）：
    python scripts/backfill_pla_tracks.py --dry-run              # 只算不寫，印每日結果
    python scripts/backfill_pla_tracks.py                        # 預設：所有 plate_ok=false 且有原圖的日子
    python scripts/backfill_pla_tracks.py --start 2026-10-01 --end 2026-11-08
    python scripts/backfill_pla_tracks.py --dates 2026-10-26,2026-10-27
    python scripts/backfill_pla_tracks.py --plate-size 1125x1625 --all-status   # 連已成功的也重算
"""
import os
import sys
import argparse
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # noqa: E402
from collectors.base import TAIPEI_TZ  # noqa: E402
from collectors.pla_tracks_vectorize import PlaTracksVectorizeCollector  # noqa: E402
from storage.db import connect_supabase  # noqa: E402

SELECT_SQL = """
SELECT r.report_date::text, r.chart_s3_key
FROM spatial.pla_tracks_runs r
WHERE r.chart_s3_key IS NOT NULL
  AND (%(start)s::date IS NULL OR r.report_date >= %(start)s::date)
  AND (%(end)s::date IS NULL OR r.report_date <= %(end)s::date)
  AND (%(dates)s::date[] IS NULL OR r.report_date = ANY(%(dates)s::date[]))
  AND (%(plate)s::text IS NULL OR r.plate_size = %(plate)s::text)
  AND (%(all_status)s OR r.plate_ok IS NOT TRUE)
ORDER BY r.report_date
LIMIT %(limit)s
"""


def find_days(args) -> list[tuple[str, str]]:
    conn = connect_supabase(autocommit=True, statement_timeout_ms=60_000)
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_SQL, {
                'start': args.start, 'end': args.end,
                'dates': args.dates.split(',') if args.dates else None,
                'plate': args.plate_size, 'all_status': args.all_status,
                'limit': args.limit,
            })
            return [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--start', help='YYYY-MM-DD（含）')
    ap.add_argument('--end', help='YYYY-MM-DD（含）')
    ap.add_argument('--dates', help='逗號分隔的日期清單')
    ap.add_argument('--plate-size', help='只補某版型，例如 1125x1625')
    ap.add_argument('--all-status', action='store_true',
                    help='連 plate_ok=true 的日子也重算（預設只補版型不認得的）')
    ap.add_argument('--limit', type=int, default=200)
    ap.add_argument('--dry-run', action='store_true', help='只算不寫 DB')
    args = ap.parse_args()

    days = find_days(args)
    print(f'待補算 {len(days)} 天' + ('（dry-run，不寫 DB）' if args.dry_run else ''))
    if not days:
        return

    col = PlaTracksVectorizeCollector()
    if col._s3 is None:
        sys.exit('S3 未設定，無法讀原圖')
    writer = None
    if not args.dry_run:
        from storage.supabase_writer import SupabaseWriter
        writer = SupabaseWriter(config.SUPABASE_DB_URL)

    ok = review = failed = 0
    for report_date, s3_key in days:
        with tempfile.TemporaryDirectory(prefix='pla_backfill_') as img_dir:
            try:
                col._s3.s3.download_file(col._s3.bucket, s3_key,
                                         os.path.join(img_dir, f'{report_date}.jpg'))
                records = col.vectorize_records(report_date, img_dir, s3_key)
            except Exception as e:
                failed += 1
                print(f'✗ {report_date} 失敗: {type(e).__name__}: {e}')
                continue
        run = records[0]
        n_items = sum(1 for r in records if r['_type'] == 'item')
        tag = '✓' if run['ok'] else ('版型不認得' if not run['plate_ok'] else '待審')
        print(f"{report_date} {run['plate_size']}: 期望 {run['expected']} / 抽出 {run['extracted']} "
              f"項次 {n_items} georef_dev={run['georef_dev']} → {tag}")
        ok += bool(run['ok'])
        review += bool(run['plate_ok'] and not run['ok'])
        if writer is not None:
            if not writer.write('pla_tracks_vectorize', {'data': records},
                                datetime.now(tz=TAIPEI_TZ)):
                failed += 1
                print(f'✗ {report_date} 寫入失敗（已進 buffer，見 log）')

    print(f'完成：{len(days)} 天，通過 {ok}、待審 {review}、失敗 {failed}')


if __name__ == '__main__':
    main()
