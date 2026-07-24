#!/usr/bin/env python3
"""
回填 live.cwa_imagery_frames 歷史影像到 R2 CDN（AR-11 read-path-cdn）。

對每一列 image_key IS NULL 的 frame：算 R2 key → 上傳 bytea 到 R2 → 回填 image_key。

設計重點（連的是 Supabase transaction pooler，不可長交易 / 長 cursor）：
- keyset 分頁：每批 WHERE image_key IS NULL ORDER BY (dataset_id, observed_at)
  LIMIT N，用 (dataset_id, observed_at) > cursor 推進。每批獨立 query + commit，
  不留 server-side cursor / 不開長交易。
- 冪等：image_key IS NULL 天然可續跑；成功回填的列下次自動排除。
- 韌性：單列上傳失敗 → skip + 記數，keyset 仍推進（不會卡在壞列），全程不中斷。
- Ctrl-C：已 UPDATE 的批次已 commit，直接重跑即續傳。

總量 ~21.5k 列 / ~3.2GB，跑很久是正常的。

用法：
    python3 scripts/backfill_imagery_r2.py                  # 全量回填
    python3 scripts/backfill_imagery_r2.py --limit 5        # 只跑 5 列（驗證用）
    python3 scripts/backfill_imagery_r2.py --dry-run        # 只算 key + 統計，不上傳/不寫 DB
    python3 scripts/backfill_imagery_r2.py --batch-size 100 # 調整每批列數（預設 50）

需環境變數：SUPABASE_DB_URL + R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY /
R2_ENDPOINT_URL（R2_BUCKET 預設 mini-tw-pulse）。
"""
import argparse
import logging
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SUPABASE_DB_URL  # noqa: E402
from collectors.cwa_satellite import imagery_r2_key  # noqa: E402
from storage.r2 import get_r2_storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def fetch_batch(conn, cursor_key, limit):
    """keyset 分頁抓一批 image_key IS NULL 的列。每批獨立 query + commit（pooler 友善）。"""
    with conn.cursor() as cur:
        if cursor_key is None:
            cur.execute(
                """
                SELECT dataset_id, observed_at, mime_type, image_bytes
                  FROM live.cwa_imagery_frames
                 WHERE image_key IS NULL
                   AND image_bytes IS NOT NULL
                 ORDER BY dataset_id, observed_at
                 LIMIT %s
                """,
                (limit,),
            )
        else:
            cur.execute(
                """
                SELECT dataset_id, observed_at, mime_type, image_bytes
                  FROM live.cwa_imagery_frames
                 WHERE image_key IS NULL
                   AND image_bytes IS NOT NULL
                   AND (dataset_id, observed_at) > (%s, %s)
                 ORDER BY dataset_id, observed_at
                 LIMIT %s
                """,
                (cursor_key[0], cursor_key[1], limit),
            )
        rows = cur.fetchall()
    conn.commit()  # 結束 read tx，不留長交易
    return rows


def update_keys(conn, updates):
    """一次批次 UPDATE image_key。updates: list of (dataset_id, observed_at, image_key)。"""
    if not updates:
        return 0
    sql = """
        UPDATE live.cwa_imagery_frames AS t
           SET image_key = v.image_key
          FROM (VALUES %s) AS v(dataset_id, observed_at, image_key)
         WHERE t.dataset_id = v.dataset_id
           AND t.observed_at = v.observed_at::timestamptz
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, updates, template="(%s, %s, %s)", page_size=100)
        n = cur.rowcount
    conn.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true',
                    help='只算 key + 統計，不上傳 R2、不寫 DB')
    ap.add_argument('--limit', type=int,
                    help='最多處理幾列（測試用），預設全部')
    ap.add_argument('--batch-size', type=int, default=50,
                    help='每批列數（keyset 分頁），預設 50')
    args = ap.parse_args()

    if not SUPABASE_DB_URL:
        logger.error("SUPABASE_DB_URL 未設定")
        sys.exit(1)

    r2 = None
    if not args.dry_run:
        r2 = get_r2_storage()
        if r2 is None:
            logger.error(
                "R2 憑證未設定（需 R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / "
                "R2_ENDPOINT_URL / R2_BUCKET）"
            )
            sys.exit(1)

    conn = psycopg2.connect(SUPABASE_DB_URL)

    total_ok = total_fail = total_seen = 0
    cursor_key = None
    batch_no = 0
    try:
        while True:
            # 依 --limit 決定這批要抓幾列
            this_limit = args.batch_size
            if args.limit is not None:
                remaining = args.limit - total_seen
                if remaining <= 0:
                    break
                this_limit = min(this_limit, remaining)

            rows = fetch_batch(conn, cursor_key, this_limit)
            if not rows:
                break
            batch_no += 1

            updates = []
            batch_ok = batch_fail = 0
            for dataset_id, observed_at, mime_type, image_bytes in rows:
                total_seen += 1
                cursor_key = (dataset_id, observed_at)  # 推進 keyset（失敗列也跳過）
                key = imagery_r2_key(dataset_id, observed_at, mime_type)
                if args.dry_run:
                    batch_ok += 1
                    continue
                try:
                    r2.upload_image(key, bytes(image_bytes), mime_type)
                    updates.append((dataset_id, observed_at, key))
                    batch_ok += 1
                except Exception as e:
                    batch_fail += 1
                    logger.warning(f"  上傳失敗 {key}: {e}")

            if updates:
                update_keys(conn, updates)

            total_ok += batch_ok
            total_fail += batch_fail
            logger.info(
                f"batch#{batch_no} rows={len(rows)} ok={batch_ok} fail={batch_fail} "
                f"cursor=({cursor_key[0]},{cursor_key[1].isoformat()}) "
                f"total_ok={total_ok} total_fail={total_fail}"
            )
    except KeyboardInterrupt:
        logger.warning("中斷 (Ctrl-C)：已 commit 的批次保留，直接重跑即續傳")
    finally:
        conn.close()

    logger.info(
        f"DONE dry_run={args.dry_run} seen={total_seen} "
        f"ok={total_ok} fail={total_fail}"
    )


if __name__ == '__main__':
    main()
