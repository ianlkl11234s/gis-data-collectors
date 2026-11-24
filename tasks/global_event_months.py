"""
全球重大事件（AI 初判候選）每月期別檔：給 Mini Taiwan Pulse 長期時間條「事件點」類用。

為什麼在這裡：候選表 intel.global_event_candidate_observations 由本 repo 的 global_events 收集器寫入，
只在 Supabase 留 180 天（gis-platform migration 542）；已結束的月份要在被刪之前凍結成不再變動的檔案
（ADR-0026：不變的歷史走 R2，S3 是正本）。本模組是產檔邏輯的唯一正本；mini 的
`scripts/export/export-static-rpc-snapshots.sh @global_event_months` 只是呼叫本檔做本機回補，不另寫 SQL。

輸出（檔名是前端契約：mini `src/data/globalEventMonths.ts` 的 globalEventMonthFile）：
    S3（正本，與其他 static-rpc 快照同處）：deploy-assets/static-rpc/get_global_event_month_<YYYYMM>.json
    R2（公開，網站 /static-rpc/ 由 Worker 讀 R2 site/ 前綴）：site/static-rpc/get_global_event_month_<YYYYMM>.json
    物件 metadata：partial=true|false、sha256

規則（DB 鐵則 R3：排程只算增量）：
    - 月份依台北時間切（網站時間軸一律顯示台北時間）。
    - 本月、以及月底後未滿 FINAL_AFTER_DAYS 天的上個月：每天重產，標 partial=true（等改判、補判落定）。
    - 已結束滿 FINAL_AFTER_DAYS 天的月份：S3 已有 partial=false 的檔就跳過；沒有檔、或還是 partial 的檔才產一次定版。
    - 不重產早於 MAX_AGE_DAYS 天的月份：候選表 180 天後會被刪，重產只會得到殘缺的檔。
    - 每個候選取最新版本；不含 AI 判為雜訊者；每列＝一個候選，地點收成陣列
      （新聞提及的城市／國家代表點，不是精確發生地；座標到小數 2 位）。
    - 資料庫只讀：交易 READ ONLY＋SET LOCAL（timeout、時區），結束一律 ROLLBACK。

用法（本機回補，只產本地檔、不上傳）：
    python3 -m tasks.global_event_months --months 2026-09 2026-10 --out-dir /tmp/out --no-upload
排程：main.py run_global_event_months_task()，GLOBAL_EVENT_MONTHS_ENABLED=true 才註冊（預設關）。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

logger = logging.getLogger(__name__)

TAIPEI = ZoneInfo('Asia/Taipei')
FILE_STEM = 'get_global_event_month_'
S3_PREFIX = 'deploy-assets/static-rpc/'
R2_PREFIX = 'site/static-rpc/'
FINAL_AFTER_DAYS = 3
MAX_AGE_DAYS = 150
CACHE_PARTIAL = 'public, max-age=300, must-revalidate'
CACHE_FINAL = 'public, max-age=3600'

# psycopg2 參數：%(month_start)s、%(partial)s。月份加減在台北時區算（UTC 下 9/30 16:00Z + 1 個月會變成 10/30）。
MONTH_SQL = """
WITH latest AS (
  SELECT DISTINCT ON (o.candidate_id) o.*
  FROM intel.global_event_candidate_observations AS o
  WHERE o.observed_at >= %(month_start)s::timestamptz
    AND o.observed_at < (%(month_start)s::timestamptz + interval '1 month')
  ORDER BY o.candidate_id, o.available_at DESC, o.observation_sha256 DESC
), kept AS (
  SELECT l.candidate_id, l.observed_at, l.category, l.severity, COALESCE(l.title_zh_tw, l.source_headline) AS title,
         (SELECT jsonb_agg(jsonb_build_array(round((place ->> 'longitude')::numeric, 2), round((place ->> 'latitude')::numeric, 2),
                   CASE place ->> 'location_kind' WHEN 'country_center' THEN 'n' WHEN 'city_center' THEN 'c'
                                                  WHEN 'event_point' THEN 'e' ELSE 'o' END)
                 ORDER BY place ->> 'place_key')
            FROM jsonb_array_elements(l.places) AS place
           WHERE place ->> 'longitude' IS NOT NULL AND place ->> 'latitude' IS NOT NULL) AS pts
  FROM latest AS l
  WHERE l.decision IS DISTINCT FROM 'drop_noise'
)
SELECT jsonb_build_object(
  'format', 'global-events-month-v1',
  'month', to_char(%(month_start)s::timestamptz, 'YYYY-MM'),
  'timezone', 'Asia/Taipei',
  'partial', %(partial)s,
  'generated_at', now(),
  'note', 'AI 初判候選（未經研究確認），每列＝一則報導候選；不含 AI 判為雜訊者；座標是新聞提及的城市／國家代表點，不是精確發生地',
  'source', 'GDELT GKG 2.1 → Mini Taiwan Pulse 全球情勢 AI 初判（intel.global_event_candidate_observations，每個候選取最新版本）',
  'coverage', jsonb_build_object('first_observed_at', (SELECT min(observed_at) FROM latest),
                                 'last_observed_at', (SELECT max(observed_at) FROM latest)),
  'candidates', (SELECT count(*) FROM latest),
  'noise_excluded', (SELECT count(*) FROM latest WHERE decision = 'drop_noise'),
  'no_location', (SELECT count(*) FROM kept WHERE pts IS NULL),
  'kinds', jsonb_build_object('n', 'country_center', 'c', 'city_center', 'e', 'event_point', 'o', 'other'),
  'cols', jsonb_build_array('id', 't', 'cat', 'sev', 'title', 'pts'),
  'rows', COALESCE((SELECT jsonb_agg(jsonb_build_array(
      substr(k.candidate_id, 6, 12), extract(epoch FROM k.observed_at)::bigint, k.category, k.severity, left(k.title, 24), k.pts)
      ORDER BY k.observed_at, k.candidate_id) FROM kept AS k WHERE k.pts IS NOT NULL), '[]'::jsonb)
)::text
"""


def file_name(month: str) -> str:
    """'2026-10' → 'get_global_event_month_202610.json'"""
    return f"{FILE_STEM}{month.replace('-', '')}.json"


def month_end(month: str) -> date:
    y, m = map(int, month.split('-'))
    return date(y + (m == 12), m % 12 + 1, 1)


def months_between(first: str, last: str) -> list[str]:
    y, m = map(int, first.split('-'))
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def is_final(month: str, today: date) -> bool:
    """月底（台北）後滿 FINAL_AFTER_DAYS 天才定版。"""
    return today >= month_end(month) + timedelta(days=FINAL_AFTER_DAYS)


def too_old(month: str, today: date) -> bool:
    """月初早於 MAX_AGE_DAYS 天前：候選表可能已刪掉一部分。"""
    y, m = map(int, month.split('-'))
    return date(y, m, 1) < today - timedelta(days=MAX_AGE_DAYS)


def scheduled_months(today: date) -> list[str]:
    """每日排程要看的月份：上個月與本月（上個月已定版且 S3 有定版檔時由 plan 跳過）。"""
    this = today.strftime('%Y-%m')
    prev = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
    return [prev, this]


def plan(months: list[str], today: date, existing_partial: dict[str, bool | None], force: bool = False) -> list[tuple[str, bool, str]]:
    """回傳 [(month, partial, 動作說明)]；動作為 'skip:<原因>' 或 'run'。

    existing_partial：S3 現有物件的 partial（True／False），沒有檔為 None。
    """
    out = []
    for month in months:
        partial = not is_final(month, today)
        if too_old(month, today):
            out.append((month, partial, f'skip:早於 {MAX_AGE_DAYS} 天，候選表可能已刪掉一部分'))
        elif not partial and existing_partial.get(month) is False and not force:
            out.append((month, partial, 'skip:已有定版檔'))
        else:
            out.append((month, partial, 'run'))
    return out


def query_month(conn, month: str, partial: bool) -> bytes:
    """唯讀查一個月，回傳緊湊 JSON bytes。"""
    month_start = f"{month}-01T00:00:00+08:00"
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("SET LOCAL statement_timeout = '120s'")
        cur.execute("SET LOCAL TimeZone = 'Asia/Taipei'")
        cur.execute(MONTH_SQL, {'month_start': month_start, 'partial': partial})
        text = cur.fetchone()[0]
    conn.rollback()
    payload = json.loads(text)
    if payload.get('format') != 'global-events-month-v1' or payload.get('month') != month:
        raise ValueError(f"{month}: 產出格式不符（format={payload.get('format')}, month={payload.get('month')}）")
    return json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def _stored_partial(store, key: str) -> bool | None:
    """S3／R2 物件的 partial metadata；沒有檔回 None；metadata 缺漏當成 partial（保守：會重產）。"""
    from botocore.exceptions import ClientError
    try:
        head = store.s3.head_object(Bucket=store.bucket, Key=key)
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey', 'NotFound'):
            return None
        raise
    return (head.get('Metadata') or {}).get('partial', 'true') != 'false'


def run(months: list[str] | None = None, *, force: bool = False, upload: bool = True,
        out_dir: str | None = None, today: date | None = None, conn=None, s3=None, r2=None) -> dict:
    """產檔主流程。回傳統計 dict（給 notify_global_event_months）。

    每個月獨立：一個月失敗不影響其他月，最後一起回報。
    upload=False：只寫 out_dir，不碰 S3／R2（本機回補、驗證用）。
    """
    today = today or datetime.now(TAIPEI).date()
    months = months or scheduled_months(today)
    stats = {'ok': 0, 'skipped': 0, 'failed': 0, 'bytes': 0, 'months': [], 'failures': [], 'r2': 'off'}

    if upload and s3 is None:
        from storage.s3 import S3Storage
        s3 = S3Storage()
    if upload and r2 is None:
        from storage.r2 import get_r2_storage
        r2 = get_r2_storage()
    stats['r2'] = 'on' if (upload and r2 is not None) else 'off'

    # 定版檔要 S3（正本）與 R2（公開）都有才算：只看 S3 的話，S3 成功、R2 失敗的那個月隔天會被跳過，R2 永遠缺定版檔。
    existing: dict[str, bool | None] = {}
    if upload:
        for m in months:
            on_s3 = _stored_partial(s3, S3_PREFIX + file_name(m))
            on_r2 = _stored_partial(r2, R2_PREFIX + file_name(m)) if r2 is not None else False
            existing[m] = False if (on_s3 is False and on_r2 is False) else (None if on_s3 is None else True)
    todo = plan(months, today, existing, force=force)

    own_conn = conn is None
    if own_conn and any(action == 'run' for _, _, action in todo):
        from storage.db import connect_supabase
        conn = connect_supabase(autocommit=False, statement_timeout_ms=120_000)
    try:
        for month, partial, action in todo:
            if action != 'run':
                stats['skipped'] += 1
                logger.info("[global_event_months] %s %s", month, action)
                continue
            try:
                body = query_month(conn, month, partial)
                sha = hashlib.sha256(body).hexdigest()
                name = file_name(month)
                if out_dir:
                    Path(out_dir).mkdir(parents=True, exist_ok=True)
                    tmp = Path(out_dir) / f".{name}.tmp"
                    tmp.write_bytes(body)
                    tmp.replace(Path(out_dir) / name)  # 成功才換掉正式檔
                if upload:
                    meta = {'partial': 'true' if partial else 'false', 'sha256': sha}
                    s3.s3.put_object(Bucket=s3.bucket, Key=S3_PREFIX + name, Body=body,
                                     ContentType='application/json', Metadata=meta)
                    if r2 is not None:
                        r2.s3.put_object(Bucket=r2.bucket, Key=R2_PREFIX + name, Body=body,
                                         ContentType='application/json',
                                         CacheControl=CACHE_PARTIAL if partial else CACHE_FINAL, Metadata=meta)
                stats['ok'] += 1
                stats['bytes'] += len(body)
                stats['months'].append(f"{month}{'（未完結）' if partial else ''}")
                logger.info("[global_event_months] %s %s %d bytes", month, 'partial' if partial else 'final', len(body))
            except Exception as e:  # 一個月失敗不擋其他月
                try:
                    conn.rollback()
                except Exception:
                    pass
                stats['failed'] += 1
                stats['failures'].append(f"{month}: {e}")
                logger.exception("[global_event_months] %s 失敗", month)
    finally:
        if own_conn and conn is not None:
            conn.close()
    return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--months', nargs='+', help='YYYY-MM；不給則為排程預設（上個月＋本月）')
    ap.add_argument('--since', help='YYYY-MM：從這個月到本月全部（回補用）')
    ap.add_argument('--force', action='store_true', help='已有定版檔也重產')
    ap.add_argument('--out-dir', help='另外寫一份到本地目錄')
    ap.add_argument('--no-upload', action='store_true', help='不上傳 S3／R2（只寫 --out-dir）')
    args = ap.parse_args(argv)
    today = datetime.now(TAIPEI).date()
    months = args.months or (months_between(args.since, today.strftime('%Y-%m')) if args.since else None)
    if args.no_upload and not args.out_dir:
        ap.error('--no-upload 要搭配 --out-dir')
    stats = run(months, force=args.force, upload=not args.no_upload, out_dir=args.out_dir, today=today)
    print(json.dumps(stats, ensure_ascii=False))
    return 1 if stats['failed'] else 0


if __name__ == '__main__':
    sys.exit(main())
