#!/usr/bin/env python3
"""
每日軌跡凍結匯出（AR-14 Track C / D-B 決議）。

動機：Supabase 上的 *_trails_daily 是「滾動視窗」——cleanup cron 會把舊資料刪掉
（bus / bus_intercity 3 天、ship 7 天、flight 7~9 天）。要讓嵌入式地圖能回放
任意歷史日，必須每天把「昨天」凍結成靜態檔上 S3 永久保存。

格式（D-B 決議：小檔 gzip JSON、大檔 Arrow）：
    flights        → gzip JSON（單日 ~2MB）
    ships          → Arrow IPC（單日 ~27MB）
    bus            → Arrow IPC
    bus_intercity  → Arrow IPC

⚠️ Arrow 一律寫「未壓縮」IPC file format：arrow-js（AR-15 前端消費端）讀不了
   LZ4/ZSTD 壓縮的 record batch，開啟 compression 會在未來靜默炸掉前端。

欄位語意「鏡像對應 RPC」，前端接靜態檔時不用改 parser：
    get_ship_trails          → mmsi, ship_type, trail
    get_flight_trails        → flight_id, callsign, aircraft_type, origin, destination, trail
    get_bus_trails           → plate_numb, direction, route_uid, route_name, city, trail
    get_bus_intercity_trails → plate_numb, direction, route_uid, route_name, city, trail
（point_count / refreshed_at 是 DB 內部欄位，RPC 不回，這裡也不寫進檔案。）

讀取方式：collector 端直連 psycopg2 查 live.* 底層表，**不打 PostgREST RPC**
（AR-14 指定：避開 API 層 timeout）。每批獨立短查詢 + keyset 分頁，不開長交易、
不留 server-side cursor，對 Supavisor transaction pooler 友善。

用法：
    python3 scripts/export_daily_trails.py                          # 昨天（Asia/Taipei）全部 dataset
    python3 scripts/export_daily_trails.py --date 2026-08-07
    python3 scripts/export_daily_trails.py --datasets ships,flights
    python3 scripts/export_daily_trails.py --backfill 3             # 回補最近 3 天（含 --date 當天往前）
    python3 scripts/export_daily_trails.py --dry-run                # 只產本地檔，不上傳、不動 manifest

輸出：
    s3://<S3_BUCKET>/trails/<dataset>/<YYYY-MM-DD>.<arrow|json.gz>
    s3://<S3_BUCKET>/trails/<dataset>/manifest.json   （merge 既有 dates）

排程：main.py 的 run_trails_export_task() 每日 TRAILS_EXPORT_TIME（預設 02:00
Asia/Taipei）呼叫 export_range()，失敗走 notify_trails_export → Telegram。

需環境變數：SUPABASE_DB_URL、S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY / S3_REGION
需套件：pyarrow、boto3、psycopg2（皆已列於 requirements.txt）

Exit code：
    0 = 全部 dataset × 日期都成功
    1 = 任一組合失敗（0 rows / 筆數對不上 / 上傳驗證失敗）；會跑完全部再一次列出失敗清單
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from storage.db import connect_supabase  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

TAIPEI = ZoneInfo('Asia/Taipei')
S3_PREFIX = 'trails'
DEFAULT_OUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'trails_export'
)
# 每批 keyset 分頁列數。ship trail 單列 ~2.5KB，2000 列 ≈ 5MB payload，
# 對 pooler 是「短查詢」等級，不會逼近 2 分鐘 timeout。
DEFAULT_BATCH_SIZE = 2000

CONTENT_TYPE_ARROW = 'application/vnd.apache.arrow.file'


@dataclass(frozen=True)
class Dataset:
    """一個 dataset 的匯出契約：來源表、鏡像 RPC 的欄位、keyset 分頁鍵、輸出格式。"""
    name: str
    table: str                    # live.* 底層表（非 RPC）
    columns: tuple[str, ...]      # 鏡像對應 RPC 回傳欄位（順序即檔案欄位順序）
    arrow_types: tuple[str, ...]  # 與 columns 對齊：'string' | 'int16'
    key_cols: tuple[str, ...]     # keyset 分頁鍵（必須是 PK 中 day 之後的欄位，且是 columns 的前綴）
    fmt: str                      # 'arrow' | 'json.gz'
    rpc: str                      # 對應的 RPC 名（僅供 log / 文件對照）
    retention_days: int           # DB cleanup 保留天數（超出即無法回補）


DATASETS: dict[str, Dataset] = {
    'ships': Dataset(
        name='ships',
        table='live.ship_trails_daily',
        columns=('mmsi', 'ship_type', 'trail'),
        arrow_types=('string', 'string', 'string'),
        key_cols=('mmsi',),
        fmt='arrow',
        rpc='get_ship_trails(target_date)',
        retention_days=7,
    ),
    'flights': Dataset(
        name='flights',
        table='live.flight_trails_daily',
        columns=('flight_id', 'callsign', 'aircraft_type', 'origin', 'destination', 'trail'),
        arrow_types=('string',) * 6,
        key_cols=('flight_id',),
        fmt='json.gz',
        rpc='get_flight_trails(target_date)',
        retention_days=7,
    ),
    'bus': Dataset(
        name='bus',
        table='live.bus_trails_daily',
        # 主站是分 22 縣市呼叫 get_bus_trails(date, cities) 再合併；這裡直連底層表
        # 一次撈完，但保留 city 欄位讓下游仍能按縣市切。
        columns=('plate_numb', 'direction', 'route_uid', 'route_name', 'city', 'trail'),
        arrow_types=('string', 'int16', 'string', 'string', 'string', 'string'),
        key_cols=('plate_numb', 'direction'),
        fmt='arrow',
        rpc='get_bus_trails(target_date, cities)',
        retention_days=3,
    ),
    'bus_intercity': Dataset(
        name='bus_intercity',
        table='live.bus_intercity_trails_daily',
        # 這裡的 city 是 SubAuthorityID（業者代號），沿用 RPC 的欄位名。
        columns=('plate_numb', 'direction', 'route_uid', 'route_name', 'city', 'trail'),
        arrow_types=('string', 'int16', 'string', 'string', 'string', 'string'),
        key_cols=('plate_numb', 'direction'),
        fmt='arrow',
        rpc='get_bus_intercity_trails(target_date)',
        retention_days=3,
    ),
}


# ------------------------------------------------------------
# DB 讀取（keyset 分頁，全部以單日條件約束 + LIMIT）
# ------------------------------------------------------------

def count_rows(conn, ds: Dataset, day: date) -> int:
    """該日總筆數。先數再串流，串完比對，用來抓「中途被 refresh 撕裂」。"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {ds.table} WHERE day = %s", (day,))
        return int(cur.fetchone()[0])


def fetch_rows(conn, ds: Dataset, day: date, batch_size: int) -> list[tuple]:
    """keyset 分頁把單日全部撈回來。

    每批 = 一筆獨立短查詢（`WHERE day = %s AND (key...) > (...) ORDER BY key LIMIT n`），
    直接走 PK 索引 (day, <key_cols>)，不會全表掃描、不開長交易。
    """
    cols_sql = ', '.join(ds.columns)
    key_sql = ', '.join(ds.key_cols)
    n_keys = len(ds.key_cols)
    placeholders = ', '.join(['%s'] * n_keys)

    rows: list[tuple] = []
    cursor_key: tuple | None = None
    while True:
        with conn.cursor() as cur:
            if cursor_key is None:
                cur.execute(
                    f"SELECT {cols_sql} FROM {ds.table} "
                    f"WHERE day = %s ORDER BY {key_sql} LIMIT %s",
                    (day, batch_size),
                )
            else:
                cur.execute(
                    f"SELECT {cols_sql} FROM {ds.table} "
                    f"WHERE day = %s AND ({key_sql}) > ({placeholders}) "
                    f"ORDER BY {key_sql} LIMIT %s",
                    (day, *cursor_key, batch_size),
                )
            batch = cur.fetchall()

        if not batch:
            break
        rows.extend(batch)
        # key_cols 是 columns 的前綴，所以直接取每列前 n_keys 個值當游標
        cursor_key = tuple(batch[-1][:n_keys])
        if len(batch) < batch_size:
            break
    return rows


# ------------------------------------------------------------
# 寫檔
# ------------------------------------------------------------

def write_arrow(ds: Dataset, rows: Sequence[tuple], path: str) -> tuple[int, int]:
    """寫未壓縮 Arrow IPC file。回傳 (raw_bytes, file_bytes)。

    ⚠️ 不開 compression：arrow-js 讀不了壓縮的 record batch。
    """
    import pyarrow as pa

    type_map = {'string': pa.string(), 'int16': pa.int16()}
    schema = pa.schema([
        pa.field(col, type_map[t]) for col, t in zip(ds.columns, ds.arrow_types)
    ])
    arrays = [
        pa.array([r[i] for r in rows], type=schema.field(i).type)
        for i in range(len(ds.columns))
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)
    raw_bytes = table.nbytes

    with pa.OSFile(path, 'wb') as sink:
        # IpcWriteOptions 預設 compression=None —— 這正是我們要的，別動。
        with pa.ipc.new_file(sink, schema) as writer:
            writer.write_table(table)
    return raw_bytes, os.path.getsize(path)


def write_gzip_json(ds: Dataset, rows: Sequence[tuple], path: str) -> tuple[int, int]:
    """寫 gzip JSON（body = 純 row 物件陣列，形狀同 RPC 回傳）。回傳 (raw_bytes, file_bytes)。"""
    payload = [dict(zip(ds.columns, r)) for r in rows]
    raw = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    with gzip.open(path, 'wb', compresslevel=9) as f:
        f.write(raw)
    return len(raw), os.path.getsize(path)


def read_back_count(ds: Dataset, path: str) -> int:
    """把剛寫好的檔重新讀一次數筆數（檔內筆數 == DB 筆數 的驗證來源）。"""
    if ds.fmt == 'arrow':
        import pyarrow as pa
        with pa.memory_map(path, 'rb') as src:
            return pa.ipc.open_file(src).read_all().num_rows
    with gzip.open(path, 'rb') as f:
        return len(json.load(f))


# ------------------------------------------------------------
# S3
# ------------------------------------------------------------

def get_s3_client():
    import boto3
    kwargs: dict[str, Any] = {'region_name': config.S3_REGION}
    if config.S3_ACCESS_KEY and config.S3_SECRET_KEY:
        kwargs['aws_access_key_id'] = config.S3_ACCESS_KEY
        kwargs['aws_secret_access_key'] = config.S3_SECRET_KEY
    if config.S3_ENDPOINT:
        kwargs['endpoint_url'] = config.S3_ENDPOINT
    return boto3.client('s3', **kwargs)


def upload_and_verify(s3, bucket: str, key: str, path: str, ds: Dataset) -> None:
    """上傳後重新 HEAD 確認物件存在且大小一致。不一致就 raise。"""
    with open(path, 'rb') as f:
        body = f.read()
    put_kwargs: dict[str, Any] = {'Bucket': bucket, 'Key': key, 'Body': body}
    if ds.fmt == 'arrow':
        put_kwargs['ContentType'] = CONTENT_TYPE_ARROW
    else:
        # ContentEncoding=gzip 讓瀏覽器 fetch() 自動解壓，前端不用自己 gunzip
        put_kwargs['ContentType'] = 'application/json'
        put_kwargs['ContentEncoding'] = 'gzip'
    s3.put_object(**put_kwargs)

    head = s3.head_object(Bucket=bucket, Key=key)
    remote_size = int(head['ContentLength'])
    if remote_size != len(body):
        raise RuntimeError(
            f"上傳驗證失敗：s3://{bucket}/{key} ContentLength={remote_size} "
            f"≠ 本地 {len(body)} bytes"
        )
    logger.info(f"  [{ds.name}] HEAD 驗證 OK：s3://{bucket}/{key} ({remote_size:,} bytes)")


def update_manifest(s3, bucket: str, ds: Dataset, entries: list[dict]) -> None:
    """merge 既有 manifest 的 dates（同日覆蓋），寫回 trails/<dataset>/manifest.json。"""
    key = f"{S3_PREFIX}/{ds.name}/manifest.json"
    existing: dict[str, Any] = {}
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        logger.info(f"  [{ds.name}] manifest 不存在，建立新的")

    by_date = {d['date']: d for d in existing.get('dates', []) if isinstance(d, dict) and 'date' in d}
    for e in entries:
        by_date[e['date']] = e

    manifest = {
        'dataset': ds.name,
        'format': ds.fmt,
        'source_rpc': ds.rpc,
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'dates': sorted(by_date.values(), key=lambda d: d['date']),
    }
    body = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json')
    logger.info(f"  [{ds.name}] manifest 更新：{len(manifest['dates'])} 天")


# ------------------------------------------------------------
# 主流程
# ------------------------------------------------------------

def export_one(conn, s3, bucket: str, ds: Dataset, day: date,
               out_dir: str, batch_size: int, dry_run: bool) -> dict:
    """匯出單一 (dataset, day)。成功回傳 manifest entry，失敗 raise。"""
    day_str = day.isoformat()
    total = count_rows(conn, ds, day)

    # ⚠️ rows=0 防呆（硬性）：dates 類 mv 會謊報有資料（見 pulse BACKLOG BL-25），
    # 所以絕不能靠 mv／summary 表判斷，要以實際 rows 為準；0 rows 一律當失敗。
    if total == 0:
        raise RuntimeError(
            f"[{ds.name}] {day_str} 在 {ds.table} 查到 0 rows —— 不產檔、不上傳。"
            f" 可能原因：該日已超過 DB 保留天數（{ds.retention_days} 天）、collector 當天停擺、"
            f" 或 refresh_{ds.name}_trails_daily 沒跑。"
        )

    rows = fetch_rows(conn, ds, day, batch_size)
    if len(rows) != total:
        raise RuntimeError(
            f"[{ds.name}] {day_str} 串流筆數 {len(rows)} ≠ count(*) {total}"
            f"（多半是匯出途中撞到 refresh 的 DELETE+INSERT）。重跑即可。"
        )

    os.makedirs(out_dir, exist_ok=True)
    ext = 'arrow' if ds.fmt == 'arrow' else 'json.gz'
    filename = f"{day_str}.{ext}"
    path = os.path.join(out_dir, ds.name, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if ds.fmt == 'arrow':
        raw_bytes, file_bytes = write_arrow(ds, rows, path)
    else:
        raw_bytes, file_bytes = write_gzip_json(ds, rows, path)

    file_rows = read_back_count(ds, path)
    if file_rows != total:
        raise RuntimeError(f"[{ds.name}] {day_str} 檔內筆數 {file_rows} ≠ DB 筆數 {total}")

    key = f"{S3_PREFIX}/{ds.name}/{filename}"
    if dry_run:
        logger.info(f"  [{ds.name}] --dry-run：略過上傳（本地檔 {path}）")
    else:
        upload_and_verify(s3, bucket, key, path, ds)

    ratio = (file_bytes / raw_bytes * 100) if raw_bytes else 0
    logger.info(
        f"SUMMARY dataset={ds.name} date={day_str} rows={total:,} "
        f"raw={raw_bytes:,}B file={file_bytes:,}B ({ratio:.1f}%) "
        f"key={key}{' [dry-run]' if dry_run else ''}"
    )
    return {
        'date': day_str,
        'rows': total,
        'bytes': file_bytes,
        'file': filename,
    }


def export_range(datasets: Sequence[str] | None = None,
                 end_date: date | None = None,
                 backfill: int = 1,
                 dry_run: bool = False,
                 out_dir: str = DEFAULT_OUT_DIR,
                 batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """匯出 datasets × 最近 backfill 天，回傳統計 dict（CLI 與排程共用的入口）。

    ⚠️ `end_date=None` 代表「執行當下的昨天」，而且**一定要在這裡才解析**：
       排程是在啟動時註冊、每晚才執行的，若在註冊時就把日期算死，之後每一晚
       都會重複匯出同一天。

    參數錯誤／環境變數缺漏 → raise ValueError（CLI 對應 exit 2）。
    個別 (dataset, day) 失敗不中斷，跑完在回傳值的 failures 列出（CLI 對應 exit 1）。

    Returns:
        {'datasets', 'dates', 'ok', 'failed', 'failures', 'rows', 'bytes', 'dry_run'}
    """
    today_tpe = datetime.now(TAIPEI).date()
    if end_date is None:
        end_date = today_tpe - timedelta(days=1)

    # 凍結「還在寫入中的今天」會產生半天的殘檔，而且 rows>0 防呆抓不到 → 直接擋。
    if end_date >= today_tpe:
        raise ValueError(
            f"拒絕匯出 {end_date}：今天（Asia/Taipei {today_tpe}）資料仍在寫入，"
            f"凍結會得到不完整的一天。請等到隔天再跑。"
        )
    if backfill < 1:
        raise ValueError('backfill 需 >= 1')

    names = list(datasets) if datasets else list(DATASETS)
    unknown = [n for n in names if n not in DATASETS]
    if unknown:
        raise ValueError(f"未知 dataset：{unknown}；可用：{list(DATASETS)}")

    if not config.SUPABASE_DB_URL:
        raise ValueError('SUPABASE_DB_URL 未設定')

    bucket = config.S3_BUCKET
    s3 = None
    if not dry_run:
        if not bucket:
            raise ValueError('S3_BUCKET 未設定（或用 dry_run 只產本地檔）')
        s3 = get_s3_client()

    days = sorted(end_date - timedelta(days=i) for i in range(backfill))

    logger.info(
        f"匯出 datasets={names} dates={[d.isoformat() for d in days]} "
        f"{'[dry-run]' if dry_run else f'→ s3://{bucket}/{S3_PREFIX}/'}"
    )

    failures: list[str] = []
    ok = 0
    total_rows = 0
    total_bytes = 0
    conn = connect_supabase(autocommit=True)
    try:
        for name in names:
            ds = DATASETS[name]
            entries: list[dict] = []
            for day in days:
                try:
                    entry = export_one(conn, s3, bucket, ds, day,
                                       out_dir, batch_size, dry_run)
                except Exception as e:
                    logger.error(f"FAILED dataset={name} date={day.isoformat()}: {e}")
                    failures.append(f"{name}/{day.isoformat()}: {e}")
                    continue
                entries.append(entry)
                ok += 1
                total_rows += entry['rows']
                total_bytes += entry['bytes']
            if entries and not dry_run:
                try:
                    update_manifest(s3, bucket, ds, entries)
                except Exception as e:
                    logger.error(f"FAILED manifest dataset={name}: {e}")
                    failures.append(f"{name}/manifest: {e}")
    finally:
        conn.close()

    return {
        'datasets': names,
        'dates': [d.isoformat() for d in days],
        'ok': ok,
        'failed': len(failures),
        'failures': failures,
        'rows': total_rows,
        'bytes': total_bytes,
        'dry_run': dry_run,
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description='把單日動態軌跡從 Supabase 凍結成靜態檔上 S3（AR-14）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('--date', help='匯出日期 YYYY-MM-DD（預設＝昨天，Asia/Taipei）')
    ap.add_argument('--datasets', default=','.join(DATASETS),
                    help=f"逗號分隔，預設全部：{','.join(DATASETS)}")
    ap.add_argument('--backfill', type=int, default=1, metavar='N',
                    help='回補最近 N 天（從 --date 往前含當天），預設 1')
    ap.add_argument('--dry-run', action='store_true',
                    help='只產本地檔並驗筆數，不上傳、不動 manifest')
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR, help=f'本地輸出目錄（預設 {DEFAULT_OUT_DIR}）')
    ap.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                    help=f'keyset 分頁每批列數（預設 {DEFAULT_BATCH_SIZE}）')
    args = ap.parse_args()

    end_date = None
    if args.date:
        try:
            end_date = date.fromisoformat(args.date)
        except ValueError:
            logger.error(f"--date 格式錯誤：{args.date}（需 YYYY-MM-DD）")
            return 2

    try:
        stats = export_range(
            datasets=[n.strip() for n in args.datasets.split(',') if n.strip()],
            end_date=end_date,
            backfill=args.backfill,
            dry_run=args.dry_run,
            out_dir=args.out_dir,
            batch_size=args.batch_size,
        )
    except ValueError as e:
        logger.error(str(e))
        return 2

    if stats['failed']:
        logger.error(f"完成，但有 {stats['failed']} 項失敗：")
        for f in stats['failures']:
            logger.error(f"  - {f}")
        return 1

    logger.info(f"全部成功（{len(stats['datasets'])} datasets × {len(stats['dates'])} 天）")
    return 0


if __name__ == '__main__':
    sys.exit(main())
