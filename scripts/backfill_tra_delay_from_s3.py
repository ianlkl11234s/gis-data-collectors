#!/usr/bin/env python3
"""台鐵誤點歷史回填：S3 歸檔 → analytics.tra_train_delay_daily

背景
────
live.train_positions 只保留 7 天（metadata.retention_policies），
更早的誤點歷史只存在 S3 歸檔裡。本腳本把指定日期的 raw JSON 灌進
analytics.tra_positions_import，再呼叫 analytics.refresh_tra_delay_daily(),
複用 migration 369 那一份聚合邏輯（不在 Python 端另寫一套，避免 drift）。

S3 上有兩種歸檔格式（皆為 collector 原始回傳，含 DelayTime）：
  1. tra_train/archives/YYYY-MM-DD.tar.gz   一天一包（2026-02-28 ~ 2026-03-18）
  2. tra_train/YYYY/MM/DD/*.json            逐檔（2025-12 ~ 2026-02）

⚠ 已知限制
  - reference.daily_schedules 的台鐵班表只從 2026-03-03 開始。更早的日期
    回填後 scheduled_trains / coverage_pct / near_destination 會是 NULL，
    只有觀測端指標（observed_trains / delayed_over_N / max / p90）有值。
  - refresh 讀的是 union view（live.train_positions ∪ tra_positions_import）。
    若對「近 7 天、live 端還留著」的日期用 --force 重新回填，同一天會同時
    存在於兩邊而被重複計算（obs_count 加倍）。回填請以 live 保留期之前的
    日期為限；近 7 天要重算直接呼叫 analytics.refresh_tra_delay_daily() 即可。

用法
────
    python3 scripts/backfill_tra_delay_from_s3.py --list
    python3 scripts/backfill_tra_delay_from_s3.py --from 2026-03-03 --to 2026-03-18
    python3 scripts/backfill_tra_delay_from_s3.py --from 2026-01-01 --to 2026-01-31 --force
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3
import psycopg2

import config

COLLECTOR = "tra_train"
ARCHIVE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.tar\.gz$")
# _fetch_time 是 collector 端的 naive 本地時間；已比對同筆 TDX UpdateTime
# （2026-03-17T00:01:00+08:00 vs _fetch_time 2026-03-17T00:01:33）確認為台灣時間。
TAIPEI_OFFSET = "+08"


def _s3():
    return boto3.client(
        "s3",
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=config.S3_REGION or "ap-northeast-1",
    )


def _daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


# ── S3 盤點 ──────────────────────────────────────────────────────

def list_available(s3) -> dict[str, str]:
    """回傳 {YYYY-MM-DD: 'archive' | 'legacy'}"""
    avail: dict[str, str] = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=f"{COLLECTOR}/archives/"):
        for obj in page.get("Contents", []):
            m = ARCHIVE_RE.match(obj["Key"].rsplit("/", 1)[-1])
            if m:
                avail[m.group(1)] = "archive"

    # 舊格式：tra_train/YYYY/MM/DD/*.json —— 只掃到「日」層級即可
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=f"{COLLECTOR}/2", Delimiter="/"):
        for pre in page.get("CommonPrefixes", []):
            year = pre["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if not (year.isdigit() and len(year) == 4):
                continue
            for mpage in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=pre["Prefix"], Delimiter="/"):
                for mpre in mpage.get("CommonPrefixes", []):
                    month = mpre["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                    for dpage in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=mpre["Prefix"], Delimiter="/"):
                        for dpre in dpage.get("CommonPrefixes", []):
                            day = dpre["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                            key = f"{year}-{month}-{day}"
                            avail.setdefault(key, "legacy")
    return avail


# ── 解析 ─────────────────────────────────────────────────────────

def _rows_from_payload(payload: dict) -> list[tuple]:
    """collector 原始回傳 dict → COPY 用的 tuple list"""
    fetch_time = payload.get("fetch_time")
    rows = []
    for r in payload.get("data", []):
        if not r.get("TrainNo"):
            continue
        tt = r.get("TrainTypeName") or {}
        train_type = tt.get("Zh_tw", "") if isinstance(tt, dict) else str(tt)
        ts = r.get("_fetch_time") or fetch_time
        if not ts:
            continue
        rows.append((
            str(r["TrainNo"]),
            train_type,
            str(r.get("StationID") or ""),
            int(r.get("DelayTime") or 0),
            f"{ts}{TAIPEI_OFFSET}",
        ))
    return rows


def fetch_archive_day(s3, day: str) -> list[tuple]:
    key = f"{COLLECTOR}/archives/{day}.tar.gz"
    buf = io.BytesIO()
    s3.download_fileobj(config.S3_BUCKET, key, buf)
    buf.seek(0)
    rows: list[tuple] = []
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile() or not member.name.endswith(".json"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            try:
                rows.extend(_rows_from_payload(json.load(f)))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"     ⚠ 跳過 {member.name}: {e}")
    return rows


def fetch_legacy_day(s3, day: str) -> list[tuple]:
    y, m, d = day.split("-")
    prefix = f"{COLLECTOR}/{y}/{m}/{d}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        o["Key"]
        for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix)
        for o in page.get("Contents", [])
        if o["Key"].endswith(".json") and not o["Key"].endswith("latest.json")
    ]
    if not keys:
        return []

    def _get(key: str) -> list[tuple]:
        b = io.BytesIO()
        s3.download_fileobj(config.S3_BUCKET, key, b)
        b.seek(0)
        raw = b.read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        try:
            return _rows_from_payload(json.loads(raw))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return []

    rows: list[tuple] = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for chunk in pool.map(_get, keys):
            rows.extend(chunk)
    return rows


# ── 寫入 ─────────────────────────────────────────────────────────

def load_day(conn, s3, day: str, kind: str, force: bool) -> tuple[int, int]:
    """回傳 (raw 列數, 聚合車次列數)"""
    with conn.cursor() as cur:
        if not force:
            cur.execute(
                "SELECT count(*) FROM analytics.tra_train_delay_daily WHERE service_date = %s",
                (day,),
            )
            if cur.fetchone()[0] > 0:
                print(f"   ⏭  {day} 已有資料，跳過（--force 可覆寫）")
                return (0, -1)

    rows = fetch_archive_day(s3, day) if kind == "archive" else fetch_legacy_day(s3, day)
    if not rows:
        print(f"   ⚠ {day} S3 無可用資料")
        return (0, 0)

    buf = io.StringIO()
    for tn, tt, st, dl, ts in rows:
        # COPY text 格式：欄位以 \t 分隔，需跳脫反斜線與定位字元
        clean = [str(v).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")
                 for v in (tn, tt, st, dl, ts)]
        buf.write("\t".join(clean) + "\n")
    buf.seek(0)

    with conn.cursor() as cur:
        cur.execute("TRUNCATE analytics.tra_positions_import")
        cur.copy_expert(
            "COPY analytics.tra_positions_import "
            "(train_no, train_type, station_id, delay_minutes, collected_at) FROM STDIN",
            buf,
        )
        cur.execute("SELECT analytics.refresh_tra_delay_daily(%s)", (day,))
        inserted = cur.fetchone()[0]
        cur.execute("TRUNCATE analytics.tra_positions_import")
    conn.commit()
    return (len(rows), inserted)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="d_from", help="起始日 YYYY-MM-DD")
    ap.add_argument("--to", dest="d_to", help="結束日 YYYY-MM-DD")
    ap.add_argument("--list", action="store_true", help="只列出 S3 可回填的日期")
    ap.add_argument("--force", action="store_true", help="覆寫已存在的日期")
    args = ap.parse_args()

    if not config.S3_BUCKET:
        print("✗ S3_BUCKET 未設定"); return 1
    if not config.SUPABASE_DB_URL:
        print("✗ SUPABASE_DB_URL 未設定"); return 1

    s3 = _s3()
    print("盤點 S3 歸檔…")
    avail = list_available(s3)
    if not avail:
        print("✗ S3 上找不到任何 tra_train 歸檔"); return 1

    days_sorted = sorted(avail)
    if args.list:
        arch = [d for d in days_sorted if avail[d] == "archive"]
        leg = [d for d in days_sorted if avail[d] == "legacy"]
        print(f"\n可回填共 {len(avail)} 天：{days_sorted[0]} ~ {days_sorted[-1]}")
        print(f"  archive 格式 {len(arch)} 天：{arch[0] if arch else '-'} ~ {arch[-1] if arch else '-'}")
        print(f"  legacy  格式 {len(leg)} 天：{leg[0] if leg else '-'} ~ {leg[-1] if leg else '-'}")
        print("\n⚠ 班表（reference.daily_schedules）只從 2026-03-03 起，")
        print("  更早的日期回填後只有觀測端指標，無 scheduled_trains / coverage_pct。")
        return 0

    if not args.d_from or not args.d_to:
        ap.error("需要 --from 與 --to（或用 --list 查看範圍）")

    d1 = datetime.strptime(args.d_from, "%Y-%m-%d").date()
    d2 = datetime.strptime(args.d_to, "%Y-%m-%d").date()
    targets = [d.isoformat() for d in _daterange(d1, d2) if d.isoformat() in avail]
    missing = [d.isoformat() for d in _daterange(d1, d2) if d.isoformat() not in avail]

    print(f"\n區間 {args.d_from} ~ {args.d_to}：S3 有 {len(targets)} 天、缺 {len(missing)} 天")
    if missing:
        print(f"  缺：{missing[0]} ~ {missing[-1]}（共 {len(missing)} 天，S3 上沒有歸檔）")
    if not targets:
        return 0

    conn = psycopg2.connect(config.SUPABASE_DB_URL)
    ok = skipped = failed = 0
    total_rows = 0
    try:
        for day in targets:
            print(f"\n▶ {day} [{avail[day]}]")
            try:
                raw_n, agg_n = load_day(conn, s3, day, avail[day], args.force)
            except Exception as e:
                conn.rollback()
                print(f"   ✗ 失敗：{e}")
                failed += 1
                continue
            if agg_n == -1:
                skipped += 1
            elif agg_n == 0:
                failed += 1
            else:
                ok += 1
                total_rows += raw_n
                print(f"   ✓ raw {raw_n:,} 筆 → 車次 {agg_n:,} 列")
    finally:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE analytics.tra_positions_import")
        conn.commit()
        conn.close()

    print(f"\n{'=' * 50}")
    print(f"完成 {ok} 天 | 跳過 {skipped} 天 | 失敗 {failed} 天 | raw 共 {total_rows:,} 筆")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
