#!/usr/bin/env python3
"""回補 JMA 發布幀：從 S3 T2 網格重算 LA PNG → R2 imagery/jma/… → 寫 live.weather_raster_frames image 欄。

契約：analytics docs/topic-research/japan_opendata/jma-frontend-contract.md §7。編碼與 collector 共用
collectors/weather_raster/publish.py，所以回補產物與即時發布相同規格。

T2 來源：
  radar／rasrf／risk_land／risk_inund／snow_depth  weather-grid/jma/{product}/{YYYYMMDD JST}/{HH}.npz（spool.load_hour_npz）
  uv_anal                                          weather-grid/jma/uv_anal/{YYYY}/{YYYYMMDD}.npz（同格式，一日一檔）
  sst_him／sea_ice                                 weather-grid/jma/{product}/{YYYY}/{YYYYMMDD}.npz（陣列 sst_dc／ice）

冪等（R2 只保留 90 天）：DB 列 image_key 已填 且 published_at 在 89 天內 且 R2 HEAD 存在 → 跳過；否則重發並更新
published_at＝now()。T2 裡的缺幀（missing）不產圖、DB 列不動。DB 沒有對應 frames 列的幀照樣上 R2 但 UPDATE 0 列，
計入 no_row（回報用，不補建列）。

用法：
  python3 scripts/backfill_jma_imagery.py --dry-run                                # 預設 2026-10-10..今天（JST）全部產品
  python3 scripts/backfill_jma_imagery.py --products snow_depth,radar --start 2026-10-14 --end 2026-10-14 --dry-run
  python3 scripts/backfill_jma_imagery.py --products snow_depth                    # 實際上傳 R2＋寫 DB

--dry-run：讀 S3 T2、實際編碼（量測 PNG 大小）、讀 DB 判斷跳過；不上傳、不寫 DB。
需環境變數：S3_*（讀 T2）；非 dry-run 另需 R2_*、SUPABASE_DB_URL。DB 只讀時若 SUPABASE_DB_URL 未設，dry-run 視為全部要發。
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from collectors.weather_raster import publish as pub  # noqa: E402
from collectors.weather_raster import spool as sp  # noqa: E402

JST = timezone(timedelta(hours=9))
SOURCE = "jma"
RASTER = ("radar", "rasrf", "risk_land", "risk_inund", "snow_depth")
ALL = RASTER + ("uv_anal", "sst_him", "sea_ice")
KEEP_DAYS = 89
_DSN_RE = re.compile(r"postgres(ql)?://\S+")


def redact(msg) -> str:
    return _DSN_RE.sub("<REDACTED>", str(msg))


def parse_vt(vt: str) -> datetime:
    return datetime.strptime(vt, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def days(start: date, end: date):
    d = start
    while d <= end:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


def npz_meta(data: bytes) -> dict:
    with np.load(io.BytesIO(data)) as z:
        return json.loads(str(z["meta"]))


def iter_frames(s3, product: str, day: str):
    """yield (valid_time, grid, render_kwargs)；缺幀不 yield。"""
    if product in RASTER or product == "uv_anal":
        if product in RASTER:
            keys = sorted(f["key"] for f in s3.list_files(f"weather-grid/{SOURCE}/{product}/{day}/")
                          if f["key"].endswith(".npz"))
        else:
            keys = [f"weather-grid/{SOURCE}/uv_anal/{day[:4]}/{day}.npz"]
        for key in keys:
            data = s3.get_file(key)
            if not data:
                continue
            bounds = npz_meta(data)["georef"]["bounds_lonlat"]
            for vt, grid in sp.load_hour_npz(io.BytesIO(data)):
                if grid is not None:
                    yield parse_vt(vt), grid, {"bounds_lonlat": bounds}
        return
    data = s3.get_file(f"weather-grid/{SOURCE}/{product}/{day[:4]}/{day}.npz")
    if not data:
        return
    with np.load(io.BytesIO(data)) as z:
        meta = json.loads(str(z["meta"]))
        grid = z["sst_dc" if product == "sst_him" else "ice"]
    d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=JST)
    yield d0.astimezone(timezone.utc), grid, {"georef": meta["georef"]}


def db_connect():
    if not config.SUPABASE_DB_URL:
        return None
    import psycopg2
    try:
        return psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15)
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"DB 連線失敗：{redact(e)}")


def existing_rows(conn, product: str, t0: datetime, t1: datetime) -> dict:
    """{valid_time(UTC datetime): (status, image_key, published_at)}"""
    if conn is None:
        return {}
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '60s'")
        cur.execute(
            "SELECT valid_time, status, image_key, published_at FROM live.weather_raster_frames "
            "WHERE source=%s AND product=%s AND valid_time >= %s AND valid_time < %s",
            (SOURCE, product, t0, t1))
        rows = {r[0].astimezone(timezone.utc): r[1:] for r in cur.fetchall()}
    conn.commit()
    return rows


UPDATE_SQL = (
    "UPDATE live.weather_raster_frames SET image_key=%s, image_bytes=%s, image_width=%s, image_height=%s, "
    "lon_min=%s, lon_max=%s, lat_min=%s, lat_max=%s, encoding=%s, published_at=%s "
    "WHERE source=%s AND product=%s AND valid_time=%s")


def write_rows(conn, product: str, batch: list[tuple[datetime, dict]]) -> int:
    n = 0
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '60s'")
        for vt, f in batch:
            cur.execute(UPDATE_SQL, (f["image_key"], f["image_bytes"], f["image_width"], f["image_height"],
                                     f["lon_min"], f["lon_max"], f["lat_min"], f["lat_max"], f["encoding"],
                                     f["published_at"], SOURCE, product, vt))
            n += cur.rowcount
    conn.commit()
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    today = datetime.now(JST).date()
    ap.add_argument("--start", default="2026-10-10")
    ap.add_argument("--end", default=today.isoformat())
    ap.add_argument("--products", default=",".join(ALL))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-frames", type=int, default=0, help="每產品最多處理幾幀（0＝不限；驗證用）")
    args = ap.parse_args()
    products = [p.strip() for p in args.products.split(",") if p.strip()]
    bad = [p for p in products if p not in ALL]
    if bad:
        raise SystemExit(f"不支援的產品：{bad}（可用 {ALL}）")
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    from storage.s3 import S3Storage
    s3 = S3Storage()
    r2 = None
    if not args.dry_run:
        from storage.r2 import get_r2_storage
        r2 = get_r2_storage()
        if r2 is None or not config.SUPABASE_DB_URL:
            raise SystemExit("非 dry-run 需要 R2_* 與 SUPABASE_DB_URL")
    conn = db_connect()
    if conn is None:
        print("⚠ SUPABASE_DB_URL 未設定：不判斷 DB 既有 image 欄，視為全部要發")

    now = datetime.now(timezone.utc)
    t0 = datetime(start.year, start.month, start.day, tzinfo=JST)
    t1 = datetime(end.year, end.month, end.day, tzinfo=JST) + timedelta(days=1)
    report = {}
    for product in products:
        rows = existing_rows(conn, product, t0 - timedelta(hours=1), t1)
        st = {"t2_frames": 0, "skipped": 0, "publish": 0, "bytes": 0, "sizes": [], "no_row": 0,
              "db_missing_rows": sum(1 for r in rows.values() if r[0] == "missing"), "updated": 0, "failed": 0,
              "encode_sec": 0.0}
        batch: list[tuple[datetime, dict]] = []
        stop = False
        for day in days(start, end):
            for vt, grid, kw in iter_frames(s3, product, day):
                st["t2_frames"] += 1
                row = rows.get(vt)
                if row is None and conn is not None:
                    st["no_row"] += 1
                if row and row[1] and row[2] and row[2] > now - timedelta(days=KEEP_DAYS):
                    try:
                        if args.dry_run or r2.head(row[1]) is not None:
                            st["skipped"] += 1
                            continue
                    except Exception as e:  # noqa: BLE001
                        print(f"  ⚠ R2 HEAD {row[1]}：{e}（改為重發）")
                tic = time.perf_counter()
                r = pub.render(product, grid, **kw)
                st["encode_sec"] += time.perf_counter() - tic
                st["publish"] += 1
                st["bytes"] += len(r["data"])
                st["sizes"].append(len(r["data"]))
                if not args.dry_run:
                    key = pub.r2_key(product, vt, r["ext"])
                    try:
                        r2.upload_image(key, r["data"], r["content_type"])
                        batch.append((vt, pub.image_fields(key, r, datetime.now(timezone.utc))))
                    except Exception as e:  # noqa: BLE001
                        st["failed"] += 1
                        print(f"  ✗ R2 {key}：{e}")
                    if len(batch) >= 50:
                        st["updated"] += write_rows(conn, product, batch)
                        batch = []
                if args.max_frames and st["publish"] >= args.max_frames:
                    stop = True
                    break
            if stop:
                break
        if batch:
            st["updated"] += write_rows(conn, product, batch)
        sizes = st.pop("sizes")
        st["mean_bytes"] = int(statistics.mean(sizes)) if sizes else 0
        st["max_bytes"] = max(sizes) if sizes else 0
        st["encode_sec"] = round(st["encode_sec"], 1)
        report[product] = st
        print(f"[{product}] {json.dumps(st, ensure_ascii=False)}", flush=True)
    total = sum(v["bytes"] for v in report.values())
    print(f"{'DRY-RUN ' if args.dry_run else ''}合計 發布 {sum(v['publish'] for v in report.values())} 幀、"
          f"{total / 1e6:.1f} MB（{args.start}..{args.end}）")
    if conn is not None:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"✗ {type(e).__name__}: {redact(e)}")
        sys.exit(1)
