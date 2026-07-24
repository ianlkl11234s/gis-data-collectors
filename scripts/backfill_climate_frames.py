#!/usr/bin/env python3
"""一次性 backfill：補 GFS wind10m 過去 N 天 00Z f000 analysis 幀。

我們自家 raw 歸檔近期只有 f120，缺 f000。從 NOAA AWS Open Data
（noaa-gfs-bdp-pds，公開免認證）直接補抓各日 00Z f000 的 UGRD/VGRD 10m subset，
上傳自家 S3 raw 結構（沿用 collector 的 key 慣例）+ 註冊
live.global_climate_grids（leadtime_hr=0，ON CONFLICT DO NOTHING）。

之後跑 climate_bake 一輪，這些 row 會被 _plan_wind_frames 撿為過去 14 天
daily 00Z analysis 幀。CMEMS 歷史不需 backfill：既有 daily nc row 已在 grids/S3，
bake 直接讀既有檔切多幀。

用法：
  ./cvenv/bin/python scripts/backfill_climate_frames.py --days 14
  ./cvenv/bin/python scripts/backfill_climate_frames.py --days 14 --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2

import config
from collectors.base import TAIPEI_TZ
from collectors.global_climate.noaa_gfs import NoaaGfsCollector, GFS_VARIABLES

WIND = next(v for v in GFS_VARIABLES if v["id"] == "gfs_wind10m")
# GFS 全球格點 bbox（與 collector 同值）
BBOX_WKT = "POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14, help="回補過去幾天（不含今日）")
    ap.add_argument("--dry-run", action="store_true", help="不上傳 S3、不寫 DB，只驗證抓檔")
    args = ap.parse_args()

    coll = NoaaGfsCollector()
    now = datetime.now(timezone.utc)
    rows: list[tuple] = []

    with tempfile.TemporaryDirectory(prefix="bf_gfs_") as td:
        tmp = Path(td)
        for d in range(1, args.days + 1):
            day = (now - timedelta(days=d)).replace(hour=0, minute=0, second=0, microsecond=0)
            ds = day.strftime("%Y%m%d")
            idx = coll._fetch_idx(ds, "00", 0)
            if not idx:
                print(f"  skip {ds} 00Z f000（無 idx）")
                continue
            grib = tmp / f"wind10m_{ds}_f000.grib2"
            if not coll._range_pull(ds, "00", 0, idx, [WIND["pattern"]], grib):
                print(f"  skip {ds}（range pull 失敗）")
                continue
            digest = coll._compute_digest(grib, WIND)
            if not digest:
                print(f"  skip {ds}（digest 空）")
                continue
            s3_uri = None if args.dry_run else coll._upload_s3(grib, "gfs_wind10m", day)
            rows.append((
                "gfs_wind10m", day.isoformat(), day.isoformat(), 0, BBOX_WKT,
                json.dumps(digest, ensure_ascii=False), s3_uri, None,
                grib.stat().st_size, datetime.now(TAIPEI_TZ).isoformat(),
            ))
            print(f"  ok {ds} 00Z f000  size={grib.stat().st_size}  s3={s3_uri}")

        if not rows:
            print("沒有可回補的資料"); return 1
        if args.dry_run:
            print(f"[dry-run] 會寫 {len(rows)} 筆（未上傳、未寫 DB）"); return 0

        with psycopg2.connect(config.SUPABASE_DB_URL) as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO live.global_climate_grids
                      (dataset_id, observed_at, init_at, leadtime_hr, bbox, digest,
                       s3_uri, pmtiles_uri, raw_size_bytes, collected_at)
                    VALUES (%s,%s,%s,%s, ST_GeomFromText(%s,4326), %s::jsonb, %s,%s,%s,%s)
                    ON CONFLICT (dataset_id, observed_at) DO NOTHING
                    """,
                    r,
                )
            conn.commit()
    print(f"backfill 完成：{len(rows)} 筆 gfs_wind10m f000 已註冊（ON CONFLICT DO NOTHING）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
