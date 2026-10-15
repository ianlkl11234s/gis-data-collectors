#!/usr/bin/env python3
"""回補 JMA 分析圖：從 S3 T1 逐日累積 → S3 快照 → R2 imagery/jma/ana_*/ → live.weather_raster_frames（upsert 列＋image 欄）。

模組與規格：collectors/weather_raster/analysis.py、docs/JMA_RASTER_CONTRACT.md「分析圖」。collector（jma_raster）每輪只看近
JAPAN_JMA_ANALYSIS_LOOKBACK_DAYS 天；本腳本補任意區間（預設 2026-10-10..昨天 JST）。

冪等：快照以 sig（演算法版本＋納入的 T1 日清單）判斷是否可沿用；R2 同 key 覆寫；DB 以 PK upsert
（與 collector 相同的「status 只升不降」規則），image 欄一律更新。--recompute 忽略既有快照、全部從 T1 重算。

用法：
  python3 scripts/backfill_jma_analysis.py --dry-run                       # 全 family、2026-10-10..昨天
  python3 scripts/backfill_jma_analysis.py --families snow --start 2026-10-10 --end 2026-10-15 --dry-run
  python3 scripts/backfill_jma_analysis.py --families rain,risk_land       # 實際寫 S3 快照＋R2＋DB

--dry-run：讀 S3 T1（快照有就讀）、實際累積與編碼（量 PNG 大小）；快照只留記憶體，不寫 S3／R2／DB。
需環境變數：S3_*；非 dry-run 另需 R2_* 與 SUPABASE_DB_URL。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from collectors.weather_raster import analysis as ana  # noqa: E402
from collectors.weather_raster import publish as pub  # noqa: E402

_DSN_RE = re.compile(r"postgres(ql)?://\S+")

COLS = ["source", "product", "valid_time", "status", "nonzero_pixels"] + list(pub.IMAGE_FIELDS)
UPSERT_SQL = (
    f"INSERT INTO live.weather_raster_frames ({','.join(COLS)}) VALUES %s "
    "ON CONFLICT (source, product, valid_time) DO UPDATE SET "
    + ",".join(f"{c}=EXCLUDED.{c}" for c in COLS[3:]) + ",collected_at=now() "
    "WHERE (CASE live.weather_raster_frames.status WHEN 'missing' THEN 0 WHEN 'partial' THEN 1 WHEN 'ok' THEN 2 ELSE -1 END)"
    " <= (CASE EXCLUDED.status WHEN 'missing' THEN 0 WHEN 'partial' THEN 1 WHEN 'ok' THEN 2 ELSE -1 END)"
)


def redact(msg) -> str:
    return _DSN_RE.sub("<REDACTED>", str(msg))


def write_rows(conn, rows: list[dict]) -> int:
    from psycopg2.extras import execute_values
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '60s'")
        cur.execute("SET LOCAL lock_timeout = '10s'")
        execute_values(cur, UPSERT_SQL, [tuple(r.get(c) for c in COLS) for r in rows], page_size=200)
        n = cur.rowcount
    conn.commit()
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    today = datetime.now(ana.JST).date()
    ap.add_argument("--start", default=config.JAPAN_JMA_ANALYSIS_EPOCH)
    ap.add_argument("--end", default=(today - timedelta(days=1)).isoformat())
    ap.add_argument("--families", default=",".join(ana.FAMILIES))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--recompute", action="store_true", help="忽略既有 S3 快照，全部從 T1 重算")
    args = ap.parse_args()
    fams = [f.strip() for f in args.families.split(",") if f.strip()]
    bad = [f for f in fams if f not in ana.FAMILIES]
    if bad:
        raise SystemExit(f"不支援的 family：{bad}（可用 {list(ana.FAMILIES)}）")
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    from storage.s3 import S3Storage
    store = ana.S3StateStore(S3Storage(), read_only=args.dry_run)
    r2, conn = None, None
    if not args.dry_run:
        from storage.r2 import get_r2_storage
        r2 = get_r2_storage()
        if r2 is None or not config.SUPABASE_DB_URL:
            raise SystemExit("非 dry-run 需要 R2_* 與 SUPABASE_DB_URL")
        import psycopg2
        try:
            conn = psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15)
        except Exception as e:  # noqa: BLE001
            raise SystemExit(f"DB 連線失敗：{redact(e)}")
    engine = ana.AnalysisEngine(store, r2, epoch=date.fromisoformat(config.JAPAN_JMA_ANALYSIS_EPOCH),
                                min_coverage=config.JAPAN_JMA_ANALYSIS_MIN_COVERAGE, log_prefix="[analysis-backfill]")
    tic = time.perf_counter()
    report: dict[str, dict] = {}
    for name in fams:
        family = ana.FAMILIES[name]
        batch: list[dict] = []
        for d in ana.target_days(start, end):
            if not engine.eligible(family, d, today):
                continue
            rows = engine.publish_day(family, d, recompute=args.recompute, dry_run=args.dry_run)
            for r in rows:
                st = report.setdefault(r["product"], {"frames": 0, "status": Counter(), "sizes": [], "cov": [],
                                                      "max_value": 0, "nonzero_last": 0, "days": []})
                st["frames"] += 1
                st["status"][r["status"]] += 1
                st["days"].append(r["_day"])
                if "_render" in r:
                    st["sizes"].append(r["_bytes"])
                    st["cov"].append(r["_render"]["coverage_mean"])
                    st["max_value"] = max(st["max_value"], r["_render"]["max_value"])
                    st["nonzero_last"] = r.get("nonzero_pixels", 0)
            batch.extend(ana.db_row(r) for r in rows)
        if conn is not None and batch:
            print(f"[{name}] DB upsert {write_rows(conn, batch)} 列")
    for product, st in report.items():
        sizes = st.pop("sizes")
        cov = st.pop("cov")
        days = st.pop("days")
        out = {**st, "status": dict(st["status"]), "range": f"{days[0]}..{days[-1]}" if days else None,
               "png_mean_bytes": int(statistics.mean(sizes)) if sizes else 0, "png_max_bytes": max(sizes, default=0),
               "coverage_mean_last": cov[-1] if cov else None}
        print(f"[{product}] {json.dumps(out, ensure_ascii=False)}", flush=True)
    print(f"{'DRY-RUN ' if args.dry_run else ''}engine {json.dumps(engine.stats)}；{time.perf_counter() - tic:.1f}s"
          f"（{start}..{end}）")
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
