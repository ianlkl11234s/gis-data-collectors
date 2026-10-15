#!/usr/bin/env python3
"""回補 ひまわり B13 發布幀：直接向上游抓近 ~5 天的整點幀 → 灰階拼圖 → R2 gray_jpeg_v1＋T2 npz＋DB image 欄。

為什麼不走 S3：himawari 在 2026-10-16 之前沒有 T2（只有 T3 原始 tar，放 DEEP_ARCHIVE，不 restore），
上游保留約 119 小時（targetTimes_fd.json 只列約 36 小時，但更舊的磚直接打得到，所以用整點算術列舉；
上游偶有整幀 404 的洞，記為 upstream_404，不算錯誤）。

編碼與 collector 共用 collectors/weather_raster/publish.py（mosaic_gray＋render），T2 小時檔格式與 meta 與
collector close_hour 相同（spool.write_hour_npz＋jma_raster.mosaic_t2_meta），所以回補產物與即時產物同規格。

冪等：
  R2／DB  frames 列 image_key 已填 且 published_at 在 89 天內 且 R2 HEAD 存在 → 跳過；否則重發並更新 published_at。
          DB 沒有對應 frames 列 → 照樣上 R2／T2，UPDATE 0 列，計 no_row（不補建列）。
  T2      S3 weather-grid/jma/himawari_b13/{YYYYMMDD JST}/{HH}.npz 已存在就不寫（collector 上線後的小時由 collector 自己寫）。
速率：每請求間隔 ≥ 1/--rps 秒（--rps 上限 1.0）。5 天約 118 幀×12 磚 ≈ 1,416 請求 ≈ 24 分鐘。

用法：
  python3 scripts/backfill_jma_himawari_imagery.py --dry-run --plan-only      # 只列舉與判斷跳過，不打上游
  python3 scripts/backfill_jma_himawari_imagery.py --dry-run --max-frames 3   # 實抓 3 幀、實際編碼量大小，不上傳不寫
  python3 scripts/backfill_jma_himawari_imagery.py                            # 實際上傳 R2＋T2＋寫 DB

--dry-run：讀 DB 判斷跳過（SUPABASE_DB_URL 未設則視為全部要發）；不上傳、不寫 DB、不寫 S3。
非 dry-run 需要 R2_*、S3_*、SUPABASE_DB_URL。
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: E402
import backfill_jma_imagery as base  # noqa: E402  共用 redact／db_connect／existing_rows／write_rows
from collectors.global_climate import jma_raster as jr  # noqa: E402
from collectors.global_climate.jma_common import JMA_BOSAI_BASE, new_jma_session  # noqa: E402
from collectors.weather_raster import publish as pub  # noqa: E402
from collectors.weather_raster import spool as sp  # noqa: E402

PRODUCT = "himawari_b13"
JST = jr.JST


def hourly_vts(start: datetime, end: datetime) -> list[datetime]:
    """[start, end] 內的 UTC 整點（含端點）。"""
    t = start.replace(minute=0, second=0, microsecond=0)
    if t < start:
        t += timedelta(hours=1)
    out = []
    while t <= end:
        out.append(t)
        t += timedelta(hours=1)
    return out


def t2_key(vt: datetime) -> tuple[str, str, str]:
    j = vt.astimezone(JST)
    day, hh = j.strftime("%Y%m%d"), j.strftime("%H")
    return f"weather-grid/{jr.SOURCE}/{PRODUCT}/{day}/{hh}.npz", day, hh


class Fetcher:
    def __init__(self, rps: float):
        self.session = new_jma_session("jma_himawari_backfill")
        self.gap = 1.0 / min(rps, 1.0)
        self._last = 0.0
        self.requests = 0

    def get(self, url: str) -> tuple[int, bytes]:
        wait = self._last + self.gap - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()
        self.requests += 1
        try:
            r = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
            return r.status_code, (r.content if r.status_code == 200 else b"")
        except Exception:  # noqa: BLE001
            return -1, b""

    def frame(self, vt: datetime) -> tuple[str, dict]:
        """回 (狀態, tiles)。狀態：ok／upstream_404（第一磚 404＝上游無此幀）／incomplete。"""
        p = jr.PRODUCTS[PRODUCT]
        stamp = vt.strftime(jr.VT_FMT)
        x0, y0, nx, ny = jr.canvas(p.coarse_zoom)
        tiles: dict = {}
        for i, (x, y) in enumerate((x, y) for y in range(y0, y0 + ny) for x in range(x0, x0 + nx)):
            url = f"{JMA_BOSAI_BASE}/" + p.url.format(bt=stamp, vt=stamp, z=p.coarse_zoom, x=x, y=y)
            status, body = self.get(url)
            if status == 404 and i == 0:
                return "upstream_404", {}
            if status != 200:
                return "incomplete", tiles
            tiles[(x, y)] = body
        return "ok", tiles


def write_t2(s3, vt: datetime, grid, georef: dict) -> bool:
    key, day, hh = t2_key(vt)
    with tempfile.TemporaryDirectory() as td:
        gp = Path(td) / "g.npz"
        sp.save_grid(gp, grid)
        local = Path(td) / f"{hh}.npz"
        sp.write_hour_npz(local, [(vt.strftime(jr.VT_FMT), gp)], grid.shape,
                          jr.mosaic_t2_meta(PRODUCT, day, hh, georef),
                          compression=config.JAPAN_JMA_RASTER_GRID_COMPRESSION)
        from storage.s3 import put_verified
        return put_verified(s3, local, key, sp.sha256_file(local), "STANDARD")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--hours", type=int, default=118, help="回補最近幾小時（上游約留 119h；預設 118）")
    ap.add_argument("--start", help="UTC ISO（覆寫 --hours），例 2026-10-11T00:00")
    ap.add_argument("--end", help="UTC ISO，預設＝現在往前一個整點")
    ap.add_argument("--rps", type=float, default=1.0, help="每秒請求數上限（≤1）")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--plan-only", action="store_true", help="只列舉與判斷跳過，不打上游（需搭配 --dry-run）")
    ap.add_argument("--no-t2", action="store_true", help="不寫 S3 T2")
    ap.add_argument("--max-frames", type=int, default=0, help="最多實抓幾幀（0＝不限；驗證用）")
    args = ap.parse_args()
    if args.plan_only and not args.dry_run:
        raise SystemExit("--plan-only 只能搭配 --dry-run")

    now = datetime.now(timezone.utc)
    end = (datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc) if args.end
           else now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1))
    start = (datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc) if args.start
             else end - timedelta(hours=args.hours - 1))
    vts = hourly_vts(start, end)

    r2 = s3 = None
    if not args.dry_run:
        from storage.r2 import get_r2_storage
        from storage.s3 import S3Storage
        r2 = get_r2_storage()
        if r2 is None or not config.SUPABASE_DB_URL:
            raise SystemExit("非 dry-run 需要 R2_* 與 SUPABASE_DB_URL")
        if not args.no_t2:
            s3 = S3Storage()
    conn = base.db_connect()
    if conn is None:
        print("⚠ SUPABASE_DB_URL 未設定：不判斷 DB 既有 image 欄，視為全部要發")
    rows = base.existing_rows(conn, PRODUCT, start - timedelta(minutes=1), end + timedelta(minutes=1))

    p = jr.PRODUCTS[PRODUCT]
    x0, y0, nx, ny = jr.canvas(p.coarse_zoom)
    georef = sp.georef(p.coarse_zoom, x0, y0, nx * 256, ny * 256)
    bounds = georef["bounds_lonlat"]
    fetcher = Fetcher(args.rps)
    st = {"vts": len(vts), "skipped": 0, "to_fetch": 0, "fetched": 0, "upstream_404": 0, "incomplete": 0,
          "publish": 0, "uploaded": 0, "updated": 0, "failed": 0, "no_row": 0, "db_rows": len(rows),
          "t2_written": 0, "t2_exists": 0, "t2_failed": 0}
    sizes: list[int] = []
    batch: list[tuple[datetime, dict]] = []
    keep = now - timedelta(days=base.KEEP_DAYS)
    for vt in vts:
        row = rows.get(vt)
        if row is None and conn is not None:
            st["no_row"] += 1
        if row and row[1] and row[2] and row[2] > keep:
            try:
                if args.dry_run or r2.head(row[1]) is not None:
                    st["skipped"] += 1
                    continue
            except Exception as e:  # noqa: BLE001
                print(f"  ⚠ R2 HEAD {row[1]}：{e}（改為重發）")
        st["to_fetch"] += 1
        if args.plan_only:
            continue
        if args.max_frames and st["fetched"] >= args.max_frames:
            continue
        st["fetched"] += 1
        status, tiles = fetcher.frame(vt)
        if status != "ok":
            st[status] += 1
            print(f"  · {vt:%Y-%m-%d %H:%M}Z {status}")
            continue
        grid = pub.mosaic_gray(tiles, x0, y0, nx, ny)
        if grid is None:
            st["incomplete"] += 1
            continue
        r = pub.render(PRODUCT, grid, bounds_lonlat=bounds)
        st["publish"] += 1
        sizes.append(len(r["data"]))
        if args.dry_run:
            continue
        if s3 is not None:
            key = t2_key(vt)[0]
            try:
                if s3.head(key) is not None:
                    st["t2_exists"] += 1
                elif write_t2(s3, vt, grid, georef):
                    st["t2_written"] += 1
                else:
                    st["t2_failed"] += 1
            except Exception as e:  # noqa: BLE001
                st["t2_failed"] += 1
                print(f"  ✗ T2 {key}：{e}")
        key = pub.r2_key(PRODUCT, vt, r["ext"])
        try:
            r2.upload_image(key, r["data"], r["content_type"])
            st["uploaded"] += 1
            batch.append((vt, pub.image_fields(key, r, datetime.now(timezone.utc))))
        except Exception as e:  # noqa: BLE001
            st["failed"] += 1
            print(f"  ✗ R2 {key}：{e}")
        if len(batch) >= 24:
            st["updated"] += base.write_rows(conn, PRODUCT, batch)
            batch = []
    if batch:
        st["updated"] += base.write_rows(conn, PRODUCT, batch)
    st["requests"] = fetcher.requests
    st["mean_bytes"] = int(statistics.mean(sizes)) if sizes else 0
    st["max_bytes"] = max(sizes) if sizes else 0
    print(f"[{PRODUCT}] {json.dumps(st, ensure_ascii=False)}")
    print(f"{'DRY-RUN ' if args.dry_run else ''}{'PLAN ' if args.plan_only else ''}"
          f"{start:%Y-%m-%d %H:%M}Z..{end:%Y-%m-%d %H:%M}Z：{len(vts)} 個整點，待處理 {st['to_fetch']}、跳過 {st['skipped']}；"
          f"預估上游請求 ≤ {st['to_fetch'] * nx * ny}（約 {st['to_fetch'] * nx * ny / min(args.rps, 1.0) / 60:.0f} 分鐘）")
    if conn is not None:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001
        print(f"✗ {type(e).__name__}: {base.redact(e)}")
        sys.exit(1)
