#!/usr/bin/env python3
"""JMA 季節觀測一次性歷史回填（migration 439 apply 之後執行；可重跑，全部 ON CONFLICT DO NOTHING）。

內容：
  1. 生物季節累年 CSV（sakura/data/ruinenchi/{004,005,013,014,015,016}.csv，Shift_JIS）
     指定年份（預設 2021–2025）的觀測值＋平年値 → live.jma_phenology_observations（source='ruinenchi_csv'）
     日期「月×100＋日」；0／rm 0＝缺測不寫列；冬春種目 12 月觀測記在隔年欄、秋種目 1 月觀測記在前一年欄。
  2. 山頂初冠雪目視紀錄（fsn_cap/records/{code}.html，44 山）指定寒候年（預設 2021–2026）
     → live.jma_snowcap（judgement_method='visual'，source='records_html'）

預設只預演（抓資料、算筆數，不連 DB）。正式寫入：
  python3 scripts/backfill_jma_phenology.py --apply                         # 用 config.SUPABASE_DB_URL
  python3 scripts/backfill_jma_phenology.py --apply --env-file ../gis-platform/.env --dsn-key DATABASE_POOL_URL
--raw-dir 可把下載的原檔存下（冷儲存用）。連線字串一律不印出。
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors.base import TAIPEI_TZ  # noqa: E402
from collectors.global_climate.jma_common import new_jma_session  # noqa: E402
from collectors.global_climate.jma_seasonal import (  # noqa: E402
    RUINENCHI_CODES, URL_AMEDAS_TABLE, URL_RUINENCHI, URL_SNOWCAP, URL_SNOWCAP_RECORD,
    build_station_index, http_time, parse_ruinenchi_csv, parse_snowcap_page, parse_snowcap_record,
)


def _get(session, url: str):
    last = None
    for _ in range(3):
        try:
            r = session.get(url, timeout=60)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2)
    raise RuntimeError(f"{url}: {last}")


def _save(raw_dir: Path | None, name: str, content: bytes) -> None:
    if raw_dir:
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / name).write_bytes(content)


def collect(years: list[int], snow_years: list[int], raw_dir: Path | None) -> list[dict]:
    session = new_jma_session("jma-seasonal-backfill")
    collected_at = datetime.now(TAIPEI_TZ).isoformat()
    csv_text = {}
    csv_time = {}
    for code in RUINENCHI_CODES:
        url = URL_RUINENCHI.format(code=code)
        r = _get(session, url)
        _save(raw_dir, f"ruinenchi_{code}.csv", r.content)
        csv_text[code] = r.content.decode("shift_jis")
        csv_time[code] = http_time(r.headers.get("Last-Modified"))
    amedas = _get(session, URL_AMEDAS_TABLE)
    _save(raw_dir, "amedastable.json", amedas.content)
    stations = build_station_index(csv_text["004"], amedas.json())

    rows: list[dict] = []
    for code in RUINENCHI_CODES:
        rows += parse_ruinenchi_csv(csv_text[code], code, stations, years=years,
                                    source_ref=URL_RUINENCHI.format(code=code),
                                    source_time=csv_time[code], collected_at=collected_at)

    if snow_years:
        page = _get(session, URL_SNOWCAP)
        page.encoding = "utf-8"
        _save(raw_dir, "fsn_cap_index.html", page.content)
        _first, _status, mountains = parse_snowcap_page(page.text, collected_at=collected_at)
        for name, mt in mountains.items():
            r = _get(session, URL_SNOWCAP_RECORD.format(code=mt["code"]))
            r.encoding = "utf-8"
            _save(raw_dir, f"fsn_cap_record_{mt['code']}.html", r.content)
            rows += parse_snowcap_record(r.text, mountain_code=mt["code"], mountain_name=name,
                                         observatory=mt["observatory"], years=snow_years,
                                         collected_at=collected_at)
            time.sleep(0.3)
    return rows


def summarize(rows: list[dict]) -> None:
    from collections import Counter
    ph = [r for r in rows if r["_type"] == "phenology"]
    sc = [r for r in rows if r["_type"] == "snowcap"]
    print(f"phenology 列數 {len(ph)}（無座標 {sum(1 for r in ph if r['lat'] is None)}、"
          f"無站號 {sum(1 for r in ph if not r['station_no'])}、代替種目 {sum(1 for r in ph if r['alt_species'])}）")
    for (code, year), n in sorted(Counter((r["phenomenon_code"], r["season_year"]) for r in ph).items()):
        print(f"  {code} {year}: {n}")
    cross = [r for r in ph if int(r["obs_date"][:4]) != r["season_year"]]
    print(f"  跨年列（obs_date 年 ≠ season_year）{len(cross)}")
    print(f"snowcap（visual）列數 {len(sc)}，山數 {len({r['mountain_code'] for r in sc})}")
    for year, n in sorted(Counter(r["season_year"] for r in sc).items()):
        print(f"  寒候年 {year}: {n}")


def write(rows: list[dict], dsn: str) -> None:
    import psycopg2
    from psycopg2.extras import execute_values
    from storage.supabase_writer import SupabaseWriter

    tables = SupabaseWriter._JMA_SEASONAL_TABLES
    conn = psycopg2.connect(dsn)
    try:
        with conn, conn.cursor() as cur:
            for kind in ("phenology", "snowcap"):
                table, cols, _req = tables[kind]
                vals = [tuple(r.get(c) for c in cols) for r in rows if r["_type"] == kind]
                if not vals:
                    continue
                cur.execute(f"SELECT count(*) FROM {table}")
                before = cur.fetchone()[0]
                execute_values(cur, f"INSERT INTO {table} ({','.join(cols)}) VALUES %s ON CONFLICT DO NOTHING",
                               vals, page_size=500)
                cur.execute(f"SELECT count(*) FROM {table}")
                print(f"{table}: 送出 {len(vals)}，新增 {cur.fetchone()[0] - before}")
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--years", default="2021-2025", help="生物季節年份範圍（含），預設 2021-2025")
    ap.add_argument("--snow-years", default="2021-2026", help="初冠雪目視寒候年範圍（含）；空字串＝不回填")
    ap.add_argument("--raw-dir", type=Path, help="下載原檔存放目錄（選填）")
    ap.add_argument("--apply", action="store_true", help="正式寫入（預設只預演算筆數）")
    ap.add_argument("--env-file", type=Path, help="從 dotenv 檔讀連線字串（不印出）")
    ap.add_argument("--dsn-key", default="SUPABASE_DB_URL", help="dotenv／環境變數的鍵名")
    args = ap.parse_args()

    def rng(s: str) -> list[int]:
        if not s:
            return []
        a, _, b = s.partition("-")
        return list(range(int(a), int(b or a) + 1))

    rows = collect(rng(args.years), rng(args.snow_years), args.raw_dir)
    summarize(rows)
    if not args.apply:
        print("（預演：未連 DB；加 --apply 正式寫入）")
        return
    if args.env_file:
        from dotenv import dotenv_values
        dsn = dotenv_values(args.env_file).get(args.dsn_key)
    else:
        import os
        import config
        dsn = os.getenv(args.dsn_key) or config.SUPABASE_DB_URL
    if not dsn:
        sys.exit(f"找不到連線字串（鍵 {args.dsn_key}）")
    write(rows, dsn)


if __name__ == "__main__":
    main()
