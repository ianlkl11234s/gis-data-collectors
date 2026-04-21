#!/usr/bin/env python3
"""
seed_reservoir_sediment.py — 水庫淤積量 seed（WRA 32727）

抓 https://opendata.wra.gov.tw/api/v2/572bda99-0593-4aee-9409-03c82423f8eb
→ parse → UPDATE reference.reservoir_geometry 三個欄位：
   latest_measured_capacity_wan / latest_sediment_wan / latest_measured_at

用法：
    python3 scripts/seed_reservoir_sediment.py         # 抓 API + update
    python3 scripts/seed_reservoir_sediment.py --dry   # 只抓不寫

覆蓋範圍：WRA API 目前只回「臺灣北區」15 筆（翡翠 / 石門 / 新山 / 寶山 / 青潭堰 等），
其他區域資料未公告。未來若 API 擴充覆蓋，直接重跑即可。

更新頻率：不定期（但建議每年跑一次，因 WRA 每年測量）。

依賴：requests, psycopg2
2026-04-21 初版
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import requests
import psycopg2

API_URL = (
    "https://opendata.wra.gov.tw/api/v2/"
    "572bda99-0593-4aee-9409-03c82423f8eb"
    "?format=JSON"
)


def parse_roc_date(s: str) -> date | None:
    """將 ROC YYYMM / YYYYMM 轉成西元 DATE（取該月 1 日）。
    11312 → 民國 113 年 12 月 → 2024-12-01
    10611 →      106 年 11 月 → 2017-11-01
    """
    if not s:
        return None
    s = str(s).strip()
    if len(s) < 5:
        return None
    try:
        if len(s) == 5:
            roc_y, month = int(s[:3]), int(s[3:5])
        elif len(s) == 6:
            roc_y, month = int(s[:4]), int(s[4:6])
        elif len(s) == 7:
            # YYYMMDD e.g. 1130712
            roc_y, month, day = int(s[:3]), int(s[3:5]), int(s[5:7])
            return date(roc_y + 1911, month, day)
        else:
            return None
        return date(roc_y + 1911, month, 1)
    except (ValueError, TypeError):
        return None


def _flt(v):
    if v in (None, "", "-"):
        return None
    try:
        return float(str(v).replace(",", ""))
    except ValueError:
        return None


def fetch() -> list[dict]:
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def parse(raw: list[dict]) -> list[dict]:
    out = []
    for r in raw:
        name = (r.get("水庫名稱") or "").strip()
        if not name:
            continue
        out.append({
            "res_name": name,
            "roc_year": r.get("民國年"),
            "latest_measured_capacity_wan":
                _flt(r.get("最新施測有效容量")),
            "latest_sediment_wan":
                _flt(r.get("水庫淤積量")),
            "latest_measured_at":
                parse_roc_date(r.get("最近完成庫容測量時間")),
            "region": (r.get("地區別") or "").strip(),
        })
    return out


def update(conn, rows: list[dict]) -> dict:
    sql = """
        UPDATE reference.reservoir_geometry
        SET latest_measured_capacity_wan = %(latest_measured_capacity_wan)s,
            latest_sediment_wan          = %(latest_sediment_wan)s,
            latest_measured_at           = %(latest_measured_at)s,
            updated_at                   = now()
        WHERE res_name = %(res_name)s
        RETURNING compare_id, res_name
    """
    stats = {"updated": 0, "not_found": []}
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, r)
            res = cur.fetchone()
            if res:
                stats["updated"] += 1
            else:
                stats["not_found"].append(r["res_name"])
    conn.commit()
    return stats


def load_db_url() -> str:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("SUPABASE_DB_URL="):
            return line.split("=", 1)[1]
    raise RuntimeError("SUPABASE_DB_URL not set")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="只抓不寫")
    args = ap.parse_args()

    raw = fetch()
    rows = parse(raw)
    print(f"[seed] fetched {len(raw)} raw, parsed {len(rows)}")
    for r in rows[:3]:
        print(f"  {r['res_name']:<12} cap={r['latest_measured_capacity_wan']} "
              f"sed={r['latest_sediment_wan']} at={r['latest_measured_at']}")

    if args.dry:
        print("[seed] --dry: skip DB")
        return 0

    db_url = os.environ.get("SUPABASE_DB_URL") or load_db_url()
    conn = psycopg2.connect(db_url)
    try:
        stats = update(conn, rows)
        print(f"[seed] updated={stats['updated']}/{len(rows)}")
        if stats["not_found"]:
            print(f"[seed] res_name not in reference.reservoir_geometry "
                  f"(需手動對 name): {stats['not_found']}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
