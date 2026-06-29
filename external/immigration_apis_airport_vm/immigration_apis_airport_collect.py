#!/usr/bin/env python3
"""immigration_apis_airport 單檔 collector — HiCloud VM 專用版。

跟主 repo (data-collectors) 完全一致的儲存規格：
  ┌── Supabase ──────────────────────────────────
  │  history : realtime.border_airport_snapshot  (INSERT, append-only)
  │  columns : airport, terminal, in_out, in_out_code,
  │            gender, nationality, age_band, pax_count,
  │            endpoint_code, collected_at
  └─────────────────────────────────────────────
  ┌── 本地 JSON snapshot（供每日 archive 上 S3）──
  │  path : <DATA_DIR>/immigration_apis_airport/YYYY/MM/DD/iaa_HHMM.json
  └─────────────────────────────────────────────

來源：移民署 APIS https://opendata.immigration.gov.tw/APIS/{code}
  6 active 端點：TPE1/TPE5/TPE51/TPE52/RMQ5/TSA1

⚠️ Taiwan IP required — opendata.immigration.gov.tw 擋 Zeabur 等國際雲商出口 IP，
   本檔必須在 HiCloud VM 跑。對應主 repo 的 IMMIGRATION_APIS_AIRPORT_ENABLED 須設 false。
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import requests
import urllib3
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# ────────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent
load_dotenv(APP_DIR / ".env")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    sys.exit("FATAL: SUPABASE_DB_URL 未設定（請編輯 .env）")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/immigration-apis-airport/data"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

TAIPEI_TZ = timezone(timedelta(hours=8))
APIS_BASE = "https://opendata.immigration.gov.tw/APIS"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 6 個 active 端點（探勘 2026-06-28 確認）
ENDPOINTS = [
    {"code": "TPE1",  "airport": "TPE", "terminal": None, "in_out": "in"},
    {"code": "TPE5",  "airport": "TPE", "terminal": None, "in_out": "out"},
    {"code": "TPE51", "airport": "TPE", "terminal": "1",  "in_out": "in"},
    {"code": "TPE52", "airport": "TPE", "terminal": "2",  "in_out": "out"},
    {"code": "RMQ5",  "airport": "RMQ", "terminal": None, "in_out": "out"},
    {"code": "TSA1",  "airport": "TSA", "terminal": None, "in_out": "in"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://opendata.immigration.gov.tw/",
    "Origin": "https://opendata.immigration.gov.tw",
}


def _int(v) -> Optional[int]:
    try: return int(str(v).strip().replace(",", ""))
    except (TypeError, ValueError): return None


# ────────────────────────────────────────────────────────────────────
def fetch_one(code: str) -> list[dict]:
    resp = requests.get(f"{APIS_BASE}/{code}", headers=HEADERS,
                        timeout=REQUEST_TIMEOUT, verify=False)
    resp.raise_for_status()
    try:
        return resp.json() or []
    except Exception:
        return []


def fetch_all(now: datetime) -> tuple[list[dict], dict, list[str]]:
    """抓 6 端點 → 統一 records list"""
    records: list[dict] = []
    endpoint_stats: dict[str, int] = {}
    failed: list[str] = []

    iso = now.isoformat()
    for ep in ENDPOINTS:
        code = ep["code"]
        try:
            rows = fetch_one(code)
        except requests.RequestException as e:
            failed.append(code)
            logging.warning(f"[{code}] 抓取失敗: {e}")
            continue

        before = len(records)
        for r in rows:
            pax = _int(r.get("paxCnt"))
            if pax is None or pax <= 0:
                continue
            io_code = (r.get("inOutTransit") or "").strip()
            io_label = "in" if io_code == "1" else ("out" if io_code == "5" else "transit")
            records.append({
                "airport":       (r.get("airport") or ep["airport"] or "").strip() or None,
                "terminal":      (r.get("terminal") or ep["terminal"] or "").strip() or None,
                "in_out":        io_label,
                "in_out_code":   io_code or None,
                "gender":        (r.get("gender") or "").strip() or None,
                "nationality":   (r.get("nationality") or "").strip() or None,
                "age_band":      (r.get("age") or "").strip() or None,
                "pax_count":     pax,
                "endpoint_code": code,
                "collected_at":  iso,
            })
        endpoint_stats[code] = len(records) - before

    return records, endpoint_stats, failed


# ────────────────────────────────────────────────────────────────────
# Local snapshot
# ────────────────────────────────────────────────────────────────────
def save_snapshot(result: dict, ts: datetime) -> Path:
    date_dir = DATA_DIR / "immigration_apis_airport" / ts.strftime("%Y/%m/%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    filepath = date_dir / f"iaa_{ts.strftime('%H%M')}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, default=str)
    return filepath


# ────────────────────────────────────────────────────────────────────
# Supabase write — append-only, no upsert
# ────────────────────────────────────────────────────────────────────
HISTORY_TABLE = "realtime.border_airport_snapshot"
COLUMNS = [
    "airport", "terminal", "in_out", "in_out_code",
    "gender", "nationality", "age_band", "pax_count",
    "endpoint_code", "collected_at",
]


def write_to_supabase(records: list[dict]) -> int:
    if not records:
        return 0
    values = [tuple(r.get(c) for c in COLUMNS) for r in records]
    col_names = ",".join(COLUMNS)
    sql = f"INSERT INTO {HISTORY_TABLE} ({col_names}) VALUES %s"
    with psycopg2.connect(DB_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=1000)
        conn.commit()
    return len(values)


# ────────────────────────────────────────────────────────────────────
def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("immigration_apis_airport")
    ts = datetime.now(TAIPEI_TZ)

    try:
        records, stats, failed = fetch_all(ts)
        log.info(f"抓到 {len(records)} 細格 / endpoint_stats={stats} / failed={failed}")

        snap_path = save_snapshot({
            "fetch_time": ts.isoformat(),
            "row_count": len(records),
            "endpoint_stats": stats,
            "failed": failed,
            "data": records,
        }, ts)
        log.info(f"snapshot → {snap_path}")

        n = write_to_supabase(records)
        log.info(f"Supabase 寫入: {n} 筆")
        return 0
    except Exception as exc:
        log.error(f"FAILED: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
