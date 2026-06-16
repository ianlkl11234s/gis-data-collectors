#!/usr/bin/env python3
"""cdc_public_health_weekly 單檔 collector — HiCloud VM 專用版（疾管署公衛週報）。

跟主 repo (data-collectors/collectors/cdc_public_health_weekly.py) 完全一致的 schema：
  ┌── Supabase ──────────────────────────────────
  │  table   : realtime.public_health_weekly
  │  UNIQUE  : (disease_code, iso_year, iso_week, county_code,
  │             township_code, age_group, gender, is_imported)
  │  conflict: DO NOTHING
  └─────────────────────────────────────────────

3 個 dataset：
  - rods-influenza               RODS_Influenza_like_illness.csv       類流感急診
  - aagstable-weekly-dengue      Weekly_Age_County_Gender_061.csv      登革熱週確診（到鄉鎮）
  - rods-enteroviral-infection   RODS_EnteroviralInfection.csv         腸病毒急診

排程：每週四 11:00 抓一次（CDC 約 10:00 發布上週資料）。
過濾：只保留近 2 年（KEEP_YEARS=2）。

⚠ 為什麼走外部：Zeabur 出口 IP 連 od.cdc.gov.tw timeout（IP 段被擋；本檔 commit 訊息有實證）。
⚠ od.cdc.gov.tw 憑證缺 SKI → verify=False 必開。
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

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

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/cdc-public-health/data"))
KEEP_YEARS = int(os.environ.get("KEEP_YEARS", "2"))

TAIPEI_TZ = timezone(timedelta(hours=8))
HTTP_TIMEOUT = (15, 60)

# CDC 同 NHI 憑證缺 SKI
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATASETS = [
    {"disease_code": "influenza",
     "filename": "RODS_Influenza_like_illness.csv",
     "dataset_id": "rods-influenza",
     "kind": "rods"},
    {"disease_code": "dengue",
     "filename": "Weekly_Age_County_Gender_061.csv",
     "dataset_id": "aagstable-weekly-dengue",
     "kind": "dengue"},
    {"disease_code": "enterovirus",
     "filename": "RODS_EnteroviralInfection.csv",
     "dataset_id": "rods-enteroviral-infection",
     "kind": "rods"},
]
CDC_CSV_BASE = "https://od.cdc.gov.tw/eic"

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; TaipeiGISBot/1.0; +https://github.com/)",
    "Accept": "text/csv, application/csv, */*",
})


# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────
def _num(v):
    try:
        if v is None or v == "" or v == "-":
            return None
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _int(v):
    f = _num(v)
    return int(f) if f is not None else None


def _bool(v):
    if v is None or v == "":
        return None
    s = str(v).strip()
    if s in ("1", "Y", "y", "是", "true", "True"):
        return True
    if s in ("0", "N", "n", "否", "false", "False"):
        return False
    return None


def _norm(row: dict, *keys: str):
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() not in ("", "-"):
            return str(v).strip()
    return None


def parse_rods(row, disease_code, dataset_id):
    year = _int(_norm(row, "年", "Year"))
    week = _int(_norm(row, "週", "Week"))
    cnt  = _num(_norm(row, "類流感急診就診人次", "腸病毒急診就診人次", "就診數"))
    if not year or not week or cnt is None:
        return None
    return {
        "disease_code":   disease_code,
        "iso_year":       year,
        "iso_week":       week,
        "county_code":    _norm(row, "縣市別代碼", "縣市碼") or "",
        "county_name":    _norm(row, "縣市", "County"),
        "township_code":  "",
        "township_name":  None,
        "age_group":      _norm(row, "年齡別", "年齡") or "",
        "gender":         "",
        "is_imported":    None,
        "metric_value":   cnt,
        "source_dataset": dataset_id,
    }


def parse_dengue(row, disease_code, dataset_id):
    year = _int(_norm(row, "發病年份", "年"))
    week = _int(_norm(row, "發病週別", "週"))
    cnt  = _num(_norm(row, "確定病例數", "病例數"))
    if not year or not week or cnt is None:
        return None
    return {
        "disease_code":   disease_code,
        "iso_year":       year,
        "iso_week":       week,
        "county_code":    _norm(row, "縣市別代碼", "縣市碼") or "",
        "county_name":    _norm(row, "縣市", "County"),
        "township_code":  _norm(row, "鄉鎮別代碼", "鄉鎮碼") or "",
        "township_name":  _norm(row, "鄉鎮", "Township"),
        "age_group":      _norm(row, "年齡層", "年齡") or "",
        "gender":         _norm(row, "性別") or "",
        "is_imported":    _bool(_norm(row, "是否為境外移入", "境外")),
        "metric_value":   cnt,
        "source_dataset": dataset_id,
    }


# ────────────────────────────────────────────────────────────────────
# Fetch + parse
# ────────────────────────────────────────────────────────────────────
def fetch_dataset(ds: dict, cutoff_year: int, log: logging.Logger) -> list[dict]:
    url = f"{CDC_CSV_BASE}/{ds['filename']}"
    resp = SESSION.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    parser = parse_dengue if ds["kind"] == "dengue" else parse_rods
    out: list[dict] = []
    for r in reader:
        n = parser(r, ds["disease_code"], ds["dataset_id"])
        if n and n["iso_year"] >= cutoff_year:
            out.append(n)
    log.info(f"{ds['disease_code']}: {len(out)} 筆（>= {cutoff_year}）")
    return out


# ────────────────────────────────────────────────────────────────────
# Snapshot + Supabase write
# ────────────────────────────────────────────────────────────────────
def save_snapshot(result: dict, ts: datetime) -> Path:
    date_dir = DATA_DIR / "cdc_public_health_weekly" / ts.strftime("%Y/%m/%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    fp = date_dir / f"cdc_public_health_weekly_{ts.strftime('%H%M')}.json"
    summary = {**result, "data_sample": result.get("data", [])[:50]}
    summary.pop("data", None)
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, default=str)
    return fp


TABLE = "realtime.public_health_weekly"
COLUMNS = [
    "disease_code", "iso_year", "iso_week",
    "county_code", "county_name", "township_code", "township_name",
    "age_group", "gender", "is_imported",
    "metric_value", "source_dataset", "collected_at",
]
CONFLICT_KEY = (
    "(disease_code, iso_year, iso_week, county_code, township_code, "
    "age_group, gender, is_imported)"
)


def write_to_supabase(records: list[dict], ts: datetime) -> int:
    if not records:
        return 0
    rows = []
    for r in records:
        rows.append((
            r["disease_code"], r["iso_year"], r["iso_week"],
            r["county_code"], r["county_name"],
            r["township_code"], r["township_name"],
            r["age_group"], r["gender"], r["is_imported"],
            r["metric_value"], r["source_dataset"], ts.isoformat(),
        ))
    sql = (
        f"INSERT INTO {TABLE} ({','.join(COLUMNS)}) VALUES %s "
        f"ON CONFLICT {CONFLICT_KEY} DO NOTHING"
    )
    with psycopg2.connect(DB_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
            inserted = cur.rowcount
        conn.commit()
    return inserted if inserted >= 0 else len(rows)


# ────────────────────────────────────────────────────────────────────
def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("cdc_public_health_weekly")
    ts = datetime.now(TAIPEI_TZ)
    cutoff_year = ts.year - KEEP_YEARS + 1

    all_records: list[dict] = []
    stats: dict[str, int | str] = {}
    for ds in DATASETS:
        try:
            rows = fetch_dataset(ds, cutoff_year, log)
            all_records.extend(rows)
            stats[ds["disease_code"]] = len(rows)
        except Exception as e:
            stats[ds["disease_code"]] = f"error: {str(e)[:160]}"
            log.error(f"{ds['disease_code']}: {e}")

    try:
        inserted = write_to_supabase(all_records, ts)
        log.info(f"✓ DB 寫入 {inserted} 筆（總候選 {len(all_records)}）")
    except Exception as e:
        log.error(f"DB 寫入失敗: {e}")
        inserted = 0

    result = {
        "fetch_time": ts.isoformat(),
        "total": len(all_records),
        "inserted": inserted,
        "per_disease": stats,
        "data": all_records,
    }
    snap = save_snapshot(result, ts)
    log.info(f"snapshot: {snap}")
    return 0 if inserted > 0 or len(all_records) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
