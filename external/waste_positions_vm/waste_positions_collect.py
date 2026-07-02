#!/usr/bin/env python3
"""waste_positions 單檔 collector — HiCloud VM 專用版（垃圾車 GPS：高雄/新北/台南）。

跟主 repo (data-collectors) 完全一致的儲存規格：
  ┌── Supabase ──────────────────────────────────
  │  history : spatial.waste_positions_realtime (INSERT, append-only)
  │  columns : city, vehicle_no, route_id, status,
  │            geometry (SRID 4326), observed_at, source_url
  └─────────────────────────────────────────────
  ┌── 本地 JSON snapshot（供每日 archive 上 S3）──
  │  path : <DATA_DIR>/waste_positions/YYYY/MM/DD/waste_positions_HHMM.json
  └─────────────────────────────────────────────

DB 寫入失敗 → 存 <DATA_DIR>/buffer/*.json，下輪開頭自動補寫（vm_buffer.py）。

來源 (3 家政府開放資料 GPS)：
  - 高雄 openapi.kcg.gov.tw     JSON wrapper, x/y, ISO8601
  - 新北 data.ntpc.gov.tw       CSV (UTF-8 BOM), longitude/latitude
  - 台南 soa.tainan.gov.tw      JSON wrapper (與高雄同框架)

排程：每 2 分鐘跑一次，01:00–06:00 quiet hours 跳過。
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# ────────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent

# VM 部署時 vm_buffer.py 跟本檔放同目錄；repo 內直接跑則 fallback 到 external/vm_common/
try:
    from vm_buffer import connect_with_retry, flush_pending, has_pending, save_batch
except ImportError:
    sys.path.insert(0, str(APP_DIR.parent / "vm_common"))
    from vm_buffer import connect_with_retry, flush_pending, has_pending, save_batch

load_dotenv(APP_DIR / ".env")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    sys.exit("FATAL: SUPABASE_DB_URL 未設定（請編輯 .env）")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/waste-positions/data"))
BUFFER_DIR = DATA_DIR / "buffer"  # DB 寫入失敗的暫存區（vm_buffer）
CITIES = os.environ.get("CITIES", "Kaohsiung,NewTaipei,Tainan").split(",")
QUIET_HOURS = os.environ.get("QUIET_HOURS", "01-06")  # "HH-HH" or "off"

TAIPEI_TZ = timezone(timedelta(hours=8))
HTTP_TIMEOUT = (10, 60)
RETRY_WAITS = [5, 15]  # 失敗後 sleep, 共 3 次嘗試

CITY_NAMES = {"Kaohsiung": "高雄市", "NewTaipei": "新北市", "Tainan": "臺南市"}
ENDPOINTS = {
    "Kaohsiung": "https://openapi.kcg.gov.tw/Api/Service/Get/aaf4ce4b-4ca8-43de-bfaf-6dc97e89cac0",
    "NewTaipei": "https://data.ntpc.gov.tw/api/datasets/28ab4122-60e1-4065-98e5-abccb69aaca6/csv/file",
    "Tainan":    "https://soa.tainan.gov.tw/Api/Service/Get/2c8a70d5-06f2-4353-9e92-c40d33bcd969",
}
PARKED_KEYWORDS = ("停車場", "區隊", "清潔隊", "車隊")
TIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; TaipeiGISBot/1.0; +https://github.com/)",
    "Accept": "*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
})


# ────────────────────────────────────────────────────────────────────
# Utils
# ────────────────────────────────────────────────────────────────────
def parse_quiet_hours(spec: str) -> tuple[int, int] | None:
    if not spec or spec.lower() in ("none", "off"):
        return None
    try:
        s, e = spec.split("-")
        return int(s), int(e)
    except ValueError:
        return None


def is_quiet(hour: int, qh: tuple[int, int] | None) -> bool:
    if qh is None:
        return False
    s, e = qh
    if s == e:
        return False
    return s <= hour < e if s < e else (hour >= s or hour < e)


def classify_status(location: str) -> str:
    if not location:
        return "unknown"
    return "parked" if any(kw in location for kw in PARKED_KEYWORDS) else "collecting"


def parse_observed_at(raw: str | None, fallback: datetime) -> str:
    if not raw:
        return fallback.isoformat()
    raw = raw.strip()
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=TAIPEI_TZ).isoformat()
        except ValueError:
            continue
    return fallback.isoformat()


def safe_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def get_with_retry(url: str, label: str, log: logging.Logger) -> requests.Response:
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < len(RETRY_WAITS):
                log.warning(f"{label} retry {attempt + 1}/3 after {RETRY_WAITS[attempt]}s: {type(e).__name__}")
                _time.sleep(RETRY_WAITS[attempt])
                continue
            raise


# ────────────────────────────────────────────────────────────────────
# Fetchers (3 家)
# ────────────────────────────────────────────────────────────────────
def normalize_soa(rows: list, city: str, url: str, fetch_time: datetime) -> list[dict]:
    """高雄/台南共用 SOA schema (x/y/linid/car/time/location)"""
    out = []
    for row in rows:
        lng = safe_float(row.get("x"))
        lat = safe_float(row.get("y"))
        if lat is None or lng is None:
            continue
        location = row.get("location") or ""
        out.append({
            "city": city,
            "vehicle_no": (row.get("car") or "").strip(),
            "route_id": (row.get("linid") or "").strip() or None,
            "lat": lat,
            "lng": lng,
            "location": location,
            "observed_at": parse_observed_at(row.get("time"), fetch_time),
            "status": classify_status(location),
            "source_url": url,
        })
    return out


def fetch_kaohsiung(fetch_time: datetime, log) -> list[dict]:
    url = ENDPOINTS["Kaohsiung"]
    body = get_with_retry(url, "高雄市", log).json()
    if not body.get("success"):
        raise RuntimeError(f"Kaohsiung API rejected: {body.get('message')}")
    return normalize_soa(body.get("data") or [], "高雄市", url, fetch_time)


def fetch_tainan(fetch_time: datetime, log) -> list[dict]:
    url = ENDPOINTS["Tainan"]
    body = get_with_retry(url, "臺南市", log).json()
    if not body.get("success"):
        raise RuntimeError(f"Tainan API rejected: {body.get('message')}")
    return normalize_soa(body.get("data") or [], "臺南市", url, fetch_time)


def fetch_new_taipei(fetch_time: datetime, log) -> list[dict]:
    url = ENDPOINTS["NewTaipei"]
    r = get_with_retry(url, "新北市", log)
    text = r.content.decode("utf-8-sig")  # CSV with BOM
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        lng = safe_float(row.get("longitude"))
        lat = safe_float(row.get("latitude"))
        if lat is None or lng is None:
            continue
        location = row.get("location") or ""
        out.append({
            "city": "新北市",
            "vehicle_no": (row.get("car") or "").strip(),
            "route_id": (row.get("lineid") or "").strip() or None,
            "lat": lat,
            "lng": lng,
            "location": location,
            "observed_at": parse_observed_at(row.get("time"), fetch_time),
            "status": classify_status(location),
            "district": (row.get("cityname") or "").strip() or None,
            "source_url": url,
        })
    return out


FETCHERS = {"Kaohsiung": fetch_kaohsiung, "NewTaipei": fetch_new_taipei, "Tainan": fetch_tainan}


# ────────────────────────────────────────────────────────────────────
# Snapshot + Supabase write
# ────────────────────────────────────────────────────────────────────
def save_snapshot(result: dict, ts: datetime) -> Path:
    date_dir = DATA_DIR / "waste_positions" / ts.strftime("%Y/%m/%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    fp = date_dir / f"waste_positions_{ts.strftime('%H%M')}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, default=str)
    return fp


HISTORY_TABLE = "spatial.waste_positions_realtime"
COLUMNS = ["city", "vehicle_no", "route_id", "status", "geometry", "observed_at", "source_url"]


def write_to_supabase(conn, records: list[dict]) -> int:
    """append-only history INSERT，無 current 表、無 UPSERT（conn 由 caller 管理）"""
    rows = []
    for r in records:
        lat, lng = r.get("lat"), r.get("lng")
        vno = (r.get("vehicle_no") or "").strip()
        if lat is None or lng is None or not vno:
            continue
        rows.append((
            r.get("city"),
            vno,
            r.get("route_id"),
            r.get("status") or "unknown",
            f"SRID=4326;POINT({lng} {lat})",
            r.get("observed_at"),
            r.get("source_url"),
        ))
    if not rows:
        return 0

    sql = f"INSERT INTO {HISTORY_TABLE} ({','.join(COLUMNS)}) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def _flush_write(conn, payload: dict) -> None:
    """flush_pending 用：還原 buffer payload → 走同一條寫入路徑"""
    write_to_supabase(conn, payload["records"])


# ────────────────────────────────────────────────────────────────────
def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("waste_positions")
    ts = datetime.now(TAIPEI_TZ)

    qh = parse_quiet_hours(QUIET_HOURS)
    if is_quiet(ts.hour, qh):
        log.info(f"quiet_hours ({QUIET_HOURS}) → skip")
        return 0

    # 1) fetch + snapshot（失敗不影響 buffer 補寫）
    all_records: list[dict] = []
    fetch_ok = False
    try:
        city_stats: dict[str, dict] = {}
        for city in CITIES:
            city_name = CITY_NAMES.get(city, city)
            fetcher = FETCHERS.get(city)
            if not fetcher:
                log.error(f"{city_name}: 未支援的城市代碼")
                city_stats[city] = {"name": city_name, "error": "unsupported city"}
                continue
            try:
                records = fetcher(ts, log)
                all_records.extend(records)
                collecting = sum(1 for r in records if r.get("status") == "collecting")
                parked = sum(1 for r in records if r.get("status") == "parked")
                city_stats[city] = {"name": city_name, "count": len(records),
                                    "collecting": collecting, "parked": parked}
                log.info(f"{city_name}: {len(records)} 筆 (出勤 {collecting} / 待命 {parked})")
            except Exception as exc:
                err = str(exc)[:200]
                city_stats[city] = {"name": city_name, "error": err}
                log.error(f"{city_name}: {err}")

        result = {
            "fetch_time": ts.isoformat(),
            "total": len(all_records),
            "by_city": city_stats,
            "data": all_records,
        }

        snap_path = save_snapshot(result, ts)
        log.info(f"snapshot → {snap_path}")
        fetch_ok = True
    except Exception as exc:
        log.error(f"FAILED: {exc}", exc_info=True)

    # 2) DB：連線 retry → 先補寫積壓 buffer → 寫本輪；寫入失敗存 buffer 不丟資料
    if not all_records and not has_pending(BUFFER_DIR):
        return 0 if fetch_ok else 1

    conn = None
    try:
        conn = connect_with_retry(
            lambda: psycopg2.connect(DB_URL, connect_timeout=15), log=log)
        flush_pending(conn, BUFFER_DIR, _flush_write, log=log)
        if all_records:
            n = write_to_supabase(conn, all_records)
            log.info(f"Supabase 寫入: {n} 筆")
    except Exception as exc:
        log.error(f"DB 寫入失敗: {exc}")
        if all_records:
            saved = save_batch(BUFFER_DIR, "waste_positions",
                               {"ts": ts.isoformat(), "records": all_records}, log=log)
            if saved is None:
                return 1  # buffer 也存不進去才算真的丟資料
    finally:
        if conn is not None:
            conn.close()

    return 0 if fetch_ok else 1


if __name__ == "__main__":
    sys.exit(main())
