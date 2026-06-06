#!/usr/bin/env python3
"""ship_ais 單檔 collector — HiCloud VM 專用版。

跟主 repo (data-collectors) 完全一致的儲存規格：
  ┌── Supabase ──────────────────────────────────
  │  history : realtime.ship_positions  (INSERT)
  │  current : realtime.ship_current    (UPSERT by mmsi)
  │  columns : mmsi, ship_name, ship_type, lat, lng,
  │            speed, heading, collected_at, geom (SRID 4326)
  └─────────────────────────────────────────────
  ┌── 本地 JSON snapshot（供每日 archive 上 S3）──
  │  path : <DATA_DIR>/ship_ais/YYYY/MM/DD/ship_ais_HHMM.json
  │  content : {fetch_time, ship_count, data: [...processed_ships]}
  └─────────────────────────────────────────────

來源：航港局「臺灣海域船舶即時資訊系統」https://mpbais.motcmpb.gov.tw/
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# ────────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent
load_dotenv(APP_DIR / ".env")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    sys.exit("FATAL: SUPABASE_DB_URL 未設定（請編輯 .env）")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/ship-ais/data"))
AIS_URL = "https://mpbais.motcmpb.gov.tw/aismpb/tools/geojsonais.ashx"
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

TAIPEI_TZ = timezone(timedelta(hours=8))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://mpbais.motcmpb.gov.tw/aismpb/",
}

# 對照表（與主 repo collectors/ship_ais.py 一致）
VESSEL_TYPES = {
    0: "未指定", 20: "地效翼船", 30: "漁船", 31: "拖船", 32: "拖船(大型)",
    33: "疏浚船", 34: "潛水作業船", 35: "軍艦", 36: "帆船", 37: "遊艇",
    40: "高速船", 50: "引水船", 51: "搜救船", 52: "拖船", 53: "港口小艇",
    54: "防污船", 55: "執法船", 90: "其他",
}


def vessel_type_name(code: int) -> str:
    if 60 <= code <= 69:
        return "客輪"
    if 70 <= code <= 79:
        return "貨船"
    if 80 <= code <= 89:
        return "油輪"
    return VESSEL_TYPES.get(code, f"其他({code})")


# ────────────────────────────────────────────────────────────────────
# Fetch + process
# ────────────────────────────────────────────────────────────────────
def fetch_and_process(ts: datetime) -> dict:
    """抓 GeoJSON → 正規化成 processed_ships（欄位與主 repo collect() 對齊）"""
    resp = requests.get(AIS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    geojson = resp.json()
    features = geojson.get("features", []) or []

    processed_ships = []
    fetch_iso = ts.isoformat()
    for feature in features:
        props = feature.get("properties", {})
        coords = (feature.get("geometry") or {}).get("coordinates", [None, None])
        lng, lat = coords[0], coords[1]
        vtype = props.get("Ship_and_Cargo_Type", 0)
        processed_ships.append({
            "mmsi": props.get("MMSI"),
            "imo": props.get("IMO_Number"),
            "ship_name": props.get("ShipName"),
            "call_sign": props.get("Call_Sign"),
            "vessel_type": vtype,
            "vessel_type_name": vessel_type_name(vtype),
            "nav_status": props.get("Navigational_Status"),
            "lon": lng,
            "lat": lat,
            "sog": props.get("SOG"),
            "cog": props.get("COG"),
            "heading": props.get("True_Heading"),
            "rot": props.get("Rate_of_Turn"),
            "length": props.get("Overall_Length"),
            "width": props.get("Breadth"),
            "draught": props.get("Draught"),
            "destination": props.get("Destination"),
            "eta": props.get("ETA"),
            "record_time": props.get("Record_Time"),
            "_fetch_time": fetch_iso,
        })
    return {
        "fetch_time": fetch_iso,
        "ship_count": len(processed_ships),
        "data": processed_ships,
    }


# ────────────────────────────────────────────────────────────────────
# Local snapshot (給 daily archive 用，路徑/格式與主 repo LocalStorage 對齊)
# ────────────────────────────────────────────────────────────────────
def save_snapshot(result: dict, ts: datetime) -> Path:
    date_dir = DATA_DIR / "ship_ais" / ts.strftime("%Y/%m/%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    filepath = date_dir / f"ship_ais_{ts.strftime('%H%M')}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, default=str)
    return filepath


# ────────────────────────────────────────────────────────────────────
# Supabase write (規格與主 repo TABLE_MAP['ship_ais'] + _write_to_db 一致)
# ────────────────────────────────────────────────────────────────────
HISTORY_TABLE = "realtime.ship_positions"
CURRENT_TABLE = "realtime.ship_current"
CURRENT_KEY = "mmsi"
COLUMNS = ["mmsi", "ship_name", "ship_type", "lat", "lng", "speed", "heading", "collected_at", "geom"]


def write_to_supabase(processed: list[dict], ts: datetime) -> tuple[int, int]:
    """history INSERT + current UPSERT，同 transaction"""
    iso_ts = ts.isoformat()
    history_rows = []
    seen_current: dict[str, tuple] = {}
    for r in processed:
        mmsi = str(r.get("mmsi")) if r.get("mmsi") else None
        if not mmsi:
            continue
        lat, lng = r.get("lat"), r.get("lon")
        row = (
            mmsi,
            (r.get("ship_name") or "").strip(),
            r.get("vessel_type_name", ""),
            lat,
            lng,
            r.get("sog"),
            r.get("heading"),
            iso_ts,
            f"SRID=4326;POINT({lng} {lat})" if (lat and lng) else None,
        )
        history_rows.append(row)
        seen_current[mmsi] = row  # 去重：同 mmsi 保留最後一筆
    current_rows = list(seen_current.values())

    if not history_rows:
        return 0, 0

    col_names = ",".join(COLUMNS)
    sql_history = f"INSERT INTO {HISTORY_TABLE} ({col_names}) VALUES %s"
    update_cols = [c for c in COLUMNS if c != CURRENT_KEY]
    update_set = ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
    sql_current = (
        f"INSERT INTO {CURRENT_TABLE} ({col_names}) VALUES %s "
        f"ON CONFLICT ({CURRENT_KEY}) DO UPDATE SET {update_set}"
    )

    with psycopg2.connect(DB_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql_history, history_rows, page_size=1000)
            if current_rows:
                execute_values(cur, sql_current, current_rows, page_size=1000)
        conn.commit()

    return len(history_rows), len(current_rows)


# ────────────────────────────────────────────────────────────────────
def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("ship_ais")
    ts = datetime.now(TAIPEI_TZ)

    try:
        result = fetch_and_process(ts)
        log.info(f"抓到 {result['ship_count']} 艘船")

        snap_path = save_snapshot(result, ts)
        log.info(f"snapshot → {snap_path}")

        h, c = write_to_supabase(result["data"], ts)
        log.info(f"Supabase 寫入: history={h}, current={c}")
        return 0
    except Exception as exc:
        log.error(f"FAILED: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
