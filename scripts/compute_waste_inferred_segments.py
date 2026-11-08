"""
compute_waste_inferred_segments.py
===================================
對 A 類 hwms flat schedule routes（COUNT(DISTINCT arrival_sec)=1）跑 Valhalla /route
取 stop-to-stop 沿馬路距離，寫進 spatial.waste_route_inferred_segments。

對應 BL-22 路網距離升級。
（2026-11：Zeabur OSRM 停運不搬，改用自架 Valhalla https://valhalla.itsmigu.com；
  需環境變數 VALHALLA_TOKEN，VALHALLA_URL 可覆寫。輸出：distance_m 公尺、duration_s 秒，與舊 OSRM 版同義。）
RPC migration 085 LEFT JOIN 此表使用。

流程：
1. 拿 A 類 routes 的 stops（按 city, route_id, seq 排序，dedup 同 coord）
2. 對每 route 內相鄰 (seq i-1, seq i) 兩 stop 呼叫 Valhalla
   POST {VALHALLA_URL}/route  {"locations":[{lat,lon},{lat,lon}],"costing":"auto"}
3. 解析 trip.summary.length（km→m）+ trip.summary.time（秒）
4. UPSERT 進 spatial.waste_route_inferred_segments

用法：
  python3 compute_waste_inferred_segments.py             # dry-run
  python3 compute_waste_inferred_segments.py --commit    # 實際寫入

速率：Valhalla 跑在 4GB 小主機，預設 sleep 50ms 避免打滿 CPU
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests
import psycopg2

VALHALLA_URL = os.getenv("VALHALLA_URL", "https://valhalla.itsmigu.com").strip().rstrip("/")
VALHALLA_TOKEN = os.getenv("VALHALLA_TOKEN", "").strip()  # 從環境變數讀取，不寫死在程式碼
if not VALHALLA_TOKEN:
    sys.exit("VALHALLA_TOKEN 未設定：請先 export VALHALLA_TOKEN=...（Bearer token，見 SETUP-INVENTORY.md）")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def _load_db_url() -> str:
    env_path = PROJECT_ROOT.parent / "gis-platform/.env"
    with open(env_path) as f:
        for line in f:
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError(f"DATABASE_URL not found in {env_path}")


def fetch_flat_routes(conn, skip_done: bool = True) -> list[tuple[str, str]]:
    """A 類 routes：全 route 同一個 arrival_time（hwms flat schedule）。
    skip_done=True 時排除已有 segments 紀錄的 route（resume 用）"""
    cur = conn.cursor()
    sql = """
        SELECT s.city, s.route_id
        FROM spatial.waste_collection_stops s
        WHERE s.arrival_time ~ '^[0-9]{1,2}:[0-9]{2}$'
        GROUP BY s.city, s.route_id
        HAVING COUNT(DISTINCT s.arrival_time) = 1 AND COUNT(*) > 1
    """
    if skip_done:
        sql += """
        EXCEPT
        SELECT city, route_id FROM spatial.waste_route_inferred_segments
        """
    sql += " ORDER BY 1, 2"
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_route_stops(conn, city: str, route_id: str) -> list[tuple[int, float, float]]:
    """取 route 的 stops 按 seq 排序，dedup 同 (seq, coord) 多 vehicle_type"""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (seq, ST_X(geometry), ST_Y(geometry))
            seq, ST_X(geometry), ST_Y(geometry)
        FROM spatial.waste_collection_stops
        WHERE city = %s AND route_id = %s AND seq IS NOT NULL
        ORDER BY seq, ST_X(geometry), ST_Y(geometry)
    """, (city, route_id))
    rows = cur.fetchall()
    cur.close()
    return [(seq, float(lng), float(lat)) for seq, lng, lat in rows]


def valhalla_route(lng1: float, lat1: float, lng2: float, lat2: float, session: requests.Session, retries: int = 3) -> tuple[float, int] | None:
    """呼叫 Valhalla /route（costing=auto）取 distance(m) + duration(s)。失敗回 None。網路斷線 retry。"""
    url = f"{VALHALLA_URL}/route"
    payload = {
        "locations": [{"lat": lat1, "lon": lng1}, {"lat": lat2, "lon": lng2}],
        "costing": "auto",
        "directions_options": {"units": "kilometers"},
    }
    headers = {"Authorization": f"Bearer {VALHALLA_TOKEN}"}
    for attempt in range(retries):
        try:
            resp = session.post(url, json=payload, headers=headers, timeout=15)
            if resp.status_code == 400:
                return None  # 找不到路徑／附近無道路（Valhalla 以 400 回報），不 retry
            resp.raise_for_status()
            summary = resp.json()["trip"]["summary"]
            return float(summary["length"]) * 1000.0, int(summary["time"])  # length 為 km
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
            print(f"[ERR] Valhalla after {retries} retries: {e}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"[ERR] Valhalla {lng1:.5f},{lat1:.5f}→{lng2:.5f},{lat2:.5f}: {e}", file=sys.stderr)
            return None
    return None


def upsert_segment(conn, city: str, route_id: str, seq: int, distance_m: float, duration_s: int):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO spatial.waste_route_inferred_segments
            (city, route_id, seq, distance_m, osrm_duration_s, source, computed_at)
        VALUES (%s, %s, %s, %s, %s, 'valhalla', NOW())
        ON CONFLICT (city, route_id, seq) DO UPDATE SET
            distance_m      = EXCLUDED.distance_m,
            osrm_duration_s = EXCLUDED.osrm_duration_s,
            source          = EXCLUDED.source,
            computed_at     = EXCLUDED.computed_at
    """, (city, route_id, seq, distance_m, duration_s))
    cur.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--commit", action="store_true", help="實際寫入 DB（預設 dry-run）")
    ap.add_argument("--limit-routes", type=int, default=None, help="限制處理前 N 個 route")
    ap.add_argument("--sleep-ms", type=int, default=50, help="每 call 之間 sleep（ms，避免打死 Valhalla）")
    args = ap.parse_args()

    conn = psycopg2.connect(_load_db_url())
    if args.commit:
        conn.autocommit = False

    session = requests.Session()

    print("[1] 撈 A 類 flat schedule routes...")
    routes = fetch_flat_routes(conn)
    print(f"[1] 共 {len(routes):,} routes")
    if args.limit_routes:
        routes = routes[:args.limit_routes]
        print(f"[1] 限制處理前 {len(routes)} routes")

    t_start = time.time()
    total_calls = 0
    total_failed = 0
    seq_1_inserted = 0  # 起點 (seq=1, distance=0)

    for r_idx, (city, route_id) in enumerate(routes, start=1):
        stops = fetch_route_stops(conn, city, route_id)
        if len(stops) < 2:
            continue

        # seq=1 起點插 distance=0
        first_seq = stops[0][0]
        if args.commit:
            upsert_segment(conn, city, route_id, first_seq, 0.0, 0)
        seq_1_inserted += 1

        # 對相鄰 stops 呼叫 Valhalla
        for i in range(1, len(stops)):
            prev_seq, prev_lng, prev_lat = stops[i - 1]
            curr_seq, curr_lng, curr_lat = stops[i]

            result = valhalla_route(prev_lng, prev_lat, curr_lng, curr_lat, session)
            total_calls += 1
            if result is None:
                total_failed += 1
                continue

            distance_m, duration_s = result
            if args.commit:
                upsert_segment(conn, city, route_id, curr_seq, distance_m, duration_s)

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000)

        if args.commit and r_idx % 20 == 0:
            conn.commit()
            elapsed = time.time() - t_start
            print(f"[2] {r_idx}/{len(routes)} routes done · {total_calls} Valhalla calls · "
                  f"{total_failed} fail · {elapsed:.1f}s elapsed · "
                  f"ETA {(elapsed / r_idx) * (len(routes) - r_idx):.0f}s")

    if args.commit:
        conn.commit()

    elapsed = time.time() - t_start
    print(f"\n[總計]")
    print(f"  routes processed:  {len(routes):,}")
    print(f"  Valhalla calls total:  {total_calls:,}")
    print(f"  Valhalla calls failed: {total_failed:,}")
    print(f"  seq=1 (起點 0):    {seq_1_inserted:,}")
    print(f"  elapsed:           {elapsed:.1f}s")
    if args.commit:
        print(f"  [OK] 寫入 spatial.waste_route_inferred_segments")
    else:
        print(f"  [dry-run] 沒寫入 DB")

    conn.close()


if __name__ == "__main__":
    main()
