#!/usr/bin/env python3
"""
回填 realtime.satellite_passes（中國軍偵衛星通過台灣事件）。

來源：realtime.satellite_tle_history（每顆衛星每個 epoch 的 TLE）
做法：對每個 (date, sat) 找最近 epoch 的 TLE → 用 SGP4 採樣 30s →
      偵測進出台灣 bbox (lng 117.5-122.5, lat 21.5-26.5) →
      連續區間記為一筆 pass，track 為 LineString。
之後一次 UPDATE counties[] 用 spatial.county_boundaries 對 track 做 ST_Intersects。
最後 call realtime.rebuild_satellite_passes_daily() 重算彙總。

用法：
    python3 scripts/backfill_satellite_passes.py                       # 回填全部 78 天
    python3 scripts/backfill_satellite_passes.py --start 2026-06-01    # 指定起點
    python3 scripts/backfill_satellite_passes.py --start 2026-06-13 --end 2026-06-13  # 單日
    python3 scripts/backfill_satellite_passes.py --groups YAOGAN,JILIN # 只跑特定群

執行時間預估：617 顆 × 30s 採樣 × 1 天 ≈ 1.8M SGP4 / day → ~20s/day → 78 天 ~25 分鐘。
"""
import argparse
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from sgp4.api import Satrec, jday

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SUPABASE_DB_URL  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# 台灣 + 周邊島 bbox（含東沙不含太平島；放寬讓 pass 起訖完整）
BBOX_LNG = (117.5, 122.5)
BBOX_LAT = (21.5, 26.5)
SAMPLE_SEC = 30  # SGP4 採樣間隔
TPE_OFFSET = timedelta(hours=8)

# 中國群辨識（與既有 CONSTELLATION_PATTERNS 相容，但獨立一份避免耦合）
def cn_group_of(name: str, constellation: str) -> str | None:
    n = (name or '').upper()
    if n.startswith('YAOGAN'):       return 'YAOGAN'
    if n.startswith('JILIN'):        return 'JILIN'
    if n.startswith('GAOFEN'):       return 'GAOFEN'
    if n.startswith('TJS'):          return 'TJS'
    if n.startswith('SHIYAN'):       return 'SHIYAN'
    if n.startswith('LUDI TANCE'):   return 'LUDI_TANCE'
    if n.startswith('CHUTIAN'):      return 'CHUTIAN'
    if n.startswith('QIANFAN'):      return 'QIANFAN'
    if constellation == 'BeiDou':    return 'BEIDOU'
    return None


def sgp4_latlng(sat: Satrec, ts: datetime) -> tuple[float, float, float] | None:
    """回傳 (lat, lng, alt_km)；SGP4 失敗時回 None"""
    jd, fr = jday(ts.year, ts.month, ts.day,
                  ts.hour, ts.minute, ts.second + ts.microsecond / 1e6)
    e, r, _v = sat.sgp4(jd, fr)
    if e != 0:
        return None
    x, y, z = r
    d = jd - 2451545.0 + fr
    gmst = math.radians(math.fmod(280.46061837 + 360.98564736629 * d, 360.0))
    xe = x * math.cos(gmst) + y * math.sin(gmst)
    ye = -x * math.sin(gmst) + y * math.cos(gmst)
    lng = math.degrees(math.atan2(ye, xe))
    lat = math.degrees(math.atan2(z, math.sqrt(xe * xe + ye * ye)))
    alt = math.sqrt(x * x + y * y + z * z) - 6371.0
    return lat, lng, alt


def in_bbox(lat: float, lng: float) -> bool:
    return BBOX_LAT[0] <= lat <= BBOX_LAT[1] and BBOX_LNG[0] <= lng <= BBOX_LNG[1]


def detect_passes_for_day(
    sat: Satrec, day_start_utc: datetime, sat_meta: dict
) -> list[dict]:
    """掃一整天，回傳 pass list。每筆含 start/end UTC、duration、min_alt、points(LineString WKT)"""
    passes = []
    current = []
    t = day_start_utc
    day_end = day_start_utc + timedelta(days=1)

    while t < day_end:
        r = sgp4_latlng(sat, t)
        if r is None:
            # 進度中斷視為退出 bbox
            if current:
                passes.append(_finalize_pass(current, sat_meta))
                current = []
        else:
            lat, lng, alt = r
            if in_bbox(lat, lng):
                current.append((t, lat, lng, alt))
            else:
                if current:
                    passes.append(_finalize_pass(current, sat_meta))
                    current = []
        t += timedelta(seconds=SAMPLE_SEC)

    if current:
        passes.append(_finalize_pass(current, sat_meta))
    return passes


def _finalize_pass(points: list[tuple], meta: dict) -> dict:
    start = points[0][0]
    end = points[-1][0]
    pass_date_tw = (start + TPE_OFFSET).date()
    pass_hour_tw = (start + TPE_OFFSET).hour
    min_alt = min(p[3] for p in points)
    # LineString 至少 2 點；單點 pass 補一個微步
    if len(points) == 1:
        points.append(points[0])
    # WKT；經度先緯度後
    wkt = 'LINESTRING(' + ','.join(f'{p[2]:.5f} {p[1]:.5f}' for p in points) + ')'
    return {
        'norad_id':       meta['norad_id'],
        'name':           meta['name'],
        'cn_group':       meta['cn_group'],
        'constellation':  meta['constellation'],
        'orbit_type':     meta['orbit_type'],
        'pass_start_utc': start,
        'pass_end_utc':   end,
        'pass_date_tw':   pass_date_tw,
        'pass_hour_tw':   pass_hour_tw,
        'duration_sec':   int((end - start).total_seconds()),
        'min_alt_km':     round(min_alt, 1),
        'track_wkt':      wkt,
    }


def fetch_cn_tles_for_day(conn, day_utc: datetime, groups: list[str] | None) -> list[dict]:
    """
    對 day_utc 00:00 之前最近一筆 TLE 拉出（每顆衛星一筆），
    限制 epoch 距離 ≤ 7 天，避免太舊的 TLE。
    """
    group_filter_sql = ""
    params = [day_utc]
    if groups:
        group_filter_sql = "AND cn_group = ANY(%s)"
        params.append(groups)

    sql = f"""
    WITH cn AS (
      SELECT norad_id, name, constellation, orbit_type, tle_line1, tle_line2, tle_epoch,
        CASE
          WHEN upper(name) LIKE 'YAOGAN%%'      THEN 'YAOGAN'
          WHEN upper(name) LIKE 'JILIN%%'       THEN 'JILIN'
          WHEN upper(name) LIKE 'GAOFEN%%'      THEN 'GAOFEN'
          WHEN upper(name) LIKE 'TJS%%'         THEN 'TJS'
          WHEN upper(name) LIKE 'SHIYAN%%'      THEN 'SHIYAN'
          WHEN upper(name) LIKE 'LUDI TANCE%%'  THEN 'LUDI_TANCE'
          WHEN upper(name) LIKE 'CHUTIAN%%'     THEN 'CHUTIAN'
          WHEN upper(name) LIKE 'QIANFAN%%'     THEN 'QIANFAN'
          WHEN constellation = 'BeiDou'         THEN 'BEIDOU'
        END AS cn_group,
        fetched_at,
        ROW_NUMBER() OVER (
          PARTITION BY norad_id
          ORDER BY fetched_at DESC
        ) AS rn
      FROM realtime.satellite_tle_history
      WHERE fetched_at <= %s
        AND fetched_at >= %s - INTERVAL '7 days'
        AND tle_line1 IS NOT NULL AND tle_line2 IS NOT NULL
        AND (
          upper(name) LIKE 'YAOGAN%%' OR upper(name) LIKE 'JILIN%%' OR
          upper(name) LIKE 'GAOFEN%%' OR upper(name) LIKE 'TJS%%' OR
          upper(name) LIKE 'SHIYAN%%' OR upper(name) LIKE 'LUDI TANCE%%' OR
          upper(name) LIKE 'CHUTIAN%%' OR upper(name) LIKE 'QIANFAN%%' OR
          constellation = 'BeiDou'
        )
    )
    SELECT norad_id, name, constellation, orbit_type, tle_line1, tle_line2, cn_group
      FROM cn WHERE rn = 1 {group_filter_sql};
    """
    # day_utc 二次用到（INTERVAL 7 days 那段）
    full_params = [day_utc, day_utc] + ([groups] if groups else [])
    with conn.cursor() as cur:
        cur.execute(sql, full_params)
        rows = cur.fetchall()
    return [
        {
            'norad_id': r[0], 'name': r[1], 'constellation': r[2],
            'orbit_type': r[3], 'tle_line1': r[4], 'tle_line2': r[5],
            'cn_group': r[6],
        }
        for r in rows
    ]


def insert_passes(conn, passes: list[dict]) -> int:
    if not passes:
        return 0
    cols = ['norad_id', 'name', 'cn_group', 'constellation', 'orbit_type',
            'pass_start_utc', 'pass_end_utc', 'pass_date_tw', 'pass_hour_tw',
            'duration_sec', 'min_alt_km', 'track']
    template = ('(%(norad_id)s,%(name)s,%(cn_group)s,%(constellation)s,%(orbit_type)s,'
                '%(pass_start_utc)s,%(pass_end_utc)s,%(pass_date_tw)s,%(pass_hour_tw)s,'
                '%(duration_sec)s,%(min_alt_km)s,ST_GeomFromText(%(track_wkt)s, 4326))')
    sql = (f"INSERT INTO realtime.satellite_passes ({','.join(cols)}) VALUES %s "
           f"ON CONFLICT (norad_id, pass_start_utc) DO NOTHING")
    with conn.cursor() as cur:
        execute_values(cur, sql, passes, template=template, page_size=1000)
        n = cur.rowcount
    conn.commit()
    return n


def update_counties_for_range(conn, start: datetime, end: datetime) -> int:
    """對指定區間還沒填 counties 的 pass 做空間 join"""
    sql = """
    UPDATE realtime.satellite_passes p
       SET counties = COALESCE(array_remove(c.arr, NULL), '{}')
      FROM (
        SELECT p2.pass_id, array_agg(DISTINCT cb.county) AS arr
          FROM realtime.satellite_passes p2
          LEFT JOIN spatial.county_boundaries cb
            ON ST_Intersects(p2.track, cb.geom)
         WHERE p2.pass_date_tw BETWEEN %s AND %s
         GROUP BY p2.pass_id
      ) c
     WHERE p.pass_id = c.pass_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start.date(), end.date()))
        n = cur.rowcount
    conn.commit()
    return n


def rebuild_daily(conn, start, end) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT realtime.rebuild_satellite_passes_daily(%s::date, %s::date)",
                    (start.date() if isinstance(start, datetime) else start,
                     end.date() if isinstance(end, datetime) else end))
        n = cur.fetchone()[0]
    conn.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', help='起始日 YYYY-MM-DD（UTC），預設 = tle_history 最早日')
    ap.add_argument('--end', help='結束日 YYYY-MM-DD（UTC，含），預設 = 昨天')
    ap.add_argument('--groups', help='逗號分隔，限定 cn_group，例 YAOGAN,JILIN')
    ap.add_argument('--skip-daily', action='store_true', help='只 insert passes 不 rebuild daily')
    args = ap.parse_args()

    conn = psycopg2.connect(SUPABASE_DB_URL)

    # 決定範圍
    if args.start:
        start = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(fetched_at)::date FROM realtime.satellite_tle_history")
            start = datetime.combine(cur.fetchone()[0], datetime.min.time()).replace(tzinfo=timezone.utc)
    if args.end:
        end = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        end = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)

    groups = [g.strip().upper() for g in args.groups.split(',')] if args.groups else None

    logger.info(f"範圍 {start.date()} → {end.date()}  groups={groups or 'ALL_CN'}")

    total_passes = 0
    day = start
    while day <= end:
        sats = fetch_cn_tles_for_day(conn, day, groups)
        if not sats:
            logger.warning(f"{day.date()} 無 TLE 可用，跳過")
            day += timedelta(days=1)
            continue

        day_passes = []
        bad_sgp4 = 0
        for s in sats:
            try:
                sr = Satrec.twoline2rv(s['tle_line1'], s['tle_line2'])
            except Exception:
                bad_sgp4 += 1
                continue
            ps = detect_passes_for_day(sr, day, s)
            day_passes.extend(ps)

        inserted = insert_passes(conn, day_passes)
        total_passes += inserted
        logger.info(f"{day.date()} sats={len(sats)} passes={len(day_passes)} "
                    f"inserted={inserted} sgp4_err={bad_sgp4}")
        day += timedelta(days=1)

    # 一次性 update counties
    logger.info("更新 counties[] (ST_Intersects against spatial.county_boundaries)...")
    n_c = update_counties_for_range(conn, start, end)
    logger.info(f"  counties 更新 {n_c} 列")

    if not args.skip_daily:
        logger.info("重建 satellite_passes_daily...")
        n_d = rebuild_daily(conn, start, end)
        logger.info(f"  daily rows = {n_d}")

    logger.info(f"DONE: 新增 {total_passes} pass。")


if __name__ == '__main__':
    main()
