"""
垃圾車 OSRM map-matching collector

Input:
    spatial.waste_positions_realtime

Output:
    realtime.waste_trails_matched_daily

此 collector 不抓外部政府 API；它把已收進 DB 的 GPS trail 批次送 OSRM
`/match`，產出沿 OSM 路網的 LineString 與每個 GPS 點在該 LineString 上的
progress timeline。前端只透過 public.get_waste_trails_matched_day 讀取。
"""

from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional

import psycopg2
from psycopg2.extras import execute_values
import requests

import config
from .base import BaseCollector, TAIPEI_TZ


HTTP_TIMEOUT = (5, 30)
STATUS_CHARS = {
    'collecting': 'c',
    'returning': 'r',
    'parked': 'p',
    'offline': 'o',
    'unknown': 'u',
}


@dataclass
class TrailPoint:
    observed_at: str
    epoch: int
    lat: float
    lng: float
    status: str


@dataclass
class Trip:
    city: str
    vehicle_no: str
    route_id: Optional[str]
    trip_id: int
    started_at: str
    ended_at: str
    points: list[TrailPoint]


@dataclass
class MatchedSegment:
    city: str
    vehicle_no: str
    route_id: Optional[str]
    trip_id: int
    segment_seq: int
    started_at: str
    ended_at: str
    polyline: list[tuple[float, float]]
    timeline: str
    point_count: int
    confidence: float


def _parse_points(raw: Any) -> list[TrailPoint]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    points: list[TrailPoint] = []
    for p in raw or []:
        points.append(TrailPoint(
            observed_at=str(p['observed_at']),
            epoch=int(p['epoch']),
            lat=float(p['lat']),
            lng=float(p['lng']),
            status=str(p.get('status') or 'unknown'),
        ))
    return points


def _chunks_with_overlap(points: list[TrailPoint], max_points: int) -> Iterable[tuple[int, list[TrailPoint]]]:
    """Yield chunks with a one-point overlap so OSRM segments stay visually continuous."""
    if max_points < 2:
        max_points = 2
    if len(points) <= max_points:
        yield 0, points
        return

    start = 0
    seq = 0
    step = max_points - 1
    while start < len(points) - 1:
        chunk = points[start:start + max_points]
        if len(chunk) >= 2:
            yield seq, chunk
        seq += 1
        start += step


def _distance2_point_to_segment(px: float, py: float, ax: float, ay: float, bx: float, by: float) -> tuple[float, float]:
    dx = bx - ax
    dy = by - ay
    denom = dx * dx + dy * dy
    if denom == 0:
        return (px - ax) ** 2 + (py - ay) ** 2, 0.0
    t = ((px - ax) * dx + (py - ay) * dy) / denom
    t = max(0.0, min(1.0, t))
    x = ax + dx * t
    y = ay + dy * t
    return (px - x) ** 2 + (py - y) ** 2, t


def _polyline_lengths(coords: list[tuple[float, float]]) -> tuple[list[float], float]:
    cumulative = [0.0]
    total = 0.0
    for a, b in zip(coords, coords[1:]):
        # Degree-space length is enough for normalized progress over small urban segments.
        seg = math.hypot(b[0] - a[0], b[1] - a[1])
        total += seg
        cumulative.append(total)
    return cumulative, total


def _nearest_progress(coords: list[tuple[float, float]], lng: float, lat: float) -> float:
    if len(coords) < 2:
        return 0.0
    cumulative, total = _polyline_lengths(coords)
    if total <= 0:
        return 0.0

    best_dist = float('inf')
    best_at = 0.0
    for idx, (a, b) in enumerate(zip(coords, coords[1:])):
        dist2, local_t = _distance2_point_to_segment(lng, lat, a[0], a[1], b[0], b[1])
        if dist2 < best_dist:
            seg_len = cumulative[idx + 1] - cumulative[idx]
            best_dist = dist2
            best_at = cumulative[idx] + seg_len * local_t
    return max(0.0, min(1.0, best_at / total))


class WasteMatchCollector(BaseCollector):
    """OSRM map-match recent waste GPS trips into daily progress trails."""

    name = "waste_match"
    interval_minutes = config.WASTE_MATCH_INTERVAL
    COLLECT_TIMEOUT = 300

    def __init__(
        self,
        target_days: Optional[list[date]] = None,
        cities: Optional[list[str]] = None,
        osrm_url: Optional[str] = None,
    ):
        super().__init__()
        self.target_days = target_days
        self.cities = [c for c in (cities or config.WASTE_MATCH_CITIES) if c]
        self.osrm_url = (osrm_url or config.OSRM_URL).rstrip('/')
        self.session = requests.Session()
        if config.OSRM_TOKEN:
            self.session.headers.update({'Authorization': f'Bearer {config.OSRM_TOKEN}'})

    def _default_target_days(self) -> list[date]:
        today = datetime.now(TAIPEI_TZ).date()
        return [today - timedelta(days=i) for i in range(max(1, config.WASTE_MATCH_TARGET_DAYS))]

    def _connect(self):
        if not config.SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL is required for waste_match")
        conn = psycopg2.connect(config.SUPABASE_DB_URL)
        conn.autocommit = False
        return conn

    def _find_unmatched_trips(self, conn, target_day: date) -> list[Trip]:
        sql = """
        WITH
        bounds AS (
            SELECT
                (%(target_day)s::date::timestamp AT TIME ZONE 'Asia/Taipei') AS start_at,
                ((%(target_day)s::date + 1)::timestamp AT TIME ZONE 'Asia/Taipei') AS end_at
        ),
        raw AS (
            -- DISTINCT ON 去掉 (city, vehicle_no, observed_at) 重複 row
            -- 台南 / 新北 polling 每 ~2 min 重疊抓 endpoint 最近 N 分鐘 GPS
            -- → 同 timestamp 同座標被 append 2-4 次（無 UNIQUE constraint）
            -- → OSRM /match HMM 收到「相鄰兩點時間差為 0」直接 400 Bad Request
            -- 高雄走 SSE 推送無 dup，DISTINCT ON 對它無感
            SELECT DISTINCT ON (w.city, w.vehicle_no, w.observed_at)
                w.vehicle_no,
                w.city,
                w.route_id,
                w.status,
                w.observed_at,
                ST_Y(w.geometry) AS lat,
                ST_X(w.geometry) AS lng,
                w.geometry
            FROM spatial.waste_positions_realtime w
            CROSS JOIN bounds b
            WHERE w.city = ANY(%(cities)s)
              AND w.observed_at >= b.start_at
              AND w.observed_at < b.end_at
            ORDER BY w.city, w.vehicle_no, w.observed_at, w.ingested_at
        ),
        snapped AS (
            SELECT
                r.vehicle_no, r.city, r.route_id, r.status, r.observed_at,
                COALESCE(s.lat, r.lat) AS lat,
                COALESCE(s.lng, r.lng) AS lng
            FROM raw r
            LEFT JOIN LATERAL (
                SELECT
                    ST_Y(stop.geometry) AS lat,
                    ST_X(stop.geometry) AS lng,
                    stop.id
                FROM spatial.waste_collection_stops stop
                WHERE
                    r.status = 'collecting'
                    AND stop.city = r.city
                    AND stop.geometry && ST_Expand(r.geometry, 0.00072)
                ORDER BY r.geometry <-> stop.geometry
                LIMIT 1
            ) s ON true
        ),
        with_speed AS (
            SELECT
                *,
                LAG(observed_at) OVER (PARTITION BY city, vehicle_no ORDER BY observed_at) AS prev_t,
                LAG(lat)         OVER (PARTITION BY city, vehicle_no ORDER BY observed_at) AS prev_lat,
                LAG(lng)         OVER (PARTITION BY city, vehicle_no ORDER BY observed_at) AS prev_lng
            FROM snapped
        ),
        speed_filtered AS (
            SELECT
                vehicle_no, city, route_id, status, observed_at, lat, lng,
                prev_t,
                CASE
                    WHEN prev_t IS NULL THEN 0
                    ELSE (
                        6371 * 2 * ASIN(SQRT(
                            POWER(SIN(RADIANS(lat - prev_lat) / 2), 2)
                            + COS(RADIANS(prev_lat)) * COS(RADIANS(lat))
                            * POWER(SIN(RADIANS(lng - prev_lng) / 2), 2)
                        ))
                    ) / NULLIF(EXTRACT(EPOCH FROM (observed_at - prev_t)) / 3600.0, 0)
                END AS speed_kmh
            FROM with_speed
        ),
        cleaned AS (
            SELECT vehicle_no, city, route_id, status, observed_at, lat, lng
            FROM speed_filtered
            WHERE prev_t IS NULL OR speed_kmh IS NULL OR speed_kmh <= 60
        ),
        with_gap AS (
            SELECT
                *,
                LAG(observed_at) OVER (PARTITION BY city, vehicle_no ORDER BY observed_at) AS prev_t2
            FROM cleaned
        ),
        with_trip AS (
            SELECT
                vehicle_no, city, route_id, status, observed_at, lat, lng,
                -- trip-gap 15 min（900s）：高雄 5-10min gap 僅 1pct、台南 8pct
                -- 太緊（10 min）會把台南「車短暫停車 6-9 min」誤切成新 trip
                -- 切碎後 trip 只剩 2 點，OSRM 無法 segment → 0pct success
                SUM(CASE
                    WHEN prev_t2 IS NULL THEN 0
                    WHEN EXTRACT(EPOCH FROM (observed_at - prev_t2)) > 900 THEN 1
                    ELSE 0
                END) OVER (PARTITION BY city, vehicle_no ORDER BY observed_at) AS trip_id
            FROM with_gap
        ),
        grouped AS (
            SELECT
                city,
                vehicle_no,
                MAX(route_id) AS route_id,
                trip_id::INT AS trip_id,
                MIN(observed_at) AS started_at,
                MAX(observed_at) AS ended_at,
                COUNT(*)::INT AS point_count,
                jsonb_agg(
                    jsonb_build_object(
                        'observed_at', observed_at,
                        'epoch', EXTRACT(EPOCH FROM observed_at)::BIGINT,
                        'lat', ROUND(lat::numeric, 6),
                        'lng', ROUND(lng::numeric, 6),
                        'status', COALESCE(status, 'unknown')
                    )
                    ORDER BY observed_at
                ) AS points
            FROM with_trip
            GROUP BY city, vehicle_no, trip_id
            HAVING COUNT(*) >= 2
        )
        SELECT
            g.city, g.vehicle_no, g.route_id, g.trip_id,
            g.started_at, g.ended_at, g.points
        FROM grouped g
        WHERE NOT EXISTS (
            SELECT 1
            FROM realtime.waste_trails_matched_daily m
            WHERE m.day = %(target_day)s::date
              AND m.city = g.city
              AND m.vehicle_no = g.vehicle_no
              AND m.trip_id = g.trip_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM realtime.waste_match_attempts a
            WHERE a.day = %(target_day)s::date
              AND a.city = g.city
              AND a.vehicle_no = g.vehicle_no
              AND a.trip_id = g.trip_id
        )
        ORDER BY g.started_at
        LIMIT %(max_trips)s;
        """
        with conn.cursor() as cur:
            cur.execute(sql, {
                'target_day': target_day.isoformat(),
                'cities': self.cities,
                'max_trips': config.WASTE_MATCH_MAX_TRIPS,
            })
            rows = cur.fetchall()

        trips: list[Trip] = []
        for city, vehicle_no, route_id, trip_id, started_at, ended_at, points in rows:
            trips.append(Trip(
                city=city,
                vehicle_no=vehicle_no,
                route_id=route_id,
                trip_id=int(trip_id),
                started_at=started_at.isoformat(),
                ended_at=ended_at.isoformat(),
                points=_parse_points(points),
            ))
        return trips

    def _call_osrm(self, points: list[TrailPoint]) -> dict[str, Any]:
        coords = ";".join(f"{p.lng:.6f},{p.lat:.6f}" for p in points)
        timestamps = ";".join(str(p.epoch) for p in points)
        radiuses = ";".join(str(config.WASTE_MATCH_RADIUS_M) for _ in points)
        url = f"{self.osrm_url}/match/v1/driving/{coords}"
        params = {
            'geometries': 'geojson',
            'overview': 'full',
            'radiuses': radiuses,
            'timestamps': timestamps,
            'annotations': 'true',
            'gaps': 'split',
        }
        resp = self.session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get('code') != 'Ok':
            raise RuntimeError(f"OSRM match failed: {payload.get('code')} {payload.get('message', '')}")
        return payload

    def _segments_from_osrm(self, trip: Trip, chunk: list[TrailPoint], payload: dict[str, Any], base_seq: int) -> list[MatchedSegment]:
        matchings = payload.get('matchings') or []
        tracepoints = payload.get('tracepoints') or []
        segments: list[MatchedSegment] = []

        for match_idx, matching in enumerate(matchings):
            confidence = float(matching.get('confidence') or 0)
            if confidence < config.WASTE_MATCH_MIN_CONFIDENCE:
                continue

            coords_raw = (((matching.get('geometry') or {}).get('coordinates')) or [])
            coords = [(float(lng), float(lat)) for lng, lat in coords_raw]
            if len(coords) < 2:
                continue

            associated: list[tuple[TrailPoint, dict[str, Any]]] = []
            for idx, tracepoint in enumerate(tracepoints):
                if tracepoint is None:
                    continue
                if int(tracepoint.get('matchings_index', -1)) != match_idx:
                    continue
                if idx >= len(chunk):
                    continue
                associated.append((chunk[idx], tracepoint))

            if len(associated) < 2:
                continue

            timeline_parts: list[str] = []
            last_progress = 0.0
            for point, tracepoint in associated:
                loc = tracepoint.get('location') or [point.lng, point.lat]
                progress = _nearest_progress(coords, float(loc[0]), float(loc[1]))
                # OSRM can snap noisy points around intersections backwards by a few meters.
                # Clamp to monotonic progress to avoid visual jitter on replay.
                progress = max(last_progress, progress)
                last_progress = progress
                status_char = STATUS_CHARS.get(point.status, 'u')
                timeline_parts.append(f"{point.epoch},{progress:.6f},{status_char},{trip.trip_id}")

            segments.append(MatchedSegment(
                city=trip.city,
                vehicle_no=trip.vehicle_no,
                route_id=trip.route_id,
                trip_id=trip.trip_id,
                segment_seq=base_seq + match_idx,
                started_at=associated[0][0].observed_at,
                ended_at=associated[-1][0].observed_at,
                polyline=coords,
                timeline=";".join(timeline_parts),
                point_count=len(associated),
                confidence=confidence,
            ))

        return segments

    def _insert_segments(self, conn, target_day: date, segments: list[MatchedSegment]) -> int:
        if not segments:
            return 0
        values = []
        for s in segments:
            geojson = json.dumps({
                'type': 'LineString',
                'coordinates': [[lng, lat] for lng, lat in s.polyline],
            }, separators=(',', ':'))
            values.append((
                target_day,
                s.city,
                s.vehicle_no,
                s.route_id,
                s.trip_id,
                s.segment_seq,
                s.started_at,
                s.ended_at,
                geojson,
                s.timeline,
                s.point_count,
                s.confidence,
            ))

        sql = """
        INSERT INTO realtime.waste_trails_matched_daily (
            day, city, vehicle_no, route_id, trip_id, segment_seq,
            started_at, ended_at, geometry, timeline, point_count, confidence, matched_at
        )
        VALUES %s
        ON CONFLICT (day, city, vehicle_no, trip_id, segment_seq)
        DO UPDATE SET
            route_id = EXCLUDED.route_id,
            started_at = EXCLUDED.started_at,
            ended_at = EXCLUDED.ended_at,
            geometry = EXCLUDED.geometry,
            timeline = EXCLUDED.timeline,
            point_count = EXCLUDED.point_count,
            confidence = EXCLUDED.confidence,
            matched_at = NOW();
        """
        template = """
        (%s, %s, %s, %s, %s, %s, %s, %s,
         ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
         %s, %s, %s, NOW())
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, values, template=template, page_size=100)
        conn.commit()
        return len(segments)

    def _record_attempt(
        self,
        conn,
        target_day: date,
        trip: Trip,
        success: bool,
        reason: Optional[str] = None,
    ) -> None:
        """紀錄一筆 OSRM match 嘗試結果，避免下輪重試。

        - success=True: 至少寫進一筆 segment
        - success=False: OSRM NoMatch / low-confidence / HTTP error / chunk 全 skip
        ON CONFLICT 更新 attempted_at + reason，保留最新狀態。
        """
        sql = """
        INSERT INTO realtime.waste_match_attempts
            (day, city, vehicle_no, trip_id, attempted_at, success, reason)
        VALUES (%s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT (day, city, vehicle_no, trip_id)
        DO UPDATE SET
            attempted_at = NOW(),
            success = EXCLUDED.success,
            reason = EXCLUDED.reason;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (
                target_day,
                trip.city,
                trip.vehicle_no,
                trip.trip_id,
                success,
                reason,
            ))
        conn.commit()

    def _match_trip(self, trip: Trip) -> Iterable[tuple[list[MatchedSegment], bool]]:
        for chunk_seq, chunk in _chunks_with_overlap(trip.points, config.WASTE_MATCH_MAX_POINTS):
            try:
                payload = self._call_osrm(chunk)
                segments = self._segments_from_osrm(trip, chunk, payload, chunk_seq * 100)
                yield segments, len(segments) == 0
            except Exception as exc:
                print(f"   ⚠ OSRM match failed {trip.city}/{trip.vehicle_no}/trip={trip.trip_id}/chunk={chunk_seq}: {exc}")
                yield [], True
            time.sleep(0.05)

    def collect(self) -> dict:
        target_days = self.target_days or self._default_target_days()
        totals = {
            'target_days': [d.isoformat() for d in target_days],
            'cities': self.cities,
            'matched_segments': 0,
            'matched_trips_seen': 0,
            'failed_or_skipped_chunks': 0,
            'by_day': {},
        }

        with self._connect() as conn:
            for target_day in target_days:
                trips = self._find_unmatched_trips(conn, target_day)
                day_stats = {'trips': len(trips), 'segments': 0, 'skipped_chunks': 0}
                print(f"   {target_day}: {len(trips)} unmatched trips")

                for trip in trips:
                    totals['matched_trips_seen'] += 1
                    before = day_stats['segments']
                    trip_skipped = 0
                    for segments, skipped in self._match_trip(trip):
                        if segments:
                            day_stats['segments'] += self._insert_segments(conn, target_day, segments)
                        if skipped:
                            trip_skipped += 1
                    if day_stats['segments'] == before and trip_skipped == 0:
                        trip_skipped += 1
                    day_stats['skipped_chunks'] += trip_skipped

                    # 不論成功失敗都寫 attempt，避免下輪 NoMatch / low-confidence trip 重複跑
                    success = day_stats['segments'] > before
                    reason = None if success else (
                        'no_segments_or_low_confidence' if trip_skipped > 0 else 'no_match_attempt_made'
                    )
                    try:
                        self._record_attempt(conn, target_day, trip, success, reason)
                    except Exception as exc:
                        # attempt marker 失敗不該擋住 collector 繼續跑
                        print(f"   ⚠ attempt marker failed {trip.city}/{trip.vehicle_no}/{trip.trip_id}: {exc}")

                totals['matched_segments'] += day_stats['segments']
                totals['failed_or_skipped_chunks'] += day_stats['skipped_chunks']
                totals['by_day'][target_day.isoformat()] = day_stats

        print(f"   ✓ matched segments: {totals['matched_segments']}")
        return totals


def _parse_date_list(value: str) -> list[date]:
    out = []
    for raw in value.split(','):
        raw = raw.strip()
        if raw:
            out.append(date.fromisoformat(raw))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Match waste GPS trails to OSRM road network.")
    parser.add_argument("--target-date", help="Comma-separated Taiwan dates, e.g. 2026-05-07,2026-05-06")
    parser.add_argument("--cities", help="Comma-separated city names, e.g. 高雄市,臺南市")
    parser.add_argument("--osrm-url", help="OSRM base URL, e.g. http://localhost:5000")
    args = parser.parse_args()

    target_days = _parse_date_list(args.target_date) if args.target_date else None
    cities = [c for c in args.cities.split(',') if c] if args.cities else None
    collector = WasteMatchCollector(target_days=target_days, cities=cities, osrm_url=args.osrm_url)
    result = collector.collect()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
