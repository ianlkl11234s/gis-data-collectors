#!/usr/bin/env python3
"""HiCloud mirror for ISOHE; deploy together with marine_observation.py.

It intentionally uses the same three-table transaction/upsert contract as the
main collector.  It is not installed or scheduled by this repository change.
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
from psycopg2.extras import Json, execute_values

from marine_observation import isohe_payload_to_long

try:
    from vm_buffer import connect_with_retry, flush_pending, save_batch
except ImportError as exc:  # VM deployment must include external/vm_common/vm_buffer.py
    raise RuntimeError("vm_buffer.py is required beside this collector") from exc

PORTS = ("TP", "KL", "TC", "KH", "HL", "SA", "BD", "AP")
RESOURCES = {"wave": "Wave", "current": "Current", "tide": "Tide", "wind": "Wind"}
URL = "https://isohe.ihmt.gov.tw/opendata/{resource}?port={port}&format=JSON"
TZ = timezone(timedelta(hours=8))
APP_DIR = Path(__file__).resolve().parent
load_dotenv(APP_DIR / ".env")


def fetch(ts: str) -> tuple[list[dict], list[dict], dict]:
    stations, readings, raw = {}, [], {}
    for port in PORTS:
        for kind, resource in RESOURCES.items():
            key = f"{port}:{kind}"
            response = requests.get(URL.format(resource=resource, port=port), timeout=30)
            response.raise_for_status()
            payload = response.json()
            raw[key] = payload
            s_rows, r_rows = isohe_payload_to_long(port, kind, payload, ts)
            if not s_rows or not r_rows:
                raise RuntimeError(f"{key}: no valid station coordinates or readings")
            stations.update({s["station_uid"]: s for s in s_rows})
            readings.extend(r_rows)
    return list(stations.values()), readings, raw


def write(conn, stations: list[dict], readings: list[dict]) -> None:
    with conn.cursor() as cur:
        if stations:
            cols = ['station_uid','source_network','source_station_id','origin_org','distribution_org','station_type','name_zh','aliases','longitude','latitude','geom','observed_elements','source_status','source_url','license','provenance','first_seen_at','last_seen_at']
            values = []
            for r in stations:
                geom = f"POINT({r['longitude']} {r['latitude']})"
                values.append(tuple(Json(r.get(c) or []) if c in ('aliases','observed_elements') else Json(r.get(c) or {}) if c == 'provenance' else geom if c == 'geom' else r.get(c) for c in cols))
            template = '(' + ','.join(['%s'] * 10 + ['ST_GeomFromText(%s,4326)'] + ['%s'] * 7) + ')'
            updates = ','.join(f'{c}=EXCLUDED.{c}' for c in cols if c != 'station_uid')
            execute_values(cur, f"INSERT INTO reference.marine_observation_stations ({','.join(cols)}) VALUES %s ON CONFLICT (station_uid) DO UPDATE SET {updates}", values, template=template)
        if readings:
            cols = ['station_uid','source_network','source_station_id','observed_at','metric_code','depth_key','value_raw','value_numeric','unit_source','unit_canonical','vertical_datum','is_missing','is_valid','missing_reason','source_status','quality_flags','payload_sha256','collected_at','geom_at_observation']
            values = []
            for r in readings:
                geom = f"POINT({r['longitude']} {r['latitude']})"
                values.append(tuple(Json(r.get(c) or {}) if c == 'quality_flags' else geom if c == 'geom_at_observation' else r.get(c) for c in cols))
            template = '(' + ','.join(['%s'] * 18 + ['ST_GeomFromText(%s,4326)']) + ')'
            execute_values(cur, f"INSERT INTO live.marine_observation_readings ({','.join(cols)}) VALUES %s ON CONFLICT (station_uid,observed_at,metric_code,depth_key) DO UPDATE SET value_raw=EXCLUDED.value_raw,value_numeric=EXCLUDED.value_numeric,unit_source=EXCLUDED.unit_source,unit_canonical=EXCLUDED.unit_canonical,vertical_datum=EXCLUDED.vertical_datum,is_missing=EXCLUDED.is_missing,is_valid=EXCLUDED.is_valid,missing_reason=EXCLUDED.missing_reason,source_status=EXCLUDED.source_status,quality_flags=EXCLUDED.quality_flags,payload_sha256=EXCLUDED.payload_sha256,collected_at=EXCLUDED.collected_at,geom_at_observation=EXCLUDED.geom_at_observation", values, template=template, page_size=1000)
            eligible = [r for r in readings if r.get('is_valid') and not r.get('is_missing') and r.get('value_numeric') is not None]
            latest = {}
            for r in eligible:
                key = (r['station_uid'], r['metric_code'], r['depth_key'])
                previous = latest.get(key)
                if previous is None or str(r['observed_at']) >= str(previous['observed_at']):
                    latest[key] = r
            current_cols = ['station_uid','metric_code','depth_key','observed_at','value_raw','value_numeric','unit_source','unit_canonical','vertical_datum','is_missing','is_valid','source_status','quality_flags','payload_sha256','collected_at','geom_at_observation']
            current = []
            for r in latest.values():
                geom = f"POINT({r['longitude']} {r['latitude']})"
                current.append(tuple(Json(r.get(c) or {}) if c == 'quality_flags' else geom if c == 'geom_at_observation' else r.get(c) for c in current_cols))
            updates = ','.join(f'{c}=EXCLUDED.{c}' for c in current_cols if c not in ('station_uid','metric_code','depth_key'))
            if current:
                template = '(' + ','.join(['%s'] * 15 + ['ST_GeomFromText(%s,4326)']) + ')'
                execute_values(cur, f"INSERT INTO live.marine_observation_current ({','.join(current_cols)}) VALUES %s ON CONFLICT (station_uid,metric_code,depth_key) DO UPDATE SET {updates} WHERE live.marine_observation_current.observed_at <= EXCLUDED.observed_at", current, template=template, page_size=1000)
    conn.commit()


def _flush_write(conn, payload: dict) -> None:
    """vm_buffer callback: a buffered normalized batch has the same DB contract."""
    write(conn, payload["stations"], payload["readings"])


def save_snapshot(data_dir: Path, timestamp: datetime, raw: dict) -> Path:
    output = data_dir / "isohe_port_marine" / timestamp.strftime("%Y/%m/%d")
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"isohe_port_marine_{timestamp:%H%M}.json"
    path.write_text(json.dumps({"collected_at": timestamp.isoformat(), "raw_payload": raw}, ensure_ascii=False), encoding="utf-8")
    return path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log = logging.getLogger("isohe_port_marine")
    timestamp = datetime.now(TZ)
    data_dir = Path(os.getenv("DATA_DIR", "/var/lib/isohe-port-marine/data"))
    buffer_dir = data_dir / "buffer"

    try:
        stations, readings, raw = fetch(timestamp.isoformat())
        snapshot = save_snapshot(data_dir, timestamp, raw)
        log.info("Fetched stations=%s readings=%s snapshot=%s", len(stations), len(readings), snapshot)
    except Exception as exc:
        log.error("Fetch or local snapshot failed: %s", exc, exc_info=True)
        return 1

    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        log.error("SUPABASE_DB_URL is required after migration apply")
        return 1

    payload = {"stations": stations, "readings": readings}
    conn = None
    try:
        conn = connect_with_retry(lambda: psycopg2.connect(db_url, connect_timeout=15), log=log)
        # Oldest buffered data first; failure remains buffered and current batch still
        # attempts a write so a transient bad historical row cannot block recovery.
        flush_pending(conn, buffer_dir, _flush_write, log=log)
        write(conn, stations, readings)
        log.info("Supabase 寫入: stations=%s readings=%s", len(stations), len(readings))
        return 0
    except Exception as exc:
        log.error("Supabase write failed: %s", exc, exc_info=True)
        buffered = save_batch(buffer_dir, "isohe_port_marine", payload, log=log)
        if buffered is None:
            log.critical("DB failed and buffer save failed; this run cannot be recovered")
        # Fail closed: cron sees failure even if raw snapshot survived, while buffer
        # ensures the normalized batch is retried next run.
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    sys.exit(main())
