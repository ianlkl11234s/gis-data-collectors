#!/usr/bin/env python3
"""HiCloud mirror for ISOHE; deploy together with marine_observation.py and marine_write.py.

It intentionally uses the same three-table transaction/upsert contract as the
main collector.  It is not installed or scheduled by this repository change.
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

from marine_observation import isohe_payload_to_long
import marine_write

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
# Whole-run cap (cron cadence is 10 min). See docs/investigations/2026-10-12-marine-write-stall.md
RUN_DEADLINE_SEC = int(os.getenv("ISOHE_RUN_DEADLINE_SEC", "480"))
LOOKBACK_HOURS = float(os.getenv("MARINE_WRITE_LOOKBACK_HOURS", str(marine_write.DEFAULT_LOOKBACK_HOURS)))


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


def _begin(cur) -> None:
    """SET LOCAL timeouts as the first statements of each transaction."""
    for stmt in marine_write.timeout_statements():
        cur.execute(stmt)


def _load_station_state(state_path: Path | None) -> tuple[str | None, datetime | None]:
    if state_path is None or not state_path.exists():
        return None, None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        return state.get("hash"), datetime.fromisoformat(state["written_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return None, None


def _save_station_state(state_path: Path | None, roster_hash: str, written_at: datetime) -> None:
    if state_path is None:
        return
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"hash": roster_hash, "written_at": written_at.isoformat()}), encoding="utf-8")
    tmp.replace(state_path)


def write(conn, stations: list[dict], readings: list[dict], state_path: Path | None = None) -> int:
    """Same contract as storage/supabase_writer._write_marine; returns readings sent."""
    if stations:
        roster_hash = marine_write.station_roster_hash(stations)
        last_hash, last_written = _load_station_state(state_path)
        now = datetime.now(TZ)
        if marine_write.station_write_due(roster_hash, last_hash, last_written, now):
            values = [tuple(Json(r.get(c) or []) if c in ('aliases', 'observed_elements')
                            else Json(r.get(c) or {}) if c == 'provenance'
                            else marine_write.point_wkt(r) if c == 'geom' else r.get(c)
                            for c in marine_write.STATION_COLS) for r in stations]
            with conn.cursor() as cur:
                _begin(cur)
                execute_values(cur, marine_write.stations_upsert_sql(), values,
                               template=marine_write.stations_template(), page_size=500)
            conn.commit()
            _save_station_state(state_path, roster_hash, now)
    sent = 0
    if readings:
        with conn.cursor() as cur:
            _begin(cur)
            cur.execute(marine_write.CUTOFF_SQL, (sorted({r['station_uid'] for r in readings}),))
            latest = {(uid, metric, depth): observed for uid, metric, depth, observed in cur.fetchall()}
            recent = marine_write.filter_recent_readings(readings, latest, LOOKBACK_HOURS)
            sent = len(recent)
            if recent:
                values = [tuple(Json(r.get(c) or {}) if c == 'quality_flags'
                                else marine_write.point_wkt(r) if c == 'geom_at_observation'
                                else r.get(c) for c in marine_write.READING_COLS) for r in recent]
                execute_values(cur, marine_write.readings_upsert_sql(), values,
                               template=marine_write.readings_template(), page_size=1000)
                current = [tuple(Json(r.get(c) or {}) if c == 'quality_flags'
                                 else marine_write.point_wkt(r) if c == 'geom_at_observation'
                                 else r.get(c) for c in marine_write.CURRENT_COLS)
                           for r in marine_write.latest_current_rows(recent)]
                if current:
                    execute_values(cur, marine_write.current_upsert_sql(), current,
                                   template=marine_write.current_template(), page_size=1000)
        conn.commit()
    return sent


def _flush_write(conn, payload: dict, state_path: Path | None = None) -> None:
    """vm_buffer callback: a buffered normalized batch has the same DB contract.

    Replays are safe: rows older than the DB's latest-minus-lookback are not
    resent, and the upsert guard (collected_at) never lets an older batch
    overwrite a row written by a newer collection.
    """
    write(conn, payload["stations"], payload["readings"], state_path)


def acquire_run_lock(lock_path: Path):
    """Non-blocking exclusive lock; returns the open file or None if another run holds it."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def arm_deadline(seconds: int) -> None:
    """Hard runtime cap. SIGALRM keeps its default disposition (process
    termination by the kernel), so it fires even while blocked inside a
    psycopg2/libpq C call — a Python handler would not."""
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.alarm(seconds)


def save_snapshot(data_dir: Path, timestamp: datetime, raw: dict) -> Path:
    output = data_dir / "isohe_port_marine" / timestamp.strftime("%Y/%m/%d")
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"isohe_port_marine_{timestamp:%H%M}.json"
    path.write_text(json.dumps({"collected_at": timestamp.isoformat(), "raw_payload": raw}, ensure_ascii=False), encoding="utf-8")
    return path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log = logging.getLogger("isohe_port_marine")
    arm_deadline(RUN_DEADLINE_SEC)
    timestamp = datetime.now(TZ)
    data_dir = Path(os.getenv("DATA_DIR", "/var/lib/isohe-port-marine/data"))
    buffer_dir = data_dir / "buffer"
    state_path = data_dir / "state" / "station_roster.json"
    run_lock = acquire_run_lock(data_dir / "state" / "collect.lock")
    if run_lock is None:
        log.error("Another collect run still holds the lock; refusing to stack runs")
        return 1

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
        flush_pending(conn, buffer_dir, lambda c, p: _flush_write(c, p, state_path), log=log)
        sent = write(conn, stations, readings, state_path)
        log.info("Supabase 寫入: stations=%s readings_sent=%s/%s", len(stations), sent, len(readings))
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
