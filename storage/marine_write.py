"""Write-path contract for the shared marine observation tables (stdlib only).

Used by storage/supabase_writer.py (Zeabur) and copied byte-for-byte to
external/isohe_port_marine_vm/marine_write.py (HiCloud VM); a test enforces
the copy stays identical.  Background: docs/investigations/2026-10-12-marine-write-stall.md

Rules:
- readings / current only rewrite a row when its observed content changed
  (payload_sha256 and collected_at are bookkeeping and never trigger a write);
- a batch collected earlier never overwrites a row written by a later
  collection (buffer replay guard);
- only rows within LOOKBACK of the latest valid observation already in
  live.marine_observation_current (per station/metric/depth) are sent;
- stations are written in their own short transaction only when the roster
  content changes, or every STATION_REFRESH to advance last_seen_at.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Iterable

STATEMENT_TIMEOUT_MS = 120_000
LOCK_TIMEOUT_MS = 10_000
DEFAULT_LOOKBACK_HOURS = 3.0
STATION_REFRESH = timedelta(hours=6)

STATION_COLS = [
    'station_uid', 'source_network', 'source_station_id', 'origin_org', 'distribution_org',
    'station_type', 'name_zh', 'aliases', 'longitude', 'latitude', 'geom',
    'observed_elements', 'source_status', 'source_url', 'license', 'provenance',
    'first_seen_at', 'last_seen_at',
]
STATION_HASH_EXCLUDE = frozenset({'first_seen_at', 'last_seen_at', 'geom', '_type'})

READING_COLS = [
    'station_uid', 'source_network', 'source_station_id', 'observed_at', 'metric_code', 'depth_key',
    'value_raw', 'value_numeric', 'unit_source', 'unit_canonical', 'vertical_datum',
    'is_missing', 'is_valid', 'missing_reason', 'source_status', 'quality_flags',
    'payload_sha256', 'collected_at', 'geom_at_observation',
]
READING_KEY = ('station_uid', 'observed_at', 'metric_code', 'depth_key')
# Observed content.  payload_sha256 / collected_at are deliberately absent.
READING_COMPARE = [
    'value_raw', 'value_numeric', 'unit_source', 'unit_canonical', 'vertical_datum',
    'is_missing', 'is_valid', 'missing_reason', 'source_status', 'quality_flags',
    'geom_at_observation',
]

CURRENT_COLS = [
    'station_uid', 'metric_code', 'depth_key', 'observed_at', 'value_raw', 'value_numeric',
    'unit_source', 'unit_canonical', 'vertical_datum', 'is_missing', 'is_valid', 'source_status',
    'quality_flags', 'payload_sha256', 'collected_at', 'geom_at_observation',
]
CURRENT_KEY = ('station_uid', 'metric_code', 'depth_key')
CURRENT_COMPARE = [
    'observed_at', 'value_raw', 'value_numeric', 'unit_source', 'unit_canonical', 'vertical_datum',
    'source_status', 'quality_flags', 'geom_at_observation',
]

CUTOFF_SQL = (
    "SELECT station_uid, metric_code, depth_key, observed_at "
    "FROM live.marine_observation_current WHERE station_uid = ANY(%s)"
)


def timeout_statements() -> list[str]:
    """SET LOCAL equivalents; must run as the first statements of each transaction."""
    return [
        f"SELECT set_config('statement_timeout', '{STATEMENT_TIMEOUT_MS}', true)",
        f"SELECT set_config('lock_timeout', '{LOCK_TIMEOUT_MS}', true)",
    ]


def _tuple(prefix: str, cols: Iterable[str]) -> str:
    return '(' + ','.join(f'{prefix}{c}' for c in cols) + ')'


def stations_upsert_sql() -> str:
    # Only issued when station_write_due(); last_seen_at always differs then, so the
    # guard below is just "an older roster (buffer replay) never wins".
    updates = ','.join(f'{c}=EXCLUDED.{c}' for c in STATION_COLS if c not in ('station_uid', 'first_seen_at'))
    return (
        f"INSERT INTO reference.marine_observation_stations AS t ({','.join(STATION_COLS)}) VALUES %s "
        f"ON CONFLICT (station_uid) DO UPDATE SET {updates} "
        f"WHERE t.last_seen_at <= EXCLUDED.last_seen_at"
    )


def stations_template() -> str:
    return '(' + ','.join(['%s'] * 10 + ['ST_GeomFromText(%s,4326)'] + ['%s'] * 7) + ')'


def readings_upsert_sql() -> str:
    updates = ','.join(f'{c}=EXCLUDED.{c}' for c in READING_COLS if c not in READING_KEY + ('source_network', 'source_station_id'))
    return (
        f"INSERT INTO live.marine_observation_readings AS t ({','.join(READING_COLS)}) VALUES %s "
        f"ON CONFLICT ({','.join(READING_KEY)}) DO UPDATE SET {updates} "
        f"WHERE t.collected_at <= EXCLUDED.collected_at "
        f"AND {_tuple('t.', READING_COMPARE)} IS DISTINCT FROM {_tuple('EXCLUDED.', READING_COMPARE)}"
    )


def readings_template() -> str:
    return '(' + ','.join(['%s'] * (len(READING_COLS) - 1) + ['ST_GeomFromText(%s,4326)']) + ')'


def current_upsert_sql() -> str:
    updates = ','.join(f'{c}=EXCLUDED.{c}' for c in CURRENT_COLS if c not in CURRENT_KEY)
    return (
        f"INSERT INTO live.marine_observation_current AS t ({','.join(CURRENT_COLS)}) VALUES %s "
        f"ON CONFLICT ({','.join(CURRENT_KEY)}) DO UPDATE SET {updates} "
        f"WHERE t.observed_at <= EXCLUDED.observed_at AND t.collected_at <= EXCLUDED.collected_at "
        f"AND {_tuple('t.', CURRENT_COMPARE)} IS DISTINCT FROM {_tuple('EXCLUDED.', CURRENT_COMPARE)}"
    )


def current_template() -> str:
    return '(' + ','.join(['%s'] * (len(CURRENT_COLS) - 1) + ['ST_GeomFromText(%s,4326)']) + ')'


def station_roster_hash(stations: list[dict]) -> str:
    """Content hash of the roster, ignoring per-round seen timestamps."""
    canonical = sorted(
        ({k: v for k, v in s.items() if k not in STATION_HASH_EXCLUDE} for s in stations),
        key=lambda s: str(s.get('station_uid')),
    )
    raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True, default=str).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def station_write_due(roster_hash: str, last_hash: str | None, last_written: datetime | None, now: datetime) -> bool:
    if roster_hash != last_hash or last_written is None:
        return True
    return now - last_written >= STATION_REFRESH


def _parse_ts(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else None


def filter_recent_readings(readings: list[dict], latest: dict[tuple, object],
                           lookback_hours: float = DEFAULT_LOOKBACK_HOURS) -> list[dict]:
    """Keep rows newer than (latest valid observation in DB - lookback) per key.

    Unknown keys, unparseable or naive timestamps are kept (over-send is safe;
    the conditional upsert makes it a no-op).
    """
    window = timedelta(hours=lookback_hours)
    cutoffs = {}
    for key, value in latest.items():
        ts = _parse_ts(value)
        if ts is not None:
            cutoffs[key] = ts - window
    kept = []
    for row in readings:
        cutoff = cutoffs.get((row.get('station_uid'), row.get('metric_code'), row.get('depth_key')))
        observed = _parse_ts(row.get('observed_at'))
        if cutoff is None or observed is None or observed >= cutoff:
            kept.append(row)
    return kept


def latest_current_rows(readings: list[dict]) -> list[dict]:
    """Newest valid, non-missing numeric reading per current key."""
    latest: dict[tuple, dict] = {}
    for row in readings:
        if not (row.get('is_valid') and not row.get('is_missing') and row.get('value_numeric') is not None):
            continue
        key = tuple(row[c] for c in CURRENT_KEY)
        previous = latest.get(key)
        if previous is None or str(row['observed_at']) >= str(previous['observed_at']):
            latest[key] = row
    return list(latest.values())


def point_wkt(row: dict) -> str | None:
    lon, lat = row.get('longitude'), row.get('latitude')
    return f'POINT({lon} {lat})' if lon is not None and lat is not None else None
