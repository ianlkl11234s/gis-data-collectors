#!/usr/bin/env python3
"""Read-only health snapshot for Hermes' gis-data-collectors monitoring jobs.

Reads only the GIS_MONITOR_* environment variables. It never prints credentials,
writes to Supabase/S3, or mutates the collector repository.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import psycopg2
import yaml

from gis_collectors_monitor_policy import classify_anomaly, classify_archive, transition_incident

REPO = Path("/opt/data/gis-data-collectors")
TAIPEI = timezone(timedelta(hours=8))
STATE_DIR = Path(os.environ.get("GIS_MONITOR_STATE_DIR", "/opt/data/home/.hermes/state/gis-collectors-monitor"))
STATE_FILE = STATE_DIR / "incidents.json"
REQUIRED = (
    "GIS_MONITOR_SUPABASE_DB_URL",
    "GIS_MONITOR_S3_BUCKET",
    "GIS_MONITOR_S3_REGION",
    "GIS_MONITOR_S3_ACCESS_KEY",
    "GIS_MONITOR_S3_SECRET_KEY",
)


def parse_archive_date(key: str) -> str | None:
    name = key.rsplit("/", 1)[-1]
    if not name.endswith(".tar.gz"):
        return None
    value = name.removesuffix(".tar.gz")
    try:
        date.fromisoformat(value)
    except ValueError:
        return None
    return value


def load_incident_state() -> tuple[dict, bool]:
    """Load only sanitized local notification state; corrupt state is safely reset."""
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return (data.get("incidents", {}) if isinstance(data, dict) else {}), False
    except FileNotFoundError:
        return {}, False
    except (OSError, json.JSONDecodeError):
        return {}, True


def save_incident_state(incidents: dict) -> None:
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps({"schema_version": 1, "incidents": incidents}, separators=(",", ":")), encoding="utf-8")
    temp.chmod(0o600)
    temp.replace(STATE_FILE)


def annotate_notification_state(out: dict, now: datetime) -> None:
    """Apply notification policy after all read-only checks have completed."""
    state, was_reset = load_incident_state()
    events: list[dict] = []
    confirmed: list[dict] = []
    for candidate in out.get("incident_candidates", []):
        level = "critical"
        if candidate.get("kind") in {"supabase_unavailable", "s3_unavailable"}:
            level = "watch"
        if candidate.get("kind") == "s3_archive":
            level = classify_archive(candidate, now).get("level", level)
        fingerprint = f"{candidate.get('kind')}:{candidate.get('target', candidate.get('collector', candidate.get('host', 'global')))}"
        event, state = transition_incident(state, fingerprint, level, now.isoformat())
        if event["event"] != "silent":
            events.append({**event, "candidate": candidate})
        if event["level"] == "critical":
            confirmed.append(candidate)
    save_incident_state(state)
    out["incident_candidates"] = confirmed
    out["channel_events"] = events
    out["expected_conditions"] = [
        classify_anomaly(item) for item in out.get("supabase", {}).get("anomalies", [])
        if classify_anomaly(item).get("level") == "expected"
    ]
    out["overall_status"] = "red" if confirmed else ("yellow" if events else "green")
    if was_reset:
        out.setdefault("watch_candidates", []).append({"kind": "incident_state_reset", "severity": "watch"})


def main() -> None:
    now = datetime.now(TAIPEI)
    out: dict = {
        "schema_version": 1,
        "generated_at": now.isoformat(),
        "credential_presence": {
            key: "present" if os.environ.get(key) else "missing" for key in REQUIRED
        },
        "supabase": {"status": "unknown"},
        "s3": {"status": "unknown"},
        "incident_candidates": [],
    }
    if any(not os.environ.get(key) for key in REQUIRED):
        out["incident_candidates"].append({
            "kind": "monitor_credentials_missing", "severity": "critical"
        })
        print(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
        return

    tables = (yaml.safe_load((REPO / "config/realtime_tables.yaml").read_text(encoding="utf-8")) or {}).get("tables", [])
    layer_map = yaml.safe_load((REPO / "config/cross_layer_map.yaml").read_text(encoding="utf-8")) or {}

    # Exactly one read-only RPC for all configured tables.
    try:
        payload = json.dumps([
            {"schema": t["schema"], "table": t["table"], "time_column": t.get("time_column", "collected_at")}
            for t in tables
        ])
        with psycopg2.connect(os.environ["GIS_MONITOR_SUPABASE_DB_URL"], connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM realtime.health_snapshot(%s::jsonb)", (payload,))
                rows = cur.fetchall()
        results = {(schema, table): (max_time, error) for schema, table, max_time, _count, error in rows}
        counts = {key: 0 for key in ("OK", "STALE", "DEAD", "NEVER", "ERROR")}
        anomalies: list[dict] = []
        for spec in tables:
            max_time, error = results.get((spec["schema"], spec["table"]), (None, "missing_rpc_row"))
            age_min: int | None = None
            if error:
                state = "ERROR"
            elif max_time is None:
                state = "NEVER"
            else:
                if max_time.tzinfo is None:
                    max_time = max_time.replace(tzinfo=TAIPEI)
                age_min = max(0, int((now - max_time).total_seconds() / 60))
                interval = int(spec.get("expected_interval_min", 60))
                state = "OK" if age_min < interval * 3 else ("STALE" if age_min < interval * 12 else "DEAD")
            counts[state] += 1
            if state != "OK":
                item = {
                    "kind": "supabase_freshness",
                    "state": state,
                    "target": f"{spec['schema']}.{spec['table']}",
                    "owner": spec.get("owner_collector"),
                    "critical": bool(spec.get("critical")),
                    "age_min": age_min,
                    "notes": spec.get("notes", ""),
                }
                anomalies.append(item)
                if state in ("ERROR", "DEAD") and item["critical"]:
                    out["incident_candidates"].append({**item, "severity": "critical"})
        out["supabase"] = {"status": "ok", "table_count": len(tables), "counts": counts, "anomalies": anomalies[:20]}
    except Exception as exc:
        out["supabase"] = {"status": "unavailable", "error_type": type(exc).__name__}
        out["incident_candidates"].append({"kind": "supabase_unavailable", "severity": "critical", "error_type": type(exc).__name__})

    # S3 metadata and VM snapshots: List/Get only; no object or bucket mutation.
    try:
        s3 = boto3.client(
            "s3",
            region_name=os.environ["GIS_MONITOR_S3_REGION"],
            aws_access_key_id=os.environ["GIS_MONITOR_S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["GIS_MONITOR_S3_SECRET_KEY"],
        )
        bucket = os.environ["GIS_MONITOR_S3_BUCKET"]
        archive_checks: list[dict] = []
        for collector, spec in layer_map.items():
            if not spec.get("enabled"):
                continue
            for prefix_spec in spec.get("s3_prefixes", []):
                if not prefix_spec.get("expected_daily", True):
                    continue
                prefix = prefix_spec.get("prefix", "")
                latest: str | None = None
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        item_date = parse_archive_date(obj["Key"])
                        if item_date and (latest is None or item_date > latest):
                            latest = item_date
                allowed_lag = int(prefix_spec.get("archive_lag_days", 0))
                # Archives are daily end-of-day artifacts. During the current day,
                # yesterday is the newest normal archive; VM collectors additionally
                # retain N days locally before upload.
                cutoff = (now.date() - timedelta(days=allowed_lag + 1)).isoformat()
                state = "OK" if latest and latest >= cutoff else ("MISSING" if latest is None else "STALE")
                item = {
                    "collector": collector,
                    "state": state,
                    "latest_date": latest,
                    "required_on_or_after": cutoff,
                    "critical": bool(spec.get("critical")),
                }
                archive_checks.append(item)
                if state != "OK" and item["critical"]:
                    out["incident_candidates"].append({"kind": "s3_archive", "severity": "critical", **item})

        latest_keys: dict[str, str] = {}
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="_external_vm_health/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                rest = key[len("_external_vm_health/"):]
                if "/" not in rest:
                    continue
                host = rest.split("/", 1)[0]
                if host not in latest_keys or key > latest_keys[host]:
                    latest_keys[host] = key
        vm_checks: list[dict] = []
        for host, key in sorted(latest_keys.items()):
            try:
                snapshot = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
                generated = datetime.fromisoformat(snapshot.get("generated_at", ""))
                if generated.tzinfo is None:
                    generated = generated.replace(tzinfo=TAIPEI)
                age_hours = round((now - generated).total_seconds() / 3600, 1)
                item = {"host": host, "state": "LOST" if age_hours > 30 else "OK", "age_hours": age_hours}
            except Exception as exc:
                item = {"host": host, "state": "UNAVAILABLE", "error_type": type(exc).__name__}
            vm_checks.append(item)
            if item["state"] != "OK":
                out["incident_candidates"].append({"kind": "external_vm", "severity": "critical", **item})
        out["s3"] = {"status": "ok", "archive_checks": archive_checks, "vm_checks": vm_checks}
    except Exception as exc:
        out["s3"] = {"status": "unavailable", "error_type": type(exc).__name__}
        out["incident_candidates"].append({"kind": "s3_unavailable", "severity": "critical", "error_type": type(exc).__name__})

    annotate_notification_state(out, now)
    print(json.dumps(out, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
