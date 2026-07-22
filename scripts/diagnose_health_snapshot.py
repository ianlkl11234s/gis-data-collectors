#!/usr/bin/env python3
"""Bounded, read-only performance probe for realtime.health_snapshot()."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg2
import yaml

DEFAULT_MANIFEST = Path(__file__).resolve().parents[1] / "config/realtime_tables.yaml"


def sanitize_error(error: object) -> str:
    """Return only the exception class, never a credential-bearing message."""
    return type(error).__name__


def summarize_probe(metrics: dict[str, Any]) -> str:
    payload = {
        key: metrics[key]
        for key in ("connect_ms", "execute_ms", "fetch_ms", "row_count")
        if key in metrics
    }
    if metrics.get("error"):
        payload["error_type"] = sanitize_error(metrics["error"])
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def load_manifest(path: Path) -> list[dict[str, Any]]:
    return (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("tables", [])


def probe(db_url: str, tables: list[dict[str, Any]], timeout_ms: int) -> dict[str, Any]:
    payload = json.dumps([
        {"schema": table["schema"], "table": table["table"], "time_column": table.get("time_column", "collected_at")}
        for table in tables
    ])
    metrics: dict[str, Any] = {}
    try:
        started = time.perf_counter()
        with psycopg2.connect(db_url, connect_timeout=15) as conn:
            metrics["connect_ms"] = round((time.perf_counter() - started) * 1000, 1)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL TRANSACTION READ ONLY")
                cur.execute("SET LOCAL statement_timeout = %s", (str(timeout_ms),))
                started = time.perf_counter()
                cur.execute("SELECT * FROM realtime.health_snapshot(%s::jsonb)", (payload,))
                metrics["execute_ms"] = round((time.perf_counter() - started) * 1000, 1)
                started = time.perf_counter()
                rows = cur.fetchall()
                metrics["fetch_ms"] = round((time.perf_counter() - started) * 1000, 1)
                metrics["row_count"] = len(rows)
    except Exception as exc:  # output is sanitized by summarize_probe
        metrics["error"] = exc
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--statement-timeout-ms", type=int, default=15_000)
    args = parser.parse_args()
    db_url = os.environ.get("GIS_MONITOR_SUPABASE_DB_URL")
    if not db_url:
        print(json.dumps({"error_type": "MissingCredential"}))
        return 2
    print(summarize_probe(probe(db_url, load_manifest(args.table_manifest), args.statement_timeout_ms)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
