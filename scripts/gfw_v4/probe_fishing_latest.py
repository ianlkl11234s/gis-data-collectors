#!/usr/bin/env python3
"""Probe the latest GFW Fishing Effort date without changing any assets."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from _common import add_output_argument, add_repo_to_path, load_env_file, prepare_output, require_gfw_token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-08-18")
    parser.add_argument("--end-date", default="2026-08-27")
    parser.add_argument("--env-file", type=Path, help="optional dotenv file; values are never printed")
    add_output_argument(parser)
    args = parser.parse_args()
    add_repo_to_path()
    load_env_file(args.env_file.expanduser().resolve() if args.env_file else None)

    from scripts.gfw_east_asia_v4_poc import FIXED_BBOX
    from scripts.gfw_hourly_tracks_poc import _polygon, _report_body
    import config

    output = prepare_output(args.output_dir)
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if end < start:
        parser.error("--end-date must not be earlier than --start-date")
    params = {
        "format": "JSON",
        "temporal-resolution": "DAILY",
        "datasets[0]": "public-global-fishing-effort:latest",
        "date-range": f"{start.isoformat()},{end.isoformat()}",
        "spatial-aggregation": "true",
        "group-by": "FLAGANDGEARTYPE",
    }
    body = _report_body(_polygon((FIXED_BBOX[1], FIXED_BBOX[0], FIXED_BBOX[3], FIXED_BBOX[2])))
    response = requests.post(
        config.GFW_REPORT_URL,
        params=params,
        json=body,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {require_gfw_token(config)}",
            "User-Agent": "GIS-DataCollectors/gfw-v4-local-poc",
        },
        timeout=180,
    )
    if not response.ok:
        raise RuntimeError(f"GFW latest probe HTTP {response.status_code}")
    payload = response.json()
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    dates: list[str] = []

    def visit(value: object) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            if value.get("date") is not None and value.get("hours") is not None:
                try:
                    if float(value["hours"]) > 0:
                        dates.append(str(value["date"])[:10])
                except (TypeError, ValueError):
                    pass
            for item in value.values():
                visit(item)

    visit(payload)
    result = {
        "accessed_at": datetime.now(timezone.utc).isoformat(),
        "request": {
            "dataset_alias": params["datasets[0]"],
            "date_range": params["date-range"],
            "temporal_resolution": params["temporal-resolution"],
            "spatial_aggregation": True,
            "bbox": list(FIXED_BBOX),
        },
        "http_status": response.status_code,
        "resolved_dataset_version": response.headers.get("x-datasets"),
        "response_body_bytes": len(response.content),
        "response_sha256": hashlib.sha256(canonical).hexdigest(),
        "latest_observed_active_date": max(dates) if dates else None,
        "active_dates": sorted(set(dates)),
        "finalization_status": "not_provided_by_gfw",
        "revision_semantics": "dynamic_api_data_may_be_revised",
    }
    (output / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    print(json.dumps({"output_dir": str(output), **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
