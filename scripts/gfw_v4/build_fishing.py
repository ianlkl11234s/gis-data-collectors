#!/usr/bin/env python3
"""Fetch and build the independent daily Fishing Effort POC sample."""

from __future__ import annotations

import argparse
import gzip
import json
from datetime import date
from pathlib import Path

from _common import add_output_argument, add_repo_to_path, load_env_file, prepare_output, require_gfw_token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selected-day", default="2026-08-21")
    parser.add_argument("--env-file", type=Path, help="optional dotenv file; values are never printed")
    add_output_argument(parser)
    args = parser.parse_args()
    add_repo_to_path()
    load_env_file(args.env_file.expanduser().resolve() if args.env_file else None)

    from scripts.gfw_east_asia_v4_poc import (
        MeasuredGFWReportClient,
        build_fishing_effort_sample,
        fetch_fishing_effort_phase,
    )
    import config

    output = prepare_output(args.output_dir)
    client = MeasuredGFWReportClient(require_gfw_token(config))
    selected_day = date.fromisoformat(args.selected_day)
    payloads, metrics = fetch_fishing_effort_phase(
        client=client, selected_day=selected_day
    )
    metrics["raw_response_saved"] = True
    raw = json.dumps(
        payloads, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    (output / "raw-payloads.json.gz").write_bytes(
        gzip.compress(raw, compresslevel=9, mtime=0)
    )
    result, _assets = build_fishing_effort_sample(
        payloads,
        selected_day=selected_day,
        resolved_dataset_version=metrics["resolved_dataset_versions"][0],
        root=output,
    )
    (output / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    (output / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    print(json.dumps({"output_dir": str(output), "metrics": metrics, "result": result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
