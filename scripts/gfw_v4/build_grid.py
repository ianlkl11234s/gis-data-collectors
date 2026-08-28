#!/usr/bin/env python3
"""Build HIGH→local 0.1-degree hourly Grid PMTiles and detail shards."""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from datetime import date
from pathlib import Path

from _common import add_output_argument, add_repo_to_path, prepare_output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--presence-compare", type=Path, required=True, help="comparison SQLite")
    parser.add_argument("--selected-day", default="2026-08-21")
    add_output_argument(parser)
    args = parser.parse_args()
    add_repo_to_path()

    compare_path = args.presence_compare.expanduser().resolve()
    if not compare_path.is_file():
        parser.error(f"presence comparison SQLite not found: {compare_path}")
    output = prepare_output(args.output_dir)

    from scripts.gfw_east_asia_v4_poc import build_grid_artifacts_from_compare_sqlite

    started = time.perf_counter()
    grid, assets = build_grid_artifacts_from_compare_sqlite(
        compare_path,
        selected_day=date.fromisoformat(args.selected_day),
        root=output,
        semantic_readback=True,
    )
    raw_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    metrics = {
        "wall_time_seconds": round(time.perf_counter() - started, 6),
        "peak_rss_bytes": raw_rss if sys.platform == "darwin" else raw_rss * 1024,
        "grid": grid,
        "assets": assets,
    }
    (output / "grid.metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    print(json.dumps({
        "output_dir": str(output),
        "wall_time_seconds": metrics["wall_time_seconds"],
        "peak_rss_bytes": metrics["peak_rss_bytes"],
        "assets": len(assets),
        "artifact_bytes": sum(item["bytes"] for item in assets),
        "max_hour_members": grid["max_hour_members"],
        "max_hour_cells": grid["max_hour_cells"],
        "semantic_cells": sum(
            hour["pmtiles"]["semantic_readback"]["unique_cells"]
            for hour in grid["hours"]
        ),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
