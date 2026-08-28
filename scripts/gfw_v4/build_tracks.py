#!/usr/bin/env python3
"""Build selected-day HIGH GFW v4 track day-packs for the local POC."""

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
    parser.add_argument("--input", type=Path, required=True, help="HIGH phase NDJSON")
    parser.add_argument("--selected-day", default="2026-08-21")
    add_output_argument(parser)
    args = parser.parse_args()
    add_repo_to_path()

    input_path = args.input.expanduser().resolve()
    if not input_path.is_file():
        parser.error(f"HIGH NDJSON not found: {input_path}")
    output = prepare_output(args.output_dir)

    from scripts.gfw_east_asia_v4_poc import build_track_daypacks, read_ndjson

    started = time.perf_counter()
    tracks, assets = build_track_daypacks(
        read_ndjson(input_path),
        selected_day=date.fromisoformat(args.selected_day),
        root=output,
    )
    raw_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    metrics = {
        "wall_time_seconds": round(time.perf_counter() - started, 6),
        "peak_rss_bytes": raw_rss if sys.platform == "darwin" else raw_rss * 1024,
        "tracks": tracks,
        "assets": assets,
    }
    (output / "tracks.metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    print(json.dumps({
        "output_dir": str(output),
        "wall_time_seconds": metrics["wall_time_seconds"],
        "peak_rss_bytes": metrics["peak_rss_bytes"],
        "buckets": [
            {
                "bucket": item["bucket"],
                "segments": item["segment_count"],
                "points": item["point_count"],
                "json_bytes": item["candidates"]["gzip_json"]["transfer_bytes"],
                "binary_bytes": item["candidates"]["typed_binary"]["transfer_bytes"],
            }
            for item in tracks["buckets"]
        ],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
