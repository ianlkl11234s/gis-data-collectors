#!/usr/bin/env python3
"""Finalize an existing local GFW v4 shadow output and verify its readback."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

from _common import add_repo_to_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="existing build root")
    parser.add_argument("--phase-root", type=Path, required=True, help="LOW/HIGH comparison root")
    parser.add_argument("--fishing-root", type=Path, required=True, help="Fishing Effort raw phase root")
    parser.add_argument("--latest-probe-dir", type=Path, required=True)
    parser.add_argument("--selected-day", default="2026-08-21")
    args = parser.parse_args()
    add_repo_to_path()

    from scripts.gfw_east_asia_v4_poc import (
        FIXED_BBOX,
        SCHEMA_VERSION,
        _asset,
        _atomic_json,
        build_fishing_effort_sample,
        readback_artifacts,
    )

    root = args.root.expanduser().resolve()
    phase_root = args.phase_root.expanduser().resolve()
    fishing_root = args.fishing_root.expanduser().resolve()
    latest_probe_dir = args.latest_probe_dir.expanduser().resolve()
    manifest_path = root / "manifest.json"
    if not root.is_dir() or manifest_path.exists():
        raise RuntimeError("root must exist and not already have a manifest")
    selected_day = date.fromisoformat(args.selected_day)

    tracks_metrics = json.loads((root / "tracks.metrics.json").read_text(encoding="utf-8"))
    grid_metrics = json.loads((root / "grid.metrics.json").read_text(encoding="utf-8"))
    low_metrics = json.loads((phase_root / "low.metrics.json").read_text(encoding="utf-8"))
    high_metrics = json.loads((phase_root / "high.metrics.json").read_text(encoding="utf-8"))
    parity = json.loads((phase_root / "presence-parity.json").read_text(encoding="utf-8"))
    fishing_metrics = json.loads((fishing_root / "metrics.json").read_text(encoding="utf-8"))
    raw_path = fishing_root / "raw-payloads.json.gz"
    raw_bytes = gzip.decompress(raw_path.read_bytes())
    fishing_payloads = json.loads(raw_bytes)
    raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    source_accessed_at = datetime.fromtimestamp(raw_path.stat().st_mtime, tz=timezone.utc).isoformat()
    latest_probe = json.loads((latest_probe_dir / "result.json").read_text(encoding="utf-8"))
    latest_probe["request"]["group_by"] = "FLAGANDGEARTYPE"

    effort, effort_assets = build_fishing_effort_sample(
        fishing_payloads,
        selected_day=selected_day,
        resolved_dataset_version=fishing_metrics["resolved_dataset_versions"][0],
        root=root,
        latest_observed_active_date=latest_probe["latest_observed_active_date"],
        source_response_sha256=raw_sha256,
        source_accessed_at=source_accessed_at,
    )
    quality = effort["quality"]
    if any(quality[key] for key in (
        "invalid_rows", "negative_hours_rejected", "wrong_day_rows", "boundary_overlap_rows",
    )):
        raise RuntimeError("Fishing Effort failed final quality gate")

    assets = [*grid_metrics["assets"], *tracks_metrics["assets"], *effort_assets]
    readback = readback_artifacts(assets, root=root, verify_pmtiles=True)
    if not readback["individual_mvt_content_verified"]:
        raise RuntimeError("final PMTiles semantic readback is incomplete")
    evidence_root = root / "evidence"
    evidence_root.mkdir()
    for source, name in (
        (phase_root / "low.metrics.json", "presence-low.metrics.json"),
        (phase_root / "high.metrics.json", "presence-high.metrics.json"),
        (phase_root / "presence-parity.json", "presence-parity.json"),
        (fishing_root / "metrics.json", "fishing-effort.metrics.json"),
    ):
        shutil.copy2(source, evidence_root / name)
    _atomic_json(evidence_root / "fishing-latest-probe.json", latest_probe)

    bench_assets = []
    for bucket in tracks_metrics["tracks"]["buckets"]:
        bench_assets.extend((
            {"bucket": bucket["bucket"], "format": "json.gz", **bucket["candidates"]["gzip_json"]["assets"][0]},
            {"bucket": bucket["bucket"], "format": "binary", **bucket["candidates"]["typed_binary"]["assets"][0]},
        ))
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "poc": True,
        "shadow_only": True,
        "immutable_local_output": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "release_id": selected_day.isoformat(),
        "selected_utc_date": selected_day.isoformat(),
        "bbox": list(FIXED_BBOX),
        "production_cutover": False,
        "days": [{"display_date": selected_day.isoformat(), "assets": bench_assets}],
        "presence_route_decision": "HIGH_required_for_lossless_0_1_degree_cell_assignment",
        "presence_parity": parity,
        "source_phases": {"LOW": low_metrics, "HIGH": high_metrics},
        "grid": grid_metrics["grid"],
        "tracks": tracks_metrics["tracks"],
        "fishing_effort": effort,
        "fishing_effort_source_phase": fishing_metrics,
        "fishing_effort_latest_probe": latest_probe,
        "build_metrics": {
            "grid": {key: grid_metrics[key] for key in ("wall_time_seconds", "peak_rss_bytes")},
            "tracks": {key: tracks_metrics[key] for key in ("wall_time_seconds", "peak_rss_bytes")},
        },
        "layer_separation": {
            "gfwHourlyGrid": "independent_layer_1",
            "gfwHourlyTracks": "independent_layer_2",
            "gfwFishingEffort": "independent_layer_3",
            "gfwDarkVessels": "independent_existing_layer_4_untouched",
        },
        "artifacts": assets,
        "artifact_bytes": sum(asset["bytes"] for asset in assets),
        "readback": readback,
        "browser_bench": {"status": "pending_local_acceptance"},
        "release_truth": {
            "build": "passed_local_shadow",
            "contract_wire": "poc_manifest_and_bench_only",
            "stage": "passed_local_immutable_directory",
            "upload": "not_run",
            "readback": "passed_local_hash_json_binary_pmtiles_semantic",
            "pull": "not_run",
            "deploy": "not_run",
            "HTTP": "not_run",
            "browser": "pending_local_acceptance",
        },
    }
    _atomic_json(manifest_path, manifest)
    if json.loads(manifest_path.read_text(encoding="utf-8"))["artifact_bytes"] != readback["checked_bytes"]:
        raise RuntimeError("manifest artifact byte total readback failed")
    print(json.dumps({
        "manifest": str(manifest_path),
        "artifact_count": len(assets),
        "artifact_bytes": manifest["artifact_bytes"],
        "readback": readback,
        "fishing": effort,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
