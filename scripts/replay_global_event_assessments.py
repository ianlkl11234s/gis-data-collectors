"""Revalidate one accepted source cohort from immutable cache; never fetch or infer.

Dry-run is the default. --apply writes a new run/handoff and candidate observations,
without modifying the original run, routing queue, or source checkpoint.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
from collectors.global_events import (
    GlobalEventsCollector,
    STAGE1_PROMPT_VERSION,
    artifact_sha256,
    candidate_display_records,
    content_sha256,
    immutable_write,
)

REPAIR_PROFILE = "stage1-cached-validation-repair-v1"


class CacheOnlyCollector(GlobalEventsCollector):
    def _request_stage1(self, candidates):
        raise RuntimeError("maintenance replay forbids every provider request")

    def collect(self):
        raise RuntimeError("maintenance replay forbids source collection")

    def run(self):
        raise RuntimeError("maintenance replay never advances a source checkpoint")


def prepare_replay(source_output: Path, batch_id: str, source_run_id: str) -> dict:
    root = Path(config.LOCAL_DATA_DIR) / "global_events"
    source_output = source_output.resolve()
    if not source_output.is_relative_to(root.resolve()):
        raise ValueError("source output must remain inside the production data volume")
    output = json.loads(source_output.read_text())
    source_manifest = output["handoff_manifest"]
    source_receipts = output["_supabase_receipts"]
    source_receipt = next(r for r in source_receipts if r["_type"] == "collector_run")
    if (
        source_receipt["status"] != "accepted"
        or not source_receipt["archive_eligible"]
        or source_receipt["batch_id"] != batch_id
        or source_receipt["run_id"] != source_run_id
        or source_manifest["run_id"] != source_run_id
        or source_manifest["batch_id"] != batch_id
    ):
        raise ValueError("replay requires the explicitly targeted accepted cohort")
    handoff_dir = root / "handoff" / source_output.parent.relative_to(root)
    batch_path = handoff_dir / f"{batch_id}.json"
    source_run_path = handoff_dir / f"{source_run_id}.json"
    if artifact_sha256(batch_path) != source_manifest["batch_sha256"]:
        raise ValueError("source batch artifact hash mismatch")
    if artifact_sha256(source_run_path) != source_manifest["run_sha256"]:
        raise ValueError("source run artifact hash mismatch")
    batch = json.loads(batch_path.read_text())
    source_run = json.loads(source_run_path.read_text())
    if batch["content_sha256"] != content_sha256(
        {"schema_version": batch["schema_version"], "payload": batch["payload"]}
    ):
        raise ValueError("source batch content hash mismatch")
    if (
        source_run["input_batch_id"] != batch_id
        or source_run["input_content_sha256"] != batch["content_sha256"]
        or source_run["prompt_version"] != STAGE1_PROMPT_VERSION
        or source_run["model"] != config.GLOBAL_EVENTS_QWEN_MODEL
        or source_run["output_artifact_sha256"] != content_sha256(source_run["result"])
    ):
        raise ValueError("source run lineage/model/prompt mismatch")

    candidates = batch["payload"]["candidates"]
    chunk_size = min(10, max(1, config.GLOBAL_EVENTS_QWEN_CHUNK_SIZE))
    chunks = [
        candidates[i : i + chunk_size] for i in range(0, len(candidates), chunk_size)
    ]
    observations = source_run["stage1_observation"]["chunks"]
    if len(chunks) != len(observations):
        raise ValueError("cached source chunk layout mismatch")
    cache_hashes = []
    for chunk, observation in zip(chunks, observations):
        cache_key = content_sha256(
            {
                "model": source_run["model"],
                "prompt_version": source_run["prompt_version"],
                "candidates": chunk,
            }
        )
        cache_path = root / "stage1_cache" / f"{cache_key}.json"
        cached = json.loads(
            cache_path.read_text()
        )  # Missing cache fails; never requests Qwen.
        if (
            observation["candidate_count"] != len(chunk)
            or cached["raw_response_sha256"] != observation["raw_response_sha256"]
            or not source_run["started_at"]
            <= cached["assessed_at"]
            <= source_run["finished_at"]
        ):
            raise ValueError("cached provider lineage mismatch")
        cache_hashes.append(
            {"cache_key": cache_key, "sha256": artifact_sha256(cache_path)}
        )

    collector = CacheOnlyCollector.__new__(CacheOnlyCollector)
    collector._normalization_lineage = []
    collector._stage1_validation_rejections = []
    collector._stage1_validation_diagnostics = []
    collector._assessment_times = {}
    collector._stage1_usage = {}
    collector._stage1_observation = {}
    collector._raw_response_sha256 = None
    result = collector._assess_in_chunks(candidates)
    if collector._raw_response_sha256 != source_run["raw_response_sha256"]:
        raise ValueError("aggregate provider response hash changed")
    if not all(c["cache_hit"] for c in collector._stage1_observation["chunks"]):
        raise ValueError("maintenance replay must be entirely cached")

    result_hash = content_sha256(result)
    run_id = "run_" + content_sha256([REPAIR_PROFILE, source_run_id, result_hash])[:32]
    plan_path = root / "maintenance" / f"{run_id}.plan.json"
    if plan_path.exists():
        plan = json.loads(plan_path.read_text())
        if (
            plan["run"]["output_artifact_sha256"] != result_hash
            or plan["cache_hashes"] != cache_hashes
        ):
            raise ValueError("immutable replay plan mismatch")
        return plan

    now = datetime.now(timezone.utc).isoformat()
    valid = len(result["assessments"])
    rejected = len(collector._stage1_validation_rejections)
    observation = {
        **collector._stage1_observation,
        "replayed_from_run_id": source_run_id,
        "repair_profile": REPAIR_PROFILE,
        "provider_called": False,
        "provider_started_at": source_run["started_at"],
        "provider_finished_at": source_run["finished_at"],
        "cache_hashes": cache_hashes,
    }
    run = {
        **source_run,
        "run_id": run_id,
        "started_at": now,
        "finished_at": now,
        "valid_assessment_count": valid,
        "rejected_assessment_count": rejected,
        "validation_status": (
            "accepted_all"
            if not rejected
            else ("accepted_partial" if valid else "accepted_all_rejected")
        ),
        "traditional_chinese_gate": (
            "canonical_all_passed" if not rejected else "canonical_survivors_passed"
        ),
        "normalization_lineage": collector._normalization_lineage,
        "validation_rejections": collector._stage1_validation_rejections,
        "validation_diagnostics": collector._stage1_validation_diagnostics,
        "source_warning": f"Cached validation repair of {source_run_id}; no new provider request; title and GDELT metadata only",
        "stage1_observation": observation,
        "usage": {
            "input_units": 0,
            "output_units": 0,
            "reasoning_units": 0,
            "cost_usd": 0,
        },
        "output_artifact_sha256": result_hash,
        "result": result,
    }
    # Provider assessed_at is unchanged; only DB available_at reflects repair time.
    display = candidate_display_records(
        batch, result, source_run["finished_at"], collector._assessment_times
    )
    manifest = {
        **source_manifest,
        "run_id": run_id,
        "run_sha256": hashlib.sha256(
            (
                json.dumps(run, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
            ).encode()
        ).hexdigest(),
        "run_key": f"global_events/handoff/runs/{run_id}.json",
        "run_object_key": f"global_events/handoff/runs/{run_id}.json",
        "manifest_object_key": f"global_events/handoff/manifests/{batch_id}.{run_id}.manifest.json",
        "stage1_sha256": result_hash,
        "stage1_observation": observation,
        "created_at": now,
    }
    receipt = {
        **source_receipt,
        "run_id": run_id,
        "started_at": now,
        "finished_at": now,
        "output_artifact_sha256": result_hash,
        "receipt": manifest,
    }
    return {
        "run": run,
        "manifest": manifest,
        "batch_path": str(batch_path),
        "plan_path": str(plan_path),
        "cache_hashes": cache_hashes,
        "output": {
            "data": [{"batch_id": batch_id}],
            "_candidate_display_records": display,
            "_supabase_receipts": [
                next(r for r in source_receipts if r["_type"] == "collector_batch"),
                receipt,
            ],
        },
    }


def apply_replay(plan: dict, writer=None) -> None:
    from collectors.base import get_supabase_writer

    batch_path = Path(plan["batch_path"])
    run_path = batch_path.parent / f"{plan['run']['run_id']}.json"
    immutable_write(Path(plan["plan_path"]), plan)
    immutable_write(run_path, plan["run"])
    if artifact_sha256(run_path) != plan["manifest"]["run_sha256"]:
        raise ValueError("immutable repair run hash mismatch")
    collector = CacheOnlyCollector.__new__(CacheOnlyCollector)
    if not collector._upload_handoff(
        batch_path, run_path, plan["manifest"], plan["run"]["run_id"]
    ):
        raise RuntimeError("maintenance S3 manifest-last handoff failed; safe to retry")
    writer = writer or get_supabase_writer()
    if writer is None or not writer.write(
        "global_events", plan["output"], datetime.now(timezone.utc)
    ):
        raise RuntimeError("maintenance DB write failed; safe to retry")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-output", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    plan = prepare_replay(args.source_output, args.batch_id, args.source_run_id)
    if args.apply:
        apply_replay(plan)
    display = plan["output"]["_candidate_display_records"]
    print(
        json.dumps(
            {
                "mode": "applied" if args.apply else "dry_run",
                "run_id": plan["run"]["run_id"],
                "batch_id": args.batch_id,
                "source_run_id": args.source_run_id,
                "provider_called": False,
                "candidate_count": len(display),
                "decisions": dict(Counter(r["decision"] or "pending" for r in display)),
                "located_candidates": sum(bool(r["places"]) for r in display),
                "place_count": sum(len(r["places"]) for r in display),
                "validation_rejections": plan["run"]["validation_rejections"],
                "diagnostic_count": len(plan["run"]["validation_diagnostics"]),
                "raw_response_sha256": plan["run"]["raw_response_sha256"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
