"""A/B one Stage1 prompt revision against the immutable S3 handoff batches.

Reads real candidate cohorts from ``global_events/handoff/batches/`` and sends
each chunk twice -- once with the baseline system prompt and output contract,
once with the current one -- so the two arms differ only in the prompt. Nothing
is written back: no run manifest, no handoff, no candidate observation, no
source checkpoint. Two provider calls per chunk, and ``--max-calls`` is a hard
ceiling checked before every request.

The baseline arm is reproduced by intercepting the request body the real
``_request_stage1`` built and swapping in the recorded baseline prompt plus the
two contract fields that changed, rather than by re-implementing the request.
That keeps every other parameter (model, temperature, max_tokens, JSON mode,
reasoning effort, fallbacks) byte-identical between arms.

Usage:
    set -a; source .env; set +a
    python3 scripts/eval_global_event_prompt_ab.py --chunks 3 --out eval.json
"""

from __future__ import annotations

import argparse
import copy
import json
from collections import Counter
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
from collectors.global_events import (
    STAGE1_PROMPT_VERSION,
    GlobalEventsCollector,
    candidate_display_records,
    gazetteer_entries_from_locations,
    validate_stage1,
)

# The v3 prompt, recorded verbatim so the baseline arm stays reproducible after
# the module constant moves on. Do not "tidy" this string.
BASELINE_PROMPT_VERSION = "global-events-stage1/v3"
BASELINE_SYSTEM_PROMPT = (
    "輸入只有 GDELT 標題與 metadata；只輸出 user "
    "output_contract 指定的 JSON，不要 markdown、解釋或額外欄位；"
    "所有中文欄位必須使用臺灣正體中文（zh-TW，例如臺灣、資訊、影響），"
    "不得混入簡體字。每個候選都要回傳判斷，decision 只是分類，不是刪除指令。"
    "依事件本身的人命、生活、社會或跨境影響判斷；臺灣關聯獨立填寫，"
    "不得僅因與臺灣無關而降級或判為 drop_noise。低重要性資料仍要完整輸出。"
    "標題盡量40字內、摘要120字內、兩項理由各80字內，避免重複贅詞。"
    "location_evidence_ids 只選標題明確支持的發生地或受影響地，basis 必須逐字引用"
    "該 evidence source_url 的輸入標題片段；不得把發言者國籍、新聞來源所在地、"
    "單純背景提及當發生地。無法支持就回傳空陣列，不猜座標。"
)
BASELINE_BASIS_CONTRACT = (
    "exact substring from that evidence source URL's input title"
)


class _BudgetedSession:
    """Wraps the collector session: counts calls and rewrites the baseline arm."""

    def __init__(self, inner, max_calls: int):
        self.inner = inner
        self.max_calls = max_calls
        self.calls = 0
        self.arm = "current"

    def post(self, url, **kwargs):
        if self.calls >= self.max_calls:
            raise RuntimeError(f"provider call budget exhausted at {self.max_calls}")
        if self.arm == "baseline":
            body = kwargs["json"]
            body["messages"][0]["content"] = BASELINE_SYSTEM_PROMPT
            user = json.loads(body["messages"][1]["content"])
            contract = user["output_contract"]
            contract["assessment_optional_fields"]["location_evidence_ids"][
                "item_fields"
            ]["basis"] = BASELINE_BASIS_CONTRACT
            contract.pop("length_hints", None)
            body["messages"][1]["content"] = json.dumps(user, ensure_ascii=False)
        self.calls += 1
        return self.inner.post(url, **kwargs)


def strict_basis_ok(selection, candidate) -> bool:
    """The v3 rule: basis must come from the evidence's *own* document title."""
    evidence = next(
        item
        for item in candidate["location_evidence"]
        if item["evidence_id"] == selection["evidence_id"]
    )
    return any(
        selection["basis"] in document["title"]
        and document["url"] == evidence["source_url"]
        for document in candidate["representative_documents"]
    )


def batch_gazetteer(batch: dict) -> dict:
    """Approximate the persisted index using only this batch's own GDELT names."""
    entries: dict = {}
    for candidate in batch["payload"]["candidates"]:
        entries.update(
            gazetteer_entries_from_locations(candidate.get("location_evidence", []))
        )
    return entries


def score(arm: str, chunk, raw, observation, gazetteer) -> dict:
    rejections: list = []
    diagnostics: list = []
    validated = validate_stage1(
        copy.deepcopy(raw), chunk, None, rejections, diagnostics
    )
    assessments = validated["assessments"]
    relaxed = sum(
        1 for item in assessments if item.get("location_evidence_ids")
    )
    by_id = {item["candidate_id"]: item for item in chunk}
    strict = sum(
        1
        for item in assessments
        if any(
            strict_basis_ok(selection, by_id[item["candidate_id"]])
            for selection in item.get("location_evidence_ids", [])
        )
    )
    records = candidate_display_records(
        {"batch_id": "batch_" + "0" * 24, "payload": {"candidates": chunk}},
        {"assessments": assessments},
        "2026-09-05T00:00:00+00:00",
        None,
        gazetteer,
    )
    lengths = Counter()
    for item in assessments:
        for field in (
            "title_zh_tw",
            "summary_zh_tw",
            "reason_zh_tw",
            "taiwan_impact_zh_tw",
        ):
            lengths[field] += len(item.get(field) or "")
    return {
        "arm": arm,
        "candidates": len(chunk),
        "assessments": len(assessments),
        "rejections": len(rejections),
        "rejection_errors": [item["error"][:90] for item in rejections],
        "location_diagnostics": len(diagnostics),
        "selection_relaxed": relaxed,
        "selection_strict": strict,
        "decisions": dict(Counter(item["decision"] for item in assessments)),
        "severities": dict(Counter(item["severity"] for item in assessments)),
        "taiwan_none": sum(
            1 for item in assessments if item["taiwan_relationship"] == "none"
        ),
        "empty_taiwan_impact": sum(
            1
            for item in raw.get("assessments", [])
            if isinstance(item, dict) and not (item.get("taiwan_impact_zh_tw") or "")
        ),
        "located": sum(1 for record in records if record["places"]),
        "zh_chars": dict(lengths),
        "finish_reason": observation.get("finish_reason"),
        "content_length": observation.get("content_length"),
        "usage": observation.get("usage") or {},
        "records": [
            {
                "headline": record["source_headline"],
                "title_zh_tw": record["title_zh_tw"],
                "decision": record["decision"],
                "severity": record["severity"],
                "countries": sorted(
                    {
                        place["country_code"]
                        for place in record["places"]
                        if place["country_code"]
                    }
                ),
                "place_kinds": sorted(
                    {place["source_kind"] for place in record["places"]}
                ),
            }
            for record in records
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-dir", default="../eval")
    parser.add_argument("--chunks", type=int, default=3)
    parser.add_argument("--max-calls", type=int, default=10)
    parser.add_argument("--out", default="../eval/result.json")
    parser.add_argument(
        "--arms",
        default="baseline,current",
        help="Comma-separated arms to call. Re-running only 'current' after a "
        "prompt fix reuses the recorded baseline and costs 1 call per chunk.",
    )
    args = parser.parse_args()

    chunk_size = min(10, max(1, config.GLOBAL_EVENTS_QWEN_CHUNK_SIZE))
    paths = sorted(Path(args.batch_dir).glob("batch_*.json"))
    batches = sorted(
        (json.loads(path.read_text()) for path in paths),
        key=lambda b: b["payload"]["observation_window"]["last_slot"],
        reverse=True,
    )[: args.chunks]
    if len(batches) < args.chunks:
        raise SystemExit(f"need {args.chunks} batches, found {len(batches)}")

    collector = GlobalEventsCollector()
    session = _BudgetedSession(collector._session, args.max_calls)
    collector._session = session

    results = []
    for index, batch in enumerate(batches, 1):
        chunk = batch["payload"]["candidates"][:chunk_size]
        gazetteer = batch_gazetteer(batch)
        arms = {}
        for arm in [name.strip() for name in args.arms.split(",") if name.strip()]:
            session.arm = arm
            collector._stage1_observation = {}
            collector._stage1_usage = {}
            try:
                raw = collector._request_stage1(chunk)
            except Exception as exc:  # Record, never retry: the budget is hard.
                arms[arm] = {
                    "arm": arm,
                    "error": f"{type(exc).__name__}: {exc}"[:300],
                    "finish_reason": (collector._stage1_observation or {}).get(
                        "finish_reason"
                    ),
                    "content_length": (collector._stage1_observation or {}).get(
                        "content_length"
                    ),
                }
                continue
            arms[arm] = score(
                arm, chunk, raw, collector._stage1_observation, gazetteer
            )
        results.append(
            {
                "chunk": index,
                "batch_id": batch["batch_id"],
                "last_slot": batch["payload"]["observation_window"]["last_slot"],
                "gazetteer_names": len(gazetteer),
                "arms": arms,
            }
        )
        print(f"chunk {index}: calls so far {session.calls}", flush=True)

    payload = {
        "arms": args.arms,
        "baseline_prompt_version": BASELINE_PROMPT_VERSION,
        "current_prompt_version": STAGE1_PROMPT_VERSION,
        "model": config.GLOBAL_EVENTS_QWEN_MODEL,
        "chunk_size": chunk_size,
        "provider_calls": session.calls,
        "chunks": results,
    }
    Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"provider calls used: {session.calls} -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
