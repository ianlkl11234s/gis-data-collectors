"""Read-only, rerunnable OpenRouter benchmark for news-event annotation.

The script never imports or runs the collector write path and never writes
``live.news_events``.  Use a local JSONL sample, or opt into a bounded SELECT
with ``--db-sample``.  Results default to /tmp so benchmark artifacts are not
accidentally committed.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import requests

import config
from collectors.news_events import CATEGORY_ENUM, SYSTEM_PROMPT_HEADER, TownshipGazetteer


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OUTPUT_JSON = Path("/tmp/news_openrouter_benchmark.json")
DEFAULT_OUTPUT_MARKDOWN = Path("/tmp/news_openrouter_benchmark.md")
VALID_FIELDS = ("category", "gis_relevance", "severity", "is_event")


def annotation_response_format(row_count: int) -> dict[str, Any]:
    """Strict schema shared by benchmark candidates; row count prevents partial output."""
    nullable_string = {"anyOf": [{"type": "string"}, {"type": "null"}]}
    item = {
        "type": "object",
        "properties": {
            "idx": {"type": "integer", "minimum": 0, "maximum": max(0, row_count - 1)},
            "county": nullable_string,
            "township": nullable_string,
            "category": {"type": "string", "enum": list(CATEGORY_ENUM)},
            "summary": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "gis_relevance": {"type": "integer", "minimum": 0, "maximum": 3},
            "severity": {"type": "integer", "minimum": 0, "maximum": 3},
            "is_event": {"type": "boolean"},
        },
        "required": [
            "idx", "county", "township", "category", "summary", "confidence",
            "gis_relevance", "severity", "is_event",
        ],
        "additionalProperties": False,
    }
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "news_event_annotations",
            "strict": True,
            "schema": {
                "type": "array",
                "items": item,
                "minItems": row_count,
                "maxItems": row_count,
            },
        },
    }


def _load_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_number}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"JSONL row {line_number} must be an object")
            if not row.get("title"):
                raise ValueError(f"JSONL row {line_number} is missing title")
            rows.append(row)
            if len(rows) >= limit:
                break
    return rows


def load_db_sample(limit: int) -> list[dict[str, Any]]:
    """Bounded read-only sample; deliberately contains only a SELECT + LIMIT."""
    dsn = getattr(config, "SUPABASE_DB_URL", None)
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL is required for --db-sample")
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - depends on local extras
        raise RuntimeError("psycopg2 is required for --db-sample") from exc
    query = (
        "SELECT source, url, title, summary, county "
        "FROM live.news_events ORDER BY published_ts DESC LIMIT %s"
    )
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            return [
                {"source": row[0], "url": row[1], "title": row[2], "summary": row[3] or "",
                 "county_hint": row[4]}
                for row in cur.fetchall()
            ]


def load_gazetteer(path: Path | None) -> TownshipGazetteer:
    if path is None:
        path = config.LOCAL_DATA_DIR / "news_events" / "township_gazetteer.json"
    if not path.exists():
        raise RuntimeError("gazetteer JSON is required; pass --gazetteer-json")
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("gazetteer JSON is unreadable") from exc
    gazetteer = TownshipGazetteer(rows if isinstance(rows, list) else [])
    if gazetteer.is_empty():
        raise RuntimeError("gazetteer JSON is empty")
    return gazetteer


def _parse_annotations(raw: str) -> list[dict[str, Any]]:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        text = text.rsplit("```", 1)[0]
    parsed = json.loads(text)
    if isinstance(parsed, dict):
        parsed = next((value for value in parsed.values() if isinstance(value, list)), [])
    if not isinstance(parsed, list):
        raise ValueError("model JSON must be an array or contain an array")
    return [item for item in parsed if isinstance(item, dict)]


def _request_model(model: str, rows: list[dict[str, Any]], gazetteer: TownshipGazetteer,
                   api_key: str, timeout: int, *, structured: bool = False,
                   disable_reasoning: bool = False, max_tokens: int | None = None) -> dict[str, Any]:
    prompt_rows = []
    for idx, row in enumerate(rows):
        item = {"idx": idx, "title": row["title"], "summary": row.get("summary") or ""}
        if row.get("county_hint"):
            item["county_hint"] = row["county_hint"]
        prompt_rows.append(json.dumps(item, ensure_ascii=False))
    started = time.monotonic()
    try:
        request_body: dict[str, Any] = {
            "model": model,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT_HEADER + "\n".join(gazetteer.prompt_lines())},
                {"role": "user", "content": "\n".join(prompt_rows)},
            ],
        }
        if structured:
            request_body["response_format"] = annotation_response_format(len(rows))
            request_body["provider"] = {"require_parameters": True}
        if disable_reasoning:
            request_body["reasoning"] = {"enabled": False}
        if max_tokens is not None:
            request_body["max_tokens"] = max(1, max_tokens)
        response = requests.post(
            OPENROUTER_URL,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=request_body,
            timeout=max(1, timeout),
        )
        response.raise_for_status()
        payload = response.json()
        choices = payload.get("choices") if isinstance(payload, dict) else None
        choice = choices[0] if isinstance(choices, list) and choices else {}
        message = choice.get("message") if isinstance(choice, dict) else {}
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise ValueError("response has no message content")
        annotations = _parse_annotations(content)
        usage = payload.get("usage") if isinstance(payload, dict) else {}
        usage = usage if isinstance(usage, dict) else {}
        return {"model": model, "annotations": annotations, "latency_seconds": round(time.monotonic() - started, 4),
                "tokens": {"input": usage.get("prompt_tokens", usage.get("input_tokens")),
                           "output": usage.get("completion_tokens", usage.get("output_tokens"))},
                "provider_cost_usd": usage.get("cost"), "error": None}
    except Exception as exc:
        # Never retain request/response bodies or provider text: either can contain news.
        return {"model": model, "annotations": [], "latency_seconds": round(time.monotonic() - started, 4),
                "tokens": {"input": None, "output": None}, "provider_cost_usd": None,
                "error": f"OpenRouter request failed ({type(exc).__name__})"}


def evaluate_annotations(rows: list[dict[str, Any]], annotations: list[dict[str, Any]],
                         gazetteer: TownshipGazetteer, gold: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    expected = set(range(len(rows)))
    idx_values = [item.get("idx") for item in annotations if isinstance(item.get("idx"), int)]
    counts = Counter(idx_values)
    valid = {idx: next(item for item in annotations if item.get("idx") == idx)
             for idx in expected if counts[idx] == 1}
    missing = sorted(expected - set(idx_values))
    duplicates = sorted(idx for idx, count in counts.items() if count > 1)
    invalid_idx = sorted(idx for idx in counts if idx not in expected)
    location_claims = valid_locations = 0
    county_claims = valid_counties = 0
    township_claims = valid_townships = 0
    downgraded_to_county = 0
    field_valid = {field: 0 for field in VALID_FIELDS}
    for item in valid.values():
        raw_county = gazetteer._norm(item.get("county"))
        raw_township = gazetteer._norm(item.get("township"))
        if raw_county or raw_township:
            location_claims += 1
            county_valid = raw_county in gazetteer.county_codes
            township_valid = bool(raw_township) and (raw_county, raw_township) in gazetteer.township_codes
            if raw_county:
                county_claims += 1
                valid_counties += int(county_valid)
            if raw_township:
                township_claims += 1
                valid_townships += int(township_valid)
                downgraded_to_county += int(county_valid and not township_valid)
            if county_valid and (not raw_township or township_valid):
                valid_locations += 1
        if item.get("category") in CATEGORY_ENUM:
            field_valid["category"] += 1
        if isinstance(item.get("gis_relevance"), int) and 0 <= item["gis_relevance"] <= 3:
            field_valid["gis_relevance"] += 1
        if isinstance(item.get("severity"), int) and 0 <= item["severity"] <= 3:
            field_valid["severity"] += 1
        if isinstance(item.get("is_event"), bool):
            field_valid["is_event"] += 1
    result: dict[str, Any] = {
        "expected": len(rows), "returned_objects": len(annotations), "unique_valid_idx": len(valid),
        "missing_idx": missing, "duplicate_idx": duplicates, "invalid_idx": invalid_idx,
        "json_complete": not missing and not duplicates and not invalid_idx and len(valid) == len(rows),
        "gazetteer": {
            "location_claims": location_claims,
            "valid_claims": valid_locations,
            "validation_rate": valid_locations / location_claims if location_claims else None,
            "county_claims": county_claims,
            "valid_counties": valid_counties,
            "township_claims": township_claims,
            "valid_townships": valid_townships,
            "downgraded_to_county": downgraded_to_county,
        },
        "field_consistency": {field: {"valid": value, "rate": value / len(rows) if rows else None}
                              for field, value in field_valid.items()},
    }
    if gold is not None:
        exact = 0
        fields = {field: 0 for field in ("county", "township", *VALID_FIELDS)}
        gold_rows = gold[:len(rows)]
        for idx, gold_row in enumerate(gold_rows):
            predicted = valid.get(idx)
            if not predicted:
                continue
            matches = [predicted.get(field) == gold_row.get(field) for field in fields]
            exact += int(all(matches))
            for field, matched in zip(fields, matches):
                fields[field] += int(matched)
        denominator = len(gold_rows)
        result["gold_accuracy"] = {"denominator": denominator,
                                   "exact": exact / denominator if denominator else None,
                                   "fields": {field: count / denominator if denominator else None
                                              for field, count in fields.items()}}
    return result


def _manual_review(rows: list[dict[str, Any]], annotations: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    by_idx = {item.get("idx"): item for item in annotations if isinstance(item.get("idx"), int)}
    return [{"idx": idx, "title": row["title"][:120], "prediction": by_idx.get(idx)}
            for idx, row in enumerate(rows[:limit])]


def run_benchmark(rows: list[dict[str, Any]], models: list[str], gazetteer: TownshipGazetteer,
                  api_key: str, timeout: int, max_cost_usd: float, concurrency: int,
                  gold: list[dict[str, Any]] | None = None, dry_run: bool = False,
                  structured: bool = False, disable_reasoning: bool = False,
                  max_tokens: int | None = None) -> dict[str, Any]:
    report: dict[str, Any] = {"input_count": len(rows), "models": [], "gold_supplied": gold is not None,
                              "note": ("Accuracy is omitted without gold labels; consistency is not correctness.")}
    if dry_run:
        report["dry_run"] = True
        report["planned_models"] = models
        return report
    spent = 0.0
    pending = list(models)
    # A budget is only enforceable between provider responses. Keep such runs sequential;
    # otherwise concurrent calls could start after the cap has been reached.
    workers = 1 if max_cost_usd >= 0 else max(1, concurrency)
    def evaluate(response: dict[str, Any]) -> dict[str, Any]:
        response["metrics"] = evaluate_annotations(rows, response["annotations"], gazetteer, gold)
        response["manual_review"] = _manual_review(rows, response["annotations"])
        return response
    if workers == 1:
        for model in pending:
            if max_cost_usd >= 0 and spent >= max_cost_usd:
                report["models"].append({"model": model, "skipped": "hard_budget_stop"})
                continue
            response = evaluate(_request_model(
                model, rows, gazetteer, api_key, timeout,
                structured=structured, disable_reasoning=disable_reasoning, max_tokens=max_tokens,
            ))
            cost = response.get("provider_cost_usd")
            if isinstance(cost, (int, float)):
                spent += float(cost)
            report["models"].append(response)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for response in executor.map(
                lambda model: _request_model(
                    model, rows, gazetteer, api_key, timeout,
                    structured=structured, disable_reasoning=disable_reasoning, max_tokens=max_tokens,
                ),
                pending,
            ):
                report["models"].append(evaluate(response))
    report["provider_cost_observed_usd"] = round(spent, 8)
    report["max_cost_usd"] = max_cost_usd
    return report


def _markdown(report: dict[str, Any]) -> str:
    lines = ["# News OpenRouter benchmark", "", report["note"], ""]
    for model in report.get("models", []):
        if model.get("skipped"):
            lines.append(f"## {model['model']}\n\nSkipped: {model['skipped']}\n")
            continue
        metrics = model["metrics"]
        lines.extend([f"## {model['model']}", "",
                      f"- JSON complete: {metrics['json_complete']}",
                      f"- Missing idx: {metrics['missing_idx']}",
                      f"- Duplicate idx: {metrics['duplicate_idx']}",
                      f"- Latency: {model['latency_seconds']}s", "",
                      "### Manual review (not ground truth)", "",
                      "| idx | title | prediction |", "|---:|---|---|"])
        for row in model["manual_review"]:
            lines.append(f"| {row['idx']} | {row['title']} | `{json.dumps(row['prediction'], ensure_ascii=False)}` |")
        if "gold_accuracy" in metrics:
            lines.append(f"\nGold exact accuracy: {metrics['gold_accuracy']['exact']}")
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-jsonl", type=Path)
    source.add_argument("--db-sample", action="store_true")
    parser.add_argument("--gold-jsonl", type=Path)
    parser.add_argument("--gazetteer-json", type=Path)
    parser.add_argument("--models", default="qwen/qwen3.7-flash")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-cost-usd", type=float, default=0.05, help="negative disables cap")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--structured", action="store_true", help="require strict JSON Schema output")
    parser.add_argument("--disable-reasoning", action="store_true")
    parser.add_argument("--max-tokens", type=int)
    parser.add_argument("--output-json", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--output-markdown", type=Path, default=DEFAULT_OUTPUT_MARKDOWN)
    args = parser.parse_args(argv)
    if args.limit < 1 or args.concurrency < 1 or args.max_cost_usd < -1 or (args.max_tokens is not None and args.max_tokens < 1):
        parser.error("limit/concurrency must be positive; max-cost-usd must be >= -1")
    rows = _load_jsonl(args.input_jsonl, args.limit) if args.input_jsonl else load_db_sample(args.limit)
    gold = _load_jsonl(args.gold_jsonl, args.limit) if args.gold_jsonl else None
    models = [model.strip() for model in args.models.split(",") if model.strip()]
    if not models:
        parser.error("--models must contain at least one model")
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not args.dry_run and not api_key:
        parser.error("OPENROUTER_API_KEY is required unless --dry-run")
    report = run_benchmark(rows, models, load_gazetteer(args.gazetteer_json), api_key, args.timeout,
                           args.max_cost_usd, args.concurrency, gold, args.dry_run,
                           args.structured, args.disable_reasoning, args.max_tokens)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output_markdown.write_text(_markdown(report), encoding="utf-8")
    print(f"wrote benchmark reports: {args.output_json} and {args.output_markdown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
