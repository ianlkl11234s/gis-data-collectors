#!/usr/bin/env python3
"""Capture replayable, pre-screen GDELT GKG metadata without calling an LLM.

The script is deliberately separate from ``GlobalEventsCollector``: it uses its
public GKG parser, but never builds candidates, writes Supabase, uploads S3, or
contacts OpenRouter.  A failed index/download/parse is recorded as an error and
returns a non-zero exit code; it is never represented as an empty slot.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

# Permit the documented ``python3 scripts/...`` invocation without requiring
# callers to set PYTHONPATH themselves.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

import config
from collectors.global_events import (
    GKGArtifact,
    parse_gkg_artifact,
    parse_master_index,
    selected_artifact_manifest,
)

UTC = timezone.utc
CAPTURE_VERSION = "global-events-history-shadow/v1"
TOKEN_ESTIMATE_BYTES_PER_TOKEN = 4


def atomic_json(path: Path, value: object) -> None:
    """Atomically replace mutable capture state or a completed slot artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temporary)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def parse_utc_slot(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y%m%d%H%M%S")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("slot must be UTC YYYYMMDDHHMMSS") from exc
    if parsed.minute % 15 or parsed.second:
        raise argparse.ArgumentTypeError("slot must be aligned to 15 minutes")
    return parsed.replace(tzinfo=UTC)


def slot_key(stream: str, slot: str) -> str:
    return f"{stream}/{slot}"


def utc_slot(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M%S")


def wall_now() -> str:
    return datetime.now(UTC).isoformat()


def json_sha256(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "slots": {}}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid checkpoint {path}: {exc}") from exc
    if loaded.get("version") != 1 or not isinstance(loaded.get("slots"), dict):
        raise RuntimeError(f"unsupported checkpoint format: {path}")
    return loaded


def index_urls() -> dict[str, str]:
    return {
        "standard": str(config.GLOBAL_EVENTS_STANDARD_INDEX),
        "translation": str(config.GLOBAL_EVENTS_TRANSLATION_INDEX),
    }


@dataclass
class CaptureResult:
    stream: str
    slot: str
    status: str
    input_count: int = 0
    unique_count: int = 0
    late_count: int = 0
    downloaded_bytes: int = 0
    metadata_bytes: int = 0
    wall_seconds: float = 0.0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "slot": self.slot,
            "status": self.status,
            "input_count": self.input_count,
            "unique_count": self.unique_count,
            "late_count": self.late_count,
            "downloaded_bytes": self.downloaded_bytes,
            "metadata_bytes": self.metadata_bytes,
            "wall_seconds": self.wall_seconds,
            "estimated_model_input_tokens": (self.metadata_bytes + 3)
            // TOKEN_ESTIMATE_BYTES_PER_TOKEN,
            "token_estimate_method": f"UTF-8 metadata bytes / {TOKEN_ESTIMATE_BYTES_PER_TOKEN}, rounded up",
            "error": self.error,
        }


class ShadowCapture:
    def __init__(self, output_dir: Path, session: requests.Session | None = None):
        self.output_dir = output_dir
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "mini-taiwan-pulse/global-events-history-shadow",
                "Accept": "text/plain, application/zip",
            }
        )
        self.checkpoint_path = output_dir / "checkpoint.json"

    def get_index(self, stream: str) -> list[GKGArtifact]:
        response = self.session.get(index_urls()[stream], timeout=(20, 90))
        response.raise_for_status()
        return parse_master_index(response.text, stream)

    def download(self, artifact: GKGArtifact) -> bytes:
        response = self.session.get(artifact.url, timeout=(20, 180))
        response.raise_for_status()
        payload = response.content
        if len(payload) != artifact.expected_bytes:
            raise ValueError(f"size mismatch: expected {artifact.expected_bytes}, got {len(payload)}")
        digest = hashlib.md5(payload).hexdigest()  # provider's published checksum
        if digest != artifact.expected_md5:
            raise ValueError("md5 mismatch against master index")
        return payload

    def _completed(self, checkpoint: dict[str, Any], artifact: GKGArtifact) -> bool:
        item = checkpoint["slots"].get(slot_key(artifact.stream, artifact.slot), {})
        artifact_path = self.output_dir / "slots" / artifact.stream / f"{artifact.slot}.json"
        return item.get("status") == "succeeded" and artifact_path.exists()

    def capture_artifact(self, artifact: GKGArtifact, checkpoint: dict[str, Any]) -> CaptureResult:
        key = slot_key(artifact.stream, artifact.slot)
        if self._completed(checkpoint, artifact):
            existing = checkpoint["slots"][key]
            return CaptureResult(
                artifact.stream,
                artifact.slot,
                "skipped_checkpoint",
                input_count=int(existing.get("input_count", 0)),
                unique_count=int(existing.get("unique_count", 0)),
                late_count=int(existing.get("late_count", 0)),
                downloaded_bytes=int(existing.get("downloaded_bytes", 0)),
                metadata_bytes=int(existing.get("metadata_bytes", 0)),
                wall_seconds=0.0,
            )
        started = time.monotonic()
        try:
            payload = self.download(artifact)
            records = parse_gkg_artifact(artifact, payload)
            # "late" means the article's own GKG timestamp is more than one
            # 15-minute slot earlier than this artifact's publication slot.
            slot_time = parse_utc_slot(artifact.slot)
            late_count = sum(
                datetime.fromisoformat(record["published_ts"]) < slot_time - timedelta(minutes=15)
                for record in records
            )
            metadata_bytes = len(
                json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
            )
            result = CaptureResult(
                artifact.stream,
                artifact.slot,
                "succeeded",
                input_count=len(records),
                unique_count=len({record["url_norm"] for record in records}),
                late_count=late_count,
                downloaded_bytes=len(payload),
                metadata_bytes=metadata_bytes,
            )
            result.wall_seconds = round(time.monotonic() - started, 3)
            slot_document = {
                "capture_version": CAPTURE_VERSION,
                "captured_at": wall_now(),
                "artifact": selected_artifact_manifest([artifact])[0],
                "download_sha256": hashlib.sha256(payload).hexdigest(),
                "stats": result.as_dict(),
                "records": records,
            }
            artifact_path = self.output_dir / "slots" / artifact.stream / f"{artifact.slot}.json"
            atomic_json(artifact_path, slot_document)
            checkpoint["slots"][key] = {
                **result.as_dict(),
                "status": "succeeded",
                "artifact_sha256": json_sha256(slot_document),
                "download_sha256": slot_document["download_sha256"],
                "finished_at": wall_now(),
            }
        except (requests.RequestException, OSError, ValueError, zipfile.BadZipFile) as exc:
            result = CaptureResult(artifact.stream, artifact.slot, "failed", error=f"{type(exc).__name__}: {exc}")
            result.wall_seconds = round(time.monotonic() - started, 3)
            checkpoint["slots"][key] = {
                **result.as_dict(),
                "finished_at": wall_now(),
            }
        else:
            checkpoint["slots"][key]["wall_seconds"] = result.wall_seconds
        checkpoint["updated_at"] = wall_now()
        atomic_json(self.checkpoint_path, checkpoint)
        return result

    def run(self, slots: Iterable[datetime], streams: Iterable[str]) -> dict[str, Any]:
        run_started = time.monotonic()
        run_started_at = wall_now()
        checkpoint = load_checkpoint(self.checkpoint_path)
        wanted = [utc_slot(slot) for slot in slots]
        indexes: dict[str, dict[str, GKGArtifact]] = {}
        index_errors: list[dict[str, str]] = []
        for stream in streams:
            try:
                indexes[stream] = {item.slot: item for item in self.get_index(stream)}
            except requests.RequestException as exc:
                index_errors.append({"stream": stream, "error": f"{type(exc).__name__}: {exc}"})
        results: list[CaptureResult] = []
        for stream in streams:
            if stream not in indexes:
                continue
            for slot in wanted:
                artifact = indexes[stream].get(slot)
                if artifact is None:
                    results.append(CaptureResult(stream, slot, "missing_from_index", error="requested slot absent from fetched master index"))
                    continue
                results.append(self.capture_artifact(artifact, checkpoint))
        source_manifest = selected_artifact_manifest(
            artifact
            for stream in indexes.values()
            for slot, artifact in stream.items()
            if slot in wanted
        )
        summary = {
            "capture_version": CAPTURE_VERSION,
            "started_at": run_started_at,
            "requested_slots": wanted,
            "streams": list(streams),
            "openrouter_api_key_present": bool(os.environ.get("OPENROUTER_API_KEY")),
            "index_errors": index_errors,
            "slots": [result.as_dict() for result in results],
            "source_manifest": source_manifest,
            "source_manifest_sha256": json_sha256(source_manifest),
        }
        summary["finished_at"] = wall_now()
        summary["wall_seconds"] = round(time.monotonic() - run_started, 3)
        summary["status"] = "succeeded" if not index_errors and all(
            item.status in {"succeeded", "skipped_checkpoint"} for item in results
        ) else "failed"
        summary["totals"] = {
            field: sum(getattr(item, field) for item in results)
            for field in ("input_count", "unique_count", "late_count", "downloaded_bytes", "metadata_bytes")
        }
        summary["totals"]["estimated_model_input_tokens"] = (
            summary["totals"]["metadata_bytes"] + 3
        ) // TOKEN_ESTIMATE_BYTES_PER_TOKEN
        atomic_json(
            self.output_dir / "runs" / f"run_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json",
            summary,
        )
        return summary


def requested_slots(args: argparse.Namespace, now: datetime) -> list[datetime]:
    if args.mode == "run-once":
        floor = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
        return [floor - timedelta(minutes=15)]
    end = args.end or now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    start = args.start or end - timedelta(hours=args.hours)
    if start >= end:
        raise ValueError("start must precede end")
    slots = []
    current = start
    while current < end:
        slots.append(current)
        current += timedelta(minutes=15)
    return slots


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("history", "run-once"), default="history")
    parser.add_argument("--output-dir", type=Path, default=Path("data/global_events_history_capture"))
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--start", type=parse_utc_slot)
    parser.add_argument("--end", type=parse_utc_slot)
    parser.add_argument("--streams", nargs="+", choices=("standard", "translation"), default=["standard", "translation"])
    args = parser.parse_args(argv)
    if args.hours <= 0 or args.hours > 168:
        parser.error("--hours must be between 1 and 168")
    if args.mode == "run-once" and (args.start or args.end):
        parser.error("--start/--end apply only to history mode")
    try:
        report = ShadowCapture(args.output_dir).run(requested_slots(args, datetime.now(UTC)), args.streams)
    except (RuntimeError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
