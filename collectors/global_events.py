"""Canonical GDELT global-events metadata collector.

Private Collector -> Qwen handoffs remain the immutable research input. After
that handoff succeeds, an allowlisted AI-candidate projection is written through
platform migration 397. This is not formal event/version publication; that
remains owned by the existing Publisher workflow. Disabled by default.

Only GKG metadata is retained: URL, title, source, themes, locations, people,
organisations and tone.  Article bodies are never downloaded or persisted.
"""

from __future__ import annotations

import hashlib
import html
import io
import json
import logging
import math
import os
import re
import tempfile
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

import config
from collectors.base import BaseCollector

logger = logging.getLogger(__name__)
UTC = timezone.utc
SCHEMA_VERSION = "global-events/compact-candidate-batch/v2"
PROFILE_VERSION = "gdelt-gkg-story-candidates-v4"
EXTRACTOR_PROFILE_VERSION = "gdelt-gkg-document-shadow-v1"
CANDIDATE_ID_VERSION = "global-events/content-family-windows/v2"
PRODUCER_NAME = "gdelt_gkg_story_candidates"
SOURCE_INDEXES = {
    "standard": "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
    "translation": "https://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
}
RECORD_ID_RE = re.compile(r"^\d{14}-[A-Za-z0-9]+$")
TITLE_RE = re.compile(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", re.I | re.S)
TRACKING_KEYS = {"fbclid", "gclid", "dclid", "ref", "referrer", "oc"}
IMPACT_PATTERNS = {
    "armed_conflict": re.compile(
        r"\b(?:airstrike|bombardment|drone strike|missile strike|invasion|ceasefire|military attack)\b",
        re.I,
    ),
    "state_trade_action": re.compile(
        r"\b(?:sanction|retaliatory tariff|trade war|tariff.{0,45}(?:goods|imports|exports))\b",
        re.I,
    ),
    "national_politics": re.compile(
        r"\b(?:coup|martial law|state of emergency|presidential election|general election|referendum|peace talks)\b",
        re.I,
    ),
    "major_disaster": re.compile(
        r"\b(?:earthquake|hurricane|typhoon|cyclone|volcanic eruption|landslide|major flood|wildfire|tsunami warning)\b",
        re.I,
    ),
    "public_health": re.compile(
        r"\b(?:outbreak|pandemic|epidemic|h5n1|bird flu|public health emergency)\b",
        re.I,
    ),
    "critical_infrastructure": re.compile(
        r"\b(?:cyber ?attack|nationwide blackout|shipping lane.{0,30}(?:blocked|closed))\b",
        re.I,
    ),
    "mass_casualty_or_disruption": re.compile(
        r"\b(?:[2-9]\d|[1-9]\d{2,}|dozens|hundreds|thousands)\s+(?:people\s+)?(?:killed|dead|missing|injured|evacuated|displaced)\b",
        re.I,
    ),
}
NOISE_PATTERNS = {
    "obituary": re.compile(
        r"\b(?:obituary|dies at \d+|died at \d+|tributes paid to)\b", re.I
    ),
    "entertainment_or_sports": re.compile(
        r"\b(?:album|box office|football match|tour de france|us open)\b", re.I
    ),
}
OPENROUTER_429_MAX_ATTEMPTS = 2
OPENROUTER_RETRY_FALLBACK_SECONDS = 5.0
OPENROUTER_RETRY_CAP_SECONDS = 30.0
STAGE1_PROMPT_VERSION = "global-events-stage1/v3"
STAGE1_RUN_SCHEMA_VERSION = "global-events-stage1-shadow/v3"
TRADITIONALIZATION_POLICY_VERSION = "opencc-1.4.2-s2tw-single-pass-2026-09-03.1"


@dataclass(frozen=True)
class GKGArtifact:
    stream: str
    slot: str
    expected_bytes: int
    expected_md5: str
    url: str

    @property
    def source_id(self) -> str:
        return f"gdelt_gkg_{self.stream}"


class GKGIndexGap(RuntimeError):
    def __init__(self, message: str, completed: list[GKGArtifact]):
        super().__init__(message)
        self.completed = completed


class GKGArtifactUnavailable(RuntimeError):
    """An indexed artifact is temporarily unavailable (for example, 404)."""

    def __init__(self, artifact: GKGArtifact, status_code: int):
        super().__init__(
            f"{artifact.stream}:{artifact.slot} artifact unavailable (HTTP {status_code})"
        )
        self.artifact = artifact
        self.status_code = status_code


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def artifact_sha256(path: Path) -> str:
    """Hash the exact immutable JSON bytes handed to object storage."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_url(raw: str | None) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
        host = (parsed.hostname or "").lower().rstrip(".")
        if host.startswith("www."):
            host = host[4:]
        port = f":{parsed.port}" if parsed.port else ""
    except ValueError:
        return ""
    query = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in TRACKING_KEYS
    ]
    query.sort()
    return urlunsplit(
        (
            (
                "https"
                if parsed.scheme.lower() in {"http", "https"}
                else parsed.scheme.lower()
            ),
            host + port,
            parsed.path.rstrip("/") or "/",
            urlencode(query),
            "",
        )
    )


def source_domain(source: str, url: str) -> str:
    host = (urlsplit(url).hostname or source).lower().strip(".")
    return host[4:] if host.startswith("www.") else host


def story_title_fingerprint(title: str) -> str:
    cleaned = re.sub(r"^\s*world news\s*:\s*", "", title or "", flags=re.I)
    cleaned = re.sub(
        r"\s*(?:\||-|–|—)\s*(?:national news|national|news|world|region|xinhua)\s*$",
        "",
        cleaned,
        flags=re.I,
    )
    return " ".join(re.sub(r"[^\w]+", " ", html.unescape(cleaned)).casefold().split())


def _number(raw: str, integer: bool = False) -> float | int | None:
    try:
        return int(float(raw)) if integer else float(raw)
    except (TypeError, ValueError):
        return None


def parse_gkg_locations(raw: str) -> list[dict[str, Any]]:
    out = []
    for item in (raw or "").split(";"):
        if not item:
            continue
        parts = item.split("#") + [""] * 7
        out.append(
            {
                "location_type": _number(parts[0], True),
                "name": parts[1] or None,
                "country_code": parts[2] or None,
                "adm1_code": parts[3] or None,
                "latitude": _number(parts[4]),
                "longitude": _number(parts[5]),
                "feature_id": parts[6] or None,
            }
        )
    return out


def _names(raw: str) -> list[str]:
    values, seen = [], set()
    for item in (raw or "").split(";"):
        value = item.split(",", 1)[0].strip()
        if value and value not in seen:
            values.append(value)
            seen.add(value)
    return values


def parse_gkg_tone(raw: str) -> dict[str, Any]:
    parts = (raw or "").split(",") + [""] * 7
    return {
        "tone": _number(parts[0]),
        "positive_score": _number(parts[1]),
        "negative_score": _number(parts[2]),
        "polarity": _number(parts[3]),
        "activity_reference_density": _number(parts[4]),
        "self_group_reference_density": _number(parts[5]),
        "word_count": _number(parts[6], True),
    }


def parse_gdelt_ts(raw: str, fallback: str) -> str:
    value = (raw or fallback).strip()[:14]
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=UTC).isoformat()
    except ValueError:
        return (
            datetime.strptime(fallback, "%Y%m%d%H%M%S").replace(tzinfo=UTC).isoformat()
        )


def parse_master_index(text: str, stream: str) -> list[GKGArtifact]:
    suffix = ".translation.gkg.csv.zip" if stream == "translation" else ".gkg.csv.zip"
    artifacts = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[2].endswith(suffix):
            continue
        name = parts[2].rsplit("/", 1)[-1]
        slot = name[:14]
        if not slot.isdigit():
            continue
        try:
            size = int(parts[0])
        except ValueError:
            continue
        artifacts.append(
            GKGArtifact(
                stream,
                slot,
                size,
                parts[1].lower(),
                parts[2].replace("http://", "https://", 1),
            )
        )
    return sorted(artifacts, key=lambda item: item.slot)


def selected_artifact_manifest(
    artifacts: Iterable[GKGArtifact],
) -> list[dict[str, Any]]:
    """Return the stable lineage manifest for the files actually selected."""
    return [
        {
            "stream": artifact.stream,
            "slot": artifact.slot,
            "url": artifact.url,
            "expected_bytes": artifact.expected_bytes,
            "expected_md5": artifact.expected_md5,
        }
        for artifact in artifacts
    ]


def iter_logical_rows(lines: Iterable[str]) -> Iterable[list[str]]:
    current: list[str] | None = None
    for physical in lines:
        physical = physical.rstrip("\r\n")
        parts = physical.split("\t", 26)
        if len(parts) == 27 and RECORD_ID_RE.match(parts[0]):
            if current is not None:
                yield current
            current = parts
        elif current is not None:
            current[26] += "\n" + physical
    if current is not None:
        yield current


def parse_gkg_artifact(artifact: GKGArtifact, payload: bytes) -> list[dict[str, Any]]:
    records = []
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        if len(members) != 1:
            raise ValueError(
                f"{artifact.stream}:{artifact.slot} ZIP must contain one member"
            )
        with archive.open(members[0]) as binary:
            text = io.TextIOWrapper(
                binary, encoding="utf-8", errors="replace", newline=""
            )
            for row in iter_logical_rows(text):
                if len(row) != 27:
                    continue
                title_match = TITLE_RE.search(row[26] or "")
                title = (
                    re.sub(r"\s+", " ", html.unescape(title_match.group(1))).strip()
                    if title_match
                    else ""
                )
                url = canonical_url(row[4])
                if not url or not title:
                    continue
                impact = sorted(
                    name
                    for name, pattern in IMPACT_PATTERNS.items()
                    if pattern.search(title)
                )
                noise = sorted(
                    name
                    for name, pattern in NOISE_PATTERNS.items()
                    if pattern.search(title)
                )
                records.append(
                    {
                        "source_id": artifact.source_id,
                        "source_stream": artifact.stream,
                        "source_name": row[3].strip() or source_domain(row[3], url),
                        "source_domain": source_domain(row[3], url),
                        "source_language": (
                            "en" if artifact.stream == "standard" else None
                        ),
                        "url": row[4].strip(),
                        "url_norm": url,
                        "title": title[:500],
                        "published_ts": parse_gdelt_ts(row[1], artifact.slot),
                        "gkg_record_id": row[0],
                        "gkg_slot": artifact.slot,
                        "gkg_themes": _names(row[7]),
                        "gkg_locations": parse_gkg_locations(row[9]),
                        "gkg_persons": _names(row[11]),
                        "gkg_organizations": _names(row[13]),
                        "gkg_tone": parse_gkg_tone(row[15]),
                        "impact_signals": impact,
                        "noise_signals": noise,
                    }
                )
    return records


def _candidate_id(fingerprint: str, first_slot: str, last_slot: str) -> str:
    identity = {
        "candidate_id_version": CANDIDATE_ID_VERSION,
        "content_families": [
            {
                "fingerprint": fingerprint,
                "first_slot": first_slot,
                "last_slot": last_slot,
            }
        ],
    }
    return f"cand_{content_sha256(identity)[:24]}"


def build_compact_batch(
    groups: list[dict[str, Any]],
    *,
    source_manifest_sha256: str,
    source_registry_sha256: str | None,
    producer_git_commit: str,
) -> dict[str, Any]:
    if not re.fullmatch(r"[a-f0-9]{64}", source_manifest_sha256):
        raise ValueError(
            "source_manifest_sha256 must be a 64-character lower-case SHA-256"
        )
    if source_registry_sha256 is not None and not re.fullmatch(
        r"[a-f0-9]{64}", source_registry_sha256
    ):
        raise ValueError(
            "source_registry_sha256 must be a 64-character lower-case SHA-256"
        )
    if not re.fullmatch(r"[a-f0-9]{40}", producer_git_commit):
        raise ValueError(
            "production handoff requires a full 40-character producer Git SHA"
        )
    selected = []
    for rank, group in enumerate(
        sorted(
            groups,
            key=lambda item: (
                group_signal_priority(item["impact_signals"]),
                len(item["domains"]),
                item["fingerprint"],
            ),
            reverse=True,
        ),
        1,
    ):
        representatives = []
        location_evidence = []
        seen_representatives = set()
        for row in group["rows"]:
            key = (row["title"], row["url"], row["source_domain"])
            if key in seen_representatives:
                continue
            seen_representatives.add(key)
            representatives.append(
                {
                    "title": row["title"],
                    "url": row["url"],
                    "source_domain": row["source_domain"],
                    "selection_reason": "content_family_representative",
                }
            )
            for location in row.get("gkg_locations", []):
                evidence = {
                    **location,
                    "country_code_scheme": "fips10",
                    "source_url": row["url"],
                    "source_kind": "gdelt_metadata_mention",
                }
                for coordinate, bound in (("latitude", 90), ("longitude", 180)):
                    value = evidence[coordinate]
                    if (
                        type(value) not in (int, float)
                        or not math.isfinite(value)
                        or not -bound <= value <= bound
                    ):
                        evidence[coordinate] = None
                evidence["evidence_id"] = f"loc_{content_sha256(evidence)[:24]}"
                if evidence not in location_evidence and len(location_evidence) < 30:
                    location_evidence.append(evidence)
            if len(representatives) == 5:
                break
        selected.append(
            {
                "candidate_id": _candidate_id(
                    group["fingerprint"], group["first_slot"], group["last_slot"]
                ),
                "routing_rank": rank,
                "rule_tier": (
                    "A_candidate" if len(group["domains"]) >= 5 else "B_broad_signal"
                ),
                "possible_relation_group": None,
                "observation_window": {
                    "first_slot": group["first_slot"],
                    "last_slot": group["last_slot"],
                },
                "coverage": {
                    "content_families": 1,
                    "documents": group["documents"],
                    "raw_domains": len(group["domains"]),
                    "confirmed_editorial_groups": 0,
                    "known_editorial_groups": 0,
                    "unknown_source_groups": len(group["domains"]),
                    "distributor_groups": 0,
                },
                "routing_evidence": {
                    "streams": sorted(group["streams"]),
                    "categories": sorted(group["categories"]),
                    "impact_signals": sorted(group["impact_signals"]),
                    "noise_signals": sorted(group["noise_signals"]),
                },
                "representative_documents": representatives,
                # These are mentions, not confirmed occurrence coordinates.
                "location_evidence": location_evidence,
            }
        )
    slots = [
        slot
        for group in selected
        for slot in (
            group["observation_window"]["first_slot"],
            group["observation_window"]["last_slot"],
        )
    ]
    payload = {
        "producer": {
            "name": PRODUCER_NAME,
            "profile_version": PROFILE_VERSION,
            "extractor_profile_version": EXTRACTOR_PROFILE_VERSION,
            "candidate_id_version": CANDIDATE_ID_VERSION,
            "git_commit": producer_git_commit,
        },
        "observation_window": {
            "first_slot": min(slots) if slots else None,
            "last_slot": max(slots) if slots else None,
        },
        "source_manifest_sha256": source_manifest_sha256,
        "source_registry_sha256": source_registry_sha256,
        "candidate_count": len(selected),
        "candidates": selected,
    }
    digest = content_sha256({"schema_version": SCHEMA_VERSION, "payload": payload})
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": f"batch_{digest[:24]}",
        "content_sha256": digest,
        "payload": payload,
    }


def candidate_batch_slice(
    batch: dict[str, Any], candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    """Keep the existing compact contract while draining a source window."""
    selected = [
        {**candidate, "routing_rank": rank}
        for rank, candidate in enumerate(candidates, 1)
    ]
    slots = [
        slot
        for candidate in selected
        for slot in candidate["observation_window"].values()
    ]
    payload = {
        **batch["payload"],
        "candidates": selected,
        "candidate_count": len(selected),
        "observation_window": {
            "first_slot": min(slots) if slots else None,
            "last_slot": max(slots) if slots else None,
        },
    }
    digest = content_sha256({"schema_version": SCHEMA_VERSION, "payload": payload})
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": f"batch_{digest[:24]}",
        "content_sha256": digest,
        "payload": payload,
    }


def candidate_display_records(
    batch: dict[str, Any],
    stage1: dict[str, Any],
    assessed_at: str,
    assessment_times: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Explicit public allowlist: AI assessments never become formal publications."""
    assessments = {item["candidate_id"]: item for item in stage1["assessments"]}
    records = []
    for candidate in batch["payload"]["candidates"]:
        documents = candidate["representative_documents"]
        # Never truncate a URL into a different document. Oversized URLs and
        # the full model response remain in the immutable private handoff;
        # an all-oversized candidate is visible as unlocated/pending publicly.
        source_urls = list(
            dict.fromkeys(
                document["url"]
                for document in documents
                if len(document["url"]) <= 8192
            )
        )
        assessment = (
            assessments.get(candidate["candidate_id"], {}) if source_urls else {}
        )
        evidence_by_id = {
            item["evidence_id"]: item for item in candidate.get("location_evidence", [])
        }
        places = []
        seen_places = set()
        selected_place_found = False
        location_selections = [
            (selection, False)
            for selection in assessment.get("location_evidence_ids", [])
        ] + [
            (
                {
                    "evidence_id": evidence_id,
                    "basis": "來源新聞的地理提及，僅供概略定位；未確認為精確發生地。",
                },
                True,
            )
            for evidence_id in evidence_by_id
        ]
        for selection, is_fallback in location_selections:
            # Prefer usable model-selected evidence. Without one, source metadata
            # may supply explicitly approximate related places, never invented
            # coordinates or a fabricated quotation from the headline.
            if (is_fallback and selected_place_found) or len(places) >= 20:
                break
            evidence = evidence_by_id[selection["evidence_id"]]
            if evidence["source_url"] not in source_urls:
                continue
            if (
                not isinstance(evidence.get("name"), str)
                or not evidence["name"].strip()
            ):
                continue
            # GKG country/city/landmark coordinates are representative only.
            # State/ADM1 mentions stay unlocated rather than inventing a city.
            kind = {1: "country_center", 3: "city_center", 4: "city_center"}.get(
                evidence.get("location_type")
            )
            longitude, latitude = evidence.get("longitude"), evidence.get("latitude")
            if kind is None or not all(
                type(value) in (int, float) and math.isfinite(value)
                for value in (longitude, latitude)
            ):
                continue
            if not -180 <= longitude <= 180 or not -90 <= latitude <= 90:
                continue
            place_identity = (
                kind,
                evidence.get("country_code"),
                evidence.get("name"),
                longitude,
                latitude,
            )
            if place_identity in seen_places:
                continue
            seen_places.add(place_identity)
            selected_place_found = selected_place_found or not is_fallback
            lineage_id = (
                f"metadata_fallback_{evidence['evidence_id']}"
                if is_fallback
                else evidence["evidence_id"]
            )
            places.append(
                {
                    "place_key": f"place_{content_sha256(place_identity)[:24]}",
                    "name": evidence["name"],
                    "country_code": evidence.get("country_code"),
                    "country_code_scheme": "fips10",
                    "location_kind": kind,
                    "longitude": longitude,
                    "latitude": latitude,
                    "evidence_url": evidence["source_url"],
                    "location_lineage": f"{kind}:gdelt:{lineage_id}",
                    "evidence_basis": selection["basis"],
                    "source_kind": "gdelt_metadata_mention",
                }
            )
        record = {
            "candidate_id": candidate["candidate_id"],
            "observed_at": parse_gdelt_ts(
                candidate["observation_window"]["first_slot"], ""
            ),
            "assessed_at": (
                (assessment_times or {}).get(candidate["candidate_id"], assessed_at)
                if assessment
                else None
            ),
            "assessment_status": "assessed" if assessment else "pending",
            "ai_group_id": (
                f"aigroup_{content_sha256([batch['batch_id'], assessment['event_group']])[:24]}"
                if assessment
                else None
            ),
            "source_urls": source_urls,
            # Public preview only; the full title remains in the private batch.
            "source_headline": documents[0]["title"][:500] if documents else None,
            "places": places,
        }
        for field in (
            "title_zh_tw",
            "summary_zh_tw",
            "category",
            "severity",
            "decision",
            "taiwan_relationship",
            "taiwan_impact_zh_tw",
            "confidence",
            "reason_zh_tw",
        ):
            record[field] = assessment.get(field)
        records.append(record)
    return records


def group_signal_priority(signals: Iterable[str]) -> int:
    return max(
        (
            {
                "armed_conflict": 7,
                "national_politics": 6,
                "state_trade_action": 5,
                "major_disaster": 5,
                "public_health": 5,
                "critical_infrastructure": 5,
                "mass_casualty_or_disruption": 3,
            }.get(signal, 0)
            for signal in signals
        ),
        default=0,
    )


def openrouter_retry_delay(response: requests.Response) -> float:
    raw = (getattr(response, "headers", None) or {}).get("Retry-After")
    try:
        delay = float(raw)
    except (TypeError, ValueError):
        return OPENROUTER_RETRY_FALLBACK_SECONDS
    if delay < 0 or delay != delay:
        return OPENROUTER_RETRY_FALLBACK_SECONDS
    return min(delay, OPENROUTER_RETRY_CAP_SECONDS)


def validate_stage1(
    result: Any,
    candidates: list[dict[str, Any]],
    normalization_lineage: list[dict[str, Any]] | None = None,
    validation_rejections: list[dict[str, Any]] | None = None,
    validation_diagnostics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if (
        not isinstance(result, dict)
        or set(result) != {"assessments"}
        or not isinstance(result["assessments"], list)
    ):
        raise ValueError("Stage1 output must contain only assessments")
    expected = {item["routing_rank"]: item["candidate_id"] for item in candidates}
    candidates_by_id = {item["candidate_id"]: item for item in candidates}
    seen = set()
    valid_assessments = []
    required = {
        "candidate_id",
        "candidate_rank",
        "decision",
        "event_group",
        "title_zh_tw",
        "summary_zh_tw",
        "category",
        "severity",
        "severity_source",
        "taiwan_relationship",
        "taiwan_impact_zh_tw",
        "confidence",
        "reason_zh_tw",
    }
    enums = {
        "decision": {"keep_core", "keep_watch", "drop_noise"},
        "category": {
            "accident",
            "crime",
            "disaster",
            "traffic",
            "health",
            "policy",
            "other",
        },
        "taiwan_relationship": {"direct", "indirect", "none", "unknown"},
    }
    try:
        from opencc import OpenCC

        converter = OpenCC("s2tw.json")
    except ImportError as exc:
        raise RuntimeError(
            "OpenCC is required for the Stage1 Traditional Chinese gate"
        ) from exc

    def reject(
        item: Any,
        message: str,
        *,
        candidate_id: str | None = None,
        candidate_rank: int | None = None,
        field: str | None = None,
    ) -> None:
        if validation_rejections is None:
            raise ValueError(message)
        field_match = re.search(r"Stage1 ([a-z_]+)", message)
        validation_rejections.append(
            {
                "candidate_id": candidate_id
                or (item.get("candidate_id") if isinstance(item, dict) else None),
                "candidate_rank": (
                    candidate_rank
                    if candidate_rank is not None
                    else (
                        item.get("candidate_rank") if isinstance(item, dict) else None
                    )
                ),
                "error_code": "invalid_assessment",
                "field": field or (field_match.group(1) if field_match else None),
                "error": message,
            }
        )

    def diagnose(item: Any, message: str) -> None:
        if validation_diagnostics is None:
            raise ValueError(message)
        validation_diagnostics.append(
            {
                "reported_candidate_rank": (
                    item.get("candidate_rank") if isinstance(item, dict) else None
                ),
                "error_code": "unmapped_stage1_output",
                "error": message,
            }
        )

    for item in result["assessments"]:
        if not isinstance(item, dict):
            diagnose(item, "Stage1 assessment must be an object")
            continue
        rank = item.get("candidate_rank")
        if type(rank) is not int or rank not in expected or rank in seen:
            diagnose(item, "Stage1 candidate rank/id lineage mismatch")
            continue
        seen.add(rank)
        if item.get("candidate_id") != expected[rank]:
            reject(
                item,
                "Stage1 candidate rank/id lineage mismatch",
                candidate_id=expected[rank],
                candidate_rank=rank,
                field="candidate_id",
            )
            continue
        try:
            if set(item) - {"location_evidence_ids"} != required:
                raise ValueError("Stage1 assessment schema mismatch")
            selections = item.get("location_evidence_ids", [])
            if not isinstance(selections, list) or len(selections) > 8:
                raise ValueError("invalid Stage1 location_evidence_ids")
            candidate = candidates_by_id[item["candidate_id"]]
            evidence_by_id = {
                evidence["evidence_id"]: evidence
                for evidence in candidate.get("location_evidence", [])
            }
            selected_ids = set()
            for selection in selections:
                if not isinstance(selection, dict) or set(selection) != {
                    "evidence_id",
                    "role",
                    "basis",
                }:
                    raise ValueError("invalid Stage1 location_evidence_ids schema")
                evidence = evidence_by_id.get(selection["evidence_id"])
                if selection["evidence_id"] in selected_ids:
                    raise ValueError("invalid Stage1 location_evidence_ids duplicate")
                selected_ids.add(selection["evidence_id"])
                basis = selection["basis"]
                if (
                    evidence is None
                    or selection["role"] not in {"event_location", "affected_area"}
                    or not isinstance(basis, str)
                    or not 2 <= len(basis.strip()) <= 500
                    or not any(
                        basis in document["title"]
                        and document["url"] == evidence["source_url"]
                        for document in candidate["representative_documents"]
                    )
                ):
                    raise ValueError(
                        "invalid Stage1 location_evidence_ids source/basis"
                    )
            event_group = item["event_group"]
            if not isinstance(event_group, str) or not re.fullmatch(
                r"E\d{3,}", event_group
            ):
                raise ValueError("invalid Stage1 event_group")
            for field, allowed in enums.items():
                if item[field] not in allowed:
                    raise ValueError(f"invalid Stage1 {field}")
            if (
                item["severity_source"] != "inferred"
                or type(item["severity"]) is not int
                or not 0 <= item["severity"] <= 3
                or isinstance(item["confidence"], bool)
                or not isinstance(item["confidence"], (int, float))
                or not 0 <= item["confidence"] <= 1
            ):
                raise ValueError("invalid inferred severity/confidence")
            for field in (
                "title_zh_tw",
                "summary_zh_tw",
                "taiwan_impact_zh_tw",
                "reason_zh_tw",
            ):
                raw_value = item[field]
                if not isinstance(raw_value, str) or not raw_value.strip():
                    raise ValueError(f"Stage1 {field} is blank")
                value = raw_value.strip()
                # Use the official OpenCC s2tw character/variant policy once.
                # s2twp phrase dictionaries and repeated conversions can alter
                # already-canonical wording, so neither belongs in this gate.
                normalized = converter.convert(value).strip()
                if not normalized:
                    raise ValueError(f"Stage1 {field} is blank after normalization")
                if (
                    len(normalized)
                    > {
                        "title_zh_tw": 500,
                        "summary_zh_tw": 3000,
                        "taiwan_impact_zh_tw": 2000,
                        "reason_zh_tw": 2000,
                    }[field]
                ):
                    raise ValueError(f"Stage1 {field} exceeds display contract length")
                if normalized != value and normalization_lineage is not None:
                    normalization_lineage.append(
                        {"candidate_id": item["candidate_id"], "field": field}
                    )
                item[field] = normalized
        except (KeyError, TypeError, ValueError) as exc:
            reject(item, str(exc))
            continue
        valid_assessments.append(item)
    if seen != set(expected):
        if validation_rejections is None:
            raise ValueError("Stage1 omitted candidates")
        for rank in sorted(set(expected) - seen):
            validation_rejections.append(
                {
                    "candidate_id": expected[rank],
                    "candidate_rank": rank,
                    "error_code": "omitted_candidate",
                    "field": None,
                    "error": "Stage1 omitted candidate",
                }
            )
    if validation_rejections is not None and (
        len(valid_assessments) + len(validation_rejections) != len(candidates)
    ):
        raise ValueError("Stage1 candidate validation reconciliation mismatch")
    result["assessments"] = sorted(
        valid_assessments, key=lambda item: item["candidate_rank"]
    )
    return result


def immutable_write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            if path.read_bytes() != encoded:
                raise RuntimeError(f"immutable artifact collision: {path}")
    finally:
        tmp.unlink(missing_ok=True)


def atomic_replace_json(path: Path, value: object) -> None:
    """Write mutable operational state without the immutable-artifact gate."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


class GlobalEventsCollector(BaseCollector):
    name = "global_events"
    interval_minutes = getattr(config, "GLOBAL_EVENTS_INTERVAL", 60)
    COLLECT_TIMEOUT = 1800

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "mini-taiwan-pulse/global-events-metadata-collector",
                "Accept": "text/plain, application/zip",
            }
        )
        self._pending_checkpoints: dict[str, str] = {}
        self._supabase_receipts: list[dict[str, Any]] = []
        self._raw_response_sha256: str | None = None
        self._stage1_usage: dict[str, Any] = {}
        self._stage1_observation: dict[str, Any] = {}
        self._normalization_lineage: list[dict[str, Any]] = []
        self._pending_routing_work: dict[str, Any] | None = None
        self._assessment_times: dict[str, str] = {}

    @property
    def checkpoint_path(self) -> Path:
        return Path(config.LOCAL_DATA_DIR) / self.name / "checkpoint.json"

    @property
    def routing_pending_path(self) -> Path:
        return Path(config.LOCAL_DATA_DIR) / self.name / "routing_pending.json"

    @property
    def source_registry_path(self) -> Path:
        return (
            Path(__file__).resolve().parents[1]
            / "config"
            / "global_events_source_registry.yaml"
        )

    def require_db_write(self) -> bool:
        # Enabling the production collector always requires the migration-389
        # receipt writer. This fails closed when the DB toggle/credentials are
        # absent instead of advancing a cursor without an immutable receipt.
        return bool(getattr(config, "GLOBAL_EVENTS_ENABLED", False))

    def _load_checkpoints(self) -> dict[str, str]:
        try:
            payload = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            streams = payload.get("streams", {})
            return {
                stream: slot
                for stream, slot in streams.items()
                if stream in SOURCE_INDEXES
                and isinstance(slot, str)
                and re.fullmatch(r"\d{14}", slot)
            }
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid global_events checkpoint: {exc}") from exc

    def _save_checkpoints(self, updates: dict[str, str]) -> None:
        if not updates:
            return
        current = self._load_checkpoints()
        current.update({k: v for k, v in updates.items() if v > current.get(k, "")})
        atomic_replace_json(
            self.checkpoint_path,
            {
                "version": 1,
                "streams": current,
                "updated_at": datetime.now(UTC).isoformat(),
                "status": "succeeded",
            },
        )

    def _select_pending(
        self, artifacts: list[GKGArtifact], checkpoint: str | None
    ) -> list[GKGArtifact]:
        max_files = max(
            1, int(getattr(config, "GLOBAL_EVENTS_MAX_FILES_PER_STREAM", 8))
        )
        if not artifacts:
            return []
        if not checkpoint:
            return artifacts[
                -max(1, int(getattr(config, "GLOBAL_EVENTS_INITIAL_SLOTS", 4))) :
            ]
        by_slot = {item.slot: item for item in artifacts}
        cursor = datetime.strptime(checkpoint, "%Y%m%d%H%M%S")
        selected = []
        for _ in range(max_files):
            cursor += timedelta(minutes=15)
            slot = cursor.strftime("%Y%m%d%H%M%S")
            if slot not in by_slot:
                if slot <= artifacts[-1].slot:
                    raise GKGIndexGap(
                        f"{artifacts[0].stream} index gap at {slot}", selected
                    )
                break
            selected.append(by_slot[slot])
        return selected

    def _download_artifact(self, artifact: GKGArtifact) -> bytes:
        response = self._session.get(
            artifact.url, timeout=(20, max(60, config.REQUEST_TIMEOUT))
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code == 404:
                # GDELT can publish the index row before the ZIP is available.
                # Do not parse partial data or advance this stream cursor; the
                # next scheduled run will retry, while this remains visible as
                # an artifact-unavailable error (not an index gap).
                raise GKGArtifactUnavailable(artifact, status_code) from exc
            raise
        payload = response.content
        if len(payload) != artifact.expected_bytes:
            raise ValueError(f"{artifact.stream}:{artifact.slot} size mismatch")
        if (
            hashlib.md5(payload).hexdigest() != artifact.expected_md5
        ):  # noqa: S324 - provider checksum
            raise ValueError(f"{artifact.stream}:{artifact.slot} md5 mismatch")
        return payload

    def _save_raw(
        self, artifact: GKGArtifact, payload: bytes, *, success: bool
    ) -> Path:
        now = datetime.now(UTC)
        root = (
            Path(config.LOCAL_DATA_DIR) / self.name / "raw" / now.strftime("%Y/%m/%d")
        )
        digest = hashlib.sha256(payload).hexdigest()
        path = root / f"{artifact.stream}_{artifact.slot}_{digest[:24]}.zip"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(payload)
        if success:
            path.with_suffix(".success").touch()
        return path

    def _cleanup_success_raw(self) -> None:
        cutoff = (
            datetime.now(UTC).timestamp()
            - max(24, int(getattr(config, "GLOBAL_EVENTS_RAW_RETENTION_HOURS", 72)))
            * 3600
        )
        root = Path(config.LOCAL_DATA_DIR) / self.name / "raw"
        for path in root.glob("**/*.zip") if root.exists() else []:
            marker = path.with_suffix(".success")
            if marker.exists() and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
                marker.unlink(missing_ok=True)

    def _request_stage1(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        key = os.environ.get("OPENROUTER_API_KEY", "").strip()
        if not key:
            raise RuntimeError(
                "OPENROUTER_API_KEY is required when global_events has candidates"
            )
        model = getattr(config, "GLOBAL_EVENTS_QWEN_MODEL", "qwen/qwen3.7-flash")
        prompt = [
            {
                "candidate_rank": c["routing_rank"],
                "candidate_id": c["candidate_id"],
                "titles": [d["title"] for d in c["representative_documents"]],
                "urls": [d["url"] for d in c["representative_documents"]],
                "coverage": c["coverage"],
                "impact_signals": c["routing_evidence"]["impact_signals"],
                "location_evidence": c.get("location_evidence", []),
            }
            for c in candidates
        ]
        output_contract = {
            "top_level_only": ["assessments"],
            "assessment_count": len(candidates),
            "assessment_additional_properties": False,
            "assessment_required_fields": [
                "candidate_id",
                "candidate_rank",
                "decision",
                "event_group",
                "title_zh_tw",
                "summary_zh_tw",
                "category",
                "severity",
                "severity_source",
                "taiwan_relationship",
                "taiwan_impact_zh_tw",
                "confidence",
                "reason_zh_tw",
            ],
            "assessment_optional_fields": {
                "location_evidence_ids": {
                    "type": "array",
                    "max_items": 8,
                    "item_fields": {
                        "evidence_id": "copy an input evidence_id",
                        "role": "event_location|affected_area",
                        "basis": "exact substring from that evidence source URL's input title",
                    },
                },
            },
            "decision_enum": ["keep_core", "keep_watch", "drop_noise"],
            "category_enum": [
                "accident",
                "crime",
                "disaster",
                "traffic",
                "health",
                "policy",
                "other",
            ],
            "taiwan_relationship_enum": ["direct", "indirect", "none", "unknown"],
            "severity_source": "inferred",
            "severity_range": [0, 3],
            "confidence_range": [0, 1],
            "event_group_pattern": "^E\\d{3,}$",
            "traditional_chinese_required": True,
            "traditional_chinese_locale": (
                "zh-TW; use 臺灣 terms and avoid Simplified Chinese"
            ),
        }
        request_kwargs = {
            "headers": {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            "json": {
                "model": model,
                "temperature": 0,
                "max_tokens": min(
                    8192,
                    max(
                        256,
                        int(
                            getattr(
                                config, "GLOBAL_EVENTS_QWEN_MAX_OUTPUT_TOKENS", 8192
                            )
                        ),
                    ),
                ),
                "response_format": {"type": "json_object"},
                # Qwen3.7 Flash exposes JSON mode but not JSON-schema
                # enforcement. Disable its default reasoning so the bounded
                # response budget is reserved for the JSON object; local
                # validate_stage1 remains the final strict gate.
                "reasoning": {"effort": "none"},
                "messages": [
                    {
                        "role": "system",
                        "content": (
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
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {"candidates": prompt, "output_contract": output_contract},
                            ensure_ascii=False,
                        ),
                    },
                ],
            },
            "timeout": max(30, int(getattr(config, "GLOBAL_EVENTS_QWEN_TIMEOUT", 90))),
        }
        for attempt in range(OPENROUTER_429_MAX_ATTEMPTS):
            response = self._session.post(
                "https://openrouter.ai/api/v1/chat/completions", **request_kwargs
            )
            try:
                response.raise_for_status()
                break
            except requests.HTTPError as exc:
                error_response = exc.response if exc.response is not None else response
                if (
                    getattr(error_response, "status_code", None) != 429
                    or attempt + 1 >= OPENROUTER_429_MAX_ATTEMPTS
                ):
                    raise
                delay = openrouter_retry_delay(error_response)
                logger.warning(
                    "OpenRouter Stage1 HTTP 429; retrying attempt %d/%d in %.1fs",
                    attempt + 2,
                    OPENROUTER_429_MAX_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)
        payload = response.json()
        usage = payload.get("usage") or {}
        completion_details = usage.get("completion_tokens_details")
        reasoning_units = usage.get("reasoning_tokens")
        if reasoning_units is None and isinstance(completion_details, dict):
            reasoning_units = completion_details.get("reasoning_tokens")
        self._stage1_usage = {
            "input_units": usage.get("prompt_tokens", usage.get("input_tokens")),
            "output_units": usage.get("completion_tokens", usage.get("output_tokens")),
            "reasoning_units": reasoning_units,
            "cost_usd": usage.get("cost"),
        }
        choices = payload.get("choices")
        choice = choices[0] if isinstance(choices, list) and choices else {}
        message = choice.get("message") if isinstance(choice, dict) else {}
        message = message if isinstance(message, dict) else {}
        finish_reason = (
            choice.get("finish_reason") if isinstance(choice, dict) else None
        )
        content = message.get("content")
        content_length = len(content) if isinstance(content, str) else None
        self._stage1_observation = {
            "finish_reason": finish_reason if isinstance(finish_reason, str) else None,
            "content_length": content_length,
            "usage": self._stage1_usage,
        }
        cost = usage.get("cost")
        if cost is not None and float(cost) > float(
            getattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)
        ):
            logger.warning(
                "Qwen Stage1 usage cost exceeded post-hoc alert threshold: %.6f",
                float(cost),
            )
        if isinstance(content, str):
            # Preserve only a digest of the provider bytes even when the
            # response is truncated or malformed; the raw content never
            # enters a receipt or exception message.
            self._raw_response_sha256 = hashlib.sha256(
                content.encode("utf-8")
            ).hexdigest()
        if finish_reason != "stop":
            raise ValueError(
                "OpenRouter Stage1 incomplete response: "
                f"finish_reason={finish_reason!r}, content_length={content_length}"
            )
        if not isinstance(content, str) or not content.strip():
            raise ValueError("OpenRouter message content must be a non-empty string")
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(), flags=re.I)
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "OpenRouter Stage1 JSON decode failed: "
                f"finish_reason={finish_reason!r}, content_length={content_length}, "
                f"line={exc.lineno}, column={exc.colno}, char={exc.pos}"
            ) from exc

    def _assess_in_chunks(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        """Bound output size and reuse completed chunks after provider failures."""
        chunk_size = min(
            10, max(1, int(getattr(config, "GLOBAL_EVENTS_QWEN_CHUNK_SIZE", 10)))
        )
        results = []
        observations = []
        response_hashes = []
        totals: dict[str, Any] = {}
        next_group = 1
        for offset in range(0, len(candidates), chunk_size):
            chunk = candidates[offset : offset + chunk_size]
            cache_key = content_sha256(
                {
                    "model": getattr(
                        config, "GLOBAL_EVENTS_QWEN_MODEL", "qwen/qwen3.7-flash"
                    ),
                    "prompt_version": STAGE1_PROMPT_VERSION,
                    "candidates": chunk,
                }
            )
            cache_path = (
                Path(config.LOCAL_DATA_DIR)
                / self.name
                / "stage1_cache"
                / f"{cache_key}.json"
            )
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                self._stage1_usage = {}
                self._stage1_observation = {}
                self._raw_response_sha256 = None
                try:
                    raw_result = self._request_stage1(chunk)
                    if (
                        not isinstance(raw_result, dict)
                        or set(raw_result) != {"assessments"}
                        or not isinstance(raw_result["assessments"], list)
                    ):
                        raise ValueError("Stage1 output must contain only assessments")
                except Exception:
                    # Preserve all earlier successful chunks and current error evidence.
                    if observations:
                        self._stage1_observation = {
                            "chunks": observations + [self._stage1_observation],
                            "completed_chunks": len(observations),
                        }
                    raise
                cached = {
                    "result": raw_result,
                    "raw_response_sha256": self._raw_response_sha256,
                    "observation": self._stage1_observation,
                    "usage": self._stage1_usage,
                    "assessed_at": datetime.now(UTC).isoformat(),
                }
                # Only complete JSON provider outputs are cached. The strict
                # candidate validator still runs for cached responses too.
                immutable_write(cache_path, cached)
                cache_hit = False
            else:
                cache_hit = True
            rejected: list[dict[str, Any]] = []
            diagnostics: list[dict[str, Any]] = []
            validated = validate_stage1(
                cached["result"],
                chunk,
                self._normalization_lineage,
                rejected,
                diagnostics,
            )
            self._stage1_validation_rejections.extend(rejected)
            self._stage1_validation_diagnostics.extend(diagnostics)
            # E001 in two independent requests does not mean the same event.
            groups: dict[str, str] = {}
            for assessment in validated["assessments"]:
                self._assessment_times[assessment["candidate_id"]] = cached[
                    "assessed_at"
                ]
                local_group = assessment["event_group"]
                if local_group not in groups:
                    groups[local_group] = f"E{next_group:03d}"
                    next_group += 1
                assessment["event_group"] = groups[local_group]
                results.append(assessment)
            observation = {
                **cached["observation"],
                "candidate_count": len(chunk),
                "cache_hit": cache_hit,
                "raw_response_sha256": cached["raw_response_sha256"],
            }
            observations.append(observation)
            response_hashes.append(
                cached["raw_response_sha256"] or content_sha256(cached["result"])
            )
            for key, value in ({} if cache_hit else cached["usage"]).items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    totals[key] = totals.get(key, 0) + value
            self._stage1_usage = totals
            self._stage1_observation = {
                "chunks": observations,
                "completed_chunks": len(observations),
                "usage": totals,
            }
            self._raw_response_sha256 = (
                response_hashes[0]
                if len(response_hashes) == 1
                else content_sha256(response_hashes)
            )
        if totals.get("cost_usd", 0) > float(
            getattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)
        ):
            logger.warning(
                "Qwen Stage1 run usage exceeded post-hoc cost alert threshold: %.6f",
                totals["cost_usd"],
            )
        return {"assessments": results}

    def _upload_handoff(
        self,
        batch_path: Path,
        run_manifest_path: Path,
        manifest: dict[str, Any],
        run_id: str,
    ) -> bool:
        if not config.S3_BUCKET:
            return False
        from storage.s3 import S3Storage

        s3 = S3Storage()
        prefix = "global_events/handoff"
        uploads = [
            (batch_path, f"{prefix}/batches/{batch_path.name}"),
            (run_manifest_path, f"{prefix}/runs/{run_id}.json"),
        ]
        # GDELT raw ZIPs are L0 recovery material and stay local for the
        # configured 24-72h window. Stage1 is wrapped in the run manifest;
        # never publish a bare {"assessments": ...} object.
        for path, key in uploads:
            if not s3.upload_file(path, key):
                return False
        # Each attempt has its own immutable commit marker. A successful S3
        # upload followed by a DB outage must remain safely retryable.
        manifest_path = batch_path.parent / f"{batch_path.stem}.{run_id}.manifest.json"
        existed = manifest_path.exists()
        immutable_write(manifest_path, manifest)
        uploaded = s3.upload_file(
            manifest_path, f"{prefix}/manifests/{manifest_path.name}"
        )
        if not uploaded and not existed:
            manifest_path.unlink(missing_ok=True)
        return uploaded

    def collect(self) -> dict[str, Any]:
        if not getattr(config, "GLOBAL_EVENTS_ENABLED", False):
            return {
                "data": [],
                "status": "disabled",
                "db_contract_status": "migration_389_receipts",
            }
        started_at = datetime.now(UTC).isoformat()
        run_id = f"run_{uuid.uuid4().hex}"
        producer_sha = getattr(config, "GLOBAL_EVENTS_PRODUCER_GIT_COMMIT", "")
        self._raw_response_sha256 = None
        self._stage1_usage = {}
        self._stage1_observation = {}
        self._normalization_lineage = []
        self._stage1_validation_rejections = []
        self._stage1_validation_diagnostics = []
        self._pending_routing_work = None
        self._assessment_times = {}
        checkpoints = self._load_checkpoints()
        try:
            routing_work = json.loads(
                self.routing_pending_path.read_text(encoding="utf-8")
            )
        except FileNotFoundError:
            routing_work = None
        if routing_work and routing_work.get("version") != 1:
            raise RuntimeError("unsupported global_events pending routing version")
        if routing_work and all(
            checkpoints.get(stream, "") >= slot
            for stream, slot in routing_work["pending_checkpoints"].items()
        ):
            # A crash after committing the source checkpoint must not replay
            # a fully drained queue on restart.
            routing_work = None
        groups: dict[str, dict[str, Any]] = {}
        raw_paths = []
        raw_metadata = []
        source_manifest: dict[str, list[dict[str, Any]]] = {}
        index_snapshot_hashes: dict[str, str] = {}
        stream_stats = {}
        errors = []
        pending: dict[str, str] = {}
        for stream, index_url in [] if routing_work else SOURCE_INDEXES.items():
            try:
                index_response = self._session.get(
                    getattr(config, f"GLOBAL_EVENTS_{stream.upper()}_INDEX", index_url),
                    timeout=config.REQUEST_TIMEOUT,
                )
                index_response.raise_for_status()
                index_text = index_response.text
                index_snapshot_hashes[stream] = hashlib.sha256(
                    index_text.encode()
                ).hexdigest()
                artifacts = parse_master_index(index_text, stream)
                selected = self._select_pending(artifacts, checkpoints.get(stream))
                source_manifest[stream] = selected_artifact_manifest(selected)
                completed = []
                for artifact in selected:
                    payload = self._download_artifact(artifact)
                    raw = self._save_raw(artifact, payload, success=False)
                    raw_paths.append(raw)
                    raw_metadata.append(
                        {
                            "stream": artifact.stream,
                            "slot": artifact.slot,
                            "source_url": artifact.url,
                            "expected_bytes": artifact.expected_bytes,
                            "expected_md5": artifact.expected_md5,
                            "sha256": hashlib.sha256(payload).hexdigest(),
                            "local_path": str(raw),
                        }
                    )
                    for row in parse_gkg_artifact(artifact, payload):
                        fingerprint = story_title_fingerprint(row["title"])
                        group = groups.setdefault(
                            fingerprint,
                            {
                                "fingerprint": fingerprint,
                                "rows": [],
                                "domains": set(),
                                "streams": set(),
                                "categories": set(),
                                "impact_signals": set(),
                                "noise_signals": set(),
                                "first_slot": artifact.slot,
                                "last_slot": artifact.slot,
                                "documents": 0,
                            },
                        )
                        group["rows"].append(row)
                        group["domains"].add(row["source_domain"])
                        group["streams"].add(stream)
                        group["impact_signals"].update(row["impact_signals"])
                        group["noise_signals"].update(row["noise_signals"])
                        group["first_slot"] = min(group["first_slot"], artifact.slot)
                        group["last_slot"] = max(group["last_slot"], artifact.slot)
                        group["documents"] += 1
                    completed.append(artifact.slot)
                if completed:
                    pending[stream] = completed[-1]
                stream_stats[stream] = {
                    "checkpoint_before": checkpoints.get(stream),
                    "checkpoint_after": pending.get(stream, checkpoints.get(stream)),
                    "files_processed": len(completed),
                    "latest_slot": artifacts[-1].slot if artifacts else None,
                    "index_sha256": index_snapshot_hashes[stream],
                }
            except Exception as exc:
                errors.append(f"{stream}: {str(exc)[:300]}")
                stream_stats[stream] = {
                    "checkpoint_before": checkpoints.get(stream),
                    "error": str(exc)[:300],
                    "index_sha256": index_snapshot_hashes.get(stream),
                }
        # Conservative routing: title-level impact and no explicit noise only; coverage is diagnostics.
        routed = [
            group
            for group in groups.values()
            if group["impact_signals"]
            and all(not row["noise_signals"] for row in group["rows"])
        ]
        max_candidates = min(
            100, max(1, int(getattr(config, "GLOBAL_EVENTS_QWEN_MAX_CANDIDATES", 100)))
        )
        routed.sort(
            key=lambda item: (
                group_signal_priority(item["impact_signals"]),
                len(item["domains"]),
                item["fingerprint"],
            ),
            reverse=True,
        )
        registry_hash = (
            hashlib.sha256(self.source_registry_path.read_bytes()).hexdigest()
            if self.source_registry_path.exists()
            else None
        )
        if routing_work:
            full_batch = routing_work["batch"]
            source_manifest = routing_work["source_manifest"]
            raw_metadata = routing_work["raw_metadata"]
            raw_paths = [Path(raw["local_path"]) for raw in raw_metadata]
            stream_stats = routing_work["streams"]
            pending = routing_work["pending_checkpoints"]
        else:
            full_batch = build_compact_batch(
                routed,
                source_manifest_sha256=content_sha256(source_manifest),
                source_registry_sha256=registry_hash,
                producer_git_commit=producer_sha,
            )
            routing_work = {
                "version": 1,
                "batch": full_batch,
                "source_manifest": source_manifest,
                "raw_metadata": raw_metadata,
                "streams": stream_stats,
                "pending_checkpoints": pending,
            }
            if not errors:
                # Persist the entire keyword-selected cohort before any model
                # request, including overflow. Drain it before fetching more
                # files; no source checkpoint or raw-success marker yet.
                atomic_replace_json(self.routing_pending_path, routing_work)
        all_candidates = full_batch["payload"]["candidates"]
        batch = candidate_batch_slice(full_batch, all_candidates[:max_candidates])
        deferred_count = max(0, len(all_candidates) - max_candidates)
        stage1: dict[str, Any] | None = None
        if batch["payload"]["candidates"] and not errors:
            try:
                stage1 = self._assess_in_chunks(batch["payload"]["candidates"])
            except Exception as exc:
                errors.append(f"stage1: {str(exc)[:300]}")
                # Never upload an invalid/fake Stage1 artifact. The compact
                # batch and failed manifest/receipt remain auditable instead.
                stage1 = None
        elif not batch["payload"]["candidates"] and not errors:
            stage1 = {"assessments": []}
            # Migration 389 requires an accepted run to carry a response hash;
            # for a deterministic no-candidate run this is the canonical empty
            # Stage1 artifact rather than a fabricated model response.
            self._raw_response_sha256 = content_sha256(stage1)
        run_dir = (
            Path(config.LOCAL_DATA_DIR)
            / self.name
            / "handoff"
            / datetime.now(UTC).strftime("%Y/%m/%d")
        )
        batch_path = run_dir / f"{batch['batch_id']}.json"
        immutable_write(batch_path, batch)
        batch_object_sha256 = artifact_sha256(batch_path)
        finished_at = datetime.now(UTC).isoformat()
        run_manifest_path = run_dir / f"{run_id}.json"
        run_manifest = None
        if stage1 is not None and not errors:
            valid_count = len(stage1["assessments"])
            rejected_count = len(self._stage1_validation_rejections)
            if rejected_count == 0:
                validation_status = "accepted_all"
                traditional_chinese_gate = "canonical_all_passed"
            elif valid_count == 0:
                validation_status = "accepted_all_rejected"
                traditional_chinese_gate = "canonical_survivors_passed"
            else:
                validation_status = "accepted_partial"
                traditional_chinese_gate = "canonical_survivors_passed"
            run_manifest = {
                "schema_version": STAGE1_RUN_SCHEMA_VERSION,
                "run_id": run_id,
                "status": "accepted",
                "started_at": started_at,
                "finished_at": finished_at,
                "input_batch_id": batch["batch_id"],
                "input_content_sha256": batch["content_sha256"],
                "input_contract": "compact-candidate-batch-v2",
                "lineage_complete": True,
                "archive_eligible": True,
                "production_publishable": False,
                "candidate_count": batch["payload"]["candidate_count"],
                "valid_assessment_count": valid_count,
                "rejected_assessment_count": rejected_count,
                "validation_status": validation_status,
                "model": getattr(
                    config, "GLOBAL_EVENTS_QWEN_MODEL", "qwen/qwen3.7-flash"
                ),
                "prompt_version": STAGE1_PROMPT_VERSION,
                "output_schema_version": "stage1-assessment-v2",
                "output_artifact_sha256": content_sha256(stage1),
                "raw_response_sha256": self._raw_response_sha256,
                "candidate_batch_sha256": batch["content_sha256"],
                "traditionalization_policy_version": TRADITIONALIZATION_POLICY_VERSION,
                "traditional_chinese_gate": traditional_chinese_gate,
                "normalization_lineage": self._normalization_lineage,
                "validation_rejections": self._stage1_validation_rejections,
                "validation_diagnostics": self._stage1_validation_diagnostics,
                "source_warning": "title and GDELT metadata only; no article full text",
                "stage1_observation": self._stage1_observation,
                "usage": self._stage1_usage,
                "result": stage1,
            }
            immutable_write(run_manifest_path, run_manifest)
        run_object_sha256 = artifact_sha256(run_manifest_path) if run_manifest else None
        batch_object_key = f"global_events/handoff/batches/{batch_path.name}"
        run_object_key = f"global_events/handoff/runs/{run_id}.json"
        manifest_object_key = (
            f"global_events/handoff/manifests/{batch_path.stem}.{run_id}.manifest.json"
        )
        manifest = {
            "schema_version": "global-events/handoff-manifest/v1",
            "batch_id": batch["batch_id"],
            "run_id": run_id,
            "run_sha256": run_object_sha256,
            "batch_object_key": batch_object_key,
            "run_object_key": run_object_key,
            "manifest_object_key": manifest_object_key,
            "batch_key": batch_object_key,
            "run_key": run_object_key,
            "batch_sha256": batch_object_sha256,
            "batch_content_sha256": batch["content_sha256"],
            "stage1_sha256": content_sha256(stage1) if stage1 is not None else None,
            "raw_objects": raw_metadata,
            "streams": stream_stats,
            "routing": {
                "selected_count": len(all_candidates),
                "assessed_limit": max_candidates,
                "deferred_count": deferred_count,
            },
            "stage1_observation": self._stage1_observation,
            "created_at": datetime.now(UTC).isoformat(),
            "archive_eligible": True if run_manifest is not None else False,
            "production_publishable": False,
            "db_contract_status": "migration_389_receipts",
        }
        handoff_ok = False
        source_or_stage1_failed = bool(errors)
        if run_manifest is not None:
            handoff_ok = self._upload_handoff(
                batch_path, run_manifest_path, manifest, run_id
            )
        if not handoff_ok:
            if not errors:
                errors.append("handoff: S3 manifest-last upload unavailable or failed")
        elif not errors:
            self._pending_routing_work = {
                **routing_work,
                "batch": candidate_batch_slice(
                    full_batch, all_candidates[max_candidates:]
                ),
            }
            if deferred_count == 0:
                self._pending_checkpoints = pending
        manifest["archive_eligible"] = bool(handoff_ok and not errors)
        # Migration 389 deliberately exposes only terminal accepted/failed
        # states; source gaps and handoff failures are failed receipts, not a
        # third partial state.
        run_status = "failed" if errors else "accepted"
        batch_receipt = {
            "_type": "collector_batch",
            "batch_id": batch["batch_id"],
            "schema_version": batch["schema_version"],
            "content_sha256": batch["content_sha256"],
            "observation_first_slot": batch["payload"]["observation_window"][
                "first_slot"
            ],
            "observation_last_slot": batch["payload"]["observation_window"][
                "last_slot"
            ],
            "source_manifest_sha256": batch["payload"]["source_manifest_sha256"],
            "source_registry_sha256": batch["payload"]["source_registry_sha256"],
            "producer_name": batch["payload"]["producer"]["name"],
            "producer_profile_version": batch["payload"]["producer"]["profile_version"],
            "extractor_profile_version": batch["payload"]["producer"][
                "extractor_profile_version"
            ],
            "candidate_id_version": batch["payload"]["producer"][
                "candidate_id_version"
            ],
            "producer_git_commit": batch["payload"]["producer"]["git_commit"],
            "candidate_count": batch["payload"]["candidate_count"],
            "artifact_repo": "ianlkl11234s/pulse-intel-workbench",
            "artifact_path": f"batches/{datetime.now(UTC):%Y/%m/%d}/{batch['batch_id']}.json",
            "archive_eligible": bool(handoff_ok and not errors),
            "production_publishable": False,
            "receipt": manifest,
        }
        run_receipt = {
            "_type": "collector_run",
            "run_id": run_id,
            "batch_id": batch["batch_id"] if run_status == "accepted" else None,
            "status": run_status,
            "model": getattr(config, "GLOBAL_EVENTS_QWEN_MODEL", "qwen/qwen3.7-flash"),
            "prompt_version": STAGE1_PROMPT_VERSION,
            "output_schema_version": "stage1-assessment-v2",
            "started_at": started_at,
            "finished_at": finished_at,
            "candidate_count": batch["payload"]["candidate_count"],
            "input_content_sha256": batch["content_sha256"],
            "output_artifact_sha256": content_sha256(stage1) if stage1 else None,
            "raw_response_sha256": self._raw_response_sha256,
            "archive_eligible": bool(handoff_ok and not errors),
            "production_publishable": False,
            "error_type": (
                "source_or_stage1_failed"
                if source_or_stage1_failed
                else ("handoff_failed" if not handoff_ok else None)
            ),
            "error_message": "; ".join(errors) if errors else None,
            "receipt": manifest,
        }
        # A failed receipt must not reserve the content-addressed batch key;
        # retrying the same source content must be able to insert an accepted
        # batch later. Accepted runs atomically carry exactly batch + run.
        self._supabase_receipts = (
            [batch_receipt, run_receipt] if run_status == "accepted" else [run_receipt]
        )
        self._cleanup_success_raw()
        result = {
            "data": [
                {
                    "batch_id": batch["batch_id"],
                    "content_sha256": batch["content_sha256"],
                    "stage1_count": len(stage1["assessments"]) if stage1 else 0,
                    "production_publishable": False,
                }
            ],
            "streams": stream_stats,
            "candidate_count": len(batch["payload"]["candidates"]),
            "deferred_candidate_count": deferred_count,
            "_candidate_display_records": (
                candidate_display_records(
                    batch, stage1, finished_at, self._assessment_times
                )
                if handoff_ok and stage1 is not None and not errors
                else []
            ),
            "handoff_manifest": manifest,
            "_supabase_receipts": self._supabase_receipts,
            "db_contract_status": "migration_389_receipts",
            "health": {
                "status": "ERROR" if errors else "OK",
                "freshness": "UNKNOWN" if not pending else "CURRENT",
            },
        }
        if errors:
            result["_collector_error"] = "; ".join(errors)
        return result

    def run(self) -> dict[str, Any]:
        stats = super().run()
        if "error" not in stats and self._pending_routing_work is not None:
            remaining = self._pending_routing_work["batch"]["payload"][
                "candidate_count"
            ]
            if remaining:
                atomic_replace_json(
                    self.routing_pending_path, self._pending_routing_work
                )
            else:
                self._save_checkpoints(self._pending_checkpoints)
                for raw in self._pending_routing_work["raw_metadata"]:
                    Path(raw["local_path"]).with_suffix(".success").touch()
                self.routing_pending_path.unlink(missing_ok=True)
        elif "error" not in stats and self._pending_checkpoints:
            self._save_checkpoints(self._pending_checkpoints)
        self._pending_checkpoints = {}
        self._pending_routing_work = None
        return stats


if __name__ == "__main__":
    print(json.dumps(GlobalEventsCollector().run(), ensure_ascii=False, indent=2))
