"""GDELT GKG international-media reports about Taiwan.

Only GDELT metadata is retained.  In particular, V2.1QUOTATIONS and article
body text are never copied into collector output or local archives.
"""

from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import logging
import os
import queue
import re
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
import yaml

import config
from collectors.base import BaseCollector, TAIPEI_TZ


logger = logging.getLogger(__name__)
csv.field_size_limit(16 * 1024 * 1024)

RECORD_ID_RE = re.compile(r"^\d{14}-\S+$")
TITLE_RE = re.compile(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", re.IGNORECASE | re.DOTALL)
TAIWAN_TERMS_RE = re.compile(
    r"(?:\btaiwan(?:ese)?\b|\btaipei\b|\btaiwan\s+strait\b|"
    r"\bcross[-\s]?strait\b|\btsmc\b|\btaiwan\s+semiconductor\b|"
    r"台灣|臺灣|台湾|台北|臺北|台海|兩岸|两岸|中華民國|中华民国|"
    r"대만|타이완|타이베이|대만해협|тайван(?:ь)?|тайбэй|تايوان|تايبيه|"
    r"\btayvan\b|\btaypey\b|đài\s+loan|đài\s+bắc|ไต้หวัน)",
    re.IGNORECASE,
)
TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref_src"}

TOPICS = (
    "cross_strait",
    "security_defense",
    "diplomacy",
    "economy_trade",
    "technology_semiconductors",
    "society_culture",
    "disaster_environment",
    "other",
)
SOURCE_KINDS = (
    "foreign_editorial_media",
    "taiwan_editorial_media",
    "aggregator_press_release_or_ugc",
    "unclear",
)
SOURCE_LOCATION_LEVELS = {"country", "city"}
SOURCE_LOCATION_METHODS = {"country_registry", "outlet_registry", "government_capital"}
SOURCE_LOCATION_CONFIDENCES = {"verified", "fallback"}
SOURCE_LOCATION_FIELDS = (
    "source_country",
    "source_city",
    "source_location_label",
    "source_latitude",
    "source_longitude",
    "source_location_level",
    "source_location_method",
    "source_location_confidence",
)

SYSTEM_PROMPT = """You are a strict news triage classifier. Judge whether Taiwan is a
substantial subject of each report using only the supplied GDELT metadata. Do not infer
facts absent from the metadata. A Taiwan location, Taiwanese company, or person mentioned
only incidentally is not enough.

taiwan_relevance:
0 = unrelated or false match
1 = incidental/weak mention; not a meaningful Taiwan report
2 = Taiwan is a substantial part of the report
3 = Taiwan is the central subject

importance (not emergency severity):
0 = trivial/no usable signal
1 = niche or routine
2 = broadly consequential
3 = critical geopolitical, security, economic, or societal impact

Return one result for every input id. topics must contain only allowed enum values.
source_kind describes the publisher represented by source_domain; foreign means outside
Taiwan. If the metadata is insufficient, use unclear rather than guessing. Return JSON only.
Do not add explanations or free-text fields."""


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


def canonical_url(raw: str | None) -> str:
    """Normalize a report URL for cross-file and cross-stream deduplication."""
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
        host = (parsed.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        port = f":{parsed.port}" if parsed.port else ""
    except ValueError:
        return raw
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in TRACKING_KEYS
    ]
    query.sort()
    scheme = "https" if parsed.scheme.lower() in {"http", "https"} else parsed.scheme.lower()
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((scheme, host + port, path, urlencode(query), ""))


def source_domain(source: str, url: str) -> str:
    host = (urlsplit(url).hostname or source).lower().strip().strip(".")
    return host[4:] if host.startswith("www.") else host


def extract_title(extras_xml: str) -> str:
    match = TITLE_RE.search(extras_xml or "")
    if not match:
        return ""
    return re.sub(r"\s+", " ", html.unescape(match.group(1))).strip()


def has_tw_location(raw: str) -> bool:
    for location in (raw or "").split(";"):
        parts = location.split("#")
        if len(parts) >= 3 and parts[2].upper() == "TW":
            return True
    return False


def iter_logical_rows(text: Iterable[str]) -> Iterator[list[str]]:
    """Join translation-stream physical continuation lines into field 27."""
    current: list[str] | None = None
    for physical in text:
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


def candidate_rules(row: list[str], title: str) -> list[str]:
    rules: list[str] = []
    if has_tw_location(row[9]):
        rules.append("location_country_tw")
    nonlocation_blob = " ".join((title, row[7], row[11], row[13]))
    if TAIWAN_TERMS_RE.search(nonlocation_blob):
        rules.append("taiwan_nonlocation_registry_v2")
    return rules


def _split_gkg_names(raw: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for item in (raw or "").split(";"):
        value = item.split(",", 1)[0].strip()
        if value and value not in seen:
            values.append(value)
            seen.add(value)
    return values


def _number(raw: str, integer: bool = False):
    try:
        return int(float(raw)) if integer else float(raw)
    except (TypeError, ValueError):
        return None


def parse_gkg_locations(raw: str) -> list[dict]:
    """Normalize GKG locations without retaining the provider's raw field."""
    locations: list[dict] = []
    for item in (raw or "").split(";"):
        if not item:
            continue
        parts = item.split("#")
        parts.extend([""] * (7 - len(parts)))
        locations.append({
            "location_type": _number(parts[0], integer=True),
            "name": parts[1] or None,
            "country_code": parts[2] or None,
            "adm1_code": parts[3] or None,
            "latitude": _number(parts[4]),
            "longitude": _number(parts[5]),
            "feature_id": parts[6] or None,
        })
    return locations


def parse_gkg_tone(raw: str) -> dict | None:
    """Normalize V1.5TONE's seven positional values into named JSON."""
    if not raw:
        return None
    parts = raw.split(",")
    parts.extend([""] * (7 - len(parts)))
    return {
        "tone": _number(parts[0]),
        "positive_score": _number(parts[1]),
        "negative_score": _number(parts[2]),
        "polarity": _number(parts[3]),
        "activity_reference_density": _number(parts[4]),
        "self_group_reference_density": _number(parts[5]),
        "word_count": _number(parts[6], integer=True),
    }


def _parse_gdelt_ts(raw: str, fallback_slot: str) -> str:
    value = (raw or fallback_slot).strip()[:14]
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return datetime.strptime(fallback_slot, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).isoformat()


def parse_master_index(text: str, stream: str) -> list[GKGArtifact]:
    suffix = ".translation.gkg.csv.zip" if stream == "translation" else ".gkg.csv.zip"
    artifacts: list[GKGArtifact] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[2].endswith(suffix):
            continue
        name = parts[2].rsplit("/", 1)[-1]
        slot = name[:14]
        if not slot.isdigit():
            continue
        artifacts.append(
            GKGArtifact(
                stream=stream,
                slot=slot,
                expected_bytes=int(parts[0]),
                expected_md5=parts[1].lower(),
                url=parts[2].replace("http://", "https://", 1),
            )
        )
    return sorted(artifacts, key=lambda item: item.slot)


def validate_stage1(payload: object, expected_ids: set[int]) -> list[dict]:
    """Validate exact response shape, IDs, schemas, ranges, and enums."""
    if not isinstance(payload, dict) or set(payload) != {"items"}:
        raise ValueError("Stage1 response must be an object containing only 'items'")
    items = payload["items"]
    if not isinstance(items, list):
        raise ValueError("Stage1 items must be an array")
    required = {"id", "source_kind", "taiwan_relevance", "importance", "topics"}
    if len(items) != len(expected_ids):
        raise ValueError("Stage1 response item count mismatch")
    got_ids: list[int] = []
    for item in items:
        if not isinstance(item, dict) or set(item) != required:
            raise ValueError("Stage1 item schema mismatch")
        item_id = item["id"]
        if type(item_id) is not int:  # bool is an int subclass and must not pass
            raise ValueError("Stage1 id must be an integer")
        got_ids.append(item_id)
        if item["source_kind"] not in SOURCE_KINDS:
            raise ValueError(f"invalid source_kind for id={item_id}")
        if type(item["taiwan_relevance"]) is not int or item["taiwan_relevance"] not in range(4):
            raise ValueError(f"invalid taiwan_relevance for id={item_id}")
        if type(item["importance"]) is not int or item["importance"] not in range(4):
            raise ValueError(f"invalid importance for id={item_id}")
        topics = item["topics"]
        if not isinstance(topics, list) or any(topic not in TOPICS for topic in topics):
            raise ValueError(f"invalid topics for id={item_id}")
        if len(topics) != len(set(topics)):
            raise ValueError(f"duplicate topics for id={item_id}")
    if set(got_ids) != expected_ids or len(got_ids) != len(set(got_ids)):
        raise ValueError("Stage1 response IDs missing, duplicated, or unexpected")
    return items


class IntlMediaTaiwanCollector(BaseCollector):
    """GDELT standard + translation GKG collector, scheduled every 15 minutes."""

    name = "intl_media_taiwan"
    interval_minutes = getattr(config, "INTL_MEDIA_TAIWAN_INTERVAL", 15)
    COLLECT_TIMEOUT = 900

    INDEXES = {
        "standard": getattr(
            config,
            "INTL_MEDIA_TAIWAN_STANDARD_INDEX",
            "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
        ),
        "translation": getattr(
            config,
            "INTL_MEDIA_TAIWAN_TRANSLATION_INDEX",
            "https://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
        ),
    }

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": "mini-taiwan-pulse/gdelt-intl-media-metadata-collector",
                "Accept": "text/plain, application/zip, application/json",
            }
        )
        self._pending_checkpoints: dict[str, str] = {}
        self._domain_registry: dict[str, dict] | None = None

    @property
    def checkpoint_path(self) -> Path:
        return Path(config.LOCAL_DATA_DIR) / self.name / "checkpoint.json"

    def require_db_write(self) -> bool:
        return True

    def run(self) -> dict:
        stats = super().run()
        if "error" not in stats:
            try:
                self._save_checkpoints(self._pending_checkpoints)
            except Exception as exc:
                # Main rows are already committed.  Do not advance an uncertain
                # checkpoint; DB url_norm uniqueness makes the next replay safe.
                logger.error("[%s] post-commit checkpoint/health failed: %s", self.name, exc)
        self._pending_checkpoints = {}
        return stats

    def _load_checkpoints(self) -> dict[str, str]:
        # DB source state is authoritative.  Local JSON only accelerates startup
        # and remains a fallback for a temporarily unavailable read connection.
        if self.supabase_writer:
            try:
                with self.supabase_writer.with_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT source_stream, to_char(checkpoint_slot_ts AT TIME ZONE 'UTC', "
                            "'YYYYMMDDHH24MISS') FROM live.intl_media_taiwan_source_state "
                            "WHERE source_id IN ('gdelt_gkg_standard','gdelt_gkg_translation') LIMIT 2"
                        )
                        rows = cur.fetchall()
                db_checkpoints = {stream: slot for stream, slot in rows if stream in self.INDEXES and slot}
                if db_checkpoints:
                    return db_checkpoints
            except Exception as exc:
                logger.warning("[%s] source-state checkpoint read failed; using local fallback: %s", self.name, exc)
        try:
            payload = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid checkpoint: {exc}") from exc
        streams = payload.get("streams")
        if not isinstance(streams, dict):
            raise RuntimeError("invalid checkpoint: streams must be an object")
        return {
            stream: slot
            for stream, slot in streams.items()
            if stream in self.INDEXES and isinstance(slot, str) and len(slot) == 14 and slot.isdigit()
        }

    def _save_checkpoints(self, updates: dict[str, str]) -> None:
        if not updates:
            return
        checkpoints = self._load_checkpoints()
        for stream, slot in updates.items():
            if slot > checkpoints.get(stream, ""):
                checkpoints[stream] = slot
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.checkpoint_path.with_suffix(".tmp")
        temp.write_text(
            json.dumps(
                {"version": 1, "streams": checkpoints, "updated_at": datetime.now(timezone.utc).isoformat()},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        os.replace(temp, self.checkpoint_path)

    def _select_pending(
        self, artifacts: list[GKGArtifact], checkpoint: str | None
    ) -> list[GKGArtifact]:
        max_files = max(1, int(getattr(config, "INTL_MEDIA_TAIWAN_MAX_FILES_PER_STREAM", 4)))
        if checkpoint:
            by_slot = {item.slot: item for item in artifacts}
            cursor = datetime.strptime(checkpoint, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            selected: list[GKGArtifact] = []
            for _ in range(max_files):
                cursor = cursor + timedelta(minutes=15)
                expected = cursor.strftime("%Y%m%d%H%M%S")
                item = by_slot.get(expected)
                if item is None:
                    if expected <= artifacts[-1].slot:
                        raise GKGIndexGap(
                            f"GDELT {artifacts[-1].stream} index gap at {expected}; "
                            "later slots were not skipped",
                            selected,
                        )
                    break
                selected.append(item)
            return selected
        initial_slots = max(1, int(getattr(config, "INTL_MEDIA_TAIWAN_INITIAL_SLOTS", 1)))
        return artifacts[-initial_slots:]

    def _fetch_index(self, stream: str) -> list[GKGArtifact]:
        response = self._session.get(self.INDEXES[stream], timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        artifacts = parse_master_index(response.text, stream)
        if not artifacts:
            raise RuntimeError(f"GDELT {stream} index contained no GKG artifacts")
        return artifacts

    def _download_artifact(self, artifact: GKGArtifact) -> bytes:
        response = self._session.get(
            artifact.url,
            timeout=(min(20, config.REQUEST_TIMEOUT), max(60, config.REQUEST_TIMEOUT)),
        )
        response.raise_for_status()
        payload = response.content
        if len(payload) != artifact.expected_bytes:
            raise RuntimeError(
                f"{artifact.stream}:{artifact.slot} size mismatch "
                f"expected={artifact.expected_bytes} actual={len(payload)}"
            )
        digest = hashlib.md5(payload).hexdigest()  # nosec B324 - upstream integrity field
        if digest != artifact.expected_md5:
            raise RuntimeError(f"{artifact.stream}:{artifact.slot} md5 mismatch")
        return payload

    def _parse_artifact(self, artifact: GKGArtifact, payload: bytes) -> tuple[list[dict], str]:
        records: list[dict] = []
        latest_item_ts: str | None = None
        logical_rows = 0
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            members = [name for name in archive.namelist() if not name.endswith("/")]
            if len(members) != 1:
                raise RuntimeError(f"unexpected zip members for {artifact.stream}:{artifact.slot}")
            with archive.open(members[0]) as binary:
                text = io.TextIOWrapper(binary, encoding="utf-8", errors="replace", newline="")
                for row in iter_logical_rows(text):
                    if len(row) != 27:
                        continue
                    logical_rows += 1
                    published_ts = _parse_gdelt_ts(row[1], artifact.slot)
                    if latest_item_ts is None or published_ts > latest_item_ts:
                        latest_item_ts = published_ts
                    title = extract_title(row[26])
                    rules = candidate_rules(row, title)
                    url_norm = canonical_url(row[4])
                    if not rules or not url_norm:
                        continue
                    domain = source_domain(row[3], row[4])
                    report_key = hashlib.sha256(url_norm.encode("utf-8", "replace")).hexdigest()
                    records.append(
                        {
                            "source_id": artifact.source_id,
                            "source_stream": artifact.stream,
                            "source_domain": domain,
                            "source_country": None,
                            "source_city": None,
                            "source_location_label": None,
                            "source_latitude": None,
                            "source_longitude": None,
                            "source_location_level": None,
                            "source_location_method": None,
                            "source_location_confidence": None,
                            "source_language": "en" if artifact.stream == "standard" else None,
                            "source_name": row[3].strip() or domain,
                            "url": row[4].strip(),
                            "url_norm": url_norm,
                            "report_key": report_key,
                            "title_original": title or None,
                            "quality_flags": [] if title else ["missing_title"],
                            "summary_zh": None,
                            "published_ts": _parse_gdelt_ts(row[1], artifact.slot),
                            "gkg_record_id": row[0],
                            "gkg_slot_ts": _parse_gdelt_ts(artifact.slot, artifact.slot),
                            "gkg_themes": _split_gkg_names(row[7]),
                            "gkg_locations": parse_gkg_locations(row[9]),
                            "gkg_persons": _split_gkg_names(row[11]),
                            "gkg_organizations": _split_gkg_names(row[13]),
                            "gkg_tone": parse_gkg_tone(row[15]) or {},
                            "candidate_rules": rules,
                        }
                    )
        if logical_rows == 0:
            raise RuntimeError(f"{artifact.stream}:{artifact.slot} contained no valid GKG rows")
        return records, latest_item_ts or _parse_gdelt_ts(artifact.slot, artifact.slot)

    @property
    def domain_registry_path(self) -> Path:
        return Path(__file__).resolve().parents[1] / "config" / "intl_media_domain_registry.yaml"

    def _load_domain_registry(self) -> dict[str, dict]:
        if self._domain_registry is not None:
            return self._domain_registry
        payload = yaml.safe_load(self.domain_registry_path.read_text(encoding="utf-8")) or {}
        registry: dict[str, dict] = {}
        locations = payload.get("locations") or {}
        country_labels = locations.get("countries") or {}
        outlet_locations = locations.get("outlets") or {}
        kind_map = {
            "foreign_media": "foreign_editorial_media",
            "taiwan_media": "taiwan_editorial_media",
            "aggregator_nonmedia": "aggregator_press_release_or_ugc",
        }
        for classification, countries in (payload.get("classifications") or {}).items():
            source_kind = kind_map.get(classification)
            if not source_kind or not isinstance(countries, dict):
                continue
            for country, domains in countries.items():
                for domain in domains or []:
                    country_code = None if country == "unknown" else str(country)
                    location = self._source_location_metadata(
                        country_code,
                        country_labels.get(country_code) if country_code else None,
                        outlet_locations.get(str(domain).lower()),
                    )
                    registry[str(domain).lower()] = {
                        "source_kind": source_kind,
                        **location,
                    }
        unregistered_location_domains = set(outlet_locations) - set(registry)
        if unregistered_location_domains:
            raise ValueError(
                "source outlet locations require an exact classified domain: "
                f"{sorted(unregistered_location_domains)}"
            )
        self._domain_registry = registry
        return registry

    @staticmethod
    def _source_location_metadata(
        country: str | None,
        country_label: str | None,
        outlet_location: dict | None,
    ) -> dict:
        """Resolve publisher location only from the reviewed registry.

        GKG mentioned places, domain suffixes, and LLM output are intentionally not
        inputs here. Country-only metadata is the safe fallback; coordinates are
        emitted only for an explicit outlet/capital mapping.
        """
        empty = {field: None for field in SOURCE_LOCATION_FIELDS}
        if not country:
            return empty
        if not isinstance(country_label, str) or not country_label.strip():
            raise ValueError(f"missing source country label for {country}")
        resolved = {
            **empty,
            "source_country": country,
            "source_location_label": country_label.strip(),
            "source_location_level": "country",
            "source_location_method": "country_registry",
            "source_location_confidence": "verified",
        }
        if outlet_location is None:
            return resolved
        if not isinstance(outlet_location, dict):
            raise ValueError(f"invalid source outlet location for {country}")
        level = outlet_location.get("level")
        method = outlet_location.get("method")
        confidence = outlet_location.get("confidence")
        label = outlet_location.get("label")
        city = outlet_location.get("city")
        latitude = outlet_location.get("latitude")
        longitude = outlet_location.get("longitude")
        if level not in SOURCE_LOCATION_LEVELS:
            raise ValueError(f"invalid source location level: {level}")
        if method not in SOURCE_LOCATION_METHODS:
            raise ValueError(f"invalid source location method: {method}")
        if confidence not in SOURCE_LOCATION_CONFIDENCES:
            raise ValueError(f"invalid source location confidence: {confidence}")
        if not isinstance(label, str) or not label.strip():
            raise ValueError("source location label is required")
        if level == "city" and (not isinstance(city, str) or not city.strip()):
            raise ValueError("source city is required for city-level mappings")
        if (latitude is None) != (longitude is None):
            raise ValueError("source coordinates must be provided as a pair")
        if latitude is not None:
            if isinstance(latitude, bool) or isinstance(longitude, bool):
                raise ValueError("source coordinates must be numeric")
            latitude = float(latitude)
            longitude = float(longitude)
            if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
                raise ValueError("source coordinates are out of range")
        return {
            **resolved,
            "source_city": city.strip() if isinstance(city, str) else None,
            "source_location_label": label.strip(),
            "source_latitude": latitude,
            "source_longitude": longitude,
            "source_location_level": level,
            "source_location_method": method,
            "source_location_confidence": confidence,
        }

    def _partition_by_domain_registry(self, rows: list[dict]) -> tuple[list[dict], list[dict]]:
        """Return registry-excluded rows and the foreign/unknown Stage1 denominator."""
        registry = self._load_domain_registry()
        excluded: list[dict] = []
        llm_input: list[dict] = []
        for row in rows:
            known = registry.get(row["source_domain"])
            if known and known["source_kind"] in {
                "taiwan_editorial_media", "aggregator_press_release_or_ugc"
            }:
                excluded.append({**row, **known})
            else:
                prepared = {
                    **row,
                    **(
                        {field: known.get(field) for field in SOURCE_LOCATION_FIELDS}
                        if known
                        else {field: None for field in SOURCE_LOCATION_FIELDS}
                    ),
                }
                if known:
                    prepared["source_kind"] = known["source_kind"]
                llm_input.append(prepared)
        return excluded, llm_input

    def _existing_url_norms(self, urls: list[str]) -> set[str]:
        if not self.supabase_writer or not urls:
            return set()
        existing: set[str] = set()
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    for offset in range(0, len(urls), 1000):
                        cur.execute(
                            "SELECT url_norm FROM live.intl_media_taiwan "
                            "WHERE url_norm = ANY(%s) LIMIT 1000",
                            (urls[offset : offset + 1000],),
                        )
                        existing.update(row[0] for row in cur.fetchall())
        except Exception as exc:
            logger.warning("[%s] existing-url query failed; DB unique key remains fallback: %s", self.name, exc)
        return existing

    @staticmethod
    def _compact_llm_item(item_id: int, row: dict) -> dict:
        return {
            "id": item_id,
            "source_domain": row.get("source_domain", ""),
            "title": (row.get("title_original") or "")[:500],
            "themes": row.get("gkg_themes", [])[:30],
            "locations": (row.get("gkg_locations") or [])[:20],
            "persons": row.get("gkg_persons", [])[:20],
            "organizations": row.get("gkg_organizations", [])[:20],
        }

    @staticmethod
    def _response_format(model: str) -> dict:
        if model == "openai/gpt-oss-20b":
            item = {
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "source_kind", "taiwan_relevance", "importance", "topics"],
                "properties": {
                    "id": {"type": "integer"},
                    "source_kind": {"type": "string", "enum": list(SOURCE_KINDS)},
                    "taiwan_relevance": {"type": "integer", "minimum": 0, "maximum": 3},
                    "importance": {"type": "integer", "minimum": 0, "maximum": 3},
                    "topics": {"type": "array", "items": {"type": "string", "enum": list(TOPICS)}},
                },
            }
            return {
                "type": "json_schema",
                "json_schema": {
                    "name": "taiwan_news_triage",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["items"],
                        "properties": {"items": {"type": "array", "items": item}},
                    },
                },
            }
        return {"type": "json_object"}

    def _request_batch(self, model: str, batch: list[dict]) -> tuple[dict, float]:
        api_key = getattr(config, "OPENROUTER_API_KEY", "")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is not configured")
        body = {
            "model": model,
            "temperature": 0,
            "max_tokens": 3000,
            **({"provider": {"require_parameters": True}} if model == "openai/gpt-oss-20b" else {}),
            "reasoning": {"effort": "minimal" if model == "openai/gpt-oss-20b" else "none", "exclude": True},
            "response_format": self._response_format(model),
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"allowed_topics": TOPICS, "allowed_source_kinds": SOURCE_KINDS, "items": batch},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            ],
        }
        started = time.monotonic()
        response = self._session.post(
            getattr(config, "OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions"),
            json=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/mini-taiwan-pulse",
                "X-Title": "Mini Taiwan Pulse GDELT collector",
            },
            timeout=(10, int(getattr(config, "INTL_MEDIA_TAIWAN_LLM_HTTP_TIMEOUT", 45))),
        )
        if not response.ok:
            try:
                error = response.json().get("error", {})
                message = error.get("message") if isinstance(error, dict) else str(error)
            except (ValueError, AttributeError):
                message = response.reason
            raise RuntimeError(f"OpenRouter HTTP {response.status_code}: {str(message)[:300]}")
        return response.json(), time.monotonic() - started

    def _request_with_deadline(self, model: str, batch: list[dict]) -> tuple[dict, float]:
        deadline = int(getattr(config, "INTL_MEDIA_TAIWAN_LLM_HARD_TIMEOUT", 60))
        result_queue: queue.Queue = queue.Queue(maxsize=1)

        def call() -> None:
            try:
                result_queue.put((True, self._request_batch(model, batch)))
            except BaseException as exc:  # forwarded to the collector thread
                result_queue.put((False, exc))

        worker = threading.Thread(target=call, name="openrouter-stage1", daemon=True)
        worker.start()
        try:
            ok, value = result_queue.get(timeout=deadline)
        except queue.Empty as exc:
            raise TimeoutError(f"OpenRouter hard timeout after {deadline}s") from exc
        if ok:
            return value
        raise value

    @staticmethod
    def _parse_llm_content(raw: dict) -> object:
        content = raw["choices"][0]["message"]["content"]
        if not isinstance(content, str) or not content.strip():
            raise ValueError("OpenRouter returned empty content")
        content = re.sub(r"^```(?:json)?\s*", "", content.strip(), flags=re.IGNORECASE)
        content = re.sub(r"\s*```$", "", content)
        return json.loads(content)

    def _classify_batch(self, rows: list[dict]) -> tuple[list[dict], dict]:
        compact = [self._compact_llm_item(index, row) for index, row in enumerate(rows)]
        expected_ids = set(range(len(rows)))
        primary = getattr(config, "INTL_MEDIA_TAIWAN_STAGE1_MODEL", "openai/gpt-oss-20b")
        fallback = getattr(config, "INTL_MEDIA_TAIWAN_STAGE1_FALLBACK_MODEL", "qwen/qwen3.7-flash")
        max_attempts = min(3, max(1, int(getattr(config, "INTL_MEDIA_TAIWAN_LLM_MAX_ATTEMPTS", 3))))
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            model = primary if attempt == 1 else fallback
            try:
                raw, latency = self._request_with_deadline(model, compact)
                judgments = validate_stage1(self._parse_llm_content(raw), expected_ids)
                usage = raw.get("usage") or {}
                by_id = {item["id"]: item for item in judgments}
                processed_at = datetime.now(timezone.utc).isoformat()
                output: list[dict] = []
                for index, row in enumerate(rows):
                    judgment = by_id[index]
                    output.append(
                        {
                            **row,
                            "source_kind": row.get("source_kind") or judgment["source_kind"],
                            "taiwan_relevance": judgment["taiwan_relevance"],
                            "importance": judgment["importance"],
                            "topics": judgment["topics"],
                            "severity_source": "inferred",
                            "source_kind_method": (
                                "domain_registry" if row.get("source_kind") else "openrouter"
                            ),
                            "llm_model": model,
                            "llm_processed_at": processed_at,
                        }
                    )
                return output, {
                    "model": model,
                    "attempts": attempt,
                    "latency_seconds": latency,
                    "prompt_tokens": usage.get("prompt_tokens") or 0,
                    "completion_tokens": usage.get("completion_tokens") or 0,
                    "cost_usd": usage.get("cost") or 0,
                }
            except (KeyError, ValueError, RuntimeError, TimeoutError, requests.RequestException, json.JSONDecodeError) as exc:
                last_error = exc
                logger.warning(
                    "[%s] Stage1 attempt %d/%d model=%s failed: %s",
                    self.name,
                    attempt,
                    max_attempts,
                    model,
                    exc,
                )
                if attempt < max_attempts:
                    time.sleep(min(4, 2 ** (attempt - 1)))
        raise RuntimeError(f"Stage1 failed after {max_attempts} attempts: {last_error}")

    def _annotate(self, rows: list[dict]) -> tuple[list[dict], dict]:
        batch_size = max(1, min(30, int(getattr(config, "INTL_MEDIA_TAIWAN_LLM_BATCH_SIZE", 10))))
        output: list[dict] = []
        totals = {"batches": 0, "attempts": 0, "prompt_tokens": 0, "completion_tokens": 0, "cost_usd": 0.0}
        models: set[str] = set()
        for offset in range(0, len(rows), batch_size):
            annotated, usage = self._classify_batch(rows[offset : offset + batch_size])
            output.extend(annotated)
            totals["batches"] += 1
            totals["attempts"] += usage["attempts"]
            totals["prompt_tokens"] += usage["prompt_tokens"]
            totals["completion_tokens"] += usage["completion_tokens"]
            totals["cost_usd"] += float(usage["cost_usd"])
            models.add(usage["model"])
        totals["cost_usd"] = round(totals["cost_usd"], 8)
        totals["models"] = sorted(models)
        logger.info(
            "[%s] OpenRouter batches=%d attempts=%d models=%s tokens=%d/%d cost=$%.6f",
            self.name,
            totals["batches"],
            totals["attempts"],
            ",".join(totals["models"]),
            totals["prompt_tokens"],
            totals["completion_tokens"],
            totals["cost_usd"],
        )
        return output, totals

    def collect(self) -> dict:
        checkpoints = self._load_checkpoints()
        candidates: list[dict] = []
        seen_urls: set[str] = set()
        stream_stats: dict[str, dict] = {}
        pending_checkpoints: dict[str, str] = {}
        source_states: list[dict] = []
        errors: list[str] = []

        for stream in ("standard", "translation"):
            stream_count = 0
            latest_item_ts = None
            completed_slot = None
            selected: list[GKGArtifact] = []
            latest_slot = None
            gap_error = None
            try:
                artifacts = self._fetch_index(stream)
                latest_slot = artifacts[-1].slot
                try:
                    selected = self._select_pending(artifacts, checkpoints.get(stream))
                except GKGIndexGap as exc:
                    # Process and commit only the contiguous prefix.  The gap is
                    # recorded as an error and no later slot is skipped.
                    selected = exc.completed
                    gap_error = str(exc)
                for artifact in selected:
                    rows, artifact_latest_item_ts = self._parse_artifact(
                        artifact, self._download_artifact(artifact)
                    )
                    latest_item_ts = max(latest_item_ts or artifact_latest_item_ts, artifact_latest_item_ts)
                    stream_count += len(rows)
                    for row in rows:
                        if row["url_norm"] not in seen_urls:
                            candidates.append(row)
                            seen_urls.add(row["url_norm"])
                    completed_slot = artifact.slot
                    pending_checkpoints[stream] = artifact.slot
                checkpoint_after = completed_slot or checkpoints.get(stream)
                success = gap_error is None
                if gap_error:
                    errors.append(f"{stream}: {gap_error}")
                if success and latest_item_ts is None:
                    latest_item_ts = _parse_gdelt_ts(latest_slot, latest_slot)
                source_states.append({
                    "source_id": f"gdelt_gkg_{stream}",
                    "source_stream": stream,
                    "enabled": True,
                    "poll_interval_minutes": 15,
                    "checkpoint_slot_ts": (
                        _parse_gdelt_ts(checkpoint_after, latest_slot) if checkpoint_after else None
                    ),
                    "latest_item_ts": latest_item_ts,
                    "success": success,
                    "advance_checkpoint_on_error": bool(completed_slot),
                    "records_seen_last_run": stream_count,
                    "last_error": gap_error,
                    "feed_url": f"gdelt://gkg/{stream}",
                })
                stream_stats[stream] = {
                    "available_latest_slot": latest_slot,
                    "checkpoint_before": checkpoints.get(stream),
                    "checkpoint_after": checkpoint_after,
                    "files_processed": len(selected),
                    "candidate_rows": stream_count,
                    "bootstrap": checkpoints.get(stream) is None,
                    "bootstrap_start_slot": selected[0].slot if checkpoints.get(stream) is None and selected else None,
                    **({"error": gap_error} if gap_error else {}),
                }
            except Exception as exc:
                errors.append(f"{stream}: {exc}")
                checkpoint_after = completed_slot or checkpoints.get(stream)
                source_states.append({
                    "source_id": f"gdelt_gkg_{stream}",
                    "source_stream": stream,
                    "enabled": True,
                    "poll_interval_minutes": 15,
                    "checkpoint_slot_ts": (
                        _parse_gdelt_ts(checkpoint_after, checkpoint_after) if checkpoint_after else None
                    ),
                    "latest_item_ts": latest_item_ts,
                    "success": False,
                    "advance_checkpoint_on_error": bool(completed_slot),
                    "records_seen_last_run": stream_count,
                    "last_error": str(exc)[:500],
                    "feed_url": f"gdelt://gkg/{stream}",
                })
                stream_stats[stream] = {
                    "error": str(exc)[:500],
                    "checkpoint_before": checkpoints.get(stream),
                    "checkpoint_after": checkpoint_after,
                    "files_processed": len(selected),
                    "candidate_rows": stream_count,
                }

        existing = self._existing_url_norms([row["url_norm"] for row in candidates])
        fresh = [row for row in candidates if row["url_norm"] not in existing]
        registry_excluded, llm_input = self._partition_by_domain_registry(fresh)
        try:
            llm_annotated, usage = self._annotate(llm_input) if llm_input else ([], {
                "batches": 0,
                "attempts": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost_usd": 0.0,
                "models": [],
            })
            annotated = llm_annotated
        except Exception as exc:
            # A failed Stage1 means none of the files represented by these rows
            # are continuously complete.  Persist the error ledger atomically,
            # but keep both checkpoints unchanged so the whole range is retried.
            errors.append(f"openrouter_stage1: {exc}")
            for state in source_states:
                if state["source_stream"] in pending_checkpoints:
                    stream = state["source_stream"]
                    state.update({
                        "success": False,
                        "advance_checkpoint_on_error": False,
                        "checkpoint_slot_ts": (
                            _parse_gdelt_ts(checkpoints[stream], checkpoints[stream])
                            if checkpoints.get(stream) else None
                        ),
                        "last_error": str(exc)[:500],
                    })
            pending_checkpoints = {}
            annotated = []
            usage = {
                "batches": 0,
                "attempts": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost_usd": 0.0,
                "models": [],
            }
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        for row in annotated:
            row["collected_at"] = collected_at

        self._pending_checkpoints = pending_checkpoints
        return {
            "data": annotated,
            "source_states": source_states,
            "streams": stream_stats,
            "candidate_rows": len(candidates),
            "existing_url_duplicates": len(existing),
            "registry_excluded_taiwan": sum(
                row["source_kind"] == "taiwan_editorial_media" for row in registry_excluded
            ),
            "registry_excluded_aggregator": sum(
                row["source_kind"] == "aggregator_press_release_or_ugc"
                for row in registry_excluded
            ),
            "new_rows": len(annotated),
            "missing_title": sum(not row.get("title_original") for row in annotated),
            "rejected_rows": 0,
            "llm_batches": usage["batches"],
            "llm_attempts": usage["attempts"],
            "llm_models": usage["models"],
            "llm_prompt_tokens": usage["prompt_tokens"],
            "llm_completion_tokens": usage["completion_tokens"],
            "llm_cost_usd": usage["cost_usd"],
            **({"_collector_error": "; ".join(errors)} if errors else {}),
        }


if __name__ == "__main__":
    collector = IntlMediaTaiwanCollector()
    print(json.dumps(collector.run(), ensure_ascii=False, indent=2))
