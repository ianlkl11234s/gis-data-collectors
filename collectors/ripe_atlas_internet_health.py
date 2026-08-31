"""RIPE Atlas evidence collector for Taiwan internet health.

Only measurements and probes in the reviewed, versioned roster are queried.
The collector stores finite five-minute aggregates in the shared internet
health contract.  Provider results are evidence, never a national status
decision; raw responses remain in the existing private local/S3 archive path.
"""
from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

import config
from collectors.base import BaseCollector
from collectors.internet_health import _iso, _number, _source_run


UTC = timezone.utc
ATLAS_BASE = "https://atlas.ripe.net/api/v2"
ROSTER_SCHEMA = "ripe_internet_health_roster.v1"
SOURCE = "ripe_atlas"
EVIDENCE_FAMILY = "ripe_atlas"


def _digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _safe_error(exc: Exception) -> str:
    message = str(exc)
    key = getattr(config, "RIPE_ATLAS_API_KEY", "")
    if key:
        message = message.replace(key, "[redacted]")
    return f"{type(exc).__name__}: {message[:300]}"


def load_ripe_roster(path: str | Path | None = None) -> dict[str, Any]:
    """Load the reviewed internal-only roster without environment secrets."""
    roster_path = Path(path or config.RIPE_INTERNET_HEALTH_ROSTER_PATH)
    try:
        payload = yaml.safe_load(roster_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"RIPE roster cannot be loaded: {roster_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("RIPE roster must be a mapping")
    if payload.get("schema_version") != ROSTER_SCHEMA:
        raise ValueError(f"RIPE roster schema_version must be {ROSTER_SCHEMA}")
    if payload.get("review_status") != "approved":
        raise ValueError("RIPE roster review_status must be approved")
    if payload.get("internal_only") is not True:
        raise ValueError("RIPE roster must remain internal_only")
    return payload


def _atlas_measurements(roster: dict[str, Any]) -> list[dict[str, Any]]:
    section = roster.get("ripe_atlas")
    measurements = section.get("measurements") if isinstance(section, dict) else None
    if not isinstance(measurements, list) or not measurements:
        raise ValueError("RIPE Atlas reviewed measurement roster is empty")
    normalized: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for item in measurements:
        if not isinstance(item, dict):
            raise ValueError("RIPE Atlas measurement entry must be a mapping")
        try:
            measurement_id = int(item["measurement_id"])
            address_family = int(item["address_family"])
            interval_seconds = int(item.get("interval_seconds", 300))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("RIPE Atlas measurement id/AF/interval is invalid") from exc
        measurement_type = str(item.get("measurement_type", "")).lower()
        if measurement_id <= 0 or measurement_id in seen_ids:
            raise ValueError("RIPE Atlas measurement IDs must be unique positive integers")
        if measurement_type != "ping":
            raise ValueError("RIPE Atlas safe MVP accepts reviewed ping measurements only")
        if address_family not in (4, 6) or interval_seconds < 60:
            raise ValueError("RIPE Atlas address_family/interval is outside the safe contract")
        probes = item.get("probes")
        if not isinstance(probes, list) or not probes:
            raise ValueError(f"RIPE Atlas measurement {measurement_id} has no reviewed probes")
        normalized_probes: list[dict[str, int | None]] = []
        probe_ids: set[int] = set()
        for probe in probes:
            if not isinstance(probe, dict):
                raise ValueError("RIPE Atlas probe entry must be a mapping")
            try:
                probe_id = int(probe["probe_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("RIPE Atlas probe_id is invalid") from exc
            if probe_id <= 0 or probe_id in probe_ids:
                raise ValueError("RIPE Atlas probe IDs must be unique within a measurement")
            asn_value = probe.get("asn")
            asn = int(asn_value) if asn_value not in (None, "") else None
            normalized_probes.append({"probe_id": probe_id, "asn": asn})
            probe_ids.add(probe_id)
        seen_ids.add(measurement_id)
        normalized.append({
            "measurement_id": measurement_id,
            "measurement_type": measurement_type,
            "address_family": address_family,
            "interval_seconds": interval_seconds,
            "target_group": str(item.get("target_group") or "reviewed_target"),
            "probes": normalized_probes,
        })
    return normalized


def _bucket_bounds(timestamp: int | float, seconds: int = 300) -> tuple[datetime, datetime]:
    start_epoch = int(float(timestamp)) // seconds * seconds
    start = datetime.fromtimestamp(start_epoch, UTC)
    return start, start + timedelta(seconds=seconds)


def _normalize_results(
    payloads: dict[int, list[dict[str, Any]]],
    measurements: list[dict[str, Any]],
    *,
    run_id: str,
    collected_at: str,
) -> tuple[list[dict[str, Any]], int, int, str | None]:
    """Deduplicate results and build conservative 5-minute AF aggregates."""
    measurement_by_id = {item["measurement_id"]: item for item in measurements}
    probe_asn: dict[tuple[int, int], int | None] = {}
    expected_probes: dict[int, set[int]] = defaultdict(set)
    expected_asns: dict[int, set[int]] = defaultdict(set)
    native_intervals: dict[int, list[int]] = defaultdict(list)
    target_groups: dict[int, set[str]] = defaultdict(set)
    for item in measurements:
        af = item["address_family"]
        native_intervals[af].append(item["interval_seconds"])
        target_groups[af].add(item["target_group"])
        for probe in item["probes"]:
            probe_id = int(probe["probe_id"])
            expected_probes[af].add(probe_id)
            probe_asn[(item["measurement_id"], probe_id)] = probe["asn"]
            if probe["asn"] is not None:
                expected_asns[af].add(int(probe["asn"]))

    buckets: dict[tuple[int, datetime], dict[str, Any]] = {}
    seen: set[tuple[int, int, int, int, str]] = set()
    rejected = 0
    duplicates = 0
    latest: str | None = None
    for measurement_id, rows in payloads.items():
        measurement = measurement_by_id.get(measurement_id)
        if not measurement:
            rejected += len(rows)
            continue
        allowed_probe_ids = {int(item["probe_id"]) for item in measurement["probes"]}
        expected_af = measurement["address_family"]
        for row in rows:
            if not isinstance(row, dict):
                rejected += 1
                continue
            try:
                row_measurement_id = int(row.get("msm_id"))
                probe_id = int(row.get("prb_id"))
                timestamp = int(float(row.get("timestamp")))
                address_family = int(row.get("af"))
            except (TypeError, ValueError):
                rejected += 1
                continue
            result_type = str(row.get("type") or row.get("msm_name") or "ping").lower()
            if result_type == "ping":
                pass
            elif result_type == "ping6":
                result_type = "ping"
            if (
                row_measurement_id != measurement_id
                or probe_id not in allowed_probe_ids
                or address_family != expected_af
                or result_type != "ping"
            ):
                rejected += 1
                continue
            identity = (measurement_id, probe_id, timestamp, address_family, result_type)
            if identity in seen:
                duplicates += 1
                continue
            seen.add(identity)
            start, end = _bucket_bounds(timestamp)
            bucket = buckets.setdefault((address_family, start), {
                "end": end,
                "reported_probes": set(),
                "successful_probes": set(),
                "successful_asns": set(),
                "sent": 0,
                "received": 0,
                "rtts": [],
                "latest": None,
                "measurement_ids": set(),
            })
            bucket["reported_probes"].add(probe_id)
            bucket["measurement_ids"].add(measurement_id)
            sent = _number(row.get("sent"))
            received = _number(row.get("rcvd"))
            if sent is not None and sent >= 0:
                bucket["sent"] += int(sent)
            if received is not None and received >= 0:
                bucket["received"] += int(received)
            if received is not None and received > 0:
                bucket["successful_probes"].add(probe_id)
                asn = probe_asn.get((measurement_id, probe_id))
                if asn is not None:
                    bucket["successful_asns"].add(asn)
                avg = _number(row.get("avg"))
                if avg is not None and avg >= 0:
                    bucket["rtts"].append(float(avg))
            observed = _iso(timestamp)
            if observed and (bucket["latest"] is None or observed > bucket["latest"]):
                bucket["latest"] = observed
            if observed and (latest is None or observed > latest):
                latest = observed

    observations: list[dict[str, Any]] = []
    for (af, start), bucket in sorted(buckets.items()):
        expected_probe_count = len(expected_probes[af])
        expected_asn_count = len(expected_asns[af])
        values = {
            f"probe_connectivity_ratio_ipv{af}": (
                len(bucket["reported_probes"]) / expected_probe_count if expected_probe_count else None,
                "ratio",
                len(bucket["reported_probes"]),
            ),
            f"ping_success_ratio_ipv{af}": (
                bucket["received"] / bucket["sent"] if bucket["sent"] > 0 else None,
                "ratio",
                len(bucket["reported_probes"]),
            ),
            f"median_rtt_ms_ipv{af}": (
                statistics.median(bucket["rtts"]) if bucket["rtts"] else None,
                "milliseconds",
                len(bucket["rtts"]),
            ),
            f"reachable_asn_ratio_ipv{af}": (
                len(bucket["successful_asns"]) / expected_asn_count if expected_asn_count else None,
                "ratio",
                len(bucket["successful_asns"]),
            ),
        }
        stale_after = max(
            config.RIPE_ATLAS_STALE_AFTER_SECONDS,
            max(native_intervals[af], default=300) * 3,
        )
        for signal, (value, unit, sample_count) in values.items():
            observed_at = bucket["end"].isoformat()
            observations.append({
                "_type": "observation",
                "run_id": run_id,
                "source": SOURCE,
                "evidence_family": EVIDENCE_FAMILY,
                "source_observation_id": f"ripe_atlas:{signal}:TW:{observed_at}",
                "entity_type": "country",
                "entity_id": "TW",
                "entity_name": "Taiwan",
                "signal": signal,
                "observed_at": observed_at,
                "window_start": start.isoformat(),
                "window_end": bucket["end"].isoformat(),
                "value": value,
                "unit": unit,
                "baseline_value": None,
                "change_ratio": None,
                "reported_status": "unknown",
                "incident_kind": None,
                "confidence": None,
                "sample_count": sample_count,
                "stale_after_seconds": stale_after,
                "source_updated_at": bucket["latest"],
                "collected_at": collected_at,
                "quality_flags": {"missing_value": True} if value is None else {},
                "metadata": {
                    "address_family": af,
                    "measurement_ids": sorted(bucket["measurement_ids"]),
                    "target_groups": sorted(target_groups[af]),
                    "expected_probe_count": expected_probe_count,
                    "expected_asn_count": expected_asn_count,
                    "independence_group": "ripe_ncc",
                    "scope": "single_provider_family",
                    "public_visibility": "internal_only",
                },
            })
    return observations, rejected, duplicates, latest


class RipeAtlasInternetHealthCollector(BaseCollector):
    name = "ripe_atlas_internet_health"
    interval_minutes = config.RIPE_ATLAS_INTERNET_HEALTH_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (internet-health-ripe-atlas)"})
        if config.RIPE_ATLAS_API_KEY:
            self._session.headers["Authorization"] = f"Key {config.RIPE_ATLAS_API_KEY}"

    def require_db_write(self) -> bool:
        return True

    def _get_results(self, measurement: dict[str, Any], start: datetime, stop: datetime) -> list[dict[str, Any]]:
        probe_ids = ",".join(str(item["probe_id"]) for item in measurement["probes"])
        response = self._session.get(
            f"{ATLAS_BASE}/measurements/{measurement['measurement_id']}/results/",
            params={
                "start": int(start.timestamp()),
                "stop": int(stop.timestamp()),
                "probe_ids": probe_ids,
                "public_only": "true",
                "format": "json",
            },
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("RIPE Atlas results response must be a list")
        return [item for item in payload if isinstance(item, dict)]

    def collect(self) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        started = datetime.now(UTC)
        requested_from = started - timedelta(minutes=config.RIPE_ATLAS_LOOKBACK_MINUTES)
        collected_at = started.isoformat()
        try:
            roster = load_ripe_roster()
            measurements = _atlas_measurements(roster)
        except Exception as exc:
            run = _source_run(
                run_id=run_id, source=SOURCE, started_at=collected_at,
                finished_at=datetime.now(UTC).isoformat(), status="failed",
                requested_from=requested_from.isoformat(), requested_to=started.isoformat(),
                source_updated_at=None, received=0, written=0, rejected=0,
                error_code="config_missing", error_message=_safe_error(exc),
                metadata={"public_visibility": "internal_only"},
            )
            return {
                "data": [run], "raw_payload": {}, "run_id": run_id,
                "run_status": "failed", "observation_count": 0,
                "_collector_error": "RIPE Atlas reviewed roster is unavailable",
            }

        raw_payload: dict[str, Any] = {}
        payloads: dict[int, list[dict[str, Any]]] = {}
        endpoint_status: dict[str, Any] = {}
        for measurement in measurements:
            measurement_id = measurement["measurement_id"]
            try:
                rows = self._get_results(measurement, requested_from, started)
                payloads[measurement_id] = rows
                raw_payload[str(measurement_id)] = rows
                endpoint_status[str(measurement_id)] = {"status": "succeeded", "records": len(rows)}
            except Exception as exc:
                endpoint_status[str(measurement_id)] = {
                    "status": "failed",
                    "error_code": "http_error" if isinstance(exc, requests.RequestException) else "parse_error",
                    "error": _safe_error(exc),
                }

        observations, rejected, duplicates, source_updated_at = _normalize_results(
            payloads, measurements, run_id=run_id, collected_at=collected_at
        )
        succeeded = sum(item["status"] == "succeeded" for item in endpoint_status.values())
        failed = len(endpoint_status) - succeeded
        status = "succeeded" if failed == 0 else ("partial" if succeeded else "failed")
        error_code = None
        if status == "failed":
            error_code = "http_error"
        elif status == "partial":
            error_code = "endpoint_partial"
        elif not observations:
            error_code = "empty"
        if source_updated_at:
            source_dt = datetime.fromisoformat(source_updated_at)
            if (started - source_dt).total_seconds() > config.RIPE_ATLAS_STALE_AFTER_SECONDS:
                status = "partial"
                error_code = "stale"
        elif observations:
            status = "partial"
            error_code = "stale"
        received = sum(len(rows) for rows in payloads.values())
        run = _source_run(
            run_id=run_id, source=SOURCE, started_at=collected_at,
            finished_at=datetime.now(UTC).isoformat(), status=status,
            requested_from=requested_from.isoformat(), requested_to=started.isoformat(),
            source_updated_at=source_updated_at, received=received, written=len(observations),
            rejected=rejected, error_code=error_code, error_message=None,
            metadata={
                "endpoints": endpoint_status,
                "roster_schema": roster["schema_version"],
                "roster_version": roster.get("version"),
                "roster_sha256": _digest(roster),
                "duplicate_results": duplicates,
                "overlap_minutes": config.RIPE_ATLAS_OVERLAP_MINUTES,
                "public_visibility": "internal_only",
                "independence_group": "ripe_ncc",
            },
        )
        result = {
            "data": [run, *observations],
            "raw_payload": raw_payload,
            "run_id": run_id,
            "run_status": status,
            "observation_count": len(observations),
            "endpoint_status": endpoint_status,
        }
        if status == "failed":
            result["_collector_error"] = "all RIPE Atlas measurement endpoints failed"
        return result
