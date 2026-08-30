"""Cloudflare Radar and IODA internet-health collectors.

The two providers deliberately remain separate scheduler jobs and failure
domains.  They only share the canonical database contract.  Provider data is
evidence, not a direct assertion that Taiwan is online or offline.

This MVP does not run a cross-provider detector.  It can emit a narrow
Cloudflare ``provider_status=normal`` heartbeat only when fresh netflow data
and both event endpoints agree that the requested window is empty.  IODA
remains ``unknown`` until its alert schema is validated.  National
normal/degraded/outage classification is deliberately deferred.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

import config
from collectors.base import BaseCollector


UTC = timezone.utc
CLOUDFLARE_BASE = "https://api.cloudflare.com/client/v4/radar"
IODA_SIGNALS_URL = "https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country/{location}"
IODA_ALERTS_URL = "https://api.ioda.inetintel.cc.gatech.edu/v2/outages/alerts"
def _iso(value: Any) -> str | None:
    """Normalize ISO or Unix timestamps to timezone-aware UTC ISO strings."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 10_000_000_000:  # milliseconds
            seconds /= 1000
        dt = datetime.fromtimestamp(seconds, tz=UTC)
    else:
        text = str(value).strip()
        if not text:
            return None
        if re.fullmatch(r"\d+(?:\.\d+)?", text):
            return _iso(float(text))
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _seconds(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return default


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _digest(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "unknown").strip().lower()).strip("_") or "unknown"


def _quality_flags(**flags: bool) -> dict[str, bool]:
    return {key: value for key, value in flags.items() if value}


def _cloudflare_confidence_level(value: Any) -> float | None:
    """Map Radar's ordinal 1..5 confidence scale to canonical 0..1."""
    level = _number(value)
    if level is None or not level.is_integer():
        return None
    return {1: 0.2, 2: 0.4, 3: 0.6, 4: None, 5: 1.0}.get(int(level))


def _source_run(
    *,
    run_id: str,
    source: str,
    started_at: str,
    finished_at: str,
    status: str,
    requested_from: str | None,
    requested_to: str | None,
    source_updated_at: str | None,
    received: int,
    written: int,
    rejected: int,
    error_code: str | None,
    error_message: str | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "_type": "source_run",
        "run_id": run_id,
        "source": source,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "requested_from": requested_from,
        "requested_to": requested_to,
        "source_updated_at": source_updated_at,
        "records_received": received,
        "records_written": written,
        "records_rejected": rejected,
        "error_code": error_code,
        "error_message": error_message,
        "metadata": metadata,
    }


def _http_error_code(exc: Exception) -> str:
    if isinstance(exc, requests.Timeout):
        return "http_timeout"
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return f"http_{status}" if status else "http_error"
    if isinstance(exc, requests.RequestException):
        return "http_error"
    return "parse_error"


def _safe_error(exc: Exception) -> str:
    """Return a bounded message without request headers or credentials."""
    message = str(exc)
    token = getattr(config, "CLOUDFLARE_RADAR_API_TOKEN", "")
    if token:
        message = message.replace(token, "[redacted]")
    return f"{type(exc).__name__}: {message[:300]}"


def _is_stale(source_updated_at: str | None, reference: datetime, threshold_seconds: int) -> bool:
    if not source_updated_at:
        return False
    try:
        updated = datetime.fromisoformat(source_updated_at)
    except ValueError:
        return False
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    return (reference - updated.astimezone(UTC)).total_seconds() > threshold_seconds


def _cloudflare_event_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Read a known Radar event envelope; unknown shape is not empty evidence."""
    result: Any = payload.get("result", payload)
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    if not isinstance(result, dict):
        raise ValueError("Cloudflare event result must be an object or list")
    for name in ("annotations", "anomalies", "outages", "trafficAnomalies"):
        if name not in result:
            continue
        value = result[name]
        if not isinstance(value, list):
            raise ValueError(f"Cloudflare event field {name} must be a list")
        return [item for item in value if isinstance(item, dict)]
    raise ValueError("Cloudflare event response has no recognized event list")


def _cloudflare_meta(payload: dict[str, Any]) -> dict[str, Any]:
    result = payload.get("result")
    return result.get("meta", {}) if isinstance(result, dict) and isinstance(result.get("meta"), dict) else {}


def _cloudflare_series(payload: dict[str, Any], run_id: str, collected_at: str) -> tuple[list[dict], int]:
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Cloudflare netflows result must be an object")
    meta = _cloudflare_meta(payload)
    source_updated_at = _iso(meta.get("lastUpdated") or meta.get("last_updated"))
    confidence_info = meta.get("confidenceInfo") if isinstance(meta.get("confidenceInfo"), dict) else {}
    confidence = _cloudflare_confidence_level(confidence_info.get("level"))
    normalization = meta.get("normalization")
    records: list[dict] = []
    rejected = 0
    for series_name, series in result.items():
        if not str(series_name).lower().startswith("serie") or not isinstance(series, dict):
            continue
        timestamps = series.get("timestamps") or series.get("timestamp") or []
        values = series.get("values") or []
        if not isinstance(timestamps, list) or not isinstance(values, list):
            rejected += 1
            continue
        signal = _slug(series.get("name") or series.get("label") or f"netflows_{series_name}")
        for idx, timestamp in enumerate(timestamps):
            observed_at = _iso(timestamp)
            if not observed_at:
                rejected += 1
                continue
            raw_value = values[idx] if idx < len(values) else None
            value = _number(raw_value)
            missing = raw_value is None or value is None
            record_id = f"cloudflare:{signal}:{config.INTERNET_HEALTH_LOCATION}:{observed_at}"
            records.append({
                "_type": "observation",
                "run_id": run_id,
                "source": "cloudflare_radar",
                "evidence_family": "cloudflare",
                "source_observation_id": record_id,
                "entity_type": "country",
                "entity_id": config.INTERNET_HEALTH_LOCATION,
                "entity_name": "Taiwan",
                "signal": signal,
                "observed_at": observed_at,
                "window_start": observed_at,
                "window_end": None,
                "value": value,
                "unit": normalization or "provider_normalized",
                "baseline_value": None,
                "change_ratio": None,
                "reported_status": "unknown",
                "incident_kind": None,
                "confidence": confidence,
                "sample_count": None,
                "stale_after_seconds": config.CLOUDFLARE_RADAR_STALE_AFTER_SECONDS,
                "source_updated_at": source_updated_at,
                "collected_at": collected_at,
                "quality_flags": _quality_flags(missing_value=missing, provider_normalized=bool(normalization)),
                "metadata": {
                    "series": series_name,
                    "normalization": normalization,
                    "confidence_info": confidence_info,
                },
            })
    return records, rejected


def _cloudflare_entity(item: dict[str, Any]) -> tuple[str, str, str | None]:
    asns = item.get("asns") or item.get("asn")
    if isinstance(asns, list) and asns:
        raw = asns[0]
        if isinstance(raw, dict):
            entity_id = str(raw.get("asn") or raw.get("id") or raw.get("value") or "unknown")
            return "asn", entity_id, raw.get("name")
        return "asn", str(raw), None
    if isinstance(asns, (str, int)):
        return "asn", str(asns), None
    locations = item.get("locations") or item.get("location")
    if isinstance(locations, list) and locations:
        raw = locations[0]
        if isinstance(raw, dict):
            return "country", str(raw.get("code") or raw.get("id") or config.INTERNET_HEALTH_LOCATION), raw.get("name")
        return "country", str(raw), None
    if isinstance(locations, str):
        return "country", locations, None
    return "country", config.INTERNET_HEALTH_LOCATION, "Taiwan"


def _cloudflare_incidents(
    payload: dict[str, Any],
    endpoint: str,
    run_id: str,
    collected_at: str,
    requested_from: str | None = None,
    requested_to: str | None = None,
) -> list[dict]:
    """Normalize provider events as single-family watch observations.

    A Cloudflare anomaly/outage is important evidence, but it cannot open a
    composite incident by itself.  The >=2-family detector owns the permanent
    incident lifecycle table.
    """
    items = _cloudflare_event_items(payload)
    incidents: list[dict] = []
    for item in items:
        entity_type, entity_id, entity_name = _cloudflare_entity(item)
        provider_id = item.get("id") or item.get("uuid") or item.get("eventId")
        start = _iso(item.get("startDate") or item.get("start_time") or item.get("start")) or collected_at
        end = _iso(item.get("endDate") or item.get("end_time") or item.get("end"))
        # traffic_anomalies is a latest/paginated endpoint, not a date-range
        # endpoint.  Apply the requested collection window client-side.  The
        # same defensive filter also protects outage responses from returning
        # historical rows outside their requested range.
        window_start = _iso(requested_from)
        window_end = _iso(requested_to)
        if window_end and start > window_end:
            continue
        if window_start and end and end < window_start:
            continue
        provider_status = _slug(item.get("status") or item.get("verification") or "unverified")
        verified = provider_status in {"verified", "confirmed"}
        signal = "outage_annotation" if endpoint == "outages" else "traffic_anomaly"
        kind = "single_asn_outage" if entity_type == "asn" else "national_outage"
        incidents.append({
            "_type": "observation",
            "run_id": run_id,
            "source": "cloudflare_radar",
            "evidence_family": "cloudflare",
            "source_observation_id": str(provider_id or _digest(item)),
            "entity_type": entity_type,
            "entity_id": entity_id,
            "entity_name": entity_name,
            "signal": signal,
            "observed_at": start,
            "window_start": start,
            "window_end": end,
            "value": None,
            "unit": "provider_event",
            "baseline_value": None,
            "change_ratio": None,
            "reported_status": "watch",
            "incident_kind": kind,
            "confidence": _number(item.get("confidence")),
            "sample_count": None,
            "stale_after_seconds": config.CLOUDFLARE_RADAR_STALE_AFTER_SECONDS,
            "source_updated_at": _iso(item.get("lastUpdated") or item.get("updatedAt") or item.get("updated_at")),
            "collected_at": collected_at,
            "quality_flags": _quality_flags(provider_event=True, provider_verified=verified),
            "metadata": {"provider_status": provider_status, "provider_event": item},
        })
    return incidents


def _cloudflare_provider_normal(
    netflow_rows: list[dict[str, Any]],
    endpoint_status: dict[str, Any],
    run_id: str,
    collected_at: str,
    reference: datetime,
) -> dict[str, Any] | None:
    """Build a provider-level normal heartbeat under strict preconditions.

    A successful source run alone is never evidence that connectivity is
    normal.  The heartbeat requires a recent non-null netflow sample plus two
    successfully parsed, empty event endpoints for the same request window.
    It is still one Cloudflare evidence family, not a national conclusion.
    """
    event_endpoints = ("traffic_anomalies", "outages")
    if any(
        endpoint_status.get(name, {}).get("status") != "succeeded"
        or endpoint_status.get(name, {}).get("records") != 0
        for name in event_endpoints
    ):
        return None
    valid_rows = [row for row in netflow_rows if row.get("value") is not None]
    if not valid_rows:
        return None
    latest = max(valid_rows, key=lambda row: row["observed_at"])
    observed_at = latest["observed_at"]
    if _is_stale(observed_at, reference, config.CLOUDFLARE_RADAR_STALE_AFTER_SECONDS):
        return None
    return {
        "_type": "observation",
        "run_id": run_id,
        "source": "cloudflare_radar",
        "evidence_family": "cloudflare",
        "source_observation_id": (
            f"cloudflare:provider_status:{config.INTERNET_HEALTH_LOCATION}:{observed_at}"
        ),
        "entity_type": "country",
        "entity_id": config.INTERNET_HEALTH_LOCATION,
        "entity_name": "Taiwan",
        "signal": "provider_status",
        "observed_at": observed_at,
        "window_start": observed_at,
        "window_end": None,
        "value": None,
        "unit": "status",
        "baseline_value": None,
        "change_ratio": None,
        "reported_status": "normal",
        "incident_kind": None,
        "confidence": latest.get("confidence"),
        "sample_count": None,
        "stale_after_seconds": config.CLOUDFLARE_RADAR_STALE_AFTER_SECONDS,
        "source_updated_at": observed_at,
        "collected_at": collected_at,
        "quality_flags": {"provider_normal_heartbeat": True},
        "metadata": {
            "basis": "fresh_netflow_and_empty_event_endpoints",
            "event_endpoints": list(event_endpoints),
            "scope": "single_provider_family",
            "composite_detector": "deferred",
        },
    }


def _ioda_points(series: dict[str, Any]) -> list[tuple[str | None, Any]]:
    """Return (timestamp, value) pairs while preserving explicit nulls."""
    values = series.get("values")
    start_raw = series.get("from")
    step = _seconds(series.get("step") or series.get("nativeStep"), 0)
    start_iso = _iso(start_raw)
    start_dt = datetime.fromisoformat(start_iso) if start_iso else None
    points: list[tuple[str | None, Any]] = []
    if isinstance(values, dict):
        for key, value in values.items():
            points.append((_iso(key), value))
        return points
    if not isinstance(values, list):
        return points
    for idx, item in enumerate(values):
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            points.append((_iso(item[0]), item[1]))
            continue
        if isinstance(item, dict) and any(key in item for key in ("timestamp", "time", "ts")):
            timestamp = item.get("timestamp") or item.get("time") or item.get("ts")
            value = item.get("value") if "value" in item else item.get("values", item)
            points.append((_iso(timestamp), value))
            continue
        timestamp = (start_dt + timedelta(seconds=step * idx)).isoformat() if start_dt and step else start_iso
        points.append((timestamp, item))
    return points


def _ioda_nested_value(raw_value: Any, datasource: str) -> tuple[float | None, int | None, dict[str, Any]]:
    if not isinstance(raw_value, dict):
        return _number(raw_value), None, {}
    agg = raw_value.get("agg_values") if isinstance(raw_value.get("agg_values"), dict) else raw_value
    keys = ("loss_pct", "median", "p50", "avg", "mean", "value") if "loss" in datasource or "latency" in datasource else ("value", "median", "avg", "mean")
    value = next((_number(agg.get(key)) for key in keys if _number(agg.get(key)) is not None), None)
    sample_count = next((int(number) for key in ("probe_count", "sample_count", "count", "samples") if (number := _number(agg.get(key))) is not None), None)
    return value, sample_count, {"raw_value": raw_value}


def _ioda_observations(payload: dict[str, Any], run_id: str, collected_at: str) -> tuple[list[dict], int, str | None]:
    outer = payload.get("data")
    if not isinstance(outer, list):
        raise ValueError("IODA signals data must be a nested list")
    series_list: list[dict[str, Any]] = []
    for group in outer:
        if isinstance(group, list):
            series_list.extend(item for item in group if isinstance(item, dict))
        elif isinstance(group, dict):
            series_list.append(group)
    records: list[dict] = []
    rejected = 0
    global_latest_valid: str | None = None
    for series in series_list:
        datasource = _slug(series.get("datasource"))
        subtype = _slug(series.get("subtype")) if series.get("subtype") else ""
        signal = datasource if not subtype or subtype == "unknown" else f"{datasource}_{subtype}"
        entity_type = _slug(series.get("entityType") or "country")
        entity_id = str(series.get("entityCode") or series.get("entityFqid") or config.INTERNET_HEALTH_LOCATION)
        entity_name = series.get("entityName") or entity_id
        step = _seconds(series.get("step") or series.get("nativeStep"), 0)
        points = _ioda_points(series)
        if not points:
            rejected += 1
            continue
        parsed_points = []
        series_latest_valid: str | None = None
        for observed_at, raw_value in points:
            if not observed_at:
                rejected += 1
                continue
            value, sample_count, nested_meta = _ioda_nested_value(raw_value, datasource)
            parsed_points.append((observed_at, raw_value, value, sample_count, nested_meta))
            if value is not None:
                if series_latest_valid is None or observed_at > series_latest_valid:
                    series_latest_valid = observed_at
                if global_latest_valid is None or observed_at > global_latest_valid:
                    global_latest_valid = observed_at
        last_valid_index = max(
            (idx for idx, point in enumerate(parsed_points) if point[2] is not None),
            default=-1,
        )
        for idx, (observed_at, raw_value, value, sample_count, nested_meta) in enumerate(parsed_points):
            records.append({
                "_type": "observation",
                "run_id": run_id,
                "source": "ioda",
                "evidence_family": "ioda",
                "source_observation_id": f"ioda:{signal}:{entity_type}:{entity_id}:{observed_at}",
                "entity_type": entity_type,
                "entity_id": entity_id,
                "entity_name": entity_name,
                "signal": signal,
                "observed_at": observed_at,
                "window_start": observed_at,
                "window_end": (datetime.fromisoformat(observed_at) + timedelta(seconds=step)).isoformat() if step else None,
                "value": value,
                "unit": "percent" if "loss" in datasource else "provider_native",
                "baseline_value": None,
                "change_ratio": None,
                "reported_status": "unknown",
                "incident_kind": None,
                "confidence": None,
                "sample_count": sample_count,
                "stale_after_seconds": max(config.IODA_STALE_AFTER_SECONDS, step * 3),
                "source_updated_at": series_latest_valid,
                "collected_at": collected_at,
                "quality_flags": _quality_flags(
                    missing_value=value is None,
                    trailing_null=value is None and idx > last_valid_index,
                ),
                "metadata": {
                    "entity_fqid": series.get("entityFqid"),
                    "datasource": series.get("datasource"),
                    "subtype": series.get("subtype"),
                    "step": step,
                    "native_step": series.get("nativeStep"),
                    **nested_meta,
                },
            })
    return records, rejected, global_latest_valid


class CloudflareRadarCollector(BaseCollector):
    name = "cloudflare_radar"
    interval_minutes = config.CLOUDFLARE_RADAR_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (internet-health-cloudflare)"})
        if config.CLOUDFLARE_RADAR_API_TOKEN:
            self._session.headers["Authorization"] = f"Bearer {config.CLOUDFLARE_RADAR_API_TOKEN}"

    def require_db_write(self) -> bool:
        return True

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self._session.get(f"{CLOUDFLARE_BASE}/{path}", params=params, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Cloudflare response must be an object")
        if payload.get("success") is False:
            raise ValueError("Cloudflare API returned success=false")
        return payload

    def collect(self) -> dict:
        run_id = str(uuid.uuid4())
        started = datetime.now(UTC)
        requested_from = started - timedelta(minutes=config.INTERNET_HEALTH_LOOKBACK_MINUTES)
        collected_at = started.isoformat()
        if not config.CLOUDFLARE_RADAR_API_TOKEN:
            run = _source_run(
                run_id=run_id, source="cloudflare_radar", started_at=collected_at,
                finished_at=datetime.now(UTC).isoformat(), status="failed",
                requested_from=requested_from.isoformat(), requested_to=started.isoformat(),
                source_updated_at=None, received=0, written=0, rejected=0,
                error_code="config_missing", error_message="CLOUDFLARE_RADAR_API_TOKEN is not configured",
                metadata={"location": config.INTERNET_HEALTH_LOCATION, "endpoints": {}},
            )
            return {
                "data": [run], "raw_payload": {}, "run_id": run_id, "run_status": "failed",
                "observation_count": 0, "incident_count": 0, "endpoint_status": {},
                "_collector_error": "Cloudflare Radar credential is not configured",
            }
        data: list[dict] = []
        raw_payload: dict[str, Any] = {}
        endpoint_status: dict[str, Any] = {}
        rejected = 0
        netflow_rows: list[dict[str, Any]] = []
        endpoints = (
            ("netflows", "netflows/timeseries", {
                "location": config.INTERNET_HEALTH_LOCATION,
                "dateStart": requested_from.isoformat(),
                "dateEnd": started.isoformat(),
                "aggInterval": "15m",
                "product": "ALL",
                "format": "json",
            }),
            ("traffic_anomalies", "traffic_anomalies", {
                "location": config.INTERNET_HEALTH_LOCATION,
                "type": "LOCATION",
                "limit": 100,
                "offset": 0,
            }),
            ("outages", "annotations/outages", {
                "location": config.INTERNET_HEALTH_LOCATION,
                "dateStart": requested_from.isoformat(),
                "dateEnd": started.isoformat(),
            }),
        )
        for endpoint, path, params in endpoints:
            try:
                payload = self._get(path, params)
                raw_payload[endpoint] = payload
                if endpoint == "netflows":
                    rows, rejected_rows = _cloudflare_series(payload, run_id, collected_at)
                    netflow_rows = rows
                    data.extend(rows)
                    rejected += rejected_rows
                    count = len(rows)
                else:
                    rows = _cloudflare_incidents(
                        payload,
                        endpoint,
                        run_id,
                        collected_at,
                        requested_from.isoformat(),
                        started.isoformat(),
                    )
                    data.extend(rows)
                    count = len(rows)
                endpoint_status[endpoint] = {"status": "succeeded", "records": count}
            except Exception as exc:
                endpoint_status[endpoint] = {"status": "failed", "error_code": _http_error_code(exc), "error": _safe_error(exc)}
        provider_normal = _cloudflare_provider_normal(
            netflow_rows, endpoint_status, run_id, collected_at, started
        )
        if provider_normal:
            data.append(provider_normal)
        succeeded = sum(item["status"] == "succeeded" for item in endpoint_status.values())
        failed = len(endpoint_status) - succeeded
        status = "succeeded" if failed == 0 else ("partial" if succeeded else "failed")
        if status == "failed":
            error_code = next((item.get("error_code") for item in endpoint_status.values()), "http_error")
        elif status == "partial":
            error_code = "endpoint_partial"
        elif not data:
            error_code = "empty"
        else:
            error_code = None
        finished_at = datetime.now(UTC).isoformat()
        latest_valid_netflow = max(
            (row["observed_at"] for row in netflow_rows if row.get("value") is not None),
            default=None,
        )
        source_updated_at = latest_valid_netflow
        if status != "failed" and netflow_rows and not latest_valid_netflow:
            status = "partial"
            error_code = "stale"
        if status != "failed" and _is_stale(
            source_updated_at, started, config.CLOUDFLARE_RADAR_STALE_AFTER_SECONDS
        ):
            status = "partial"
            error_code = "stale"
        run = _source_run(
            run_id=run_id, source="cloudflare_radar", started_at=collected_at, finished_at=finished_at,
            status=status, requested_from=requested_from.isoformat(), requested_to=started.isoformat(),
            source_updated_at=source_updated_at, received=len(data), written=len(data), rejected=rejected,
            error_code=error_code, error_message=None,
            metadata={"endpoints": endpoint_status, "location": config.INTERNET_HEALTH_LOCATION},
        )
        result = {
            "data": [run, *data],
            "raw_payload": raw_payload,
            "run_id": run_id,
            "run_status": status,
            "observation_count": sum(row.get("_type") == "observation" for row in data),
            "incident_count": sum(row.get("_type") == "incident" for row in data),
            "endpoint_status": endpoint_status,
        }
        if status == "failed":
            result["_collector_error"] = "all Cloudflare Radar endpoints failed"
        return result


class IodaInternetHealthCollector(BaseCollector):
    name = "ioda_internet_health"
    interval_minutes = config.IODA_INTERNET_HEALTH_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (internet-health-ioda)"})

    def require_db_write(self) -> bool:
        return True

    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self._session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("IODA response must be an object")
        if payload.get("error"):
            raise ValueError("IODA API returned an error envelope")
        return payload

    def collect(self) -> dict:
        run_id = str(uuid.uuid4())
        started = datetime.now(UTC)
        requested_from = started - timedelta(minutes=config.INTERNET_HEALTH_LOOKBACK_MINUTES)
        collected_at = started.isoformat()
        data: list[dict] = []
        raw_payload: dict[str, Any] = {}
        endpoint_status: dict[str, Any] = {}
        rejected = 0
        source_updated_at: str | None = None
        try:
            payload = self._get(
                IODA_SIGNALS_URL.format(location=config.INTERNET_HEALTH_LOCATION),
                params={"from": int(requested_from.timestamp()), "until": int(started.timestamp())},
            )
            raw_payload["signals"] = payload
            rows, rejected, source_updated_at = _ioda_observations(payload, run_id, collected_at)
            data.extend(rows)
            endpoint_status["signals"] = {"status": "succeeded", "records": len(rows)}
        except Exception as exc:
            endpoint_status["signals"] = {"status": "failed", "error_code": _http_error_code(exc), "error": _safe_error(exc)}

        # The alerts schema has not yet been validated live.  Keep it behind an
        # explicit flag; a timeout or schema drift must not discard raw signals.
        if config.IODA_ALERTS_ENABLED:
            try:
                payload = self._get(IODA_ALERTS_URL, params={
                    "entityType": "country", "entityCode": config.INTERNET_HEALTH_LOCATION,
                    "from": int(requested_from.timestamp()), "until": int(started.timestamp()),
                })
                raw_payload["alerts"] = payload
                endpoint_status["alerts"] = {"status": "succeeded", "records": 0, "detail": "schema_unvalidated_no_ingest"}
            except Exception as exc:
                endpoint_status["alerts"] = {"status": "failed", "error_code": _http_error_code(exc), "error": _safe_error(exc)}
        else:
            endpoint_status["alerts"] = {"status": "disabled", "detail": "schema_unvalidated"}

        signal_ok = endpoint_status["signals"]["status"] == "succeeded"
        enabled_failures = [item for item in endpoint_status.values() if item["status"] == "failed"]
        status = "failed" if not signal_ok else ("partial" if enabled_failures else "succeeded")
        if status == "failed":
            error_code = endpoint_status["signals"].get("error_code", "http_error")
        elif status == "partial":
            error_code = "endpoint_partial"
        elif not data:
            error_code = "empty"
        else:
            error_code = None
        if status != "failed" and data and not source_updated_at:
            status = "partial"
            error_code = "stale"
        if status != "failed" and _is_stale(source_updated_at, started, config.IODA_STALE_AFTER_SECONDS):
            status = "partial"
            error_code = "stale"
        finished_at = datetime.now(UTC).isoformat()
        run = _source_run(
            run_id=run_id, source="ioda", started_at=collected_at, finished_at=finished_at,
            status=status, requested_from=requested_from.isoformat(), requested_to=started.isoformat(),
            source_updated_at=source_updated_at, received=len(data), written=len(data), rejected=rejected,
            error_code=error_code, error_message=None,
            metadata={
                "endpoints": endpoint_status,
                "location": config.INTERNET_HEALTH_LOCATION,
                "raw_redistribution": "prohibited_pending_legal_review",
            },
        )
        result = {
            "data": [run, *data],
            "raw_payload": raw_payload,
            "run_id": run_id,
            "run_status": status,
            "observation_count": len(data),
            "incident_count": 0,
            "endpoint_status": endpoint_status,
        }
        if status == "failed":
            result["_collector_error"] = "IODA raw signals endpoint failed"
        return result
