"""Shared normalization for fixed-station marine observations.

This module deliberately keeps producer networks separate.  It only turns
source payloads into the canonical station / long-reading contract; it never
uses distance to merge two instruments.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Iterable
from xml.etree import ElementTree as ET


MISSING_TOKENS = {"", "-99", "-99.0", "-999", "-999.0", "nan", "null", "none"}


def is_missing(value: Any) -> bool:
    return value is None or str(value).strip().lower() in MISSING_TOKENS


def number_or_none(value: Any) -> float | None:
    if is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def payload_sha256(payload: Any) -> str:
    if isinstance(payload, bytes):
        raw = payload
    elif isinstance(payload, str):
        raw = payload.encode("utf-8")
    else:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def station_uid(network: str, source_station_id: Any) -> str:
    return f"{network}:{str(source_station_id).strip()}"


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _local_name(node: ET.Element) -> str:
    return node.tag.rsplit("}", 1)[-1].lower()


def _xml_text(node: ET.Element, *names: str) -> str | None:
    wanted = {name.lower() for name in names}
    for child in node.iter():
        if _local_name(child) in wanted and child.text and child.text.strip():
            return child.text.strip()
    return None


def cwa_metadata_to_stations(xml_payload: bytes | str, collected_at: str) -> list[dict]:
    """Parse O-B0076 XML defensively; CWA has changed element casing before."""
    root = ET.fromstring(xml_payload)
    stations: list[dict] = []
    seen: set[str] = set()
    for node in root.iter():
        if _local_name(node) not in {"location", "station"}:
            continue
        source_id = _xml_text(node, "stationID", "stationId", "station_id")
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        lon = number_or_none(_xml_text(node, "StationLongitude", "longitude", "lon"))
        lat = number_or_none(_xml_text(node, "StationLatitude", "latitude", "lat"))
        if lon is None or lat is None:
            continue
        status = _xml_text(node, "StationStatus", "stationStatus", "status")
        stations.append({
            "station_uid": station_uid("cwa", source_id),
            "source_network": "cwa",
            "source_station_id": source_id,
            "origin_org": _xml_text(node, "StationChargeIns", "affiliation", "authority", "organization") or "unknown",
            "distribution_org": "CWA",
            "station_type": (_xml_text(node, "StationAttribute", "stationType", "type") or "marine_station").lower(),
            "name_zh": _xml_text(node, "StationName", "locationName", "stationName", "name") or source_id,
            "aliases": [],
            "longitude": lon,
            "latitude": lat,
            "observed_elements": [x.strip() for x in (_xml_text(node, "ObservedPropertyNames") or "").split(",") if x.strip()],
            "source_status": status,
            "source_url": "https://opendata.cwa.gov.tw/dataset/observation/O-B0076-001",
            "license": None,
            "provenance": {"dataset_id": "O-B0076-001"},
            "first_seen_at": collected_at,
            "last_seen_at": collected_at,
        })
    return stations


_CWA_METRICS = {
    "TideHeight": ("tide_height", "m"),
    "WaveHeight": ("wave_height", "m"),
    "WaveDirection": ("wave_direction_deg", "degree"),
    "WavePeriod": ("wave_period", "s"),
    "SeaTemperature": ("sea_temperature", "degC"),
    "StationPressure": ("station_pressure", "hPa"),
    "WindSpeed": ("wind_speed", "m/s"),
    "WindDirection": ("wind_direction_deg", "degree"),
    "MaximumWindSpeed": ("max_wind_speed", "m/s"),
    "CurrentDirection": ("current_direction_deg", "degree"),
    "CurrentSpeed": ("current_speed", "m/s"),
}


def _iter_dicts(payload: Any) -> Iterable[dict]:
    if isinstance(payload, dict):
        yield payload
        for value in payload.values():
            yield from _iter_dicts(value)
    elif isinstance(payload, list):
        for value in payload:
            yield from _iter_dicts(value)


def cwa_readings_to_long(payload: dict, collected_at: str) -> list[dict]:
    """Turn O-B0075 wide records into canonical long rows without coercing missing to 0."""
    records: list[dict] = []
    seen: set[tuple[str, str]] = set()
    digest = payload_sha256(payload)
    root = payload.get("Records") or payload.get("records") or {}
    sea = root.get("SeaSurfaceObs") or root.get("seaSurfaceObs") or {}
    locations = _as_list(sea.get("Location") or sea.get("location"))
    normalized: list[tuple[str, str, dict]] = []
    for location in locations:
        if not isinstance(location, dict):
            continue
        station = location.get("Station") or location.get("station") or {}
        source_id = station.get("StationID") or station.get("stationID")
        times = location.get("StationObsTimes") or location.get("stationObsTimes") or {}
        for point in _as_list(times.get("StationObsTime") or times.get("stationObsTime")):
            if not isinstance(point, dict):
                continue
            observed_at = point.get("DateTime") or point.get("dateTime")
            weather = dict(point.get("WeatherElements") or point.get("WeatherElement") or {})
            for nested_key in ("PrimaryAnemometer", "SecondaryAnemometer"):
                nested = weather.pop(nested_key, {}) or {}
                if isinstance(nested, dict):
                    weather.update(nested)
            if source_id and observed_at:
                normalized.append((str(source_id), str(observed_at), weather))
    if not normalized:
        for row in _iter_dicts(payload):
            source_id, observed_at = row.get("StationID"), row.get("DateTime")
            if source_id and observed_at:
                normalized.append((str(source_id), str(observed_at), row))
    for source_id, observed_at, weather in normalized:
        key = (source_id, observed_at)
        if key in seen:
            continue
        seen.add(key)
        for source_field, (metric_code, unit) in _CWA_METRICS.items():
            if source_field not in weather:
                continue
            raw = weather.get(source_field)
            missing = is_missing(raw)
            value = number_or_none(raw)
            valid = not missing and value is not None
            records.append({
                "station_uid": station_uid("cwa", source_id),
                "source_network": "cwa",
                "source_station_id": source_id,
                "observed_at": observed_at,
                "metric_code": metric_code,
                "depth_key": "surface",
                "value_raw": None if raw is None else str(raw),
                "value_numeric": value,
                "unit_source": unit,
                "unit_canonical": unit,
                "vertical_datum": None,
                "is_missing": missing,
                "is_valid": valid,
                "missing_reason": "source_missing_sentinel" if missing else ("non_numeric_value" if not valid else None),
                "source_status": None,
                "quality_flags": {"missing": missing, "valid": valid},
                "payload_sha256": digest,
                "collected_at": collected_at,
                "longitude": None,
                "latitude": None,
            })
    return records


_ISOHE_METRICS = {
    "wave": {"Hs_m": ("wave_height", "m"), "Tp_sec": ("wave_period", "s"), "Wave_Direction_degree": ("wave_direction_deg", "degree"), "Direction_deg": ("wave_direction_deg", "degree")},
    "current": {"Velocity_cms": ("current_speed", "cm/s"), "Velocity_Direction_degree": ("current_direction_deg", "degree"), "Current_Direction": ("current_direction_deg", "degree"), "Direction_deg": ("current_direction_deg", "degree")},
    "wind": {"Wind_Speed_ms": ("wind_speed", "m/s"), "Wind_Direction_degree": ("wind_direction_deg", "degree"), "Wind_Direction": ("wind_direction_deg", "degree"), "Direction_deg": ("wind_direction_deg", "degree")},
    "tide": {"Tide_TWVD_m": ("tide_twvd", "m"), "Tide_CDL_m": ("tide_cdl", "m"), "Tide_REF_m": ("tide_ref", "m")},
}


def isohe_payload_to_long(port_code: str, kind: str, payload: Any, collected_at: str) -> tuple[list[dict], list[dict]]:
    """Normalize one ISOHE endpoint.  Tide datums remain distinct metric codes."""
    envelopes = payload if isinstance(payload, list) else [payload]
    rows: list[tuple[dict, dict]] = []
    for envelope in envelopes:
        if not isinstance(envelope, dict):
            continue
        nested = envelope.get("Datas") or envelope.get("data") or envelope.get("Data")
        if isinstance(nested, list):
            rows.extend((row, envelope) for row in nested if isinstance(row, dict))
        else:
            rows.append((envelope, envelope))
    station_id = f"{port_code}:{kind}"
    digest = payload_sha256(payload)
    out: list[dict] = []
    first_lon: float | None = None
    first_lat: float | None = None
    for row, envelope in rows:
        observed_at = row.get("DateTime") or row.get("ObsTime") or row.get("Time") or row.get("datetime")
        if not observed_at:
            continue
        observed_text = str(observed_at).strip()
        if len(observed_text) == 14 and observed_text.isdigit():
            observed_at = f"{observed_text[:4]}-{observed_text[4:6]}-{observed_text[6:8]}T{observed_text[8:10]}:{observed_text[10:12]}:{observed_text[12:14]}+08:00"
        lon = number_or_none(row.get("Station_Longitude") or row.get("Longitude") or row.get("lon") or envelope.get("Station_Longitude") or envelope.get("Longitude") or envelope.get("lon"))
        lat = number_or_none(row.get("Station_Latitude") or row.get("Latitude") or row.get("lat") or envelope.get("Station_Latitude") or envelope.get("Latitude") or envelope.get("lat"))
        if first_lon is None and first_lat is None and lon is not None and lat is not None:
            first_lon, first_lat = lon, lat
        seen_metrics: set[str] = set()
        for source_field, (metric_code, unit) in _ISOHE_METRICS.get(kind, {}).items():
            if source_field not in row or metric_code in seen_metrics:
                continue
            seen_metrics.add(metric_code)
            raw = row.get(source_field)
            missing = is_missing(raw)
            value = number_or_none(raw)
            valid = not missing and value is not None
            out.append({
                "station_uid": station_uid("isohe", f"{port_code.lower()}:{kind.lower()}"),
                "source_network": "isohe", "source_station_id": station_id,
                "observed_at": observed_at, "metric_code": metric_code, "depth_key": "surface",
                "value_raw": None if raw is None else str(raw),
                "value_numeric": value / 100 if source_field == "Velocity_cms" and value is not None else value,
                "unit_source": unit, "unit_canonical": "m/s" if source_field == "Velocity_cms" else unit,
                "vertical_datum": metric_code.removeprefix("tide_").upper() if metric_code.startswith("tide_") else None,
                "is_missing": missing, "is_valid": valid,
                "missing_reason": "source_missing_sentinel" if missing else ("non_numeric_value" if not valid else None),
                "source_status": None,
                "quality_flags": {"missing": missing, "valid": valid}, "payload_sha256": digest,
                "collected_at": collected_at, "longitude": lon, "latitude": lat,
            })
    if first_lon is None or first_lat is None:
        return [], []
    for row in out:
        if row["longitude"] is None or row["latitude"] is None:
            row["longitude"], row["latitude"] = first_lon, first_lat
    stations = [{
        "station_uid": station_uid("isohe", f"{port_code.lower()}:{kind.lower()}"),
        "source_network": "isohe", "source_station_id": station_id,
        "origin_org": "ISOHE/port", "distribution_org": "ISOHE/port",
        "station_type": "port_sensor", "name_zh": station_id,
        "aliases": [], "longitude": first_lon, "latitude": first_lat,
        "observed_elements": sorted({metric for metric, _unit in _ISOHE_METRICS.get(kind, {}).values()}), "source_status": None,
        "source_url": "https://isohe.ihmt.gov.tw/opendata/", "license": None,
        "provenance": {"port": port_code, "kind": kind},
        "first_seen_at": collected_at, "last_seen_at": collected_at,
    }]
    return stations, out
