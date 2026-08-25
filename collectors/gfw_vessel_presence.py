"""Global Fishing Watch 每日 vessel presence collector。

此 provider 與 AISStream / ship_ais 完全分開。GFW report API 需要申請 token，
因此預設 disabled；沒有 ``GFW_ACCESS_TOKEN`` 時不會發出任何 HTTP request。
Raw response 也不會上傳 S3，直到帳號條款明確允許永久保存（license gate）。

官方 schema：
https://globalfishingwatch.org/our-apis/documentation/docs/v3/4wings/report
dataset ``public-global-presence:latest`` 是 AIS-derived vessel presence，
不是暗船偵測或完整航跡；parser 刻意接受不同版本的 entries/欄位別名。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

import config
from .base import BaseCollector, TAIPEI_TZ

GFW_REPORT_URL = "https://gateway.api.globalfishingwatch.org/v3/4wings/report"
GFW_DATASET = "public-global-presence:latest"

# 相同五個 AIS corridor；GFW API 每次只跑一個 report，collector 順序請求各區。
DEFAULT_ZONES = (
    ("taiwan_north_east", (24.0, 119.5, 27.5, 124.5)),
    ("yonaguni_ishigaki", (23.5, 122.0, 25.0, 124.8)),
    ("miyako_okinawa", (24.0, 124.0, 27.5, 129.0)),
    ("amami", (27.0, 127.5, 30.5, 131.5)),
    ("kyushu_southwest", (30.0, 128.0, 34.0, 133.5)),
)


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _first(row: dict, *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _parse_time(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    if not text:
        return fallback
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromtimestamp(float(value), timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _number(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _unwrap_entries(payload: Any) -> list[dict]:
    """Support report JSON versions and dataset-version wrapper objects."""
    if isinstance(payload, list):
        rows: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            if any(key in item for key in ("vessel_id", "vesselId", "vesselIdRaw", "id", "ship_id")):
                rows.append(item)
            else:
                rows.extend(_unwrap_entries(item))
        return rows
    if not isinstance(payload, dict):
        return []
    if any(key in payload for key in ("vessel_id", "vesselId", "vesselIdRaw", "id", "ship_id")):
        return [payload]
    for key in ("entries", "data", "results", "rows"):
        value = payload.get(key)
        if isinstance(value, (list, dict)):
            nested = _unwrap_entries(value)
            if nested:
                return nested
    # Official examples can wrap rows under a resolved dataset key such as
    # {"public-global-presence:v3.0": [...]}; recurse only when this object is
    # not itself a vessel row.
    rows: list[dict] = []
    for value in payload.values():
        if isinstance(value, (list, dict)):
            rows.extend(_unwrap_entries(value))
    return rows


def _polygon(bbox: tuple[float, float, float, float]) -> dict:
    min_lat, min_lon, max_lat, max_lon = bbox
    return {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, min_lat], [max_lon, min_lat],
            [max_lon, max_lat], [min_lon, max_lat], [min_lon, min_lat],
        ]],
    }


def _report_body(polygon: dict) -> dict:
    """官方 POST schema：geojson 欄位是 FeatureCollection object。"""
    feature_collection = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "properties": {}, "geometry": polygon}],
    }
    return {"geojson": feature_collection}


def _next_offset(payload: Any) -> str | None:
    """Return a non-zero pagination cursor found anywhere in the response."""
    if isinstance(payload, list):
        for item in payload:
            value = _next_offset(item)
            if value is not None:
                return value
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("nextOffset", "next_offset"):
        value = payload.get(key)
        if value not in (None, "", 0, "0"):
            return str(value)
    for value in payload.values():
        nested = _next_offset(value)
        if nested is not None:
            return nested
    return None


class GFWVesselPresenceCollector(BaseCollector):
    name = "gfw_vessel_presence"
    interval_minutes = config.GFW_VESSEL_PRESENCE_INTERVAL
    COLLECT_TIMEOUT = 900

    def should_persist_local(self) -> bool:
        return config.GFW_RAW_ARCHIVE_ENABLED

    def require_db_write(self) -> bool:
        # Without an approved raw archive, DB failure must not silently fall
        # back to a local raw-response buffer.
        return True

    def __init__(self):
        super().__init__()
        self._token = config.GFW_ACCESS_TOKEN
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json", "User-Agent": "GIS-DataCollectors/gfw-vessel-presence"})

    @staticmethod
    def normalize_entries(payload: Any, *, snapshot_date: str, received_at: str, zone: str, dataset: str = GFW_DATASET) -> list[dict]:
        rows: list[dict] = []
        for index, source in enumerate(_unwrap_entries(payload)):
            vessel_id = _first(source, "vessel_id", "vesselId", "vesselIdRaw", "id", "ship_id")
            if vessel_id is None:
                continue
            vessel_id = str(vessel_id).strip()
            if not vessel_id:
                continue
            observed_at = _parse_time(_first(source, "observed_at", "date", "timestamp", "entryTimestamp", "first_transmission"), received_at)
            longitude = _number(_first(source, "longitude", "lon", "lng"))
            latitude = _number(_first(source, "latitude", "lat"))
            coords_ok = longitude is not None and latitude is not None and -180 <= longitude <= 180 and -90 <= latitude <= 90
            if not coords_ok:
                longitude = latitude = None
            raw_mmsi = _first(source, "mmsi", "MMSI")
            mmsi = str(raw_mmsi).strip() if raw_mmsi is not None else None
            if mmsi is not None and (not mmsi.isdigit() or len(mmsi) != 9):
                mmsi = None
            quality_flags = ["grid_cell_center"]
            if not coords_ok:
                quality_flags.append("missing_or_invalid_coordinates")
            normalized = {
                "snapshot_date": snapshot_date,
                "source_dataset_id": dataset,
                "vessel_id": vessel_id,
                "mmsi": mmsi,
                "observed_at": observed_at,
                "received_at": received_at,
                "ship_name": _first(source, "shipName", "ship_name", "name"),
                "vessel_type": _first(source, "vessel_type", "vesselType", "vessel_type_name"),
                "flag": _first(source, "flag", "flag_code", "flagCountry"),
                "longitude": longitude,
                "latitude": latitude,
                "presence_quality": "accepted" if coords_ok else "suspect",
                # GFW report lat/lon are grid-cell centers, not precise AIS points.
                "quality_flags": quality_flags,
                "source_properties": source,
                "zone": zone,
                "source_index": index,
            }
            normalized["record_hash"] = _canonical_hash(normalized)
            normalized["source_event_key"] = _canonical_hash({"dataset": dataset, "vessel_id": vessel_id, "observed_at": observed_at, "longitude": longitude, "latitude": latitude})
            rows.append(normalized)
        return rows

    def _fetch_report(self, polygon: dict, start: str, end: str) -> tuple[dict, str | None]:
        if not self._token:
            raise RuntimeError("GFW_ACCESS_TOKEN 未設定；GFW collector 保持 disabled")
        params = {
            "format": "JSON",
            "group-by": "VESSEL_ID",
            "temporal-resolution": "DAILY",
            "datasets[0]": GFW_DATASET,
            "date-range": f"{start},{end}",
            "spatial-aggregation": "false",
            "spatial-resolution": "HIGH",
        }
        response = self._session.post(
            config.GFW_REPORT_URL,
            params=params,
            json=_report_body(polygon),
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=config.REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        resolved = response.headers.get("x-datasets") or response.headers.get("X-Datasets")
        return response.json(), resolved

    def collect(self) -> dict:
        if not self._token:
            raise RuntimeError("GFW_ACCESS_TOKEN 未設定；跳過 live API（auth/data PoC pending）")
        now = datetime.now(timezone.utc)
        snapshot = datetime.now(timezone.utc).date() - timedelta(days=config.GFW_DATA_LAG_DAYS)
        start = snapshot.isoformat()
        end = (snapshot + timedelta(days=1)).isoformat()
        run_id = str(uuid.uuid4())
        all_rows: list[dict] = []
        responses: list[Any] = []
        resolved_versions: list[str] = []
        errors: list[str] = []
        for zone, bbox in DEFAULT_ZONES:
            try:
                payload, resolved = self._fetch_report(_polygon(bbox), start, end)
                responses.append(payload)
                if resolved:
                    resolved_versions.append(resolved)
                next_offset = _next_offset(payload)
                if next_offset is not None:
                    raise RuntimeError(
                        f"paginated response nextOffset={next_offset}; refusing truncated snapshot"
                    )
                all_rows.extend(self.normalize_entries(payload, snapshot_date=start, received_at=now.isoformat(), zone=zone))
            except Exception as exc:
                errors.append(f"{zone}: {exc}")

        dedup: dict[str, dict] = {}
        for row in all_rows:
            dedup.setdefault(row["source_event_key"], row)
        snapshots = list(dedup.values())
        for row in snapshots:
            row.pop("zone", None)
            row.pop("source_index", None)
            row["run_id"] = run_id
            row["raw_archive_key"] = None
        status = "succeeded" if not errors else ("partial" if snapshots else "failed")
        run = {
            "_type": "run", "run_id": run_id, "provider": "global_fishing_watch",
            "source_dataset_id": GFW_DATASET, "resolved_dataset_version": ";".join(sorted(set(resolved_versions))) or None,
            "snapshot_date": start, "status": status, "started_at": now.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(), "source_window_start": start,
            "source_window_end": end, "query_parameters": {"zones": [name for name, _ in DEFAULT_ZONES], "dataset": GFW_DATASET, "temporal_resolution": "DAILY", "data_lag_days": config.GFW_DATA_LAG_DAYS},
            "result_count": len(snapshots), "duplicate_count": len(all_rows) - len(snapshots),
            "rejected_count": 0, "response_sha256": _canonical_hash(responses), "archive_verified_at": None,
            "quality_summary": {"zone_count": len(DEFAULT_ZONES), "errors": errors, "auth_data_poc": "pending", "data_lag_days": config.GFW_DATA_LAG_DAYS, "location_semantics": "grid_cell_center"},
            "error_message": "; ".join(errors) if errors else None,
        }
        result = {"data": [run, *[{"_type": "snapshot", **row} for row in snapshots],], "run_id": run_id, "snapshot_date": start, "status": status, "record_count": len(snapshots)}
        if errors:
            result["_collector_error"] = "; ".join(errors)
        return result
