"""ISOHE commercial-port marine sensors.  Disabled on Zeabur; run via Taiwan egress."""
from __future__ import annotations

from datetime import datetime
import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.marine_observation import isohe_payload_to_long

PORTS = ("TP", "KL", "TC", "KH", "HL", "SA", "BD", "AP")  # MZ legacy XML is deliberately excluded.
KINDS = ("wave", "current", "tide", "wind")
URL_TEMPLATE = "https://isohe.ihmt.gov.tw/opendata/{resource}?port={port}&format=JSON"
RESOURCE_BY_KIND = {"wave": "Wave", "current": "Current", "tide": "Tide", "wind": "Wind"}


class IsohePortMarineCollector(BaseCollector):
    name = "isohe_port_marine"
    interval_minutes = config.ISOHE_PORT_MARINE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (isohe-port-marine)"})

    def require_db_write(self) -> bool:
        """Do not report success when the shared canonical transaction failed."""
        return True

    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        stations: dict[str, dict] = {}
        readings: list[dict] = []
        endpoint_errors: dict[str, str] = {}
        raw_payload: dict[str, object] = {}
        for port in PORTS:
            for kind in KINDS:
                key = f"{port}:{kind}"
                try:
                    response = self._session.get(URL_TEMPLATE.format(port=port, resource=RESOURCE_BY_KIND[kind]), timeout=config.REQUEST_TIMEOUT)
                    response.raise_for_status()
                    payload = response.json()
                    raw_payload[key] = payload
                    source_stations, source_readings = isohe_payload_to_long(port, kind, payload, collected_at)
                    if not source_stations or not source_readings:
                        endpoint_errors[key] = "no valid station coordinates or readings"
                        continue
                    stations.update({s["station_uid"]: s for s in source_stations})
                    readings.extend(source_readings)
                except Exception as exc:
                    endpoint_errors[key] = str(exc)
        if len(endpoint_errors) == len(PORTS) * len(KINDS):
            raise RuntimeError("ISOHE all endpoints failed; verify Taiwan egress before declaring source unavailable")
        return {
            "data": [{"_type": "station", **s} for s in stations.values()] + [{"_type": "reading", **r} for r in readings],
            "station_count": len(stations), "reading_count": len(readings), "endpoint_errors": endpoint_errors,
            "raw_payload": raw_payload,
        }
