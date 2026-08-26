"""CWA O-B0076 station metadata + O-B0075 rolling 48h marine readings."""
from __future__ import annotations

from datetime import datetime
import ssl
import requests
from requests.adapters import HTTPAdapter

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.marine_observation import cwa_metadata_to_stations, cwa_readings_to_long

METADATA_URL = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-B0076-001"
READINGS_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-B0075-001"


class _CwaTlsAdapter(HTTPAdapter):
    """Retain CA/hostname verification while accepting CWA's legacy no-SKI chain."""

    def __init__(self):
        self._ssl_context = ssl.create_default_context()
        if hasattr(ssl, "VERIFY_X509_STRICT"):
            self._ssl_context.verify_flags &= ~ssl.VERIFY_X509_STRICT
        super().__init__()

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)


class CwaMarineObservationCollector(BaseCollector):
    name = "cwa_marine_observation"
    interval_minutes = config.CWA_MARINE_OBSERVATION_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.mount("https://", _CwaTlsAdapter())
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (cwa-marine)",
            "Authorization": config.CWA_API_KEY or "",
        })

    def require_db_write(self) -> bool:
        """A source fetch without the canonical DB transaction is not a successful run."""
        return True

    def collect(self) -> dict:
        if not config.CWA_API_KEY:
            raise RuntimeError("CWA_API_KEY 未設定")
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        metadata = self._session.get(METADATA_URL, timeout=config.REQUEST_TIMEOUT)
        metadata.raise_for_status()
        readings = self._session.get(READINGS_URL, params={"format": "JSON"}, timeout=config.REQUEST_TIMEOUT)
        readings.raise_for_status()
        stations = cwa_metadata_to_stations(metadata.content, collected_at)
        rows = cwa_readings_to_long(readings.json(), collected_at)
        if len(stations) < 70:
            raise RuntimeError(f"CWA marine station roster drift: expected baseline >=70, got {len(stations)}")
        if not rows:
            raise RuntimeError("CWA marine rolling feed parsed zero readings")
        known = {s["source_station_id"] for s in stations}
        station_positions = {s["source_station_id"]: (s["longitude"], s["latitude"]) for s in stations}
        orphan_ids = sorted({r["source_station_id"] for r in rows if r["source_station_id"] not in known})
        accepted = [r for r in rows if r["source_station_id"] in known]
        for row in accepted:
            row["longitude"], row["latitude"] = station_positions[row["source_station_id"]]
        quarantined = [r for r in rows if r["source_station_id"] not in known]
        quarantine_rows = [{
            "_type": "quarantine", "source_network": "cwa", "source_station_id": source_id,
            "reason": "reading_station_missing_from_O-B0076-001", "row_count": sum(r["source_station_id"] == source_id for r in quarantined),
            "collected_at": collected_at,
        } for source_id in orphan_ids]
        return {
            "data": [{"_type": "station", **s} for s in stations] + [{"_type": "reading", **r} for r in accepted] + quarantine_rows,
            "station_count": len(stations), "reading_count": len(accepted), "orphan_station_ids": orphan_ids,
            "orphan_count": len(orphan_ids), "raw_payload": {"metadata": metadata.text, "readings": readings.json()},
        }
