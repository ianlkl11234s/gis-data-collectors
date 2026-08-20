"""低頻動物福利服務據點快照（預設停用）。

三個來源都是完整清單快照；完整 raw 留在 LocalStorage，交由 archive.py 歸檔，
normalized rows 僅保留公開機構資訊，不把姓名欄寫入 Supabase。
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from collectors.base import BaseCollector, TAIPEI_TZ


MOA_8705 = "datagov:8705"
MOA_97070 = "datagov:97070"
VET_URL = "https://data.moa.gov.tw/Service/OpenData/DataFileService.aspx?IsTransData=1&UnitId=078"
PET_BUSINESS_URL = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?IsTransData=1&UnitId=fNT9RMo8PQRO"
PROTECTION_OFFICES_URL = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=FczRQaLNjcvP&IsTransData=1"

COUNTY_CODES = {
    "臺北市": "A", "新北市": "F", "桃園市": "H", "臺中市": "B", "臺南市": "D",
    "高雄市": "E", "基隆市": "C", "新竹市": "O", "新竹縣": "J", "苗栗縣": "K",
    "彰化縣": "N", "南投縣": "M", "雲林縣": "P", "嘉義市": "I", "嘉義縣": "Q",
    "屏東縣": "T", "宜蘭縣": "G", "花蓮縣": "U", "臺東縣": "V", "澎湖縣": "X",
    "金門縣": "W", "連江縣": "Z",
}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _date(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10]


def _hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _canonical_entity_key(*parts: Any) -> str:
    value = "|".join(
        re.sub(r"[\s　]+", "", (_text(part) or "").replace("台", "臺")).casefold()
        for part in parts
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _county_from_address(address: Any) -> tuple[str | None, str | None]:
    text = _text(address) or ""
    for name, code in COUNTY_CODES.items():
        if name in text:
            return code, name
    return None, None


class _AnimalWelfarePointCollector(BaseCollector):
    source_dataset_id: str
    source_url: str
    point_type: str
    min_rows: int
    interval_minutes: int
    required_fields: tuple[str, ...]
    key_field: str

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (animal-welfare-points)",
            "Accept": "application/json, text/plain, */*",
        })
        retry = Retry(total=config.ANIMAL_WELFARE_POINTS_HTTP_RETRIES,
                      connect=config.ANIMAL_WELFARE_POINTS_HTTP_RETRIES,
                      read=config.ANIMAL_WELFARE_POINTS_HTTP_RETRIES,
                      backoff_factor=0.8, status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset(("GET",)), raise_on_status=False)
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    def _failed(self, now: datetime, reason: str) -> dict:
        run_id = str(uuid.uuid4())
        return {"data": [{"_type": "run", "run_id": run_id, "run_status": "failed",
                           "is_complete": False, "snapshot_date": now.date().isoformat(),
                           "collected_at": now.isoformat(), "source_dataset_id": self.source_dataset_id,
                           "row_count": 0, "payload_sha256": None, "quality_note": reason}],
                "run_id": run_id, "run_status": "failed", "is_complete": False,
                "snapshot_date": now.date().isoformat(), "collected_at": now.isoformat(),
                "source_dataset_id": self.source_dataset_id, "row_count": 0,
                "payload_sha256": None, "quality_note": reason, "raw_payload": None,
                "_collector_error": reason}

    def _normalize(self, row: dict[str, Any], run_id: str, now: datetime) -> dict:
        raise NotImplementedError

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        try:
            response = self._session.get(self.source_url, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            return self._failed(now, f"http_or_json_error: {exc}")
        if not isinstance(payload, list) or len(payload) < self.min_rows:
            return self._failed(now, f"incomplete: rows={len(payload) if isinstance(payload, list) else 'non-array'} min={self.min_rows}")
        if any(not isinstance(row, dict) for row in payload):
            return self._failed(now, "incomplete: non-object row")
        if any(any(_text(row.get(field)) is None for field in self.required_fields) for row in payload):
            return self._failed(now, f"incomplete: missing required fields {self.required_fields}")
        keys = [_text(row.get(self.key_field)) for row in payload]
        if len(set(keys)) != len(keys):
            return self._failed(now, "incomplete: duplicate stable key")
        run_id = str(uuid.uuid4())
        try:
            normalized = [self._normalize(row, run_id, now) for row in payload]
        except ValueError as exc:
            return self._failed(now, f"incomplete: {exc}")
        run = {"_type": "run", "run_id": run_id, "run_status": "complete", "is_complete": True,
               "snapshot_date": now.date().isoformat(), "collected_at": now.isoformat(),
               "source_dataset_id": self.source_dataset_id, "row_count": len(normalized),
               "payload_sha256": hashlib.sha256(response.content).hexdigest(),
               "quality_note": f"complete snapshot; min_rows={self.min_rows}"}
        return {"data": [run, *normalized], "run_id": run_id, "run_status": "complete",
                "is_complete": True, "snapshot_date": run["snapshot_date"],
                "collected_at": run["collected_at"], "source_dataset_id": self.source_dataset_id,
                "row_count": len(normalized), "payload_sha256": run["payload_sha256"],
                "quality_note": run["quality_note"], "raw_payload": payload}


class AnimalVeterinaryClinicsCollector(_AnimalWelfarePointCollector):
    name = "animal_veterinary_clinics"
    source_dataset_id, source_url, point_type = MOA_8705, VET_URL, "veterinary_clinic"
    interval_minutes = config.ANIMAL_VETERINARY_CLINICS_INTERVAL
    min_rows = config.ANIMAL_VETERINARY_CLINICS_MIN_ROWS
    required_fields = ("字號", "縣市", "機構名稱", "機構地址")
    key_field = "字號"

    def _normalize(self, row, run_id, now):
        status = _text(row.get("狀態"))
        return {"_type": "snapshot", "run_id": run_id, "snapshot_date": now.date().isoformat(),
                "source_dataset_id": self.source_dataset_id, "source_record_key": f"{self.source_dataset_id}:{_text(row['字號'])}",
                "canonical_entity_key": _canonical_entity_key(row.get("機構名稱"), row.get("機構地址")),
                "point_type": self.point_type,
                "service_tags": [tag for tag in (_text(row.get("執照類別")),) if tag],
                "name": _text(row["機構名稱"]), "county_code": COUNTY_CODES.get(_text(row["縣市"])),
                "county_name": _text(row["縣市"]), "address": _text(row["機構地址"]),
                "phone": _text(row.get("機構電話")), "status_norm": "active" if status == "開業" else "unknown",
                "status_raw": status, "valid_from": _date(row.get("發照日期")), "valid_to": None,
                "longitude": None, "latitude": None, "geom": None, "geocode_method": None, "geocode_confidence": None,
                "details": {"license_type": _text(row.get("執照類別"))}, "record_hash": _hash(row),
                "quality_flags": ([] if status == "開業" else ["status_unmapped_reissued"])
                + ([] if _text(row.get("機構電話")) else ["missing_phone"]),
                "collected_at": now.isoformat(), "source_observed_at": None}


class AnimalLicensedPetBusinessesCollector(_AnimalWelfarePointCollector):
    name = "animal_licensed_pet_businesses"
    source_dataset_id, source_url, point_type = MOA_97070, PET_BUSINESS_URL, "licensed_pet_business"
    interval_minutes = config.ANIMAL_LICENSED_PET_BUSINESSES_INTERVAL
    min_rows = config.ANIMAL_LICENSED_PET_BUSINESSES_MIN_ROWS
    required_fields = ("ID", "legaltype", "legalname")
    key_field = "ID"

    def _normalize(self, row, run_id, now):
        status = _text(row.get("state_flag"))
        address = _text(row["legaladdress"])
        county_code, county_name = _county_from_address(address)
        valid_to = _date(row.get("validdate"))
        today = now.date().isoformat()
        tags = [v for v in (_text(row.get("busitem")), _text(row.get("animaltype"))) if v]
        return {"_type": "snapshot", "run_id": run_id, "snapshot_date": now.date().isoformat(),
                "source_dataset_id": self.source_dataset_id, "source_record_key": f"{self.source_dataset_id}:{_text(row['ID'])}",
                "canonical_entity_key": _canonical_entity_key(row.get("legalname"), row.get("legaladdress")),
                "point_type": self.point_type, "service_tags": tags, "name": _text(row["legalname"]),
                "county_code": county_code, "county_name": county_name,
                "address": address, "phone": None,
                "status_norm": ("expired" if valid_to < today else "active") if valid_to else "unknown",
                "status_raw": status, "valid_from": None, "valid_to": valid_to,
                "longitude": None, "latitude": None, "geom": None, "geocode_method": None, "geocode_confidence": None,
                "details": {"legaltype": _text(row.get("legaltype")), "validnum": _text(row.get("validnum")), "animaltype": _text(row.get("animaltype")),
                            "busitem": _text(row.get("busitem"))}, "record_hash": _hash(row),
                "quality_flags": (["state_code_unmapped"] if status else ["missing_state_flag"])
                + ([] if address else ["missing_address"])
                + ([] if county_code else ["county_unresolved"]),
                "collected_at": now.isoformat(), "source_observed_at": None}


class AnimalProtectionOfficesCollector(_AnimalWelfarePointCollector):
    name = "animal_protection_offices"
    source_dataset_id, source_url, point_type = "datagov:134283", PROTECTION_OFFICES_URL, "animal_protection_office"
    interval_minutes = config.ANIMAL_PROTECTION_OFFICES_INTERVAL
    min_rows = config.ANIMAL_PROTECTION_OFFICES_MIN_ROWS
    required_fields = ("ID", "AnimalProtectName", "Address")
    key_field = "ID"

    def _normalize(self, row, run_id, now):
        phone = _text(row.get("Phone"))
        return {"_type": "snapshot", "run_id": run_id, "snapshot_date": now.date().isoformat(),
            "source_dataset_id": self.source_dataset_id, "source_record_key": f"{self.source_dataset_id}:{_text(row['ID'])}",
                "canonical_entity_key": _canonical_entity_key(row.get("AnimalProtectName"), row.get("Address")),
                "point_type": self.point_type, "service_tags": [], "name": _text(row["AnimalProtectName"]),
                "county_code": _county_from_address(row["Address"])[0], "county_name": _county_from_address(row["Address"])[1], "address": _text(row["Address"]),
                "phone": phone, "status_norm": "listed", "status_raw": "listed",
                "valid_from": None, "valid_to": None, "longitude": None, "latitude": None, "geom": None,
                "geocode_method": None, "geocode_confidence": None,
                "details": {"url": _text(row.get("Url")), "seqno": _text(row.get("Seqno"))},
                "record_hash": _hash(row), "quality_flags": [] if phone else ["missing_phone"],
                "collected_at": now.isoformat(), "source_observed_at": None}
