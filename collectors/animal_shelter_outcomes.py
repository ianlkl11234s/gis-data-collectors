"""農業部動物福利月報收集器。

兩個資料集都回傳「截至目前的完整歷史月報」，不是事件流：

* ``datagov:41236``：收容成果，canonical grain 為 年／月／縣市。
* ``datagov:73396``：收容壓力與去向，ID 唯一但官方保留同一月／縣市的
  22 筆 revision，因此絕不以 grain 去重。

每個 source 是獨立 collector job，失敗只留下 failed run ledger，不會覆寫既有
資料。完整 raw payload 由既有 LocalStorage → archive.py → S3 流程保存。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from collectors.base import BaseCollector, TAIPEI_TZ


SOURCE_OUTCOMES = "datagov:41236"
SOURCE_PRESSURE = "datagov:73396"
OUTCOMES_URL = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=DyplMIk3U1hf&IsTransData=1"
PRESSURE_URL = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=p9yPwrCs2OtC&IsTransData=1"


def _text(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _int(value: Any, field: str) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc


def _metric(value: Any) -> Any:
    """將月報數值轉成可分析型別；原始字串仍在 raw archive。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("%"):
        try:
            return float(text[:-1])
        except ValueError:
            return text
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return text


class _AnimalShelterMonthlyCollector(BaseCollector):
    source_dataset_id: str
    source_url: str
    min_rows: int
    required_metric_fields: tuple[str, ...]
    expected_min_counties = 22
    canonical_grain: bool = False

    interval_minutes = 43200
    COLLECT_TIMEOUT = 180

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (animal-welfare-monthly)",
            "Accept": "application/json, text/plain, */*",
        })
        retries = Retry(
            total=config.ANIMAL_SHELTER_HTTP_RETRIES,
            connect=config.ANIMAL_SHELTER_HTTP_RETRIES,
            read=config.ANIMAL_SHELTER_HTTP_RETRIES,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(("GET",)),
            raise_on_status=False,
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retries))

    def _failed_result(self, now: datetime, reason: str) -> dict:
        run_id = str(uuid.uuid4())
        return {
            "data": [{
                "_type": "run",
                "run_id": run_id,
                "run_status": "failed",
                "is_complete": False,
                "snapshot_date": now.date().isoformat(),
                "collected_at": now.isoformat(),
                "source_dataset_id": self.source_dataset_id,
                "source_observed_at": None,
                "row_count": 0,
                "payload_sha256": None,
                "quality_note": reason,
            }],
            "run_id": run_id,
            "run_status": "failed",
            "is_complete": False,
            "row_count": 0,
            "snapshot_date": now.date().isoformat(),
            "collected_at": now.isoformat(),
            "source_dataset_id": self.source_dataset_id,
            "payload_sha256": None,
            "quality_note": reason,
            "raw_payload": None,
            "_collector_error": reason,
        }

    def _normalize(
        self,
        row: dict[str, Any],
        run_id: str,
        now: datetime,
        revision_no: int,
        duplicate_count: int,
    ) -> dict[str, Any]:
        source_id = _int(row.get("ID"), "ID")
        source_year = _int(row.get("rpt_year"), "rpt_year")
        report_month = _int(row.get("rpt_month"), "rpt_month")
        county_code = _text(row.get("rpt_country_code"))
        county_name = _text(row.get("rpt_county"))
        if not county_code or not county_name:
            raise ValueError("missing county identity")
        metrics = {
            key: _metric(value)
            for key, value in row.items()
            if key not in {"ID", "rpt_year", "rpt_month", "rpt_country_code", "rpt_county"}
        }
        grain_key = f"{source_year}:{report_month:02d}:{county_code}"
        quality_flags = []
        if report_month == 0:
            quality_flags.append("annual_record")
        if duplicate_count > 1:
            quality_flags.append("grain_revision")
        record_hash = hashlib.sha256(
            json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {
            "_type": "outcome",
            # live staging contract（finalizer 會投影到 analytics.*_monthly）
            "run_id": run_id,
            "snapshot_date": now.date().isoformat(),
            "collected_at": now.isoformat(),
            "source_dataset_id": self.source_dataset_id,
            "source_record_key": f"{self.source_dataset_id}:{source_id}",
            "source_record_id": f"{self.source_dataset_id}:{source_id}",
            "source_id": source_id,
            "report_year": source_year + 1911,
            "source_report_year": source_year,
            "report_month": report_month,
            "county_code": county_code,
            "county_name": county_name,
            "report_grain_key": grain_key,
            "revision_no": revision_no,
            "revision_index": revision_no,
            "duplicate_grain_count": duplicate_count,
            "metrics": metrics,
            "record_hash": record_hash,
            "quality_flags": quality_flags,
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        try:
            response = self._session.get(self.source_url, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            raw_payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            return self._failed_result(now, f"http_or_json_error: {exc}")
        if not isinstance(raw_payload, list):
            return self._failed_result(now, "incomplete: root is not a JSON array")
        if len(raw_payload) < self.min_rows:
            return self._failed_result(now, f"incomplete: {len(raw_payload)} rows below minimum {self.min_rows}")
        if not all(isinstance(row, dict) for row in raw_payload):
            return self._failed_result(now, "incomplete: non-object row present")

        required = ("ID", "rpt_year", "rpt_month", "rpt_country_code", "rpt_county")
        if any(any(_text(row.get(field)) is None for field in required) for row in raw_payload):
            return self._failed_result(now, "incomplete: missing required ID/report/county field")
        if any(any(field not in row for field in self.required_metric_fields) for row in raw_payload):
            fields = ", ".join(self.required_metric_fields)
            return self._failed_result(now, f"incomplete: missing source-specific metric field ({fields})")
        county_codes = {_text(row.get("rpt_country_code")) for row in raw_payload}
        if len(county_codes) < self.expected_min_counties:
            return self._failed_result(
                now,
                f"incomplete: {len(county_codes)} counties below minimum {self.expected_min_counties}",
            )
        try:
            ids = [_int(row["ID"], "ID") for row in raw_payload]
            months = [_int(row["rpt_month"], "rpt_month") for row in raw_payload]
            years = [_int(row["rpt_year"], "rpt_year") for row in raw_payload]
        except ValueError as exc:
            return self._failed_result(now, f"incomplete: {exc}")
        if len(set(ids)) != len(ids):
            return self._failed_result(now, "incomplete: duplicate source ID")
        if any(month < 0 or month > 12 for month in months):
            return self._failed_result(now, "incomplete: report month outside 0..12")
        if not self.canonical_grain and any(month == 0 for month in months):
            return self._failed_result(now, "incomplete: pressure report month cannot be annual (0)")
        if any(year <= 0 for year in years):
            return self._failed_result(now, "incomplete: report year is not positive")

        grains = [(year, month, _text(row["rpt_country_code"])) for year, month, row in zip(years, months, raw_payload)]
        if self.canonical_grain and len(set(grains)) != len(grains):
            return self._failed_result(now, "incomplete: duplicate canonical year/month/county grain")

        run_id = str(uuid.uuid4())
        payload_sha256 = hashlib.sha256(response.content).hexdigest()
        grain_counts = defaultdict(int)
        for grain in grains:
            grain_counts[grain] += 1
        revision_counts: defaultdict[tuple[int, int, str], int] = defaultdict(int)
        normalized = []
        try:
            for row, year, month in zip(raw_payload, years, months):
                grain = (year, month, _text(row["rpt_country_code"]))
                revision_counts[grain] += 1
                normalized.append(
                    self._normalize(
                        row,
                        run_id,
                        now,
                        revision_counts[grain],
                        grain_counts[grain],
                    )
                )
        except ValueError as exc:
            return self._failed_result(now, f"incomplete: {exc}")
        run = {
            "_type": "run",
            "run_id": run_id,
            "run_status": "complete",
            "is_complete": True,
            "snapshot_date": now.date().isoformat(),
            "collected_at": now.isoformat(),
            "source_dataset_id": self.source_dataset_id,
            "source_observed_at": None,
            "row_count": len(normalized),
            "payload_sha256": payload_sha256,
            "quality_note": (
                "full historical monthly JSON array; rates converted to percentage points; "
                f"canonical_grain={self.canonical_grain}"
            ),
        }
        return {
            "data": [run, *normalized],
            "run_id": run_id,
            "run_status": "complete",
            "is_complete": True,
            "row_count": len(normalized),
            "snapshot_date": now.date().isoformat(),
            "collected_at": now.isoformat(),
            "source_dataset_id": self.source_dataset_id,
            "payload_sha256": payload_sha256,
            "quality_note": run["quality_note"],
            "raw_payload": raw_payload,
        }


class AnimalShelterOutcomesCollector(_AnimalShelterMonthlyCollector):
    """收容成果月報（41236），每月一筆縣市／月份 canonical grain。"""

    name = "animal_shelter_outcomes"
    interval_minutes = config.ANIMAL_SHELTER_OUTCOMES_INTERVAL
    source_dataset_id = SOURCE_OUTCOMES
    source_url = OUTCOMES_URL
    min_rows = config.ANIMAL_SHELTER_OUTCOMES_MIN_ROWS
    required_metric_fields = ("accept_count", "adopt_count")
    canonical_grain = True


class AnimalShelterPressureCollector(_AnimalShelterMonthlyCollector):
    """收容壓力與去向月報（73396），保留同 grain 的官方 revision。"""

    name = "animal_shelter_pressure"
    interval_minutes = config.ANIMAL_SHELTER_PRESSURE_INTERVAL
    source_dataset_id = SOURCE_PRESSURE
    source_url = PRESSURE_URL
    min_rows = config.ANIMAL_SHELTER_PRESSURE_MIN_ROWS
    required_metric_fields = ("max_stay_dog_count", "max_stay_cat_count")


if __name__ == "__main__":
    AnimalShelterOutcomesCollector().run()
