"""農業部動物認領養完整快照收集器。

來源沒有可靠的事件流，也不能從名單消失推論動物已被認養。因此每次完整
下載都先建立一個不可變快照；current 狀態、每日存量和連續缺席判定全部由
gis-platform 的 ``live.finalize_animal_adoption_snapshot(run_id)`` 在同一筆交易中
完成。原始 JSON 僅保存在本地 collector archive，供 S3 冷備份與日後重算。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from collectors.base import BaseCollector, TAIPEI_TZ


SOURCE_DATASET_ID = "datagov:85903"
SOURCE_RESOURCE_ID = "QcbUEzN6E6DL"
ANIMAL_ADOPTION_URL = (
    "https://data.moa.gov.tw/Service/OpenData/TransService.aspx"
    f"?UnitId={SOURCE_RESOURCE_ID}&IsTransData=1"
)


def _text(value: Any) -> str | None:
    """將官方空字串正規化為 NULL；其餘一律保留來源字面值。"""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _non_null_key(value: Any, sentinel: str = "__unknown__") -> str:
    """daily UNIQUE grain 不能含 NULL，否則 PostgreSQL 不會發生衝突更新。"""
    return _text(value) or sentinel


def _canonical_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _parse_source_date(value: Any) -> str | None:
    """把可辨識日期正規化成 ISO date；不猜測無法辨識的來源字串。"""
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text.replace('/', '-')).isoformat()
    except ValueError:
        return None


def _parse_source_datetime(value: Any) -> str | None:
    """官方 animal_update 無 timezone 時，依來源所在地明確附上 +08。"""
    text = _text(value)
    if not text:
        return None
    normalized = text.replace('/', '-').replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI_TZ)
    return parsed.isoformat()


class AnimalAdoptionCollector(BaseCollector):
    """全國待認養名單，每日完整快照（預設停用）。"""

    name = "animal_adoption"
    interval_minutes = config.ANIMAL_ADOPTION_INTERVAL
    COLLECT_TIMEOUT = 120

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (animal-adoption-snapshot)",
            "Accept": "application/json, text/plain, */*",
        })
        retry = Retry(
            total=config.ANIMAL_ADOPTION_HTTP_RETRIES,
            connect=config.ANIMAL_ADOPTION_HTTP_RETRIES,
            read=config.ANIMAL_ADOPTION_HTTP_RETRIES,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(("GET",)),
            raise_on_status=False,
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    def _failed_result(self, now: datetime, reason: str) -> dict:
        """保留失敗 ledger，但絕不產 snapshot/current/daily 的假零值。"""
        return {
            "data": [],
            "run_id": str(uuid.uuid4()),
            "run_status": "failed",
            "is_complete": False,
            "row_count": 0,
            "snapshot_date": now.date().isoformat(),
            "collected_at": now.isoformat(),
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_observed_at": None,
            "payload_sha256": None,
            "quality_note": reason,
            "raw_payload": None,
            "_collector_error": reason,
        }

    def _normalize(self, row: dict[str, Any], run_id: str, now: datetime) -> dict[str, Any]:
        """只轉欄位名稱／空值，不把來源異常日期矯造成看似可信的資料。"""
        animal_id = _text(row.get("animal_id"))
        assert animal_id, "完整性 gate 已保證 animal_id 存在"

        closeddate_raw = _text(row.get("animal_closeddate"))
        closeddate = _parse_source_date(closeddate_raw)
        date_flags: list[str] = []
        if closeddate_raw == "2999-12-31":
            closeddate = None  # 官方的 open-listing sentinel，不是實際結案日

        source_opendate_raw = _text(row.get("animal_opendate"))
        source_opendate = _parse_source_date(source_opendate_raw)
        if source_opendate_raw and source_opendate is None:
            date_flags.append("animal_opendate_unparseable")
        elif source_opendate and source_opendate.startswith("1900-"):
            source_opendate = None
            date_flags.append("animal_opendate_1900_sentinel")
        elif source_opendate and source_opendate > now.date().isoformat():
            date_flags.append("animal_opendate_future")

        source_update_raw = _text(row.get("animal_update"))
        source_observed_at = _parse_source_datetime(source_update_raw)
        if source_update_raw and source_observed_at is None:
            date_flags.append("animal_update_unparseable")
        record_hash = hashlib.sha256(_canonical_json(row).encode("utf-8")).hexdigest()
        return {
            "_type": "animal",
            "run_id": run_id,
            "snapshot_date": now.date().isoformat(),
            "source_dataset_id": SOURCE_DATASET_ID,
            # 保留 namespace 欄位，未來上游若重用 animal_id 不會破壞唯一語意。
            "source_record_key": f"{SOURCE_DATASET_ID}:{animal_id}",
            "animal_id_raw": animal_id,
            "animal_subid_raw": _text(row.get("animal_subid")),
            "shelter_id": _non_null_key(row.get("animal_shelter_pkid")),
            "shelter_name": _text(row.get("shelter_name")),
            "county_code": _non_null_key(row.get("animal_area_pkid")),
            "animal_kind": _non_null_key(row.get("animal_kind")),
            "animal_sex": _text(row.get("animal_sex")),
            "animal_bodytype": _text(row.get("animal_bodytype")),
            "animal_colour": _text(row.get("animal_colour")),
            "animal_age": _text(row.get("animal_age")),
            "source_status": _text(row.get("animal_status")),
            "animal_opendate": source_opendate,
            "animal_closeddate": closeddate,
            "animal_foundplace": _text(row.get("animal_foundplace")),
            "animal_place": _text(row.get("animal_place")),
            "animal_breed": _text(row.get("animal_Variety") or row.get("animal_variety")),
            "animal_sterilization": _text(row.get("animal_sterilization")),
            "animal_bacterin": _text(row.get("animal_bacterin")),
            "animal_title": _text(row.get("animal_title")),
            "animal_remark": _text(row.get("animal_remark")),
            "animal_caption": _text(row.get("animal_caption")),
            # 僅保留 URL；圖片授權／下載另行決策。
            "image_url": _text(row.get("album_file")),
            "shelter_address": _text(row.get("shelter_address")),
            "shelter_tel": _text(row.get("shelter_tel")),
            "record_hash": record_hash,
            "source_observed_at": source_observed_at,
            "quality_flags": date_flags,
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        try:
            response = self._session.get(ANIMAL_ADOPTION_URL, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            raw_payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            return self._failed_result(now, f"http_or_json_error: {exc}")

        if not isinstance(raw_payload, list):
            return self._failed_result(now, "incomplete: root is not a JSON array")
        if len(raw_payload) < config.ANIMAL_ADOPTION_MIN_ROWS:
            return self._failed_result(
                now,
                f"incomplete: {len(raw_payload)} rows below minimum {config.ANIMAL_ADOPTION_MIN_ROWS}",
            )
        if not all(isinstance(row, dict) for row in raw_payload):
            return self._failed_result(now, "incomplete: non-object row present")

        animal_ids = [_text(row.get("animal_id")) for row in raw_payload]
        if any(animal_id is None for animal_id in animal_ids):
            return self._failed_result(now, "incomplete: missing animal_id")
        if len(set(animal_ids)) != len(animal_ids):
            return self._failed_result(now, "incomplete: duplicate animal_id")

        run_id = str(uuid.uuid4())
        normalized = [self._normalize(row, run_id, now) for row in raw_payload]
        payload_sha256 = hashlib.sha256(response.content).hexdigest()
        opendate_flagged_rows = sum(
            any(flag.startswith("animal_opendate_") for flag in row["quality_flags"])
            for row in normalized
        )
        return {
            "data": normalized,
            "run_id": run_id,
            "run_status": "complete",
            "is_complete": True,
            "row_count": len(normalized),
            "snapshot_date": now.date().isoformat(),
            "collected_at": now.isoformat(),
            "source_dataset_id": SOURCE_DATASET_ID,
            # 上游沒有可信的整批觀測時刻；不能把 animal_update 偽裝成 run-level timestamp。
            "source_observed_at": None,
            "payload_sha256": payload_sha256,
            "quality_note": (
                f"full JSON array; {opendate_flagged_rows} rows have animal_opendate quality flags; "
                "animal_closeddate=2999-12-31 normalized to NULL"
            ),
            # LocalStorage → tasks/archive.py → S3；DB 刻意不重複存 raw payload。
            "raw_payload": raw_payload,
        }


if __name__ == "__main__":
    AnimalAdoptionCollector().run()
