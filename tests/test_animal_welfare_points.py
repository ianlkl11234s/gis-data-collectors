from datetime import datetime
import json
import inspect

from collectors.animal_welfare_points import (
    AnimalLicensedPetBusinessesCollector,
    AnimalProtectionOfficesCollector,
    AnimalVeterinaryClinicsCollector,
)
from storage.supabase_writer import SupabaseWriter


NOW = datetime.fromisoformat("2026-08-20T12:00:00+08:00")


def _collector(cls):
    return object.__new__(cls)


class _Response:
    content = b'[{"ID":"1"}]'

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Session:
    def __init__(self, payload):
        self.payload = payload

    def get(self, *_args, **_kwargs):
        return _Response(self.payload)


def test_veterinary_normalization_excludes_personal_name():
    row = {"字號": "A123", "縣市": "臺北市", "執照類別": "獸醫師", "狀態": "開業",
           "機構名稱": "測試動物醫院", "負責獸醫": "不應寫入", "機構電話": "02-1234",
           "發照日期": "20260109", "機構地址": "臺北市測試路1號"}
    out = _collector(AnimalVeterinaryClinicsCollector)._normalize(row, "run", NOW)
    assert out["source_record_key"] == "datagov:8705:A123"
    assert out["canonical_entity_key"]
    assert "負責獸醫" not in out
    assert out["status_norm"] == "active"
    assert out["valid_from"] == "2026-01-09"


def test_veterinary_missing_license_type_does_not_emit_null_text_array_item():
    row = {"字號": "A124", "縣市": "臺北市", "狀態": "開業",
           "機構名稱": "測試動物醫院", "機構地址": "臺北市測試路2號"}
    out = _collector(AnimalVeterinaryClinicsCollector)._normalize(row, "run", NOW)
    assert out["service_tags"] == []


def test_pet_business_normalization_excludes_owners_and_preserves_raw_status():
    row = {"ID": "1", "legaltype": "A060", "legalname": "測試寵物業", "legaladdress": "臺北市1號",
           "busitem": "ABC", "animaltype": "狗、貓", "validnum": "V1", "validdate": "2029-08-18T00:00:00",
           "own_name": "不應寫入", "bos_name": "不應寫入", "state_flag": "P"}
    out = _collector(AnimalLicensedPetBusinessesCollector)._normalize(row, "run", NOW)
    assert "own_name" not in out and "bos_name" not in out
    assert out["status_norm"] == "active" and out["status_raw"] == "P"
    assert "不應寫入" not in json.dumps(out, ensure_ascii=False)
    assert out["valid_to"] == "2029-08-18"
    assert "state_code_unmapped" in out["quality_flags"]


def test_pet_business_missing_address_is_preserved_as_unlocated():
    row = {"ID": "2", "legaltype": "A060", "legalname": "地址待補寵物業",
           "legaladdress": "", "validdate": "2029-08-18T00:00:00", "state_flag": "P"}
    out = _collector(AnimalLicensedPetBusinessesCollector)._normalize(row, "run", NOW)
    assert out["address"] is None
    assert out["county_code"] is None
    assert "missing_address" in out["quality_flags"]


def test_protection_offices_normalization_keeps_public_contact_and_url_only():
    row = {"ID": "AP00000022", "AnimalProtectName": "連江縣政府產業發展處",
           "Address": "連江縣南竿鄉清水村101號", "Phone": "0836-22347",
           "Url": "https://www.matsu.gov.tw/", "Seqno": "22"}
    out = _collector(AnimalProtectionOfficesCollector)._normalize(row, "run", NOW)
    assert out["source_record_key"] == "datagov:134283:AP00000022"
    assert out["details"]["url"].startswith("https://")
    assert out["phone"] == "0836-22347"
    assert out["status_norm"] == "listed"


def test_collect_complete_gate_returns_run_and_snapshots():
    collector = _collector(AnimalProtectionOfficesCollector)
    collector._session = _Session([{"ID": "AP1", "AnimalProtectName": "機關", "Address": "臺北市中正區1號",
                                    "Phone": "02-1", "Url": "https://example.gov.tw", "Seqno": "1"}])
    collector.min_rows = 1
    result = collector.collect()
    assert result["is_complete"] is True
    assert result["row_count"] == 1
    assert result["data"][1]["source_record_key"] == "datagov:134283:AP1"


def test_collect_duplicate_key_is_failed_ledger_without_snapshots():
    collector = _collector(AnimalProtectionOfficesCollector)
    row = {"ID": "AP1", "AnimalProtectName": "機關", "Address": "臺北市中正區1號"}
    collector._session = _Session([row, row])
    collector.min_rows = 1
    result = collector.collect()
    assert result["is_complete"] is False
    assert result["_collector_error"] == "incomplete: duplicate stable key"
    assert not [r for r in result["data"] if r.get("_type") == "snapshot"]


def test_writer_uses_migration_358_columns_and_finalizer():
    source = inspect.getsource(SupabaseWriter._write_multi_table)
    assert "source_record_key" in source
    assert "longitude" in source and "latitude" in source and "geom" in source
    assert "live.finalize_animal_welfare_point_run" in source
    assert "live.animal_welfare_point_snapshots" in source
    assert "c == 'quality_flags'" in source
    assert "c in ('service_tags', 'quality_flags')" not in source


def test_failed_result_keeps_ledger_run_id_and_writer_transform():
    collector = _collector(AnimalProtectionOfficesCollector)
    collector._session = _Session([])
    collector.min_rows = 1
    result = collector.collect()
    assert result["run_id"]
    writer = object.__new__(SupabaseWriter)
    records = writer._transform_animal_welfare_points(result, NOW)
    assert len(records) == 1
    assert records[0]["_type"] == "run"
    assert records[0]["run_id"] == result["run_id"]
