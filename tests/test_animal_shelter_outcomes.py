"""農業部 41236/73396 月報 collector contract tests."""

from contextlib import contextmanager
from unittest.mock import MagicMock

import config


def _response(payload):
    response = MagicMock()
    response.content = b"canonical fixture"
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _row(source_id, month=6, county_code="City000001"):
    return {
        "ID": source_id,
        "rpt_year": 114,
        "rpt_country_code": county_code,
        "rpt_county": "臺北市",
        "rpt_month": month,
        "accept_count": 10,
        "adopt_count": 3,
        "adopt_rate": "30%",
        "max_stay_dog_count": 4,
        "max_stay_cat_count": 2,
    }


def _collector(cls):
    collector = cls.__new__(cls)
    collector._session = MagicMock()
    collector.expected_min_counties = 1
    return collector


def test_outcomes_accepts_annual_month_zero_and_normalizes_metrics(monkeypatch):
    from collectors.animal_shelter_outcomes import AnimalShelterOutcomesCollector

    monkeypatch.setattr(config, "ANIMAL_SHELTER_OUTCOMES_MIN_ROWS", 1)
    collector = _collector(AnimalShelterOutcomesCollector)
    collector.min_rows = 1
    payload = [_row(1, month=0)]
    collector._session.get.return_value = _response(payload)

    result = collector.collect()

    assert result["is_complete"] is True
    row = result["data"][1]
    assert row["source_record_key"] == "datagov:41236:1"
    assert row["report_month"] == 0
    assert row["report_year"] == 2025
    assert row["metrics"]["adopt_rate"] == 30.0
    assert row["quality_flags"] == ["annual_record"]
    assert row["duplicate_grain_count"] == 1
    assert row["record_hash"]
    assert result["raw_payload"] == payload


def test_outcomes_duplicate_canonical_grain_fails(monkeypatch):
    from collectors.animal_shelter_outcomes import AnimalShelterOutcomesCollector

    monkeypatch.setattr(config, "ANIMAL_SHELTER_OUTCOMES_MIN_ROWS", 1)
    collector = _collector(AnimalShelterOutcomesCollector)
    collector.min_rows = 1
    collector._session.get.return_value = _response([_row(1), _row(2)])

    result = collector.collect()

    assert result["is_complete"] is False
    assert result["data"][0]["run_status"] == "failed"
    assert "canonical" in result["quality_note"]


def test_pressure_preserves_duplicate_grain_revision(monkeypatch):
    from collectors.animal_shelter_outcomes import AnimalShelterPressureCollector

    monkeypatch.setattr(config, "ANIMAL_SHELTER_PRESSURE_MIN_ROWS", 1)
    collector = _collector(AnimalShelterPressureCollector)
    collector.min_rows = 1
    payload = [_row(1), _row(2)]
    collector._session.get.return_value = _response(payload)

    result = collector.collect()

    assert result["is_complete"] is True
    rows = result["data"][1:]
    assert [row["revision_index"] for row in rows] == [1, 2]
    assert all(row["duplicate_grain_count"] == 2 for row in rows)
    assert all("grain_revision" in row["quality_flags"] for row in rows)
    assert len({row["source_record_key"] for row in rows}) == 2


def test_pressure_rejects_annual_month_zero(monkeypatch):
    from collectors.animal_shelter_outcomes import AnimalShelterPressureCollector

    monkeypatch.setattr(config, "ANIMAL_SHELTER_PRESSURE_MIN_ROWS", 1)
    collector = _collector(AnimalShelterPressureCollector)
    collector.min_rows = 1
    collector._session.get.return_value = _response([_row(1, month=0)])

    result = collector.collect()

    assert result["is_complete"] is False
    assert "cannot be annual" in result["quality_note"]


def test_complete_run_requires_expected_county_coverage(monkeypatch):
    from collectors.animal_shelter_outcomes import AnimalShelterOutcomesCollector

    monkeypatch.setattr(config, "ANIMAL_SHELTER_OUTCOMES_MIN_ROWS", 1)
    collector = _collector(AnimalShelterOutcomesCollector)
    collector.min_rows = 1
    collector.expected_min_counties = 2
    collector._session.get.return_value = _response([_row(1)])

    result = collector.collect()

    assert result["is_complete"] is False
    assert "counties below minimum" in result["quality_note"]


def test_complete_run_requires_source_specific_metrics(monkeypatch):
    from collectors.animal_shelter_outcomes import AnimalShelterPressureCollector

    monkeypatch.setattr(config, "ANIMAL_SHELTER_PRESSURE_MIN_ROWS", 1)
    collector = _collector(AnimalShelterPressureCollector)
    collector.min_rows = 1
    row = _row(1)
    del row["max_stay_cat_count"]
    collector._session.get.return_value = _response([row])

    result = collector.collect()

    assert result["is_complete"] is False
    assert "source-specific metric" in result["quality_note"]


def test_writer_finalizes_complete_run_without_writing_failed_rows(monkeypatch):
    from storage.supabase_writer import SupabaseWriter

    writer = SupabaseWriter.__new__(SupabaseWriter)
    executed_values = []
    cursor = MagicMock()
    conn = MagicMock()

    @contextmanager
    def fake_txn(_conn):
        yield cursor

    monkeypatch.setattr(writer, "_txn", fake_txn)
    monkeypatch.setattr(
        "storage.supabase_writer.execute_values",
        lambda _cur, sql, values, page_size=100: executed_values.append((sql, values)),
    )

    run = {
        "_type": "run",
        "run_id": "00000000-0000-0000-0000-000000000002",
        "run_status": "complete",
        "is_complete": True,
        "snapshot_date": "2026-08-19",
        "row_count": 1,
        "collected_at": "2026-08-19T00:00:00+08:00",
        "source_dataset_id": "datagov:73396",
        "payload_sha256": "a" * 64,
        "quality_note": "full",
    }
    row = {
        "_type": "outcome",
        "run_id": run["run_id"],
        "snapshot_date": run["snapshot_date"],
        "source_dataset_id": run["source_dataset_id"],
        "source_record_key": "datagov:73396:1",
        "source_id": 1,
        "report_year": 2025,
        "source_report_year": 114,
        "report_month": 6,
        "county_code": "City000001",
        "county_name": "臺北市",
        "report_grain_key": "114:06:City000001",
        "revision_no": 1,
        "duplicate_grain_count": 1,
        "metrics": {"max_stay_dog_count": 1},
        "record_hash": "b" * 64,
        "collected_at": run["collected_at"],
        "quality_flags": [],
    }

    writer._write_multi_table(conn, "animal_shelter_pressure", [run, row])

    assert any("animal_shelter_outcome_runs" in sql for sql, _ in executed_values)
    assert any("animal_shelter_outcomes" in sql for sql, _ in executed_values)
    assert any("finalize_animal_shelter_outcome_run" in str(call) for call in cursor.execute.call_args_list)


def test_writer_dispatches_both_monthly_collectors_to_multi_table_path():
    from datetime import datetime

    from storage.supabase_writer import SupabaseWriter

    writer = SupabaseWriter.__new__(SupabaseWriter)
    run = {
        "_type": "run",
        "run_id": "00000000-0000-0000-0000-000000000004",
        "is_complete": False,
    }
    result = {"data": [run]}

    assert writer._transform("animal_shelter_outcomes", result, datetime(2026, 8, 19)) == [run]
    assert writer._transform("animal_shelter_pressure", result, datetime(2026, 8, 19)) == [run]


def test_writer_failed_run_only_records_ledger(monkeypatch):
    from storage.supabase_writer import SupabaseWriter

    writer = SupabaseWriter.__new__(SupabaseWriter)
    executed_values = []
    cursor = MagicMock()
    conn = MagicMock()

    @contextmanager
    def fake_txn(_conn):
        yield cursor

    monkeypatch.setattr(writer, "_txn", fake_txn)
    monkeypatch.setattr(
        "storage.supabase_writer.execute_values",
        lambda _cur, sql, values, page_size=100: executed_values.append((sql, values)),
    )
    run = {
        "_type": "run",
        "run_id": "00000000-0000-0000-0000-000000000003",
        "run_status": "failed",
        "is_complete": False,
        "snapshot_date": "2026-08-19",
        "row_count": 0,
        "collected_at": "2026-08-19T00:00:00+08:00",
        "source_dataset_id": "datagov:41236",
        "quality_note": "incomplete: root is not a JSON array",
    }

    writer._write_multi_table(conn, "animal_shelter_outcomes", [run])

    assert len(executed_values) == 1
    assert "animal_shelter_outcome_runs" in executed_values[0][0]
    assert not any("animal_shelter_outcomes (" in sql for sql, _ in executed_values)
    assert not any("finalize_animal_shelter_outcome_run" in str(call) for call in cursor.execute.call_args_list)
