"""待認領養完整快照的完整性與安全寫入契約。"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock

import requests

import config


def _collector():
    from collectors.animal_adoption import AnimalAdoptionCollector

    collector = AnimalAdoptionCollector.__new__(AnimalAdoptionCollector)
    collector._session = MagicMock()
    return collector


def _response(payload):
    response = MagicMock()
    response.content = b'["canonical fixture"]'
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _row(animal_id: str = 'A-1'):
    return {
        'animal_id': animal_id,
        'animal_subid': 'same-subid',
        'animal_shelter_pkid': '2',
        'animal_area_pkid': '1',
        'animal_kind': 'DOG',
        'animal_status': 'OPEN',
        'animal_opendate': '1900-01-01',
        'animal_closeddate': '2999-12-31',
        'animal_update': '2026/08/19 08:30:00',
        'album_file': 'https://example.test/image.jpg',
    }


def test_collect_archives_raw_payload_and_normalizes_sentinels(monkeypatch):
    monkeypatch.setattr(config, 'ANIMAL_ADOPTION_MIN_ROWS', 1)
    collector = _collector()
    payload = [_row()]
    collector._session.get.return_value = _response(payload)

    result = collector.collect()

    assert result['is_complete'] is True
    assert result['raw_payload'] == payload  # LocalStorage/archive.py 會保存它
    assert result['payload_sha256']
    record = result['data'][0]
    assert record['source_record_key'] == 'datagov:85903:A-1'
    assert record['animal_closeddate'] is None
    assert record['quality_flags'] == ['animal_opendate_1900_sentinel']
    assert record['animal_opendate'] is None
    assert record['source_observed_at'] == '2026-08-19T08:30:00+08:00'
    assert record['image_url'] == 'https://example.test/image.jpg'
    assert record['animal_id_raw'] == 'A-1'
    assert 'raw_json' not in record


def test_zero_or_partial_payload_is_failed_and_has_no_snapshots(monkeypatch):
    monkeypatch.setattr(config, 'ANIMAL_ADOPTION_MIN_ROWS', 2)
    collector = _collector()
    collector._session.get.return_value = _response([_row()])

    result = collector.collect()

    assert result['is_complete'] is False
    assert result['run_status'] == 'failed'
    assert result['data'] == []
    assert 'below minimum' in result['quality_note']


def test_duplicate_animal_id_never_becomes_complete(monkeypatch):
    monkeypatch.setattr(config, 'ANIMAL_ADOPTION_MIN_ROWS', 1)
    collector = _collector()
    collector._session.get.return_value = _response([_row('A-1'), _row('A-1')])

    result = collector.collect()

    assert result['is_complete'] is False
    assert result['data'] == []
    assert 'duplicate animal_id' in result['quality_note']


def test_http_failure_returns_failed_ledger_without_fake_zero(monkeypatch):
    monkeypatch.setattr(config, 'ANIMAL_ADOPTION_MIN_ROWS', 1)
    collector = _collector()
    collector._session.get.side_effect = requests.Timeout('offline')

    result = collector.collect()

    assert result['is_complete'] is False
    assert result['row_count'] == 0
    assert result['data'] == []
    assert 'http_or_json_error' in result['quality_note']


def test_writer_only_finalizes_complete_run(monkeypatch):
    from storage.supabase_writer import SupabaseWriter

    writer = SupabaseWriter.__new__(SupabaseWriter)
    executed_values = []
    cursor = MagicMock()
    conn = MagicMock()

    @contextmanager
    def fake_txn(_conn):
        yield cursor

    monkeypatch.setattr(writer, '_txn', fake_txn)
    monkeypatch.setattr(
        'storage.supabase_writer.execute_values',
        lambda _cur, sql, values, page_size=100: executed_values.append((sql, values)),
    )

    incomplete = [{
        '_type': 'run', 'run_id': '00000000-0000-0000-0000-000000000001',
        'run_status': 'failed', 'is_complete': False, 'snapshot_date': '2026-08-19',
        'row_count': 0, 'collected_at': '2026-08-19T00:00:00+08:00',
        'source_dataset_id': 'datagov:85903', 'source_observed_at': None,
        'payload_sha256': None, 'quality_note': 'timeout',
    }]
    writer._write_multi_table(conn, 'animal_adoption', incomplete)
    assert len(executed_values) == 1  # run ledger only
    assert not any('finalize_animal_adoption_snapshot' in str(call) for call in cursor.execute.call_args_list)

    complete = [{**incomplete[0], 'run_status': 'complete', 'is_complete': True, 'row_count': 1}, {
        '_type': 'animal', 'run_id': incomplete[0]['run_id'], 'source_record_key': 'datagov:85903:A-1',
        'animal_id': 'A-1', 'shelter_id': '2', 'county_code': '1', 'animal_kind': 'DOG',
        'record_hash': 'x', 'snapshot_at': '2026-08-19T00:00:00+08:00',
    }]
    writer._write_multi_table(conn, 'animal_adoption', complete)
    assert any('animal_adoption_snapshots' in sql for sql, _values in executed_values)
    assert any('finalize_animal_adoption_snapshot' in str(call) for call in cursor.execute.call_args_list)
    # 完整 run 只有 finalize 成功後才由 platform function 改成 succeeded。
    assert executed_values[-2][1][0][1] == 'running'
