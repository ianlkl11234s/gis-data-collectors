"""iot_wra／uswg 站點 upsert：沒變就不 UPDATE（2026-10-15 Disk IO 第三輪 W1）。"""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from storage.supabase_writer import SupabaseWriter, _station_changed_cond


@pytest.fixture
def writer_and_sqls(monkeypatch):
    writer = SupabaseWriter.__new__(SupabaseWriter)
    pool = MagicMock()
    pool.statement_timeout_ms = 30_000

    @contextmanager
    def borrow(timeout=None):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = MagicMock()
        conn.cursor.return_value.__exit__.return_value = None
        yield conn

    pool.borrow.side_effect = borrow
    writer._pool = pool

    sqls = []
    monkeypatch.setattr(
        'storage.supabase_writer.execute_values',
        lambda cur, sql, values, page_size=100, **kw: sqls.append((sql, list(values))),
    )
    return writer, sqls


def _where_left(sql):
    cond = sql.split(' WHERE ', 1)[1]
    return cond.split(' IS DISTINCT FROM ')[0]


def test_changed_cond_excludes_updated_at():
    cond = _station_changed_cond(['name', 'lat', 'updated_at'])
    assert cond == '(s.name, s.lat) IS DISTINCT FROM (EXCLUDED.name, EXCLUDED.lat)'


def test_iot_wra_stations_upsert_only_when_changed(writer_and_sqls):
    writer, sqls = writer_and_sqls
    writer._upsert_iot_wra_stations([
        {'iow_station_id': 'u1', 'station_type': 'river', 'name': 'A', 'updated_at': 't'},
        {'iow_station_id': 'u1', 'station_type': 'river', 'name': 'A2', 'updated_at': 't'},
        {'iow_station_id': 'u1', 'station_type': 'watergate', 'name': 'B', 'updated_at': 't'},
    ])
    (sql, values), = sqls
    assert 'INSERT INTO public.iot_wra_stations AS s' in sql
    assert 'ON CONFLICT (iow_station_id, station_type) DO UPDATE SET' in sql
    left = _where_left(sql)
    for col in ['station_id', 'name', 'county_code', 'county_name', 'town_code',
                'town_name', 'basin_code', 'basin_name', 'admin_name',
                'hydro_station_type', 'lat', 'lng']:
        assert f's.{col}' in left, f'{col} 漏比對'
    assert 's.updated_at' not in left
    assert 's.iow_station_id' not in left and 's.station_type' not in left
    # 同批去重仍保留（同一 PK 只留最後一筆）
    assert len(values) == 2


def test_uswg_stations_upsert_only_when_changed(writer_and_sqls):
    writer, sqls = writer_and_sqls
    writer._upsert_uswg_stations([
        {'iow_station_id': 'u1', 'name': 'A', 'updated_at': 't'},
        {'iow_station_id': 'u2', 'name': 'B', 'updated_at': 't'},
    ])
    (sql, values), = sqls
    assert 'INSERT INTO public.uswg_stations AS s' in sql
    assert 'ON CONFLICT (iow_station_id) DO UPDATE SET' in sql
    left = _where_left(sql)
    for col in ['station_id', 'name', 'county_code', 'county_name', 'town_code',
                'town_name', 'admin_name', 'hydro_station_type', 'lat', 'lng']:
        assert f's.{col}' in left, f'{col} 漏比對'
    assert 's.updated_at' not in left and 's.iow_station_id' not in left
    assert 'basin' not in left
    assert len(values) == 2


def test_empty_rows_skip_db(writer_and_sqls):
    writer, sqls = writer_and_sqls
    writer._upsert_iot_wra_stations([])
    writer._upsert_uswg_stations([])
    assert sqls == []
