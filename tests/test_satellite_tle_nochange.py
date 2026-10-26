"""satellite_tle 沒變就不 UPDATE、history 只送變動列（2026-10-15 Disk IO 第三輪 T1）。

不打真實 DB：攔截 execute_values，依 SQL 判斷是 satellite_tle 還是 history，
模擬 RETURNING 只回傳「新增或變動」的 norad_id。
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from storage.supabase_writer import SupabaseWriter


def _sat(norad, epoch='26287.5', name='SAT'):
    return {
        'norad_id': norad, 'name': name, 'intl_designator': '2026-001A',
        'constellation': '', 'orbit_type': 'LEO',
        'tle_line1': f'1 {norad} line1 {epoch}', 'tle_line2': f'2 {norad} line2',
        'tle_epoch': epoch, 'inclination': 53.0, 'eccentricity': 0.0001,
        'period_min': 95.0, 'decay_date': None, 'is_decayed': False,
        'object_type': 'PAYLOAD',
    }


@pytest.fixture
def writer_and_calls(monkeypatch):
    """回傳 (writer, conn, calls, set_returning)。

    calls: [(sql, values, kwargs)]；set_returning(ids) 設定 satellite_tle UPSERT 的 RETURNING。
    """
    writer = SupabaseWriter.__new__(SupabaseWriter)
    writer._pool = MagicMock()
    writer._pool.statement_timeout_ms = 30_000

    calls = []
    state = {'returning': []}

    def fake_execute_values(cur, sql, values, page_size=100, template=None, fetch=False):
        calls.append((sql, list(values), {'page_size': page_size, 'fetch': fetch}))
        if 'INTO live.satellite_tle AS t' in sql:
            return [(i,) for i in state['returning']]
        return None

    monkeypatch.setattr('storage.supabase_writer.execute_values', fake_execute_values)

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None

    def set_returning(ids):
        state['returning'] = list(ids)

    return writer, conn, calls, set_returning


TS = datetime(2026, 10, 15, 6, 27, tzinfo=timezone.utc)


def _tle_calls(calls):
    return [c for c in calls if 'INTO live.satellite_tle AS t' in c[0]]


def _hist_calls(calls):
    return [c for c in calls if 'satellite_tle_history' in c[0]]


def test_upsert_only_updates_changed_rows(writer_and_calls):
    writer, conn, calls, set_returning = writer_and_calls
    set_returning([])
    writer._write_satellite_tle(conn, {'data_all': [_sat(1), _sat(2)]}, TS)

    (sql, values, kwargs), = _tle_calls(calls)
    assert 'ON CONFLICT (norad_id) DO UPDATE SET' in sql
    assert 'IS DISTINCT FROM' in sql
    assert 'RETURNING norad_id' in sql
    assert kwargs['fetch'] is True, 'RETURNING 跨頁要 fetch=True 才收得齊'
    assert len(values) == 2, '仍送出全部列（新增的列靠 INSERT 進表）'


def test_compare_columns_cover_all_but_pk_and_updated_at(writer_and_calls):
    writer, conn, calls, set_returning = writer_and_calls
    writer._write_satellite_tle(conn, {'data_all': [_sat(1)]}, TS)
    sql = _tle_calls(calls)[0][0]
    cond = sql.split(' WHERE ', 1)[1].split(' RETURNING', 1)[0]
    left = cond.split(' IS DISTINCT FROM ')[0]
    for col in ['name', 'intl_designator', 'constellation', 'orbit_type',
                'tle_line1', 'tle_line2', 'tle_epoch', 'inclination',
                'eccentricity', 'period_min', 'decay_date', 'is_decayed',
                'object_type']:
        assert f't.{col}' in left, f'{col} 漏比對 → 該欄變動時不會更新'
    assert 't.updated_at' not in left, 'updated_at 每輪都不同，納入比對等於沒做'
    assert 't.norad_id' not in left


def test_history_receives_only_changed_rows(writer_and_calls):
    writer, conn, calls, set_returning = writer_and_calls
    # RETURNING 回傳 int；collector 給的 norad_id 可能是 str → 要能對得上
    set_returning([2])
    writer._write_satellite_tle(
        conn, {'data_all': [_sat('1'), _sat('2', epoch='26288.1'), _sat('3')]}, TS)

    (sql, values, _), = _hist_calls(calls)
    assert 'ON CONFLICT (norad_id, tle_epoch) DO NOTHING' in sql, '保留 unique 保險'
    assert [v[0] for v in values] == ['2']
    assert values[0][6] == '26288.1'


def test_history_skipped_when_nothing_changed(writer_and_calls):
    writer, conn, calls, set_returning = writer_and_calls
    set_returning([])
    writer._write_satellite_tle(conn, {'data_all': [_sat(1), _sat(2)]}, TS)
    assert _hist_calls(calls) == []


def test_rows_without_tle_lines_are_not_sent(writer_and_calls):
    writer, conn, calls, set_returning = writer_and_calls
    bad = _sat(9)
    bad['tle_line2'] = ''
    set_returning([1])
    writer._write_satellite_tle(conn, {'data_all': [_sat(1), bad]}, TS)
    assert [v[0] for v in _tle_calls(calls)[0][1]] == [1]
    assert [v[0] for v in _hist_calls(calls)[0][1]] == [1]


def test_history_failure_propagates_so_current_is_rolled_back(writer_and_calls, monkeypatch):
    """review F100：history 失敗要往上拋（同 transaction → current 一併 rollback，下輪才會重送）。"""
    writer, conn, calls, set_returning = writer_and_calls
    set_returning([1])

    import storage.supabase_writer as sw
    real = sw.execute_values

    def boom(cur, sql, values, **kw):
        if 'satellite_tle_history' in sql:
            raise RuntimeError('history down')
        return real(cur, sql, values, **kw)

    monkeypatch.setattr(sw, 'execute_values', boom)
    with pytest.raises(RuntimeError, match='history down'):
        writer._write_satellite_tle(conn, {'data_all': [_sat(1)]}, TS)
    conn.rollback.assert_called()
    conn.commit.assert_not_called()
