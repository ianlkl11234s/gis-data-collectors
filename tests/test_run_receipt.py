"""執行收據（gis-platform mig 558 report_collector_run）：實際新增列數、上游時間、退回舊心跳。

鐵則：沒算的值不送（DB 記未知），不可用 0 代替；新函式不存在時同一輪退回舊心跳，部署不會壞。
"""
from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from storage import run_receipt

TPE = timezone(timedelta(hours=8))
ROOT = Path(__file__).resolve().parents[1]


# ── 純函式 ──────────────────────────────────────────────────────────────

def test_payload_omits_unknown_fields():
    p = run_receipt.build_payload('zz_none', True, 12, None, run={})
    assert p['success'] is True and p['records'] == 12 and p['error'] is None
    for key in ('new_rows', 'rows_basis', 'parse_ok', 'source_date', 'upstream_max_at'):
        assert key not in p  # 未知 ≠ 0


def test_payload_keeps_zero_and_normalises_values():
    now = datetime(2026, 11, 18, 12, 0, tzinfo=TPE)
    p = run_receipt.build_payload('zz_none', True, 5, None, now=now, run={
        'new_rows': 0, 'rows_basis': 'dedup', 'parse_ok': False,
        'source_date': '115/11/17', 'upstream_max_at': '2026-11-18T11:50:00+08:00'})
    assert p['new_rows'] == 0 and p['rows_basis'] == 'dedup' and p['parse_ok'] is False
    assert p['source_date'] == '2026-11-17'
    assert p['upstream_max_at'] == '2026-11-18T11:50:00+08:00'


@pytest.mark.parametrize('value', ['2999-01-01', 'today', '2026-02-30', '', None, 'abc'])
def test_bad_source_date_dropped(value):
    assert run_receipt.norm_source_date(value, today=date(2026, 11, 18)) is None


@pytest.mark.parametrize('value, expected', [
    ('2026/11/17', '2026-11-17'), ('20261117', '2026-11-17'), ('2026-11-17T08:00:00', '2026-11-17'),
    (date(2026, 11, 17), '2026-11-17'), ('115-11-17', '2026-11-17')])
def test_source_date_formats(value, expected):
    assert run_receipt.norm_source_date(value, today=date(2026, 11, 18)) == expected


def test_max_upstream_drops_future_and_garbage():
    now = datetime(2026, 11, 18, 12, 0, tzinfo=TPE)
    got = run_receipt.max_upstream(
        ['2026-11-18T11:00:00+08:00', '2026-11-18 11:30:00', 'bad', None, '2026-11-25T00:00:00+08:00'], now)
    assert got == datetime(2026, 11, 18, 11, 30, tzinfo=TPE)  # naive 視為台北；7 天後的丟掉


def test_run_info_from_records_and_result():
    info = run_receipt.run_info(
        {'_source_date': '2026-11-17', '_parse': True},
        records=[{'snapshot_at': '2026-11-18T10:00:00+08:00'}, {'snapshot_at': '2026-11-18T11:00:00+08:00'}],
        upstream_col='snapshot_at', rows=(3, 'dedup'))
    assert info['new_rows'] == 3 and info['rows_basis'] == 'dedup'
    assert info['upstream_max_at'] == datetime(2026, 11, 18, 11, 0, tzinfo=TPE)
    assert info['source_date'] == '2026-11-17' and info['parse_ok'] is True
    assert run_receipt.run_info({}, rows=None) == {}


def test_freshness_policy_from_realtime_tables():
    run_receipt._policy_cache.clear()
    summer = datetime(2026, 7, 1, 12, 0, tzinfo=TPE)
    winter = datetime(2026, 12, 1, 12, 0, tzinfo=TPE)
    assert run_receipt.freshness_policy('weather', summer) == {'freshness_kind': 'regular', 'expected_interval_min': 60}
    assert run_receipt.freshness_policy('npa_traffic_accident_a1', summer)['freshness_kind'] == 'event'
    assert run_receipt.freshness_policy('env_jp_wbgt', summer)['freshness_kind'] == 'regular'
    run_receipt._policy_cache.clear()
    assert run_receipt.freshness_policy('env_jp_wbgt', winter)['freshness_kind'] == 'off_season'
    assert run_receipt.freshness_policy('zz_not_a_collector', winter) == {}
    run_receipt._policy_cache.clear()


# ── writer：實際新增列數 ─────────────────────────────────────────────────

@pytest.fixture
def writer(monkeypatch, tmp_path):
    monkeypatch.setattr('config.SUPABASE_DB_URL', 'postgresql://test')
    monkeypatch.setattr('storage.supabase_writer.BUFFER_DIR', tmp_path / 'buffer')
    from storage.db import SupabaseConnectionPool
    pool = MagicMock(spec=SupabaseConnectionPool)
    pool.statement_timeout_ms = 30_000
    monkeypatch.setattr('storage.supabase_writer.SupabaseConnectionPool', lambda: pool)
    from storage.supabase_writer import SupabaseWriter
    w = SupabaseWriter('postgresql://test')
    return w, pool


def _conn():
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    return conn, cursor


def _fake_execute_values(monkeypatch, returned):
    seen = []

    def fake(cur, sql, values, page_size=100, fetch=False):
        seen.append((sql, fetch))
        return returned if fetch else None
    monkeypatch.setattr('storage.supabase_writer.execute_values', fake)
    return seen


def test_do_nothing_counts_returned_rows(writer, monkeypatch):
    w, _ = writer
    seen = _fake_execute_values(monkeypatch, [(1,), (1,)])
    conn, _ = _conn()
    rows = [{'reservoir_id': str(i), 'snapshot_at': '2026-11-18T10:00:00+08:00'} for i in range(5)]
    assert w._write_to_db(conn, 'water_reservoir', rows, datetime(2026, 11, 18)) == (2, 'dedup')
    assert seen[0][0].endswith('ON CONFLICT DO NOTHING RETURNING 1') and seen[0][1] is True


def test_do_update_counts_only_inserted(writer, monkeypatch):
    from storage.supabase_tables import TABLE_MAP
    name = next(k for k, v in TABLE_MAP.items()
                if v.get('upsert_key') and v.get('upsert_strategy') != 'do_nothing'
                and not v.get('is_multi_table') and not v.get('is_reference') and not v.get('history_dedup_cols'))
    w, _ = writer
    seen = _fake_execute_values(monkeypatch, [(True,), (False,), (True,)])
    conn, _ = _conn()
    got = w._write_to_db(conn, name, [{c: None for c in TABLE_MAP[name]['columns']}], datetime(2026, 11, 18))
    assert got == (2, 'dedup')
    assert seen[0][0].endswith('RETURNING (xmax = 0)')


def test_plain_insert_is_append(writer, monkeypatch):
    w, _ = writer
    seen = _fake_execute_values(monkeypatch, None)
    conn, _ = _conn()
    rows = [{'station_id': 'A'}, {'station_id': 'B'}]
    assert w._write_to_db(conn, 'weather', rows, datetime(2026, 11, 18)) == (2, 'append')
    assert seen[0][1] is False and 'RETURNING' not in seen[0][0]  # 單純 INSERT 不加 RETURNING


def test_multi_table_is_unknown(writer, monkeypatch):
    w, _ = writer
    monkeypatch.setattr(w, '_write_multi_table', lambda *a: None)
    conn, _ = _conn()
    assert w._write_to_db(conn, 'freeway_vd', [{'_type': 'vd'}], datetime(2026, 11, 18)) is None


# ── writer：收據內容與退回舊心跳 ─────────────────────────────────────────

def _tracking(pool, fail_run_with=None):
    cursors = []

    @contextmanager
    def borrow(timeout=None):
        conn, cursor = _conn()

        def execute(sql, params=None):
            if fail_run_with is not None and 'report_collector_run' in str(sql):
                raise fail_run_with
        cursor.execute.side_effect = execute
        cursors.append(cursor)
        yield conn
    pool.borrow.side_effect = borrow
    return cursors


def _calls(cursors, fn):
    return [c.args[1] for cur in cursors for c in cur.execute.call_args_list if fn in str(c.args[0])]


def test_write_sends_receipt_with_upstream_and_new_rows(writer, monkeypatch):
    w, pool = writer
    cursors = _tracking(pool)
    _fake_execute_values(monkeypatch, [(1,)])
    result = {'data': [{'reservoir_id': '1', 'snapshot_at': '2026-11-18T10:00:00+08:00'},
                       {'reservoir_id': '2', 'snapshot_at': '2026-11-18T11:00:00+08:00'}]}
    assert w.write('water_reservoir', result, datetime(2026, 11, 18, 11, 5, tzinfo=TPE)) is True
    (name, payload_json), = _calls(cursors, 'report_collector_run')
    payload = json.loads(payload_json)
    assert name == 'water_reservoir' and payload['records'] == 2
    assert payload['new_rows'] == 1 and payload['rows_basis'] == 'dedup'
    assert payload['upstream_max_at'] == '2026-11-18T11:00:00+08:00'
    assert payload['freshness_kind'] == 'regular' and payload['expected_interval_min'] == 60
    assert _calls(cursors, 'report_collector_heartbeat') == []  # 新函式成功就不呼叫舊的


def test_zero_records_sends_known_zero_and_source_date(writer):
    w, pool = writer
    cursors = _tracking(pool)
    result = {'data': [], '_source_date': '2026-11-10', '_parse': True}
    assert w.write('wra_drought_alert', result, datetime(2026, 11, 18, tzinfo=TPE)) is True
    (_, payload_json), = _calls(cursors, 'report_collector_run')
    payload = json.loads(payload_json)
    assert payload['new_rows'] == 0 and payload['source_date'] == '2026-11-10' and payload['parse_ok'] is True
    assert payload['freshness_kind'] == 'event'


def test_missing_function_falls_back_and_backs_off(writer, monkeypatch):
    """mig 558 未套用：undefined_function → 同一輪改呼叫舊心跳，之後 1 小時內直接走舊心跳。"""
    w, pool = writer
    err = RuntimeError('function report_collector_run(text, jsonb) does not exist')
    err.pgcode = '42883'
    cursors = _tracking(pool, fail_run_with=err)
    w._report_heartbeat('weather', True, 3)
    assert _calls(cursors, 'report_collector_heartbeat') == [('weather', True, 3, None)]
    assert w._run_rpc_retry_at > 0
    w._report_heartbeat('weather', False, 0, 'boom')
    assert len(_calls(cursors, 'report_collector_run')) == 1  # 第二輪不再試新函式
    assert _calls(cursors, 'report_collector_heartbeat')[-1] == ('weather', False, 0, 'boom')


def test_other_run_error_falls_back_without_backoff(writer):
    w, pool = writer
    cursors = _tracking(pool, fail_run_with=RuntimeError('statement timeout'))
    w._report_heartbeat('weather', True, 1)
    assert _calls(cursors, 'report_collector_heartbeat') == [('weather', True, 1, None)]
    assert w._run_rpc_retry_at == 0.0


# ── VM 共用模組 ─────────────────────────────────────────────────────────

class _VmCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args):
        self.conn.calls.append((sql, args))
        if 'report_collector_run' in sql and self.conn.missing:
            err = RuntimeError('missing')
            err.pgcode = '42883'
            raise err


class _VmConn:
    def __init__(self, missing):
        self.calls, self.commits, self.rollbacks, self.missing = [], 0, 0, missing

    def cursor(self):
        return _VmCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def vm_receipt(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / 'external' / 'vm_common'))
    sys.modules.pop('run_receipt', None)
    import run_receipt as mod
    yield mod
    sys.modules.pop('run_receipt', None)


def test_vm_report_run_uses_new_function(vm_receipt):
    conn = _VmConn(missing=False)
    assert vm_receipt.report_run(conn, 'ship_ais', True, 10, run={'new_rows': 4, 'rows_basis': 'dedup'}) == 'run'
    (sql, (name, payload_json)), = conn.calls
    payload = json.loads(payload_json)
    assert 'report_collector_run' in sql and name == 'ship_ais'
    assert payload == {'success': True, 'records': 10, 'error': None, 'new_rows': 4, 'rows_basis': 'dedup'}


def test_vm_report_run_falls_back(vm_receipt):
    conn = _VmConn(missing=True)
    assert vm_receipt.report_run(conn, 'ship_ais', False, 0, 'x' * 900) == 'heartbeat'
    assert conn.rollbacks == 1 and conn.commits == 1
    sql, args = conn.calls[-1]
    assert 'report_collector_heartbeat' in sql and args[:3] == ('ship_ais', False, 0) and len(args[3]) == 500
