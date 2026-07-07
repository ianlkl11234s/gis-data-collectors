"""SupabaseWriter 行為單元測試（連線池版本）。

不打真實 DB。用 mock 攔截 pool 借連線 + cursor.execute。
驗證重點：collector 寫入路徑在「DB 暫時不可用」時 graceful degrade 到
buffer，不會卡 collector / 不會吞掉資料。

對應 plan 5.2：tests/test_supabase_writer.py
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def writer_with_mock_pool(monkeypatch, tmp_path):
    """SupabaseWriter 配上完全 mock 的 SupabaseConnectionPool。

    回傳 (writer, mock_pool) — 測試可控制 borrow() 的行為。
    """
    monkeypatch.setattr('config.SUPABASE_DB_URL', 'postgresql://test')
    # supabase_writer.py module-level 已用 config.LOCAL_DATA_DIR 算 BUFFER_DIR
    # 必須改 module attribute 才生效
    monkeypatch.setattr('storage.supabase_writer.BUFFER_DIR', tmp_path / 'buffer')

    from storage.db import SupabaseConnectionPool
    mock_pool = MagicMock(spec=SupabaseConnectionPool)
    # spec 只涵蓋 class 屬性，statement_timeout_ms 是 __init__ 實例屬性要手動補
    mock_pool.statement_timeout_ms = 30_000
    mock_pool.snapshot.return_value = {
        'pool_initialized': True,
        'minconn': 2,
        'maxconn': 5,
        'borrow_timeout_sec': 5.0,
        'connect_failures': 0,
        'breaker_open': False,
    }

    from contextlib import contextmanager

    @contextmanager
    def fake_borrow(timeout=None):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = None
        yield conn

    mock_pool.borrow.side_effect = fake_borrow

    # 攔截 SupabaseConnectionPool() 建構 — 直接讓 writer __init__ 拿到 mock
    monkeypatch.setattr(
        'storage.supabase_writer.SupabaseConnectionPool',
        lambda: mock_pool,
    )

    # execute_values 內部會探 cursor.connection.encoding，mock 不支援
    # 改成 no-op，測試重點不是 SQL 細節而是借/還 conn 行為
    monkeypatch.setattr(
        'storage.supabase_writer.execute_values',
        lambda cur, sql, values, page_size=100: None,
    )

    from storage.supabase_writer import SupabaseWriter
    # 重置跨測試共享的 class 級別 dict（避免測試順序污染）
    SupabaseWriter._db_consecutive_errors.clear()

    w = SupabaseWriter('postgresql://test')
    return w, mock_pool


def test_health_snapshot_uses_pool_no_borrow(writer_with_mock_pool):
    """health_snapshot() 應透過 pool.snapshot()，不該 borrow 連線。"""
    writer, mock_pool = writer_with_mock_pool
    snap = writer.health_snapshot()

    assert 'connected' in snap
    assert 'connect_failures' in snap
    assert 'breaker_open' in snap
    mock_pool.snapshot.assert_called()
    mock_pool.borrow.assert_not_called()


def test_with_conn_public_api_returns_pool_borrow(writer_with_mock_pool):
    """with_conn() 必須 delegate 到 pool.borrow()。"""
    writer, mock_pool = writer_with_mock_pool

    with writer.with_conn() as conn:
        # conn 必須是 mock pool 借出來的物件
        assert conn is not None
        # cursor() 應該可呼叫
        with conn.cursor() as cur:
            pass

    mock_pool.borrow.assert_called()


def test_write_fail_falls_to_buffer(writer_with_mock_pool, tmp_path):
    """borrow 失敗（PoolBorrowTimeout）→ 資料進 buffer，不 raise 給 collector。"""
    writer, mock_pool = writer_with_mock_pool
    from storage.db import PoolBorrowTimeout

    def borrow_fails(timeout=None):
        raise PoolBorrowTimeout("simulated pool exhausted")

    mock_pool.borrow.side_effect = borrow_fails

    # 用 youbike — 有 transformer
    result = {
        'data': [
            {'StationUID': 'TEST_1', '_city': 'Taipei',
             'AvailableRentBikes': 5, 'AvailableReturnBikes': 3}
        ]
    }
    ts = datetime(2026, 6, 26, 12, 0, 0)

    # 不該 raise
    writer.write('youbike', result, ts)

    # buffer dir 應有新檔
    buffer_dir = tmp_path / 'buffer'
    assert buffer_dir.exists()
    files = list(buffer_dir.glob('*.json'))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload['collector'] == 'youbike'
    assert payload['result'] == result


def test_write_breaker_open_falls_to_buffer(writer_with_mock_pool, tmp_path):
    """斷路器開啟（PoolBreakerOpen）→ 同樣 fallback 到 buffer。"""
    writer, mock_pool = writer_with_mock_pool
    from storage.db import PoolBreakerOpen

    def borrow_blocked(timeout=None):
        raise PoolBreakerOpen("breaker open")

    mock_pool.borrow.side_effect = borrow_blocked

    result = {
        'data': [
            {'StationUID': 'TEST_2', '_city': 'Taipei',
             'AvailableRentBikes': 1, 'AvailableReturnBikes': 1}
        ]
    }
    ts = datetime(2026, 6, 26, 12, 0, 0)

    writer.write('youbike', result, ts)

    buffer_dir = tmp_path / 'buffer'
    files = list(buffer_dir.glob('*.json'))
    assert len(files) == 1


def test_write_generic_exception_falls_to_buffer(writer_with_mock_pool, tmp_path, monkeypatch):
    """任意 exception（例：DB query 失敗）→ 資料進 buffer。"""
    writer, mock_pool = writer_with_mock_pool

    # 覆寫 execute_values 為丟錯（模擬 SQL 執行失敗）
    monkeypatch.setattr(
        'storage.supabase_writer.execute_values',
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db query exploded"))
    )

    result = {
        'data': [
            {'StationUID': 'TEST_3', '_city': 'Taipei',
             'AvailableRentBikes': 0, 'AvailableReturnBikes': 0}
        ]
    }
    ts = datetime(2026, 6, 26, 12, 0, 0)

    writer.write('youbike', result, ts)

    buffer_dir = tmp_path / 'buffer'
    files = list(buffer_dir.glob('*.json'))
    assert len(files) >= 1


def test_concurrent_writes_dont_block_each_other(writer_with_mock_pool):
    """並發 write 應同時跑 — 沒有共用 RLock 序列化。"""
    writer, mock_pool = writer_with_mock_pool

    import time as _time
    from contextlib import contextmanager

    @contextmanager
    def slow_borrow(timeout=None):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = None
        # 模擬 DB 寫入耗時 50ms（但每個 thread 借自己的 conn）
        _time.sleep(0.05)
        yield conn

    mock_pool.borrow.side_effect = slow_borrow

    result = {'data': [{'StationUID': 'X', '_city': 'Taipei',
                        'AvailableRentBikes': 0, 'AvailableReturnBikes': 0}]}
    ts = datetime(2026, 6, 26, 12, 0, 0)

    def worker():
        writer.write('youbike', result, ts)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    start = _time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = _time.monotonic() - start

    # 10 個 thread 並發 50ms 工作，理想 < 0.2s；舊版 RLock 會 > 0.5s
    assert elapsed < 0.3, f"並發 write 跑了 {elapsed}s，疑似序列化"


def test_flush_buffer_borrows_single_conn(writer_with_mock_pool, tmp_path):
    """flush_buffer 應只借一條 conn 跑完整批，不是每筆借一次。"""
    writer, mock_pool = writer_with_mock_pool

    # 預先寫 3 筆 buffer
    buffer_dir = tmp_path / 'buffer'
    buffer_dir.mkdir(parents=True, exist_ok=True)
    for i in range(3):
        payload = {
            'collector': 'youbike',
            'timestamp': '2026-06-26T12:00:00+00:00',
            'result': {'data': [{'StationUID': f'STA_{i}', '_city': 'Taipei',
                                  'AvailableRentBikes': 0, 'AvailableReturnBikes': 0}]},
        }
        (buffer_dir / f'youbike_buffered_{i}.json').write_text(json.dumps(payload))

    mock_pool.borrow.reset_mock()
    writer.flush_buffer()

    # 整批應只 borrow 一次
    assert mock_pool.borrow.call_count == 1, (
        f"flush_buffer borrowed {mock_pool.borrow.call_count} times, expected 1"
    )

    # 所有 buffer 應已清空（mock cursor 不會 raise，所以全部視為成功）
    remaining = list(buffer_dir.glob('*.json'))
    assert len(remaining) == 0


def test_flush_buffer_skips_when_pool_unavailable(writer_with_mock_pool, tmp_path):
    """borrow 失敗時，flush_buffer 應 silent return，不 raise，不刪 buffer。"""
    writer, mock_pool = writer_with_mock_pool
    from storage.db import PoolBreakerOpen

    buffer_dir = tmp_path / 'buffer'
    buffer_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        'collector': 'youbike',
        'timestamp': '2026-06-26T12:00:00+00:00',
        'result': {'data': [{'StationUID': 'STA', '_city': 'Taipei',
                              'AvailableRentBikes': 0, 'AvailableReturnBikes': 0}]},
    }
    buf_file = buffer_dir / 'youbike_test.json'
    buf_file.write_text(json.dumps(payload))

    mock_pool.borrow.side_effect = lambda timeout=None: (_ for _ in ()).throw(
        PoolBreakerOpen("breaker open")
    )

    # 不該 raise
    writer.flush_buffer()

    # buffer 檔不該被刪
    assert buf_file.exists()


def test_consecutive_error_alert_threshold(writer_with_mock_pool, tmp_path):
    """連續失敗達 _DB_ERROR_ALERT_THRESHOLD 次才送 Telegram，避免洗版。"""
    writer, mock_pool = writer_with_mock_pool
    from storage.db import PoolBreakerOpen

    mock_pool.borrow.side_effect = lambda timeout=None: (_ for _ in ()).throw(
        PoolBreakerOpen("breaker open")
    )

    result = {'data': [{'StationUID': 'X', '_city': 'Taipei',
                        'AvailableRentBikes': 0, 'AvailableReturnBikes': 0}]}
    ts = datetime(2026, 6, 26, 12, 0, 0)

    with patch('storage.supabase_writer.send_telegram') as tg:
        # 1, 2 次：不該 alert
        writer.write('youbike', result, ts)
        writer.write('youbike', result, ts)
        assert tg.call_count == 0
        # 第 3 次：剛好達閾值，alert 一次
        writer.write('youbike', result, ts)
        assert tg.call_count == 1
        # 第 4 次：已經 alert 過，不再重複
        writer.write('youbike', result, ts)
        assert tg.call_count == 1


def test_do_nothing_upsert_is_targetless(writer_with_mock_pool, monkeypatch):
    """do_nothing 策略必須生成無目標 ON CONFLICT DO NOTHING。

    lightning_events 有雙 unique index（uk_eventid + uk_dedup），
    指定 (event_id) 只護一個 — feed 用新 event_id 重發同筆落雷時
    dedup_hash 撞第二個 index 會炸整批（2026-07-03 事故回歸測試）。
    """
    writer, mock_pool = writer_with_mock_pool
    mock_pool.statement_timeout_ms = 30_000  # _txn 的 SET LOCAL 會讀（spec mock 沒有此屬性）
    captured = []
    monkeypatch.setattr(
        'storage.supabase_writer.execute_values',
        lambda cur, sql, values, page_size=100: captured.append(sql),
    )
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None
    records = [{
        'event_id': 'E1', 'strike_time': '2026-07-03T00:00:00+08:00',
        'lon': 121.0, 'lat': 23.5, 'intensity_ka': -12.3,
        'strike_type': 'CG', 'dedup_hash': 'h1',
        'geom': 'SRID=4326;POINT(121.0 23.5)',
        'observed_at': '2026-07-03T00:00:00+08:00',
        'collected_at': '2026-07-03T00:01:00+08:00',
    }]
    writer._write_to_db(conn, 'lightning_events', records, datetime(2026, 7, 3))

    history_sqls = [s for s in captured if 'lightning_events' in s]
    assert history_sqls, '應產生寫入 lightning_events 的 SQL'
    assert 'ON CONFLICT DO NOTHING' in history_sqls[0]
    assert 'ON CONFLICT (' not in history_sqls[0]


# ============================================================
# 心跳併入主寫入連線（成功路徑只 borrow 一次）
# ============================================================

def _youbike_result():
    return {'data': [{'StationUID': 'HB_1', '_city': 'Taipei',
                      'AvailableRentBikes': 2, 'AvailableReturnBikes': 3}]}


def test_write_success_single_borrow_heartbeat_same_conn(writer_with_mock_pool):
    """成功寫入應只 borrow 一次，心跳在同一條 conn 上跑（獨立 transaction）。"""
    writer, mock_pool = writer_with_mock_pool
    mock_pool.statement_timeout_ms = 30_000  # _txn 的 SET LOCAL 會讀

    from contextlib import contextmanager
    borrowed_conns = []

    @contextmanager
    def tracking_borrow(timeout=None):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = None
        borrowed_conns.append((conn, cursor))
        yield conn

    mock_pool.borrow.side_effect = tracking_borrow

    writer.write('youbike', _youbike_result(), datetime(2026, 7, 7, 12, 0, 0))

    # 整個成功路徑（主寫入 + 心跳）只 borrow 一次
    assert mock_pool.borrow.call_count == 1, (
        f"write() borrowed {mock_pool.borrow.call_count} times, expected 1"
    )

    # 心跳 SQL 必須跑在同一條 conn 的 cursor 上
    _, cursor = borrowed_conns[0]
    heartbeat_calls = [c for c in cursor.execute.call_args_list
                       if 'report_collector_heartbeat' in str(c.args[0])]
    assert heartbeat_calls, '心跳 SQL 應在主寫入的 conn 上執行'
    assert heartbeat_calls[0].args[1][0] == 'youbike'
    assert heartbeat_calls[0].args[1][1] is True


def test_write_heartbeat_failure_does_not_affect_main_write(writer_with_mock_pool, tmp_path):
    """心跳失敗必須被吞掉：主寫入視為成功，不進 buffer、不發告警、不 raise。"""
    writer, mock_pool = writer_with_mock_pool
    mock_pool.statement_timeout_ms = 30_000

    from contextlib import contextmanager

    @contextmanager
    def borrow_heartbeat_broken(timeout=None):
        conn = MagicMock()
        cursor = MagicMock()

        def execute(sql, params=None):
            if 'report_collector_heartbeat' in str(sql):
                raise RuntimeError('heartbeat proc exploded')

        cursor.execute.side_effect = execute
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = None
        yield conn

    mock_pool.borrow.side_effect = borrow_heartbeat_broken

    with patch('storage.supabase_writer.send_telegram') as tg:
        # 不該 raise
        writer.write('youbike', _youbike_result(), datetime(2026, 7, 7, 12, 0, 0))

    # 主寫入視為成功：沒有 buffer 檔、錯誤計數歸零、沒發 Telegram
    buffer_dir = tmp_path / 'buffer'
    assert list(buffer_dir.glob('*.json')) == []
    assert writer._db_consecutive_errors.get('youbike', 0) == 0
    assert tg.call_count == 0
    # 心跳併入主連線後仍只 borrow 一次
    assert mock_pool.borrow.call_count == 1


def test_write_failure_heartbeat_still_borrows_own_conn(writer_with_mock_pool, tmp_path):
    """失敗路徑行為不變：主寫入 borrow 失敗後，心跳仍自行借短 timeout 連線回報。"""
    writer, mock_pool = writer_with_mock_pool
    mock_pool.statement_timeout_ms = 30_000
    from storage.db import PoolBorrowTimeout

    from contextlib import contextmanager
    calls = []

    @contextmanager
    def borrow_first_fails(timeout=None):
        calls.append(timeout)
        if len(calls) == 1:
            raise PoolBorrowTimeout('pool exhausted')
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        conn.cursor.return_value.__exit__.return_value = None
        yield conn

    mock_pool.borrow.side_effect = borrow_first_fails

    writer.write('youbike', _youbike_result(), datetime(2026, 7, 7, 12, 0, 0))

    # 主寫入 borrow（預設 timeout=None）失敗 → 資料進 buffer；
    # 心跳自行 borrow（timeout=1）回報失敗狀態
    assert calls == [None, 1]
    assert len(list((tmp_path / 'buffer').glob('*.json'))) == 1


# ============================================================
# Buffer 容量上限（SUPABASE_BUFFER_MAX_FILES）
# ============================================================

def test_write_to_buffer_enforces_max_files(writer_with_mock_pool, tmp_path, monkeypatch):
    """buffer 檔數達上限時，寫新檔前應先刪最舊（依 mtime），總數不超過上限。"""
    import os
    writer, _ = writer_with_mock_pool
    monkeypatch.setattr('config.SUPABASE_BUFFER_MAX_FILES', 3)

    buffer_dir = tmp_path / 'buffer'
    buffer_dir.mkdir(parents=True, exist_ok=True)
    # 3 個既有檔，mtime 遞增（zz_oldest 檔名字典序最大但 mtime 最舊 —
    # 驗證是按 mtime 不是按檔名刪）
    base = 1_700_000_000
    for i, name in enumerate(['zz_oldest.json', 'aa_mid.json', 'mm_new.json']):
        f = buffer_dir / name
        f.write_text('{}')
        os.utime(f, (base + i, base + i))

    writer._write_to_buffer('youbike', {'data': []}, datetime(2026, 7, 7, 12, 0, 0))

    remaining = {f.name for f in buffer_dir.glob('*.json')}
    assert len(remaining) == 3
    assert 'zz_oldest.json' not in remaining, '應刪除 mtime 最舊的檔'
    assert 'aa_mid.json' in remaining and 'mm_new.json' in remaining
    assert any(n.startswith('youbike_') for n in remaining), '新 buffer 檔應已寫入'
