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
    SupabaseWriter._history_dedup_heartbeat_date.clear()

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


# ============================================================
# road_congestion history dedup（內容未變不寫 history，current 仍全量 upsert）
# ============================================================

def _road_congestion_record(section_uid='SEC1', level=2, speed=40.0, tt=120):
    return {
        'section_uid': section_uid, 'section_id': 'S1', 'source': 'thb',
        'city': None, 'authority_code': 'THB',
        'travel_time': tt, 'travel_speed': speed, 'congestion_level': level,
        'congestion_level_id': level, 'data_sources': 'VD',
        'data_collect_time': '2026-09-01T12:00:00+08:00',
        'collected_at': '2026-09-01T12:00:00+08:00',
    }


def _run_road_congestion_write(writer_with_mock_pool, monkeypatch, records, prev_rows,
                                today=None, seed_heartbeat=True):
    """跑一次 _write_to_db('road_congestion', ...)，回傳 (history_values, current_values)。

    prev_rows 對應 dedup SELECT 的欄位順序：
    (section_uid, congestion_level, travel_speed, travel_time) —
    即 table_config['current_key'] + table_config['history_dedup_cols']。

    today: 模擬的「當前台北日期」（date），預設 2026-09-01，monkeypatch
        `storage.supabase_writer._taipei_today`。
    seed_heartbeat: True（預設）＝模擬「今天已經 heartbeat 過」，直接走
        正常 dedup 路徑；False＝模擬「今天第一次寫入」，應觸發全量 heartbeat。
    """
    from datetime import date as _date
    today = today or _date(2026, 9, 1)
    monkeypatch.setattr('storage.supabase_writer._taipei_today', lambda: today)

    writer, mock_pool = writer_with_mock_pool
    mock_pool.statement_timeout_ms = 30_000  # _txn 的 SET LOCAL 會讀
    if seed_heartbeat:
        writer._history_dedup_heartbeat_date['road_congestion'] = today

    captured: dict = {}

    def fake_execute_values(cur, sql, values, page_size=100, template=None):
        if 'road_sections_live' in sql:
            captured['history'] = values
        elif 'road_sections_current' in sql:
            captured['current'] = values

    monkeypatch.setattr('storage.supabase_writer.execute_values', fake_execute_values)

    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = prev_rows
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None

    writer._write_to_db(conn, 'road_congestion', records, datetime(2026, 9, 1, 12, 0, 0))
    return captured.get('history', []), captured.get('current', [])


def test_road_congestion_history_dedup_new_section_writes(writer_with_mock_pool, monkeypatch):
    """current 表查無此 section_uid（新路段）→ 一定寫 history。"""
    record = _road_congestion_record('NEW1')
    history, current = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=[],
    )
    assert len(history) == 1
    assert len(current) == 1  # current 永遠全量 upsert，不受 dedup 影響


def test_road_congestion_history_dedup_unchanged_skips(writer_with_mock_pool, monkeypatch):
    """(congestion_level, travel_speed, travel_time) 完全相同 → 略過 history。"""
    record = _road_congestion_record('SEC1', level=2, speed=40.0, tt=120)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history, current = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=prev_rows,
    )
    assert history == []
    assert len(current) == 1  # current 仍要 upsert


def test_road_congestion_history_dedup_level_changed_writes(writer_with_mock_pool, monkeypatch):
    """congestion_level 變動 → 寫 history。"""
    record = _road_congestion_record('SEC1', level=3, speed=40.0, tt=120)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history, _ = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=prev_rows,
    )
    assert len(history) == 1


def test_road_congestion_history_dedup_speed_changed_writes(writer_with_mock_pool, monkeypatch):
    """travel_speed 變動 → 寫 history。"""
    record = _road_congestion_record('SEC1', level=2, speed=25.0, tt=120)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history, _ = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=prev_rows,
    )
    assert len(history) == 1


def test_road_congestion_history_dedup_travel_time_changed_writes(writer_with_mock_pool, monkeypatch):
    """travel_time 變動 → 寫 history。"""
    record = _road_congestion_record('SEC1', level=2, speed=40.0, tt=200)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history, _ = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=prev_rows,
    )
    assert len(history) == 1


def test_road_congestion_history_dedup_float_precision_tolerant(writer_with_mock_pool, monkeypatch):
    """travel_speed/travel_time 是 DB REAL(float4) 欄位，round-trip 會有 ~1e-6 誤差；
    同一個值不應被誤判為「變動」（回歸測試：float4 精度誤差曾讓 dedup 形同失效）。
    """
    record = _road_congestion_record('SEC1', level=2, speed=43.2, tt=120.5)
    # 模擬 float4 round-trip 後讀回來的浮點誤差（非精確 43.2 / 120.5）
    prev_rows = [('SEC1', 2, 43.20000076294899, 120.50000190734863)]
    history, _ = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=prev_rows,
    )
    assert history == [], 'float4 精度誤差不應被當成內容變動'


def test_road_congestion_history_dedup_mixed_batch(writer_with_mock_pool, monkeypatch):
    """同批次混合：未變的略過、變動的寫入，互不影響。"""
    unchanged = _road_congestion_record('SEC1', level=2, speed=40.0, tt=120)
    changed = _road_congestion_record('SEC2', level=1, speed=50.0, tt=90)
    prev_rows = [('SEC1', 2, 40.0, 120), ('SEC2', 3, 50.0, 90)]
    history, current = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [unchanged, changed], prev_rows=prev_rows,
    )
    assert len(history) == 1  # 只有 SEC2 變動
    assert len(current) == 2  # current 兩筆都 upsert


# ------------------------------------------------------------
# 每日 heartbeat：同一台北日第一次寫入 bypass dedup 全量寫入
# ------------------------------------------------------------

def test_road_congestion_heartbeat_first_write_of_day_writes_all(writer_with_mock_pool, monkeypatch):
    """今天第一次寫入（heartbeat 尚未跑過）→ 即使內容跟 prev 完全相同也要全量寫入。"""
    from datetime import date as _date
    unchanged = _road_congestion_record('SEC1', level=2, speed=40.0, tt=120)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history, current = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [unchanged], prev_rows=prev_rows,
        today=_date(2026, 9, 1), seed_heartbeat=False,
    )
    assert len(history) == 1, 'heartbeat 當輪應 bypass dedup，即使未變動也要寫 history'
    assert len(current) == 1


def test_road_congestion_heartbeat_later_writes_same_day_dedup_normally(writer_with_mock_pool, monkeypatch):
    """同一天後續輪次 → heartbeat 已跑過，走正常 dedup（未變動要略過）。"""
    from datetime import date as _date
    writer, _ = writer_with_mock_pool
    today = _date(2026, 9, 1)

    # 第一輪：heartbeat（今天第一次）
    first = _road_congestion_record('SEC1', level=2, speed=40.0, tt=120)
    history1, _ = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [first], prev_rows=[],
        today=today, seed_heartbeat=False,
    )
    assert len(history1) == 1

    # 第二輪：同一天、內容未變 → 應略過（heartbeat 旗標已消耗）
    second = _road_congestion_record('SEC1', level=2, speed=40.0, tt=120)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history2, current2 = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [second], prev_rows=prev_rows,
        today=today, seed_heartbeat=False,  # 不強制重 seed，沿用上一輪跑完後的狀態
    )
    assert history2 == [], '同一天第二輪起，未變動應正常 dedup 略過'
    assert len(current2) == 1


def test_road_congestion_heartbeat_new_day_writes_all_again(writer_with_mock_pool, monkeypatch):
    """跨到新的台北日 → 即使前一天已經 heartbeat 過，新的一天要再全量寫一次。"""
    from datetime import date as _date
    writer, _ = writer_with_mock_pool

    day1 = _date(2026, 9, 1)
    day2 = _date(2026, 9, 2)

    # day1：先跑過一次 heartbeat（模擬「今天已經 heartbeat 過」的既有狀態）
    writer._history_dedup_heartbeat_date['road_congestion'] = day1

    # day2 第一輪：即使內容跟 prev 完全相同，也要因為跨天而全量寫
    record = _road_congestion_record('SEC1', level=2, speed=40.0, tt=120)
    prev_rows = [('SEC1', 2, 40.0, 120)]
    history, current = _run_road_congestion_write(
        writer_with_mock_pool, monkeypatch, [record], prev_rows=prev_rows,
        today=day2, seed_heartbeat=False,
    )
    assert len(history) == 1, '跨到新的台北日應該重新全量寫入一次'
    assert len(current) == 1
    assert writer._history_dedup_heartbeat_date['road_congestion'] == day2


def test_road_congestion_other_tables_unaffected(writer_with_mock_pool, monkeypatch):
    """沒有 history_dedup_cols 的 collector（如 youbike）行為完全不變：全量寫 history。"""
    writer, mock_pool = writer_with_mock_pool
    mock_pool.statement_timeout_ms = 30_000

    captured = []
    monkeypatch.setattr(
        'storage.supabase_writer.execute_values',
        lambda cur, sql, values, page_size=100, template=None: captured.append((sql, values)),
    )
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = None

    records = [
        {'station_uid': 'HB_1', 'city': 'Taipei', 'available_rent': 2,
         'available_return': 3, 'total': 5, 'collected_at': '2026-09-01T12:00:00+08:00'},
        {'station_uid': 'HB_1', 'city': 'Taipei', 'available_rent': 2,
         'available_return': 3, 'total': 5, 'collected_at': '2026-09-01T12:05:00+08:00'},
    ]
    writer._write_to_db(conn, 'youbike', records, datetime(2026, 9, 1, 12, 5, 0))

    history_calls = [v for sql, v in captured if 'youbike_snapshots' in sql]
    assert history_calls and len(history_calls[0]) == 2, 'youbike 無 dedup 設定，history 應全量寫入兩筆'
    # 沒有 history_dedup_cols → 不應呼叫 SELECT 撈 prev state（cursor.execute 只有 _txn 的 SET LOCAL 一次）
    select_calls = [c for c in cursor.execute.call_args_list if 'youbike_current' in str(c)]
    assert not select_calls
