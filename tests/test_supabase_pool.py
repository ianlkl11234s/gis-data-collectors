"""SupabaseConnectionPool 單元測試。

不打真實 DB。用 mock 在 psycopg2.pool 層攔截 getconn / putconn 行為，
驗證：
- borrow timeout 真的會 raise（不無限等）
- 斷路器在連續失敗後開啟、冷卻過期後重置
- with 區塊 raise 時，連線以 close=True 歸還（讓死連線退場）
- 並發 borrow 不互相阻塞
- snapshot() 不會借連線（不放大故障）

對應 plan 5.1：tests/test_supabase_pool.py
"""
from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from psycopg2 import pool as pg_pool
from psycopg2 import OperationalError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _live_conn(name=None):
    """模擬一條「活連線」：closed=0 讓 borrow() 的 pre-ping（SELECT 1）會過。

    MagicMock 的 cursor()/execute()/fetchone() 預設就不會 raise，所以 pre-ping
    只卡在 .closed 這關 — 預設 MagicMock().closed 是 truthy 會被判死連線。
    """
    c = MagicMock(name=name)
    c.closed = 0
    return c


@pytest.fixture
def fake_pool_factory(monkeypatch):
    """攔截 psycopg2 ThreadedConnectionPool 建構。回傳 (mock_pool_cls, get_state) 元組。

    mock_pool_cls.return_value 是 mock pool 實例 — 測試可改它的 getconn / putconn 行為。
    """
    created: list[MagicMock] = []

    def factory(*args, **kwargs):
        # 不用 spec — ThreadedConnectionPool 用 __new__ + dynamic attrs，spec 抓不全
        p = MagicMock()
        p.getconn.side_effect = lambda: _live_conn()
        p.putconn.side_effect = lambda conn, close=False: None
        p.closeall.side_effect = lambda: None
        created.append(p)
        return p

    monkeypatch.setattr(
        'storage.db.pg_pool.ThreadedConnectionPool',
        factory,
    )
    return created


@pytest.fixture
def pool(fake_pool_factory, monkeypatch):
    """已初始化的 SupabaseConnectionPool（用 fake 底層）。"""
    # 強制 config 用測試值
    monkeypatch.setattr('config.SUPABASE_DB_URL', 'postgresql://test')
    monkeypatch.setattr('config.SUPABASE_CONNECT_TIMEOUT', 5)
    monkeypatch.setattr('config.SUPABASE_STATEMENT_TIMEOUT_MS', 30000)
    monkeypatch.setattr('config.SUPABASE_POOL_MIN', 2)
    monkeypatch.setattr('config.SUPABASE_POOL_MAX', 5)
    monkeypatch.setattr('config.SUPABASE_BORROW_TIMEOUT_SEC', 0.5)

    from storage.db import SupabaseConnectionPool
    p = SupabaseConnectionPool()
    yield p
    p.close()


# ---------------------------------------------------------------------------
# Borrow timeout
# ---------------------------------------------------------------------------

def test_borrow_succeeds_normally(pool, fake_pool_factory):
    """正常借得到連線時，with 區塊內 conn 可用，結束時 putconn(close=False)。"""
    underlying = fake_pool_factory[0]
    conn_returned = _live_conn()
    underlying.getconn.side_effect = lambda: conn_returned

    with pool.borrow() as conn:
        assert conn is conn_returned

    underlying.putconn.assert_called_once_with(conn_returned, close=False)


def test_borrow_timeout_raises_when_pool_full(pool, fake_pool_factory):
    """池滿 (PoolError) 持續到 timeout → raise PoolBorrowTimeout。"""
    from storage.db import PoolBorrowTimeout
    underlying = fake_pool_factory[0]
    underlying.getconn.side_effect = pg_pool.PoolError("connection pool exhausted")

    start = time.monotonic()
    with pytest.raises(PoolBorrowTimeout):
        with pool.borrow(timeout=0.3) as conn:
            pass
    elapsed = time.monotonic() - start
    # 應該大約在 timeout 後就放棄（容許 0.5s 上限）
    assert 0.25 <= elapsed <= 0.6, f"borrow waited {elapsed}s, expected ~0.3s"


def test_borrow_uses_default_timeout_when_not_specified(pool, fake_pool_factory):
    """borrow() 不傳 timeout 時用 config 預設值（測試 fixture 設 0.5s）。"""
    from storage.db import PoolBorrowTimeout
    underlying = fake_pool_factory[0]
    underlying.getconn.side_effect = pg_pool.PoolError("exhausted")

    start = time.monotonic()
    with pytest.raises(PoolBorrowTimeout):
        with pool.borrow() as conn:
            pass
    elapsed = time.monotonic() - start
    assert 0.4 <= elapsed <= 0.7


# ---------------------------------------------------------------------------
# 斷路器
# ---------------------------------------------------------------------------

def test_breaker_opens_on_operational_error(pool, fake_pool_factory):
    """建線失敗（OperationalError）→ 記錄失敗 + 斷路器開啟。"""
    from storage.db import PoolBreakerOpen
    underlying = fake_pool_factory[0]
    underlying.getconn.side_effect = OperationalError("Connection refused")

    # 第一次：raise PoolBreakerOpen 並記錄
    with pytest.raises(PoolBreakerOpen):
        with pool.borrow() as conn:
            pass

    # 斷路器應開啟（next_connect_at 在未來）
    assert pool._connect_failures == 1
    assert pool._breaker_open()


def test_breaker_blocks_subsequent_calls_during_cooldown(pool, fake_pool_factory):
    """斷路器開啟後，後續 borrow 直接 raise（不再打底層）。"""
    from storage.db import PoolBreakerOpen
    underlying = fake_pool_factory[0]

    # 手動設斷路器開啟（模擬已失敗）
    pool._connect_failures = 3
    pool._next_connect_at = time.monotonic() + 60

    underlying.getconn.reset_mock()
    with pytest.raises(PoolBreakerOpen):
        with pool.borrow() as conn:
            pass
    # 重點：fast-fail，不該打到底層 getconn
    underlying.getconn.assert_not_called()


def test_breaker_resets_on_successful_borrow(pool, fake_pool_factory):
    """連線成功後，斷路器計數重置為 0。"""
    underlying = fake_pool_factory[0]

    # 預設失敗一次
    pool._connect_failures = 2
    pool._next_connect_at = 0  # 已過期，允許重試

    underlying.getconn.side_effect = lambda: _live_conn()
    with pool.borrow() as conn:
        pass

    assert pool._connect_failures == 0
    assert not pool._breaker_open()


# ---------------------------------------------------------------------------
# 死連線回收
# ---------------------------------------------------------------------------

def test_exception_in_with_block_closes_connection(pool, fake_pool_factory):
    """with 區塊內 raise 時，連線以 close=True 歸還（讓死連線退場）。"""
    underlying = fake_pool_factory[0]
    conn_returned = _live_conn()
    underlying.getconn.side_effect = lambda: conn_returned

    with pytest.raises(ValueError):
        with pool.borrow() as conn:
            raise ValueError("simulated db error")

    underlying.putconn.assert_called_once_with(conn_returned, close=True)


def test_normal_exit_returns_connection_to_pool(pool, fake_pool_factory):
    """正常結束時，連線以 close=False 歸還（可重用）。"""
    underlying = fake_pool_factory[0]
    conn_returned = _live_conn()
    underlying.getconn.side_effect = lambda: conn_returned

    with pool.borrow() as conn:
        pass

    underlying.putconn.assert_called_once_with(conn_returned, close=False)


# ---------------------------------------------------------------------------
# pre-ping（死連線在交出前先換掉）
# ---------------------------------------------------------------------------

def test_preping_swaps_dead_connection(pool, fake_pool_factory):
    """借到已被 server 回收的死連線（conn.closed != 0）時，關掉換一條活的再交出。

    對應生產問題：Supavisor 在交易間回收 backend → 下一個借到的 collector 撞
    `connection already closed`。pre-ping 應在交出前就換掉它。
    """
    underlying = fake_pool_factory[0]
    dead = MagicMock(name="dead")
    dead.closed = 1                       # server 已回收 → pre-ping 判死
    live = _live_conn(name="live")
    conns = iter([dead, live])
    underlying.getconn.side_effect = lambda: next(conns)

    with pool.borrow() as conn:
        assert conn is live               # 交出去的是活線，不是死線

    underlying.putconn.assert_any_call(dead, close=True)   # 死線被丟棄
    underlying.putconn.assert_any_call(live, close=False)  # 活線正常歸還


def test_preping_swaps_on_select_error(pool, fake_pool_factory):
    """conn.closed 為 0 但 SELECT 1 丟例外（server 靜默斷線）也應判死並換線。"""
    underlying = fake_pool_factory[0]
    dead = _live_conn(name="dead")        # closed=0 但 cursor 會 raise
    dead.cursor.side_effect = OperationalError("server closed the connection unexpectedly")
    live = _live_conn(name="live")
    conns = iter([dead, live])
    underlying.getconn.side_effect = lambda: next(conns)

    with pool.borrow() as conn:
        assert conn is live

    underlying.putconn.assert_any_call(dead, close=True)


def test_preping_all_dead_raises_breaker(pool, fake_pool_factory):
    """連換幾條都是死連線 → raise PoolBreakerOpen，讓呼叫端進 buffer（不無限迴圈）。"""
    from storage.db import PoolBreakerOpen
    underlying = fake_pool_factory[0]
    dead = MagicMock()
    dead.closed = 1
    underlying.getconn.side_effect = lambda: dead

    with pytest.raises(PoolBreakerOpen):
        with pool.borrow() as conn:
            pass


# ---------------------------------------------------------------------------
# 並發
# ---------------------------------------------------------------------------

def test_concurrent_borrows_get_independent_connections(pool, fake_pool_factory):
    """多 thread 同時 borrow 不互相 block（每個拿自己的 mock conn）。"""
    underlying = fake_pool_factory[0]
    # 每次 getconn 回不同 mock
    counter = {'n': 0}
    lock = threading.Lock()

    def make_conn():
        with lock:
            counter['n'] += 1
        return _live_conn(name=f"conn-{counter['n']}")

    underlying.getconn.side_effect = make_conn

    borrowed: list = []

    def worker():
        with pool.borrow() as conn:
            borrowed.append(conn)
            time.sleep(0.05)  # 模擬寫 DB 期間其他 thread 應能各自運作

    threads = [threading.Thread(target=worker) for _ in range(10)]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    # 10 個 thread 同時跑 50ms 工作，理想並發下 < 0.2s
    # 序列化會 > 0.5s — 確保是並發
    assert elapsed < 0.3, f"並發 borrow 跑了 {elapsed}s，太慢"
    assert len(borrowed) == 10
    # 確認真的拿到 10 條不同 mock
    assert len({id(c) for c in borrowed}) == 10


def test_wedged_conn_does_not_block_others(pool, fake_pool_factory):
    """事故模擬：一個 thread 卡住 conn，其他 thread 仍應能借到別的 conn 完成。"""
    underlying = fake_pool_factory[0]

    # 兩條 mock conn
    conn_pool = [_live_conn(name="conn-A"), _live_conn(name="conn-B")]
    idx = {'i': 0}
    glock = threading.Lock()

    def get_next():
        with glock:
            c = conn_pool[idx['i'] % len(conn_pool)]
            idx['i'] += 1
        return c

    underlying.getconn.side_effect = get_next

    wedged_started = threading.Event()
    release_wedge = threading.Event()
    others_done = threading.Event()

    def wedged_writer():
        with pool.borrow() as conn:
            wedged_started.set()
            release_wedge.wait()

    def fast_writer():
        with pool.borrow() as conn:
            time.sleep(0.01)

    wedged_t = threading.Thread(target=wedged_writer)
    wedged_t.start()
    wedged_started.wait()

    # 卡住期間，其他 5 個 thread 應能跑完
    others = [threading.Thread(target=fast_writer) for _ in range(5)]
    start = time.monotonic()
    for t in others:
        t.start()
    for t in others:
        t.join()
    elapsed = time.monotonic() - start

    others_done.set()
    release_wedge.set()
    wedged_t.join()

    # 其他 thread 應在合理時間內完成（不被 wedged 卡）
    assert elapsed < 0.3, f"其他 thread 被卡住的 conn 影響，跑了 {elapsed}s"


# ---------------------------------------------------------------------------
# Snapshot（health endpoint 用）
# ---------------------------------------------------------------------------

def test_snapshot_does_not_borrow_connection(pool, fake_pool_factory):
    """snapshot() 不該觸發 getconn — 健康檢查不能放大故障。"""
    underlying = fake_pool_factory[0]
    underlying.getconn.reset_mock()

    snap = pool.snapshot()

    underlying.getconn.assert_not_called()
    assert 'breaker_open' in snap
    assert 'connect_failures' in snap
    assert 'minconn' in snap
    assert 'maxconn' in snap
    assert 'pool_initialized' in snap


def test_snapshot_reflects_breaker_state(pool):
    """斷路器開啟時 snapshot 該反映出來。"""
    snap = pool.snapshot()
    assert snap['breaker_open'] is False

    pool._connect_failures = 5
    pool._next_connect_at = time.monotonic() + 100

    snap = pool.snapshot()
    assert snap['breaker_open'] is True
    assert snap['connect_failures'] == 5
