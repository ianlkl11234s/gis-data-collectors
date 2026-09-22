"""AR-06：statement_timeout 保護在 Supavisor transaction pooler 下是否真的生效。

背景（2026-07-02 實測）：
連 Supavisor transaction mode pooler（port 6543）時，psycopg2 連線的
startup `options=-c statement_timeout=...` 會被 pooler 丟棄
→ `SHOW statement_timeout` 回 `0`、`SELECT pg_sleep(35)` 完整跑完不被砍。
唯一可靠的做法是在【與工作語句同一個 transaction】內下 SET LOCAL，這正是
`SupabaseWriter._txn()` 做的事。本測試同時驗證「缺陷仍在」與「_txn 有修好」。

需要真實測試 DB，必須同時提供明確 flag 與測試專用 DSN：
`TEST_SUPABASE_DB_URL=... python3 -m pytest --run-live-supabase tests/test_statement_timeout_pooler.py -v`
"""
from __future__ import annotations

import os
import time

import pytest

import psycopg2
from psycopg2.errors import QueryCanceled

pytestmark = pytest.mark.live_supabase


@pytest.fixture(scope="module")
def live_supabase_dsn(request):
    if not request.config.getoption("--run-live-supabase"):
        pytest.skip("需要 --run-live-supabase 才會連線測試 Supabase")
    dsn = os.environ.get("TEST_SUPABASE_DB_URL", "").strip()
    if not dsn:
        pytest.fail("--run-live-supabase 需要 TEST_SUPABASE_DB_URL（不可沿用 production DSN）")
    return dsn


@pytest.fixture
def short_timeout_pool(monkeypatch, live_supabase_dsn):
    """建一個 statement_timeout=2s 的小型 pool，指向真實 pooler。用完即關。"""
    import config
    monkeypatch.setattr(config, "SUPABASE_DB_URL", live_supabase_dsn)

    from storage.db import SupabaseConnectionPool
    pool = SupabaseConnectionPool(
        minconn=1, maxconn=2, statement_timeout_ms=2000,
    )
    try:
        yield pool
    finally:
        pool.close()


def test_startup_options_dropped_by_pooler(short_timeout_pool):
    """記錄缺陷：透過 startup options 傳的 statement_timeout 被 transaction pooler 丟棄。

    pool 用 statement_timeout_ms=2000 建線（startup options 帶 `-c statement_timeout=2000`）。
    若 pooler 有採納，autocommit 下 SHOW 應回 '2s'。實測【永遠不是 2s】——回的是
    server/role 預設或前一個 pooled client 洩漏的 session 值（觀察到 0 / 5s / 10s），
    證明 startup options 被 pooler 丟棄、且 autocommit 下的有效 timeout 不可控
    （曾實測到 0 = 完全無保護，pg_sleep(35) 跑滿不被砍）。
    """
    with short_timeout_pool.borrow() as conn:  # borrow() 內 autocommit=True
        with conn.cursor() as cur:
            cur.execute("SHOW statement_timeout")
            value = cur.fetchone()[0]
    assert value != "2s", (
        f"startup options 竟被 pooler 採納（SHOW={value!r} == 我們設的 2s）；"
        "若連線改走 session mode / 直連 5432 才會如此，需重新評估保護策略。"
    )


def test_txn_enforces_statement_timeout(short_timeout_pool, live_supabase_dsn):
    """驗證修正：SupabaseWriter._txn() 用 SET LOCAL 讓 statement_timeout 真的生效。

    在 _txn transaction 內 SHOW 應為 2s，且 pg_sleep(5) 會在 ~2s 被砍
    （QueryCanceled），不會跑滿 5s。
    """
    from storage.supabase_writer import SupabaseWriter

    writer = SupabaseWriter(live_supabase_dsn)
    writer._pool.close()
    writer._pool = short_timeout_pool  # 換成 2s timeout 的 pool

    with writer._pool.borrow() as conn:
        # SET LOCAL 在 transaction 內生效
        with writer._txn(conn) as cur:
            cur.execute("SHOW statement_timeout")
            assert cur.fetchone()[0] == "2s"

        # pg_sleep(5) 應在 ~2s 被 statement_timeout 砍掉
        t0 = time.monotonic()
        with pytest.raises(QueryCanceled):
            with writer._txn(conn) as cur:
                cur.execute("SELECT pg_sleep(5)")
        elapsed = time.monotonic() - t0

    assert elapsed < 4.0, (
        f"pg_sleep(5) 跑了 {elapsed:.1f}s 才結束，statement_timeout 未在 ~2s 生效"
    )
