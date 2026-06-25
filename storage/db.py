"""Supabase 連線 helper

集中「正確的」psycopg2 連線參數，供需要直接連 DB 的 collector / task 使用。
參考樣板：storage/supabase_writer.py:_connect。

為什麼存在：
裸 psycopg2.connect(SUPABASE_DB_URL) 在 Supavisor transaction mode (port 6543) 下
若連線含長 query 或長 idle 期（例：satellite_passes_daily 的 SGP4 ~40s 純 Python 段）
會被 pooler 端切 SSL，丟「SSL connection has been closed unexpectedly」。
helper 加 connect_timeout + statement_timeout + idle_in_transaction_session_timeout
+ TCP keepalive，從根本避免這種斷線。
"""
from __future__ import annotations

import psycopg2

import config


def connect_supabase(autocommit: bool = True,
                     statement_timeout_ms: int | None = None):
    """建立保護完整的 Supabase psycopg2 連線。

    Args:
        autocommit: 預設 True。避免「開了 transaction 但中間長時間沒 SQL」被
            idle_in_transaction_session_timeout 砍掉。需要交易性的呼叫端
            自己關掉。
        statement_timeout_ms: 覆寫單筆 SQL timeout。預設使用
            config.SUPABASE_STATEMENT_TIMEOUT_MS（30s）。對跑大 UPDATE / SP
            的 collector 可拉長（例：satellite_passes_daily 給 300_000 = 5 分鐘）。
    """
    timeout = statement_timeout_ms or config.SUPABASE_STATEMENT_TIMEOUT_MS
    conn = psycopg2.connect(
        config.SUPABASE_DB_URL,
        connect_timeout=config.SUPABASE_CONNECT_TIMEOUT,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        options=(
            f"-c statement_timeout={timeout} "
            f"-c idle_in_transaction_session_timeout={timeout}"
        ),
    )
    conn.autocommit = autocommit
    return conn
