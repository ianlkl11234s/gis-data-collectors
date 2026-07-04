"""Supabase 連線 helper + 連線池。

connect_supabase()：集中「正確的」psycopg2 連線參數（connect_timeout +
statement_timeout + keepalive 全套），給需要獨立連線的呼叫端用
（satellite_passes_daily / waste_match 等長任務）。

SupabaseConnectionPool：給 SupabaseWriter 用的執行緒連線池，包一層在
psycopg2.pool.ThreadedConnectionPool 外面，加：
- borrow(timeout) context manager — 借不到就 raise，呼叫端進 buffer
- 連線斷路器（指數退避，從原本 supabase_writer 搬過來）
- 死連線自動丟棄（except 時 putconn(close=True)）

為什麼要 pool：
事故（2026-06-26）— 原本 SupabaseWriter 用「一條 conn + 一把 RLock」，
psycopg2 連線 wedge（TCP 沒斷但 server 不回）時所有 collector 排隊死等
RLock 3 小時。改成 pool 後一條死連線只影響當下借它的 collector，其他
collector 借別條繼續寫。
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool as pg_pool

import config

logger = logging.getLogger(__name__)


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
    # ⚠️ 連的是 Supavisor transaction mode pooler（6543）時，此 startup `options`
    # 會被 pooler 丟棄 → SHOW statement_timeout = 0（AR-06 實測 2026-07-02）。
    # 對「一條連線 = 一個交易」的長任務（satellite_passes_daily / waste_match）
    # 若真的要保護單筆 SQL，需在【該筆 SQL 所在的 transaction 內】自行下
    # SET LOCAL statement_timeout（見 storage/supabase_writer.py 的 _txn()）。
    # 保留 options 是為了直連 5432 / session-mode pooler 時仍能生效。
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


class PoolBorrowTimeout(Exception):
    """borrow() 在 timeout 內拿不到空閒連線時 raise。

    呼叫端的正確反應：把資料寫進 buffer，下次重試。**不要**繼續等。
    """


class PoolBreakerOpen(Exception):
    """連線斷路器開啟中（連續建線失敗，正在冷卻）時 raise。"""


class SupabaseConnectionPool:
    """ThreadedConnectionPool wrapper：borrow timeout + 斷路器 + 死連線自動丟。

    Usage:
        pool = SupabaseConnectionPool()
        with pool.borrow() as conn:
            with conn.cursor() as cur:
                cur.execute("...")
        # __exit__ 自動 putconn；若 with 區塊 raise，自動 putconn(close=True)
    """

    # 斷路器冷卻參數（從舊 SupabaseWriter 搬過來，沿用既有指數退避）
    _CB_COOLDOWN_BASE = 15      # 第一次失敗後冷卻秒數
    _CB_COOLDOWN_MAX = 300      # 冷卻上限

    def __init__(self,
                 minconn: Optional[int] = None,
                 maxconn: Optional[int] = None,
                 borrow_timeout: Optional[float] = None,
                 statement_timeout_ms: Optional[int] = None):
        self.minconn = minconn if minconn is not None else config.SUPABASE_POOL_MIN
        self.maxconn = maxconn if maxconn is not None else config.SUPABASE_POOL_MAX
        self.borrow_timeout = (borrow_timeout
                               if borrow_timeout is not None
                               else config.SUPABASE_BORROW_TIMEOUT_SEC)
        self.statement_timeout_ms = (statement_timeout_ms
                                     if statement_timeout_ms is not None
                                     else config.SUPABASE_STATEMENT_TIMEOUT_MS)

        # 斷路器狀態（thread-safe via _state_lock）
        self._connect_failures = 0
        self._next_connect_at = 0.0
        self._state_lock = threading.Lock()

        # condition 用來實作 borrow timeout（psycopg2 pool 沒原生 timeout）
        self._cond = threading.Condition()
        self._pool: Optional[pg_pool.ThreadedConnectionPool] = None
        self._init_pool()

    def _init_pool(self) -> None:
        """初始化底層 ThreadedConnectionPool。建線失敗會記到斷路器。

        ThreadedConnectionPool 的 minconn 是 lazy — 不會在 __init__ 強制建滿，
        而是首次 getconn 才建。所以這裡只負責設定 pool 物件本身。
        """
        try:
            # ⚠️ 此 startup `options` 在 Supavisor transaction mode pooler（6543）
            # 會被丟棄（AR-06 實測 2026-07-02：SHOW statement_timeout = 0、
            # pg_sleep(35) 不被砍）。SupabaseWriter 的實際保護改由每個寫入
            # transaction 內的 SET LOCAL 提供（見 supabase_writer.SupabaseWriter._txn）。
            # 這裡保留 options 僅為直連 5432 / session-mode 時的備援，勿倚賴它。
            self._pool = pg_pool.ThreadedConnectionPool(
                minconn=self.minconn,
                maxconn=self.maxconn,
                dsn=config.SUPABASE_DB_URL,
                connect_timeout=config.SUPABASE_CONNECT_TIMEOUT,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
                options=(
                    f"-c statement_timeout={self.statement_timeout_ms} "
                    f"-c idle_in_transaction_session_timeout={self.statement_timeout_ms}"
                ),
            )
            with self._state_lock:
                self._connect_failures = 0
                self._next_connect_at = 0.0
            logger.info(
                f"Supabase pool 初始化成功 (min={self.minconn}, max={self.maxconn}, "
                f"borrow_timeout={self.borrow_timeout}s)"
            )
        except Exception as e:
            self._record_connect_failure(e)
            self._pool = None

    def _record_connect_failure(self, err: Exception) -> None:
        """記錄建線失敗，依指數退避計算冷卻到期時間。"""
        with self._state_lock:
            self._connect_failures += 1
            cooldown = min(
                self._CB_COOLDOWN_BASE * (2 ** (self._connect_failures - 1)),
                self._CB_COOLDOWN_MAX,
            )
            self._next_connect_at = time.monotonic() + cooldown
            logger.warning(
                f"Supabase 連線失敗（第 {self._connect_failures} 次，"
                f"冷卻 {cooldown}s）: {err}"
            )

    def _record_connect_success(self) -> None:
        with self._state_lock:
            if self._connect_failures > 0:
                logger.info(f"Supabase 連線恢復（先前連續失敗 {self._connect_failures} 次）")
            self._connect_failures = 0
            self._next_connect_at = 0.0

    def _breaker_open(self) -> bool:
        with self._state_lock:
            return time.monotonic() < self._next_connect_at

    # pre-ping：連續拿到死連線時最多換幾條，避免無限迴圈。實務上換 1 條就會拿到
    # 全新 TCP 連線，設 3 只是安全邊際。
    _MAX_STALE_SWAPS = 3

    @contextmanager
    def borrow(self, timeout: Optional[float] = None):
        """借一條連線，with 區塊結束自動歸還；異常時強制關閉並丟棄。

        借出前會 pre-ping（SELECT 1）確認連線存活：Supavisor transaction pool
        會在交易之間回收 backend，池裡閒置的連線可能 server 端已死、client 端
        conn.closed 仍為 0，下一個借到它的 collector 一 execute 就撞
        `connection already closed`。pre-ping 失敗就關掉換一條，確保交出去的一定
        是活線（等同 SQLAlchemy pool_pre_ping）。

        Raises:
            PoolBreakerOpen: 斷路器開啟中（冷卻期內）／連續換線都拿到死連線
            PoolBorrowTimeout: timeout 內所有連線都 busy
        """
        if self._breaker_open():
            raise PoolBreakerOpen("Supabase 連線斷路器開啟中（冷卻中）")

        # 若 pool 之前建失敗，這裡再試一次
        if self._pool is None:
            self._init_pool()
            if self._pool is None:
                raise PoolBreakerOpen("Supabase 連線池不可用")

        deadline = time.monotonic() + (timeout if timeout is not None
                                       else self.borrow_timeout)

        # 借一條「確認活著」的連線：pre-ping 失敗就關掉換一條（最多 _MAX_STALE_SWAPS 次）
        conn = None
        for _ in range(self._MAX_STALE_SWAPS + 1):
            candidate = self._getconn_blocking(deadline)
            if self._preping(candidate):
                conn = candidate
                break
            # pre-ping 失敗 = 死連線：close=True 真關掉並移出池，下次 getconn 建新的
            logger.warning("borrow: 連線 pre-ping 失敗（死連線），關閉並換一條")
            try:
                self._pool.putconn(candidate, close=True)
            except Exception:
                pass
        if conn is None:
            # 連換幾條都死 → 視為 DB 暫時不可用，讓呼叫端進 buffer（不是 bug）
            raise PoolBreakerOpen(
                f"borrow: 連續 {self._MAX_STALE_SWAPS + 1} 條連線 pre-ping 均失敗"
            )

        broken = False
        try:
            yield conn
        except Exception:
            broken = True
            raise
        finally:
            # close=True 時 psycopg2 pool 會把連線真關掉並從池移除，下次 getconn
            # 會建新的。這是讓死連線退場的關鍵。
            try:
                self._pool.putconn(conn, close=broken)
            except Exception as e:
                logger.warning(f"putconn 失敗（已忽略）: {e}")

    def _getconn_blocking(self, deadline: float):
        """從底層 pool 取一條連線（池滿時阻塞到 deadline），並設好 autocommit。

        Raises:
            PoolBorrowTimeout: deadline 內所有連線都 busy（池滿）
            PoolBreakerOpen: 建線本身失敗（DB unreachable / pooler 拒連）
        """
        conn = None
        last_err: Optional[Exception] = None
        while True:
            try:
                conn = self._pool.getconn()
                break
            except pg_pool.PoolError as e:
                # PoolError = 連線池滿（所有連線都 borrowed 出去）
                last_err = e
                if time.monotonic() >= deadline:
                    raise PoolBorrowTimeout(
                        f"borrow timeout {self.borrow_timeout}s — 所有連線都 busy"
                    ) from e
                # 短暫等待後重試
                time.sleep(0.1)
            except psycopg2.OperationalError as e:
                # 建線本身失敗（DB unreachable / pooler 拒連 / 等）
                self._record_connect_failure(e)
                raise PoolBreakerOpen(f"Supabase 建線失敗: {e}") from e

        if conn is None:
            raise PoolBorrowTimeout(f"borrow failed: {last_err}")

        # 標記成功（第一次成功就重置斷路器）
        self._record_connect_success()
        # 確保 autocommit=True（避免 idle_in_transaction_session_timeout 砍，
        # 也讓 pre-ping 的 SELECT 1 不會殘留開啟的交易）
        try:
            conn.autocommit = True
        except Exception:
            pass
        return conn

    @staticmethod
    def _preping(conn) -> bool:
        """輕量存活檢查（唯讀，不碰任何資料）：conn 未關且 SELECT 1 有回 → True。

        涵蓋兩種死法：
        - client 端已知關閉（conn.closed != 0）
        - server 端靜默回收（conn.closed 仍為 0，execute 才發現 EOF）
        """
        if conn.closed:
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    def snapshot(self) -> dict:
        """無副作用的健康摘要，給 /health endpoint 用。**絕不**借連線。"""
        with self._state_lock:
            return {
                "pool_initialized": self._pool is not None,
                "minconn": self.minconn,
                "maxconn": self.maxconn,
                "borrow_timeout_sec": self.borrow_timeout,
                "connect_failures": self._connect_failures,
                "breaker_open": time.monotonic() < self._next_connect_at,
            }

    def close(self) -> None:
        """關閉整個 pool（測試 / shutdown 用）。"""
        if self._pool is not None:
            try:
                self._pool.closeall()
            except Exception:
                pass
            self._pool = None
