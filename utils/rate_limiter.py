"""
Rate Limiter — API 節流器

核心用途：保護第三方 API（如 TDX）的每秒請求上限，避免 HTTP 429。

設計：token bucket 簡化版（固定間隔節流）
- 每次 acquire() 保證與上一次間隔至少 min_interval 秒
- Thread-safe：多 collector 同時呼叫會排隊

為何不用更複雜的 token bucket / sliding window？
- TDX 限制是「X 次/秒」，固定間隔已足夠
- 實作簡單、debug 容易、行為可預測
- 副作用：throughput 上限 = rate（無法做 burst）— 對我們來說沒差

未來若要升級：
- 可加 max_burst 參數做真正的 token bucket
- 可加 per-endpoint 分類節流
"""

import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


class RateLimiter:
    """固定間隔節流器

    Usage:
        limiter = RateLimiter(rate_per_sec=4)
        limiter.acquire()  # 第一次不等
        limiter.acquire()  # 距上次若 < 0.25s 會 sleep 補足
    """

    def __init__(self, rate_per_sec: float, name: str = "default"):
        """
        Args:
            rate_per_sec: 每秒最多幾次 acquire。例如 4 代表最快 4 req/sec。
            name: 給 log 用的識別名稱
        """
        if rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec must be positive, got {rate_per_sec}")
        self.rate_per_sec = rate_per_sec
        self.min_interval = 1.0 / rate_per_sec
        self.name = name
        self._lock = threading.Lock()
        self._last_acquire = 0.0
        # 統計
        self._acquire_count = 0
        self._total_wait = 0.0

    def acquire(self) -> float:
        """取得一次 slot。若距離上次呼叫 < min_interval，會 sleep 補足。

        Returns:
            這次等了幾秒（第一次或時間已到會是 0）
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_acquire
            wait = 0.0
            if self._last_acquire > 0 and elapsed < self.min_interval:
                wait = self.min_interval - elapsed
                time.sleep(wait)
                self._total_wait += wait

            self._last_acquire = time.monotonic()
            self._acquire_count += 1
            return wait

    def get_stats(self) -> dict:
        """回傳統計數據（供 log / health check 用）"""
        with self._lock:
            return {
                'name': self.name,
                'rate_per_sec': self.rate_per_sec,
                'acquire_count': self._acquire_count,
                'total_wait_sec': round(self._total_wait, 3),
                'avg_wait_ms': round(
                    (self._total_wait / self._acquire_count * 1000)
                    if self._acquire_count else 0,
                    1,
                ),
            }


# ============================================================
# TDX 全域 limiter singleton
# ============================================================
# TDX 金鑰限制：預設 5 req/sec/key，我們用 4 留 buffer
# 所有 TDX collector 共用此 limiter，確保整個 IP 出口不會超標
# ============================================================

_tdx_limiter: Optional[RateLimiter] = None
_tdx_limiter_lock = threading.Lock()


def get_tdx_rate_limiter() -> RateLimiter:
    """取得全域 TDX rate limiter singleton

    所有 TDX collector / TDXAuth 的 HTTP 請求都應該先 acquire() 這個 limiter。
    """
    global _tdx_limiter
    if _tdx_limiter is None:
        with _tdx_limiter_lock:
            if _tdx_limiter is None:
                # 延遲 import，避免 config 初始化順序問題
                import config
                rate = getattr(config, 'TDX_RATE_LIMIT', 4)
                _tdx_limiter = RateLimiter(rate_per_sec=rate, name='tdx')
                logger.info(f"[rate_limiter] TDX limiter 初始化: {rate} req/sec")
    return _tdx_limiter
