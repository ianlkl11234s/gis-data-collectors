"""
RateLimiter 單元測試

核心目標：驗證全域節流器能在 5 req/sec 限制下保護 TDX API。

測試項目：
1. 第一次 acquire 不延遲
2. 連續 acquire 會被節流
3. 多線程 acquire 也會被排隊（thread-safe）
4. 統計資料正確
5. get_tdx_rate_limiter singleton 行為
"""

import threading
import time

import pytest

from utils.rate_limiter import RateLimiter, get_tdx_rate_limiter


class TestRateLimiterBasic:
    """單線程基本行為"""

    def test_first_acquire_no_wait(self):
        """第一次 acquire 不應等待"""
        limiter = RateLimiter(rate_per_sec=4)
        wait = limiter.acquire()
        assert wait == 0.0

    def test_second_acquire_waits(self):
        """立刻第二次 acquire 應被節流到最低間隔"""
        limiter = RateLimiter(rate_per_sec=10)  # interval = 0.1s
        limiter.acquire()
        start = time.monotonic()
        wait = limiter.acquire()
        elapsed = time.monotonic() - start

        # 應該等了約 0.1s（含 time.sleep 精度誤差）
        assert wait > 0
        assert 0.08 < elapsed < 0.15

    def test_spaced_acquires_no_wait(self):
        """間隔夠久的 acquire 不應等待"""
        limiter = RateLimiter(rate_per_sec=10)  # interval = 0.1s
        limiter.acquire()
        time.sleep(0.15)  # 超過 interval
        wait = limiter.acquire()
        assert wait == 0.0

    def test_rate_limit_enforced(self):
        """10 次 acquire 在 rate=10 下應該耗時 ~0.9s（9 個間隔）"""
        limiter = RateLimiter(rate_per_sec=10)
        start = time.monotonic()
        for _ in range(10):
            limiter.acquire()
        elapsed = time.monotonic() - start

        # 10 次 = 9 個間隔 × 0.1s = 0.9s（含 sleep 誤差）
        assert 0.85 < elapsed < 1.1, f"10 acquires 耗時 {elapsed:.2f}s（預期 ~0.9s）"


class TestRateLimiterThreadSafety:
    """多線程行為"""

    def test_multi_thread_acquires_are_serialized(self):
        """10 個線程同時 acquire，總耗時應該反映節流（不應平行通過）"""
        limiter = RateLimiter(rate_per_sec=10)  # interval = 0.1s
        results = []
        lock = threading.Lock()

        def worker():
            t = time.monotonic()
            limiter.acquire()
            with lock:
                results.append(time.monotonic() - t)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start

        # 10 個線程全部完成應約 0.9s（9 個間隔）
        # 若限流失效會 <<0.1s
        assert 0.85 < elapsed < 1.3, f"10 threads acquire 耗時 {elapsed:.2f}s（預期 ~0.9s）"

    def test_concurrent_burst_does_not_exceed_rate(self):
        """模擬實際場景：22 個線程同時 submit，不應有任何一對相隔 < min_interval"""
        limiter = RateLimiter(rate_per_sec=4)  # TDX 預設
        timestamps = []
        lock = threading.Lock()

        def worker():
            limiter.acquire()
            with lock:
                timestamps.append(time.monotonic())

        threads = [threading.Thread(target=worker) for _ in range(22)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 驗證相鄰兩次 acquire 間隔都 >= 0.25s - 容忍誤差
        timestamps.sort()
        min_interval = 1.0 / 4  # 0.25s
        for i in range(1, len(timestamps)):
            gap = timestamps[i] - timestamps[i - 1]
            # 容忍 10ms 誤差（time.sleep 不精確 + thread scheduling）
            assert gap >= min_interval - 0.01, (
                f"第 {i} 和 {i-1} 次 acquire 間隔 {gap*1000:.1f}ms，"
                f"小於 min_interval {min_interval*1000:.0f}ms"
            )


class TestRateLimiterStats:
    """統計資料正確性"""

    def test_stats_count_acquires(self):
        limiter = RateLimiter(rate_per_sec=100)
        for _ in range(5):
            limiter.acquire()

        stats = limiter.get_stats()
        assert stats['acquire_count'] == 5
        assert stats['rate_per_sec'] == 100

    def test_stats_tracks_total_wait(self):
        limiter = RateLimiter(rate_per_sec=10)  # interval = 0.1s
        for _ in range(3):
            limiter.acquire()

        stats = limiter.get_stats()
        # 3 次 = 2 個間隔 = ~0.2s total wait
        assert stats['total_wait_sec'] >= 0.15
        assert stats['total_wait_sec'] <= 0.3


class TestRateLimiterValidation:
    """參數驗證"""

    def test_rejects_zero_rate(self):
        with pytest.raises(ValueError, match="rate_per_sec must be positive"):
            RateLimiter(rate_per_sec=0)

    def test_rejects_negative_rate(self):
        with pytest.raises(ValueError, match="rate_per_sec must be positive"):
            RateLimiter(rate_per_sec=-1)


class TestTDXRateLimiterSingleton:
    """get_tdx_rate_limiter() 的 singleton 行為"""

    def test_singleton_returns_same_instance(self):
        """多次呼叫應回傳同一個 limiter"""
        a = get_tdx_rate_limiter()
        b = get_tdx_rate_limiter()
        assert a is b

    def test_singleton_reads_config(self):
        """limiter 的 rate 應符合 config.TDX_RATE_LIMIT"""
        import config
        limiter = get_tdx_rate_limiter()
        assert limiter.rate_per_sec == config.TDX_RATE_LIMIT

    def test_singleton_name_is_tdx(self):
        limiter = get_tdx_rate_limiter()
        stats = limiter.get_stats()
        assert stats['name'] == 'tdx'
