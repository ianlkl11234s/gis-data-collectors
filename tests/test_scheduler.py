"""
CollectorScheduler 單元測試

測試目標：驗證 Phase 1 的核心保護機制
1. 基本提交與執行
2. Skip-if-running（同 collector 不重疊）
3. Exception 不汙染其他任務
4. 線程名稱正確
5. get_status 正確反映狀態
6. shutdown 乾淨關閉
"""

import logging
import threading
import time

import pytest

from scheduler import CollectorScheduler


# ============================================================
# 測試用的 fake collector
# ============================================================

class FakeCollector:
    """模擬 collector，避免依賴 BaseCollector 與外部 I/O"""

    name = "fake"
    COLLECT_TIMEOUT = 10

    def __init__(self, name="fake", sleep_sec=0.0, raise_exc=None):
        self.name = name
        self.sleep_sec = sleep_sec
        self.raise_exc = raise_exc
        self.run_count = 0
        self.thread_names = []
        self._run_lock = threading.Lock()

    def run(self):
        with self._run_lock:
            self.run_count += 1
        self.thread_names.append(threading.current_thread().name)
        if self.sleep_sec > 0:
            time.sleep(self.sleep_sec)
        if self.raise_exc is not None:
            raise self.raise_exc
        return {'ok': True, 'run_count': self.run_count}


# ============================================================
# 基本執行
# ============================================================

def test_submit_and_run_basic():
    """提交後 collector 應該確實被執行"""
    sched = CollectorScheduler(max_workers=2)
    try:
        collector = FakeCollector(name="c1")
        future = sched.submit(collector)

        assert future is not None
        result = future.result(timeout=5)

        assert result == {'ok': True, 'run_count': 1}
        assert collector.run_count == 1
    finally:
        sched.shutdown(wait=True)


def test_submit_multiple_different_collectors_run_parallel():
    """不同 collector 應該平行執行（互不阻塞）"""
    sched = CollectorScheduler(max_workers=3)
    try:
        # 三個都 sleep 0.5s；若序列執行需 1.5s，平行應 <1s
        c1 = FakeCollector(name="p1", sleep_sec=0.5)
        c2 = FakeCollector(name="p2", sleep_sec=0.5)
        c3 = FakeCollector(name="p3", sleep_sec=0.5)

        start = time.monotonic()
        f1 = sched.submit(c1)
        f2 = sched.submit(c2)
        f3 = sched.submit(c3)

        f1.result(timeout=5)
        f2.result(timeout=5)
        f3.result(timeout=5)
        elapsed = time.monotonic() - start

        # 平行執行應 < 1s（允許 0.8s 裕度），如果序列執行會 >= 1.5s
        assert elapsed < 1.0, f"應平行執行但耗時 {elapsed:.2f}s（疑似序列化）"
        assert c1.run_count == 1
        assert c2.run_count == 1
        assert c3.run_count == 1
    finally:
        sched.shutdown(wait=True)


# ============================================================
# Skip-if-running：核心保護機制
# ============================================================

def test_skip_if_previous_run_still_running():
    """同一 collector 上一輪還在跑，新提交應被 skip"""
    sched = CollectorScheduler(max_workers=5)
    try:
        # sleep 較長，模擬慢 collector
        collector = FakeCollector(name="slow", sleep_sec=0.8)

        # 第一次提交：接受
        f1 = sched.submit(collector)
        assert f1 is not None

        # 稍等確保 f1 進入執行階段
        time.sleep(0.05)

        # 第二次提交：應該 skip（回傳 None）
        f2 = sched.submit(collector)
        assert f2 is None, "第二次應該被 skip"

        # 第三次提交：依然 skip
        f3 = sched.submit(collector)
        assert f3 is None

        # 等 f1 跑完
        f1.result(timeout=5)
        assert collector.run_count == 1  # 只跑了一次

        # f1 跑完後，再提交應該被接受
        f4 = sched.submit(collector)
        assert f4 is not None
        f4.result(timeout=5)
        assert collector.run_count == 2
    finally:
        sched.shutdown(wait=True)


def test_skip_count_tracked():
    """skip 次數應該被計數在 get_status"""
    sched = CollectorScheduler(max_workers=2)
    try:
        collector = FakeCollector(name="skip_test", sleep_sec=0.5)
        sched.register(collector)

        sched.submit(collector)
        time.sleep(0.05)

        # skip 3 次
        assert sched.submit(collector) is None
        assert sched.submit(collector) is None
        assert sched.submit(collector) is None

        status = sched.get_status()
        assert status['collectors']['skip_test']['skip_count'] == 3
        assert status['collectors']['skip_test']['running'] is True
    finally:
        sched.shutdown(wait=True)


def test_different_collectors_dont_block_each_other():
    """不同名稱的 collector 即使各自在跑，也不會互相 skip"""
    sched = CollectorScheduler(max_workers=5)
    try:
        c1 = FakeCollector(name="a", sleep_sec=0.3)
        c2 = FakeCollector(name="b", sleep_sec=0.3)

        f1 = sched.submit(c1)
        f2 = sched.submit(c2)

        assert f1 is not None
        assert f2 is not None

        f1.result(timeout=5)
        f2.result(timeout=5)
        assert c1.run_count == 1
        assert c2.run_count == 1
    finally:
        sched.shutdown(wait=True)


# ============================================================
# Exception 隔離
# ============================================================

def test_exception_in_collector_does_not_crash_scheduler():
    """collector 拋 exception 不應汙染 scheduler 或影響其他任務"""
    sched = CollectorScheduler(max_workers=2)
    try:
        bad = FakeCollector(name="bad", raise_exc=ValueError("oops"))
        good = FakeCollector(name="good")

        f_bad = sched.submit(bad)
        f_good = sched.submit(good)

        # bad 的 future 應該能拿到（_safe_run 吞掉 exception 後回傳 None）
        result_bad = f_bad.result(timeout=5)
        assert result_bad is None  # _safe_run 捕捉後回傳 None

        # good 照樣成功
        result_good = f_good.result(timeout=5)
        assert result_good['ok'] is True

        # scheduler 本身還能繼續接新任務
        f_again = sched.submit(good)
        f_again.result(timeout=5)
        assert good.run_count == 2
    finally:
        sched.shutdown(wait=True)


# ============================================================
# 線程名稱
# ============================================================

def test_thread_name_contains_collector_name():
    """線程名稱應該是 collector-{name}，方便 log 追蹤"""
    sched = CollectorScheduler(max_workers=2)
    try:
        collector = FakeCollector(name="named_test")
        f = sched.submit(collector)
        f.result(timeout=5)

        assert len(collector.thread_names) == 1
        assert collector.thread_names[0] == "collector-named_test"
    finally:
        sched.shutdown(wait=True)


# ============================================================
# Timeout 觀察（不中斷，只記 warning）
# ============================================================

def test_timeout_exceeded_logs_warning(caplog):
    """執行時間超過 COLLECT_TIMEOUT 應記 warning，但不中斷"""
    sched = CollectorScheduler(max_workers=2)
    try:
        collector = FakeCollector(name="slow_timeout", sleep_sec=0.3)
        collector.COLLECT_TIMEOUT = 0.1  # 強制觸發超時警告

        with caplog.at_level(logging.WARNING, logger='scheduler'):
            f = sched.submit(collector)
            result = f.result(timeout=5)

        # 實際 run 不會被強制中斷（Python 無法做到）
        assert result == {'ok': True, 'run_count': 1}
        # 但 warning 應該被記錄
        assert any(
            '超過 COLLECT_TIMEOUT' in rec.message
            for rec in caplog.records
        ), f"沒看到超時警告，實際 log: {[r.message for r in caplog.records]}"
    finally:
        sched.shutdown(wait=True)


# ============================================================
# get_status
# ============================================================

def test_get_status_shows_registered_collectors():
    """get_status 應該列出所有已註冊的 collector"""
    sched = CollectorScheduler(max_workers=2)
    try:
        c1 = FakeCollector(name="s1")
        c2 = FakeCollector(name="s2")
        sched.register(c1)
        sched.register(c2)

        status = sched.get_status()
        assert status['max_workers'] == 2
        assert status['registered'] == 2
        assert 's1' in status['collectors']
        assert 's2' in status['collectors']
        assert status['collectors']['s1']['running'] is False
    finally:
        sched.shutdown(wait=True)


def test_get_status_reflects_running_state():
    """正在跑的 collector 應該 running=True"""
    sched = CollectorScheduler(max_workers=2)
    try:
        collector = FakeCollector(name="running_test", sleep_sec=0.5)
        f = sched.submit(collector)
        time.sleep(0.05)

        status = sched.get_status()
        assert status['collectors']['running_test']['running'] is True

        f.result(timeout=5)
        time.sleep(0.05)

        status = sched.get_status()
        assert status['collectors']['running_test']['running'] is False
    finally:
        sched.shutdown(wait=True)


# ============================================================
# Shutdown
# ============================================================

def test_shutdown_waits_for_inflight_tasks():
    """shutdown(wait=True) 應等待進行中的任務完成"""
    sched = CollectorScheduler(max_workers=2)
    collector = FakeCollector(name="shutdown_test", sleep_sec=0.3)
    sched.submit(collector)
    time.sleep(0.05)

    sched.shutdown(wait=True)
    # 等 wait 回來，collector 應該已經跑完
    assert collector.run_count == 1


# ============================================================
# Auto-register on first submit
# ============================================================

def test_submit_auto_registers_collector():
    """未先 register 就 submit，scheduler 應自動註冊"""
    sched = CollectorScheduler(max_workers=2)
    try:
        collector = FakeCollector(name="auto_reg")
        # 直接 submit，不呼叫 register
        f = sched.submit(collector)
        f.result(timeout=5)

        status = sched.get_status()
        assert 'auto_reg' in status['collectors']
    finally:
        sched.shutdown(wait=True)
