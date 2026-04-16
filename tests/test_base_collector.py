"""
BaseCollector 升級後的行為測試

驗證 Phase 1 加入的新機制：
1. COLLECT_TIMEOUT class attribute 有預設值且可被子類覆寫
2. GC_THRESHOLD_SEC 條件式觸發
3. last_success_at 只在成功時更新
4. Exception 會增加 error_count 但不往上拋
"""

import gc
from unittest.mock import MagicMock, patch

import pytest

from collectors.base import BaseCollector


class DummyCollector(BaseCollector):
    """最簡單的 collector，用於測試 base class 行為"""
    name = "dummy"
    interval_minutes = 1

    def __init__(self, collect_impl=None, data=None):
        # 繞過 storage / supabase 初始化（測試時我們 mock 掉）
        self.storage = MagicMock()
        self.storage.save.return_value = '/tmp/fake.json'
        self.supabase_writer = None
        self.last_run = None
        self.last_success_at = None
        self.run_count = 0
        self.error_count = 0
        self.consecutive_errors = 0
        self._collect_impl = collect_impl
        self._data = data if data is not None else [{'x': 1}]

    def collect(self):
        if self._collect_impl is not None:
            return self._collect_impl()
        return {'total': 1, 'data': self._data}


# ============================================================
# COLLECT_TIMEOUT
# ============================================================

def test_default_collect_timeout():
    """BaseCollector 應該有預設 COLLECT_TIMEOUT"""
    assert hasattr(BaseCollector, 'COLLECT_TIMEOUT')
    assert BaseCollector.COLLECT_TIMEOUT == 300


def test_subclass_can_override_collect_timeout():
    """子類別應該可以覆寫 COLLECT_TIMEOUT"""

    class FastCollector(DummyCollector):
        COLLECT_TIMEOUT = 10

    class SlowCollector(DummyCollector):
        COLLECT_TIMEOUT = 600

    assert FastCollector.COLLECT_TIMEOUT == 10
    assert SlowCollector.COLLECT_TIMEOUT == 600
    # 父類不受影響
    assert BaseCollector.COLLECT_TIMEOUT == 300


# ============================================================
# last_success_at
# ============================================================

@patch('collectors.base.notify_success')
def test_last_success_at_set_on_success(_mock_notify):
    """成功執行後 last_success_at 應被設定"""
    collector = DummyCollector()
    assert collector.last_success_at is None

    stats = collector.run()

    assert collector.last_success_at is not None
    assert collector.last_run == collector.last_success_at
    assert 'error' not in stats


@patch('collectors.base.notify_error')
def test_last_success_at_not_updated_on_failure(_mock_notify):
    """collect() 拋 exception 時，last_success_at 不應更新"""

    def failing():
        raise ValueError("boom")

    collector = DummyCollector(collect_impl=failing)
    assert collector.last_success_at is None

    stats = collector.run()

    assert collector.last_success_at is None  # 仍為 None
    assert collector.last_run is None  # 失敗時 last_run 也不更新（保留舊行為）
    assert collector.error_count == 1
    assert collector.consecutive_errors == 1
    assert 'error' in stats
    assert 'boom' in stats['error']


@patch('collectors.base.notify_success')
@patch('collectors.base.notify_error')
def test_consecutive_errors_reset_on_success(_mock_err, _mock_ok):
    """連續錯誤在成功後應重置為 0"""
    state = {'should_fail': True}

    def maybe_fail():
        if state['should_fail']:
            raise RuntimeError("fail")
        return {'total': 1, 'data': [{'x': 1}]}

    collector = DummyCollector(collect_impl=maybe_fail)

    # 連續 2 次失敗
    collector.run()
    collector.run()
    assert collector.consecutive_errors == 2

    # 第 3 次成功
    state['should_fail'] = False
    collector.run()
    assert collector.consecutive_errors == 0
    assert collector.last_success_at is not None


# ============================================================
# GC_THRESHOLD_SEC 條件式 GC
# ============================================================

@patch('collectors.base.gc.collect')
@patch('collectors.base.notify_success')
def test_gc_not_triggered_for_short_runs(_mock_notify, mock_gc):
    """短任務（< GC_THRESHOLD_SEC）不應觸發 gc.collect()"""
    collector = DummyCollector()
    # 預設 GC_THRESHOLD_SEC=30，dummy collect 瞬間完成
    collector.run()
    mock_gc.assert_not_called()


@patch('collectors.base.gc.collect')
@patch('collectors.base.notify_success')
def test_gc_triggered_for_long_runs(_mock_notify, mock_gc):
    """長任務（>= GC_THRESHOLD_SEC）應觸發 gc.collect()"""

    class LongCollector(DummyCollector):
        GC_THRESHOLD_SEC = 0  # 讓任何任務都觸發

    collector = LongCollector()
    collector.run()
    mock_gc.assert_called_once()


# ============================================================
# get_status 包含 last_success_at
# ============================================================

@patch('collectors.base.notify_success')
def test_get_status_includes_last_success_at(_mock_notify):
    """get_status 應該包含 last_success_at 欄位"""
    collector = DummyCollector()
    status = collector.get_status()

    assert 'last_success_at' in status
    assert status['last_success_at'] is None

    collector.run()
    status = collector.get_status()
    assert status['last_success_at'] is not None
