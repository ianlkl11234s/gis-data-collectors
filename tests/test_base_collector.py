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

import config
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


@patch('collectors.base.notify_success')
def test_raw_payload_is_saved_but_redacted_from_stats_and_success_notice(mock_notify):
    """完整 raw 供 archive 保存，但不可進入回傳統計或成功通知。"""
    raw_payload = [{'private_name': 'must stay in storage only'}]
    collector = DummyCollector(data=[{'normalized': True}])
    collector.collect = lambda: {'data': collector._data, 'raw_payload': raw_payload, 'row_count': 1}

    stats = collector.run()

    saved_result = collector.storage.save.call_args.args[1]
    assert saved_result['raw_payload'] is raw_payload
    assert 'raw_payload' not in stats
    notified_stats = mock_notify.call_args.args[1]
    assert 'raw_payload' not in notified_stats


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


@patch('collectors.base.notify_error')
@patch('collectors.base.notify_success')
def test_collector_error_signal_is_persisted_before_existing_error_path(_mock_ok, _mock_err):
    """完整快照可先保存 failed ledger，再以既有錯誤統計回報失敗。"""
    collector = DummyCollector(
        collect_impl=lambda: {
            'data': [],
            'run_id': 'failed-run',
            '_collector_error': 'incomplete snapshot',
        }
    )
    collector.supabase_writer = MagicMock()

    stats = collector.run()

    collector.storage.save.assert_called_once()
    collector.supabase_writer.write.assert_called_once()
    _mock_ok.assert_not_called()
    _mock_err.assert_called_once()
    assert stats['error'] == 'incomplete snapshot'
    assert collector.last_success_at is None


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
# 告警：失敗／成功交給 utils.alerting（恢復通知由狀態機決定，base 不再自己判斷）
# ============================================================

@patch('collectors.base.notify_success')
@patch('collectors.base.notify_error')
def test_failure_reports_interval_to_alerting(mock_err, _mock_ok):
    def boom():
        raise RuntimeError("fail")

    collector = DummyCollector(collect_impl=boom)
    collector.run()
    mock_err.assert_called_once_with('dummy', 'fail', 1, interval_minutes=1)


def test_transient_failures_then_success_leave_no_alert_state():
    """未達門檻就恢復：alert_state 沒有殘留、什麼都不送（舊版會送 2 則「收集器錯誤」）。"""
    from utils import alerting

    sent = []
    alerting.set_sender(lambda text: sent.append(text) or True)
    try:
        state = {'should_fail': True}

        def maybe_fail():
            if state['should_fail']:
                raise RuntimeError("fail")
            return {'total': 1, 'data': [{'x': 1}]}

        collector = DummyCollector(collect_impl=maybe_fail)
        collector.run()
        collector.run()
        assert alerting.load_state()['entries']['collector:dummy']['reasons']['collect']['count'] == 2
        state['should_fail'] = False
        collector.run()
        assert alerting.load_state()['entries'] == {}
        assert collector.consecutive_errors == 0
        assert sent == []
    finally:
        alerting.set_sender(None)


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


@patch('collectors.base.notify_error')
@patch('collectors.base.notify_success')
def test_mojibake_rejection_fails_run_even_without_require_db_write(_ok, _err):
    from storage.supabase_writer import MojibakeWriteRejected
    collector = DummyCollector()
    collector.supabase_writer = MagicMock()
    collector.supabase_writer.write.side_effect = MojibakeWriteRejected('rejected')
    stats = collector.run()
    _ok.assert_not_called()
    _err.assert_called_once()
    assert collector.last_success_at is None


# ============================================================
# collector_status 心跳：collect() 失敗也要回報
# ============================================================

@patch('collectors.base.notify_error')
def test_collect_failure_reports_error_heartbeat(_mock_notify):
    """collect() 本身失敗時 writer 不會被呼叫，base 要自己回報失敗心跳"""
    def boom():
        raise RuntimeError("LLM annotation batch failed")

    collector = DummyCollector(collect_impl=boom)
    collector.supabase_writer = MagicMock()

    collector.run()

    collector.supabase_writer._report_heartbeat.assert_called_once_with(
        'dummy', False, 0, 'LLM annotation batch failed'
    )
    collector.supabase_writer.write.assert_not_called()


@patch('collectors.base.notify_error')
@patch('collectors.base.notify_success')
def test_write_path_failure_does_not_double_report(_mock_success, _mock_error):
    """write() 已自己回報心跳（成功或失敗），base 不再重複回報"""
    class StrictCollector(DummyCollector):
        def require_db_write(self):
            return True

    collector = StrictCollector()
    collector.supabase_writer = MagicMock()
    collector.supabase_writer.write.return_value = False

    collector.run()

    collector.supabase_writer.write.assert_called_once()
    collector.supabase_writer._report_heartbeat.assert_not_called()
