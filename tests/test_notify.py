"""
notify.py 連續錯誤告警收斂測試

背景：2026-08-30 事故，`notify_error` 用 `>= threshold` 判斷是否發送
「🚨 收集器連續錯誤告警」，導致 15 小時內狂發 460+ 則重複告警。

測試項目：
1. `_should_alert` 指數收斂邏輯（純函式，不依賴 config/網路）
2. `notify_error` 在達到收斂檢查點時才真的呼叫 send_telegram
3. `notify_recovery` 訊息格式（instance tag、中斷次數、collector 名稱）
"""

from unittest.mock import patch

import pytest

from utils.notify import _should_alert, notify_error, notify_recovery


# ============================================================
# _should_alert：指數收斂純函式
# ============================================================

@pytest.mark.parametrize("count", [3, 6, 12, 24, 48, 96, 192, 384])
def test_should_alert_fires_at_threshold_3_checkpoints(count):
    assert _should_alert(count, threshold=3) is True


@pytest.mark.parametrize("count", [1, 2, 4, 5, 7, 9, 100, 463])
def test_should_alert_silent_at_threshold_3_non_checkpoints(count):
    """4/5/7/100/463 非 3 的倍數；9 雖是 3 的倍數但商數 3 不是 2 的冪，都不該發。"""
    assert _should_alert(count, threshold=3) is False


@pytest.mark.parametrize("count", [5, 10, 20])
def test_should_alert_fires_at_threshold_5_checkpoints(count):
    assert _should_alert(count, threshold=5) is True


@pytest.mark.parametrize("count", [1, 4, 6, 15])
def test_should_alert_silent_at_threshold_5_non_checkpoints(count):
    """15 雖是 5 的倍數但商數 3 不是 2 的冪，不該發。"""
    assert _should_alert(count, threshold=5) is False


def test_should_alert_false_below_threshold():
    assert _should_alert(2, threshold=3) is False
    assert _should_alert(0, threshold=3) is False


def test_should_alert_false_for_non_positive_threshold():
    """threshold <= 0 是防呆，不應丟例外"""
    assert _should_alert(5, threshold=0) is False
    assert _should_alert(5, threshold=-1) is False


# ============================================================
# notify_error：只在收斂檢查點才真的送 Telegram 連續錯誤告警
# ============================================================

@patch('utils.notify.send_telegram')
@patch('utils.notify.send_line_notify')
@patch('utils.notify.send_webhook')
def test_notify_error_sends_escalated_alert_at_checkpoint(_mock_wh, _mock_line, mock_tg, monkeypatch):
    import config
    monkeypatch.setattr(config, 'CONSECUTIVE_ERROR_THRESHOLD', 3)

    notify_error('dummy', 'boom', consecutive_errors=6)

    mock_tg.assert_called_once()
    assert '收集器連續錯誤告警' in mock_tg.call_args.args[0]


@patch('utils.notify.send_telegram')
@patch('utils.notify.send_line_notify')
@patch('utils.notify.send_webhook')
def test_notify_error_silent_between_checkpoints(_mock_wh, _mock_line, mock_tg, monkeypatch):
    import config
    monkeypatch.setattr(config, 'CONSECUTIVE_ERROR_THRESHOLD', 3)

    notify_error('dummy', 'boom', consecutive_errors=7)

    mock_tg.assert_not_called()


@patch('utils.notify.send_telegram')
@patch('utils.notify.send_line_notify')
@patch('utils.notify.send_webhook')
def test_notify_error_sends_plain_alert_below_threshold(_mock_wh, _mock_line, mock_tg, monkeypatch):
    import config
    monkeypatch.setattr(config, 'CONSECUTIVE_ERROR_THRESHOLD', 3)

    notify_error('dummy', 'boom', consecutive_errors=1)

    mock_tg.assert_called_once()
    assert '收集器錯誤' in mock_tg.call_args.args[0]
    assert '連續錯誤告警' not in mock_tg.call_args.args[0]


# ============================================================
# notify_recovery：訊息格式
# ============================================================

@patch('utils.notify.send_telegram')
def test_notify_recovery_message_format(mock_tg):
    notify_recovery('dummy', 7)

    mock_tg.assert_called_once()
    msg = mock_tg.call_args.args[0]
    assert '收集器恢復' in msg
    assert 'dummy' in msg
    assert '7' in msg
