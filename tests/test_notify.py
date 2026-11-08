"""
notify.py 薄介面測試

背景：2026-08-30 事故 `notify_error` 每次失敗都送 Telegram（15 小時 460+ 則）；
2026-11 改成全部交給 utils/alerting.py 分級（單次瞬斷不送、達門檻送一次、恢復送一次）。
這裡只驗 notify.py 正確轉交，規則本身在 test_alerting.py。
"""

from datetime import datetime, timedelta, timezone

import pytest

from utils import alerting, notify

TPE = timezone(timedelta(hours=8))


@pytest.fixture
def sent(monkeypatch):
    out = []
    monkeypatch.setattr(alerting, "_sender", lambda text: out.append(text) or True)
    return out


def test_single_error_sends_nothing(sent):
    notify.notify_error("dummy", "boom", 1, interval_minutes=5)
    assert sent == []
    assert alerting.load_state()["entries"]["collector:dummy"]["state"] == "failing"


def test_success_clears_failing_entry(sent):
    notify.notify_error("dummy", "boom", 1, interval_minutes=5)
    notify.notify_success("dummy", {"total": 1})
    assert alerting.load_state()["entries"] == {}
    assert sent == []


def test_webhook_and_line_paths_removed():
    for name in ("send_webhook", "send_line_notify", "notify_recovery", "_should_alert",
                 "notify_disk_alert", "notify_silence_alert"):
        assert not hasattr(notify, name), name


def test_archive_success_silent_failure_red_then_recovery(sent):
    notify.notify_archive_complete({"archive": {"uploaded": 312, "skipped": 0, "failed": 0}})
    assert sent == []
    notify.notify_archive_complete({"archive": {"uploaded": 300, "skipped": 0, "failed": 2}})
    notify.notify_archive_complete({"archive": {"uploaded": 300, "skipped": 0, "failed": 2}})
    assert len(sent) == 1 and sent[0].startswith("🔴") and "2 個失敗" in sent[0]
    notify.notify_archive_complete({"archive": {"uploaded": 312, "skipped": 0, "failed": 0}})
    assert len(sent) == 2 and sent[1].startswith("✅")
    assert alerting.load_state()["routines"]["archive"]["ok"] is True


def test_trails_export_success_silent_failure_red(sent):
    notify.notify_trails_export({"ok": 4, "failed": 0, "rows": 1, "bytes": 1, "dates": ["2026-11-07"]})
    assert sent == []
    notify.notify_trails_export({"ok": 3, "failed": 1, "failures": ["bus 2026-11-07: timeout"]})
    assert len(sent) == 1 and "軌跡凍結匯出失敗" in sent[0] and "bus 2026-11-07" in sent[0]
