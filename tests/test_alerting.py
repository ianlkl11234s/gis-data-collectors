"""告警分級狀態機（utils/alerting.py）

背景：2026-11-08 盤點，Telegram 每天 15–60 則、多半是「單次瞬斷」「全綠也送」。
新規則：🔴 開始一次、恢復一次；🟡 只進日報；瞬斷不送；同時壞 ≥5 支合成一則。
"""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import config
from collectors.base import BaseCollector
from utils import alerting

TPE = timezone(timedelta(hours=8))
T0 = datetime(2026, 11, 8, 10, 0, tzinfo=TPE)


@pytest.fixture
def sent(monkeypatch):
    out = []
    monkeypatch.setattr(alerting, "_sender", lambda text: out.append(text) or True)
    monkeypatch.setattr(config, "CONSECUTIVE_ERROR_THRESHOLD", 3)
    monkeypatch.setattr(config, "ALERT_MIN_DURATION_MIN", 15)
    monkeypatch.setattr(config, "ALERT_DAILY_INTERVAL_MIN", 360)
    monkeypatch.setattr(config, "ALERT_DAILY_N", 2)
    monkeypatch.setattr(config, "ALERT_STILL_DOWN_HOURS", 24)
    monkeypatch.setattr(config, "ALERT_BURST_MERGE", 5)
    monkeypatch.setattr(config, "ALERT_BURST_WINDOW_SEC", 300)
    monkeypatch.setattr(config, "ALERT_BURST_HOLD_MIN", 60)
    monkeypatch.setattr(config, "ALERT_OVERRIDES", "news_events:6:60")
    return out


def critical(*names):
    alerting._set_critical_cache({n: True for n in names})


def fail_until(name, interval, start, n, step_min=None):
    """每 step_min 分鐘失敗一次，共 n 次；回傳最後一次的時間。"""
    step = timedelta(minutes=step_min or interval)
    t = start
    for i in range(n):
        t = start + step * i
        alerting.collector_failed(name, "HTTP 401 金鑰被拒", interval_minutes=interval, now=t)
    return t


# ── 開始／持續不重送／恢復 ───────────────────────────────

def test_fast_collector_needs_n_and_15_minutes(sent):
    critical("bus")
    # 2 分鐘級：第 3 次（4 分鐘）還不到 15 分鐘 → 仍是 failing
    fail_until("bus", 2, T0, 3)
    assert alerting.load_state()["entries"]["collector:bus"]["state"] == "failing"
    # 第 9 次（16 分鐘）才進 alerting
    t = fail_until("bus", 2, T0 + timedelta(minutes=6), 6)
    assert alerting.load_state()["entries"]["collector:bus"]["state"] == "alerting"
    # 窗內不送，窗滿才送一則
    assert alerting.flush(now=t) == []
    msgs = alerting.flush(now=t + timedelta(seconds=300))
    assert len(msgs) == 1 and msgs[0].startswith("🔴") and "bus" in msgs[0]


def test_ongoing_failure_does_not_resend_and_recovery_once(sent):
    critical("bus")
    t = fail_until("bus", 2, T0, 10)
    alerting.flush(now=t + timedelta(minutes=5))
    t = fail_until("bus", 2, t + timedelta(minutes=6), 30)
    alerting.flush(now=t + timedelta(minutes=10))
    assert len(sent) == 1
    alerting.collector_succeeded("bus", now=t + timedelta(minutes=12))
    alerting.flush(now=t + timedelta(minutes=20))
    assert len(sent) == 2 and sent[1].startswith("✅") and "bus" in sent[1]
    assert "collector:bus" not in alerting.load_state()["entries"]


def test_simulated_1_to_20_failures_then_success_sends_exactly_two(sent):
    """驗收：一支 collector 依序失敗 1..20 次再成功 → 只出 1 則 🔴＋1 則恢復。"""
    critical("youbike")
    t = T0
    for i in range(20):
        t = T0 + timedelta(minutes=10 * i)
        alerting.collector_failed("youbike", "timeout", interval_minutes=10, now=t)
        alerting.flush(now=t)
    t += timedelta(minutes=10)
    alerting.collector_succeeded("youbike", now=t)
    for k in range(1, 8):
        alerting.flush(now=t + timedelta(minutes=k))
    assert [m[0] for m in sent] == ["🔴", "✅"]


def test_transient_errors_below_threshold_send_nothing(sent):
    critical("bus")
    fail_until("bus", 2, T0, 2)
    alerting.collector_succeeded("bus", now=T0 + timedelta(minutes=5))
    alerting.flush(now=T0 + timedelta(hours=1))
    assert sent == []
    assert alerting.load_state()["entries"] == {}
    # 但仍計入當日小錯誤（隔天日報看得到）
    snap = alerting.snapshot(now=T0 + timedelta(days=1))
    assert snap["errors_yesterday"] == {"bus": 2}
    assert snap["error_total_yesterday"] == 2


def test_recovery_inside_window_cancels_start(sent):
    """進 alerting 後窗內就恢復 → 開始與恢復都不送。"""
    critical("bus")
    t = fail_until("bus", 2, T0, 9)
    alerting.collector_succeeded("bus", now=t + timedelta(minutes=2))
    alerting.flush(now=t + timedelta(minutes=30))
    assert sent == []


# ── 每日類／非 critical／放寬門檻 ────────────────────────

def test_daily_collector_alerts_at_n2_without_time_condition(sent):
    critical("weather_daily")
    alerting.collector_failed("weather_daily", "x", interval_minutes=1440, now=T0)
    assert alerting.load_state()["entries"]["collector:weather_daily"]["state"] == "failing"
    alerting.collector_failed("weather_daily", "x", interval_minutes=1440, now=T0 + timedelta(minutes=1))
    assert alerting.load_state()["entries"]["collector:weather_daily"]["state"] == "alerting"


def test_threshold_for_rules(sent):
    assert alerting.threshold_for("a", 1) == (3, 15)
    assert alerting.threshold_for("a", 359) == (3, 15)
    assert alerting.threshold_for("a", 360) == (2, 0)
    assert alerting.threshold_for("news_events", 10) == (6, 60)


def test_flaky_upstream_override_is_wider(sent):
    critical("news_events")
    fail_until("news_events", 10, T0, 5)   # 5 次、40 分：預設規則早就告警，放寬後還沒
    assert alerting.load_state()["entries"]["collector:news_events"]["state"] == "failing"
    fail_until("news_events", 10, T0 + timedelta(minutes=50), 2)  # 第 7 次、60 分
    assert alerting.load_state()["entries"]["collector:news_events"]["state"] == "alerting"


def test_non_critical_is_yellow_no_message_but_in_daily(sent):
    critical()  # 名單空 → 全部 🟡
    t = fail_until("parking", 15, T0, 4)
    alerting.flush(now=t + timedelta(hours=1))
    assert sent == []
    snap = alerting.snapshot(now=t + timedelta(hours=1))
    assert [a["name"] for a in snap["active"]] == ["parking"]
    assert snap["active"][0]["level"] == alerting.YELLOW
    alerting.collector_succeeded("parking", now=t + timedelta(hours=2))
    alerting.flush(now=t + timedelta(hours=3))
    assert sent == []  # 🟡 的恢復不送
    snap = alerting.snapshot(now=t + timedelta(hours=3))
    assert snap["active"] == [] and snap["resolved"][0]["name"] == "parking"


# ── 24 小時仍未恢復 ─────────────────────────────────────

def test_still_down_after_24h_sent_once(sent):
    critical("tra_train")
    t = fail_until("tra_train", 2, T0, 10)
    alerting.flush(now=t + timedelta(minutes=5))
    later = T0 + timedelta(hours=25)
    for k in range(5):
        alerting.collector_failed("tra_train", "x", interval_minutes=2, now=later + timedelta(minutes=2 * k))
    alerting.flush(now=later + timedelta(minutes=30))
    alerting.flush(now=later + timedelta(hours=10))
    assert len(sent) == 2 and sent[1].startswith("⏳") and "tra_train" in sent[1]


# ── 同時壞 ≥5 支合成一則 ────────────────────────────────

def test_burst_of_five_merges_into_one_and_recovers_once(sent):
    names = ["bus", "tra_train", "youbike", "freeway_vd", "road_congestion", "parking"]
    critical("bus", "tra_train", "youbike", "freeway_vd")
    for name in names:
        fail_until(name, 2, T0, 9)  # 16 分鐘都進 alerting
    t = T0 + timedelta(minutes=16)
    alerting.flush(now=t + timedelta(minutes=5))
    assert len(sent) == 1
    assert "6 支收集器同時失敗" in sent[0] and "HTTP 401" in sent[0]
    # hold 期間再壞的（例：15 分鐘級的 weather）併入，不另送
    critical("bus", "tra_train", "youbike", "freeway_vd", "weather")
    fail_until("weather", 15, T0, 3, step_min=15)
    alerting.flush(now=T0 + timedelta(minutes=40))
    assert len(sent) == 1
    # 全部恢復 → 一則
    rt = T0 + timedelta(minutes=90)
    for name in names + ["weather"]:
        alerting.collector_succeeded(name, now=rt)
    alerting.flush(now=rt + timedelta(minutes=6))
    assert len(sent) == 2 and sent[1].startswith("✅") and "7 支收集器已恢復" in sent[1]


def test_four_simultaneous_reds_send_one_combined_message(sent):
    critical("a", "b", "c", "d")
    for n in "abcd":
        fail_until(n, 2, T0, 9)
    alerting.flush(now=T0 + timedelta(minutes=30))
    assert len(sent) == 1 and "4 支收集器連續失敗" in sent[0]


# ── 收集失敗與 DB 寫入共用一個 key ──────────────────────

def test_db_and_collect_reasons_share_one_key(sent):
    critical("youbike")
    for i in range(3):
        t = T0 + timedelta(minutes=10 * i)
        alerting.collector_failed("youbike", "DB 寫入失敗", reason="db", interval_minutes=10, now=t)
        # 收集本身成功（base.run 走成功路徑）不能把 DB 的失敗清掉
        alerting.collector_succeeded("youbike", now=t)
    assert alerting.load_state()["entries"]["collector:youbike"]["state"] == "alerting"
    alerting.flush(now=T0 + timedelta(minutes=30))
    assert len(sent) == 1 and "DB 寫入" in sent[0]
    alerting.collector_succeeded("youbike", reason="db", now=T0 + timedelta(minutes=40))
    alerting.flush(now=T0 + timedelta(minutes=50))
    assert len(sent) == 2 and sent[1].startswith("✅")


# ── 非 collector：raise／clear／event ─────────────────

def test_generic_red_sends_immediately_once_and_recovers(sent):
    assert alerting.raise_alert("backup", alerting.RED, "Supabase 每日備份失敗", now=T0) == "alerting"
    alerting.raise_alert("backup", alerting.RED, "Supabase 每日備份失敗", now=T0 + timedelta(hours=1))
    assert len(sent) == 1 and sent[0].startswith("🔴")
    alerting.clear_alert("backup", "Supabase 每日備份", now=T0 + timedelta(hours=3))
    assert len(sent) == 2 and sent[1].startswith("✅") and "3 小時" in sent[1]


def test_generic_yellow_silent_then_escalation_sends(sent):
    alerting.raise_alert("disk", alerting.YELLOW, "本機磁碟警告", now=T0)
    assert sent == []
    alerting.raise_alert("disk", alerting.RED, "本機磁碟嚴重", now=T0 + timedelta(minutes=30))
    assert len(sent) == 1
    alerting.clear_alert("disk", "本機磁碟", now=T0 + timedelta(hours=2))
    assert len(sent) == 2


def test_yellow_clear_sends_nothing(sent):
    alerting.raise_alert("pla_tracks_plate", alerting.YELLOW, "版型不認得", now=T0)
    alerting.clear_alert("pla_tracks_plate", now=T0 + timedelta(days=1))
    assert sent == []


def test_event_red_sends_yellow_only_recorded(sent):
    alerting.event("watchdog", alerting.YELLOW, "Watchdog 重啟進程", now=T0)
    assert sent == []
    alerting.event("lightning_upstream", alerting.RED, "台電落雷恢復", now=T0)
    assert len(sent) == 1
    assert len(alerting.recent_events("watchdog", T0 - timedelta(minutes=1))) == 1


# ── 季節性／事件驅動：沒資料不是錯誤 ────────────────────

class _EmptyCollector(BaseCollector):
    name = "env_jp_wbgt"
    interval_minutes = 60

    def __init__(self, result):
        self.storage = MagicMock()
        self.storage.save.return_value = "/tmp/fake.json"
        self.supabase_writer = None
        self.last_run = self.last_success_at = None
        self.run_count = self.error_count = self.consecutive_errors = 0
        self._result = result

    def collect(self):
        return self._result


@pytest.mark.parametrize("result", [
    {"no_change": True, "off_season": True},   # WBGT 季外
    {"data": [], "total": 0},                   # 颱風季外／無震／無雷
])
def test_seasonal_or_event_driven_empty_result_never_alerts(sent, result):
    critical("env_jp_wbgt")
    c = _EmptyCollector(result)
    for _ in range(50):
        c.run()
    alerting.flush(now=datetime.now(TPE) + timedelta(hours=1))
    assert sent == []
    assert alerting.load_state()["entries"] == {}


# ── 狀態檔 ─────────────────────────────────────────────

def test_state_written_atomically(sent, _isolate_alert_state):
    alerting.collector_failed("bus", "x", interval_minutes=2, now=T0)
    path = _isolate_alert_state
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "collector:bus" in data["entries"]
    assert [p.name for p in path.parent.iterdir()] == ["alert_state.json"]


def test_daily_buckets_pruned_to_three_days(sent):
    for d in range(6):
        alerting.collector_failed("bus", "x", interval_minutes=2, now=T0 + timedelta(days=d))
    assert len(alerting.load_state()["daily"]) == 3
