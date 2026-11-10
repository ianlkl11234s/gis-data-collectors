"""收集器「太久沒新資料」即時告警（tasks/staleness_watch.py＋alerting.update_staleness）

使用者決定（2026-11-09）：失敗本身不發；資料斷了超過門檻才發 🔴，恢復發 ✅。
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import config
from tasks import monitoring
from tasks.staleness_watch import StalenessWatch, evaluate, plan_collector
from utils import alerting

TPE = timezone(timedelta(hours=8))
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=TPE)


@pytest.fixture
def sent(monkeypatch):
    out = []
    monkeypatch.setattr(alerting, "_sender", lambda text: out.append(text) or True)
    monkeypatch.setattr(config, "ALERT_STILL_DOWN_HOURS", 24)
    monkeypatch.setattr(config, "INSTANCE_NAME", "")
    return out


# ── 門檻 ───────────────────────────────────────────────

@pytest.mark.parametrize("interval,expected", [
    (1, 30), (2, 30), (5, 30), (10, 30), (15, 45), (60, 180), (360, 1080), (1439, 4317),
    (1440, 2880), (10080, 20160),
])
def test_threshold_levels(interval, expected):
    assert alerting.staleness_threshold_min(interval) == expected


def test_threshold_zero_interval_is_floor():
    assert alerting.staleness_threshold_min(0) == 30


# ── plan：哪些表、用什麼門檻、什麼時候不檢查 ─────────────────

def T(table, owner, interval, **kw):
    return {"schema": "live", "table": table, "owner_collector": owner,
            "expected_interval_min": interval, **kw}


CMAP = {
    "youbike": {"enabled": True, "deployment": "pulse_host",
                "supabase_tables": ["live.youbike_snapshots", "live.youbike_current"]},
    "earthquake": {"enabled": True, "deployment": "pulse_host"},
    "wbgt": {"enabled": True, "deployment": "pulse_host"},
    "old": {"enabled": False, "deployment": "disabled"},
    "marine": {"enabled": True, "deployment": "pulse_host"},
}
TABLES = [
    T("youbike_snapshots", "youbike", 15), T("youbike_current", "youbike", 15),
    T("earthquakes", "earthquake", 60, event_driven=True),
    T("wbgt_obs", "wbgt", 60, active_window={"from": "04-20", "to": "10-31"}),
    T("old_table", "old", 5),
    T("marine_readings", "marine", 60), T("marine_stations", "marine", 1440),
]


def test_plan_regular_collector_uses_its_tables():
    p = plan_collector("youbike", 15, TABLES, CMAP, NOW)
    assert p["tables"] == [("live", "youbike_snapshots"), ("live", "youbike_current")]
    assert p["threshold_min"] == 45


def test_plan_uses_slower_of_run_interval_and_table_interval():
    # 每 10 分鐘跑、但表平常每小時才有新資料 → 用 60 分鐘算門檻（3 小時）
    assert plan_collector("marine", 10, TABLES, CMAP, NOW)["threshold_min"] == 180


def test_plan_event_driven_falls_back_to_last_success():
    p = plan_collector("earthquake", 1, TABLES, CMAP, NOW)
    assert p["tables"] == [] and p["threshold_min"] == 30


def test_plan_off_season_and_disabled_not_checked():
    assert plan_collector("wbgt", 60, TABLES, CMAP, NOW) is None          # 11 月不在季內
    assert plan_collector("wbgt", 60, TABLES, CMAP, NOW.replace(month=7)) is not None
    assert plan_collector("old", 5, TABLES, CMAP, NOW) is None


def test_plan_no_registered_table_uses_last_success():
    p = plan_collector("s3_only", 1440, TABLES, CMAP, NOW)
    assert p == {"name": "s3_only", "tables": [], "threshold_min": 2880}


# ── evaluate（純記憶體） ─────────────────────────────────

DAY_AGO = NOW - timedelta(days=1)


def test_write_basis_stale_beyond_threshold():
    p = plan_collector("youbike", 15, TABLES, CMAP, NOW)
    f = evaluate(p, NOW - timedelta(minutes=46), NOW, DAY_AGO, NOW)  # 有在跑、但 46 分沒寫入
    assert f["stale"] and f["basis"] == "write"
    assert evaluate(p, NOW - timedelta(minutes=44), None, DAY_AGO, NOW)["stale"] is False


def test_event_driven_uses_last_success():
    p = plan_collector("earthquake", 5, TABLES, CMAP, NOW)
    # 事件驅動：就算很久沒寫入，只要有成功執行就正常
    assert evaluate(p, NOW - timedelta(days=9), NOW - timedelta(minutes=20), DAY_AGO, NOW)["stale"] is False
    f = evaluate(p, None, NOW - timedelta(minutes=31), DAY_AGO, NOW)
    assert f["stale"] and f["basis"] == "success"


def test_no_record_waits_from_process_start():
    p = plan_collector("earthquake", 5, TABLES, CMAP, NOW)
    assert evaluate(p, None, None, NOW - timedelta(minutes=10), NOW)["stale"] is False  # 剛重啟
    assert evaluate(p, None, None, NOW - timedelta(hours=2), NOW)["stale"] is True


def test_never_wrote_falls_back_to_last_success():
    """有登記表、但從沒經 writer 記到寫入（自己直連 DB 的收集器）→ 看最後成功執行。"""
    p = plan_collector("youbike", 15, TABLES, CMAP, NOW)
    f = evaluate(p, None, NOW - timedelta(minutes=5), DAY_AGO, NOW)
    assert f["basis"] == "success" and f["stale"] is False


# ── update_staleness：開始、恢復、去重、合併、24h ──────────────

def F(name, stale, last_min=60, threshold=30, basis="write"):
    return {"name": name, "stale": stale, "basis": basis, "threshold_min": threshold,
            "last_at": NOW - timedelta(minutes=last_min)}


def test_start_once_then_no_resend_then_recovery_once(sent):
    alerting.update_staleness([F("bus", True)], now=NOW)
    assert len(sent) == 1 and sent[0].startswith("🔴 收集器疑似壞掉：bus")
    assert "已 1 小時沒有新資料（門檻 30 分" in sent[0]
    for k in range(1, 6):
        alerting.update_staleness([F("bus", True, 60 + 15 * k)], now=NOW + timedelta(minutes=15 * k))
    assert len(sent) == 1
    alerting.update_staleness([F("bus", False, 1)], now=NOW + timedelta(hours=2))
    assert len(sent) == 2 and sent[1].startswith("✅ bus 恢復，又有新資料（中斷約 3 小時）")
    assert "stale:bus" not in alerting.load_state()["entries"]


def test_ok_collector_sends_nothing(sent):
    alerting.update_staleness([F("bus", False, 1), F("youbike", False, 5)], now=NOW)
    assert sent == [] and alerting.load_state()["entries"] == {}


def test_many_stale_merged_into_one_message_with_labels(sent):
    alerting._set_label_cache({"bus": "雙北公車位置", "youbike": "YouBike 站點"})
    alerting.update_staleness([F("bus", True), F("youbike", True, 120, 45), F("tra_train", False, 1)],
                              now=NOW)
    assert len(sent) == 1
    assert sent[0].startswith("🔴 2 支收集器疑似壞掉")
    assert "• 雙北公車位置（bus）：" in sent[0] and "• YouBike 站點（youbike）：" in sent[0]
    alerting.update_staleness([F("bus", False, 1), F("youbike", False, 1)], now=NOW + timedelta(minutes=30))
    assert len(sent) == 2 and sent[1].startswith("✅ 2 支收集器恢復")


def test_still_down_after_24h_once_and_merged(sent):
    alerting.update_staleness([F("a", True), F("b", True)], now=NOW)
    later = NOW + timedelta(hours=24)
    alerting.update_staleness([F("a", True), F("b", True)], now=later)
    alerting.update_staleness([F("a", True), F("b", True)], now=later + timedelta(hours=5))
    assert len(sent) == 2 and sent[1].startswith("⏳ 通報後已超過") and "a、b" in sent[1]


def test_daily_collector_still_down_counts_from_alert_time(sent):
    """每日類：通報時最後一筆早已超過 24 小時 → 不能 15 分鐘後就送 ⏳，要通報後滿 24 小時。"""
    f = lambda now_: {"name": "daily", "stale": True, "basis": "write", "threshold_min": 2880,
                      "last_at": NOW - timedelta(days=2, minutes=1)}
    alerting.update_staleness([f(NOW)], now=NOW)
    alerting.update_staleness([f(NOW)], now=NOW + timedelta(minutes=15))
    assert len(sent) == 1 and sent[0].startswith("🔴")
    alerting.update_staleness([f(NOW)], now=NOW + timedelta(hours=24))
    assert len(sent) == 2 and sent[1].startswith("⏳ 通報後已超過 24 小時仍未恢復")


def test_generic_red_still_down_after_24h_once(sent):
    alerting.raise_alert("backup", alerting.RED, "Supabase 每日備份失敗", now=NOW)
    alerting.raise_alert("backup", alerting.RED, "Supabase 每日備份失敗", now=NOW + timedelta(hours=23))
    alerting.raise_alert("backup", alerting.RED, "Supabase 每日備份失敗", now=NOW + timedelta(hours=25))
    alerting.raise_alert("backup", alerting.RED, "Supabase 每日備份失敗", now=NOW + timedelta(hours=49))
    assert len(sent) == 2 and sent[1].startswith("⏳ 仍未恢復")


def test_collector_dropped_from_check_is_cleared_silently(sent):
    """停用或進入季節外 → 不在 findings → 安靜收掉，只進日報「已恢復」。"""
    alerting.update_staleness([F("wbgt", True)], now=NOW)
    alerting.update_staleness([], now=NOW + timedelta(minutes=15))
    assert len(sent) == 1
    assert alerting.load_state()["entries"] == {}


def test_failures_alone_never_send(sent):
    for i in range(60):
        alerting.collector_failed("bus", "HTTP 500", interval_minutes=2, now=NOW + timedelta(minutes=2 * i))
    alerting.update_staleness([F("bus", False, 1)], now=NOW + timedelta(hours=2))
    assert sent == []


# ── StalenessWatch.run：零 DB 讀取 ─────────────────────────

@pytest.fixture
def watch_env(monkeypatch):
    monkeypatch.setattr(monitoring, "load_realtime_tables", lambda: TABLES)
    monkeypatch.setattr(monitoring, "load_cross_layer_map", lambda: CMAP)

    def boom(*_a, **_k):
        raise AssertionError("staleness_watch 不可以查 DB")
    monkeypatch.setattr(monitoring, "query_realtime_health", boom)
    monkeypatch.setattr(alerting, "_ACTIVITY", {})
    monkeypatch.setattr(alerting, "_DB_WRITES", {"ok": None, "fail": None, "fails_since_ok": 0})


def C(name, interval):
    return SimpleNamespace(name=name, interval_minutes=interval)


def test_run_uses_in_memory_writes_and_merges(sent, watch_env):
    w = StalenessWatch([C("youbike", 15), C("earthquake", 5)], now=DAY_AGO)
    alerting.mark_write("youbike", 120, now=NOW - timedelta(minutes=10))
    alerting.mark_write("youbike", 0, now=NOW)            # 0 筆不算新資料
    alerting.mark_success("youbike", now=NOW)
    alerting.mark_success("earthquake", now=NOW + timedelta(minutes=14))
    alerting.mark_db_result(True, now=NOW)
    assert w.run(now=NOW) == []
    assert w.run(now=NOW + timedelta(minutes=30)) == []   # youbike 40 分沒寫入，門檻 45
    msgs = w.run(now=NOW + timedelta(minutes=45))
    assert len(msgs) == 1 and "2 支收集器疑似壞掉" in msgs[0]
    assert "youbike：已 55 分沒有新資料" in msgs[0] and "earthquake：已 31 分沒有成功執行" in msgs[0]
    alerting.mark_write("youbike", 5, now=NOW + timedelta(minutes=50))
    alerting.mark_success("earthquake", now=NOW + timedelta(minutes=50))
    msgs = w.run(now=NOW + timedelta(minutes=60))
    assert len(msgs) == 1 and msgs[0].startswith("✅ 2 支收集器恢復")


def test_run_persists_activity_across_restart(sent, watch_env, monkeypatch):
    before = StalenessWatch([C("youbike", 15)], now=DAY_AGO)
    alerting.mark_write("youbike", 3, now=NOW - timedelta(minutes=5))
    assert before.run(now=NOW) == []
    monkeypatch.setattr(alerting, "_ACTIVITY", {})       # 重啟：行程內紀錄歸零
    after = StalenessWatch([C("youbike", 15)], now=NOW + timedelta(minutes=1))
    assert after.run(now=NOW + timedelta(minutes=30)) == []   # 存檔的 35 分前寫入 < 45
    msgs = after.run(now=NOW + timedelta(minutes=45))     # 重啟後一直沒寫入 → 照報
    assert len(msgs) == 1 and "youbike" in msgs[0]


def test_run_extra_persistent_worker_interval_from_cmap(sent, watch_env, monkeypatch):
    cmap = {**CMAP, "aisstream": {"enabled": True, "deployment": "pulse_host",
                                  "expected_interval_min": 1}}
    monkeypatch.setattr(monitoring, "load_cross_layer_map", lambda: cmap)
    w = StalenessWatch([], ["aisstream"], now=DAY_AGO)
    msgs = w.run(now=NOW)
    assert len(msgs) == 1 and "aisstream" in msgs[0]


def test_db_down_alert_once_suppresses_collectors_and_recovers(sent, watch_env):
    w = StalenessWatch([C("youbike", 15)], now=DAY_AGO)
    alerting.mark_write("youbike", 3, now=NOW)
    alerting.mark_db_result(True, now=NOW)
    for k in range(1, 40):
        alerting.mark_db_result(False, now=NOW + timedelta(minutes=k))
    assert w.run(now=NOW + timedelta(minutes=20)) == [] and sent == []  # 才 20 分
    w.run(now=NOW + timedelta(minutes=35))
    assert len(sent) == 1 and sent[0].startswith("🔴 資料庫連不上")
    w.run(now=NOW + timedelta(minutes=50))  # youbike 50 分沒寫入，但 DB 斷線期間不報收集器
    assert len(sent) == 1
    alerting.mark_db_result(True, now=NOW + timedelta(minutes=52))
    alerting.mark_write("youbike", 9, now=NOW + timedelta(minutes=52))
    w.run(now=NOW + timedelta(minutes=55))
    assert len(sent) == 2 and sent[1].startswith("✅ 恢復：資料庫寫入")


def test_one_collector_failing_writes_is_not_db_down(sent, watch_env):
    w = StalenessWatch([], now=DAY_AGO)
    for k in range(40):
        alerting.mark_db_result(False, now=NOW + timedelta(minutes=k))
        alerting.mark_db_result(True, now=NOW + timedelta(minutes=k, seconds=30))
    w.run(now=NOW + timedelta(minutes=40))
    assert sent == []


def test_writer_records_write_and_db_result(monkeypatch):
    """supabase_writer 成功寫入 ≥1 筆 → mark_write／mark_db_result(True)，失敗 → False。"""
    monkeypatch.setattr(alerting, "_ACTIVITY", {})
    monkeypatch.setattr(alerting, "_DB_WRITES", {"ok": None, "fail": None, "fails_since_ok": 0})
    from contextlib import contextmanager
    from storage.supabase_writer import SupabaseWriter
    wr = SupabaseWriter.__new__(SupabaseWriter)
    import threading
    wr._err_lock = threading.Lock()
    wr._db_consecutive_errors = {}

    @contextmanager
    def borrow(*_a, **_k):
        yield object()
    wr._pool = SimpleNamespace(borrow=borrow)
    monkeypatch.setattr(wr, "_transform", lambda *a: [{"x": 1}, {"x": 2}], raising=False)
    monkeypatch.setattr(wr, "_write_to_db", lambda *a: None, raising=False)
    monkeypatch.setattr(wr, "_report_heartbeat", lambda *a, **k: None, raising=False)
    assert wr.write("youbike", {"data": [1, 2]}, NOW) is True
    act, db = alerting.activity_snapshot()
    assert "write" in act["youbike"] and db["ok"] is not None
    monkeypatch.setattr(wr, "_write_to_db", lambda *a: (_ for _ in ()).throw(RuntimeError("down")),
                        raising=False)
    monkeypatch.setattr(wr, "_should_buffer", lambda *_: False, raising=False)
    monkeypatch.setattr(wr, "_record_db_error", lambda *_: None, raising=False)
    assert wr.write("youbike", {"data": [1]}, NOW) is False
    assert alerting.activity_snapshot()[1]["fails_since_ok"] == 1


def test_db_down_not_cleared_without_success_after_last_failure(sent, watch_env):
    """失敗滿 DB_DOWN_MIN 後 _db_down 回 False，但最後失敗之後沒有成功 → 不可宣告恢復（review F052）。"""
    w = StalenessWatch([], now=DAY_AGO)
    alerting.mark_db_result(True, now=NOW)
    for k in range(1, 40):
        alerting.mark_db_result(False, now=NOW + timedelta(minutes=k))
    w.run(now=NOW + timedelta(minutes=45))
    assert len(sent) == 1 and sent[0].startswith("🔴 資料庫連不上")
    w.run(now=NOW + timedelta(minutes=39 + 31))  # 最後失敗已超過 30 分、仍無成功
    assert len(sent) == 1 and "db_down" in alerting.load_state()["entries"]


def test_writer_first_success_after_restart_clears_persisted_db_failure(monkeypatch):
    """重啟後記憶體錯誤計數是 0，第一次成功也要清掉 alert_state 裡的 db 失敗（review F050）。"""
    import threading
    from contextlib import contextmanager
    from storage.supabase_writer import SupabaseWriter
    monkeypatch.setattr(alerting, "_ACTIVITY", {})
    for i in range(5):
        alerting.collector_failed("youbike", "db down", reason="db", interval_minutes=15,
                                  now=NOW + timedelta(minutes=15 * i))
    assert "collector:youbike" in alerting.load_state()["entries"]
    wr = SupabaseWriter.__new__(SupabaseWriter)
    wr._err_lock = threading.Lock()
    wr._db_consecutive_errors = {}
    wr._db_ok_seen = set()

    @contextmanager
    def borrow(*_a, **_k):
        yield object()
    wr._pool = SimpleNamespace(borrow=borrow)
    monkeypatch.setattr(wr, "_transform", lambda *a: [{"x": 1}], raising=False)
    monkeypatch.setattr(wr, "_write_to_db", lambda *a: None, raising=False)
    monkeypatch.setattr(wr, "_report_heartbeat", lambda *a, **k: None, raising=False)
    assert wr.write("youbike", {"data": [1]}, NOW) is True
    assert "collector:youbike" not in alerting.load_state()["entries"]
