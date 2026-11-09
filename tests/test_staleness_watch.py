"""收集器「太久沒新資料」即時告警（tasks/staleness_watch.py＋alerting.update_staleness）

使用者決定（2026-11-09）：失敗本身不發；資料斷了超過門檻才發 🔴，恢復發 ✅。
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import config
from tasks import monitoring, staleness_watch
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


# ── evaluate ──────────────────────────────────────────

def test_any_main_table_fresh_counts_as_ok():
    p = plan_collector("marine", 60, TABLES, CMAP, NOW)
    known = {("live", "marine_readings"): NOW - timedelta(minutes=30),
             ("live", "marine_stations"): NOW - timedelta(days=5)}
    assert evaluate(p, known, None, NOW - timedelta(days=1), NOW)["stale"] is False


def test_table_stale_beyond_threshold():
    p = plan_collector("youbike", 15, TABLES, CMAP, NOW)
    known = {("live", "youbike_snapshots"): NOW - timedelta(minutes=46),
             ("live", "youbike_current"): NOW - timedelta(minutes=50)}
    f = evaluate(p, known, NOW, NOW - timedelta(days=1), NOW)
    assert f["stale"] and f["basis"] == "table"
    known[("live", "youbike_current")] = NOW - timedelta(minutes=44)
    assert evaluate(p, known, None, NOW - timedelta(days=1), NOW)["stale"] is False


def test_event_driven_uses_last_success():
    p = plan_collector("earthquake", 5, TABLES, CMAP, NOW)
    started = NOW - timedelta(days=1)
    assert evaluate(p, {}, NOW - timedelta(minutes=20), started, NOW)["stale"] is False
    f = evaluate(p, {}, NOW - timedelta(minutes=31), started, NOW)
    assert f["stale"] and f["basis"] == "success"


def test_no_success_record_waits_from_process_start():
    p = plan_collector("earthquake", 5, TABLES, CMAP, NOW)
    assert evaluate(p, {}, None, NOW - timedelta(minutes=10), NOW)["stale"] is False  # 剛重啟
    assert evaluate(p, {}, None, NOW - timedelta(hours=2), NOW)["stale"] is True


def test_tables_without_any_data_fall_back_to_last_success():
    p = plan_collector("youbike", 15, TABLES, CMAP, NOW)
    f = evaluate(p, {("live", "youbike_current"): None}, NOW - timedelta(minutes=5),
                 NOW - timedelta(days=1), NOW)
    assert f["basis"] == "success" and f["stale"] is False


# ── update_staleness：開始、恢復、去重、合併、24h ──────────────

def F(name, stale, last_min=60, threshold=30, basis="table"):
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
    assert len(sent) == 2 and sent[1].startswith("⏳ 仍未恢復") and "a、b" in sent[1]


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


# ── StalenessWatch.run：查哪些表、DB 失敗不判斷 ────────────────

@pytest.fixture
def watch_env(monkeypatch):
    monkeypatch.setattr(config, "SUPABASE_ENABLED", True)
    monkeypatch.setattr(config, "SUPABASE_DB_URL", "postgres://x")
    monkeypatch.setattr(config, "STALE_CHECK_INTERVAL_MIN", 15)
    monkeypatch.setattr(monitoring, "load_realtime_tables", lambda: TABLES)
    monkeypatch.setattr(monitoring, "load_cross_layer_map", lambda: CMAP)
    monkeypatch.setattr(monitoring, "query_collector_last_success", lambda: {})
    env = SimpleNamespace(max_time={}, queried=[])

    def fake_health(tables):
        env.queried.append(sorted(t["table"] for t in tables))
        return [{"schema": t["schema"], "table": t["table"], "count_24h": 1, "error": None,
                 "max_time": env.max_time.get(t["table"])} for t in tables]
    monkeypatch.setattr(monitoring, "query_realtime_health", fake_health)
    return env


def C(name, interval, last_success=None):
    return SimpleNamespace(name=name, interval_minutes=interval, last_success_at=last_success)


def test_run_alerts_and_only_requeries_tables_that_may_be_stale(sent, watch_env):
    w = StalenessWatch([C("youbike", 15), C("earthquake", 5, NOW + timedelta(minutes=14))],
                       now=NOW - timedelta(days=1))
    watch_env.max_time = {"youbike_snapshots": NOW - timedelta(minutes=10),
                          "youbike_current": NOW - timedelta(minutes=10)}
    assert w.run(now=NOW) == []
    # 15 分鐘後：上次看到 10 分鐘前的資料，門檻 45 − 間隔 15 → 還不用重查
    assert w.run(now=NOW + timedelta(minutes=15)) == []
    assert len(watch_env.queried) == 1
    # 30 分鐘後：可能快超過 → 重查；上游停了 → 40 分沒資料，還沒到 45
    w.run(now=NOW + timedelta(minutes=30))
    assert len(watch_env.queried) == 2 and sent == []
    # 45 分鐘後：55 分沒資料 → 🔴（earthquake 的最後成功也過期，一起合併成一則）
    msgs = w.run(now=NOW + timedelta(minutes=45))
    assert len(msgs) == 1 and "2 支收集器疑似壞掉" in msgs[0]
    assert "youbike：已 55 分沒有新資料" in msgs[0] and "earthquake：已 31 分沒有成功執行" in msgs[0]


def test_run_skips_round_when_db_query_fails(sent, watch_env, monkeypatch):
    monkeypatch.setattr(monitoring, "query_realtime_health", lambda tables: [])
    w = StalenessWatch([C("youbike", 15)], now=NOW - timedelta(days=1))
    assert w.run(now=NOW) is None
    assert sent == []


def test_run_uses_db_last_success_over_restart(sent, watch_env, monkeypatch):
    """行程剛重啟（in-memory 沒紀錄），DB collector_status 顯示 2 小時沒成功 → 照報。"""
    monkeypatch.setattr(monitoring, "query_collector_last_success",
                        lambda: {"earthquake": NOW - timedelta(hours=2)})
    w = StalenessWatch([C("earthquake", 5)], now=NOW - timedelta(minutes=5))
    msgs = w.run(now=NOW)
    assert len(msgs) == 1 and "earthquake" in msgs[0] and "沒有成功執行" in msgs[0]


def test_run_extra_persistent_worker_interval_from_cmap(sent, watch_env, monkeypatch):
    cmap = {**CMAP, "aisstream": {"enabled": True, "deployment": "pulse_host",
                                  "expected_interval_min": 1}}
    monkeypatch.setattr(monitoring, "load_cross_layer_map", lambda: cmap)
    w = StalenessWatch([], ["aisstream"], now=NOW - timedelta(days=1))
    msgs = w.run(now=NOW)
    assert len(msgs) == 1 and "aisstream" in msgs[0]


def test_run_persists_last_success_across_restart(sent, watch_env, monkeypatch):
    """DB collector_status 很舊（沒新事件就不更新），但上一個行程記得它 5 分鐘前還成功 → 重啟後不誤報。"""
    monkeypatch.setattr(monitoring, "query_collector_last_success",
                        lambda: {"earthquake": NOW - timedelta(days=30)})
    before = StalenessWatch([C("earthquake", 5, NOW - timedelta(minutes=5))], now=NOW - timedelta(days=1))
    assert before.run(now=NOW) == []
    after = StalenessWatch([C("earthquake", 5)], now=NOW + timedelta(minutes=1))  # 重啟，行程內沒紀錄
    assert after.run(now=NOW + timedelta(minutes=16)) == []
    msgs = after.run(now=NOW + timedelta(minutes=40))  # 重啟後一直沒成功、超過 30 分 → 照報
    assert len(msgs) == 1 and "earthquake" in msgs[0]
