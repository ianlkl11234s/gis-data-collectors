"""一則式日報（取代速報＋分段長日報）

全綠 ≤3 行；有事依「現在還壞的 → 昨天壞過、已恢復 → 資料停更前 5 → 例行」。
render_examples() 也被 PR 說明拿來產兩份範例。
"""

from collections import namedtuple
from datetime import datetime, timedelta, timezone

import pytest

from tasks import daily_report, monitoring
from tasks.daily_report import DailyReportTask
from utils import alerting

TPE = timezone(timedelta(hours=8))
NOW = datetime(2026, 11, 8, 8, 0, tzinfo=TPE)
DiskUsage = namedtuple("DiskUsage", "total used free")


class _C:
    def __init__(self, name, interval, last_run):
        self.name, self.interval_minutes, self._last = name, interval, last_run

    def get_status(self):
        return {"name": self.name, "run_count": 10, "error_count": 0,
                "last_run": self._last.isoformat(), "last_success_at": None}


def _setup(monkeypatch, tmp_path, *, problems: bool):
    monkeypatch.setattr(daily_report.config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(daily_report.config, "INSTANCE_NAME", "")
    monkeypatch.setattr(daily_report.config, "BACKUP_ENABLED", True, raising=False)
    gb = 1024 ** 3
    monkeypatch.setattr(daily_report.shutil, "disk_usage",
                        lambda _: DiskUsage(200 * gb, (182 if problems else 124) * gb,
                                            (18 if problems else 76) * gb))
    monkeypatch.setattr(monitoring, "list_vm_health_snapshots", lambda **_: [])
    monkeypatch.setattr(monitoring, "load_cross_layer_map", lambda: {})
    monkeypatch.setattr(monitoring, "list_archive_dates_per_collector", lambda: {})
    tables = [
        {"schema": "public", "table": "drought_alert_current", "expected_interval_min": 720},
        {"schema": "live", "table": "ship_positions", "expected_interval_min": 5, "critical": True},
        {"schema": "live", "table": "bus_positions", "expected_interval_min": 2, "critical": True},
    ]
    ages = {"drought_alert_current": 3 * 1440, "ship_positions": 240, "bus_positions": 1}
    monkeypatch.setattr(monitoring, "load_realtime_tables", lambda: tables)
    monkeypatch.setattr(monitoring, "query_realtime_health", lambda *_: [
        {"schema": t["schema"], "table": t["table"],
         "max_time": NOW - timedelta(minutes=ages[t["table"]] if problems else 1)}
        for t in tables])
    from tasks import backup_watch
    monkeypatch.setattr(backup_watch, "health_status", lambda: {
        "enabled": True, "last_success_date": NOW.date().isoformat()})

    alerting.record_routine("archive", True, "312 檔", now=NOW - timedelta(hours=5))
    alerting.record_routine("trails_export", True, "4 個", now=NOW - timedelta(hours=6))
    # 昨天 37 次小錯誤（未達門檻、都自己好了）
    for i in range(37):
        alerting.collector_failed("road_event_live", "timeout", interval_minutes=5,
                                  now=NOW - timedelta(hours=20, minutes=i * 7))
        alerting.collector_succeeded("road_event_live", now=NOW - timedelta(hours=20, minutes=i * 7 - 1))

    collectors = [_C("bus", 2, NOW - timedelta(minutes=1))]
    if problems:
        alerting.set_sender(lambda _t: True)
        for i in range(150):
            alerting.collector_failed("tdx_bus_eta", "HTTP 401 Unauthorized: 金鑰被拒",
                                      interval_minutes=2, now=NOW - timedelta(hours=6, minutes=-2 * i))
        # 昨天壞過、已恢復：DB 寫入中斷 28 分
        alerting.raise_alert("db_outage", alerting.RED, "DB 寫入中斷，已自動補寫",
                             now=NOW - timedelta(hours=4, minutes=48))
        alerting.clear_alert("db_outage", now=NOW - timedelta(hours=4, minutes=20))
    return DailyReportTask(collectors)


def render_examples(monkeypatch, tmp_path) -> dict[str, str]:
    out = {}
    for label, problems in (("all_green", False), ("two_problems", True)):
        d = tmp_path / label
        d.mkdir()
        task = _setup(monkeypatch, d, problems=problems)
        out[label] = task._build_compact_report(detail_key="_daily_report/2026-11-08.md", now=NOW)
    return out


@pytest.fixture(autouse=True)
def _restore_sender():
    yield
    alerting.set_sender(None)


def test_all_green_is_three_lines(monkeypatch, tmp_path):
    report = _setup(monkeypatch, tmp_path, problems=False)._build_compact_report(now=NOW)
    lines = report.splitlines()
    assert len(lines) <= 3, report
    assert lines[0].startswith("☀️ Pulse 日報 11/08（週日）")
    assert "✅ 一切正常" in lines[1] and "小錯誤 37 次" in lines[1]
    assert lines[2].startswith("🗄 例行：備份 ✅") and "歸檔 ✅ 312 檔" in lines[2]


def test_problem_day_sections_in_order(monkeypatch, tmp_path):
    report = _setup(monkeypatch, tmp_path, problems=True)._build_compact_report(
        detail_key="_daily_report/2026-11-08.md", now=NOW)
    assert "整體：🔴 有 2 件要處理" in report
    i_broken = report.index("現在還壞的")
    i_resolved = report.index("昨天壞過、已恢復")
    i_stale = report.index("資料停更前 5")
    i_routine = report.index("🗄 例行")
    assert i_broken < i_resolved < i_stale < i_routine
    assert "收集器 tdx_bus_eta 壞" in report and "401" in report
    assert "嚴重不足" in report         # 磁碟 91%
    assert "DB 寫入中斷" in report
    # 依落後倍數排序：ship 240/5=48 倍 > drought 4320/720=6 倍
    assert report.index("ship_positions") < report.index("public.drought_alert_current")
    assert "bus_positions" not in report
    assert "ship_positions 4 小時沒更新（平常每 5 分鐘）" in report
    assert "收集器小錯誤 37 次" in report
    assert "詳細：S3 _daily_report/2026-11-08.md" in report


def test_silent_collector_listed_not_sent(monkeypatch, tmp_path):
    task = _setup(monkeypatch, tmp_path, problems=False)
    task.collectors.append(_C("water_reservoir", 60, NOW - timedelta(hours=5)))
    report = task._build_compact_report(now=NOW)
    assert "🟡 收集器 water_reservoir 已 5 小時沒執行（平常每小時）" in report
    assert "整體：🟡 有 1 件要處理" in report


def test_run_sends_one_message_uploads_and_pings(monkeypatch, tmp_path):
    task = _setup(monkeypatch, tmp_path, problems=False)
    sent, pings, uploads = [], [], []
    monkeypatch.setattr(daily_report, "send_telegram",
                        lambda text, parse_mode=None: sent.append(text) or True)
    monkeypatch.setattr(task, "_upload_full_report", lambda: uploads.append(1) or "_daily_report/x.md")
    monkeypatch.setattr(daily_report.config, "HC_DAILY_REPORT_PING_URL", "https://hc.example/ping")
    import requests
    monkeypatch.setattr(requests, "get", lambda url, timeout: pings.append(url))
    task.run()
    assert len(sent) == 1 and uploads == [1] and pings == ["https://hc.example/ping"]


def test_run_ping_skipped_when_unset_or_send_fails(monkeypatch, tmp_path):
    task = _setup(monkeypatch, tmp_path, problems=False)
    pings = []
    import requests
    monkeypatch.setattr(requests, "get", lambda url, timeout: pings.append(url))
    monkeypatch.setattr(task, "_upload_full_report", lambda: None)
    monkeypatch.setattr(daily_report.config, "HC_DAILY_REPORT_PING_URL", "")
    monkeypatch.setattr(daily_report, "send_telegram", lambda text, parse_mode=None: True)
    task.run()
    monkeypatch.setattr(daily_report.config, "HC_DAILY_REPORT_PING_URL", "https://hc.example/ping")
    monkeypatch.setattr(daily_report, "send_telegram", lambda text, parse_mode=None: False)
    task.run()
    assert pings == []


def test_run_ping_failure_does_not_raise(monkeypatch, tmp_path):
    task = _setup(monkeypatch, tmp_path, problems=False)
    monkeypatch.setattr(task, "_upload_full_report", lambda: None)
    monkeypatch.setattr(daily_report.config, "HC_DAILY_REPORT_PING_URL", "https://hc.example/ping")
    monkeypatch.setattr(daily_report, "send_telegram", lambda text, parse_mode=None: True)
    import requests

    def boom(url, timeout):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(requests, "get", boom)
    task.run()  # 不得拋出


def test_upload_full_report_writes_markdown_key(monkeypatch, tmp_path):
    task = _setup(monkeypatch, tmp_path, problems=False)
    monkeypatch.setattr(daily_report.config, "S3_BUCKET", "bucket")
    monkeypatch.setattr(task, "_build_report", lambda: "# full")
    puts = []

    class FakeS3:
        bucket = "bucket"

        class s3:
            @staticmethod
            def put_object(**kw):
                puts.append(kw)

    import storage.s3
    monkeypatch.setattr(storage.s3, "S3Storage", FakeS3)
    key = task._upload_full_report()
    assert key.startswith("_daily_report/") and key.endswith(".md")
    assert puts[0]["Body"] == b"# full" and puts[0]["Key"] == key


# ─────────────────────────────────────────────────────────
# 停更清單：停用／事件驅動／季節外不列，文字講人話（2026-11-09 誤報修正）
# ─────────────────────────────────────────────────────────

# 2026-11-09 正式主機日報列為停更的 8 張表：(schema, table, 落後分鐘)
REPORTED_20261109 = [
    ("live", "env_jp_wbgt_observations", int(18.4 * 1440)),
    ("live", "env_jp_wbgt_forecasts", int(18.4 * 1440)),
    ("live", "jma_rainy_season", int(27.7 * 1440)),
    ("live", "earthquake_shakemap_grid", int(74.2 * 1440)),
    ("air_tickets", "fare_offers", int(61.5 * 1440)),
    ("public", "drought_alert_current", int(177.7 * 1440)),
    ("live", "launch_events", 144),
    ("live", "jma_volcano_warnings", 5 * 1440),
]


def _real_yaml_report(monkeypatch, tmp_path, now, extra_ages=None):
    """用 repo 真實的 realtime_tables／cross_layer_map，只假造各表最後寫入時間。"""
    monkeypatch.setattr(daily_report.config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(daily_report.config, "INSTANCE_NAME", "")
    monkeypatch.setattr(daily_report.config, "BACKUP_ENABLED", True, raising=False)
    gb = 1024 ** 3
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(200 * gb, 124 * gb, 76 * gb))
    monkeypatch.setattr(monitoring, "list_vm_health_snapshots", lambda **_: [])
    monkeypatch.setattr(monitoring, "list_archive_dates_per_collector", lambda: {})
    from tasks import backup_watch
    monkeypatch.setattr(backup_watch, "health_status", lambda: {
        "enabled": True, "last_success_date": now.date().isoformat()})
    ages = {t: a for _, t, a in REPORTED_20261109}
    ages.update(extra_ages or {})
    monkeypatch.setattr(monitoring, "query_realtime_health", lambda tables: [
        {"schema": t["schema"], "table": t["table"], "error": None,
         "max_time": now - timedelta(minutes=ages.get(t["table"], 1))}
        for t in tables])
    return DailyReportTask([])._build_compact_report(now=now)


def test_20261109_false_alarms_gone(monkeypatch, tmp_path):
    """11/09 的 8 張表：季節外、事件驅動、停用、interval 改正後都不該上停更清單。"""
    now = datetime(2026, 11, 9, 8, 0, tzinfo=TPE)
    report = _real_yaml_report(monkeypatch, tmp_path, now)
    assert "資料停更" not in report, report
    for _, table, _ in REPORTED_20261109:
        assert table not in report


def test_stale_line_is_plain_chinese(monkeypatch, tmp_path):
    """當季的 WBGT 真的停了 → 中文名＋「X 天沒更新（平常每 2 小時）」＋表名短碼。"""
    now = datetime(2026, 7, 15, 8, 0, tzinfo=TPE)
    report = _real_yaml_report(monkeypatch, tmp_path, now,
                               extra_ages={"weather_current": 4 * 60})
    assert "日本熱中症指數（觀測） 18 天沒更新（平常每 2 小時） · env_jp_wbgt_observations" in report
    assert "日本熱中症指數（預報） 18 天沒更新（平常每天） · env_jp_wbgt_forecasts" in report
    assert "氣象即時 4 小時沒更新（平常每小時） · weather_current" in report
    assert "jma_rainy_season" not in report  # 季內仍是事件驅動


def test_stale_skip_reasons():
    cmap = {
        "off": {"enabled": False, "deployment": "disabled", "supabase_tables": ["live.shared", "live.only_off"]},
        "on": {"enabled": True, "deployment": "pulse_host", "supabase_tables": ["live.shared"]},
    }
    reason = monitoring.stale_check_skip_reason
    nov = datetime(2026, 11, 9, tzinfo=TPE)
    jul = datetime(2026, 7, 1, tzinfo=TPE)
    assert reason({"schema": "live", "table": "only_off", "owner_collector": "off"}, cmap, nov) == "disabled"
    # 停用 owner，但還有啟用中的 collector 也寫這張表 → 照常檢查
    assert reason({"schema": "live", "table": "shared", "owner_collector": "off"}, cmap, nov) is None
    assert reason({"schema": "x", "table": "y", "enabled": False}, cmap, nov) == "disabled"
    assert reason({"schema": "x", "table": "y", "event_driven": True}, cmap, nov) == "event_driven"
    w = {"schema": "x", "table": "y", "active_window": {"from": "04-29", "to": "10-21"}}
    assert reason(w, cmap, nov) == "off_season"
    assert reason(w, cmap, jul) is None
    winter = {"schema": "x", "table": "y", "active_window": {"from": "12-01", "to": "02-28"}}
    assert reason(winter, cmap, datetime(2027, 1, 5, tzinfo=TPE)) is None
    assert reason(winter, cmap, jul) == "off_season"


def test_human_interval_and_ago():
    hi, ago = daily_report._human_interval, daily_report._human_ago
    assert [hi(m) for m in (15, 60, 120, 1440, 2880, 10080, 43200)] == [
        "每 15 分鐘", "每小時", "每 2 小時", "每天", "每 2 天", "每週", "每月"]
    assert [ago(m) for m in (40, 144, 2879, int(18.4 * 1440))] == ["40 分鐘", "2 小時", "47 小時", "18 天"]


def test_enabled_entries_have_labels():
    """啟用中的收集器與要檢查的表都要有中文 label（日報／告警顯示用）。"""
    cmap = monitoring.load_cross_layer_map()
    missing = [k for k, v in cmap.items() if isinstance(v, dict) and v.get("enabled") and not v.get("label")]
    assert not missing, f"cross_layer_map.yaml 缺 label：{missing}"
    tables = monitoring.load_realtime_tables()
    missing = [f"{t['schema']}.{t['table']}" for t in tables
               if monitoring.stale_check_skip_reason(t, cmap) != "disabled" and not t.get("label")]
    assert not missing, f"realtime_tables.yaml 缺 label：{missing}"


def test_alert_uses_chinese_label():
    alerting._set_label_cache({"bus": "雙北公車位置"})
    alerting.set_sender(lambda _t: True)
    msgs = alerting.update_staleness([{"name": "bus", "stale": True, "basis": "write",
                                       "last_at": NOW - timedelta(minutes=40), "threshold_min": 30}],
                                     now=NOW)
    assert any("🔴 收集器疑似壞掉：雙北公車位置（bus）" in m for m in msgs), msgs


def test_stale_collector_listed_once_in_daily(monkeypatch, tmp_path):
    """同一支同時「連續失敗 🟡」與「太久沒新資料 🔴」→ 日報只列一列 🔴。"""
    task = _setup(monkeypatch, tmp_path, problems=False)
    alerting.set_sender(lambda _t: True)
    for i in range(10):
        alerting.collector_failed("youbike", "HTTP 500", interval_minutes=15,
                                  now=NOW - timedelta(hours=3) + timedelta(minutes=15 * i))
    alerting.update_staleness([{"name": "youbike", "stale": True, "basis": "write",
                                "last_at": NOW - timedelta(hours=3), "threshold_min": 45}],
                              now=NOW - timedelta(minutes=5))
    report = task._build_compact_report(now=NOW)
    assert report.count("youbike") == 1, report
    assert "🔴 收集器 youbike 已 2 小時 55 分沒有新資料" in report, report


def test_freshness_query_error_and_never_are_problems(monkeypatch, tmp_path):
    """查詢失敗、從未有資料的表不可當成正常（review F051）。"""
    task = _setup(monkeypatch, tmp_path, problems=False)
    tables = [
        {"schema": "live", "table": "bus_positions", "expected_interval_min": 2, "critical": True,
         "label": "雙北公車位置"},
        {"schema": "live", "table": "ship_positions", "expected_interval_min": 5, "label": "船舶位置"},
        {"schema": "live", "table": "youbike_status", "expected_interval_min": 5},
    ]
    monkeypatch.setattr(monitoring, "load_realtime_tables", lambda: tables)
    monkeypatch.setattr(monitoring, "query_realtime_health", lambda *_: [
        {"schema": "live", "table": "bus_positions", "max_time": None, "error": None},
        {"schema": "live", "table": "ship_positions", "max_time": None, "error": "timeout"},
        {"schema": "live", "table": "youbike_status", "max_time": NOW, "error": None},
    ])
    report = task._build_compact_report(now=NOW)
    assert "✅ 一切正常" not in report
    assert "🔴" in report.splitlines()[1]  # critical 表從未有資料
    assert "1 張表從未有資料：雙北公車位置" in report
    assert "1 張表查不到新鮮度（查詢失敗）：船舶位置" in report
