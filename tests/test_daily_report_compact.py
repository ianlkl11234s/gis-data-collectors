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
        alerting._set_critical_cache({"tdx_bus_eta": True})
        alerting.set_sender(lambda _t: True)
        for i in range(150):
            alerting.collector_failed("tdx_bus_eta", "HTTP 401 Unauthorized: 金鑰被拒",
                                      interval_minutes=2, now=NOW - timedelta(hours=6, minutes=-2 * i))
        alerting.flush(now=NOW - timedelta(hours=5))
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
    assert report.index("live.ship_positions") < report.index("public.drought_alert_current")
    assert "live.bus_positions" not in report
    assert "收集器小錯誤 37 次" in report
    assert "詳細：S3 _daily_report/2026-11-08.md" in report


def test_silent_collector_listed_not_sent(monkeypatch, tmp_path):
    task = _setup(monkeypatch, tmp_path, problems=False)
    task.collectors.append(_C("water_reservoir", 60, NOW - timedelta(hours=5)))
    report = task._build_compact_report(now=NOW)
    assert "🟡 收集器 water_reservoir 5 小時沒執行（應每 1 小時）" in report
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
