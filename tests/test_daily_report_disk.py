"""Daily report disk-capacity checks keep collector bytes separate from filesystem use."""

from collections import namedtuple

import pytest

from tasks import daily_report
from tasks.daily_report import DailyReportTask


DiskUsage = namedtuple("DiskUsage", "total used free")


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    path = tmp_path / "data"
    path.mkdir()
    monkeypatch.setattr(daily_report.config, "LOCAL_DATA_DIR", path)
    return path


def test_disk_capacity_below_warning_does_not_alert(monkeypatch, capsys, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 79, 21))
    monkeypatch.setattr(daily_report, "notify_disk_alert", lambda *_: pytest.fail("unexpected alert"))
    DailyReportTask([])._check_disk_usage()
    assert capsys.readouterr().out == ""


def test_disk_capacity_at_80_percent_reports_free_space(monkeypatch, capsys, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 80, 20))
    DailyReportTask([])._check_disk_usage()
    out = capsys.readouterr().out
    assert "80.0%" in out
    assert "剩餘" in out
    assert "容量警告" in out


def test_disk_capacity_at_90_percent_is_critical(monkeypatch, capsys, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 90, 10))
    DailyReportTask([])._check_disk_usage()
    out = capsys.readouterr().out
    assert "90.0%" in out
    assert "嚴重不足" in out


def test_disk_usage_failure_fails_closed_without_notification(monkeypatch, capsys, data_dir):
    def raise_oserror(_):
        raise OSError("disk unavailable")

    monkeypatch.setattr(daily_report.shutil, "disk_usage", raise_oserror)
    monkeypatch.setattr(daily_report, "notify_disk_alert", lambda *_: pytest.fail("unexpected alert"))
    DailyReportTask([])._check_disk_usage()
    assert "無法讀取檔案系統容量" in capsys.readouterr().out


def test_collector_threshold_keeps_existing_notification_tuple(monkeypatch, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 1, 99))
    monkeypatch.setattr(daily_report.config, "DISK_ALERT_THRESHOLD_MB", 1)
    sent = []
    monkeypatch.setattr(daily_report, "notify_disk_alert", lambda *args: sent.append(args))
    task = DailyReportTask([])
    (data_dir / "spool.sqlite").write_bytes(b"x" * (2 * 1024 * 1024))
    task._check_disk_usage()
    assert sent == [(2.0, 1)]


def test_existing_report_includes_shared_filesystem_warning(monkeypatch, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 90, 10))
    report = DailyReportTask([])._section_file_stats()
    assert "共享檔案系統" in report and "90.0%" in report


def test_zero_capacity_is_unknown(monkeypatch, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(0, 0, 0))
    assert "容量為 0" in DailyReportTask([])._filesystem_pressure()


def _stub_monitoring(monkeypatch):
    from tasks import monitoring

    monkeypatch.setattr(monitoring, "load_realtime_tables", lambda: [])
    monkeypatch.setattr(monitoring, "query_realtime_health", lambda *_: [])
    monkeypatch.setattr(monitoring, "load_cross_layer_map", lambda: {})
    monkeypatch.setattr(monitoring, "list_archive_dates_per_collector", lambda: {})
    monkeypatch.setattr(monitoring, "list_vm_health_snapshots", lambda **_: [])
    monkeypatch.setattr(monitoring, "load_anomaly_state", lambda: {})


def test_critical_disk_pressure_sets_speed_digest_red(monkeypatch, data_dir):
    _stub_monitoring(monkeypatch)
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 90, 10))
    digest = DailyReportTask([])._build_speed_digest()
    assert "🔴 *嚴重*" in digest
    assert "共享檔案系統" in digest


def test_critical_disk_pressure_sets_health_summary_red(monkeypatch, data_dir):
    _stub_monitoring(monkeypatch)
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 90, 10))
    summary = DailyReportTask([])._section_health_summary()
    assert "🔴 *嚴重*" in summary
    assert "檔案系統" in summary


def test_critical_disk_pressure_creates_first_action(monkeypatch, data_dir):
    _stub_monitoring(monkeypatch)
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 90, 10))
    action = DailyReportTask([])._section_today_action()
    assert "1. 立即清理或擴充共享檔案系統" in action


def test_file_stats_excludes_internal_archive_receipts(monkeypatch, data_dir):
    monkeypatch.setattr(daily_report.shutil, "disk_usage", lambda _: DiskUsage(100, 1, 99))
    receipt = data_dir / '.archive-receipts/demo/2020-01-02.json'
    receipt.parent.mkdir(parents=True)
    receipt.write_text('{}')
    payload = data_dir / 'demo/2020/01/02/demo.json'
    payload.parent.mkdir(parents=True)
    payload.write_text('{}')

    report = DailyReportTask([])._section_file_stats()
    assert "總計: *1* 個" in report
    assert ".archive-receipts" not in report
