"""daily_report retention 覆蓋 section 測試（2026-07-07 稽核補上）。

守門目標：_section_retention_coverage 三種狀態都要正確呈現且不炸日報：
  1. 有缺口 → 列出表名 + issue
  2. 無缺口 → ✅
  3. DB 函數未部署 → 優雅降級顯示提示（不能 raise）
"""

import pytest

from tasks import monitoring
from tasks.daily_report import DailyReportTask


@pytest.fixture
def task():
    return DailyReportTask(collectors=[])


def test_section_with_gaps(monkeypatch, task):
    """有缺 retention 覆蓋的表 → 逐筆列出表名 + issue。"""
    monkeypatch.setattr(
        monitoring,
        "query_retention_coverage",
        lambda: {
            "status": "ok",
            "rows": [
                ("live.public_health_weekly", "no pg_cron cleanup job"),
                ("live.news_events", "no retention policy"),
            ],
        },
    )
    out = task._section_retention_coverage()
    assert "2 表缺覆蓋" in out
    assert "live.public_health_weekly" in out
    assert "no pg_cron cleanup job" in out
    assert "live.news_events" in out


def test_section_all_covered(monkeypatch, task):
    """無缺口 → ✅ 全覆蓋。"""
    monkeypatch.setattr(
        monitoring, "query_retention_coverage", lambda: {"status": "ok", "rows": []}
    )
    out = task._section_retention_coverage()
    assert "✅" in out
    assert "retention 覆蓋" in out


def test_section_function_not_deployed(monkeypatch, task):
    """DB 函數未部署 → 顯示提示，不 raise。"""
    monkeypatch.setattr(
        monitoring, "query_retention_coverage", lambda: {"status": "not_deployed"}
    )
    out = task._section_retention_coverage()
    assert "檢查函數未部署" in out


def test_section_query_error(monkeypatch, task):
    """DB 連線失敗等其他錯誤 → 顯示查詢失敗，不 raise。"""
    monkeypatch.setattr(
        monitoring,
        "query_retention_coverage",
        lambda: {"status": "error", "message": "connection refused"},
    )
    out = task._section_retention_coverage()
    assert "查詢失敗" in out
    assert "connection refused" in out


def test_query_detects_undefined_function(monkeypatch):
    """psycopg2 丟 UndefinedFunction (42883) → status='not_deployed'（優雅降級的關鍵路徑）。"""
    psycopg2 = pytest.importorskip("psycopg2")

    monkeypatch.setattr(monitoring.config, "SUPABASE_ENABLED", True)
    monkeypatch.setattr(monitoring.config, "SUPABASE_DB_URL", "postgresql://fake/db")

    def _raise_undefined(*args, **kwargs):
        raise psycopg2.errors.lookup("42883")(
            "function metadata.check_retention_coverage() does not exist"
        )

    monkeypatch.setattr(psycopg2, "connect", _raise_undefined)
    result = monitoring.query_retention_coverage()
    assert result == {"status": "not_deployed"}


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
