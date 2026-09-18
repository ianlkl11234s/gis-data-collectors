import json
from datetime import datetime, timezone

from scripts import gis_collectors_monitor_snapshot
from scripts.gis_collectors_monitor_policy import classify_gfw_hourly_publish_health
from tasks import monitoring
from tasks.daily_report import DailyReportTask


def test_query_gfw_health_uses_public_rpc_and_reports_failed_attempt(monkeypatch):
    class Cursor:
        calls = []
        description = [
            type("Column", (), {"name": name}) for name in (
                "latest_attempt_status", "latest_attempt_started_at", "release_id",
                "latest_complete_date", "published_at",
            )
        ]

        def execute(self, sql):
            self.calls.append(sql)

        def fetchone(self):
            return (
                "failed", datetime(2026, 9, 18, 1, tzinfo=timezone.utc), "2026-09-13",
                "2026-09-13", datetime(2026, 9, 17, 2, tzinfo=timezone.utc),
            )

        def __enter__(self): return self
        def __exit__(self, *_): return False

    class Connection:
        def cursor(self): return Cursor()
        def __enter__(self): return self
        def __exit__(self, *_): return False

    import psycopg2
    monkeypatch.setattr(monitoring.config, "SUPABASE_ENABLED", True)
    monkeypatch.setattr(monitoring.config, "SUPABASE_DB_URL", "postgresql://fake/db")
    monkeypatch.setattr(psycopg2, "connect", lambda *_args, **_kwargs: Connection())
    monkeypatch.setattr(
        monitoring, "load_gfw_hourly_publish_monitor_config",
        lambda: {"source_lag_days": 5, "schedule_grace_hours": 30},
    )

    health = monitoring.query_gfw_hourly_publish_health(
        now=datetime(2026, 9, 18, 3, tzinfo=timezone.utc)
    )
    assert health["state"] == "FAILED"
    assert health["latest_complete_date"].isoformat() == "2026-09-13"
    assert Cursor.calls == [
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = '15s'",
        "SELECT * FROM public.get_gfw_hourly_publish_health() LIMIT 1",
    ]


def test_daily_gfw_section_labels_attempt_success_and_utc_source_date(monkeypatch):
    monkeypatch.setattr(monitoring, "query_gfw_hourly_publish_health", lambda: {
        "state": "OLD_RUNNING", "level": "critical", "latest_attempt_status": "running",
        "latest_attempt_started_at": datetime(2026, 9, 16, 1, tzinfo=timezone.utc),
        "published_at": datetime(2026, 9, 17, 2, tzinfo=timezone.utc),
        "latest_complete_date": "2026-09-13", "attempt_age_hours": 50.0,
        "source_age_days": 5, "source_lag_days": 5,
    })

    output = DailyReportTask(collectors=[])._section_gfw_hourly_publish()
    assert "最近嘗試: running" in output
    assert "最近成功 current" in output
    assert "資料 UTC 日期 2026-09-13" in output
    assert "OLD_RUNNING" in output


def test_gfw_real_shaped_health_is_json_serializable_with_explicit_iso_dates():
    health = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "failed",
        "latest_attempt_started_at": "2026-09-18T01:00:00Z",
        "latest_complete_date": "2026-08-21",
        "published_at": "2026-08-26T00:00:00Z",
    }, datetime(2026, 9, 18, 23, tzinfo=timezone.utc))

    payload = json.loads(json.dumps(
        {"gfw_hourly_publish": health},
        default=gis_collectors_monitor_snapshot._json_datetime_or_date,
    ))
    assert payload["gfw_hourly_publish"]["latest_complete_date"] == "2026-08-21"
    assert payload["gfw_hourly_publish"]["published_at"] == "2026-08-26T00:00:00+00:00"
    assert payload["gfw_hourly_publish"]["attempt_age_hours"] == 22.0
    assert payload["gfw_hourly_publish"]["success_age_hours"] == 575.0
