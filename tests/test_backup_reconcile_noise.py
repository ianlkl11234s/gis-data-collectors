"""backup_supabase Robot C：非收集器 S3 前綴與已刪表的狀態紀錄不再每天告警（2026-11-12）。"""
from datetime import datetime, timedelta, timezone

from tasks.backup_supabase import BackupSupabaseTask

TZ = timezone(timedelta(hours=8))


class _S3:
    def __init__(self, dates_by_prefix):
        self.dates_by_prefix = dates_by_prefix

    def list_dates(self, collector):
        return self.dates_by_prefix.get(collector, [])


def _task():
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task.dry_run = False
    task.audit_buf = []
    return task


def _codes(task):
    return [(a["severity"], a["code"], a["details"]) for a in task.audit_buf]


def test_unregistered_empty_prefix_is_not_a_collector():
    yesterday = (datetime.now(TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    task = _task()
    task.s3 = _S3({"youbike": [], "some_unknown_upload": [], "unknown_with_archives": [yesterday]})
    task._list_archive_collectors_from_s3 = lambda: ["youbike", "some_unknown_upload", "unknown_with_archives"]
    task._archive_expected_lag = lambda c: (2, None)

    assert task._check_archive_freshness() == 0
    # 登記過的收集器 archives/ 空 → 照舊告警；未登記又空 → 略過不寫 audit
    assert [(a["severity"], a["code"], a["message"]) for a in task.audit_buf] == [
        ("warn", "missing_s3", "collector youbike archives/ 目錄為空"),
    ]


def test_unregistered_prefix_with_archives_still_checked_for_staleness():
    task = _task()
    task.s3 = _S3({"unknown_with_archives": ["2026-01-01"]})
    task._list_archive_collectors_from_s3 = lambda: ["unknown_with_archives"]
    task._archive_expected_lag = lambda c: (2, None)
    assert task._check_archive_freshness() == 1
    assert task.audit_buf[0]["code"] == "upload_failed"


def test_known_non_collector_prefixes_listed_from_s3_are_excluded():
    class _Paginator:
        def paginate(self, **_):
            yield {"CommonPrefixes": [{"Prefix": f"{n}/"} for n in (
                "youbike", "_external_vm_health", "supabase-export", "warehouse-archive", "manual-backups",
                "migration", "source-archives", "trails", "events", "real_estate", "archives")]}

    class _Client:
        def get_paginator(self, _):
            return _Paginator()

    task = _task()
    task.s3 = type("S3", (), {"s3": _Client(), "bucket": "b"})()
    assert task._list_archive_collectors_from_s3() == ["youbike"]


class _Cur:
    def __init__(self, rows=None, fail=False):
        self.rows, self.fail = rows or [], fail

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if self.fail:
            raise RuntimeError("boom")
        assert "orphan_in_manifest" in sql and "interval '90 days'" in sql

    def fetchall(self):
        return self.rows


def _reconcile_task(prior_rows=None, fail=False):
    task = _task()
    task.db_conn = object()
    task.s3 = None
    task._static_schemas, task._realtime_schemas = {"public"}, set()
    task._exclude_tables, task._exclude_schemas, task._archive_covered = set(), set(), set()
    task._list_schema_tables = lambda schemas: [("public", "kept")]
    task._get_all_backup_state_tables = lambda: {"public.kept", "public.power_poles", "reference.routes"}
    task._cursor = lambda: _Cur(prior_rows, fail)
    task._check_archive_freshness = lambda: 0
    task._flush_audit = lambda: len(task.audit_buf)
    return task


def test_orphan_state_first_seen_warns_then_info():
    task = _reconcile_task(prior_rows=[("public", "power_poles")])
    counts = task.run_reconcile()
    orphans = {(a["schema_name"], a["table_name"]): a["severity"]
               for a in task.audit_buf if a["code"] == "orphan_in_manifest"}
    assert orphans == {("public", "power_poles"): "info", ("reference", "routes"): "warn"}
    assert counts["warn"] == 1 and counts["info"] == 1


def test_orphan_lookup_failure_falls_back_to_warn():
    task = _reconcile_task(fail=True)
    counts = task.run_reconcile()
    assert counts["warn"] == 2
