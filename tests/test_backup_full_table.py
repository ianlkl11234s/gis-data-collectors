"""backup_supabase：manifest override `cadence: full_table` 走整表快照。"""
from datetime import date
from pathlib import Path

import yaml

from tasks.backup_supabase import BackupSupabaseTask


class _Cursor:
    def __init__(self, count):
        self.count = count
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchone(self):
        return (self.count,)


class _S3:
    def __init__(self):
        self.uploads = []

    def upload_snapshot(self, key, data, storage_class=None):
        self.uploads.append((key, data, storage_class))
        return True


def _task(count, overrides):
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task.dry_run = False
    task.realtime_storage_class = "STANDARD"
    task.manifest = {"defaults": {"realtime": {"storage_class": "STANDARD"}}}
    task._overrides = overrides
    task.audit_buf = []
    task.s3 = _S3()
    task.cursor = _Cursor(count)
    task._cursor = lambda: task.cursor
    task._copy_table_to_gzip = lambda schema, table: b"gz"
    task._find_time_column = lambda schema, table: (_ for _ in ()).throw(AssertionError("不應找時間欄"))
    return task


def test_full_table_uploads_whole_table_to_daily_key():
    task = _task(5, {"live.ship_last_seen": {"cadence": "full_table", "storage_class": "GLACIER_IR"}})
    assert task._process_realtime_table("live", "ship_last_seen", date(2026, 10, 15)) == "uploaded"
    assert task.s3.uploads == [
        ("supabase-snapshots/realtime/2026-10-15/realtime.ship_last_seen.csv.gz", b"gz", "GLACIER_IR")
    ]
    assert not any("WHERE" in s for s in task.cursor.sql)
    assert task.audit_buf[-1]["code"] == "snapshot_uploaded"


def test_full_table_empty_table_uploads_header_only_snapshot():
    task = _task(0, {"live.ship_last_seen": {"cadence": "full_table"}})
    assert task._process_realtime_table("live", "ship_last_seen", date(2026, 10, 15)) == "uploaded"
    assert len(task.s3.uploads) == 1
    assert task.audit_buf[-1]["code"] == "snapshot_uploaded"


def test_manifest_backs_up_ship_last_seen_as_full_table():
    repo = Path(__file__).resolve().parent.parent
    manifest = yaml.safe_load((repo / "config" / "backup_manifest.yaml").read_text(encoding="utf-8"))
    assert manifest["overrides"]["live.ship_last_seen"]["cadence"] == "full_table"
    assert "live.ship_last_seen" not in manifest["exclude"]
    assert "live.ship_last_seen" not in manifest["archive_py_covered"]
