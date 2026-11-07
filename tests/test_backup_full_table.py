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
        self.headers = []

    def upload_path(self, path, key, *, storage_class=None, content_type=None, content_encoding=None):
        self.uploads.append((key, Path(path).read_bytes(), storage_class))
        self.headers.append((content_type, content_encoding))
        return True


def _gz_file(tmp_dir):
    def _make(schema, table):
        path = Path(tmp_dir) / f"backup-{schema}.{table}.csv.gz"
        path.write_bytes(b"gz")
        return path
    return _make


def _task(count, overrides, tmp_path):
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task.dry_run = False
    task.realtime_storage_class = "STANDARD"
    task.manifest = {"defaults": {"realtime": {"storage_class": "STANDARD"}}}
    task._overrides = overrides
    task.audit_buf = []
    task.s3 = _S3()
    task.cursor = _Cursor(count)
    task._cursor = lambda: task.cursor
    task._copy_table_to_gzip = _gz_file(tmp_path)
    task._find_time_column = lambda schema, table: (_ for _ in ()).throw(AssertionError("不應找時間欄"))
    return task


def test_full_table_uploads_whole_table_to_daily_key(tmp_path):
    task = _task(5, {"live.ship_last_seen": {"cadence": "full_table", "storage_class": "GLACIER_IR"}}, tmp_path)
    assert task._process_realtime_table("live", "ship_last_seen", date(2026, 10, 15)) == "uploaded"
    assert task.s3.uploads == [
        ("supabase-snapshots/realtime/2026-10-15/realtime.ship_last_seen.csv.gz", b"gz", "GLACIER_IR")
    ]
    assert not any("WHERE" in s for s in task.cursor.sql)
    assert task.audit_buf[-1]["code"] == "snapshot_uploaded"
    import json
    details = task.audit_buf[-1]["details"]
    assert (json.loads(details) if isinstance(details, str) else details)["size_bytes"] == 2
    # 與舊版 upload_snapshot 相同的 S3 標頭；暫存檔上傳後已刪
    assert task.s3.headers == [("text/csv", "gzip")]
    assert list(tmp_path.glob("backup-*")) == []


def test_full_table_empty_table_uploads_header_only_snapshot(tmp_path):
    task = _task(0, {"live.ship_last_seen": {"cadence": "full_table"}}, tmp_path)
    assert task._process_realtime_table("live", "ship_last_seen", date(2026, 10, 15)) == "uploaded"
    assert len(task.s3.uploads) == 1
    assert task.audit_buf[-1]["code"] == "snapshot_uploaded"


def test_manifest_backs_up_ship_last_seen_as_full_table():
    repo = Path(__file__).resolve().parent.parent
    manifest = yaml.safe_load((repo / "config" / "backup_manifest.yaml").read_text(encoding="utf-8"))
    assert manifest["overrides"]["live.ship_last_seen"]["cadence"] == "full_table"
    assert "live.ship_last_seen" not in manifest["exclude"]
    assert "live.ship_last_seen" not in manifest["archive_py_covered"]


# ── 串流 COPY → gzip 暫存檔 ─────────────────────────────────────

class _CopyCursor:
    def __init__(self, payload, fail=False):
        self.payload = payload
        self.fail = fail

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def mogrify(self, sql, params):
        return sql.replace("%s", "'x'").encode()

    def copy_expert(self, sql, fh):
        for i in range(0, len(self.payload), 7):
            fh.write(self.payload[i:i + 7])
        if self.fail:
            raise RuntimeError("COPY 中斷")


def _stream_task(tmp_path, monkeypatch, payload, fail=False):
    import config
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task._cursor = lambda: _CopyCursor(payload, fail)
    task.s3 = _S3()
    return task


def test_copy_streams_to_gzip_file_with_same_content(tmp_path, monkeypatch):
    import gzip
    payload = b"id,name\n" + b"".join(b"%d,row%d\n" % (i, i) for i in range(5000))
    task = _stream_task(tmp_path, monkeypatch, payload)
    path = task._copy_table_to_gzip("reference", "x")
    assert path.parent == tmp_path / "tmp" / "backup"
    assert gzip.decompress(path.read_bytes()) == payload
    # header 不帶檔名（與舊版 GzipFile(fileobj=BytesIO) 一致）
    assert not (path.read_bytes()[3] & 0x08)
    size = task._upload_gz_file(path, "supabase-snapshots/static/reference.x.csv.gz", "GLACIER_IR")
    assert size == len(task.s3.uploads[0][1])
    assert not path.exists()


def test_copy_failure_removes_tmp_file(tmp_path, monkeypatch):
    task = _stream_task(tmp_path, monkeypatch, b"a,b\n1,2\n", fail=True)
    import pytest
    with pytest.raises(RuntimeError):
        task._copy_table_partition_to_gzip("live", "t", "observed_at", None, None)
    assert list((tmp_path / "tmp" / "backup").iterdir()) == []


def test_upload_failure_raises_and_removes_tmp(tmp_path, monkeypatch):
    task = _stream_task(tmp_path, monkeypatch, b"a\n")
    task.s3.upload_path = lambda *a, **k: False
    path = task._copy_table_to_gzip("reference", "x")
    import pytest
    with pytest.raises(RuntimeError, match="回傳 False"):
        task._upload_gz_file(path, "k", "STANDARD")
    assert not path.exists()


def test_stale_tmp_from_killed_run_is_cleaned(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    d = tmp_path / "tmp" / "backup"
    d.mkdir(parents=True)
    (d / "backup-abc.csv.gz").write_bytes(b"x")
    (d / "keep.txt").write_bytes(b"x")
    assert BackupSupabaseTask._cleanup_stale_tmp() == 1
    assert [p.name for p in d.iterdir()] == ["keep.txt"]
