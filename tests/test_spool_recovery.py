"""孤兒 spool .jsonl 恢復（aisstream / ripe_ris_live 共用）的單元測試。"""

import gzip
import os
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from tests.test_aisstream_worker import _FakeS3 as _AisFakeS3
from utils.spool_recovery import compress_orphan_jsonl
from workers.aisstream import SpoolManager
from workers.ripe_ris_live import RipeRisSpoolManager

OLD_TS = datetime(2026, 8, 25, 3, 30, tzinfo=timezone.utc).timestamp()  # date=2026-08-25 hour=03


def _write(path, text='{"a":1}\n', mtime=None):
    path.write_text(text, encoding="utf-8")
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


def test_compress_orphan_preserves_mtime_and_skips_active(tmp_path):
    orphan = _write(tmp_path / "a.jsonl", mtime=OLD_TS)
    active = _write(tmp_path / "active.jsonl", mtime=OLD_TS)
    assert compress_orphan_jsonl(tmp_path, active) == 1
    gz = tmp_path / "a.jsonl.gz"
    assert not orphan.exists() and gz.exists()
    assert gzip.open(gz, "rt").read() == '{"a":1}\n'
    assert int(gz.stat().st_mtime) == int(OLD_TS)
    # 正在寫入的檔完全沒被動
    assert active.read_text() == '{"a":1}\n'
    assert not (tmp_path / "active.jsonl.gz").exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_compress_orphan_overwrites_stale_partial_gz_and_drops_empty(tmp_path):
    _write(tmp_path / "b.jsonl", "line\n", mtime=OLD_TS)
    (tmp_path / "b.jsonl.gz").write_bytes(b"partial-garbage")
    (tmp_path / "empty.jsonl").write_bytes(b"")
    assert compress_orphan_jsonl(tmp_path, None) == 1
    assert gzip.open(tmp_path / "b.jsonl.gz", "rt").read() == "line\n"
    assert not (tmp_path / "empty.jsonl").exists()


def test_compress_orphan_failure_keeps_source(tmp_path, monkeypatch):
    src = _write(tmp_path / "c.jsonl", mtime=OLD_TS)

    def boom(*_a, **_k):
        raise OSError("disk full")

    monkeypatch.setattr("utils.spool_recovery.os.replace", boom)
    assert compress_orphan_jsonl(tmp_path, None) == 0
    assert src.exists()
    assert not list(tmp_path.glob("*.tmp"))


def _only_key(s3):
    keys = [k for k in s3.s3.objects if k.endswith(".jsonl.gz")]
    assert len(keys) == 1
    return keys[0]


def test_aisstream_retry_pending_uploads_orphan_with_original_partition(tmp_path):
    s3 = _AisFakeS3()
    spool = SpoolManager(tmp_path, s3)
    orphan = _write(spool.base_dir / "aisstream-old.jsonl", mtime=OLD_TS)
    spool.append({"MessageType": "PositionReport"}, "2026-11-06T00:00:00+00:00")  # 正在寫入的檔
    active = spool._path
    assert spool.retry_pending() == 1
    assert not orphan.exists()
    assert "date=2026-08-25/hour=03/" in _only_key(s3)
    assert active.exists() and spool._file is not None  # 沒碰正在寫的檔
    assert not (spool.base_dir / (active.name + ".gz")).exists()


def test_aisstream_upload_failure_keeps_gz_for_next_round(tmp_path):
    s3 = _AisFakeS3()
    spool = SpoolManager(tmp_path, s3)
    _write(spool.base_dir / "aisstream-old.jsonl", mtime=OLD_TS)
    real_upload = s3.s3.upload_file

    def fail(*_a, **_k):
        raise RuntimeError("network")

    s3.s3.upload_file = fail
    assert spool.retry_pending() == 0
    gz = spool.base_dir / "aisstream-old.jsonl.gz"
    assert gz.exists() and int(gz.stat().st_mtime) == int(OLD_TS)
    s3.s3.upload_file = real_upload
    assert spool.retry_pending() == 1
    assert not gz.exists()
    assert "date=2026-08-25/hour=03/" in _only_key(s3)


class _RipeFakeS3Client:
    def __init__(self):
        self.objects = {}

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        value, metadata = self.objects[Key]
        return {"ContentLength": len(value), "Metadata": metadata}

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.objects[key] = (open(path, "rb").read(), ExtraArgs.get("Metadata", {}))

    def put_object(self, *, Bucket, Key, Body, Metadata, **_kw):
        self.objects[Key] = (Body, Metadata)

    def get_object(self, *, Bucket, Key):
        value = self.objects[Key][0]
        return {"Body": type("B", (), {"read": lambda self: value})()}


class _RipeFakeS3:
    bucket = "test"
    ClientError = ClientError

    def __init__(self):
        self.s3 = _RipeFakeS3Client()


def test_ripe_retry_pending_uploads_orphan_and_spares_active_file(tmp_path):
    s3 = _RipeFakeS3()
    spool = RipeRisSpoolManager(tmp_path, s3, subscription_hash="h")
    orphan = _write(spool.base_dir / "ripe-ris-old.jsonl", mtime=OLD_TS)
    spool.append({"type": "ris_message"}, "2026-11-06T00:00:00+00:00")
    active = spool._path
    assert spool.retry_pending() == 1
    assert not orphan.exists()
    assert "date=2026-08-25/hour=03/" in _only_key(s3)
    assert active.exists() and spool._file is not None
