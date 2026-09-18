import json
import gzip
import hashlib
import io
import tarfile

import config
from tasks.archive import ArchiveTask
from storage.s3 import S3Storage


class FakeS3:
    def __init__(self, status='verified', identity=None): self.status, self.identity = status, identity or {'ETag': 'one', 'VersionId': None, 'ContentLength': 1}
    def verify_archive(self, *_args): return {'status': self.status, 'identity': self.identity} if self.status == 'verified' else {'status': self.status}
    def upload_archive(self, *_args): return True
    def archive_identity(self, *_args): return {'status': 'present', 'identity': self.identity}


def make_task(monkeypatch, tmp_path, status='verified'):
    monkeypatch.setattr(config, 'LOCAL_DATA_DIR', tmp_path)
    monkeypatch.setattr(config, 'ARCHIVE_RETENTION_DAYS', 0)
    task = ArchiveTask.__new__(ArchiveTask); task.s3 = FakeS3(status)
    return task


def make_day(tmp_path):
    path = tmp_path / 'demo/2020/01/02'; path.mkdir(parents=True)
    (path / 'demo.json').write_text(json.dumps({'ok': True}))
    return path


def test_verified_archive_receipt_allows_cleanup(monkeypatch, tmp_path):
    task = make_task(monkeypatch, tmp_path); path = make_day(tmp_path)
    assert task._archive_to_s3() == {'uploaded': 0, 'skipped': 1, 'failed': 0}
    assert task._cleanup_local() == {'deleted': 1}
    assert not path.exists()


def test_wrong_archive_or_receipt_failure_keeps_raw(monkeypatch, tmp_path):
    task = make_task(monkeypatch, tmp_path, 'mismatch'); path = make_day(tmp_path)
    assert task._archive_to_s3()['failed'] == 1
    assert task._cleanup_local() == {'deleted': 0}
    assert path.exists()
    task = make_task(monkeypatch, tmp_path); task._write_receipt = lambda *_args: False
    assert task._archive_to_s3()['failed'] == 1
    assert path.exists()


def test_unknown_remote_verification_fails_closed(monkeypatch, tmp_path):
    task = make_task(monkeypatch, tmp_path, 'unknown'); path = make_day(tmp_path)
    assert task._archive_to_s3()['failed'] == 1
    assert task._cleanup_local() == {'deleted': 0}
    assert path.exists()


def test_remote_identity_change_rejects_cleanup(monkeypatch, tmp_path):
    task = make_task(monkeypatch, tmp_path); path = make_day(tmp_path)
    task._archive_to_s3()
    task.s3.identity = {'ETag': 'changed', 'VersionId': None, 'ContentLength': 1}
    assert task._cleanup_local() == {'deleted': 0}
    assert path.exists()


def test_existing_receipt_cleans_when_disk_check_would_fail(monkeypatch, tmp_path):
    task = make_task(monkeypatch, tmp_path); path = make_day(tmp_path)
    task._archive_to_s3(); task._has_archive_space = lambda *_args: False
    assert task._cleanup_local() == {'deleted': 1}
    assert not path.exists()


def test_unknown_date_file_blocks_cleanup(monkeypatch, tmp_path):
    task = make_task(monkeypatch, tmp_path); path = make_day(tmp_path)
    task._archive_to_s3(); (path / 'not-archived.txt').write_text('keep')
    assert task._cleanup_local() == {'deleted': 0}
    assert path.exists()


def test_legacy_gzip_header_does_not_change_member_verification():
    body = io.BytesIO()
    with gzip.GzipFile(fileobj=body, mode='wb', mtime=123) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as archive:
            info = tarfile.TarInfo('demo.json'); payload = b'{"ok":true}'; info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    class Client:
        def head_object(self, **_): return {'ETag': 'legacy', 'ContentLength': len(body.getvalue())}
        def get_object(self, **_): return {'Body': io.BytesIO(body.getvalue())}
    storage = S3Storage.__new__(S3Storage); storage.s3 = Client(); storage.bucket = 'test'
    assert storage.verify_archive('demo/archives/2020-01-02.tar.gz', [{'name': 'demo.json', 'sha256': hashlib.sha256(payload).hexdigest(), 'bytes': len(payload)}])['status'] == 'verified'
