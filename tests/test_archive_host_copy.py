"""tasks/archive.py：S3 已有同 key、內容不同時，本機另存成 <date>.<INSTANCE_NAME>.tar.gz。

2026-11-07 correctional_daily_snapshot 搬到 HiCloud VM 當天兩台都跑過：pulse-host 先上傳 11-07 歸檔，
VM 本機的 11-07 內容不同 → verify mismatch → 不寫 receipt → 清理永遠不刪。這裡鎖住修正後的行為。
"""
import io
import json
import tarfile

import config
from scripts import backfill_tra_delay_from_s3, gis_collectors_monitor_snapshot
from storage.s3 import S3Storage
from tasks import monitoring
from tasks.archive import ArchiveTask, archive_key, host_label, is_host_copy_key

PRIMARY = 'demo/archives/2020-01-02.tar.gz'
HOST_COPY = 'demo/archives/2020-01-02.hicloud-vm.tar.gz'
REMOTE_MEMBERS = [{'name': 'other.json', 'sha256': '0' * 64, 'bytes': 3}]


class KeyedS3:
    """依 key 記內容的假 S3：verify 比對成員清單，upload 讀實際 tar 的成員。"""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.uploads = []

    def _identity(self, key):
        return {'ETag': f'etag-{key}-{len(self.objects[key])}', 'VersionId': None, 'ContentLength': 1}

    def archive_identity(self, key):
        return {'status': 'present', 'identity': self._identity(key)} if key in self.objects else {'status': 'missing'}

    def verify_archive(self, key, members):
        if key not in self.objects:
            return {'status': 'missing'}
        return {'status': 'verified', 'identity': self._identity(key)} if self.objects[key] == members else {'status': 'mismatch'}

    def upload_archive(self, path, key):
        task = ArchiveTask.__new__(ArchiveTask)
        with tarfile.open(path, 'r:gz') as tar:
            names = [m.name for m in tar.getmembers()]
        self.uploads.append(key)
        self.objects[key] = [m for m in task._member_manifest(self.day) if m['name'] in names]
        return True


def setup(monkeypatch, tmp_path, instance='hicloud-vm', objects=None):
    monkeypatch.setattr(config, 'LOCAL_DATA_DIR', tmp_path)
    monkeypatch.setattr(config, 'ARCHIVE_RETENTION_DAYS', 0)
    monkeypatch.setattr(config, 'INSTANCE_NAME', instance)
    day = tmp_path / 'demo/2020/01/02'; day.mkdir(parents=True)
    (day / 'local.json').write_text(json.dumps({'from': 'vm'}))
    s3 = KeyedS3({PRIMARY: REMOTE_MEMBERS} if objects is None else objects); s3.day = day
    task = ArchiveTask.__new__(ArchiveTask); task.s3 = s3
    return task, s3, day


def test_conflict_saves_host_copy_and_keeps_original(monkeypatch, tmp_path):
    task, s3, day = setup(monkeypatch, tmp_path)
    assert task._archive_to_s3() == {'uploaded': 1, 'skipped': 0, 'failed': 0}
    assert s3.uploads == [HOST_COPY]                       # 原 key 從沒被上傳
    assert s3.objects[PRIMARY] == REMOTE_MEMBERS           # 原檔內容不變
    assert [m['name'] for m in s3.objects[HOST_COPY]] == ['local.json']
    receipt = json.loads((tmp_path / '.archive-receipts/demo/2020-01-02.json').read_text())
    assert receipt['s3_key'] == HOST_COPY


def test_host_copy_receipt_lets_cleanup_delete(monkeypatch, tmp_path):
    task, s3, day = setup(monkeypatch, tmp_path)
    task._archive_to_s3()
    assert task._archive_to_s3() == {'uploaded': 0, 'skipped': 1, 'failed': 0}  # 第二輪不重傳
    assert s3.uploads == [HOST_COPY]
    assert task._cleanup_local() == {'deleted': 1}
    assert not day.exists()


def test_host_copy_cleanup_still_checks_remote_identity(monkeypatch, tmp_path):
    task, s3, day = setup(monkeypatch, tmp_path)
    task._archive_to_s3()
    del s3.objects[HOST_COPY]                               # 另存檔不見了 → 不能刪本機
    assert task._cleanup_local() == {'deleted': 0}
    assert day.exists()


def test_conflict_without_instance_name_fails_closed(monkeypatch, tmp_path):
    task, s3, day = setup(monkeypatch, tmp_path, instance='')
    assert task._archive_to_s3() == {'uploaded': 0, 'skipped': 0, 'failed': 1}
    assert s3.uploads == []
    assert task._cleanup_local() == {'deleted': 0}
    assert day.exists()


def test_host_copy_mismatch_is_not_overwritten(monkeypatch, tmp_path):
    task, s3, day = setup(monkeypatch, tmp_path, objects={PRIMARY: REMOTE_MEMBERS, HOST_COPY: REMOTE_MEMBERS})
    assert task._archive_to_s3() == {'uploaded': 0, 'skipped': 0, 'failed': 1}
    assert s3.uploads == []
    assert s3.objects[HOST_COPY] == REMOTE_MEMBERS
    assert task._cleanup_local() == {'deleted': 0}
    assert day.exists()


def test_no_conflict_still_uses_primary_key(monkeypatch, tmp_path):
    task, s3, day = setup(monkeypatch, tmp_path, objects={})
    assert task._archive_to_s3() == {'uploaded': 1, 'skipped': 0, 'failed': 0}
    assert s3.uploads == [PRIMARY]
    assert task._cleanup_local() == {'deleted': 1}


def test_host_label_and_key_shape(monkeypatch):
    for raw, want in (('hicloud-vm', 'hicloud-vm'), ('主站', '主站'), ('hicloud vm/1.2', 'hicloud-vm-1-2'), ('', ''), ('..', '')):
        monkeypatch.setattr(config, 'INSTANCE_NAME', raw)
        assert host_label() == want
    assert archive_key('demo', '2020-01-02') == PRIMARY
    assert archive_key('demo', '2020-01-02', 'hicloud-vm') == HOST_COPY
    assert is_host_copy_key(HOST_COPY) and is_host_copy_key('x/archives/2020-01-02.主站.tar.gz')
    assert not is_host_copy_key(PRIMARY)


# ── 下游：讀歸檔的程式只認 YYYY-MM-DD.tar.gz，另存檔一律略過 ──

class _Paginator:
    def __init__(self, keys): self.keys = keys
    def paginate(self, **kw):
        if kw.get('Delimiter'):
            return [{'CommonPrefixes': []}]
        return [{'Contents': [{'Key': k} for k in self.keys if k.startswith(kw['Prefix'])]}]


class _Client:
    def __init__(self, keys): self.keys = keys
    def get_paginator(self, name): return _Paginator(self.keys)


def test_downstream_list_dates_ignores_host_copy():
    s3 = S3Storage.__new__(S3Storage); s3.bucket = 'b'
    s3.s3 = _Client(['demo/archives/2020-01-01.tar.gz', 'demo/archives/2020-01-03.hicloud-vm.tar.gz'])
    assert s3.list_dates('demo') == ['2020-01-01']


def test_downstream_monitoring_heartbeat_ignores_host_copy(monkeypatch):
    class Storage:
        def __init__(self): self.s3 = _Client(['demo/archives/2020-01-01.tar.gz', 'demo/archives/2020-01-03.hicloud-vm.tar.gz'])
    import storage.s3
    monkeypatch.setattr(monitoring.config, 'S3_BUCKET', 'test')
    monkeypatch.setattr(storage.s3, 'S3Storage', Storage)
    monkeypatch.setattr(monitoring, 'load_cross_layer_map', lambda: {
        'demo': {'s3_prefixes': [{'prefix': 'demo/archives/', 'expected_daily': True}]}})
    assert monitoring.list_archive_dates_per_collector() == {'demo': '2020-01-01'}


def test_downstream_scripts_ignore_host_copy():
    assert gis_collectors_monitor_snapshot.parse_archive_date(HOST_COPY) is None
    assert gis_collectors_monitor_snapshot.parse_archive_date(PRIMARY) == '2020-01-02'
    assert backfill_tra_delay_from_s3.ARCHIVE_RE.match(HOST_COPY.rsplit('/', 1)[-1]) is None


def test_archive_status_counts_only_primary(monkeypatch, tmp_path):
    monkeypatch.setattr(config, 'LOCAL_DATA_DIR', tmp_path)
    (tmp_path / 'demo').mkdir()

    class S3:
        def list_files(self, prefix):
            return [{'key': PRIMARY, 'size': 10}, {'key': HOST_COPY, 'size': 99}]
    task = ArchiveTask.__new__(ArchiveTask); task.s3 = S3()
    demo = task.get_archive_status()['collectors'][0]
    assert demo['s3_archives'] == 1 and demo['s3_size_mb'] == 0.0
