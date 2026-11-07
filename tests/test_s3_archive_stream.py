"""storage.s3：API 讀歸檔改串流（不整包讀進記憶體），輸出與舊版相同。"""
import io
import tarfile

import pytest

from storage.s3 import S3Storage


class _ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Body(io.BytesIO):
    reads = 0

    def read(self, *args):
        if not args or args[0] is None or args[0] < 0:
            raise AssertionError("不可整包 read()")
        return super().read(*args)


class _Client:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _ClientError("NoSuchKey")
        data = self.objects[Key]
        return {"Body": _Body(data), "ContentLength": len(data)}


def _tar(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 1_700_000_000
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _storage(objects):
    s = S3Storage.__new__(S3Storage)
    s.s3 = _Client(objects)
    s.bucket = "b"
    s.ClientError = _ClientError
    return s


KEY = "bus/archives/2026-11-06.tar.gz"


def test_list_archive_files_streams_and_sorts():
    s = _storage({KEY: _tar({"bus_0100.json": b"{}", "bus_0200.json": b"[1]"})})
    files = s.list_archive_files("bus", "2026-11-06")
    assert [f["filename"] for f in files] == ["bus_0200.json", "bus_0100.json"]
    assert files[0]["size"] == 3 and files[0]["source"] == "s3_archive"


def test_extract_file_from_archive_returns_member_or_none():
    s = _storage({KEY: _tar({"a.json": b"A", "b.json": b"BB"})})
    assert s.extract_file_from_archive("bus", "2026-11-06", "b.json") == b"BB"
    assert s.extract_file_from_archive("bus", "2026-11-06", "zzz.json") is None


def test_missing_or_empty_archive():
    s = _storage({"bus/archives/2026-11-05.tar.gz": b""})
    assert s.list_archive_files("bus", "2026-11-04") == []
    assert s.extract_file_from_archive("bus", "2026-11-04", "x") is None
    assert s.list_archive_files("bus", "2026-11-05") == []
    assert s.extract_file_from_archive("bus", "2026-11-05", "x") is None


def test_other_errors_propagate():
    s = _storage({})
    s.s3.get_object = lambda **k: (_ for _ in ()).throw(_ClientError("AccessDenied"))
    with pytest.raises(_ClientError):
        s.list_archive_files("bus", "2026-11-06")
