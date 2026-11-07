"""utils.streaming.download_to_file：串流寫檔、上限、失敗不留檔（不連網）。"""
import hashlib

import pytest

from utils.streaming import DownloadTooLarge, download_to_file


class _Resp:
    def __init__(self, chunks, status=200, headers=None):
        self.chunks = chunks
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def iter_content(self, chunk_size):
        yield from self.chunks

    @property
    def content(self):  # pragma: no cover - 不應被呼叫
        raise AssertionError("不可整包讀 content")


class _Session:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.resp


def test_streams_chunks_to_named_file(tmp_path):
    chunks = [b"abc", b"", b"def"]
    s = _Session(_Resp(chunks, headers={"Content-Type": "application/zip"}))
    r = download_to_file(s, "https://x/y", dest_dir=tmp_path, filename="y.zip", params={"a": 1})
    assert r.path == tmp_path / "y.zip" and r.path.read_bytes() == b"abcdef"
    assert r.size == 6 and r.sha256 == hashlib.sha256(b"abcdef").hexdigest()
    assert r.content_type == "application/zip"
    assert s.calls[0][1]["stream"] is True and s.calls[0][1]["params"] == {"a": 1}
    assert [p.name for p in tmp_path.iterdir()] == ["y.zip"]


def test_max_bytes_by_header_and_by_body_leave_no_file(tmp_path):
    with pytest.raises(DownloadTooLarge):
        download_to_file(_Session(_Resp([b"x"], headers={"Content-Length": "100"})),
                         "u", dest_dir=tmp_path, max_bytes=10)
    with pytest.raises(DownloadTooLarge):
        download_to_file(_Session(_Resp([b"x" * 6, b"x" * 6])), "u", dest_dir=tmp_path, max_bytes=10)
    assert list(tmp_path.iterdir()) == []


def test_http_error_leaves_no_file(tmp_path):
    with pytest.raises(RuntimeError, match="HTTP 503"):
        download_to_file(_Session(_Resp([], status=503)), "u", dest_dir=tmp_path, filename="f")
    assert list(tmp_path.iterdir()) == []
