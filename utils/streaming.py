"""大檔下載共用 helper：串流寫到磁碟，不整檔讀進記憶體。

**大檔 collector 怎麼接**（回應可能超過數 MB 就用這個，不要用 resp.content／resp.json()）：

    from utils.streaming import download_to_file

    session = requests.Session()          # 建在 collector __init__，每輪重用（連線池）
    result = download_to_file(session, url, dest_dir=config.LOCAL_DATA_DIR / "tmp" / "my_src",
                              timeout=(10, 120), max_bytes=2 * 1024**3)
    try:
        with open(result.path, "rb") as fh:  # 解析也盡量串流：ijson、csv.reader、tarfile 'r|gz'
            ...
    finally:
        result.path.unlink(missing_ok=True)  # 或 rename 到正式位置

- 寫到同目錄的暫存檔，完成才 rename（中途失敗不留半成品，暫存檔也會刪）。
- max_bytes：超過就中止並刪檔，避免上游異常把磁碟灌滿。
- 回傳 size 與 sha256，可直接拿去做冪等上傳（storage.s3.put_verified）。
- 只給「低頻大檔」也要搭配 utils.isolated_task 嗎？下載本身串流後記憶體很小，
  不需要子行程；只有「解析／轉檔」尖峰大時才把整個任務放進 isolated_task。
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CHUNK = 1024 * 1024


class DownloadTooLarge(RuntimeError):
    """下載內容超過 max_bytes。"""


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    size: int
    sha256: str
    content_type: str | None


def download_to_file(
    session,
    url: str,
    *,
    dest_dir: Path | str,
    filename: str | None = None,
    timeout=(10, 120),
    max_bytes: int | None = None,
    chunk_size: int = DEFAULT_CHUNK,
    **request_kwargs,
) -> DownloadResult:
    """GET url（stream=True）逐塊寫入 dest_dir，回傳檔案資訊。

    Args:
        session: requests.Session（或任何有 .get(url, stream=True, ...) 的物件）
        filename: 最終檔名；None 時保留暫存檔名（呼叫端自行搬移／刪除）
        max_bytes: 上限；Content-Length 或實際寫入超過即丟 DownloadTooLarge
        request_kwargs: 轉給 session.get（headers、params…）
    Raises:
        requests.HTTPError、DownloadTooLarge、OSError；失敗時不留任何檔案
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".download-", suffix=".part", dir=dest)
    tmp = Path(tmp_name)
    digest = hashlib.sha256()
    size = 0
    try:
        with session.get(url, stream=True, timeout=timeout, **request_kwargs) as resp:
            resp.raise_for_status()
            declared = resp.headers.get("Content-Length")
            if max_bytes is not None and declared and declared.isdigit() and int(declared) > max_bytes:
                raise DownloadTooLarge(f"Content-Length {declared} > max_bytes {max_bytes}")
            content_type = resp.headers.get("Content-Type")
            with os.fdopen(fd, "wb") as fh:
                fd = None
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    size += len(chunk)
                    if max_bytes is not None and size > max_bytes:
                        raise DownloadTooLarge(f"下載超過 max_bytes {max_bytes}")
                    digest.update(chunk)
                    fh.write(chunk)
        final = dest / filename if filename else tmp
        if filename:
            os.replace(tmp, final)
        return DownloadResult(path=final, size=size, sha256=digest.hexdigest(), content_type=content_type)
    except BaseException:
        if fd is not None:
            os.close(fd)
        tmp.unlink(missing_ok=True)
        raise
