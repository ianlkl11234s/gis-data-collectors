"""Spool 孤兒檔恢復：把沒被 rotate 到的 ``*.jsonl`` 壓成 ``*.jsonl.gz``。

worker 被 kill / 重啟時，當下正在寫的 ``.jsonl`` 不會經過 ``rotate()``，
也不會被只找 ``*.jsonl.gz`` 的 ``retry_pending()`` 看到，永遠不上傳。
本函式負責把這類孤兒檔壓縮成 .gz，之後交給既有的上傳 / 驗證 / 刪檔流程。
"""

from __future__ import annotations

import gzip
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def compress_orphan_jsonl(spool_dir: Path, active_path: Path | None = None) -> int:
    """壓縮 ``spool_dir`` 內所有不是 ``active_path`` 的 ``*.jsonl``，回傳處理檔數。

    - 保留原檔 mtime（上傳端依 mtime 決定 S3 的 date=/hour= 分區）。
    - 先寫 ``.gz.tmp`` 再 ``os.replace``，崩潰時不會留下半截的 ``.jsonl.gz``；
      同名 .gz 若已存在，以 .jsonl 為準覆蓋（.gz 只在上傳驗證完才會被刪，存在代表未上傳）。
    - 0 byte 空檔沒有資料，直接移除。
    - 任何一檔失敗只記 warning 並保留原檔，不影響其他檔。
    """
    active = active_path.resolve() if active_path else None
    processed = 0
    for source in sorted(spool_dir.glob("*.jsonl")):
        if not source.is_file() or (active is not None and source.resolve() == active):
            continue
        gz_path = source.with_suffix(source.suffix + ".gz")
        tmp_path = gz_path.with_suffix(gz_path.suffix + ".tmp")
        try:
            stat = source.stat()
            if stat.st_size == 0:
                source.unlink(missing_ok=True)
                continue
            with source.open("rb") as src, gzip.open(tmp_path, "wb", compresslevel=6) as dst:
                while chunk := src.read(1024 * 1024):
                    dst.write(chunk)
            os.utime(tmp_path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            os.replace(tmp_path, gz_path)
            source.unlink(missing_ok=True)
            processed += 1
        except Exception as exc:
            logger.warning("Spool orphan compress failed (%s): %s", source.name, exc)
            tmp_path.unlink(missing_ok=True)
    return processed
