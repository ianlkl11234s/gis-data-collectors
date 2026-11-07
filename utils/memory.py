"""主程序記憶體歸還工具。

glibc 在 free 之後常把空頁留在各執行緒的 arena 裡，RSS 不會下降；
malloc_trim(0) 會把所有 arena 中段的空頁用 madvise 還給 OS（glibc 2.8+）。
非 Linux／找不到 glibc（例如 macOS、musl）時安靜跳過。
"""

from __future__ import annotations

import ctypes
import ctypes.util
import logging
import sys

logger = logging.getLogger(__name__)

_libc = None
_libc_loaded = False


def _load_libc():
    global _libc, _libc_loaded
    if _libc_loaded:
        return _libc
    _libc_loaded = True
    if not sys.platform.startswith("linux"):
        return None
    try:
        lib = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        lib.malloc_trim.argtypes = [ctypes.c_size_t]
        lib.malloc_trim.restype = ctypes.c_int
        _libc = lib
    except (OSError, AttributeError) as exc:
        logger.info("malloc_trim 不可用，略過：%s", exc)
        _libc = None
    return _libc


def rss_kb() -> int | None:
    """目前行程的 VmRSS（kB）；讀不到回 None。"""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def malloc_trim() -> bool:
    """呼叫 glibc malloc_trim(0)；不可用時回 False。成功時記錄前後 RSS。"""
    lib = _load_libc()
    if lib is None:
        return False
    before = rss_kb()
    try:
        lib.malloc_trim(0)
    except Exception as exc:  # pragma: no cover - 防禦
        logger.info("malloc_trim 失敗：%s", exc)
        return False
    after = rss_kb()
    if before is not None and after is not None:
        logger.info("malloc_trim: VmRSS %d → %d MB", before // 1024, after // 1024)
    return True
