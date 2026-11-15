"""發布：R2 公開網域 data.itsmigu.com 的 `sound-atlas/v1/` 前綴（物件 key 規則見 DESIGN §4）。

預設只寫本機目錄（dry-run）；`SOUND_ATLAS_UPLOAD_ENABLED=true` 且 R2 四個憑證都在才真的上傳。
分片與每日檔都不可變（Cache-Control immutable）；同 key 已存在就不覆寫（R2 head 檢查），避免瀏覽器 4 小時快取拿到舊版。
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from .common import dumps_compact, gzip_bytes

log = logging.getLogger(__name__)

PREFIX = "sound-atlas/v1"
IMMUTABLE = "public, max-age=31536000, immutable"
SHORT = "public, max-age=60"


def live_slot_key(date: str, k: int) -> str:
    return f"{PREFIX}/live/{date}/slot-{k:03d}.json"


def live_hour_key(date: str, hour: int) -> str:
    return f"{PREFIX}/live/{date}/hour-{hour:02d}.json"


def live_baseline_key(date: str) -> str:
    return f"{PREFIX}/live/{date}/river-baseline.json"


def live_index_key(date: str) -> str:
    return f"{PREFIX}/live/{date}/index.json"


def daily_key(date: str, name: str) -> str:
    return f"{PREFIX}/daily/{date}/{name}"


def static_key(name: str, sha256: str) -> str:
    stem, ext = name.rsplit(".", 1)
    return f"{PREFIX}/static/{stem}-{sha256[:8]}.{ext}"


class Sink:
    """put(key, body, kind)；kind：json（gzip＋Content-Encoding）、bin（原樣）。immutable=False 只給 index。"""

    def exists(self, key: str) -> bool:  # pragma: no cover - 介面
        raise NotImplementedError

    def put(self, key: str, body: bytes, kind: str = "json", immutable: bool = True) -> bool:  # pragma: no cover
        raise NotImplementedError

    def put_json(self, key: str, doc, immutable: bool = True) -> int:
        b = dumps_compact(doc)
        self.put(key, b, "json", immutable)
        return len(b)


class LocalDirSink(Sink):
    """dry-run：寫到本機目錄（key 當相對路徑）；json 存 gzip 後的位元組（與上傳內容相同），副檔名加 .gz 方便檢視"""

    def __init__(self, root: str):
        self.root = Path(root)

    def _path(self, key: str, kind: str) -> Path:
        return self.root / (key + (".gz" if kind == "json" else ""))

    def exists(self, key: str) -> bool:
        return self._path(key, "json").exists() or self._path(key, "bin").exists()

    def put(self, key: str, body: bytes, kind: str = "json", immutable: bool = True) -> bool:
        p = self._path(key, kind)
        if immutable and p.exists():
            return False
        p.parent.mkdir(parents=True, exist_ok=True)
        data = gzip_bytes(body) if kind == "json" else body
        tmp = p.with_suffix(p.suffix + ".part")
        tmp.write_bytes(data)
        tmp.replace(p)
        return True

    def read_json(self, key: str):
        import gzip
        p = self._path(key, "json")
        return json.loads(gzip.decompress(p.read_bytes())) if p.exists() else None


class R2Sink(Sink):
    """正式上傳（storage/r2.R2Storage）。預設關閉：只有 SOUND_ATLAS_UPLOAD_ENABLED=true 才會建立。"""

    def __init__(self, r2):
        self.r2 = r2

    def exists(self, key: str) -> bool:
        return self.r2.head(key) is not None

    def put(self, key: str, body: bytes, kind: str = "json", immutable: bool = True) -> bool:
        if immutable and self.exists(key):
            log.warning("[sound_atlas] %s 已存在，不覆寫（不可變物件）", key)
            return False
        extra = {}
        if kind == "json":
            body = gzip_bytes(body)
            extra = {"ContentType": "application/json; charset=utf-8", "ContentEncoding": "gzip"}
        else:
            extra = {"ContentType": "application/octet-stream"}
        self.r2.s3.put_object(Bucket=self.r2.bucket, Key=key, Body=body, CacheControl=IMMUTABLE if immutable else SHORT, **extra)
        return True


def make_sink(upload_enabled: bool, local_root: str) -> Sink:
    if upload_enabled:
        from storage.r2 import get_r2_storage
        r2 = get_r2_storage()
        if r2 is None:
            log.warning("[sound_atlas] SOUND_ATLAS_UPLOAD_ENABLED=true 但 R2 憑證不齊，改寫本機 %s", local_root)
        else:
            return R2Sink(r2)
    return LocalDirSink(local_root)


def describe(sink: Optional[Sink]) -> str:
    return "r2" if isinstance(sink, R2Sink) else ("local" if isinstance(sink, LocalDirSink) else "none")
