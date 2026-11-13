#!/usr/bin/env python3
"""VM 單檔 collector 共用的本地 buffer + 連線 retry 小模組。

主容器（storage/supabase_writer.py）的寫入路徑有 pool + 斷路器 + 本地 buffer；
VM 上的單檔 collector 是刻意的精簡版，原本 DB 寫入失敗只 log + exit，
該輪資料直接丟。本模組補上最小安全網：

  save_batch()          DB 寫入失敗時把該輪資料存成本地 JSON 檔
  flush_pending()       每輪開頭先補寫積壓檔（成功才刪檔）
  connect_with_retry()  連線失敗 retry（預設 2 次、間隔 5s）
  has_pending()         判斷有無積壓檔（無資料的輪次可跳過連 DB）

設計約定（與 supabase_writer 的 buffer 對齊）：
  - buffer 檔 = 一輪一檔 JSON（``{name}_{YYYYmmdd_HHMMSS_ffffff}.json``）。
    選 JSON 不選 pickle：可人工檢視、無反序列化安全疑慮；rows 內的 datetime
    透過 ``json.dumps(default=str)`` 序列化成 ISO 字串，flush 端由各 collector
    的 write_fn 自行 ``fromisoformat`` 還原（collector 的 rows 本來就以
    isoformat 字串為主，psycopg2 也接受 ISO 字串寫 timestamptz）。
  - 超過 BUFFER_MAX_AGE_DAYS（預設 3 天，同主 repo）的檔直接刪除並 log
    （分區表 retention 可能已清掉對應分區）；週跑型 collector（cdc）可調大。
  - 單檔補寫失敗即中止本輪 flush（DB 大概率還沒恢復，避免逐檔重試卡死 cron）；
    壞檔（JSON 解析失敗）例外 — 直接刪除，否則會永久卡住 flush。
  - 資料庫明確回報「資料本身不合法」（classify_invalid_data_error）的檔例外 —
    移到 ``buffer_dir/quarantine/``、不再重試，繼續補後面的檔；隔離檔保留
    QUARANTINE_MAX_AGE_DAYS（30）天後刪除（同主 repo #202）。
  - 目錄上限 MAX_BUFFER_FILES 檔，超過先刪最舊的，防磁碟塞爆。

零第三方依賴（不 import psycopg2 — conn / write_fn 由 caller 傳入），
部署時跟 collector 單檔一起 scp 到同目錄即可 import，維持單檔可攜精神。
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

# Buffer 檔最大保留天數（同主 repo supabase_writer.BUFFER_MAX_AGE_DAYS）
BUFFER_MAX_AGE_DAYS = 3

# 單 collector buffer 目錄檔數上限（超過刪最舊）
MAX_BUFFER_FILES = 200

_DEFAULT_LOG = logging.getLogger("vm_buffer")

# quarantine/ 隔離檔保留天數（從移入時起算；同主 repo BUFFER_QUARANTINE_MAX_AGE_DAYS）
QUARANTINE_MAX_AGE_DAYS = 30

# ── 資料不合法的分類 ────────────────────────────────────────────────
# ⚠️ 與 storage/supabase_writer.py 的 INVALID_DATA_SQLSTATES／_INVALID_DATA_MESSAGES／
# classify_invalid_data_error() 對應，改一邊要同步另一邊（tests/test_vm_buffer.py 有對照測試）。
# VM 是單檔部署、不帶 storage/，所以自備一份；不 import psycopg2，改看例外有沒有
# pgcode 屬性（psycopg2.Error 一定有，值可能是 None）。
INVALID_DATA_SQLSTATES = {
    '23514': 'check_violation',
    '23502': 'not_null_violation',
    '22P02': 'invalid_text_representation',
    '22007': 'invalid_datetime_format',
    '22003': 'numeric_value_out_of_range',
    '42804': 'datatype_mismatch',
}
_INVALID_DATA_MESSAGES = (
    ('violates check constraint', '23514'),
    ('violates not-null constraint', '23502'),
    ('invalid input syntax for type', '22P02'),
)


def classify_invalid_data_error(err: BaseException) -> str | None:
    """資料庫明確回報資料不合法 → 回傳類別名；其餘（含非資料庫例外）回 None＝照舊重試。"""
    if not hasattr(err, 'pgcode'):
        return None
    msg = str(err)
    code = getattr(err, 'pgcode', None)
    if code is None:
        code = next((c for needle, c in _INVALID_DATA_MESSAGES if needle in msg), None)
    label = INVALID_DATA_SQLSTATES.get(code)
    if label is None:
        return None
    if code == '23514':
        # 分區表「找不到分區」也是 23514，屬暫時性問題，不隔離
        constraint = getattr(getattr(err, 'diag', None), 'constraint_name', None)
        if 'partition' in msg.lower():
            return None
        if not (constraint or 'violates check constraint' in msg):
            return None
    return label


def _quarantine_file(fp: Path, buffer_dir: Path, log: logging.Logger) -> None:
    """移到 buffer_dir/quarantine/；mtime 設為移入時間，保留期從此起算。"""
    try:
        qdir = buffer_dir / "quarantine"
        qdir.mkdir(parents=True, exist_ok=True)
        dest = qdir / fp.name
        fp.replace(dest)
        dest.touch()
    except OSError as move_err:
        log.warning(f"buffer 隔離失敗 {fp.name}: {move_err}")


def _purge_old_quarantine(buffer_dir: Path, log: logging.Logger) -> None:
    cutoff = time.time() - QUARANTINE_MAX_AGE_DAYS * 86400
    try:
        for q in (buffer_dir / "quarantine").glob("*.json"):
            if q.stat().st_mtime < cutoff:
                q.unlink(missing_ok=True)
                log.info(f"buffer 隔離檔超過 {QUARANTINE_MAX_AGE_DAYS} 天，刪除：{q.name}")
    except OSError as e:
        log.warning(f"buffer 隔離檔清理失敗: {e}")


def has_pending(buffer_dir: Path) -> bool:
    """buffer 目錄是否有積壓檔（目錄不存在視為無）"""
    return buffer_dir.is_dir() and any(buffer_dir.glob("*.json"))


def save_batch(buffer_dir: Path, name: str, payload: dict,
               log: logging.Logger | None = None) -> Path | None:
    """DB 寫入失敗時，把該輪資料存成本地 JSON 檔。

    絕不 raise（這是最後的安全網）；存檔失敗回傳 None（= 資料真的丟了）。
    """
    log = log or _DEFAULT_LOG
    try:
        buffer_dir.mkdir(parents=True, exist_ok=True)

        # size cap：超過上限先刪最舊（檔名含 timestamp，字典序 = 時間序）
        existing = sorted(buffer_dir.glob("*.json"))
        overflow = len(existing) - (MAX_BUFFER_FILES - 1)
        if overflow > 0:
            for old in existing[:overflow]:
                old.unlink(missing_ok=True)
            log.warning(f"buffer 達上限 {MAX_BUFFER_FILES} 檔，丟棄最舊 {overflow} 檔")

        now = datetime.now(timezone.utc)
        fp = buffer_dir / f"{name}_{now.strftime('%Y%m%d_%H%M%S_%f')}.json"
        fp.write_text(json.dumps({
            "name": name,
            "saved_at": now.isoformat(),
            "payload": payload,
        }, ensure_ascii=False, default=str), encoding="utf-8")
        log.info(f"本輪資料已存 buffer: {fp.name}")
        return fp
    except Exception as e:
        log.error(f"buffer 存檔失敗（該輪資料丟失）: {e}")
        return None


def flush_pending(conn, buffer_dir: Path,
                  write_fn: Callable[[object, dict], None],
                  log: logging.Logger | None = None,
                  max_age_days: int = BUFFER_MAX_AGE_DAYS) -> tuple[int, int]:
    """每輪開頭補寫積壓 buffer 檔。回傳 (補寫成功數, 丟棄數)。

    - ``write_fn(conn, payload)`` 成功才刪檔
    - 單檔補寫失敗 → rollback + 中止本輪（下一輪再試）
    - 例外：DB 明確回報資料不合法 → rollback、移到 quarantine/、繼續下一檔（不計入回傳值）
    - 超過 max_age_days 的檔刪除並 log
    - JSON 解析失敗的壞檔刪除（否則永久卡住 flush）
    """
    log = log or _DEFAULT_LOG
    if buffer_dir.is_dir():
        _purge_old_quarantine(buffer_dir, log)  # 放在「沒有積壓檔就返回」之前，buffer 空了也照清
    # 不遞迴：quarantine/ 內的檔不會被重試或過期丟棄
    files = sorted(buffer_dir.glob("*.json")) if buffer_dir.is_dir() else []
    if not files:
        return 0, 0

    log.info(f"buffer 補寫開始：{len(files)} 個積壓檔")
    now = datetime.now(timezone.utc)
    max_age = timedelta(days=max_age_days)
    flushed = dropped = 0

    for fp in files:
        try:
            wrapper = json.loads(fp.read_text(encoding="utf-8"))
            saved_at = datetime.fromisoformat(wrapper["saved_at"])
            if saved_at.tzinfo is None:
                saved_at = saved_at.replace(tzinfo=timezone.utc)
        except (KeyError, TypeError, ValueError) as e:
            fp.unlink(missing_ok=True)
            dropped += 1
            log.warning(f"buffer 壞檔丟棄：{fp.name} ({e})")
            continue

        if now - saved_at > max_age:
            fp.unlink(missing_ok=True)
            dropped += 1
            log.info(f"buffer 過期丟棄：{fp.name} (age={now - saved_at})")
            continue

        try:
            write_fn(conn, wrapper["payload"])
            fp.unlink(missing_ok=True)
            flushed += 1
            log.info(f"buffer 補寫成功：{fp.name}")
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            label = classify_invalid_data_error(e)
            if label:
                # 重試永遠失敗：隔離後繼續。只記 SQLSTATE 與 constraint／欄位名，
                # 不印 str(e)（DETAIL 會帶整列資料）。
                diag = getattr(e, 'diag', None)
                log.error(
                    f"buffer 資料不合法，隔離不再重試：檔案={fp.name} 類別={label} "
                    f"SQLSTATE={getattr(e, 'pgcode', None)} "
                    f"constraint={getattr(diag, 'constraint_name', None)} "
                    f"column={getattr(diag, 'column_name', None)}")
                _quarantine_file(fp, buffer_dir, log)
                continue
            log.warning(f"buffer 補寫失敗，中止本輪 flush：{fp.name}: {e}")
            break

    log.info(f"buffer 補寫結束：成功 {flushed} / 丟棄 {dropped}")
    return flushed, dropped


def connect_with_retry(connect_fn: Callable[[], object], retries: int = 2,
                       wait_seconds: int = 5,
                       log: logging.Logger | None = None):
    """``connect_fn()`` 失敗時 retry（預設 2 次、間隔 5s，共 3 次嘗試）。"""
    log = log or _DEFAULT_LOG
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return connect_fn()
        except Exception as e:
            last_exc = e
            if attempt < retries:
                log.warning(f"DB 連線失敗 ({attempt + 1}/{retries + 1})，"
                            f"{wait_seconds}s 後重試: {e}")
                time.sleep(wait_seconds)
    raise last_exc
