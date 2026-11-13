"""external/vm_common/vm_buffer.py 的單元測試（零 DB 依賴，conn 用 stub）"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "external" / "vm_common"))
import vm_buffer  # noqa: E402


class StubConn:
    """psycopg2 connection 替身：只需要 rollback()"""

    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


def test_save_batch_creates_json_file(tmp_path):
    payload = {"ts": "2026-07-02T12:00:00+08:00", "records": [{"mmsi": "123", "lat": 25.0}]}
    fp = vm_buffer.save_batch(tmp_path, "ship_ais", payload)
    assert fp is not None and fp.exists()
    wrapper = json.loads(fp.read_text(encoding="utf-8"))
    assert wrapper["name"] == "ship_ais"
    assert wrapper["payload"] == payload


def test_save_batch_serializes_datetime_via_default_str(tmp_path):
    ts = datetime(2026, 7, 2, 12, 0, tzinfo=timezone(timedelta(hours=8)))
    fp = vm_buffer.save_batch(tmp_path, "x", {"ts": ts, "records": []})
    wrapper = json.loads(fp.read_text(encoding="utf-8"))
    # datetime 經 default=str 序列化成字串，fromisoformat 可還原
    assert datetime.fromisoformat(wrapper["payload"]["ts"]) == ts


def test_write_fail_then_flush_recovers_data(tmp_path):
    """核心情境：write_fn 拋例外 → buffer 檔生成 → flush（write 正常）→ 檔案消失、資料完整"""
    payload = {"ts": "2026-07-02T12:00:00+08:00",
               "records": [{"mmsi": "413000001", "lat": 25.1, "lng": 121.5}]}

    # 1) DB 寫入失敗 → save_batch
    fp = vm_buffer.save_batch(tmp_path, "ship_ais", payload)
    assert fp.exists()

    # 2) DB 恢復 → flush_pending 補寫成功
    written = []
    flushed, dropped = vm_buffer.flush_pending(
        StubConn(), tmp_path, lambda conn, p: written.append(p))
    assert (flushed, dropped) == (1, 0)
    assert written == [payload]          # 資料完整
    assert not any(tmp_path.glob("*.json"))  # 檔案消失


def test_flush_aborts_on_first_failure_and_rolls_back(tmp_path):
    vm_buffer.save_batch(tmp_path, "x", {"n": 1})
    vm_buffer.save_batch(tmp_path, "x", {"n": 2})
    conn = StubConn()

    def boom(_conn, _payload):
        raise RuntimeError("db still down")

    flushed, dropped = vm_buffer.flush_pending(conn, tmp_path, boom)
    assert (flushed, dropped) == (0, 0)
    assert conn.rollbacks == 1                     # 只試第一檔就中止
    assert len(list(tmp_path.glob("*.json"))) == 2  # 檔案保留，下輪再試


def test_flush_drops_expired_files(tmp_path):
    old = datetime.now(timezone.utc) - timedelta(days=4)
    fp = tmp_path / "x_old.json"
    fp.write_text(json.dumps({"name": "x", "saved_at": old.isoformat(), "payload": {}}))
    written = []
    flushed, dropped = vm_buffer.flush_pending(
        StubConn(), tmp_path, lambda conn, p: written.append(p))
    assert (flushed, dropped) == (0, 1)
    assert not fp.exists()
    assert written == []


def test_flush_respects_custom_max_age(tmp_path):
    """cdc 週跑型：max_age_days=30 時 4 天前的檔仍要補寫"""
    old = datetime.now(timezone.utc) - timedelta(days=4)
    fp = tmp_path / "x_old.json"
    fp.write_text(json.dumps({"name": "x", "saved_at": old.isoformat(), "payload": {"n": 1}}))
    written = []
    flushed, dropped = vm_buffer.flush_pending(
        StubConn(), tmp_path, lambda conn, p: written.append(p), max_age_days=30)
    assert (flushed, dropped) == (1, 0)
    assert written == [{"n": 1}]


def test_flush_drops_corrupt_file_and_continues(tmp_path):
    (tmp_path / "a_corrupt.json").write_text("{not json")
    vm_buffer.save_batch(tmp_path, "x", {"n": 2})
    written = []
    flushed, dropped = vm_buffer.flush_pending(
        StubConn(), tmp_path, lambda conn, p: written.append(p))
    assert (flushed, dropped) == (1, 1)  # 壞檔刪除不卡 flush，好檔照補
    assert not any(tmp_path.glob("*.json"))


def test_save_batch_size_cap_drops_oldest(tmp_path, monkeypatch):
    monkeypatch.setattr(vm_buffer, "MAX_BUFFER_FILES", 3)
    for i in range(3):
        vm_buffer.save_batch(tmp_path, "x", {"n": i})
    assert len(list(tmp_path.glob("*.json"))) == 3

    vm_buffer.save_batch(tmp_path, "x", {"n": 99})
    files = sorted(tmp_path.glob("*.json"))
    assert len(files) == 3  # cap 維持
    payloads = [json.loads(f.read_text())["payload"]["n"] for f in files]
    assert 0 not in payloads and 99 in payloads  # 最舊的被丟、最新的保留


def test_has_pending(tmp_path):
    assert vm_buffer.has_pending(tmp_path / "nonexistent") is False
    assert vm_buffer.has_pending(tmp_path) is False
    vm_buffer.save_batch(tmp_path, "x", {})
    assert vm_buffer.has_pending(tmp_path) is True


def test_connect_with_retry_succeeds_after_failures(monkeypatch):
    sleeps = []
    monkeypatch.setattr(vm_buffer.time, "sleep", lambda s: sleeps.append(s))
    attempts = {"n": 0}

    def connect_fn():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise OSError("connection refused")
        return "CONN"

    assert vm_buffer.connect_with_retry(connect_fn) == "CONN"
    assert attempts["n"] == 3
    assert sleeps == [5, 5]  # 2 次 retry、間隔 5s


def test_connect_with_retry_raises_after_exhaustion(monkeypatch):
    monkeypatch.setattr(vm_buffer.time, "sleep", lambda s: None)
    with pytest.raises(OSError):
        vm_buffer.connect_with_retry(lambda: (_ for _ in ()).throw(OSError("down")))


# ─────────────────────────────────────────────────────────
# 補寫遇資料不合法 → 隔離不再重試（COL-GE-2，比照主 repo #202）
# ─────────────────────────────────────────────────────────

import psycopg2  # noqa: E402


def _pg_error(code, *, constraint=None, column=None, message='boom', base=psycopg2.DatabaseError):
    """psycopg2 例外的 pgcode／diag 是唯讀，用子類別的類別屬性蓋掉（同 test_supabase_writer）。"""
    diag = type('Diag', (), {'constraint_name': constraint, 'column_name': column})()
    cls = type('FakePgError', (base,), {'pgcode': code, 'diag': diag})
    return cls(message)


_CLASSIFY_CASES = [
    _pg_error('23514', constraint='c', message='new row for relation "x" violates check constraint "c"'),
    _pg_error('23502', column='lat'),
    _pg_error('22P02'), _pg_error('22007'), _pg_error('22003'), _pg_error('42804'),
    _pg_error('57014', message='canceling statement due to statement timeout'),
    _pg_error('55P03'), _pg_error('40P01'),
    _pg_error('23505', constraint='pk'), _pg_error('23503', constraint='fk'), _pg_error('42501'),
    _pg_error('08006', base=psycopg2.OperationalError),
    _pg_error(None, message='server closed the connection unexpectedly', base=psycopg2.OperationalError),
    _pg_error('23514', message='no partition of relation "x" found for row'),
    _pg_error('23514', constraint='x', message='new row violates partition constraint'),
    _pg_error(None, message='new row for relation "x" violates check constraint "c"'),
    _pg_error(None, message='invalid input syntax for type integer: "abc"'),
    ValueError('bad'), TimeoutError('timed out'), RuntimeError('db still down'),
]


@pytest.mark.parametrize('err', _CLASSIFY_CASES)
def test_classify_matches_main_repo_writer(err):
    """VM 自備的分類必須與 storage/supabase_writer.py 結果一致（兩份要同步）"""
    from storage.supabase_writer import classify_invalid_data_error as main_classify
    assert vm_buffer.classify_invalid_data_error(err) == main_classify(err)


def test_classify_quarantine_and_retry_examples():
    assert vm_buffer.classify_invalid_data_error(_pg_error('23502', column='lat')) == 'not_null_violation'
    assert vm_buffer.classify_invalid_data_error(
        _pg_error('23514', message='no partition of relation "x" found for row')) is None
    assert vm_buffer.classify_invalid_data_error(RuntimeError('db still down')) is None


def test_flush_quarantines_invalid_data_and_continues(tmp_path, caplog):
    bad = vm_buffer.save_batch(tmp_path, "x", {"n": 1})
    good = vm_buffer.save_batch(tmp_path, "x", {"n": 2})
    conn = StubConn()
    written = []
    secret_row = 'Failing row contains (secret-value)'

    def write_fn(_conn, payload):
        if payload["n"] == 1:
            raise _pg_error('23514', constraint='lineage_chk',
                            message=f'new row violates check constraint "lineage_chk"\nDETAIL: {secret_row}')
        written.append(payload)

    with caplog.at_level("INFO", logger="vm_buffer"):
        flushed, dropped = vm_buffer.flush_pending(conn, tmp_path, write_fn)
    assert (flushed, dropped) == (1, 0)
    assert written == [{"n": 2}]                     # 壞檔不擋後面的檔
    assert conn.rollbacks == 1
    assert not bad.exists() and (tmp_path / "quarantine" / bad.name).exists()
    assert not good.exists()
    assert not any(tmp_path.glob("*.json"))          # has_pending 不再看到隔離檔
    assert vm_buffer.has_pending(tmp_path) is False
    log_text = caplog.text
    assert "SQLSTATE=23514" in log_text and "lineage_chk" in log_text
    assert "secret-value" not in log_text            # 不印完整錯誤訊息


def test_flush_transient_error_still_aborts_and_keeps_file(tmp_path):
    vm_buffer.save_batch(tmp_path, "x", {"n": 1})
    vm_buffer.save_batch(tmp_path, "x", {"n": 2})

    def write_fn(_conn, _payload):
        raise _pg_error('57014', message='canceling statement due to statement timeout')

    flushed, dropped = vm_buffer.flush_pending(StubConn(), tmp_path, write_fn)
    assert (flushed, dropped) == (0, 0)
    assert len(list(tmp_path.glob("*.json"))) == 2
    assert not (tmp_path / "quarantine").exists()


def test_flush_does_not_retry_quarantined_files(tmp_path):
    qdir = tmp_path / "quarantine"
    qdir.mkdir(parents=True)
    (qdir / "x_q.json").write_text(json.dumps(
        {"name": "x", "saved_at": datetime.now(timezone.utc).isoformat(), "payload": {"n": 1}}))
    written = []
    assert vm_buffer.flush_pending(StubConn(), tmp_path, lambda c, p: written.append(p)) == (0, 0)
    assert written == [] and (qdir / "x_q.json").exists()


def test_flush_purges_old_quarantine_even_when_buffer_empty(tmp_path):
    import os
    import time as _time
    qdir = tmp_path / "quarantine"
    qdir.mkdir(parents=True)
    old, fresh = qdir / "x_old.json", qdir / "x_new.json"
    old.write_text("{}")
    fresh.write_text("{}")
    past = _time.time() - (vm_buffer.QUARANTINE_MAX_AGE_DAYS + 1) * 86400
    os.utime(old, (past, past))
    vm_buffer.flush_pending(StubConn(), tmp_path, lambda c, p: None)
    assert not old.exists() and fresh.exists()
