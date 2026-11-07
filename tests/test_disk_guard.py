"""utils.disk_guard：兩級告警、只清可丟／已驗證的檔、絕不刪未歸檔原始檔。"""
import os
import time
from datetime import date

import pytest

import config
from utils import disk_guard as dg

NOW = time.mktime((2026, 11, 7, 12, 0, 0, 0, 0, -1))
OLD = NOW - 40 * 86400


def _touch(path, data=b"x", mtime=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return path


@pytest.fixture
def cfg():
    return dg.GuardConfig(free_warn_pct=0, free_crit_pct=0)  # 只測資料量門檻


@pytest.fixture(autouse=True)
def _retention(monkeypatch):
    monkeypatch.setattr(config, "get_retention_days", lambda name: 7)


def test_evaluate_levels():
    c = dg.GuardConfig(data_warn_gb=1, data_crit_gb=2, free_warn_pct=15, free_crit_pct=5)
    assert dg.evaluate(0, 50, c)[0] == "ok"
    assert dg.evaluate(int(1.5 * dg._GB), 50, c)[0] == "warn"
    assert dg.evaluate(int(2.5 * dg._GB), 50, c)[0] == "critical"
    assert dg.evaluate(0, 10, c)[0] == "warn"
    level, reasons = dg.evaluate(int(1.5 * dg._GB), 3, c)
    assert level == "critical" and len(reasons) == 2


def test_clean_safe_never_touches_unarchived_raw(tmp_path, cfg):
    raw = _touch(tmp_path / "bus" / "2026" / "09" / "01" / "bus_0100.json", mtime=OLD)
    spool = _touch(tmp_path / "aisstream" / "spool" / "x.jsonl", mtime=OLD)
    state = _touch(tmp_path / "state" / "jma_quake_seen.json", mtime=OLD)
    old_tmp = _touch(tmp_path / "tmp" / "backup" / "backup-a.csv.gz", mtime=OLD)
    new_tmp = _touch(tmp_path / "tmp" / "backup" / "backup-b.csv.gz", mtime=NOW - 60)
    old_tar = _touch(tmp_path / ".tmp_bus_2026-09-01.tar.gz", mtime=OLD)
    old_mw = _touch(tmp_path / ".memwatch" / "2026-09-01.txt", mtime=OLD)
    new_mw = _touch(tmp_path / ".memwatch" / "2026-11-06.txt", mtime=NOW - 3600)

    cleaned = dg.clean_safe(tmp_path, cfg, NOW)
    assert cleaned["tmp"] == 2 and cleaned["memwatch"] == 1
    for kept in (raw, spool, state, new_tmp, new_mw):
        assert kept.exists()
    for gone in (old_tmp, old_tar, old_mw):
        assert not gone.exists()


def test_receipts_pruned_only_when_date_dir_gone_and_old(tmp_path):
    receipts = tmp_path / ".archive-receipts" / "bus"
    gone_old = _touch(receipts / "2026-08-01.json")      # 目錄已刪、夠舊 → 刪
    kept_dir = _touch(receipts / "2026-08-02.json")      # 原始檔還在 → 留
    _touch(tmp_path / "bus" / "2026" / "08" / "02" / "bus_0100.json")
    recent = _touch(receipts / "2026-10-30.json")        # 太新 → 留
    weird = _touch(receipts / "notadate.json")
    n = dg.prune_archive_receipts(tmp_path, keep_days=30, today=date(2026, 11, 7))
    assert n == 1 and not gone_old.exists()
    assert kept_dir.exists() and recent.exists() and weird.exists()


def test_receipt_min_age_respects_collector_retention(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "get_retention_days", lambda name: 60)
    r = _touch(tmp_path / ".archive-receipts" / "ship_ais" / "2026-09-20.json")  # 48 天前
    assert dg.prune_archive_receipts(tmp_path, keep_days=30, today=date(2026, 11, 7)) == 0
    assert r.exists()


def test_guard_alerts_on_escalation_realert_and_recovery(tmp_path):
    sent = []
    c = dg.GuardConfig(data_warn_gb=1e-9, data_crit_gb=1, free_warn_pct=0, free_crit_pct=0,
                       realert_hours=6)
    g = dg.DiskGuard(tmp_path, c, notify=sent.append)
    _touch(tmp_path / "bus" / "a.json", b"x" * 100)
    assert g.run(NOW)["level"] == "warn" and len(sent) == 1
    assert "警告" in sent[-1] and "未歸檔的原始檔不會自動刪除" in sent[-1]
    g.run(NOW + 60)
    assert len(sent) == 1  # 同等級不重複
    c.data_crit_gb = 1e-9
    g.run(NOW + 120)
    assert len(sent) == 2 and "嚴重" in sent[-1]
    g.run(NOW + 3600)
    assert len(sent) == 2
    g.run(NOW + 120 + 6 * 3600)
    assert len(sent) == 3  # 嚴重持續 6 小時再提醒
    c.data_warn_gb = c.data_crit_gb = 0
    assert g.run(NOW + 7 * 3600)["level"] == "ok"
    assert len(sent) == 4 and "恢復" in sent[-1]


def test_dir_sizes(tmp_path):
    _touch(tmp_path / "a" / "x" / "1.json", b"x" * 10)
    _touch(tmp_path / "b.txt", b"x" * 5)
    total, per = dg.dir_sizes(tmp_path)
    assert total == 15 and per == {"a": 10, "b.txt": 5}
