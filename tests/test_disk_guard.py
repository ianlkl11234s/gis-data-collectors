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


# ── 嚴重時暫停寫本機原始檔 ──────────────────────────────────────────────

def _crit_cfg(**kw):
    return dg.GuardConfig(free_warn_pct=20, free_crit_pct=10, resume_margin_pct=3, **kw)


def test_recovered_needs_margin_below_critical():
    c = _crit_cfg()
    assert not dg.recovered(0, 12.9, c)   # 剛好高於門檻但未達緩衝
    assert dg.recovered(0, 13.0, c)
    c2 = _crit_cfg(data_crit_gb=100)
    assert not dg.recovered(int(98 * dg._GB), 50, c2)  # 98 ≥ 100*(1-3%)
    assert dg.recovered(int(96 * dg._GB), 50, c2)


def test_pause_resume_notifies_once_each(tmp_path, monkeypatch):
    sent = []
    c = _crit_cfg()
    g = dg.DiskGuard(tmp_path, c, notify=sent.append)
    free = {"pct": 4.0}

    def fake_check(data_dir, cfg, now_ts=None):
        level, reasons = dg.evaluate(0, free["pct"], cfg)
        return dg.DiskStatus(level=level, data_bytes=0, free_pct=free["pct"],
                             free_bytes=1, reasons=reasons)
    monkeypatch.setattr(dg, "check", fake_check)

    assert g.run(NOW)["raw_writes_paused"] and dg.is_raw_paused()
    assert len(sent) == 1 and "本機原始檔暫停寫入，DB 照寫；S3 當天歸檔會缺這段" in sent[0]
    g.run(NOW + 60)
    assert len(sent) == 1                       # 持續暫停不重發
    free["pct"] = 11.0                           # 脫離 critical 但未達緩衝（13）
    g.run(NOW + 120)
    assert dg.is_raw_paused() and len(sent) == 1
    free["pct"] = 14.0                           # 達緩衝 → 恢復（warn 等級也要發恢復）
    assert not g.run(NOW + 180)["raw_writes_paused"]
    assert len(sent) == 2 and "已恢復寫入" in sent[-1]
    g.run(NOW + 240)
    assert len(sent) == 2


def test_local_storage_skips_raw_but_not_latest_or_spool(tmp_path, caplog):
    from storage.local import LocalStorage
    ls = LocalStorage(tmp_path)
    spool = tmp_path / "aisstream_spool" / "a.jsonl"
    state = tmp_path / "state" / "x.json"

    dg.pause_raw_writes(["test"])
    with caplog.at_level("WARNING"):
        assert ls.save("bus", {"data": [1]}) is None
        assert ls.save("bus", {"data": [2]}) is None
        assert ls.save_append("bus", [{"a": 1}]) is None
    assert [p for p in tmp_path.rglob("bus_*.json*")] == []
    assert ls.get_latest("bus") == {"data": [2]}          # latest.json 照更新
    assert sum("略過本機原始檔寫入" in r.message for r in caplog.records) == 1  # 只記一次
    assert dg.pause_status()["skipped_writes"] == 3

    # spool／state 不走 LocalStorage，不受旗標影響
    _touch(spool, b"keep")
    _touch(state, b"{}")
    assert spool.read_bytes() == b"keep" and state.exists()

    dg.resume_raw_writes()
    p = ls.save("bus", {"data": [3]})
    assert p is not None and p.exists()


def test_collector_run_skips_raw_but_still_writes_db(tmp_path):
    from unittest.mock import MagicMock
    from collectors.base import BaseCollector
    from storage.local import LocalStorage

    class C(BaseCollector):
        name = "dummy_pause"
        interval_minutes = 1

        def __init__(self):
            self.storage = LocalStorage(tmp_path)
            self.supabase_writer = MagicMock()
            self.supabase_writer.write.return_value = True
            self.last_run = self.last_success_at = None
            self.run_count = self.error_count = self.consecutive_errors = 0

        def collect(self):
            return {"total": 1, "data": [{"x": 1}]}

    c = C()
    dg.pause_raw_writes(["test"])
    assert c.run()["total"] == 1
    assert c.supabase_writer.write.call_count == 1       # DB 照寫
    assert list(tmp_path.rglob("dummy_pause_*.json")) == []  # 原始檔沒寫
    dg.resume_raw_writes()
    c.run()
    assert c.supabase_writer.write.call_count == 2
    assert len(list(tmp_path.rglob("dummy_pause_*.json"))) == 1


def test_production_path_uses_alerting_red_once_and_recovery(tmp_path, monkeypatch):
    """notify=None（主機實際走的路）：嚴重 🔴 一則、持續不重送、警告只進日報、恢復一則。"""
    from utils import alerting
    sent = []
    monkeypatch.setattr(alerting, "_sender", lambda text: sent.append(text) or True)
    c = dg.GuardConfig(data_warn_gb=1e-9, data_crit_gb=1, free_warn_pct=0, free_crit_pct=0,
                       realert_hours=6)
    g = dg.DiskGuard(tmp_path, c)
    _touch(tmp_path / "bus" / "a.json", b"x" * 100)
    assert g.run(NOW)["level"] == "warn"
    assert sent == []  # 🟡 不送
    c.data_crit_gb = 1e-9
    g.run(NOW + 120)
    g.run(NOW + 3600)
    g.run(NOW + 6 * 3600)
    assert len(sent) == 1 and sent[0].startswith("🔴") and "本機磁碟嚴重" in sent[0]
    c.data_warn_gb = c.data_crit_gb = 0
    assert g.run(NOW + 7 * 3600)["level"] == "ok"
    assert len(sent) == 2 and sent[1].startswith("✅")
