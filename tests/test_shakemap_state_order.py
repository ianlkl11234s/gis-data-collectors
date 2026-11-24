"""earthquake_shakemap_grid：R2 事件檔存在即視為「處理過」，DB 寫入失敗後下一輪因 R2 已有檔而跳過 DB（2026-11-22 清查）。"""
from __future__ import annotations

from unittest.mock import MagicMock

from collectors import earthquake_shakemap_grid as sg


def _shakemap(monkeypatch, *, r2_has_file, db_has_event, receipt=False):
    c = sg.EarthquakeShakemapGridCollector.__new__(sg.EarthquakeShakemapGridCollector)
    c.name = "earthquake_shakemap_grid"
    c._r2, c._s3 = MagicMock(), None
    c.supabase_writer = MagicMock()
    raw = [{"EventName": "測試", "EventDateTime": "2026-07-27T10:15:30", "Magnitude": "5.1",
            "WGS84_Lon": "121.0", "WGS84_Lat": "23.0", "EQ_WGS84_Lon": "121.5", "EQ_WGS84_Lat": "23.9",
            "Depth": "10", "PGA": "1", "PGV": "1", "Intensity": "2"}]
    monkeypatch.setattr(c, "_fetch", lambda: raw)
    monkeypatch.setattr(c, "_event_published", lambda t: r2_has_file)
    monkeypatch.setattr(c, "_already_stored", lambda t: db_has_event)
    monkeypatch.setattr(c, "_db_receipt_exists", lambda t: receipt)
    c.receipts_written = []
    monkeypatch.setattr(c, "_write_db_receipt", lambda t: c.receipts_written.append(t))
    monkeypatch.setattr(sg, "ensure_indexed", lambda *a, **k: False)
    monkeypatch.setattr(sg, "publish_event", lambda *a, **k: "k")
    return c


def test_shakemap_r2_published_but_db_missing_still_writes_db(monkeypatch):
    c = _shakemap(monkeypatch, r2_has_file=True, db_has_event=False)
    out = c.collect()
    assert out["skipped"] is False and len(out["data"]) == 1, "R2 已有檔但 DB 沒有 → 必須補寫 DB"


def test_shakemap_r2_and_db_both_have_event_skips(monkeypatch):
    c = _shakemap(monkeypatch, r2_has_file=True, db_has_event=True)
    out = c.collect()
    assert out["skipped"] is True and out["data"] == []


def test_shakemap_r2_published_no_db_writer_still_skips(monkeypatch):
    c = _shakemap(monkeypatch, r2_has_file=True, db_has_event=False)
    c.supabase_writer = None
    out = c.collect()
    assert out["skipped"] is True and out["data"] == []


def test_shakemap_expired_db_rows_with_receipt_not_rewritten(monkeypatch):
    """E1：DB 保留期限清掉列後 NCDR 仍回同一事件；已有「DB 入庫過」收據 → 不可把網格重寫回 DB。"""
    c = _shakemap(monkeypatch, r2_has_file=True, db_has_event=False, receipt=True)
    out = c.collect()
    assert out["skipped"] is True and out["data"] == []


def test_shakemap_db_present_records_receipt(monkeypatch):
    c = _shakemap(monkeypatch, r2_has_file=True, db_has_event=True)
    c.collect()
    assert c.receipts_written == ["2026-07-27T10:15:30+08:00"]


def test_shakemap_failed_write_without_receipt_still_retried(monkeypatch):
    c = _shakemap(monkeypatch, r2_has_file=True, db_has_event=False, receipt=False)
    out = c.collect()
    assert out["skipped"] is False and c.receipts_written == []
