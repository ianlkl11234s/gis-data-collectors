"""review F120：backfill 跳過條件要同時確認 R2 圖與 T2 小時檔。"""
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("backfill_himawari", ROOT / "scripts" / "backfill_jma_himawari_imagery.py")
bf = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(ROOT / "scripts"))
spec.loader.exec_module(bf)

VT = datetime(2026, 10, 20, 3, tzinfo=timezone.utc)
NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)
KEEP = NOW - timedelta(days=5)
ROW = (None, "imagery/x.jpg", NOW)


class Store:
    def __init__(self, present):
        self.present = present

    def head(self, key):
        return {"ContentLength": 1} if self.present else None


def run(s3, row=ROW, r2=None):
    st = {"skipped": 0}
    r = bf.frame_already_published(row, VT, KEEP, r2 or Store(True), s3, False, st)
    return r, st


def test_skip_only_when_t2_exists():
    assert run(Store(True))[0] is True
    r, st = run(Store(False))
    assert r is False and st["skipped"] == 0 and st["t2_missing_refetch"] == 1


def test_no_s3_keeps_old_behaviour():
    assert run(None)[0] is True


def test_missing_r2_object_or_stale_row_refetches():
    assert run(Store(True), r2=Store(False))[0] is False
    assert run(Store(True), row=(None, "k", KEEP - timedelta(days=1)))[0] is False
    assert run(Store(True), row=None)[0] is False
