"""HiCloud waste_positions VM collector：重複點靠不指定目標的 ON CONFLICT DO NOTHING 略過。

2026-10-15 Disk IO 第三輪 W2：輪詢時間窗重疊讓同一點被寫 2–4 次。
gis-platform 458 會加 (city, vehicle_no, observed_at) 唯一索引；
collector 必須先改成 ON CONFLICT DO NOTHING，否則整批 INSERT 會失敗卡 buffer。
"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "external" / "vm_common"))
sys.path.insert(0, str(ROOT / "external" / "waste_positions_vm"))
# 模組 import 時沒有 SUPABASE_DB_URL 會 sys.exit；只在 import 期間給假值，
# 之後移除，避免污染 test_test_isolation 的憑證清空檢查。
_had_db_url = "SUPABASE_DB_URL" in os.environ
os.environ.setdefault("SUPABASE_DB_URL", "postgresql://test")
try:
    import waste_positions_collect as vm  # noqa: E402
finally:
    if not _had_db_url:
        os.environ.pop("SUPABASE_DB_URL", None)


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self):
        self.commits = 0

    def cursor(self):
        return FakeCursor()

    def commit(self):
        self.commits += 1


def _rec(vno="KAA-001", observed_at="2026-10-15T10:00:00+08:00"):
    return {"city": "臺南市", "vehicle_no": vno, "route_id": "R1", "status": "collecting",
            "lat": 23.0, "lng": 120.2, "observed_at": observed_at, "source_url": "u"}


@pytest.fixture
def captured(monkeypatch):
    calls = []
    monkeypatch.setattr(vm, "execute_values",
                        lambda cur, sql, rows, page_size=100: calls.append((sql, list(rows))))
    return calls


def test_insert_is_targetless_on_conflict_do_nothing(captured):
    conn = FakeConn()
    n = vm.write_to_supabase(conn, [_rec(), _rec()])
    (sql, rows), = captured
    assert sql.startswith("INSERT INTO spatial.waste_positions_realtime ")
    assert sql.rstrip().endswith("ON CONFLICT DO NOTHING")
    assert "ON CONFLICT (" not in sql, "指定目標的話，索引還沒建好時會直接報錯"
    # 重複點照樣送出，由 DB 端略過；回傳的是送出列數
    assert n == 2 and len(rows) == 2
    assert conn.commits == 1


def test_rows_missing_coords_or_vehicle_are_dropped(captured):
    bad_coord = _rec()
    bad_coord["lat"] = None
    n = vm.write_to_supabase(FakeConn(), [_rec(), bad_coord, _rec(vno="  ")])
    assert n == 1
    assert captured[0][1][0][4] == "SRID=4326;POINT(120.2 23.0)"


def test_empty_input_skips_db(captured):
    assert vm.write_to_supabase(FakeConn(), []) == 0
    assert captured == []


def test_zeabur_path_table_map_matches_vm():
    """Zeabur 路徑（停用中）也要不指定目標的 DO NOTHING，避免重開時整批失敗。"""
    from storage.supabase_tables import TABLE_MAP
    cfg = TABLE_MAP["waste_positions"]
    assert cfg["upsert_strategy"] == "do_nothing"
    assert cfg["upsert_key"] == "city,vehicle_no,observed_at"
    assert cfg["columns"] == vm.COLUMNS
