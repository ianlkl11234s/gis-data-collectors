"""
TpmlSeatCollector 的 parse 測試（不發真實 HTTP）

驗證：
1. 欄位映射：areaId→area_id / branchName→branch_name / floorName→floor_name /
   areaName→area_name / freeCount→free_count / totalCount→total_count
2. is_closed 邏輯：全區 freeCount==0 → 該輪所有 record is_closed=True；
   任一區有空位 → 全部 False
"""

from unittest.mock import MagicMock

import pytest


FIXTURE_OPEN = [
    {"areaId": 1, "branchName": "總館", "floorName": "B1", "areaName": "自習室", "freeCount": 12, "totalCount": 60},
    {"areaId": 2, "branchName": "總館", "floorName": "2F", "areaName": "閱覽區", "freeCount": 0, "totalCount": 40},
    {"areaId": 3, "branchName": "文山分館", "floorName": "3F", "areaName": "自修室", "freeCount": 5, "totalCount": 30},
]

FIXTURE_CLOSED = [
    {"areaId": 1, "branchName": "總館", "floorName": "B1", "areaName": "自習室", "freeCount": 0, "totalCount": 60},
    {"areaId": 2, "branchName": "總館", "floorName": "2F", "areaName": "閱覽區", "freeCount": 0, "totalCount": 40},
    {"areaId": 3, "branchName": "文山分館", "floorName": "3F", "areaName": "自修室", "freeCount": 0, "totalCount": 30},
]


def _make_collector(fixture):
    """建構 TpmlSeatCollector 但繞過 BaseCollector __init__（避免 storage / supabase 連線）"""
    from collectors.tpml_seat import TpmlSeatCollector

    coll = TpmlSeatCollector.__new__(TpmlSeatCollector)
    coll.storage = MagicMock()
    coll.supabase_writer = None

    fake_resp = MagicMock()
    fake_resp.json.return_value = fixture
    fake_resp.raise_for_status.return_value = None
    coll._session = MagicMock()
    coll._session.get.return_value = fake_resp
    return coll


def test_collect_open_field_mapping():
    """開館情境：欄位映射正確、is_closed 全 False"""
    coll = _make_collector(FIXTURE_OPEN)
    result = coll.collect()

    assert result["area_count"] == 3
    assert result["branch_count"] == 2
    assert result["is_closed"] is False
    assert "error" not in result

    r = result["data"][0]
    assert r["area_id"] == 1
    assert r["branch_name"] == "總館"
    assert r["floor_name"] == "B1"
    assert r["area_name"] == "自習室"
    assert r["free_count"] == 12
    assert r["total_count"] == 60
    assert r["is_closed"] is False
    # 來源無 timestamp → observed_at = 收集當下（= collected_at）
    assert r["observed_at"] == r["collected_at"]

    assert all(rec["is_closed"] is False for rec in result["data"])


def test_collect_all_zero_marks_closed():
    """閉館情境：全區 freeCount==0 → 該輪所有 record is_closed=True"""
    coll = _make_collector(FIXTURE_CLOSED)
    result = coll.collect()

    assert result["area_count"] == 3
    assert result["is_closed"] is True
    assert all(rec["is_closed"] is True for rec in result["data"])
    assert result["free_total"] == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
