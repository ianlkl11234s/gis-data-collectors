"""震度圖事件檔（R2＋S3，2026-11-09）單元測試。不打真實 R2／S3／DB。"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from collectors.earthquake_shakemap_grid import (
    INDEX_KEY,
    build_event_payload,
    merge_index,
    publish_event,
    shakemap_event_key,
)


def _rows(event_time="2026-07-27T10:15:30+08:00", n=3):
    return [
        {
            "event_name": "測試地震", "event_time": event_time, "magnitude": 5.1,
            "eq_lon": 121.5, "eq_lat": 23.9, "depth": 10.0,
            "lon": 120.0 + i * 0.05, "lat": 22.0, "pga": 1.0 * i, "pgv": 0.1 * i,
            "intensity": i,
        }
        for i in range(n)
    ]


def test_key_is_utc():
    assert shakemap_event_key("2026-07-27T10:15:30+08:00") == "events/shakemap/20260727T021530Z.json"
    assert shakemap_event_key("2026-07-27T05:00:00+08:00") == "events/shakemap/20260726T210000Z.json"
    # 無時區＝台灣時間
    assert shakemap_event_key("2026-07-27T10:15:30") == "events/shakemap/20260727T021530Z.json"


def test_payload_columns_and_cells():
    p = build_event_payload(_rows())
    assert p["event_time"] == "2026-07-27T02:15:30Z"
    assert p["columns"] == ["lon", "lat", "pga", "pgv", "intensity"]
    assert len(p["cells"]) == 3 and p["cells"][2][4] == 2


def test_merge_index_dedupes_and_sorts():
    a = {"event_time": "2026-07-27T02:15:30Z", "key": "k1"}
    b = {"event_time": "2026-08-01T00:00:00Z", "key": "k2"}
    idx = merge_index(None, a)
    idx = merge_index(idx, b)
    idx = merge_index(idx, {**a, "cell_count": 9})
    assert [e["key"] for e in idx["events"]] == ["k2", "k1"]
    assert idx["events"][1]["cell_count"] == 9


def test_publish_writes_s3_r2_and_index():
    r2, s3 = MagicMock(), MagicMock()
    r2.head.return_value = None
    r2.get_json.return_value = None
    s3.head.return_value = None
    s3.upload_snapshot.return_value = True
    key = publish_event(r2, s3, _rows())
    assert key == "events/shakemap/20260727T021530Z.json"
    s3.upload_snapshot.assert_called_once()
    assert s3.upload_snapshot.call_args[0][0] == key
    keys = [c[0][0] for c in r2.put_json.call_args_list]
    assert keys == [key, INDEX_KEY]
    idx = r2.put_json.call_args_list[1][0][1]
    assert idx["events"][0]["felt_cell_count"] == 2


def test_publish_skips_existing_objects_but_refreshes_index():
    r2, s3 = MagicMock(), MagicMock()
    r2.head.return_value = {"ContentLength": 1}
    r2.get_json.return_value = {"events": []}
    s3.head.return_value = {"ContentLength": 1}
    publish_event(r2, s3, _rows())
    s3.upload_snapshot.assert_not_called()
    assert [c[0][0] for c in r2.put_json.call_args_list] == [INDEX_KEY]


def test_publish_s3_failure_raises_before_r2():
    r2, s3 = MagicMock(), MagicMock()
    s3.head.return_value = None
    s3.upload_snapshot.return_value = False
    with pytest.raises(RuntimeError):
        publish_event(r2, s3, _rows())
    r2.put_json.assert_not_called()


def test_publish_noop_without_r2():
    assert publish_event(None, MagicMock(), _rows()) is None
