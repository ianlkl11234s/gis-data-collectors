"""launch 歷史回溯只補保留窗內（gis-platform 544：live.launches 依 net 只留 30 天）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import collectors.launch as launch_mod
from collectors.launch import LaunchCollector


def _collector(pages):
    c = LaunchCollector.__new__(LaunchCollector)
    c.name = "launch"
    c._backfill_complete = False
    c._backfill_offset = 0
    c._fetch_previous_launches = MagicMock(side_effect=pages)
    return c


def _launch(days_ago: float) -> dict:
    net = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return {"id": f"l{days_ago}", "net": net.isoformat().replace("+00:00", "Z")}


def test_cutoff_matches_supabase_retention():
    assert launch_mod.BACKFILL_CUTOFF_DAYS == 30


def test_page_crossing_cutoff_filters_old_and_completes():
    page = [_launch(1), _launch(20), _launch(29), _launch(31), _launch(400)]
    c = _collector([(page, 5000)])
    got = c._backfill_history()
    assert [l["id"] for l in got] == ["l1", "l20", "l29"]
    assert c._backfill_complete is True
    # 已完成就不再呼叫 API
    assert c._backfill_history() == []
    assert c._fetch_previous_launches.call_count == 1


def test_page_inside_window_continues_next_offset():
    page = [_launch(1), _launch(10)]
    c = _collector([(page, 5000)])
    got = c._backfill_history()
    assert len(got) == 2
    assert c._backfill_complete is False
    assert c._backfill_offset == 100
