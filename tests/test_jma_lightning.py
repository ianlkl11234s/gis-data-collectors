"""jma_lightning（JMA LIDEN 落雷，C4）單元測試：不打網路、不打 DB。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import config
from collectors.global_climate import jma_lightning as L
from storage.supabase_tables import TABLE_MAP
from storage.supabase_writer import SupabaseWriter

COLLECTED = "2026-10-12T09:00:00+08:00"


def _feat(lon, lat, t, typ, idx=0):
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"id": f"nowc_x_liden_{idx}", "obstimeJST": t, "type": typ}}


GEO = {"type": "FeatureCollection", "features": [
    _feat(131.198, 28.183, "2026/10/10 18:55:28.860", 0, 0),
    _feat(130.941, 28.183, "2026/10/10 18:55:29.550", 4, 1),
    _feat(130.000, 28.000, "2026/10/10 18:56:00.000", 1, 2),
    _feat(130.500, 28.500, "2026/10/10 18:57:00.000", 7, 3),        # 未知 type
    _feat(130.941, 28.183, "2026/10/10 18:55:29.550", 4, 4),        # 同幀重複
    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [None, 28.1]},
     "properties": {"obstimeJST": "2026/10/10 18:58:00.000", "type": 0}},  # 缺座標
    _feat(130.1, 28.1, "bad", 0, 6),                                  # 時間無法解析
]}


def test_parse_liden_types_time_and_nulls():
    rows, unknown, dropped = L.parse_liden(GEO, "20261010100000", COLLECTED)
    assert len(rows) == 4 and dropped == 2
    by_type = {r["source_type"]: r for r in rows}
    assert by_type[4]["strike_type"] == 0          # 対地放電 → 雲對地
    assert by_type[0]["strike_type"] == 1          # 雲放電 → 雲中
    assert by_type[1]["strike_type"] == 1
    assert by_type[7]["strike_type"] is None       # 未知 → NULL，不猜
    assert unknown == {"7": 1}
    # JST 毫秒 → UTC ISO，毫秒保留
    assert by_type[0]["strike_time"] == "2026-10-10T09:55:28.860+00:00"
    assert all(r["source"] == "jma" and r["intensity_ka"] is None for r in rows)
    assert len({r["dedup_hash"] for r in rows}) == 4
    assert len({r["event_id"] for r in rows}) == 4


def test_empty_frame_is_not_error():
    rows, unknown, dropped = L.parse_liden({"type": "FeatureCollection", "features": []}, "x", COLLECTED)
    assert rows == [] and not unknown and dropped == 0


def test_decode_body_handles_raw_gzip():
    import gzip
    import json
    body = gzip.compress(json.dumps({"features": []}).encode())
    assert L.decode_body(body) == {"features": []}
    assert L.decode_body(b'{"a": 1}') == {"a": 1}


def test_frame_grid_aligned_and_latest_from_target_times():
    start = datetime(2026, 10, 12, 0, 2, tzinfo=timezone.utc)
    grid = L.frame_grid(start, datetime(2026, 10, 12, 0, 20, tzinfo=timezone.utc))
    assert grid == ["20261012000500", "20261012001000", "20261012001500", "20261012002000"]
    tt = [{"basetime": "a", "validtime": "20261012004000", "elements": ["thns"]},
          {"basetime": "b", "validtime": "20261011234500", "elements": ["liden"]},
          {"basetime": "c", "validtime": "20261011233000", "elements": ["liden", "slmcs"]}]
    assert L.latest_liden_vt(tt) == datetime(2026, 10, 11, 23, 45, tzinfo=timezone.utc)


def _collector(tmp_path, monkeypatch, frames: dict, now=None):
    monkeypatch.setattr(config, "JAPAN_JMA_LIGHTNING_BACKFILL_HOURS", 1)
    monkeypatch.setattr(config, "JAPAN_JMA_LIGHTNING_MAX_FRAMES", 50)
    monkeypatch.setattr(config, "JAPAN_JMA_LIGHTNING_SETTLE_MINUTES", 10)
    monkeypatch.setattr(L.time, "sleep", lambda s: None)
    c = L.JmaLightningCollector.__new__(L.JmaLightningCollector)
    c.name = "jma_lightning"
    c.state_path = tmp_path / "state.json"
    c._state = None
    c._pending_state = None
    latest = datetime(2026, 10, 12, 1, 0, tzinfo=timezone.utc)
    c._latest_vt = lambda now: latest
    c._fetch_frame = lambda vt: frames.get(vt, ("ok", {"features": []}))
    return c, latest


def test_plan_backfills_missing_frames_newest_first(tmp_path, monkeypatch):
    c, latest = _collector(tmp_path, monkeypatch, {})
    state = {"done": {L.vt_str(latest - timedelta(minutes=30)): 0}, "miss": {}}
    plan = c.plan_frames(latest, latest, state)
    assert plan[0] == "20261012010000"            # 最新幀優先
    assert "20261012003000" not in plan            # 已完成不重抓
    assert len(plan) == 12                         # 13 格點 − 1 已完成


def test_state_advances_only_after_success_and_settle(tmp_path, monkeypatch):
    geo = {"features": [_feat(130.0, 30.0, "2026/10/12 09:58:00.000", 4)]}
    c, latest = _collector(tmp_path, monkeypatch, {"20261012010000": ("ok", geo),
                                                    "20261012005500": ("missing", None)})
    real_now = datetime(2026, 10, 12, 1, 3, tzinfo=timezone.utc)

    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return real_now if tz is timezone.utc else real_now.astimezone(tz)
    monkeypatch.setattr(L, "datetime", _DT)
    out = c.collect()
    assert out["strike_count"] == 1 and len(out["data"]) == 1
    st = c._pending_state
    assert "20261012010000" not in st["done"]      # 未滿 settle 10 分 → 下輪再抓
    assert st["done"]["20261012004500"] == 0       # 空幀照樣標完成
    assert st["miss"]["20261012005500"] == 1 and "20261012005500" not in st["done"]
    # collect 本身不落盤；只有 run() 成功後才寫
    assert not c.state_path.exists()


def test_run_does_not_save_state_on_db_failure(tmp_path, monkeypatch):
    c, _ = _collector(tmp_path, monkeypatch, {})
    c.storage = MagicMock()
    c.supabase_writer = MagicMock()
    c.supabase_writer.write.return_value = False
    c.run_count = c.error_count = c.consecutive_errors = 0
    c.last_run = c.last_success_at = None
    monkeypatch.setattr("collectors.base.notify_error", lambda *a, **k: None)
    monkeypatch.setattr("collectors.base.notify_success", lambda *a, **k: None)
    geo = {"features": [_feat(130.0, 30.0, "2026/10/12 09:30:00.000", 4)]}
    c._fetch_frame = lambda vt: ("ok", geo)
    stats = c.run()
    assert stats.get("error")
    assert not c.state_path.exists()
    c.supabase_writer.write.return_value = True
    stats = c.run()
    assert not stats.get("error") and c.state_path.exists()


def test_missing_frame_gives_up_after_retries(tmp_path, monkeypatch):
    c, latest = _collector(tmp_path, monkeypatch, {"20261012003000": ("missing", None)})
    c._state = {"done": {}, "miss": {"20261012003000": L.MISSING_GIVE_UP - 1}}
    c.collect()
    assert c._pending_state["done"]["20261012003000"] == "missing"


def test_transform_adds_source_type_and_geom():
    w = SupabaseWriter.__new__(SupabaseWriter)
    rows, _, _ = L.parse_liden(GEO, "20261010100000", COLLECTED)
    recs = w._transform_jma_lightning({"data": rows}, datetime.now(timezone.utc))
    cols = TABLE_MAP["jma_lightning"]["columns"]
    assert "source_type" in cols
    assert all(set(cols) <= set(r) for r in recs)
    assert {r["source_type"] for r in recs} == {0, 1, 4, 7}
    assert all(r["source"] == "jma" and r["geom"].startswith("SRID=4326;POINT(") for r in recs)
    assert SupabaseWriter.TRANSFORMERS["jma_lightning"]


@pytest.mark.parametrize("name", ["jma_lightning"])
def test_registered(name):
    from collectors.registry import COLLECTOR_REGISTRY
    assert any(e.cls.name == name for e in COLLECTOR_REGISTRY)
    defaults = {p: en for p, en, _ in config._COLLECTOR_TOGGLES}
    assert defaults["JAPAN_JMA_LIGHTNING"] is False
