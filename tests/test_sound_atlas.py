"""城市聲景（sound_atlas/）：缺資料＝無法判斷、即時值 ⊑ 每日值、分片合併、發布與排程。

合成資料測試不需網路與原始檔。與前端現有每日檔（11-09／10-24）的逐位元比對需要本機原始匯出，
設 SOUND_ATLAS_TEST_RAW（放 s3raw/、radar_cache/、static/ 的目錄）與 SOUND_ATLAS_FRONTEND（前端 checkout）才跑。
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import random
from pathlib import Path

import numpy as np
import pytest

from sound_atlas import aviation, common, freeway_vd, live, publish, radar
from sound_atlas.common import GRID_CELLS, UNKNOWN, RawFile, RawSource, day_window, slot_start_utc

DATE = "2026-11-09"


# ── 契約 ────────────────────────────────────────────────────────

def test_contract_manifest_matches_bundled_files():
    m = common.contract_json("manifest.json")
    assert hashlib.sha256(common.contract_bytes("grid_spec.json")).hexdigest() == common.GRID_SHA256
    assert m["files"]
    for name, meta in m["files"].items():
        assert hashlib.sha256(common.contract_bytes(name)).hexdigest() == meta["sha256"], name


@pytest.mark.skipif(not os.environ.get("SOUND_ATLAS_FRONTEND"), reason="需要前端 checkout（SOUND_ATLAS_FRONTEND）")
def test_contract_matches_frontend_checkout():
    fe = Path(os.environ["SOUND_ATLAS_FRONTEND"])
    m = common.contract_json("manifest.json")
    checked = 0
    for name, meta in m["files"].items():
        p = fe / meta["source"]
        if not p.exists():
            continue  # 部分來源是 CDN 資產（git 忽略），worktree 不一定有
        checked += 1
        want = meta.get("source_sha256", meta["sha256"])
        assert hashlib.sha256(p.read_bytes()).hexdigest() == want, f"前端 {meta['source']} 已改，重跑 scripts/sound_atlas_build_contract.py"
    assert checked >= 4


# ── 雷達：缺幀＝255、即時雜波只會多標 ─────────────────────────────

class FakeDecoder:
    def decode(self, body):
        return np.frombuffer(body, dtype=np.uint8).copy(), {}, set()


class FakeFetcher:
    def __init__(self, frames: dict[int, np.ndarray], date: str = DATE):
        self.by_key = {radar.frame_key(slot_start_utc(date, k)): v.tobytes() for k, v in frames.items()}

    def get(self, key):
        b = self.by_key.get(key)
        return ("ok", b) if b is not None else ("missing", None)


def _frames(seed: int = 1, missing=(3, 70)) -> dict[int, np.ndarray]:
    rng = np.random.default_rng(seed)
    out = {}
    for k in range(144):
        if k in missing:
            continue
        c = np.zeros(GRID_CELLS, np.uint8)
        idx = rng.choice(GRID_CELLS, 400, replace=False)
        c[idx] = rng.integers(1, 66, 400)  # 0–64 dBZ 碼
        c[rng.choice(GRID_CELLS, 20, replace=False)] = UNKNOWN
        out[k] = c
    return out


def test_radar_daily_missing_frame_is_all_unknown():
    frames = _frames()
    payload, meta = radar.build_daily(DATE, FakeFetcher(frames), decoder=FakeDecoder(), parallel=1)
    cls = np.frombuffer(gzip.decompress(payload), np.uint8).reshape(144, -1)
    assert [f["index"] for f in meta["frames_missing"]] == [3, 70]
    assert (cls[3] == UNKNOWN).all() and (cls[70] == UNKNOWN).all()
    assert (cls[5] != UNKNOWN).any()


def test_radar_live_is_subset_of_daily():
    """逐槽即時（前一幀狀態依到達順序）合併後：每格不是等於每日值，就是 255（多標），不會反過來"""
    frames = _frames(seed=2)
    payload, _ = radar.build_daily(DATE, FakeFetcher(frames), decoder=FakeDecoder(), parallel=1)
    daily = np.frombuffer(gzip.decompress(payload), np.uint8).reshape(144, -1)
    day = live.LiveDay(DATE)
    inp = live.LiveInputs(raw=RawSource(), radar_fetcher=FakeFetcher(frames))
    start, _ = day_window(DATE)
    shards = []
    for k in range(144 + live.TAIL_SLOTS):
        now = start + k * 600 + 540
        shards.append({"slot": k, "sources": {"radar": day.run_radar(inp, now, FakeDecoder())}})
    merged = live.merge_day(DATE, shards, None, None)
    assert merged["radar"]["missing"] == {3, 70}
    for k, b in merged["radar"]["frames"].items():
        lv = np.frombuffer(b, np.uint8)
        d = daily[k]
        assert not ((lv != UNKNOWN) & (lv != d)).any(), k  # 即時有值 → 必等於每日值
    assert set(merged["radar"]["frames"]) == set(range(144)) - {3, 70}


def test_radar_live_frame_not_yet_published_is_not_declared_missing():
    day = live.LiveDay(DATE)
    inp = live.LiveInputs(raw=RawSource(), radar_fetcher=FakeFetcher({}))
    start, _ = day_window(DATE)
    out = day.run_radar(inp, start + 540, FakeDecoder())
    assert out["frames"] == [] and out["missing"] == []          # 還沒過 1 小時：前端當「尚未知道」
    out = day.run_radar(inp, start + live.RADAR_MISSING_AFTER_S + 60, FakeDecoder())
    assert 0 in out["missing"]                                   # 過 1 小時：宣告缺（無法判斷），不是 0


# ── VD：即時值 ⊑ 每日值 ──────────────────────────────────────────

def _vd_rows(rng: random.Random) -> list[freeway_vd.VdRow]:
    rows = []
    for s in range(144):
        if rng.random() < 0.05:
            continue
        status = 1 if rng.random() < 0.05 else 0
        busy = 30 <= s <= 130
        total = 0 if (status or rng.random() < (0.08 if busy else 0.4)) else rng.randint(1, 60 if busy else 8)
        speed = 0.0 if total == 0 and rng.random() < 0.8 else 80.0
        rows.append(freeway_vd.VdRow(s, total, speed, 0.0 if speed == 0 else 5.0, 2, status))
        if rng.random() < 0.03:
            rows.append(freeway_vd.VdRow(s, rng.randint(0, 200), 60.0, 3.0, 2, 0))  # 同槽重複、偶爾超量
    return rows


@pytest.mark.parametrize("seed", range(30))
def test_vd_live_values_are_subset_of_daily(seed):
    rng = random.Random(seed)
    rows = _vd_rows(rng)
    if seed % 7 == 0:
        rows = [freeway_vd.VdRow(r.slot, 0, 0.0, 0.0, r.lanes, r.status) for r in rows]  # 整日全零
    day_vals, _reasons, _state = freeway_vd.clean_vd_day(rows)
    for s in range(144):
        so_far = [r for r in rows if r.slot <= s]
        v, why = freeway_vd.clean_vd_live(so_far, [s])[s]
        if v is not None:
            assert v == day_vals[s], (seed, s, v, day_vals[s])
        else:
            assert why  # 每個 None 都有原因


def test_vd_missing_rows_are_none_not_zero():
    res = freeway_vd.clean_vd_live([], [10, 11])
    assert res[10][0] is None and res[11][0] is None


# ── 飛機：讀取失敗＝error、沒設定＝disabled、片段合併 ────────────────

def test_aviation_positions_failure_is_error_not_empty():
    day = live.LiveDay(DATE)
    start, _ = day_window(DATE)
    inp = live.LiveInputs(raw=RawSource(), radar_fetcher=FakeFetcher({}), positions=lambda t0, t1: None)
    out = day.run_aviation(inp, start + 1200)
    assert out["status"] == "error" and out["rows"] == []
    assert day.aviation_watermark == start  # 失敗不前進：下一次重讀同一段
    inp2 = live.LiveInputs(raw=RawSource(), radar_fetcher=FakeFetcher({}), positions=None)
    assert day.run_aviation(inp2, start + 1200)["status"] == "disabled"


def test_aviation_fragments_merge_to_daily_trail():
    bbox = common.grid_bbox()
    lat, lng = (bbox[1] + bbox[3]) / 2, (bbox[0] + bbox[2]) / 2
    start, _ = day_window(DATE)
    pts = [{"flight_id": "abc123", "callsign": "TEST1", "aircraft_type": None, "origin": None, "destination": "TPE",
            "pt": f"{lat + i * 0.01},{lng},{1000 + i},{start + 60 * i}", "ts": start + 60 * i, "lat": lat + i * 0.01, "lng": lng}
           for i in range(30)]
    av = aviation.AviationLive(DATE, bbox)
    frags = av.window(pts[:10]) + av.window(pts[10:20]) + av.window(pts[10:30])  # 重送一段（冪等）
    merged = aviation.merge_fragments(frags)
    assert len(merged) == 1
    assert merged[0]["trail"] == ";".join(p["pt"] for p in pts)
    assert merged[0]["callsign"] == "TEST1" and merged[0]["aircraft_type"] == ""


# ── 分片：單族失敗、合併、缺片 ────────────────────────────────────

class _BoomRaw(RawSource):
    def files(self, collector, date):
        if collector == "freeway_vd":
            raise OSError("disk gone")
        return None


def test_run_slot_isolates_family_failure():
    day = live.LiveDay(DATE)
    start, _ = day_window(DATE)
    inp = live.LiveInputs(raw=_BoomRaw(), radar_fetcher=FakeFetcher({}), positions=lambda t0, t1: [])
    doc = live.run_slot(day, inp, start + 3 * 600 + 540, set(), FakeDecoder())
    assert doc["slot"] == 3
    assert doc["sources"]["freeway_vd"]["status"] == "error"
    assert doc["sources"]["aviation"]["status"] == "ok"
    assert doc["sources"]["rain_gauge"]["status"] == "no_new_data"


def test_merge_day_missing_shard_leaves_unknown():
    ids = freeway_vd.used_vd_ids()
    v = [5] * len(ids)
    shards = [{"slot": 0, "sources": {"freeway_vd": {"slots": {"0": {"fetch_unix": 0, "v": v}}}}},
              {"slot": 2, "sources": {"freeway_vd": {"slots": {"2": {"fetch_unix": 0, "v": v}}}}}]
    m = live.merge_day(DATE, shards, None, None)
    assert m["vd_slots"] == {0, 2}
    assert m["freeway_vd"][ids[0]][1] is None   # 缺片的槽＝None（無法判斷），不是 0
    assert m["freeway_vd"][ids[0]][2] == 5
    h = live.rollup(DATE, 0, [{"slot": 0, "run_at": "", "sources": {}}, {"slot": 2, "run_at": "", "sources": {}}])
    assert h["slots_missing"] == [1, 3, 4, 5]


def test_merge_is_idempotent_on_resend():
    """發布成功但狀態沒存到 → 下一槽重送同一批；合併結果不變"""
    rows = [{"station_id": "A", "bucket_at": "2026-11-09T00:00:00+08:00", "observed_at": "2026-11-09T00:10:00+08:00", "precipitation_10min": 0.5}]
    a = live.merge_day(DATE, [{"slot": 1, "sources": {"rain_gauge": {"rows": rows}}}], None, None)
    b = live.merge_day(DATE, [{"slot": 1, "sources": {"rain_gauge": {"rows": rows}}},
                              {"slot": 2, "sources": {"rain_gauge": {"rows": rows}}}], None, None)
    assert a["rain_gauge_rows"] == b["rain_gauge_rows"]


# ── 發布與排程 ──────────────────────────────────────────────────

def test_local_sink_is_immutable_and_gzip(tmp_path):
    s = publish.LocalDirSink(str(tmp_path))
    key = publish.live_slot_key(DATE, 7)
    assert key == "sound-atlas/v1/live/2026-11-09/slot-007.json"
    assert s.put(key, b'{"a":1}') is True
    assert s.put(key, b'{"a":2}') is False
    assert s.read_json(key) == {"a": 1}


def test_make_sink_defaults_to_local(tmp_path, monkeypatch):
    called = []
    import storage.r2 as r2
    monkeypatch.setattr(r2, "get_r2_storage", lambda: called.append(1))
    assert isinstance(publish.make_sink(False, str(tmp_path)), publish.LocalDirSink)
    assert called == []


def test_live_task_skips_republished_slot_and_rolls_up_hour(tmp_path, monkeypatch):
    from tasks import sound_atlas as task
    monkeypatch.setattr(live, "river_baseline_doc", lambda d, b, f: {"schema": live.BASELINE_SCHEMA, "date_taiwan": d,
                                                                         "history_basis": "t", "stations": [{"id": "S1"}]})
    sink = publish.LocalDirSink(str(tmp_path / "out"))
    inp = live.LiveInputs(raw=RawSource(), radar_fetcher=FakeFetcher({}), positions=lambda t0, t1: [])
    t = task.SoundAtlasLive(sink=sink, inputs=inp, root=tmp_path)
    t.decoder = FakeDecoder()
    start, _ = day_window(DATE)
    r = t.run_day(DATE, start + 5 * 600 + 540)
    assert r["slot"] == 5
    assert sink.read_json(publish.live_hour_key(DATE, 0))["slots_missing"] == [0, 1, 2, 3, 4]
    assert sink.read_json(publish.live_baseline_key(DATE))["stations"] == [{"id": "S1"}]
    assert "skipped" in t.run_day(DATE, start + 5 * 600 + 580)
    # 重啟（狀態已存）仍跳過
    t2 = task.SoundAtlasLive(sink=sink, inputs=inp, root=tmp_path)
    assert "skipped" in t2.run_day(DATE, start + 5 * 600 + 590)


def test_live_schedule_minutes_default():
    from tasks import sound_atlas as task
    assert task.live_schedule_minutes() == [9, 19, 29, 39, 49, 59]


def test_toggles_default_off(monkeypatch):
    import importlib
    import config
    for k in ("SOUND_ATLAS_LIVE_ENABLED", "SOUND_ATLAS_DAILY_ENABLED", "SOUND_ATLAS_UPLOAD_ENABLED"):
        monkeypatch.delenv(k, raising=False)
    importlib.reload(config)
    assert not config.SOUND_ATLAS_LIVE_ENABLED and not config.SOUND_ATLAS_DAILY_ENABLED and not config.SOUND_ATLAS_UPLOAD_ENABLED


# ── 與前端現有每日檔逐項比對（需本機原始匯出）────────────────────────

RAW = os.environ.get("SOUND_ATLAS_TEST_RAW")
FE = os.environ.get("SOUND_ATLAS_FRONTEND")


def _strip(d, keys=("generated_at", "notes_zh", "origin")):
    if isinstance(d, dict):
        return {k: _strip(v, keys) for k, v in d.items() if k not in keys}
    if isinstance(d, list):
        return [_strip(x, keys) for x in d]
    return d


@pytest.mark.skipif(not (RAW and FE), reason="需要 SOUND_ATLAS_TEST_RAW 與 SOUND_ATLAS_FRONTEND")
@pytest.mark.parametrize("date", ["2026-11-09", "2026-10-24"])
def test_daily_parity_with_frontend_files(date):
    from sound_atlas import daily
    grid = Path(FE) / "public/sound-atlas/grid"
    ref_vd = json.loads((grid / f"freeway-vd-{date}.json").read_bytes())
    available = {v for v, x in ref_vd["vds"].items() if x["day"] != "not_in_export"}

    def trails(d):
        p = Path(RAW) / "s3raw" / f"trails__flights__{d}.json.gz"
        return aviation.trails_from_gz(p.read_bytes()) if p.exists() else None

    inp = daily.DailyInputs(raw=RawSource(archive_dir=f"{RAW}/s3raw"),
                            radar_fetcher=radar.FrameFetcher(cache_dir=f"{RAW}/radar_cache", base_url=None),
                            trails=trails, static=common.natural_static(f"{RAW}/static"), vd_available=available)
    out, rep = daily.build_all(date, inp)
    assert rep["errors"] == {}
    assert out[f"radar-rain-{date}.class.bin"] == (grid / f"radar-rain-{date}.class.bin").read_bytes()
    for name, b in out.items():
        if name.endswith(".bin"):
            continue
        ref = json.loads((grid / name).read_bytes())
        mine = json.loads(b)
        if name.startswith("freeway-vd"):
            # 舊檔 qa.rules 是當時的 docstring 原文；其餘欄位逐值相同
            mine["qa"].pop("rules"); ref["qa"].pop("rules")
            mine.pop("source", None); ref.pop("source", None)
        assert _strip(mine) == _strip(ref), name
