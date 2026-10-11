"""AMeDAS 每日 Parquet（第二批 2026-10-11）單元測試：不打網路、不碰真 S3。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from collectors.global_climate import jma_amedas, jma_amedas_daily as dly
from storage.supabase_writer import SupabaseWriter

JST = timezone(timedelta(hours=9))
TPE = timezone(timedelta(hours=8))


def _row(sid, t: datetime, temp=10.0):
    iso = t.astimezone(JST).isoformat()
    r = {c: None for c in SupabaseWriter._JMA_AMEDAS_COLS}
    r.update(station_id=sid, station_name=f"s{sid}", observed_at=iso, source_time=iso,
             collected_at=t.astimezone(TPE).isoformat(), temp=temp, wind_dir=3.0, has_snow_gauge=False)
    return {**r, "_type": "current"}


def _write_local(base: Path, t: datetime, sids=("11001", "11016"), dup_obs=None):
    """模擬 storage.save：目錄是「台北時間」收集時刻。"""
    tt = t.astimezone(TPE)
    d = base / f"{tt:%Y}" / f"{tt:%m}" / f"{tt:%d}"
    d.mkdir(parents=True, exist_ok=True)
    obs = dup_obs or t
    rows = [_row(s, obs) for s in sids]
    (d / f"jma_amedas_{tt:%H%M}.json").write_text(json.dumps({"data": rows + [{**rows[0], "_type": "observation"}]}))


def test_closable_days_respects_first_day_and_0100_jst():
    now = datetime(2026, 10, 12, 0, 59, tzinfo=JST)
    assert dly.closable_days(now, "20261005") == ["20261006", "20261007", "20261008", "20261009", "20261010"]
    assert dly.closable_days(now + timedelta(minutes=1), "20261010") == ["20261010", "20261011"]


def test_day_spans_two_taipei_dirs_dedup_and_gap_fill(tmp_path):
    base = tmp_path / "jma_amedas"
    day = "20261011"
    slots = dly.day_slots(day)
    assert slots[0] == datetime(2026, 10, 11, 0, 0, tzinfo=JST) and len(slots) == 144
    for t in slots[:-2]:
        _write_local(base, t)
    # 同一 observed_at 出現在第二個檔（latest_time 沒前進）→ 不重複
    _write_local(base, slots[5] + timedelta(minutes=3), dup_obs=slots[5])
    # 前一天的檔（不屬於 D）不可混入
    _write_local(base, slots[0] - timedelta(minutes=10))
    assert (base / "2026" / "10" / "10").is_dir(), "JST 00:00–00:59 落在台北 D-1 目錄"
    fills = []

    def fetch_slot(t):
        fills.append(t)
        return [_row("11001", t, temp=99.0)] if t == slots[-2] else []

    out = tmp_path / "o" / f"{day}.parquet"
    info = dly.build_day_parquet(day, out, local_base=base, fetch_slot=fetch_slot)
    assert fills == slots[-2:]
    assert info["slots_local"] == 142 and info["slots_filled"] == 1 and info["slots_missing"] == 1
    assert info["rows"] == 142 * 2 + 1
    t = pq.read_table(out)
    assert t.num_rows == info["rows"]
    assert [f.name for f in t.schema] == SupabaseWriter._JMA_AMEDAS_COLS
    obs = t.column("observed_at").to_pylist()
    assert min(obs) == slots[0] and max(obs) == slots[-2]
    keys = list(zip(t.column("station_id").to_pylist(), obs))
    assert len(keys) == len(set(keys))
    assert t.column("wind_dir").type.bit_width == 16
    assert b"jma_amedas_daily" in t.schema.metadata


def test_zero_rows_is_error_not_empty_file(tmp_path):
    import pytest
    with pytest.raises(RuntimeError):
        dly.build_day_parquet("20261011", tmp_path / "x.parquet", local_base=tmp_path, fetch_slot=lambda t: [])
    assert not (tmp_path / "x.parquet").exists()


class FakeS3:
    def __init__(self):
        self.objects, self.uploads = {}, []

    def head(self, key):
        return self.objects.get(key)

    def upload_path(self, local, key, *, storage_class, metadata, content_type):
        self.uploads.append(key)
        self.objects[key] = {"ContentLength": Path(local).stat().st_size,
                             "Metadata": {k: str(v) for k, v in metadata.items()}, "StorageClass": storage_class}
        return True


def _collector(tmp_path, s3):
    c = jma_amedas.JmaAmedasCollector.__new__(jma_amedas.JmaAmedasCollector)
    c.name, c._session, c._s3 = "jma_amedas", None, s3
    c._stations, c._stations_loaded_at, c._hours_done = {}, None, set()
    c.parquet_dir = tmp_path / "state" / "jma_amedas_parquet"
    c.local_base = tmp_path / "jma_amedas"
    return c


def test_close_daily_uploads_once_with_receipt(tmp_path, monkeypatch):
    s3 = FakeS3()
    c = _collector(tmp_path, s3)
    monkeypatch.setattr(c, "_fetch_slot", lambda t: [_row("11001", t)])
    c.parquet_dir.mkdir(parents=True)
    (c.parquet_dir / "_first_day.json").write_text(json.dumps({"first_day": "20261011"}))
    assert c.close_daily(datetime(2026, 10, 12, 0, 30, tzinfo=JST)) is None, "01:00 JST 前不日結"
    res = c.close_daily(datetime(2026, 10, 12, 1, 10, tzinfo=JST))
    assert [d["day"] for d in res["closed"]] == ["20261011"] and res["closed"][0]["rows"] == 144
    key = "weather-obs/jma/amedas/2026/10/20261011.parquet"
    assert s3.uploads == [key] and s3.objects[key]["StorageClass"] == "STANDARD"
    receipt = json.loads((c.parquet_dir / "20261011.json").read_text())
    assert receipt["sha256"] == s3.objects[key]["Metadata"]["sha256"]
    assert not (c.parquet_dir / "20261011.parquet").exists(), "上傳驗證後刪本機檔"
    assert c.close_daily(datetime(2026, 10, 12, 1, 20, tzinfo=JST)) is None and s3.uploads == [key]


def test_close_daily_first_run_sets_first_day_no_backfill(tmp_path, monkeypatch):
    c = _collector(tmp_path, FakeS3())
    monkeypatch.setattr(c, "_fetch_slot", lambda t: (_ for _ in ()).throw(AssertionError("不該回補上線前")))
    assert c.close_daily(datetime(2026, 10, 12, 5, 0, tzinfo=JST)) is None
    assert json.loads((c.parquet_dir / "_first_day.json").read_text())["first_day"] == "20261012"


def test_close_daily_disabled(tmp_path, monkeypatch):
    monkeypatch.setattr(jma_amedas.config, "JAPAN_JMA_AMEDAS_DAILY_PARQUET", False)
    assert _collector(tmp_path, FakeS3()).close_daily(datetime(2026, 10, 12, 5, 0, tzinfo=JST)) is None
