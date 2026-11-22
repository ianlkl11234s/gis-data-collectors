"""進度（seen／hours_done／R2 事件檔）不可早於資料寫入成功前進（2026-11-22 清查）。

jma_amedas：整點補抓的 _hours_done 先前在 collect() 就標記，DB 寫入失敗後該整點永遠不重試。
"""
from __future__ import annotations

import json
from pathlib import Path
import pytest

from collectors.base import BaseCollector
from collectors.global_climate import jma_amedas

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "jma_p0_sample.json").read_text())


def _fake_base_run(result_error=None):
    def run(self):
        out = self.collect()
        return {"error": result_error} if result_error else {k: v for k, v in out.items() if k != "data"}
    return run


# ───────────────────────── jma_amedas ─────────────────────────

def _amedas(monkeypatch, tmp_path):
    calls = []

    def fake_fetch(session, path, as_text=False, **kw):
        calls.append(path)
        if path == jma_amedas.URL_LATEST_TIME:
            return "2026-01-01T00:40:00+09:00"
        if path == jma_amedas.URL_TABLE:
            return FIXTURE["amedastable"]
        return FIXTURE["amedas_map"]

    monkeypatch.setattr(jma_amedas, "fetch", fake_fetch)
    c = jma_amedas.JmaAmedasCollector.__new__(jma_amedas.JmaAmedasCollector)
    c.name, c._session = "jma_amedas", None
    c._stations, c._stations_loaded_at, c._hours_done = {}, None, set()
    c._pending_hours = None
    c._s3, c.parquet_dir, c.local_base = None, tmp_path / "pq", tmp_path / "jma_amedas"
    return c, calls


HOUR_URL = "amedas/data/map/20260101000000.json"


def test_amedas_require_db_write():
    assert jma_amedas.JmaAmedasCollector.require_db_write(None) is True


def test_amedas_hours_done_not_advanced_when_run_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(BaseCollector, "run", _fake_base_run("required Supabase write failed"))
    c, calls = _amedas(monkeypatch, tmp_path)
    c.run()
    assert c._hours_done == set(), "DB 寫入失敗，整點不可標成已完成"
    calls.clear()
    out = c.collect()
    assert HOUR_URL in calls and out["hourly_count"] > 0, "下一輪必須重新補抓該整點"


def test_amedas_hours_done_advanced_after_successful_run(monkeypatch, tmp_path):
    monkeypatch.setattr(BaseCollector, "run", _fake_base_run())
    c, calls = _amedas(monkeypatch, tmp_path)
    c.run()
    assert c._hours_done
    calls.clear()
    out = c.collect()
    assert out["hourly_count"] == 0 and HOUR_URL not in calls
