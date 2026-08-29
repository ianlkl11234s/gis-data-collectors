from datetime import date

import pytest

from tasks import gfw_v4_live_source as source


def _presence(*, raw=False, complete=True):
    return {"raw_response_saved": raw, "resolved_dataset_versions": ["public-global-presence:v4.0"],
            "tiles": [{"next_offset_complete": complete}] * 42}


def _wire(monkeypatch, *, presence=None, quality=None):
    calls = []
    def fake_presence(**kw):
        calls.append(kw)
        output = kw["output_path"]
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text('{"vessel_id":"v"}\n', encoding="utf-8")
        return {**(presence or _presence()), "normalized_row_count": 1}
    monkeypatch.setattr(source, "fetch_presence_phase", fake_presence)
    monkeypatch.setattr(source, "fetch_fishing_effort_phase", lambda **kw: ([{"x": 1}], {"resolved_dataset_versions": ["public-global-fishing-effort:v4.0"], "raw_response_saved": False}))
    monkeypatch.setattr(source, "normalize_fishing_effort", lambda *a, **kw: ([{"cell": [1, 2]}], quality or {"invalid_rows": 0, "negative_hours_rejected": 0, "wrong_day_rows": 0, "boundary_overlap_rows": 0}))
    return calls


def test_live_source_uses_high_42_tile_presence_and_independent_effort(monkeypatch, tmp_path):
    calls = _wire(monkeypatch)
    result = source.fetch_normalized_v4_daily_source(token="t", selected_day=date(2026, 8, 21), work_dir=tmp_path / "work", client_factory=lambda token: object())
    assert calls[0]["resolution"] == "HIGH"
    assert result["raw_response_saved"] is False
    assert result["presence"]["records"]["kind"] == "normalized_spool_descriptor"
    assert result["presence"]["records"]["row_count"] == 1
    assert result["fishing_effort"]["records"]["kind"] == "normalized_spool_descriptor"
    assert result["fishing_effort"]["records"]["row_count"] == 1


@pytest.mark.parametrize("presence", [_presence(raw=True), _presence(complete=False)])
def test_live_source_rejects_raw_or_incomplete_presence(monkeypatch, tmp_path, presence):
    _wire(monkeypatch, presence=presence)
    with pytest.raises(source.GFWV4LiveSourceError):
        source.fetch_normalized_v4_daily_source(token="t", selected_day=date(2026, 8, 21), work_dir=tmp_path / "work", client_factory=lambda token: object())


def test_live_source_rejects_invalid_fishing_normalization(monkeypatch, tmp_path):
    _wire(monkeypatch, quality={"invalid_rows": 1, "negative_hours_rejected": 0, "wrong_day_rows": 0, "boundary_overlap_rows": 0})
    with pytest.raises(source.GFWV4LiveSourceError, match="quality"):
        source.fetch_normalized_v4_daily_source(token="t", selected_day=date(2026, 8, 21), work_dir=tmp_path / "work", client_factory=lambda token: object())
