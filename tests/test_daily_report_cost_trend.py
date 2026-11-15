"""日報「成本趨勢」：7／30 天前依時間戳取點、一次性跳升不連紅一個月（2026-11-16）。"""
from datetime import datetime, timedelta, timezone

from tasks.daily_report import _cost_trend_text

GB = 1024 ** 3
T0 = datetime(2026, 10, 12, tzinfo=timezone.utc)


def _points(sizes_gb):
    return [{"Timestamp": T0 + timedelta(days=i), "Maximum": gb * GB} for i, gb in enumerate(sizes_gb)]


def test_week_compares_point_seven_days_back():
    sizes = [100.0] * 34 + [107.0]   # 最新點比 7 天前 +7%
    text = _cost_trend_text(_points(sizes))
    assert "含舊版本" in text
    assert "週增 +7.0%" in text


def test_week_with_missing_days_uses_timestamp_not_index():
    pts = _points([100.0] * 35)
    del pts[-3:-1]                       # 缺兩天：第 -7 點已是 9 天前
    pts[-1]["Maximum"] = 110 * GB
    assert "週增 +10.0%" in _cost_trend_text(pts)


def test_one_off_jump_downgraded_to_yellow():
    sizes = [100.0] * 20 + [210.0] * 15   # 單日 +110 GB 的一次性歸檔
    text = _cost_trend_text(_points(sizes))
    assert "🔴" not in text and "🟡" in text
    assert "單日 +110.00 GB" in text and "一次性" in text


def test_sustained_growth_stays_red():
    sizes = [100.0 * (1.01 ** i) for i in range(35)]   # 每天 +1%，30 天約 +35%
    text = _cost_trend_text(_points(sizes))
    assert "🔴" in text and "一次性" not in text


def test_flat_is_green_and_short_history_reported():
    assert "🟢" in _cost_trend_text(_points([100.0] * 35))
    assert "資料 < 7 天" in _cost_trend_text(_points([100.0] * 5))
    assert "無資料" in _cost_trend_text([])
