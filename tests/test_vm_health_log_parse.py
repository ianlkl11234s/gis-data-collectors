"""VM health_report 的 log 解析（2026-11-12 誤報修正）。

樣本行照各收集器原始碼的 log 呼叫組出來；先斷言原始碼裡真的有這些字樣，
收集器改了 log 文字這裡會紅，提醒同步改 health_report 的 pattern。
"""
import gzip
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXT = ROOT / "external"
TZ = timezone(timedelta(hours=8))
NOW = datetime(2026, 11, 12, 6, 59, 59, tzinfo=TZ)  # 07:00 cron 跑時，07:00 那輪多半還沒寫完


@pytest.fixture(scope="module")
def health():
    spec = importlib.util.spec_from_file_location("health_report_parse_test", EXT / "_shared/health_report.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _target(health, name):
    return next(t for t in health.DEFAULT_TARGETS if t["name"] == name)


def _src(rel):
    return (EXT / rel).read_text(encoding="utf-8")


def _line(ts, level, msg):
    return f"{ts:%Y-%m-%d %H:%M:%S} [{level}] {msg}\n"


def _write_rotated(log_path: Path, lines: list[tuple[datetime, str]], gz=False):
    """午夜輪替：前一天的行進 .1（或 .1.gz），當天的留在 collect.log。"""
    today = NOW.date()
    old = "".join(text for ts, text in lines if ts.date() < today)
    new = "".join(text for ts, text in lines if ts.date() >= today)
    log_path.write_text(new, encoding="utf-8")
    if gz:
        with gzip.open(f"{log_path}.1.gz", "wt", encoding="utf-8") as f:
            f.write(old)
    else:
        Path(f"{log_path}.1").write_text(old, encoding="utf-8")


def _analyze(health, target, log_path):
    return health.analyze_log(str(log_path), target["success_pattern"], target.get("run_pattern"), now=NOW)


def test_ship_ais_counts_full_day_across_rotation(health, tmp_path):
    src = _src("ship_ais_vm/ship_ais_collect.py")
    assert "log.info(f\"抓到 {result['ship_count']} 艘船\")" in src
    assert 'log.info(f"Supabase 寫入: history={h}, current={c}")' in src
    assert 'log.error(f"FAILED: {exc}", exc_info=True)' in src

    lines = []
    for i in range(1, 160):  # 每 10 分鐘一輪，往前推超過 24h
        ts = NOW - timedelta(seconds=59) - timedelta(minutes=10 * (i - 1))
        if i == 3:  # 一輪抓取失敗
            lines.append((ts, _line(ts, "ERROR", "FAILED: HTTPSConnectionPool timeout")))
            lines.append((ts, "Traceback (most recent call last):\n"))
            continue
        lines.append((ts, _line(ts, "INFO", "抓到 1234 艘船")))
        lines.append((ts, _line(ts, "INFO", "snapshot → /var/lib/ship-ais/data/ship_ais/x.json")))
        lines.append((ts, _line(ts + timedelta(seconds=5), "INFO", "Supabase 寫入: history=1200, current=1234")))
    log = tmp_path / "collect.log"
    _write_rotated(log, sorted(lines))

    r = _analyze(health, _target(health, "ship_ais"), log)
    assert r["runs_24h"] == 144
    assert r["success_24h"] == 143
    assert r["last_count"] == 1200
    assert r["earliest_log_at"].startswith("2026-11-11T07:09")


def test_waste_positions_one_run_per_cycle_and_send_wording(health, tmp_path):
    src = _src("waste_positions_vm/waste_positions_collect.py")
    assert 'log.info(f"{city_name}: {len(records)} 筆 (出勤 {collecting} / 待命 {parked})")' in src
    assert 'log.info(f"snapshot → {snap_path}")' in src
    assert 'log.info(f"Supabase 送出: {n} 筆（重複點由 ON CONFLICT DO NOTHING 略過）")' in src
    assert 'log.info(f"quiet_hours ({QUIET_HOURS}) → skip")' in src

    lines = []
    ts = NOW - timedelta(hours=24) + timedelta(minutes=1)
    while ts <= NOW:
        if 1 <= ts.hour < 6:  # QUIET_HOURS 預設 01-06
            lines.append((ts, _line(ts, "INFO", "quiet_hours (01-06) → skip")))
        else:
            for city in ("高雄市", "新北市", "臺南市"):
                lines.append((ts, _line(ts, "INFO", f"{city}: 120 筆 (出勤 80 / 待命 40)")))
            lines.append((ts, _line(ts, "INFO", "snapshot → /var/lib/waste-positions/data/x.json")))
            lines.append((ts, _line(ts, "INFO", "Supabase 送出: 360 筆（重複點由 ON CONFLICT DO NOTHING 略過）")))
        ts += timedelta(minutes=2)
    log = tmp_path / "collect.log"
    _write_rotated(log, lines, gz=True)

    r = _analyze(health, _target(health, "waste_positions"), log)
    assert r["runs_24h"] == 19 * 30  # 一天 19 個非靜默小時 × 每小時 30 輪
    assert r["success_24h"] == r["runs_24h"]
    assert r["last_count"] == 360


def test_isohe_fetched_and_readings_sent(health, tmp_path):
    src = _src("isohe_port_marine_vm/isohe_port_marine_collect.py")
    assert '"Fetched stations=%s readings=%s snapshot=%s"' in src
    assert '"Supabase 寫入: stations=%s readings_sent=%s/%s"' in src
    assert '"Current batch was already written by buffer flush"' in src

    lines = []
    ts = NOW.replace(hour=6, minute=58, second=3)
    for i in range(150):
        lines.append((ts, _line(ts, "INFO", "Fetched stations=21 readings=84 snapshot=/var/lib/x.json")))
        msg = ("Current batch was already written by buffer flush" if i == 0
               else "Supabase 寫入: stations=21 readings_sent=84/84")
        lines.append((ts, _line(ts + timedelta(seconds=4), "INFO", msg)))
        ts -= timedelta(minutes=10)
    log = tmp_path / "collect.log"
    _write_rotated(log, sorted(lines))

    r = _analyze(health, _target(health, "isohe_port_marine"), log)
    assert r["runs_24h"] == 144
    assert r["success_24h"] == 144
    assert r["last_count"] is None  # 最後一輪是 buffer 補寫，沒有站數


def test_without_rotated_file_reports_late_earliest(health, tmp_path):
    log = tmp_path / "collect.log"
    ts = NOW.replace(hour=0, minute=0, second=1)
    log.write_text(_line(ts, "INFO", "抓到 1 艘船"), encoding="utf-8")
    r = _analyze(health, _target(health, "ship_ais"), log)
    assert r["runs_24h"] == 1
    assert r["earliest_log_at"].startswith("2026-11-12T00:00")


def test_custom_target_without_run_pattern_uses_legacy_marker(health, tmp_path):
    log = tmp_path / "collect.log"
    log.write_text(_line(NOW, "INFO", "開始抓取") + _line(NOW, "INFO", "Supabase 寫入: 5 筆"), encoding="utf-8")
    r = health.analyze_log(str(log), r"Supabase 寫入: (\d+) 筆", now=NOW)
    assert (r["runs_24h"], r["success_24h"], r["last_count"]) == (1, 1, 5)
