"""HiCloud ISOHE runner: same marine write contract, timeouts, run cap, replay safety."""
import json
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "external" / "vm_common"))
sys.path.insert(0, str(ROOT / "external" / "isohe_port_marine_vm"))
import isohe_port_marine_collect as vm  # noqa: E402
import marine_write as vm_marine_write  # noqa: E402
import vm_buffer  # noqa: E402

TZ = timezone(timedelta(hours=8))


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def fetchall(self):
        return self.conn.latest


class FakeConn:
    def __init__(self, latest=()):
        self.latest = list(latest)
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.executed.append(("COMMIT", None))
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def calls(monkeypatch):
    recorded = []

    def fake_execute_values(cur, sql, values, **kwargs):
        cur.conn.executed.append((sql, values))
        recorded.append((sql, values))

    monkeypatch.setattr(vm, "execute_values", fake_execute_values)
    return recorded


def _station(**kw):
    row = {"station_uid": "isohe:tp:wave", "source_network": "isohe", "source_station_id": "TP:wave",
           "origin_org": "ISOHE/port", "distribution_org": "ISOHE/port", "station_type": "port_sensor",
           "name_zh": "TP:wave", "aliases": [], "longitude": 121.4, "latitude": 25.1,
           "observed_elements": ["wave_height"], "source_status": None,
           "source_url": "https://isohe.ihmt.gov.tw/opendata/", "license": None,
           "provenance": {"port": "TP", "kind": "wave"},
           "first_seen_at": "2026-10-12T16:00:00+08:00", "last_seen_at": "2026-10-12T16:00:00+08:00"}
    row.update(kw)
    return row


def _reading(observed_at, value=1.0, collected_at="2026-10-12T16:00:00+08:00"):
    return {"station_uid": "isohe:tp:wave", "source_network": "isohe", "source_station_id": "TP:wave",
            "observed_at": observed_at, "metric_code": "wave_height", "depth_key": "surface",
            "value_raw": str(value), "value_numeric": value, "unit_source": "m", "unit_canonical": "m",
            "vertical_datum": None, "is_missing": False, "is_valid": True, "missing_reason": None,
            "source_status": None, "quality_flags": {"missing": False, "valid": True},
            "payload_sha256": "b" * 64, "collected_at": collected_at, "longitude": 121.4, "latitude": 25.1}


def test_vm_uses_shared_contract_module():
    assert vm.marine_write is vm_marine_write
    assert (ROOT / "storage/marine_write.py").read_bytes() == \
        (ROOT / "external/isohe_port_marine_vm/marine_write.py").read_bytes()


def test_vm_write_sets_timeouts_per_txn_and_filters_old_rows(tmp_path, calls):
    latest = [("isohe:tp:wave", "wave_height", "surface", datetime(2026, 10, 12, 16, 0, tzinfo=TZ))]
    conn = FakeConn(latest)
    rows = [_reading(f"2026-10-{d}T{h:02d}:00:00+08:00") for d in (11, 12) for h in range(0, 17)]
    sent = vm.write(conn, [_station()], rows, tmp_path / "state.json")

    assert sent == 4  # 13:00..16:00 on 10-12
    assert conn.commits == 2  # stations txn + readings txn
    stmts = [s for s, _ in conn.executed]
    starts = [0, stmts.index("COMMIT") + 1]
    for i in starts:
        assert stmts[i] == "SELECT set_config('statement_timeout', '120000', true)"
        assert stmts[i + 1] == "SELECT set_config('lock_timeout', '10000', true)"
    reading_sql = next(sql for sql, _ in calls if "marine_observation_readings" in sql)
    assert "IS DISTINCT FROM" in reading_sql and "t.collected_at <= EXCLUDED.collected_at" in reading_sql


def test_vm_station_state_file_skips_unchanged_roster(tmp_path, calls):
    state = tmp_path / "state" / "station_roster.json"
    vm.write(FakeConn(), [_station()], [], state)
    assert json.loads(state.read_text())["hash"]
    vm.write(FakeConn(), [_station(last_seen_at="2026-10-12T16:10:00+08:00")], [], state)
    assert sum("marine_observation_stations" in sql for sql, _ in calls) == 1
    vm.write(FakeConn(), [_station(name_zh="改名")], [], state)
    assert sum("marine_observation_stations" in sql for sql, _ in calls) == 2


def test_buffer_replay_of_stale_batch_cannot_overwrite_newer_values(tmp_path, calls):
    """Replay goes through the same guarded SQL; older-than-window rows are not even sent."""
    buffer_dir = tmp_path / "buffer"
    stale = {"stations": [_station()],
             "readings": [_reading("2026-10-12T09:00:00+08:00", value=9.9, collected_at="2026-10-12T09:05:00+08:00"),
                          _reading("2026-10-12T15:00:00+08:00", value=9.9, collected_at="2026-10-12T15:05:00+08:00")]}
    vm_buffer.save_batch(buffer_dir, "isohe_port_marine", stale)
    latest = [("isohe:tp:wave", "wave_height", "surface", datetime(2026, 10, 12, 16, 0, tzinfo=TZ))]
    conn = FakeConn(latest)
    flushed, _ = vm_buffer.flush_pending(conn, buffer_dir, lambda c, p: vm._flush_write(c, p, tmp_path / "s.json"))
    assert flushed == 1
    sql, values = next((s, v) for s, v in calls if "marine_observation_readings" in s)
    assert [v[3] for v in values] == ["2026-10-12T15:00:00+08:00"]
    assert "WHERE t.collected_at <= EXCLUDED.collected_at" in sql


def test_run_lock_refuses_second_holder(tmp_path):
    first = vm.acquire_run_lock(tmp_path / "collect.lock")
    assert first is not None
    try:
        assert vm.acquire_run_lock(tmp_path / "collect.lock") is None
    finally:
        first.close()
    again = vm.acquire_run_lock(tmp_path / "collect.lock")
    assert again is not None
    again.close()


def test_deadline_uses_default_sigalrm_disposition(monkeypatch):
    seen = {}
    monkeypatch.setattr(vm.signal, "signal", lambda sig, handler: seen.setdefault("handler", (sig, handler)))
    monkeypatch.setattr(vm.signal, "alarm", lambda seconds: seen.setdefault("alarm", seconds))
    vm.arm_deadline(480)
    assert seen == {"handler": (signal.SIGALRM, signal.SIG_DFL), "alarm": 480}
    assert vm.RUN_DEADLINE_SEC == 480


def test_cron_offsets_avoid_db_peaks_and_cwa_minutes():
    cron = (ROOT / "external/isohe_port_marine_vm/setup_cron.sh").read_text()
    line = next(l for l in cron.splitlines() if "isohe_port_marine_collect.py" in l)
    assert line.startswith("8-59/10 ")
    minutes = set(range(8, 60, 10))
    assert not minutes & {14, 15, 40, 7, 22, 37, 52}
    assert "timeout -k 30 540" in line
