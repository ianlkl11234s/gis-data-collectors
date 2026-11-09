"""VM 獨立腳本的 metadata.collector_status 心跳（DIO-4，2026-11-09）。

ship_ais／waste_positions／cdc 走 VM 獨立寫入路徑，原本沒更新 collector_status，
last_success 停在 2026-06。照 immigration_apis_airport 補 report_heartbeat：
有連線就用同一條、沒有就開短連線；任何錯誤只記 warning，不影響主流程。
"""
import importlib
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
APPS = {
    "ship_ais": ("ship_ais_vm", "ship_ais_collect"),
    "waste_positions": ("waste_positions_vm", "waste_positions_collect"),
    "cdc_public_health_weekly": ("cdc_public_health_weekly_vm", "cdc_public_health_weekly_collect"),
}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args):
        if self.conn.fail:
            raise RuntimeError("boom")
        self.conn.calls.append((sql, args))


class FakeConn:
    def __init__(self, fail=False):
        self.calls, self.commits, self.closed, self.fail = [], 0, False, fail

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


@pytest.fixture(params=sorted(APPS))
def app(request, monkeypatch):
    name = request.param
    folder, module = APPS[name]
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://u:p@localhost/db")  # dummy
    monkeypatch.syspath_prepend(str(ROOT / "external" / "vm_common"))
    monkeypatch.syspath_prepend(str(ROOT / "external" / folder))
    sys.modules.pop(module, None)
    mod = importlib.import_module(module)
    yield name, mod
    sys.modules.pop(module, None)


def test_heartbeat_uses_given_conn(app):
    name, mod = app
    assert mod.COLLECTOR_NAME == name
    conn = FakeConn()
    mod.report_heartbeat(True, 12, None, conn=conn)
    assert conn.calls == [("SELECT report_collector_heartbeat(%s, %s, %s, %s)", (name, True, 12, None))]
    assert conn.commits == 1 and not conn.closed  # 呼叫端的連線不由心跳關閉


def test_heartbeat_opens_and_closes_own_conn(app, monkeypatch):
    name, mod = app
    conn = FakeConn()
    monkeypatch.setattr(mod.psycopg2, "connect", lambda *a, **k: conn)
    mod.report_heartbeat(False, 0, "x" * 900)
    (_, args), = conn.calls
    assert args[:3] == (name, False, 0) and len(args[3]) == 500
    assert conn.closed


def test_heartbeat_errors_are_swallowed(app, monkeypatch):
    _, mod = app
    mod.report_heartbeat(True, 1, conn=FakeConn(fail=True))

    def refuse(*a, **k):
        raise OSError("db down")
    monkeypatch.setattr(mod.psycopg2, "connect", refuse)
    mod.report_heartbeat(False, 0, "e")  # 不拋例外
