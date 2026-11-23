"""external/tw_live_snapshots_vm：來源請求有界重試（E3）、失敗留標記＋health 新鮮度（E2）。"""
import importlib.util
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
EXT = ROOT / "external"
TZ = timezone(timedelta(hours=8))


def _load(name, path, extra_path=None):
    if extra_path:
        sys.path.insert(0, str(extra_path))
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def vm():
    return _load("collect_tw_snapshots_test", EXT / "tw_live_snapshots_vm/collect_tw_snapshots.py", ROOT / "utils")


@pytest.fixture(scope="module")
def health():
    return _load("health_report_tw_test", EXT / "_shared/health_report.py")


class _R:
    def __init__(self, status=200, body=None):
        self.status_code, self._b = status, body or {}
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def json(self):
        return self._b


class _S:
    def __init__(self, seq):
        self.seq, self.calls = list(seq), 0

    def get(self, url, timeout):
        self.calls += 1
        x = self.seq.pop(0)
        if isinstance(x, Exception):
            raise x
        return x


# ─── E3 ──────────────────────────────────────────────────────────────────────
def test_get_retries_transient_then_succeeds(vm, monkeypatch):
    monkeypatch.setattr(vm.time, "sleep", lambda s: None)
    s = _S([requests.ReadTimeout("t"), _R(503), _R(200, {"ok": 1})])
    assert vm.get_with_retry(s, "u", timeout=5).json() == {"ok": 1}
    assert s.calls == 3


def test_get_gives_up_after_bounded_attempts(vm, monkeypatch):
    monkeypatch.setattr(vm.time, "sleep", lambda s: None)
    s = _S([_R(503)] * 10)
    with pytest.raises(requests.HTTPError):
        vm.get_with_retry(s, "u", timeout=5)
    assert s.calls == vm.RETRY_ATTEMPTS


def test_get_does_not_retry_client_error(vm, monkeypatch):
    monkeypatch.setattr(vm.time, "sleep", lambda s: None)
    s = _S([_R(404)] * 3)
    with pytest.raises(requests.HTTPError):
        vm.get_with_retry(s, "u", timeout=5)
    assert s.calls == 1


# ─── E3 橫向：cdc_public_health_weekly_vm（每週一次的 cron，單次失敗就丟一週）─────────────
def test_cdc_weekly_session_has_bounded_retries():
    import os
    os.environ.setdefault("SUPABASE_DB_URL", "postgresql://test")
    sys.path.insert(0, str(EXT / "vm_common"))
    sys.path.insert(0, str(EXT / "cdc_public_health_weekly_vm"))
    import cdc_public_health_weekly_collect as cdc
    r = cdc.SESSION.get_adapter("https://od.cdc.gov.tw/eic/x.csv").max_retries
    assert 1 <= r.total <= 5 and 503 in r.status_forcelist and r.backoff_factor > 0
