"""external/moa_opendata_vm：VM runner 的到期判斷、lock、state 與 import closure 守門。"""
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VM_DIR = ROOT / "external" / "moa_opendata_vm"
sys.path.insert(0, str(VM_DIR))

import moa_opendata_run as runner  # noqa: E402

NOW = datetime(2026, 10, 24, 0, 53, tzinfo=runner.TAIPEI_TZ)


def test_is_due_without_or_with_broken_state():
    assert runner.is_due(None, 1440, NOW)
    assert runner.is_due("not-a-date", 1440, NOW)


def test_daily_due_tolerates_cron_drift_but_skips_same_day():
    yesterday_late = (NOW - timedelta(hours=23, minutes=55)).isoformat()
    assert runner.is_due(yesterday_late, 1440, NOW)
    earlier_today = (NOW - timedelta(hours=6)).isoformat()
    assert not runner.is_due(earlier_today, 1440, NOW)


def test_long_interval_waits_for_most_of_interval():
    twenty_days = (NOW - timedelta(days=20)).isoformat()
    assert not runner.is_due(twenty_days, 43200, NOW)
    twenty_eight_days = (NOW - timedelta(days=28)).isoformat()
    assert runner.is_due(twenty_eight_days, 43200, NOW)


def test_plan_named_collectors_ignore_due_state():
    state = {name: NOW.isoformat() for name in runner.COLLECTORS}
    assert runner.plan(["food_prices"], False, NOW, state) == ["food_prices"]
    assert runner.plan([], False, NOW, state) == []


def test_plan_all_due_uses_config_intervals(monkeypatch):
    monkeypatch.setattr(runner, "interval_of", lambda name: 1440 if name in ("food_prices", "animal_adoption") else 43200)
    state = {name: (NOW - timedelta(days=2)).isoformat() for name in runner.COLLECTORS}
    state["food_prices"] = (NOW - timedelta(hours=1)).isoformat()
    assert runner.plan([], True, NOW, state) == ["animal_adoption"]


def test_run_collectors_records_only_successes_and_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    state_path = tmp_path / "state" / "last_success.json"
    calls = []

    def fake_runner(args):
        calls.append(args)
        return runner.EXIT_FAILED if args[-1] == "animal_adoption" else runner.EXIT_OK

    rc = runner.run_collectors(["animal_adoption", "food_prices"], state_path, runner=fake_runner)
    assert rc == runner.EXIT_FAILED
    assert calls == [["--_child-run", "animal_adoption"], ["--_child-run", "food_prices"]]
    state = json.loads(state_path.read_text())
    assert set(state) == {"food_prices"}


def test_run_collectors_flushes_buffer_first_when_present(tmp_path, monkeypatch):
    (tmp_path / "data" / "buffer").mkdir(parents=True)
    (tmp_path / "data" / "buffer" / "x.json").write_text("{}")
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    calls = []
    rc = runner.run_collectors(["food_prices"], tmp_path / "state.json",
                               runner=lambda args: calls.append(args) or runner.EXIT_OK)
    assert rc == runner.EXIT_OK
    assert calls[0] == ["--_child-flush-buffer"]


def test_app_lock_refuses_second_holder(tmp_path):
    lock = tmp_path / "run.lock"
    with runner.app_lock(lock) as first:
        assert first
        with runner.app_lock(lock) as second:
            assert not second
    with runner.app_lock(lock) as again:
        assert again


def test_state_dir_sits_outside_data_dir(monkeypatch):
    monkeypatch.delenv("MOA_STATE_DIR", raising=False)
    monkeypatch.setenv("DATA_DIR", "/var/lib/moa-opendata/data")
    assert runner.state_dir() == Path("/var/lib/moa-opendata/state")


def test_registry_names_match_collector_classes():
    out = _run_in_subprocess(
        "import moa_opendata_run as r; r.bootstrap_import_path();"
        "print(json.dumps({n: r.load_collector_class(n).name for n in r.COLLECTORS}))"
    )
    names = json.loads(out.strip().splitlines()[-1])
    assert names == {n: n for n in runner.COLLECTORS}


def test_import_closure_is_light_and_fully_packaged():
    """bootstrap 後的 import closure 不得拉進重量級套件，且每個 repo 模組都在 deploy.sh 清單內。"""
    out = _run_in_subprocess(
        "import moa_opendata_run as r; r.bootstrap_import_path();"
        "[r.load_collector_class(n) for n in r.COLLECTORS];"
        "import tasks.archive, storage.supabase_writer;"
        "root = r.APP_DIR.resolve();"
        "files = sorted(str(Path(m.__file__).resolve().relative_to(root)) for m in list(sys.modules.values())"
        "  if getattr(m, '__file__', None) and Path(m.__file__).resolve().is_relative_to(root));"
        "heavy = [m for m in ('numpy', 'yaml', 'shapely', 'xarray', 'schedule', 'flask', 'scripts') if m in sys.modules];"
        "print(json.dumps({'files': files, 'heavy': heavy}))"
    )
    data = json.loads(out.strip().splitlines()[-1])
    assert data["heavy"] == []
    deploy = (VM_DIR / "deploy.sh").read_text()
    listed = set(re.search(r"REPO_FILES=\(\n(.*?)\n\)", deploy, re.S).group(1).split())
    loaded = {f for f in data["files"] if not f.startswith("external/")}
    assert "collectors/__init__.py" not in loaded and "tasks/__init__.py" not in loaded
    assert loaded <= listed, sorted(loaded - listed)


def test_cron_block_is_marked_and_avoids_used_minutes():
    cron = (VM_DIR / "setup_cron.sh").read_text()
    assert "# ─── moa_opendata cron BEGIN ───" in cron and "# ─── moa_opendata cron END ───" in cron
    minutes = [int(line.split()[0]) for line in cron.splitlines() if re.match(r"^\d+ \d+ \* \* \* ", line)]
    assert minutes == [53, 21]
    for minute in minutes:  # waste */2 (偶數)、isohe x8、immigration :07
        assert minute % 2 == 1 and minute % 10 != 8 and minute != 7


def test_setup_vm_never_overwrites_env():
    setup = (VM_DIR / "setup_vm.sh").read_text()
    assert 'if [ ! -f "$APP_DIR/.env" ]' in setup


def _run_in_subprocess(code: str) -> str:
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("SUPABASE", "S3_", "AWS_", "TELEGRAM"))}
    env["DATA_DIR"] = str(Path(os.environ.get("TMPDIR", "/tmp")) / "moa_vm_test_data")
    env["SUPABASE_ENABLED"] = "false"
    # config.py 的 load_dotenv() 會往上找到主 checkout 的 .env；子行程一律停用。
    proc = subprocess.run(
        [sys.executable, "-c", "import json, sys; from pathlib import Path;"
         "import dotenv; dotenv.load_dotenv = lambda *a, **k: False;"
         f"sys.path.insert(0, {str(VM_DIR)!r}); {code}"],
        cwd=str(VM_DIR), env=env, capture_output=True, text=True, timeout=120,
    )
    if proc.returncode != 0:
        pytest.fail(proc.stderr[-2000:])
    return proc.stdout


def test_child_run_fails_when_supabase_write_missing_or_failed(monkeypatch):
    """review F243：writer 缺失或寫入失敗時 child_run 要回失敗碼。"""
    from collectors.base import BaseCollector

    class Fake(BaseCollector):
        name = "fake_moa"
        interval_minutes = 60

        def should_persist_local(self):
            return False

        def collect(self):
            return {"data": [{"a": 1}]}

    monkeypatch.setattr(runner, "load_collector_class", lambda n: Fake)
    monkeypatch.setattr("collectors.base.notify_error", lambda *a, **k: None)
    monkeypatch.setattr("collectors.base.notify_success", lambda *a, **k: None)

    class Writer:
        def __init__(self, ok):
            self.ok = ok

        def write(self, *a):
            return self.ok

        def _report_heartbeat(self, *a):
            pass

    for writer, expected in ((None, runner.EXIT_FAILED), (Writer(False), runner.EXIT_FAILED),
                             (Writer(True), runner.EXIT_OK)):
        monkeypatch.setattr("collectors.base.get_supabase_writer", lambda w=writer: w)
        assert runner.child_run("x") == expected
