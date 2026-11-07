"""utils.isolated_task：子行程結果回傳、例外、逾時強殺、oom_score_adj。"""
import time

import pytest

from utils.isolated_task import IsolatedTaskError, run_isolated, set_oom_score_adj

T = "tests.isolated_task_targets"


@pytest.fixture(autouse=True)
def _oom_path(tmp_path, monkeypatch):
    path = tmp_path / "oom_score_adj"
    path.write_text("0")
    monkeypatch.setenv("ISOLATED_TASK_OOM_ADJ_PATH", str(path))
    return path


def test_returns_json_roundtripped_result():
    out = run_isolated("t_ok", f"{T}:ok", kwargs={"day": "2026-11-06"}, timeout=60)
    assert out["day"] == "2026-11-06"
    assert out["when"] == "2026-11-06"  # date 經 default=str
    import os
    assert out["pid"] != os.getpid()


def test_child_sets_oom_score_adj_before_running(_oom_path):
    assert run_isolated("t_oom", f"{T}:read_oom_adj", timeout=60) == "1000"


def test_child_exception_is_raised_and_redacted():
    with pytest.raises(IsolatedTaskError) as ei:
        run_isolated("t_boom", f"{T}:boom", timeout=60)
    msg = str(ei.value)
    assert "ValueError" in msg and "bad thing" in msg
    assert "hunter2" not in msg


def test_nonzero_exit_without_result_is_reported():
    with pytest.raises(IsolatedTaskError, match="結束碼 7"):
        run_isolated("t_exit", f"{T}:hard_exit", timeout=60)


def test_timeout_kills_child():
    start = time.monotonic()
    with pytest.raises(IsolatedTaskError, match="逾時"):
        run_isolated("t_slow", f"{T}:sleepy", kwargs={"seconds": 30}, timeout=2)
    assert time.monotonic() - start < 20


def test_bad_target_reports_error():
    with pytest.raises(IsolatedTaskError, match="ModuleNotFoundError|AttributeError"):
        run_isolated("t_bad", f"{T}:nope", timeout=60)


def test_set_oom_score_adj_failure_is_quiet(tmp_path):
    assert set_oom_score_adj(1000, path=str(tmp_path / "missing" / "x")) is False
