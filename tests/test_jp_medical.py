from __future__ import annotations

import subprocess
from pathlib import Path

import config
import pytest

from collectors.jp_medical import JapanMedicalNaviiCollector


def test_disabled_defaults_are_safe():
    assert config.JP_MEDICAL_NAVII_ENABLED is False
    assert config.JP_MEDICAL_IDWR_ENABLED is False
    assert config.JP_MEDICAL_REPORTS_ENABLED is False
    assert config.ANALYTICS_ROOT == ""
    assert config.PIPELINE_PYTHON == ""


def _configured_navii(monkeypatch, tmp_path):
    root = tmp_path / "analytics"
    source = root / "pipelines/world/jp_medical_navii/jp_medical_navii.py"
    frontend = root / "pipelines/world/jp_medical_frontend/build.py"
    source.parent.mkdir(parents=True)
    frontend.parent.mkdir(parents=True)
    source.write_text("", encoding="utf-8")
    frontend.write_text("", encoding="utf-8")
    interpreter = tmp_path / "python"
    interpreter.write_text("", encoding="utf-8")
    interpreter.chmod(0o700)
    monkeypatch.setattr(config, "ANALYTICS_ROOT", str(root))
    monkeypatch.setattr(config, "PIPELINE_PYTHON", str(interpreter))
    return source, frontend, interpreter


def test_failed_pipeline_is_a_collector_error(monkeypatch, tmp_path):
    _configured_navii(monkeypatch, tmp_path)
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a[0], 9, "", "bad source"))
    with pytest.raises(RuntimeError, match="pipeline exited 9"):
        JapanMedicalNaviiCollector().collect()


def test_frontend_build_is_opt_in_and_runs_only_after_source_success(monkeypatch, tmp_path):
    source, frontend, interpreter = _configured_navii(monkeypatch, tmp_path)
    calls = []
    monkeypatch.setenv("JP_MEDICAL_BUILD_FRONTEND", "true")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 0, "", ""),
    )

    result = JapanMedicalNaviiCollector().collect()

    assert calls == [[str(interpreter), str(source), "run"], [str(interpreter), str(frontend)]]
    assert result["frontend_bundle_built"] is True


def test_failed_source_never_starts_frontend_build(monkeypatch, tmp_path):
    _, frontend, _ = _configured_navii(monkeypatch, tmp_path)
    calls = []
    monkeypatch.setenv("JP_MEDICAL_BUILD_FRONTEND", "1")
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 3, "", "bad source"),
    )

    with pytest.raises(RuntimeError, match="pipeline exited 3"):
        JapanMedicalNaviiCollector().collect()
    assert all(command[1] != str(frontend) for command in calls)


def test_failed_frontend_build_is_not_reported_as_ready(monkeypatch, tmp_path):
    _, frontend, _ = _configured_navii(monkeypatch, tmp_path)
    calls = []
    monkeypatch.setenv("JP_MEDICAL_BUILD_FRONTEND", "on")

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 7 if command[1] == str(frontend) else 0, "", "bad bundle")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="frontend build exited 7"):
        JapanMedicalNaviiCollector().collect()
    assert calls[-1][1] == str(frontend)
