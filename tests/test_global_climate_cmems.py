import subprocess
import sys
from pathlib import Path

import pytest

from collectors.global_climate import cmems
from collectors.global_climate.cmems import CMEMS_DATASETS, OOM_FIRST_PREFIX, CmemsCollector


def test_oom_first_prefix_execs_the_command_unchanged():
    result = subprocess.run(
        OOM_FIRST_PREFIX + ["printf", "%s|", "a b", "c"], capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert result.stdout == "a b|c|"
    assert result.stderr == ""  # no /proc noise on hosts without it


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="oom_score_adj is Linux-only")
def test_oom_first_prefix_marks_child_as_preferred_oom_victim():
    result = subprocess.run(
        OOM_FIRST_PREFIX + ["cat", "/proc/self/oom_score_adj"], capture_output=True, text=True,
    )
    assert result.stdout.strip() == "1000"


def test_subset_runs_copernicusmarine_behind_oom_prefix(tmp_path, monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        (tmp_path / "cmems_sst.nc").write_bytes(b"nc")
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(cmems.subprocess, "run", fake_run)
    collector = object.__new__(CmemsCollector)
    sst = next(ds for ds in CMEMS_DATASETS if ds["id"] == "cmems_sst")

    assert collector._subset(sst, tmp_path) == Path(tmp_path / "cmems_sst.nc")
    cmd = calls[0]
    assert cmd[:len(OOM_FIRST_PREFIX)] == OOM_FIRST_PREFIX
    assert cmd[len(OOM_FIRST_PREFIX):][:2] == ["copernicusmarine", "subset"]


def test_subset_disables_dask_chunking(tmp_path, monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(cmems.subprocess, "run", fake_run)
    collector = object.__new__(CmemsCollector)
    for ds in CMEMS_DATASETS:
        collector._subset(ds, tmp_path)
    for cmd in calls:
        i = cmd.index("--chunk-size-limit")
        assert cmd[i + 1] == "0"
