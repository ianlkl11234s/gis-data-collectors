import subprocess

from collectors.global_climate import cmems
from collectors.global_climate.cmems import CMEMS_DATASETS, CmemsCollector


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
