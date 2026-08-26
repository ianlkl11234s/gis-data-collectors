"""pytest 共用 fixture 與 path 設定"""

import sys
from pathlib import Path

import pytest

# 讓 tests/ 內的 import 能找到專案根目錄的模組
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def fake_gfw_pmtiles(monkeypatch):
    """Replace only the external PMTiles build boundary in unit tests.

    Production still resolves and executes the real Tippecanoe and pmtiles
    binaries in ``scripts.gfw_hourly_browser_assets._pmtiles``.  Tests that
    assert our own asset contract instead use this deterministic stand-in so
    GitHub runners need not install the platform-specific tooling.
    """
    from scripts import gfw_hourly_browser_assets

    calls = []

    def build(*, named_inputs, output, minimum_zoom, maximum_zoom, runner):
        assert all(source.is_file() for _, source in named_inputs)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"gfw-test-pmtiles-v1\n")
        calls.append({
            "layers": tuple(layer for layer, _ in named_inputs),
            "output": output,
            "minimum_zoom": minimum_zoom,
            "maximum_zoom": maximum_zoom,
        })

    monkeypatch.setattr(gfw_hourly_browser_assets, "_pmtiles", build)
    return calls
