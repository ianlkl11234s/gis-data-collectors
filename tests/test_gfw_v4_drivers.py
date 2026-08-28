"""Static safety and syntax checks for the local-only v4 driver wrappers."""

from __future__ import annotations

import ast
from pathlib import Path


DRIVER_ROOT = Path(__file__).parents[1] / "scripts" / "gfw_v4"
DRIVERS = tuple(sorted(DRIVER_ROOT.glob("*.py")))


def test_v4_drivers_parse_and_have_main() -> None:
    assert {path.name for path in DRIVERS} == {
        "_common.py",
        "build_fishing.py",
        "build_grid.py",
        "build_tracks.py",
        "finalize_shadow.py",
        "probe_fishing_latest.py",
    }
    for path in DRIVERS:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        assert any(isinstance(node, ast.FunctionDef) and node.name == "main" for node in tree.body) or path.name == "_common.py"


def test_v4_drivers_do_not_embed_machine_paths_or_credentials() -> None:
    forbidden = ("/Users/", "/private/tmp/", "Bearer ey", "sk-")
    for path in DRIVERS:
        source = path.read_text(encoding="utf-8")
        assert not any(marker in source for marker in forbidden), path
