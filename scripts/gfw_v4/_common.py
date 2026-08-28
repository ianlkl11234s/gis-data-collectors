"""Shared bootstrap helpers for the local-only GFW v4 POC drivers."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def add_repo_to_path() -> None:
    """Make ``python3 scripts/gfw_v4/<driver>.py`` work from any cwd."""
    root = str(REPO_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


def add_output_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="New local output directory; an existing directory is rejected.",
    )


def prepare_output(path: Path) -> Path:
    path = path.expanduser().resolve()
    if path.exists():
        raise FileExistsError(f"output directory already exists: {path}")
    path.mkdir(parents=True)
    return path


def load_env_file(path: Path | None) -> None:
    """Load an explicitly supplied dotenv file without printing its values."""
    if path is None:
        return
    from dotenv import load_dotenv

    if not path.is_file():
        raise FileNotFoundError(f"env file not found: {path}")
    load_dotenv(path)


def require_gfw_token(config: object) -> str:
    token = str(getattr(config, "GFW_ACCESS_TOKEN", "") or os.getenv("GFW_ACCESS_TOKEN", ""))
    if not token:
        raise RuntimeError(
            "GFW_ACCESS_TOKEN is required; provide it through the environment or --env-file"
        )
    return token
