"""pytest 共用 fixture、secret isolation 與 network guard。"""

import os
import socket
import sys
from pathlib import Path

import psycopg2
import pytest
import requests
import dotenv
from dotenv import main as dotenv_main


def _ignore_dotenv(*_args, **_kwargs) -> bool:
    """Tests must never inherit developer or production ``.env`` files."""
    return False


# conftest is imported before test modules are collected. Patch dotenv here so
# application imports (and later importlib.reload(config)) cannot read a real
# checkout-level .env file.
dotenv.load_dotenv = _ignore_dotenv
dotenv_main.load_dotenv = _ignore_dotenv


_APPLICATION_CREDENTIAL_ENV = (
    "AISSTREAM_API_KEY",
    "API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "CAMS_API_KEY",
    "CLOUDFLARE_RADAR_API_TOKEN",
    "COPERNICUSMARINE_SERVICE_PASSWORD",
    "CWA_API_KEY",
    "FLIGHT_OPENSKY_CLIENT_SECRET",
    "FLIGHT_OPENSKY_PASSWORD",
    "GEMINI_API_KEY",
    "GFW_ACCESS_TOKEN",
    "HF_TOKEN",
    "IOW_CLIENT_SECRET",
    "LAUNCH_API_TOKEN",
    "LINE_TOKEN",
    "MOENV_API_KEY",
    "OPENROUTER_API_KEY",
    "OSRM_TOKEN",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "RIPE_ATLAS_API_KEY",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "SLACK_WEBHOOK",
    "SPACETRACK_PASSWORD",
    "SUPABASE_DB_URL",
    "TDX_APP_KEY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "WEBHOOK_URL",
    "YOUTUBE_API_KEY",
)
for _name in _APPLICATION_CREDENTIAL_ENV:
    os.environ.pop(_name, None)

# 讓 tests/ 內的 import 能找到專案根目錄的模組
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def pytest_addoption(parser):
    parser.addoption(
        "--run-live-supabase",
        action="store_true",
        default=False,
        help="run tests marked live_supabase using TEST_SUPABASE_DB_URL",
    )


@pytest.fixture(autouse=True)
def deny_unmocked_external_network(monkeypatch, request):
    """Fail every ordinary test that reaches an unmocked network boundary."""
    if request.node.get_closest_marker("live_supabase"):
        yield
        return

    attempts: list[str] = []

    def blocked(kind: str, target: object = None):
        detail = f"{kind}: {target!r}"
        attempts.append(detail)
        raise RuntimeError(f"unmocked external network access blocked in pytest ({detail})")

    monkeypatch.setattr(
        requests.sessions.Session,
        "send",
        lambda _self, prepared, **_kwargs: blocked("http", getattr(prepared, "url", None)),
    )
    monkeypatch.setattr(
        socket.socket,
        "connect",
        lambda _self, address: blocked("socket", address),
    )
    monkeypatch.setattr(
        psycopg2,
        "connect",
        lambda *args, **kwargs: blocked("postgres", kwargs.get("dsn") or (args[0] if args else None)),
    )

    yield

    # Notification code intentionally catches transport exceptions. Retain a
    # teardown assertion so swallowed attempts still fail the originating test.
    if attempts:
        pytest.fail("unexpected external network attempts: " + "; ".join(attempts))


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
