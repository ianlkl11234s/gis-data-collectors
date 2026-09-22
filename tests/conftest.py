"""pytest 共用 fixture、secret isolation 與 network guard。"""

import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlsplit

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


_ORIGINAL_HTTP_SEND = requests.sessions.Session.send
_ORIGINAL_SOCKET_CONNECT = socket.socket.connect
_ORIGINAL_SOCKET_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_SOCKET_SENDTO = socket.socket.sendto
_ORIGINAL_PSYCOPG2_CONNECT = psycopg2.connect
_NETWORK_ATTEMPTS: list[str] = []
_ACTIVE_LIVE_SUPABASE_DSN: str | None = None


def _redacted_target(kind: str, target: object = None) -> str:
    if kind == "http":
        try:
            return urlsplit(str(target)).hostname or "<redacted-host>"
        except ValueError:
            return "<redacted-host>"
    if kind == "socket" and isinstance(target, tuple) and target:
        return str(target[0])
    if kind == "postgres":
        return "<redacted-dsn>"
    return "<redacted-target>"


def _blocked_network(kind: str, target: object = None):
    detail = f"{kind}: {_redacted_target(kind, target)}"
    _NETWORK_ATTEMPTS.append(detail)
    raise RuntimeError(f"unmocked external network access blocked in pytest ({detail})")


def _guarded_http_send(_self, prepared, **_kwargs):
    return _blocked_network("http", getattr(prepared, "url", None))


def _guarded_socket_connect(_self, address):
    return _blocked_network("socket", address)


def _guarded_socket_sendto(_self, _data, address):
    return _blocked_network("socket", address)


def _guarded_psycopg2_connect(*args, **kwargs):
    dsn = kwargs.get("dsn") or (args[0] if args else None)
    if _ACTIVE_LIVE_SUPABASE_DSN and dsn == _ACTIVE_LIVE_SUPABASE_DSN:
        return _ORIGINAL_PSYCOPG2_CONNECT(*args, **kwargs)
    return _blocked_network("postgres", dsn)


# Install guards before test module collection. Individual unit tests can still
# monkeypatch these boundaries with deterministic fakes; teardown restores the
# guarded version, not the real transport.
requests.sessions.Session.send = _guarded_http_send
socket.socket.connect = _guarded_socket_connect
socket.socket.connect_ex = _guarded_socket_connect
socket.socket.sendto = _guarded_socket_sendto
psycopg2.connect = _guarded_psycopg2_connect

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
def audit_external_network(request):
    """Permit only an explicitly opted-in, exactly matched test Supabase DSN."""
    global _ACTIVE_LIVE_SUPABASE_DSN

    if _NETWORK_ATTEMPTS:
        attempts = "; ".join(_NETWORK_ATTEMPTS)
        _NETWORK_ATTEMPTS.clear()
        pytest.fail("external network attempted before test setup: " + attempts)

    is_live_supabase = request.node.get_closest_marker("live_supabase") is not None
    opted_in = request.config.getoption("--run-live-supabase")
    test_dsn = os.environ.get("TEST_SUPABASE_DB_URL", "").strip()
    _ACTIVE_LIVE_SUPABASE_DSN = test_dsn if is_live_supabase and opted_in and test_dsn else None

    yield

    attempts = list(_NETWORK_ATTEMPTS)
    _NETWORK_ATTEMPTS.clear()
    _ACTIVE_LIVE_SUPABASE_DSN = None
    expects_block = request.node.get_closest_marker("expects_network_block") is not None
    if expects_block:
        if not attempts:
            pytest.fail("test expected the network guard to block an attempt")
        return
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
