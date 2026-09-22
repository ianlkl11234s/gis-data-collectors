"""Regression coverage for the pytest safety boundary."""

import importlib
import os
import socket

import psycopg2
import pytest
import dotenv


def test_config_reload_does_not_read_checkout_dotenv(monkeypatch, tmp_path):
    import config

    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "OPENROUTER_API_KEY=must-not-enter-tests\n"
        "TELEGRAM_BOT_TOKEN=must-not-enter-tests\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    assert dotenv.load_dotenv(dotenv_path=dotenv_path) is False
    reloaded = importlib.reload(config)

    assert reloaded.OPENROUTER_API_KEY == ""
    assert reloaded.TELEGRAM_BOT_TOKEN is None


def test_application_credentials_are_scrubbed_before_collection():
    assert "OPENROUTER_API_KEY" not in os.environ
    assert "SUPABASE_DB_URL" not in os.environ
    assert "TELEGRAM_BOT_TOKEN" not in os.environ
    assert "TELEGRAM_CHAT_ID" not in os.environ


@pytest.mark.expects_network_block
def test_swallowed_telegram_attempt_is_blocked_and_redacted(monkeypatch, capsys):
    import config
    from utils.notify import send_telegram

    sentinel = "telegram-secret-must-not-appear"
    monkeypatch.setattr(config, "TELEGRAM_BOT_TOKEN", sentinel)
    monkeypatch.setattr(config, "TELEGRAM_CHAT_ID", "test-chat")

    assert send_telegram("test only") is False
    assert sentinel not in capsys.readouterr().out


@pytest.mark.expects_network_block
def test_socket_connect_ex_is_blocked():
    sock = socket.socket()
    try:
        with pytest.raises(RuntimeError, match="socket: example.invalid"):
            sock.connect_ex(("example.invalid", 443))
    finally:
        sock.close()


@pytest.mark.expects_network_block
def test_production_style_dsn_is_blocked_and_redacted():
    sentinel = "postgresql://user:secret@example.invalid/db"
    with pytest.raises(RuntimeError) as exc_info:
        psycopg2.connect(sentinel)
    assert sentinel not in str(exc_info.value)
    assert "secret" not in str(exc_info.value)
