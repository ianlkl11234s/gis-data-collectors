"""Regression coverage for the pytest safety boundary."""

import importlib
import os


def test_config_reload_does_not_read_checkout_dotenv(monkeypatch, tmp_path):
    import config

    (tmp_path / ".env").write_text(
        "OPENROUTER_API_KEY=must-not-enter-tests\n"
        "TELEGRAM_BOT_TOKEN=must-not-enter-tests\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    reloaded = importlib.reload(config)

    assert reloaded.OPENROUTER_API_KEY == ""
    assert reloaded.TELEGRAM_BOT_TOKEN is None


def test_application_credentials_are_scrubbed_before_collection():
    assert "OPENROUTER_API_KEY" not in os.environ
    assert "SUPABASE_DB_URL" not in os.environ
    assert "TELEGRAM_BOT_TOKEN" not in os.environ
    assert "TELEGRAM_CHAT_ID" not in os.environ
