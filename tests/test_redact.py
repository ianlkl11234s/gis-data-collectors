"""錯誤訊息脫敏（review F202／F242）。"""
from unittest.mock import patch

from utils.redact import redact_secrets, REDACTED
from tests.test_base_collector import DummyCollector

URL_ERR = (
    "401 Client Error: Unauthorized for url: "
    "https://opendata.cwa.gov.tw/api/v1/rest/datastore/W-C0033-001"
    "?Authorization=CWA-SECRET-123&format=JSON"
)


def test_query_params_redacted():
    out = redact_secrets(URL_ERR)
    assert "CWA-SECRET-123" not in out
    assert f"Authorization={REDACTED}&format=JSON" in out


def test_other_secret_param_names():
    for name in ("api_key", "apikey", "token", "serviceKey", "access_token"):
        out = redact_secrets(f"boom https://x.example/a?{name}=abc123&p=1")
        assert "abc123" not in out and "p=1" in out


def test_bearer_and_none():
    assert "abcdefghij123" not in redact_secrets("Authorization: Bearer abcdefghij123")
    assert redact_secrets(None) == ""
    assert redact_secrets("plain boom") == "plain boom"


@patch('collectors.base.notify_error')
def test_run_redacts_print_notify_and_heartbeat(mock_notify, capsys):
    def failing():
        raise RuntimeError(URL_ERR)

    c = DummyCollector(collect_impl=failing)
    c.supabase_writer = type("W", (), {})()
    calls = []
    c.supabase_writer._report_heartbeat = lambda *a: calls.append(a)
    stats = c.run()
    assert "CWA-SECRET-123" not in stats['error']
    assert "CWA-SECRET-123" not in mock_notify.call_args.args[1]
    assert "CWA-SECRET-123" not in capsys.readouterr().out
    assert calls and "CWA-SECRET-123" not in calls[0][3]


def test_uri_userinfo_redacted():
    """review F011：連線字串的帳密要遮。"""
    for raw, secret in (
        ("connection failed: postgresql://postgres.abc:S3cr3t-pw@pooler.example.com:6543/postgres", "S3cr3t-pw"),
        ("redis://:only-pass@cache.example:6379/0", "only-pass"),
        ("git fetch https://ghp_TOKEN123@github.example/x.git", "ghp_TOKEN123"),
        ("postgresql://u:pa/ss@h/db", "pa/ss"),
    ):
        out = redact_secrets(raw)
        assert secret not in out, out
        assert f"://{REDACTED}@" in out
    assert redact_secrets("see https://host.example/path?a=1") == "see https://host.example/path?a=1"
    assert redact_secrets(redact_secrets("x postgresql://u:p@h/db")) == f"x postgresql://{REDACTED}@h/db"
