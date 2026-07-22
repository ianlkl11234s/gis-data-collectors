import json

from scripts.diagnose_health_snapshot import sanitize_error, summarize_probe


def test_sanitize_error_never_returns_dsn_or_raw_database_message():
    class FakeOperationalError(Exception):
        pass

    error = FakeOperationalError("password=secret host=db.example.internal connection refused")

    assert sanitize_error(error) == "FakeOperationalError"


def test_probe_summary_emits_only_safe_metrics():
    class FakeOperationalError(Exception):
        pass

    rendered = summarize_probe({
        "connect_ms": 12.4,
        "execute_ms": 55.2,
        "fetch_ms": 3.1,
        "row_count": 71,
        "error": FakeOperationalError("password=secret"),
    })
    payload = json.loads(rendered)

    assert payload == {
        "connect_ms": 12.4,
        "execute_ms": 55.2,
        "fetch_ms": 3.1,
        "row_count": 71,
        "error_type": "FakeOperationalError",
    }
