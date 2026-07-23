from datetime import datetime, timezone

from scripts.gis_collectors_monitor_policy import classify_anomaly, classify_archive, transition_incident


def test_known_noncritical_event_and_dedup_anomalies_are_expected():
    event = classify_anomaly({
        "state": "NEVER", "critical": False,
        "notes": "事件驅動（無落雷即無資料）",
    })
    dedup = classify_anomaly({
        "state": "DEAD", "critical": False,
        "notes": "dedup by hash，僅新事故推進 MAX",
    })
    disabled = classify_anomaly({
        "state": "STALE", "critical": False,
        "notes": "collector disabled",
    })

    assert event["level"] == "expected"
    assert dedup["level"] == "expected"
    assert disabled["level"] == "expected"


def test_archive_is_watch_before_deadline_and_critical_after_deadline():
    item = {"state": "STALE", "critical": True}
    before = classify_archive(item, datetime(2026, 7, 22, 3, 59, tzinfo=timezone.utc), 4)
    after = classify_archive(item, datetime(2026, 7, 22, 4, 0, tzinfo=timezone.utc), 4)

    assert before["level"] == "watch"
    assert after["level"] == "critical"


def test_incident_only_alerts_on_new_worsened_and_recovered_transitions():
    now = "2026-07-22T05:00:00+00:00"
    first, state = transition_incident({}, "supabase_unavailable", "watch", now)
    second, state = transition_incident(state, "supabase_unavailable", "watch", now)
    worse, state = transition_incident(state, "supabase_unavailable", "critical", now)
    recovered, state = transition_incident(state, "supabase_unavailable", "ok", now)

    assert first["event"] == "new"
    assert second["event"] == "silent"
    assert worse["event"] == "worsened"
    assert recovered["event"] == "recovered"


def test_transient_watch_escalates_to_critical_on_third_consecutive_run():
    state = {}
    _, state = transition_incident(state, "supabase_unavailable", "watch", "t1")
    _, state = transition_incident(state, "supabase_unavailable", "watch", "t2")
    third, state = transition_incident(state, "supabase_unavailable", "watch", "t3")

    assert third == {
        "event": "worsened",
        "fingerprint": "supabase_unavailable",
        "level": "critical",
    }
    assert state["supabase_unavailable"]["level"] == "critical"
