from datetime import datetime, timezone

from scripts.gis_collectors_monitor_policy import (
    classify_anomaly,
    classify_archive,
    classify_gfw_hourly_publish_health,
    transition_incident,
)


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


def test_gfw_failed_attempt_never_masks_current_release_freshness():
    health = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "failed",
        "latest_attempt_started_at": datetime(2026, 9, 18, 1, tzinfo=timezone.utc),
        "published_at": datetime(2026, 9, 17, 2, tzinfo=timezone.utc),
        "latest_complete_date": "2026-09-13",
    }, datetime(2026, 9, 18, 3, tzinfo=timezone.utc))

    assert health["state"] == "FAILED"
    assert health["level"] == "watch"
    assert health["source_age_days"] == 5
    assert health["source_required_on_or_after"].isoformat() == "2026-09-11"


def test_gfw_old_running_and_source_stale_are_not_fresh():
    now = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)
    old_running = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "running",
        "latest_attempt_started_at": datetime(2026, 9, 17, 1, tzinfo=timezone.utc),
        "published_at": datetime(2026, 9, 18, 11, tzinfo=timezone.utc),
        "latest_complete_date": "2026-09-13",
    }, now)
    stale_source = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "succeeded",
        "latest_attempt_started_at": datetime(2026, 9, 18, 1, tzinfo=timezone.utc),
        "published_at": datetime(2026, 9, 10, 2, tzinfo=timezone.utc),
        "latest_complete_date": "2026-09-10",
    }, now)

    assert old_running["state"] == "OLD_RUNNING"
    assert old_running["level"] == "critical"
    assert stale_source["state"] == "SOURCE_STALE"
    assert stale_source["level"] == "critical"


def test_gfw_utc_schedule_grace_and_stale_success_are_independent_of_attempt():
    before_next_run = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "failed",
        "latest_attempt_started_at": "2026-09-18T00:10:00Z",
        "published_at": "2026-09-17T23:00:00Z",
        "latest_complete_date": "2026-09-12",
    }, datetime(2026, 9, 18, 0, 15, tzinfo=timezone.utc))
    after_grace = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "failed",
        "latest_attempt_started_at": "2026-09-19T08:00:00+00:00",
        "published_at": "2026-09-17T01:00:00+00:00",
        "latest_complete_date": "2026-09-13",
    }, datetime(2026, 9, 19, 12, tzinfo=timezone.utc))

    assert before_next_run["source_required_on_or_after"].isoformat() == "2026-09-11"
    assert before_next_run["state"] == "FAILED"
    assert before_next_run["latest_attempt_started_at"].tzinfo is not None
    assert after_grace["state"] == "PUBLISH_STALE"
    assert after_grace["level"] == "critical"


def test_gfw_real_rpc_shape_failed_attempt_with_old_current_is_publish_stale():
    health = classify_gfw_hourly_publish_health({
        "latest_attempt_status": "failed",
        "latest_attempt_started_at": "2026-09-18T01:00:00+00:00",
        "release_id": "2026-08-21",
        "latest_complete_date": "2026-08-21",
        "sar_latest_complete_date": "2026-08-21",
        "published_at": "2026-08-26T00:00:00+00:00",
        "freshness_hours": 551.0,
    }, datetime(2026, 9, 18, 23, tzinfo=timezone.utc))

    assert health["state"] == "SOURCE_STALE"
    assert health["level"] == "critical"
    assert health["published_at"].tzinfo is not None
