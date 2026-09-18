"""Pure, secret-free status policy for GIS collector monitoring."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

_EXPECTED_MARKERS = ("事件驅動", "event-driven", "dedup", "去重", "disabled", "停用")
_LEVEL_RANK = {"ok": 0, "expected": 0, "watch": 1, "critical": 2}


def classify_anomaly(item: dict[str, Any]) -> dict[str, Any]:
    """Classify a freshness anomaly without I/O or side effects."""
    result = dict(item)
    notes = str(result.get("notes", "")).lower()
    if not result.get("critical") and any(marker in notes for marker in _EXPECTED_MARKERS):
        result["level"] = "expected"
    elif result.get("state") in {"ERROR", "DEAD"} and result.get("critical"):
        result["level"] = "critical"
    else:
        result["level"] = "watch"
    return result


def classify_archive(item: dict[str, Any], now: datetime, deadline_hour: int = 4) -> dict[str, Any]:
    """Daily archive failures are observation-only before the local deadline."""
    result = dict(item)
    if result.get("state") == "OK":
        result["level"] = "ok"
    elif now.hour < deadline_hour:
        result["level"] = "watch"
    elif result.get("critical"):
        result["level"] = "critical"
    else:
        result["level"] = "watch"
    return result


def classify_gfw_hourly_publish_health(
    health: dict[str, Any], now: datetime, *, source_lag_days: int = 5,
    schedule_grace_hours: int = 30,
) -> dict[str, Any]:
    """Classify the current GFW release separately from its last attempt.

    ``latest_complete_date`` is a UTC source-data date, while ``published_at``
    is the successful current CDN cutover time. The UTC date cutoff is shifted
    back by the daily schedule grace, so the UTC midnight boundary before the
    next publisher run does not falsely demand tomorrow's release. A new
    ``started_at`` alone therefore never makes this health check OK.
    """
    result = dict(health)
    result["source_lag_days"] = source_lag_days
    result["schedule_grace_hours"] = schedule_grace_hours
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_utc = now.astimezone(timezone.utc)
    latest_complete_date = result.get("latest_complete_date")
    if isinstance(latest_complete_date, str):
        try:
            latest_complete_date = date.fromisoformat(latest_complete_date)
        except ValueError:
            latest_complete_date = None
    result["latest_complete_date"] = latest_complete_date
    result["source_required_on_or_after"] = (
        (now_utc - timedelta(hours=schedule_grace_hours)).date()
        - timedelta(days=source_lag_days)
    )

    published_at = result.get("published_at")
    if isinstance(published_at, str):
        try:
            published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except ValueError:
            published_at = None
    result["published_at"] = published_at
    started_at = result.get("latest_attempt_started_at")
    if isinstance(started_at, str):
        try:
            started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        except ValueError:
            started_at = None
    result["latest_attempt_started_at"] = started_at

    success_age_hours = None
    if isinstance(published_at, datetime):
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        success_age_hours = max(
            0.0, (now_utc - published_at.astimezone(timezone.utc)).total_seconds() / 3600
        )
    result["success_age_hours"] = success_age_hours
    attempt_age_hours = None
    if isinstance(started_at, datetime):
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        attempt_age_hours = max(0.0, (now_utc - started_at.astimezone(timezone.utc)).total_seconds() / 3600)
    result["attempt_age_hours"] = attempt_age_hours

    if published_at is None or latest_complete_date is None:
        result.update(state="NEVER", level="critical")
        return result

    source_age_days = (now_utc.date() - latest_complete_date).days
    result["source_age_days"] = source_age_days
    if latest_complete_date < result["source_required_on_or_after"]:
        result.update(state="SOURCE_STALE", level="critical")
        return result

    if success_age_hours is not None and success_age_hours > schedule_grace_hours:
        result.update(state="PUBLISH_STALE", level="critical")
        return result

    status = str(result.get("latest_attempt_status") or "").lower()
    if status == "failed":
        result.update(state="FAILED", level="watch")
    elif status == "running" and (attempt_age_hours is None or attempt_age_hours > schedule_grace_hours):
        result.update(state="OLD_RUNNING", level="critical")
    elif started_at is None or (attempt_age_hours is not None and attempt_age_hours > schedule_grace_hours):
        result.update(state="ATTEMPT_STALE", level="watch")
    else:
        result.update(state="OK", level="ok")
    return result


def transition_incident(
    state: dict[str, dict[str, Any]], fingerprint: str, level: str, now: str
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    """Return one notification only for new, worsened, or recovered states."""
    next_state = {key: dict(value) for key, value in state.items()}
    prior = next_state.get(fingerprint)
    if level in {"ok", "expected"}:
        if prior:
            del next_state[fingerprint]
            return {"event": "recovered", "fingerprint": fingerprint, "level": "ok"}, next_state
        return {"event": "silent", "fingerprint": fingerprint, "level": "ok"}, next_state

    if prior is None:
        next_state[fingerprint] = {
            "level": level,
            "consecutive_runs": 1,
            "first_seen": now,
            "last_seen": now,
        }
        return {"event": "new", "fingerprint": fingerprint, "level": level}, next_state

    previous_level = str(prior.get("level", "watch"))
    prior["consecutive_runs"] = int(prior.get("consecutive_runs", 0)) + 1
    prior["last_seen"] = now
    if level == "watch" and prior["consecutive_runs"] >= 3:
        level = "critical"
    if _LEVEL_RANK[level] > _LEVEL_RANK.get(previous_level, 0):
        prior["level"] = level
        return {"event": "worsened", "fingerprint": fingerprint, "level": level}, next_state
    return {"event": "silent", "fingerprint": fingerprint, "level": level}, next_state
