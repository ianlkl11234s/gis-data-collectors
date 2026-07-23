"""Pure, secret-free status policy for GIS collector monitoring."""
from __future__ import annotations

from datetime import datetime
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
