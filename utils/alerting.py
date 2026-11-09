"""
告警分級狀態機（🔴 立即／🟡 每日彙整／⚪ 不送）

設計依據：2026-11-08 Telegram 告警盤點（全綠日 1 則、平常日 1–3 則、大事故 2 則）。
規則正本寫在 docs/handbook/operations.md §1，這裡只實作。

- 🔴：開始一次、恢復一次；持續中不重送，只有超過 ALERT_STILL_DOWN_HOURS 仍未恢復補送一次。
- 🟡：只記錄，次日 08:00 日報列出（「現在還壞的」「昨天壞過、已恢復」）。
- collector：連續失敗達門檻才進 alerting，未達門檻就恢復的瞬斷完全不送（只計入日報「小錯誤」）。
  間隔 < ALERT_DAILY_INTERVAL_MIN：連續 N（CONSECUTIVE_ERROR_THRESHOLD）次且持續 ≥ ALERT_MIN_DURATION_MIN 分；
  間隔 ≥ ALERT_DAILY_INTERVAL_MIN（每日類）：連續 ALERT_DAILY_N 次，不加時間條件；
  ALERT_OVERRIDES 可對常瞬斷上游放寬。cross_layer_map.yaml 的 critical 決定 🔴／🟡。
- 同一 collector 的「收集失敗」與「DB 寫入失敗」共用 key `collector:<name>`（reasons 集合），
  一支 collector 不會收到兩種告警；全部 reason 都好了才算恢復。
- collector 的 🔴 先進 pending，由 flush()（main.py 每分鐘）送出：ALERT_BURST_WINDOW_SEC 內
  ≥ ALERT_BURST_MERGE 支同時進 alerting → 合成一則，之後 ALERT_BURST_HOLD_MIN 分鐘內再壞的併入，
  恢復時也合成一則。

狀態存在 {LOCAL_DATA_DIR}/alert_state.json（與 anomaly_state.json 同目錄），寫入 tmp+rename。
Telegram 未設定（HiCloud VM 子行程）時 send_telegram 回 False，不影響收集。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import config

log = logging.getLogger(__name__)

TAIPEI_TZ = timezone(timedelta(hours=8))
RED, YELLOW = "red", "yellow"
_KEEP_DAYS = 3
_LOCK = threading.RLock()

# collector name → 執行間隔（分鐘）；BaseCollector.__init__ 登記，DB writer 失敗時查得到
_INTERVALS: dict[str, int] = {}
_critical_cache: Optional[dict[str, bool]] = None
_label_cache: Optional[dict[str, str]] = None

# 測試注入點：預設走 Telegram 純文字（錯誤訊息常含 Markdown 特殊字元）
_sender: Optional[Callable[[str], bool]] = None


# ─────────────────────────────────────────────────────────
# 基礎設施
# ─────────────────────────────────────────────────────────

def _now(now: Optional[datetime] = None) -> datetime:
    now = now or datetime.now(TAIPEI_TZ)
    if now.tzinfo is None:
        now = now.replace(tzinfo=TAIPEI_TZ)
    return now.astimezone(TAIPEI_TZ)


def _parse(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return _now(datetime.fromisoformat(ts))
    except ValueError:
        return None


def state_path() -> Path:
    return Path(getattr(config, "LOCAL_DATA_DIR", Path("./data"))) / "alert_state.json"


def _empty_state() -> dict:
    return {"version": 1, "entries": {}, "pending": [], "daily": {}, "burst": None}


def load_state() -> dict:
    p = state_path()
    try:
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                base = _empty_state()
                base.update(data)
                return base
    except Exception as exc:  # 壞檔不擋收集，重新開始
        log.warning("alert_state 讀取失敗，重新開始: %s", exc)
    return _empty_state()


def save_state(state: dict) -> None:
    p = state_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f".{p.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, p)
    except Exception as exc:
        log.warning("alert_state 寫入失敗: %s", exc)


def _instance_tag() -> str:
    return f" [{config.INSTANCE_NAME}]" if getattr(config, "INSTANCE_NAME", "") else ""


def _send(text: str) -> bool:
    if _sender is not None:
        return bool(_sender(text))
    try:
        from utils.notify import send_telegram
        return send_telegram(text, parse_mode=None)
    except Exception as exc:  # 通知失敗不影響主程序
        log.warning("alert 發送失敗: %s", exc)
        return False


def set_sender(fn: Optional[Callable[[str], bool]]) -> None:
    """測試用：替換送出函式（None 還原為 Telegram）。"""
    global _sender
    _sender = fn


def _daily(state: dict, now: datetime) -> dict:
    day = now.date().isoformat()
    daily = state.setdefault("daily", {})
    bucket = daily.setdefault(day, {"errors": {}, "resolved": [], "events": []})
    for k in sorted(daily)[:-_KEEP_DAYS]:
        daily.pop(k, None)
    return bucket


def _fmt_duration(delta: timedelta) -> str:
    mins = int(delta.total_seconds() // 60)
    if mins < 60:
        return f"{mins} 分"
    hours, mins = divmod(mins, 60)
    if hours < 48:
        return f"{hours} 小時 {mins} 分" if mins else f"{hours} 小時"
    return f"{hours // 24} 天 {hours % 24} 小時"


def _short(text: str, limit: int = 160) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ─────────────────────────────────────────────────────────
# collector 規則
# ─────────────────────────────────────────────────────────

def register_collector(name: str, interval_minutes: int) -> None:
    _INTERVALS[name] = int(interval_minutes or 0)


def _overrides() -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for item in str(getattr(config, "ALERT_OVERRIDES", "") or "").split(","):
        parts = [p.strip() for p in item.split(":")]
        if len(parts) != 3 or not parts[0]:
            continue
        try:
            out[parts[0]] = (int(parts[1]), int(parts[2]))
        except ValueError:
            log.warning("ALERT_OVERRIDES 格式錯誤，略過: %s", item)
    return out


def threshold_for(name: str, interval_minutes: Optional[int] = None) -> tuple[int, int]:
    """回傳 (連續次數 N, 最短持續分鐘)。"""
    ov = _overrides().get(name)
    if ov:
        return ov
    interval = interval_minutes if interval_minutes is not None else _INTERVALS.get(name, 0)
    if interval >= config.ALERT_DAILY_INTERVAL_MIN:
        return config.ALERT_DAILY_N, 0
    return config.CONSECUTIVE_ERROR_THRESHOLD, config.ALERT_MIN_DURATION_MIN


def is_critical(name: str) -> bool:
    """config/cross_layer_map.yaml 的 critical（以 collector 為鍵）。"""
    global _critical_cache
    if _critical_cache is None:
        try:
            from tasks.monitoring import load_cross_layer_map
            _critical_cache = {k: bool(v.get("critical")) for k, v in load_cross_layer_map().items()
                               if isinstance(v, dict)}
        except Exception as exc:
            log.warning("讀 cross_layer_map 失敗，critical 一律視為 False: %s", exc)
            _critical_cache = {}
    return _critical_cache.get(name, False)


def _set_critical_cache(mapping: Optional[dict[str, bool]]) -> None:
    """測試用。"""
    global _critical_cache
    _critical_cache = mapping


def display_name(name: str) -> str:
    """收集器顯示名：cross_layer_map 的中文 label＋（原名）；沒 label 或非收集器退回原名。

    只在格式化訊息時查，不寫進 alert_state（name 仍是識別用的原名）。
    """
    global _label_cache
    if _label_cache is None:
        try:
            from tasks.monitoring import load_cross_layer_map
            _label_cache = {k: str(v["label"]) for k, v in load_cross_layer_map().items()
                            if isinstance(v, dict) and v.get("label")}
        except Exception as exc:
            log.warning("讀 cross_layer_map label 失敗，一律用原名: %s", exc)
            _label_cache = {}
    label = _label_cache.get(name)
    return f"{label}（{name}）" if label else name


def _set_label_cache(mapping: Optional[dict[str, str]]) -> None:
    """測試用。"""
    global _label_cache
    _label_cache = mapping


# ─────────────────────────────────────────────────────────
# 主要 API
# ─────────────────────────────────────────────────────────

def collector_failed(name: str, error: str, *, reason: str = "collect",
                     interval_minutes: Optional[int] = None,
                     now: Optional[datetime] = None) -> Optional[str]:
    """collector 一次失敗。回傳轉換結果：None／'alerting'。"""
    now = _now(now)
    if interval_minutes is not None:
        register_collector(name, interval_minutes)
    key = f"collector:{name}"
    with _LOCK:
        state = load_state()
        bucket = _daily(state, now)
        bucket["errors"][name] = bucket["errors"].get(name, 0) + 1
        entry = state["entries"].setdefault(key, {
            "state": "failing", "reasons": {}, "first_seen": now.isoformat(), "kind": "collector",
            "name": name,
        })
        r = entry["reasons"].setdefault(reason, {"count": 0, "since": now.isoformat()})
        r["count"] += 1
        r["last_error"] = _short(error, 300)
        entry["last_seen"] = now.isoformat()
        entry["last_text"] = r["last_error"]
        transition = None
        if entry["state"] == "failing":
            n, min_dur = threshold_for(name, interval_minutes)
            since = _parse(r["since"]) or now
            if r["count"] >= n and (now - since) >= timedelta(minutes=min_dur):
                level = RED if is_critical(name) else YELLOW
                entry.update(state="alerting", level=level, alerted_at=now.isoformat(),
                             notified_red=False, still_down_sent=False,
                             first_seen=min(entry.get("first_seen", now.isoformat()), r["since"]))
                state["pending"].append({"kind": "start", "key": key, "level": level,
                                         "at": now.isoformat()})
                transition = "alerting"
        else:
            _maybe_still_down(state, key, entry, now)
        save_state(state)
    return transition


def collector_succeeded(name: str, *, reason: str = "collect",
                        now: Optional[datetime] = None) -> Optional[str]:
    """collector 一次成功（reason 對應的失敗清掉）。全部 reason 清空才算恢復。"""
    key = f"collector:{name}"
    with _LOCK:
        state = load_state()
        entry = state["entries"].get(key)
        if not entry or reason not in entry.get("reasons", {}):
            return None
        now = _now(now)
        entry["reasons"].pop(reason, None)
        if entry["reasons"]:
            save_state(state)
            return None
        transition = None
        if entry["state"] == "alerting":
            _resolve(state, key, entry, now)
            transition = "recovered"
        else:
            state["entries"].pop(key, None)  # 未達門檻的瞬斷：不送任何東西
        save_state(state)
    return transition


def raise_alert(key: str, level: str, text: str, *, now: Optional[datetime] = None,
                recovery: bool = True) -> Optional[str]:
    """非 collector 類（磁碟、備份、歸檔、軌跡匯出…）的告警：ok→alerting 時 🔴 立即送、🟡 只記錄。

    已在 alerting：🟡→🔴 升級時補送一則；🔴 超過 ALERT_STILL_DOWN_HOURS 補送一次「仍未恢復」。
    recovery=False：恢復時不送通知（例：watchdog 這類單次事件）。
    """
    now = _now(now)
    to_send: list[str] = []
    with _LOCK:
        state = load_state()
        entry = state["entries"].get(key)
        if entry is None or entry.get("state") != "alerting":
            entry = {"state": "alerting", "kind": "generic", "level": level,
                     "first_seen": now.isoformat(), "alerted_at": now.isoformat(),
                     "notified_red": False, "still_down_sent": False, "recovery": recovery}
            state["entries"][key] = entry
            transition = "alerting"
            if level == RED:
                entry["notified_red"] = True
                to_send.append(f"🔴 {text}{_instance_tag()}")
        else:
            transition = None
            if level == RED and entry.get("level") != RED:
                entry["notified_red"] = True
                to_send.append(f"🔴 {text}{_instance_tag()}")
                transition = "escalated"
            elif level == RED:
                msg = _maybe_still_down(state, key, entry, now, queue=False)
                if msg:
                    to_send.append(msg)
            entry["level"] = level  # 🔴→🟡 降級不送訊息，但 notified_red 保留，恢復時照送
        entry["last_seen"] = now.isoformat()
        entry["last_text"] = _short(text, 300)
        entry["recovery"] = recovery
        save_state(state)
    for msg in to_send:
        _send(msg)
    return transition


def clear_alert(key: str, text: Optional[str] = None, *,
                now: Optional[datetime] = None) -> Optional[str]:
    """alerting→ok。只有送過 🔴 的才送恢復；🟡 的恢復只進日報「昨天壞過、已恢復」。"""
    now = _now(now)
    msg = None
    with _LOCK:
        state = load_state()
        entry = state["entries"].get(key)
        if not entry or entry.get("state") != "alerting":
            if entry:
                state["entries"].pop(key, None)
                save_state(state)
            return None
        first = _parse(entry.get("first_seen")) or now
        _record_resolved(state, key, entry, now)
        state["entries"].pop(key, None)
        if entry.get("notified_red") and entry.get("recovery", True):
            body = text or entry.get("last_text") or key
            msg = f"✅ 恢復：{body}（中斷 {_fmt_duration(now - first)}）{_instance_tag()}"
        save_state(state)
    if msg:
        _send(msg)
    return "recovered"


def event(key: str, level: str, text: str, *, now: Optional[datetime] = None) -> None:
    """單次事件（沒有恢復可言）：🔴 立即送；兩種都記進當日 events 給日報。"""
    now = _now(now)
    with _LOCK:
        state = load_state()
        _daily(state, now)["events"].append({"key": key, "level": level, "text": _short(text, 300),
                                             "at": now.isoformat()})
        save_state(state)
    if level == RED:
        _send(f"🔴 {text}{_instance_tag()}")


def record_routine(name: str, ok: bool, summary: str = "", *,
                   now: Optional[datetime] = None) -> None:
    """每日例行任務（歸檔、軌跡匯出）的最近一次結果，給日報「例行」行。"""
    now = _now(now)
    with _LOCK:
        state = load_state()
        state.setdefault("routines", {})[name] = {"at": now.isoformat(), "ok": bool(ok),
                                                  "summary": _short(summary, 120)}
        save_state(state)


def recent_events(key: str, since: datetime) -> list[dict]:
    state = load_state()
    out = []
    for bucket in state.get("daily", {}).values():
        for ev in bucket.get("events", []):
            at = _parse(ev.get("at"))
            if ev.get("key") == key and at and at >= since:
                out.append(ev)
    return out


# ─────────────────────────────────────────────────────────
# 內部：恢復、仍未恢復、pending flush
# ─────────────────────────────────────────────────────────

def _record_resolved(state: dict, key: str, entry: dict, now: datetime) -> None:
    _daily(state, now)["resolved"].append({
        "key": key, "level": entry.get("level"), "text": entry.get("last_text"),
        "first_seen": entry.get("first_seen"), "resolved_at": now.isoformat(),
    })


def _resolve(state: dict, key: str, entry: dict, now: datetime) -> None:
    """collector 由 alerting 恢復。送過 🔴（含合併告警成員）才排一則恢復。"""
    _record_resolved(state, key, entry, now)
    state["entries"].pop(key, None)
    # 還沒 flush 出去的開始事件直接撤掉（開始＋恢復都在同一窗內 → 什麼都不送）
    before = len(state["pending"])
    state["pending"] = [p for p in state["pending"] if not (p["key"] == key and p["kind"] == "start")]
    if len(state["pending"]) != before:
        return
    burst = state.get("burst")
    if burst and key in burst.get("members", []):
        burst.setdefault("recovered", []).append(key)
        burst["last_recovery_at"] = now.isoformat()
        return
    if entry.get("notified_red"):
        first = _parse(entry.get("first_seen")) or now
        state["pending"].append({"kind": "recover", "key": key, "level": RED, "at": now.isoformat(),
                                 "name": entry.get("name", key),
                                 "duration": _fmt_duration(now - first)})


def _maybe_still_down(state: dict, key: str, entry: dict, now: datetime,
                      queue: bool = True) -> Optional[str]:
    if entry.get("level") != RED or not entry.get("notified_red") or entry.get("still_down_sent"):
        return None
    first = _parse(entry.get("first_seen")) or now
    if now - first < timedelta(hours=config.ALERT_STILL_DOWN_HOURS):
        return None
    burst = state.get("burst")
    keys = [key]
    if burst and key in burst.get("members", []):
        # 合併告警的成員一起報一次
        keys = [k for k in burst["members"] if k not in burst.get("recovered", [])
                and state["entries"].get(k, {}).get("state") == "alerting"]
    for k in keys:
        if k in state["entries"]:
            state["entries"][k]["still_down_sent"] = True
    names = "、".join(display_name(state["entries"][k].get("name", k)) for k in keys if k in state["entries"])
    msg = (f"⏳ 仍未恢復（已 {_fmt_duration(now - first)}）：{names}{_instance_tag()}\n"
           "之後只在每日日報列出，恢復時會再通知。")
    if queue:
        state["pending"].append({"kind": "raw", "key": key, "level": RED, "at": now.isoformat(),
                                 "text": msg})
        return None
    return msg


def flush(now: Optional[datetime] = None) -> list[str]:
    """送出 pending（main.py 每分鐘呼叫）。回傳實際送出的訊息（測試用）。"""
    now = _now(now)
    window = timedelta(seconds=config.ALERT_BURST_WINDOW_SEC)
    messages: list[str] = []
    with _LOCK:
        state = load_state()
        pending = state.get("pending", [])
        burst = state.get("burst")
        if burst and not burst.get("recovered") and now >= (_parse(burst.get("until")) or now) \
                and not _burst_open_members(state, burst):
            state["burst"] = None  # hold 結束、成員都已不在（例：狀態檔被清）→ 收掉

        oldest = min((_parse(p["at"]) for p in pending), default=None)
        if pending and oldest and now - oldest >= window:
            starts = [p for p in pending if p["kind"] == "start"]
            recovers = [p for p in pending if p["kind"] == "recover"]
            raws = [p for p in pending if p["kind"] == "raw"]
            state["pending"] = []
            messages += _flush_starts(state, starts, now)
            if recovers:
                messages.append(_fmt_recoveries(recovers))
            messages += [p["text"] for p in raws]

        burst = state.get("burst")
        if burst and burst.get("recovered"):
            last = _parse(burst.get("last_recovery_at")) or now
            remaining = _burst_open_members(state, burst)
            hold_over = now >= (_parse(burst.get("until")) or now)
            if now - last >= window and (not remaining or hold_over):
                messages.append(_fmt_burst_recovery(state, burst, remaining, now))
                state["burst"] = None
        save_state(state)
    for msg in messages:
        _send(msg)
    return messages


def _burst_open_members(state: dict, burst: dict) -> list[str]:
    return [k for k in burst.get("members", [])
            if k not in burst.get("recovered", []) and k in state["entries"]]


def _flush_starts(state: dict, starts: list[dict], now: datetime) -> list[str]:
    live = [p for p in starts if state["entries"].get(p["key"], {}).get("state") == "alerting"]
    if not live:
        return []
    burst = state.get("burst")
    if burst and now < (_parse(burst.get("until")) or now):
        # 合併告警持續中：併入，不另送
        for p in live:
            burst["members"].append(p["key"])
            state["entries"][p["key"]]["notified_red"] = True
        return []
    entries = [state["entries"][p["key"]] for p in live]
    reds = [e for e in entries if e.get("level") == RED]
    if len(live) >= config.ALERT_BURST_MERGE:
        state["burst"] = {
            "started": now.isoformat(),
            "until": (now + timedelta(minutes=config.ALERT_BURST_HOLD_MIN)).isoformat(),
            "members": [p["key"] for p in live], "recovered": [],
        }
        for e in entries:
            e["notified_red"] = True
        if not reds:
            return []  # 全部非 critical：合併成一件事，但只進日報
        return [_fmt_burst(entries)]
    for e in reds:
        e["notified_red"] = True
    if not reds:
        return []
    if len(reds) == 1:
        e = reds[0]
        n = sum(r.get("count", 0) for r in e.get("reasons", {}).values())
        first = _parse(e.get("first_seen")) or now
        what = "DB 寫入" if set(e.get("reasons", {})) == {"db"} else "收集器"
        return [f"🔴 {what} {display_name(e['name'])} 連續失敗（{n} 次，已 {_fmt_duration(now - first)}）"
                f"{_instance_tag()}\n錯誤：{e.get('last_text', '')}\n"
                "恢復時會再通知一則；持續中不重送。"]
    lines = [f"🔴 {len(reds)} 支收集器連續失敗{_instance_tag()}"]
    for e in reds:
        lines.append(f"• {display_name(e['name'])}：{_short(e.get('last_text', ''), 100)}")
    lines.append("恢復時會再通知；持續中不重送。")
    return ["\n".join(lines)]


def _common_error(entries: list[dict]) -> tuple[str, int]:
    c = Counter(_short(e.get("last_text", ""), 80) for e in entries)
    text, n = c.most_common(1)[0]
    return text, n


def _fmt_burst(entries: list[dict]) -> str:
    err, n = _common_error(entries)
    names = [display_name(e["name"]) for e in entries]
    crit = [display_name(e["name"]) for e in entries if e.get("level") == RED]
    shown = "、".join(names[:12]) + (f"…等 {len(names)} 支" if len(names) > 12 else "")
    return (f"🔴 {len(entries)} 支收集器同時失敗{_instance_tag()}\n"
            f"共同錯誤（{n} 支）：{err}\n"
            f"含 critical：{'、'.join(crit)}\n"
            f"名單：{shown}\n"
            f"之後 {config.ALERT_BURST_HOLD_MIN} 分鐘內再壞的併入這件事；全部恢復時通知一則。")


def _fmt_recoveries(recovers: list[dict]) -> str:
    if len(recovers) == 1:
        p = recovers[0]
        return f"✅ {display_name(p['name'])} 恢復（中斷 {p['duration']}）{_instance_tag()}"
    lines = [f"✅ {len(recovers)} 支收集器恢復{_instance_tag()}"]
    lines += [f"• {display_name(p['name'])}（中斷 {p['duration']}）" for p in recovers]
    return "\n".join(lines)


def _fmt_burst_recovery(state: dict, burst: dict, remaining: list[str], now: datetime) -> str:
    started = _parse(burst.get("started")) or now
    n = len(burst.get("recovered", []))
    msg = f"✅ {n} 支收集器已恢復（事件開始於 {started:%m/%d %H:%M}，歷時約 {_fmt_duration(now - started)}）{_instance_tag()}"
    if remaining:
        names = "、".join(display_name(state["entries"][k].get("name", k)) for k in remaining)
        msg += f"\n仍未恢復：{names}（之後各自恢復時再通知）"
    return msg


# ─────────────────────────────────────────────────────────
# 日報用（唯讀）
# ─────────────────────────────────────────────────────────

def snapshot(now: Optional[datetime] = None) -> dict:
    """給日報：現在還壞的、最近 24 小時恢復的、昨天錯誤數、昨天事件。"""
    now = _now(now)
    state = load_state()
    active = []
    for key, e in state.get("entries", {}).items():
        if e.get("state") != "alerting":
            continue
        first = _parse(e.get("first_seen")) or now
        active.append({"key": key, "name": e.get("name", key), "level": e.get("level"),
                       "text": e.get("last_text", ""), "since": first,
                       "duration": _fmt_duration(now - first)})
    active.sort(key=lambda a: (a["level"] != RED, a["since"]))
    resolved, events = [], []
    cutoff = now - timedelta(hours=24)
    for bucket in state.get("daily", {}).values():
        for r in bucket.get("resolved", []):
            at = _parse(r.get("resolved_at"))
            if at and at >= cutoff:
                first = _parse(r.get("first_seen")) or at
                resolved.append({**r, "duration": _fmt_duration(at - first),
                                 "name": r["key"].split(":", 1)[-1]})
        for ev in bucket.get("events", []):
            at = _parse(ev.get("at"))
            if at and at >= cutoff:
                events.append(ev)
    yesterday = (now - timedelta(days=1)).date().isoformat()
    errors = state.get("daily", {}).get(yesterday, {}).get("errors", {})
    return {"active": active, "resolved": resolved, "events": events,
            "routines": dict(state.get("routines", {})),
            "errors_yesterday": dict(errors), "error_total_yesterday": sum(errors.values())}
