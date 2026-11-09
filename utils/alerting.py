"""
告警分級狀態機（🔴 立即／🟡 每日彙整／⚪ 不送）

設計依據：2026-11-08 Telegram 告警盤點；2026-11-09 改成「收集器真的壞掉才發」。
規則正本寫在 docs/handbook/operations.md §1，這裡只實作。

- 🔴：開始一次、恢復一次；持續中不重送，只有超過 ALERT_STILL_DOWN_HOURS 仍未恢復補送一次。
- 🟡：只記錄，次日 08:00 日報列出（「現在還壞的」「昨天壞過、已恢復」）。
- collector 的即時告警只看「多久沒新資料」（tasks/staleness_watch.py 每 STALE_CHECK_INTERVAL_MIN
  分鐘判斷一次，交給 update_staleness()）：門檻見 staleness_threshold_min()；同一輪新壞的、
  恢復的、滿 24 小時仍未恢復的各合成一則。key `stale:<name>`。
- collector 的失敗（收集失敗、DB 寫入失敗）本身**不發**即時訊息：連續失敗達門檻（threshold_for）
  只進 🟡（日報「現在還壞的」），未達門檻就恢復的瞬斷只計入日報「小錯誤」。
  同一 collector 的「收集失敗」與「DB 寫入失敗」共用 key `collector:<name>`（reasons 集合）。

狀態存在 {LOCAL_DATA_DIR}/alert_state.json（與 anomaly_state.json 同目錄），寫入 tmp+rename。
Telegram 未設定（HiCloud VM 子行程）時 send_telegram 回 False，不影響收集。
"""

from __future__ import annotations

import json
import logging
import os
import threading
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
    return {"version": 2, "entries": {}, "daily": {}}


def _migrate(state: dict) -> dict:
    """v1（連續失敗觸發 🔴、pending／burst 合併）→ v2：丟掉 pending／burst，
    collector:* 一律降成 🟡（失敗不再發即時訊息；部署當下進行中的舊 🔴 不會收到恢復）。"""
    if state.get("version", 1) >= 2:
        return state
    state.pop("pending", None)
    state.pop("burst", None)
    for key, e in state.get("entries", {}).items():
        if key.startswith("collector:") and isinstance(e, dict):
            e["level"] = YELLOW
            e["notified_red"] = False
    state["version"] = 2
    return state


def load_state() -> dict:
    p = state_path()
    try:
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                base = _empty_state()
                base["version"] = 1  # 舊檔沒有 version → 視為 v1
                base.update(data)
                return _migrate(base)
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
    """回傳 (連續次數 N, 最短持續分鐘)。只決定日報 🟡「現在還壞的」，不發即時訊息。"""
    ov = _overrides().get(name)
    if ov:
        return ov
    interval = interval_minutes if interval_minutes is not None else _INTERVALS.get(name, 0)
    if interval >= config.ALERT_DAILY_INTERVAL_MIN:
        return config.ALERT_DAILY_N, 0
    return config.CONSECUTIVE_ERROR_THRESHOLD, config.ALERT_MIN_DURATION_MIN


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
    """collector 一次失敗。回傳轉換結果：None／'alerting'。

    不發任何即時訊息：達門檻只標成 🟡（日報「現在還壞的」）。真的沒資料了由
    update_staleness() 發 🔴。
    """
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
                entry.update(state="alerting", level=YELLOW, alerted_at=now.isoformat(),
                             notified_red=False,
                             first_seen=min(entry.get("first_seen", now.isoformat()), r["since"]))
                transition = "alerting"
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
            _record_resolved(state, key, entry, now)  # 🟡 的恢復只進日報
            transition = "recovered"
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
                if _still_down_due(entry, now):
                    entry["still_down_sent"] = True
                    first = _parse(entry.get("first_seen")) or now
                    to_send.append(f"⏳ 仍未恢復（已 {_fmt_duration(now - first)}）：{entry.get('last_text') or key}"
                                   f"{_instance_tag()}\n之後只在每日日報列出，恢復時會再通知。")
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
# 內部：恢復、仍未恢復
# ─────────────────────────────────────────────────────────

def _record_resolved(state: dict, key: str, entry: dict, now: datetime) -> None:
    _daily(state, now)["resolved"].append({
        "key": key, "level": entry.get("level"), "text": entry.get("last_text"),
        "first_seen": entry.get("first_seen"), "resolved_at": now.isoformat(),
    })


def _still_down_due(entry: dict, now: datetime) -> bool:
    if entry.get("level") != RED or not entry.get("notified_red") or entry.get("still_down_sent"):
        return False
    # 從「通報當下」算（stale:* 的 first_seen 是最後一筆資料時間，每日類一通報就已超過 24 小時）
    since = _parse(entry.get("alerted_at")) or _parse(entry.get("first_seen")) or now
    return now - since >= timedelta(hours=config.ALERT_STILL_DOWN_HOURS)


# ─────────────────────────────────────────────────────────
# 收集器「太久沒新資料」（tasks/staleness_watch.py 每輪呼叫）
# ─────────────────────────────────────────────────────────

_MIN_STALE_MIN = 30  # 高頻收集器至少等 30 分鐘
_DAY_MIN = 1440


def staleness_threshold_min(interval_minutes: int) -> int:
    """多久沒新資料算「疑似壞掉」（分鐘）。門檻唯一出處。

    間隔 < 1 天：max(30 分, 3 × 間隔)（5 分→30 分、15 分→45 分、60 分→3 小時）
    間隔 ≥ 1 天：2 × 間隔（每天→晚一天才報、每週→晚一週才報）
    """
    interval = max(int(interval_minutes or 0), 1)
    if interval >= _DAY_MIN:
        return 2 * interval
    return max(_MIN_STALE_MIN, 3 * interval)


def _stale_line(f: dict, now: datetime) -> str:
    what = "沒有新資料" if f.get("basis") == "table" else "沒有成功執行"
    last = f.get("last_at")
    ago = _fmt_duration(now - last) if last else "（從未有紀錄）"
    tail = (f"，最後{'一筆' if f.get('basis') == 'table' else '成功'} {last:%m/%d %H:%M}"
            if last else "")
    return f"已 {ago}{what}（門檻 {_fmt_duration(timedelta(minutes=f['threshold_min']))}{tail}）"


def update_staleness(findings: list[dict], now: Optional[datetime] = None) -> list[str]:
    """一輪「多久沒新資料」的判斷結果 → 送出合併後的訊息，回傳實際送出的訊息（測試用）。

    findings：這一輪有檢查的收集器，每筆
      {name, stale: bool, last_at: datetime|None, threshold_min: int, basis: 'table'|'success'}
    沒出現在 findings 的 stale:* 告警（停用、季節外）安靜收掉，只進日報「已恢復」。
    findings 可帶 last_success（datetime）：存進狀態檔 last_success，重啟後當「最後成功執行」的
    下限（DB collector_status 只在有寫入時更新，沒新事件的收集器會顯示很舊）。
    """
    now = _now(now)
    started, recovered, still = [], [], []
    with _LOCK:
        state = load_state()
        entries = state["entries"]
        persisted = state.setdefault("last_success", {})
        seen = set()
        for f in findings:
            name = f["name"]
            if f.get("last_success") is not None:
                persisted[name] = _now(f["last_success"]).isoformat()
            key = f"stale:{name}"
            seen.add(key)
            entry = entries.get(key)
            line = _stale_line(f, now)
            if f.get("stale"):
                if entry is None:
                    first = f.get("last_at") or now
                    entry = entries[key] = {
                        "state": "alerting", "kind": "stale", "name": name, "level": RED,
                        "first_seen": _now(first).isoformat(), "alerted_at": now.isoformat(),
                        "notified_red": True, "still_down_sent": False, "basis": f.get("basis"),
                    }
                    started.append((name, line))
                elif _still_down_due(entry, now):
                    entry["still_down_sent"] = True
                    still.append(name)
                entry["last_text"] = f"收集器 {display_name(name)} {line}"
                entry["last_seen"] = now.isoformat()
            elif entry is not None:
                first = _parse(entry.get("first_seen")) or now
                _record_resolved(state, key, entry, now)
                entries.pop(key, None)
                recovered.append((name, _fmt_duration(now - first)))
        for key in [k for k, e in entries.items() if e.get("kind") == "stale" and k not in seen]:
            _record_resolved(state, key, entries[key], now)
            entries.pop(key, None)
        save_state(state)

    tag = _instance_tag()
    messages: list[str] = []
    if len(started) == 1:
        name, line = started[0]
        messages.append(f"🔴 收集器疑似壞掉：{display_name(name)}{tag}\n{line}\n"
                        "恢復（又有新資料）時會再通知一則；持續中不重送。")
    elif started:
        lines = [f"🔴 {len(started)} 支收集器疑似壞掉（太久沒有新資料）{tag}"]
        lines += [f"• {display_name(n)}：{line}" for n, line in started]
        lines.append("恢復時會再通知；持續中不重送。")
        messages.append("\n".join(lines))
    if len(recovered) == 1:
        name, dur = recovered[0]
        messages.append(f"✅ {display_name(name)} 恢復，又有新資料（中斷約 {dur}）{tag}")
    elif recovered:
        lines = [f"✅ {len(recovered)} 支收集器恢復，又有新資料{tag}"]
        lines += [f"• {display_name(n)}（中斷約 {d}）" for n, d in recovered]
        messages.append("\n".join(lines))
    if still:
        names = "、".join(display_name(n) for n in still)
        messages.append(f"⏳ 通報後已超過 {config.ALERT_STILL_DOWN_HOURS:g} 小時仍未恢復：{names}{tag}\n"
                        "之後只在每日日報列出，恢復時會再通知。")
    for msg in messages:
        _send(msg)
    return messages


def persisted_last_success() -> dict[str, datetime]:
    """update_staleness 存下的各收集器最後成功執行時間（跨重啟）。"""
    out = {}
    for name, ts in load_state().get("last_success", {}).items():
        dt = _parse(ts)
        if dt:
            out[name] = dt
    return out


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
    # 同一支收集器同時有「連續失敗 🟡」與「太久沒新資料 🔴」→ 只列 🔴
    stale_names = {a["name"] for a in active if a["key"].startswith("stale:")}
    active = [a for a in active if not (a["key"].startswith("collector:") and a["name"] in stale_names)]
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
