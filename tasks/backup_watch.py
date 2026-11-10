"""Supabase → S3 備份的成敗紀錄與告警（每日備份沒成功要有人知道）。

為什麼：2026-11-07 盤點發現 backup_supabase 近 16 天只跑了 4 天，Robot B 只傾印「昨天」，
漏跑的那天永遠不會進 S3，而且沒有任何告警。

- 主程序在備份子行程結束後呼叫 record_run()：判斷成敗、寫狀態檔、失敗發 Telegram。
- 每日 watchdog（BACKUP_WATCHDOG_TIME，預設 06:30）呼叫 check_today()：
  今天沒有成功紀錄（沒跑、還卡在佇列、或失敗）就發 Telegram。
- 冷卻比照 utils/disk_guard.py：同一天同一種告警只發一次；之前失敗、這次成功時發一則恢復通知。
- /health 讀 health_status()（只讀本機狀態檔，不連 DB）。

狀態檔：LOCAL_DATA_DIR/backup_watch_state.json（與 anomaly_state.json 同層，disk_guard 不會清）。
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import config

log = logging.getLogger(__name__)

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
_STATE_NAME = "backup_watch_state.json"
_KEEP_ALERT_DAYS = 14


def _state_path() -> Path:
    return Path(config.LOCAL_DATA_DIR) / _STATE_NAME


def load_state() -> dict:
    p = _state_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("backup watch 狀態檔讀取失敗: %s", exc)
        return {}


def save_state(state: dict) -> None:
    p = _state_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(p)
    except Exception as exc:
        log.warning("backup watch 狀態檔寫入失敗: %s", exc)


_URL_CREDENTIALS = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/:@]+:[^\s/@]+@")


def _redact(text: str) -> str:
    """遮 URL query 金鑰（utils.redact）＋連線字串的 user:password@（DB 錯誤可能帶 DSN）。"""
    text = _URL_CREDENTIALS.sub(r"\1<REDACTED>@", str(text))
    try:
        from utils.redact import redact_secrets
        return redact_secrets(text)
    except Exception:  # pragma: no cover
        return text


def _default_notify(text: str) -> None:
    """正式路徑：交給 utils.alerting（key=backup，🔴 開始一次、恢復一次、24h 仍未恢復補一次）。

    本模組的「每天只告警一次」去重仍保留在自己的狀態檔；alerting 再擋掉跨日重送。
    """
    try:
        from utils import alerting
        lines = text.replace("{tag}", "").split("\n")
        title = lines[0].lstrip("🚨⚠️✅ ").strip()
        body = "\n".join([title] + lines[1:])
        if text.startswith("✅"):
            alerting.clear_alert("backup", "Supabase 每日備份")
        else:
            alerting.raise_alert("backup", alerting.RED, body)
    except Exception as exc:  # 通知失敗不影響主程序
        log.warning("backup watch 通知失敗: %s", exc)


def evaluate(result: Any) -> tuple[bool, list[str]]:
    """判斷一次備份是否成功；回傳 (成功與否, 問題清單)。

    成功＝DB 與 S3 都連上、各機器人沒有未預期例外、A／B／D 沒有失敗的表。
    DB 或 S3 初始化失敗時各機器人只會印「跳過」並回傳全 0，看起來像正常——這裡要擋下。
    Robot C 的 critical 是盤點結果（例如某 collector 歸檔落後），不算備份失敗，只附在摘要。
    """
    problems: list[str] = []
    if not isinstance(result, dict):
        return False, ["備份沒有回傳結果"]
    if not result.get("db_connected"):
        problems.append("DB 沒連上")
    if not result.get("s3_ready"):
        problems.append("S3 未就緒")
    for key, label in (("static", "A 靜態"), ("realtime", "B 即時"),
                       ("reconcile", "C 盤點"), ("auth", "D auth")):
        part = result.get(key)
        if not isinstance(part, dict):
            problems.append(f"{label} 沒有結果")
            continue
        if part.get("error"):
            problems.append(f"{label} 例外: {str(part['error'])[:200]}")
            continue
        if key != "reconcile" and int(part.get("failed") or 0) > 0:
            problems.append(f"{label} 失敗 {part['failed']} 張表")
    return not problems, problems


def summarize(result: Any) -> str:
    if not isinstance(result, dict):
        return ""
    s, r = result.get("static") or {}, result.get("realtime") or {}
    c, d = result.get("reconcile") or {}, result.get("auth") or {}
    return (
        f"A 上傳 {s.get('uploaded', '?')}／略過 {s.get('skipped', '?')}；"
        f"B 上傳 {r.get('uploaded', '?')}／空日 {r.get('empty', '?')}／略過 {r.get('skipped', '?')}；"
        f"C critical {c.get('critical', '?')}；D 上傳 {d.get('uploaded', '?')}"
    )


def _prune_alerts(state: dict) -> None:
    alerted = state.get("alerted") or {}
    keep = sorted(alerted)[-_KEEP_ALERT_DAYS:]
    state["alerted"] = {k: alerted[k] for k in keep}


def _is_running(state: dict, now: datetime) -> bool:
    """running_since 在子行程強制結束時限內才算進行中（主程序中途被殺時，標記會殘留）。"""
    raw = state.get("running_since")
    if not raw:
        return False
    try:
        since = datetime.fromisoformat(raw)
    except ValueError:
        return False
    if since.tzinfo is None:
        since = since.replace(tzinfo=TAIPEI_TZ)
    limit = int(getattr(config, "BACKUP_KILL_TIMEOUT_SEC", 3600)) + 300
    return 0 <= (now - since).total_seconds() <= limit


def mark_started(now: datetime | None = None) -> None:
    """子行程啟動前呼叫：watchdog 看到「進行中」就不誤報。"""
    now = now or datetime.now(TAIPEI_TZ)
    state = load_state()
    state["running_since"] = now.isoformat()
    save_state(state)


def record_run(
    result: Any = None,
    error: str | None = None,
    now: datetime | None = None,
    notify: Callable[[str], None] | None = None,
) -> dict:
    """記錄一次備份結束（成功或失敗），必要時發告警或恢復通知。回傳更新後的狀態。"""
    notify = notify or _default_notify
    now = now or datetime.now(TAIPEI_TZ)
    today = now.date().isoformat()
    state = load_state()
    state.pop("running_since", None)

    if error is not None:
        ok, problems = False, [f"子行程失敗: {_redact(str(error))[:300]}"]
    else:
        ok, problems = evaluate(result)
        problems = [_redact(p) for p in problems]

    if ok and isinstance(result, dict) and result.get("dry_run"):
        # BACKUP_DRY_RUN：沒有真的寫 S3，不能算今天備份成功（否則 watchdog 被壓掉）
        state["last_dry_run_at"] = now.isoformat()
        save_state(state)
        return state

    prev_status = state.get("last_status")
    state["last_run_at"] = now.isoformat()
    state["last_status"] = "ok" if ok else "failed"
    state["last_problems"] = problems
    state["last_summary"] = summarize(result)
    alerted = state.setdefault("alerted", {})

    if ok:
        state["last_success_date"] = today
        state["last_success_at"] = now.isoformat()
        state["consecutive_failures"] = 0
        if prev_status == "failed" or alerted.get(today):
            notify(
                f"✅ Supabase 備份恢復正常{{tag}}\n"
                f"{now:%Y-%m-%d %H:%M} 完成；{state['last_summary']}"
            )
    else:
        state["consecutive_failures"] = int(state.get("consecutive_failures") or 0) + 1
        if not alerted.get(today):
            alerted[today] = "run_failed"
            notify(
                f"🚨 Supabase 每日備份失敗{{tag}}\n"
                f"時間：{now:%Y-%m-%d %H:%M}\n"
                f"問題：{'；'.join(problems)}\n"
                f"最後成功：{state.get('last_success_date') or '無紀錄'}"
                f"（連續失敗 {state['consecutive_failures']} 次）\n"
                f"即時表只備份「昨天」，沒補跑的那天不會進 S3。"
            )
    _prune_alerts(state)
    save_state(state)
    return state


def _audit_log_has_run_today(today_iso: str) -> bool | None:
    """狀態檔不存在時（新主機）的備援：查 backup_audit_log 今天有沒有「成功的」即時快照紀錄。

    成功證據＝今天有 realtime_snapshot 的 snapshot_uploaded／skipped_empty_day 列，且當天沒有任何
    upload_failed（失敗、略過的備份也會寫 audit 列，不能只數列數）。
    """
    if not (getattr(config, "SUPABASE_ENABLED", False) and config.SUPABASE_DB_URL):
        return None
    try:
        import psycopg2
        with psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 15000")
                cur.execute(
                    """
                    SELECT count(*) FILTER (WHERE run_kind = 'realtime_snapshot'
                                              AND code IN ('snapshot_uploaded', 'skipped_empty_day')),
                           count(*) FILTER (WHERE code = 'upload_failed')
                    FROM metadata.backup_audit_log
                    WHERE (run_at AT TIME ZONE 'Asia/Taipei')::date = %s::date
                    """,
                    (today_iso,),
                )
                realtime_rows, failed_rows = cur.fetchone()
                return realtime_rows > 0 and failed_rows == 0
    except Exception as exc:
        log.warning("backup watch 查 audit log 失敗: %s", exc)
        return None


def check_today(
    now: datetime | None = None,
    notify: Callable[[str], None] | None = None,
    audit_fallback: Callable[[str], bool | None] | None = None,
) -> str:
    """每日 watchdog：今天沒有成功紀錄就告警。回傳狀態字串（ok／running／alerted／already_alerted／disabled）。"""
    if not config.BACKUP_ENABLED:
        return "disabled"
    notify = notify or _default_notify
    audit_fallback = audit_fallback or _audit_log_has_run_today
    now = now or datetime.now(TAIPEI_TZ)
    today = now.date().isoformat()
    state = load_state()

    if state.get("last_success_date") == today:
        return "ok"
    if _is_running(state, now):
        return "running"  # 還在跑：結束時 record_run 會處理成敗
    if not state.get("last_run_at") and audit_fallback(today):
        return "ok"  # 新主機沒有狀態檔，但 DB 有今天的備份紀錄
    alerted = state.setdefault("alerted", {})
    if alerted.get(today):
        return "already_alerted"

    alerted[today] = "missing"
    if state.get("last_status") == "failed" and str(state.get("last_run_at", "")).startswith(today):
        reason = "今天的備份失敗（見上一則告警）"
    else:
        reason = "今天沒有執行紀錄（排程沒觸發、行程重啟或卡在重任務佇列）"
    notify(
        f"⚠️ Supabase 今日備份沒有成功{{tag}}\n"
        f"{reason}\n"
        f"最後成功：{state.get('last_success_date') or '無紀錄'}\n"
        f"排程 03:30（台北），本檢查 {getattr(config, 'BACKUP_WATCHDOG_TIME', '06:30')}。"
    )
    _prune_alerts(state)
    save_state(state)
    return "alerted"


def health_status() -> dict:
    """給 /health 用（只讀本機狀態檔）。"""
    state = load_state()
    return {
        "enabled": bool(config.BACKUP_ENABLED),
        "last_success_date": state.get("last_success_date"),
        "last_success_at": state.get("last_success_at"),
        "last_run_at": state.get("last_run_at"),
        "last_status": state.get("last_status"),
        "last_problems": state.get("last_problems") or [],
        "consecutive_failures": int(state.get("consecutive_failures") or 0),
        "running_since": state.get("running_since"),
    }
