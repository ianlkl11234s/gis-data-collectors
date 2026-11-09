"""收集器「太久沒新資料」即時告警（🔴 疑似壞掉／✅ 恢復）＋「資料庫連不上」。

使用者決定（2026-11-09）：單次或連續幾次失敗不發，只有資料真的斷了才發。
規則正本 docs/handbook/operations.md §1；門檻 utils/alerting.staleness_threshold_min()。

**零 DB 讀取**（2026-11-09 根治：原本每輪呼叫 health_snapshot，等於把 2026-09-04 Disk IO 事故
的讀取量加回來，見 gis-platform migrations/403）。判斷只用行程內紀錄，每輪純記憶體計算：

1. 檢查對象＝這個行程實際啟動的收集器（＝部署在本機）＋長駐 worker，
   cross_layer_map 標停用（enabled:false／deployment:disabled）的不檢查。
2. 「新資料」＝最後一次成功寫入 Supabase ≥1 筆的時間（storage/supabase_writer、aisstream worker
   呼叫 alerting.mark_write）。適用於有登記非事件驅動表（realtime_tables 的 owner_collector 或
   cross_layer_map supabase_tables）、而且曾經記到寫入的收集器。
   門檻間隔取 max(收集器執行間隔, 這些表裡最小的 expected_interval_min)。
3. 表全是事件驅動／停用表、沒登記表、或從沒記到寫入（例：自己直連 DB、只寫 S3）→ 改用
   「最後成功執行時間」（BaseCollector 成功時 alerting.mark_success）。表全在季外 → 不檢查。
4. 兩種時間每輪存進 alert_state.json，重啟後沿用；都沒有就從行程啟動算（啟動後一個門檻內不報）。
5. 資料庫連不上：所有收集器的 Supabase 寫入 DB_DOWN_MIN 分鐘內沒有一次成功、且期間有失敗 →
   🔴 一則（key db_down），寫入恢復成功時 ✅。這段期間不判斷個別收集器停更（避免全部一起報）。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable, Optional

from tasks import monitoring
from utils import alerting

log = logging.getLogger(__name__)

TableKey = tuple[str, str]
DB_DOWN_MIN = 30  # 所有收集器寫入連續失敗這麼久 → 資料庫連不上


def _key(t: dict) -> TableKey:
    return (t["schema"], t["table"])


def _aware(ts: Optional[datetime]) -> Optional[datetime]:
    return alerting._now(ts) if ts is not None else None


def _latest(*values: Optional[datetime]) -> Optional[datetime]:
    return max((_aware(v) for v in values if v is not None), default=None)


def plan_collector(name: str, interval_min: int, tables: list[dict], cmap: dict,
                   now: datetime) -> Optional[dict]:
    """決定這支收集器怎麼檢查；不檢查（停用、季節外）回 None。

    回 {name, tables: [TableKey], threshold_min}；tables 空＝只能用最後成功執行時間。
    （只讀 yaml，不碰 DB。）
    """
    cfg = cmap.get(name) if isinstance(cmap.get(name), dict) else None
    if monitoring._collector_off(cfg):
        return None
    listed = set((cfg or {}).get("supabase_tables") or [])
    own = [t for t in tables
           if t.get("owner_collector") == name or f"{t['schema']}.{t['table']}" in listed]
    reasons = [monitoring.stale_check_skip_reason(t, cmap, now) for t in own]
    checked = [t for t, r in zip(own, reasons) if r is None]
    if own and not checked and "off_season" in reasons:
        return None  # 季節性收集器、現在不在季內
    interval = int(interval_min or 0)
    if checked:
        interval = max(interval, min(int(t.get("expected_interval_min") or 0) for t in checked))
    return {"name": name, "tables": [_key(t) for t in checked],
            "threshold_min": alerting.staleness_threshold_min(interval)}


def evaluate(plan: dict, last_write: Optional[datetime], last_success: Optional[datetime],
             started_at: datetime, now: datetime) -> dict:
    """一支收集器的判斷結果（給 alerting.update_staleness）。"""
    threshold = timedelta(minutes=plan["threshold_min"])
    if plan["tables"] and last_write is not None:
        basis, last = "write", _aware(last_write)
    else:
        basis, last = "success", _aware(last_success)
    ref = last or started_at
    return {"name": plan["name"], "basis": basis, "last_at": last,
            "threshold_min": plan["threshold_min"], "stale": now - ref > threshold,
            "last_write": _aware(last_write), "last_success": _aware(last_success)}


class StalenessWatch:
    """main.py 每 STALE_CHECK_INTERVAL_MIN 分鐘呼叫 run()。"""

    def __init__(self, collectors: list, extra_names: Iterable[str] = (),
                 now: Optional[datetime] = None):
        self.collectors = collectors
        self.extra_names = list(extra_names)  # 長駐 worker（aisstream、ripe_ris_live）
        self.started_at = alerting._now(now)

    def _targets(self, cmap: dict) -> dict[str, int]:
        out = {c.name: int(c.interval_minutes or 0) for c in self.collectors}
        for name in self.extra_names:
            cfg = cmap.get(name) if isinstance(cmap.get(name), dict) else {}
            out.setdefault(name, int(cfg.get("expected_interval_min") or 0))
        return out

    def _db_down(self, db: dict, now: datetime) -> bool:
        """DB_DOWN_MIN 分鐘內沒有任何一次寫入成功、且期間有失敗。"""
        window = timedelta(minutes=DB_DOWN_MIN)
        fail, ok = _aware(db.get("fail")), _aware(db.get("ok"))
        if fail is None or now - fail > window:
            return False
        return now - (ok or self.started_at) >= window

    def run(self, now: Optional[datetime] = None) -> list[str]:
        """一輪檢查（純記憶體＋yaml）。回傳送出的訊息。"""
        now = alerting._now(now)
        activity, db = alerting.activity_snapshot()
        if self._db_down(db, now):
            mins = int((now - (_aware(db.get("ok")) or self.started_at)).total_seconds() // 60)
            alerting.raise_alert(
                "db_down", alerting.RED,
                f"資料庫連不上：收集器寫入 Supabase 已 {mins} 分鐘沒有一次成功"
                f"（期間失敗 {db.get('fails_since_ok', 0)} 次，資料先暫存 buffer）", now=now)
            return []  # DB 斷線期間不判斷個別收集器停更
        if db.get("ok") is not None:
            alerting.clear_alert("db_down", "資料庫寫入", now=now)

        tables = monitoring.load_realtime_tables()
        cmap = monitoring.load_cross_layer_map()
        saved = alerting.persisted_activity()
        findings = []
        for name, interval in self._targets(cmap).items():
            plan = plan_collector(name, interval, tables, cmap, now)
            if plan is None:
                continue
            mem = activity.get(name, {})
            findings.append(evaluate(
                plan,
                _latest(mem.get("write"), saved["last_write"].get(name)),
                _latest(mem.get("success"), saved["last_success"].get(name)),
                self.started_at, now))
        return alerting.update_staleness(findings, now=now)
