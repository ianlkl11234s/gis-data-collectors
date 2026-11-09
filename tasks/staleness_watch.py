"""收集器「太久沒新資料」即時告警（🔴 疑似壞掉／✅ 恢復）。

使用者決定（2026-11-09）：單次或連續幾次失敗不發，只有資料真的斷了才發。
規則正本 docs/handbook/operations.md §1；門檻 utils/alerting.staleness_threshold_min()。

每 STALE_CHECK_INTERVAL_MIN 分鐘（main.py 排程）跑一次：
1. 檢查對象＝這個行程實際啟動的收集器（＝部署在本機）＋長駐 worker，
   cross_layer_map 標停用（enabled:false／deployment:disabled）的不檢查。
2. 「新資料」＝該收集器寫的 realtime_tables（owner_collector 或 cross_layer_map supabase_tables）
   裡、不被 monitoring.stale_check_skip_reason 排除的表的最新時間；**任一張表有新資料就算正常**。
   門檻間隔取 max(收集器執行間隔, 這些表裡最小的 expected_interval_min)。
3. 它的表全是事件驅動／停用表、沒登記任何表、或表都還沒資料 → 改用「最後成功執行時間」
   （行程內 last_success_at、上一輪存進 alert_state 的值、metadata.collector_status.last_success_at
   三者取最新；DB 那份只在有寫入時更新，沒新事件的收集器會很舊，所以每輪把行程內的值存檔，
   重啟後不會誤報。都沒有就從行程啟動算）。表全在季節外（active_window）→ 不檢查。
4. 為了少打 DB（health_snapshot 全表一次約 12 秒），每張表記住上次看到的最新時間，
   只有「可能已經超過門檻」的表才重查。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable, Optional

import config
from tasks import monitoring
from utils import alerting

log = logging.getLogger(__name__)

TableKey = tuple[str, str]


def _key(t: dict) -> TableKey:
    return (t["schema"], t["table"])


def _aware(ts: Optional[datetime]) -> Optional[datetime]:
    return alerting._now(ts) if ts is not None else None


def plan_collector(name: str, interval_min: int, tables: list[dict], cmap: dict,
                   now: datetime) -> Optional[dict]:
    """決定這支收集器怎麼檢查；不檢查（停用、季節外）回 None。

    回 {name, tables: [TableKey], threshold_min}；tables 空＝改用最後成功執行時間。
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


def evaluate(plan: dict, table_max: dict[TableKey, Optional[datetime]],
             last_success: Optional[datetime], started_at: datetime, now: datetime) -> dict:
    """一支收集器的判斷結果（給 alerting.update_staleness）。"""
    threshold = timedelta(minutes=plan["threshold_min"])
    latest = max((_aware(table_max[k]) for k in plan["tables"] if table_max.get(k) is not None),
                 default=None)
    if latest is not None:
        return {"name": plan["name"], "basis": "table", "last_at": latest,
                "threshold_min": plan["threshold_min"], "stale": now - latest > threshold}
    last = _aware(last_success)
    ref = last or started_at
    return {"name": plan["name"], "basis": "success", "last_at": last,
            "threshold_min": plan["threshold_min"], "stale": now - ref > threshold}


class StalenessWatch:
    """main.py 每 STALE_CHECK_INTERVAL_MIN 分鐘呼叫 run()。"""

    def __init__(self, collectors: list, extra_names: Iterable[str] = (),
                 now: Optional[datetime] = None):
        self.collectors = collectors
        self.extra_names = list(extra_names)  # 長駐 worker（aisstream、ripe_ris_live）
        self.started_at = alerting._now(now)
        self._known: dict[TableKey, Optional[datetime]] = {}

    def _targets(self, cmap: dict) -> dict[str, tuple[int, Optional[datetime]]]:
        out = {c.name: (int(c.interval_minutes or 0), getattr(c, "last_success_at", None))
               for c in self.collectors}
        for name in self.extra_names:
            cfg = cmap.get(name) if isinstance(cmap.get(name), dict) else {}
            out.setdefault(name, (int(cfg.get("expected_interval_min") or 0), None))
        return out

    def run(self, now: Optional[datetime] = None) -> Optional[list[str]]:
        """一輪檢查。回傳送出的訊息；DB 查不到而跳過這輪時回 None。"""
        now = alerting._now(now)
        tables = monitoring.load_realtime_tables()
        cmap = monitoring.load_cross_layer_map()
        targets = self._targets(cmap)
        plans = [p for p in (plan_collector(n, iv, tables, cmap, now) for n, (iv, _) in targets.items())
                 if p is not None]

        # 只重查「可能已超過門檻」的表：上次看到的最新時間 + 門檻 − 一個檢查間隔 已經到了
        margin = timedelta(minutes=config.STALE_CHECK_INTERVAL_MIN)
        due: dict[TableKey, dict] = {}
        meta = {_key(t): t for t in tables}
        for p in plans:
            for k in p["tables"]:
                known = self._known.get(k)
                if known is None or now - _aware(known) >= timedelta(minutes=p["threshold_min"]) - margin:
                    due[k] = meta[k]
        if due and config.SUPABASE_ENABLED and config.SUPABASE_DB_URL:
            rows = monitoring.query_realtime_health(list(due.values()))
            if not rows:
                log.warning("staleness_watch：新鮮度查詢失敗，這輪不判斷")
                return None
            for r in rows:
                if not r.get("error"):
                    self._known[(r["schema"], r["table"])] = r.get("max_time")

        db_success = {}
        if any(not p["tables"] or all(self._known.get(k) is None for k in p["tables"]) for p in plans):
            db_success = monitoring.query_collector_last_success()
        saved = alerting.persisted_last_success()
        findings = []
        for p in plans:
            name = p["name"]
            candidates = (_aware(targets[name][1]), saved.get(name), _aware(db_success.get(name)))
            last = max((x for x in candidates if x is not None), default=None)
            f = evaluate(p, self._known, last, self.started_at, now)
            f["last_success"] = last
            findings.append(f)
        return alerting.update_staleness(findings, now=now)
