#!/usr/bin/env python3
"""VM 單檔 collector 共用的執行收據（gis-platform migration 558 的 public.report_collector_run）。

主容器走 storage/supabase_writer._report_heartbeat；VM 腳本目前各自呼叫舊的 report_collector_heartbeat，
mig 558 起它是 report_collector_run 的相容包裝（新增列數記未知），所以不改也能用。
要回報「實際新增列數／上游資料時間／來源日期／解析結果」的 VM 腳本改呼叫本模組的 report_run()：

    from run_receipt import report_run
    report_run(conn, COLLECTOR_NAME, True, records=len(rows),
               run={"new_rows": inserted, "rows_basis": "dedup", "upstream_max_at": max_obs_iso})

- 先呼叫 report_collector_run；函式不存在（migration 未套用）或失敗 → 同一輪退回舊 report_collector_heartbeat。
- run 只放已知的值；沒填＝未知（DB 記 NULL），不要用 0 代替。值的格式由 DB 端再驗一次，髒值當成沒填。
- 連線由呼叫端給、本模組不關；每次呼叫各自 commit／rollback。兩條都失敗時 raise，交給呼叫端吞（best-effort）。

零第三方依賴（不 import psycopg2），部署時跟 collector 單檔一起 scp 到同目錄即可 import。
本輪（2026-11-18）只加共用函式，VM 腳本逐支改用另案。
"""
from __future__ import annotations

import json
import logging
from typing import Optional

log = logging.getLogger(__name__)

RUN_SQL = "SELECT report_collector_run(%s, %s::jsonb)"
HEARTBEAT_SQL = "SELECT report_collector_heartbeat(%s, %s, %s, %s)"
_KEYS = ("new_rows", "rows_basis", "parse_ok", "source_date", "upstream_max_at",
         "expected_interval_min", "freshness_kind")


def build_payload(success: bool, records: int = 0, error: Optional[str] = None,
                  run: Optional[dict] = None) -> dict:
    payload = {"success": bool(success), "records": max(int(records or 0), 0),
               "error": (error or None) and str(error)[:500]}
    for key in _KEYS:
        value = (run or {}).get(key)
        if value is None or isinstance(value, float):
            continue
        if key == "new_rows" and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            continue
        payload[key] = value.isoformat() if hasattr(value, "isoformat") else value
    return payload


def _rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001
        pass


def report_run(conn, name: str, success: bool, records: int = 0, error: Optional[str] = None,
               run: Optional[dict] = None) -> str:
    """回傳實際走的路徑：'run'（新函式）或 'heartbeat'（退回舊心跳）。兩條都失敗時 raise。"""
    payload = build_payload(success, records, error, run)
    try:
        with conn.cursor() as cur:
            cur.execute(RUN_SQL, (name, json.dumps(payload, ensure_ascii=False, default=str)))
        conn.commit()
        return "run"
    except Exception as exc:  # noqa: BLE001
        _rollback(conn)
        if getattr(exc, "pgcode", None) == "42883":
            log.info("report_collector_run 不存在（gis-platform mig 558 未套用），改用舊心跳")
        else:
            log.warning(f"report_collector_run 失敗，改用舊心跳: {exc}")
    with conn.cursor() as cur:
        cur.execute(HEARTBEAT_SQL, (name, payload["success"], payload["records"], payload["error"]))
    conn.commit()
    return "heartbeat"
