"""監控用 helper：跨層健康檢查的共用邏輯。

被 tasks/daily_report.py 各 _section_* 引用，避免邏輯散在報告函式裡。

提供：
- load_cross_layer_map() / load_realtime_tables()  — 讀 yaml 真相來源
- query_realtime_health()                          — 呼叫 SB RPC 拿 50 表新鮮度
- list_archive_dates_per_collector()               — 掃 S3 archives 拿每 collector 最新日期
- list_vm_health_snapshots()                       — 撈 _external_vm_health/ 下的 host JSON
- classify_freshness()                             — STALE / OK / DEAD 判斷
"""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

import config

TAIPEI_TZ = timezone(timedelta(hours=8))
log = logging.getLogger(__name__)

# Yaml 檔位置（repo root 相對）
_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
_CROSS_LAYER_YAML = _CONFIG_DIR / "cross_layer_map.yaml"
_REALTIME_TABLES_YAML = _CONFIG_DIR / "realtime_tables.yaml"


# ────────────────────────────────────────────────────────────────────
# YAML 載入（每次重讀，量小無快取必要）
# ────────────────────────────────────────────────────────────────────
def load_cross_layer_map() -> dict[str, dict]:
    """回傳 {collector_name: {enabled, deployment, expected_interval_min, ...}}"""
    if not _CROSS_LAYER_YAML.exists():
        log.warning(f"cross_layer_map.yaml 不存在: {_CROSS_LAYER_YAML}")
        return {}
    with open(_CROSS_LAYER_YAML, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_realtime_tables() -> list[dict]:
    """回傳 [{schema, table, time_column, owner_collector, expected_interval_min, critical, ...}, ...]"""
    if not _REALTIME_TABLES_YAML.exists():
        log.warning(f"realtime_tables.yaml 不存在: {_REALTIME_TABLES_YAML}")
        return []
    with open(_REALTIME_TABLES_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("tables", [])


# ────────────────────────────────────────────────────────────────────
# Supabase realtime health
# ────────────────────────────────────────────────────────────────────
def query_realtime_health(tables: list[dict]) -> list[dict]:
    """call realtime.health_snapshot RPC 拿每張表的 max_time + count_24h。

    Returns: [{schema, table, max_time, count_24h, error}, ...]
    """
    if not (config.SUPABASE_ENABLED and config.SUPABASE_DB_URL and tables):
        return []
    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 not available — Supabase health 跳過")
        return []

    payload = json.dumps([
        {"schema": t["schema"], "table": t["table"], "time_column": t.get("time_column", "collected_at")}
        for t in tables
    ])

    results = []
    try:
        with psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM realtime.health_snapshot(%s::jsonb)", (payload,))
                for schema_n, table_n, max_time, count_24h, err in cur.fetchall():
                    results.append({
                        "schema": schema_n,
                        "table": table_n,
                        "max_time": max_time,
                        "count_24h": count_24h or 0,
                        "error": err,
                    })
    except Exception as exc:
        log.error(f"query_realtime_health 失敗: {exc}")
        return []
    return results


def classify_freshness(
    max_time: datetime | None,
    expected_interval_min: int,
    now: datetime | None = None,
) -> tuple[str, int | None]:
    """依 max_time + expected_interval_min 判斷 OK / STALE / DEAD。

    Returns: (status, age_minutes)
        status: 'OK' | 'STALE' | 'DEAD' | 'NEVER'
        age_minutes: 寫入距今分鐘數，無資料則 None
    """
    if max_time is None:
        return "NEVER", None
    now = now or datetime.now(TAIPEI_TZ)
    if max_time.tzinfo is None:
        max_time = max_time.replace(tzinfo=TAIPEI_TZ)
    age_min = int((now - max_time).total_seconds() / 60)
    threshold_stale = expected_interval_min * 3      # 期望 3x 內為 STALE
    threshold_dead  = expected_interval_min * 12     # 期望 12x 以上為 DEAD（半天沒寫）
    if age_min < threshold_stale:
        return "OK", age_min
    if age_min < threshold_dead:
        return "STALE", age_min
    return "DEAD", age_min


# ────────────────────────────────────────────────────────────────────
# S3 archive 健康
# ────────────────────────────────────────────────────────────────────
def list_archive_dates_per_collector(prefix_filter: str | None = None) -> dict[str, str]:
    """掃 S3 archives 拿每個 collector 的最新歸檔日期。

    Returns: {collector_name: 'YYYY-MM-DD'}
    """
    if not config.S3_BUCKET:
        return {}
    try:
        from storage.s3 import S3Storage
        s3 = S3Storage()
    except Exception as exc:
        log.warning(f"S3Storage 初始化失敗: {exc}")
        return {}

    result: dict[str, str] = {}
    try:
        # 掃 root 下所有 collector_name/archives/ 結構
        paginator = s3.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=""):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "/archives/" not in key:
                    continue
                if not key.endswith(".tar.gz"):
                    continue
                collector_name = key.split("/archives/")[0]
                # 日期格式為 YYYY-MM-DD.tar.gz
                fname = key.rsplit("/", 1)[-1]
                date_part = fname.replace(".tar.gz", "")
                if len(date_part) != 10 or date_part.count("-") != 2:
                    continue
                if prefix_filter and not collector_name.startswith(prefix_filter):
                    continue
                # 取最大日期（字典序 YYYY-MM-DD 等同時序）
                if collector_name not in result or date_part > result[collector_name]:
                    result[collector_name] = date_part
    except Exception as exc:
        log.error(f"list_archive_dates_per_collector 失敗: {exc}")
    return result


# ────────────────────────────────────────────────────────────────────
# External VM health snapshots
# ────────────────────────────────────────────────────────────────────
_VM_HEALTH_PREFIX = "_external_vm_health/"


def list_vm_health_snapshots(max_age_hours: int = 30) -> list[dict]:
    """掃 s3://.../{_VM_HEALTH_PREFIX}/{host}/YYYY-MM-DD.json，取每 host 最新一筆。

    Returns: [{host, snapshot_date, snapshot, age_hours, is_lost}, ...]
    """
    if not config.S3_BUCKET:
        return []
    try:
        from storage.s3 import S3Storage
        s3 = S3Storage()
    except Exception as exc:
        log.warning(f"S3Storage 初始化失敗: {exc}")
        return []

    # 先列出所有 host 下的 key
    latest_per_host: dict[str, str] = {}
    try:
        paginator = s3.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=_VM_HEALTH_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                rest = key[len(_VM_HEALTH_PREFIX):]
                if "/" not in rest or not rest.endswith(".json"):
                    continue
                host = rest.split("/", 1)[0]
                if host not in latest_per_host or key > latest_per_host[host]:
                    latest_per_host[host] = key
    except Exception as exc:
        log.error(f"list_vm_health_snapshots 列表失敗: {exc}")
        return []

    now = datetime.now(TAIPEI_TZ)
    results = []
    for host, key in latest_per_host.items():
        try:
            obj = s3.s3.get_object(Bucket=config.S3_BUCKET, Key=key)
            snapshot = json.load(io.BytesIO(obj["Body"].read()))
            gen_at_str = snapshot.get("generated_at", "")
            try:
                gen_at = datetime.fromisoformat(gen_at_str)
                if gen_at.tzinfo is None:
                    gen_at = gen_at.replace(tzinfo=TAIPEI_TZ)
                age_hours = (now - gen_at).total_seconds() / 3600
            except Exception:
                age_hours = 999
            snapshot_date = key.rsplit("/", 1)[-1].replace(".json", "")
            results.append({
                "host": host,
                "snapshot_date": snapshot_date,
                "snapshot": snapshot,
                "age_hours": age_hours,
                "is_lost": age_hours > max_age_hours,
            })
        except Exception as exc:
            log.warning(f"VM health snapshot 讀取失敗 {key}: {exc}")
            results.append({
                "host": host, "snapshot_date": None,
                "snapshot": None, "age_hours": 999, "is_lost": True,
            })
    return results


# ────────────────────────────────────────────────────────────────────
# 異常狀態持久化（給 _section_anomaly_trend 用）
# ────────────────────────────────────────────────────────────────────
def _anomaly_state_path() -> Path:
    return Path(getattr(config, "LOCAL_DATA_DIR", Path("./data"))) / "anomaly_state.json"


def load_anomaly_state() -> dict[str, dict]:
    """讀 anomaly_state.json — 結構 {anomaly_id: {first_seen, last_seen, notify_count}}"""
    p = _anomaly_state_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning(f"load_anomaly_state 失敗: {exc}")
        return {}


def save_anomaly_state(state: dict[str, dict]) -> None:
    p = _anomaly_state_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception as exc:
        log.warning(f"save_anomaly_state 失敗: {exc}")


def update_anomaly_state(current_anomalies: set[str]) -> tuple[set[str], set[str], set[str]]:
    """根據本輪偵測到的異常 id 集合，更新 state 並回傳 (new_ones, persistent_ones, resolved_ones)。

    去重規則：persistent 只在 D1/D3/D7 提報，其他天回傳但不計入 'should_notify'。
    判斷邏輯外部負責，本函式只回傳狀態分類。
    """
    state = load_anomaly_state()
    today_str = datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d")

    new_ones: set[str] = set()
    persistent_ones: set[str] = set()
    resolved_ones: set[str] = set()

    for aid in current_anomalies:
        if aid in state:
            state[aid]["last_seen"] = today_str
            state[aid]["notify_count"] = state[aid].get("notify_count", 1) + 1
            persistent_ones.add(aid)
        else:
            state[aid] = {"first_seen": today_str, "last_seen": today_str, "notify_count": 1}
            new_ones.add(aid)

    for aid in list(state.keys()):
        if aid not in current_anomalies:
            resolved_ones.add(aid)
            del state[aid]

    save_anomaly_state(state)
    return new_ones, persistent_ones, resolved_ones


def query_backup_health() -> dict | None:
    """撈 metadata.backup_audit_log + backup_state 的健康度摘要。

    Returns:
        dict {
          'severity_24h': {ok: N, info: N, warn: N, critical: N},
          'top_critical': [(schema_name, table_name, code, message)] up to 5,
          'static_tracked': N,        # backup_state 總筆數
          'static_total_kb': N,
          'last_run_per_kind': {static_snapshot: ts, realtime_snapshot: ts, reconcile: ts},
          'oldest_snapshot_age_days': N,  # static
        }
        None if DB unreachable.
    """
    if not (config.SUPABASE_ENABLED and config.SUPABASE_DB_URL):
        return None
    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 not available — backup health 跳過")
        return None

    try:
        with psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT severity, count(*)
                    FROM metadata.backup_audit_log
                    WHERE run_at > now() - interval '24 hours'
                    GROUP BY severity
                """)
                severity_24h = {row[0]: row[1] for row in cur.fetchall()}

                cur.execute("""
                    SELECT schema_name, table_name, code, left(coalesce(message,''), 80)
                    FROM metadata.backup_audit_log
                    WHERE run_at > now() - interval '24 hours'
                      AND severity = 'critical'
                    ORDER BY run_at DESC
                    LIMIT 5
                """)
                top_critical = cur.fetchall()

                cur.execute("""
                    SELECT count(*), coalesce(sum(last_size_bytes), 0) / 1024
                    FROM metadata.backup_state
                """)
                tracked, total_kb = cur.fetchone()

                cur.execute("""
                    SELECT run_kind, max(run_at)
                    FROM metadata.backup_audit_log
                    GROUP BY run_kind
                """)
                last_run = {row[0]: row[1] for row in cur.fetchall()}

                cur.execute("""
                    SELECT EXTRACT(EPOCH FROM (now() - min(last_backup_at))) / 86400
                    FROM metadata.backup_state
                """)
                row = cur.fetchone()
                oldest_days = int(row[0]) if row and row[0] is not None else None

                return {
                    'severity_24h': severity_24h,
                    'top_critical': top_critical,
                    'static_tracked': tracked or 0,
                    'static_total_kb': int(total_kb or 0),
                    'last_run_per_kind': last_run,
                    'oldest_snapshot_age_days': oldest_days,
                }
    except Exception as exc:
        log.warning("query_backup_health failed: %s", exc)
        return None


def query_retention_coverage() -> dict:
    """呼叫 metadata.check_retention_coverage() 列出缺 retention 覆蓋的表。

    DB 函數由 gis-platform migration 部署，contract：
        RETURNS TABLE(table_name text, issue text)

    Returns:
        {'status': 'ok', 'rows': [(table_name, issue), ...]}  # rows 空 = 全覆蓋
        {'status': 'not_deployed'}                            # DB 函數尚未部署
        {'status': 'error', 'message': str}                   # 連線失敗 / 其他錯誤
    """
    if not (config.SUPABASE_ENABLED and config.SUPABASE_DB_URL):
        return {'status': 'error', 'message': 'Supabase 未啟用'}
    try:
        import psycopg2
        import psycopg2.errors
    except ImportError:
        return {'status': 'error', 'message': 'psycopg2 not available'}

    try:
        with psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT table_name, issue FROM metadata.check_retention_coverage()")
                rows = cur.fetchall()
        return {'status': 'ok', 'rows': rows}
    except (psycopg2.errors.UndefinedFunction, psycopg2.errors.InvalidSchemaName):
        return {'status': 'not_deployed'}
    except Exception as exc:
        log.warning("query_retention_coverage failed: %s", exc)
        return {'status': 'error', 'message': str(exc)}


def should_notify_persistent(anomaly_id: str, state: dict[str, dict] | None = None) -> bool:
    """D1 / D3 / D7 規則：first_seen 算 D0，第 1, 3, 7 天才提報"""
    state = state if state is not None else load_anomaly_state()
    entry = state.get(anomaly_id)
    if not entry:
        return True  # 新異常一定報
    try:
        first_seen = datetime.strptime(entry["first_seen"], "%Y-%m-%d").replace(tzinfo=TAIPEI_TZ)
    except Exception:
        return True
    days_since = (datetime.now(TAIPEI_TZ) - first_seen).days
    return days_since in (1, 3, 7)
