"""每週 DB 效能體檢（2026-11-09 減量大掃除後設立；鐵則正本 mini-taiwan-pulse docs/supabase-optimization.md §0）。

週一 07:30（台北）跑一次，結果寫 LOCAL_DATA_DIR/db_efficiency_audit.json；同一天 08:00 的日報讀檔，
有發現才併進「🔧 現在還壞的」（🟡），全綠不出現。列三種漏網之魚：

「讀者」角色＝anon、authenticated（前端）與 cloud_agent_ro（分析倉庫／MCP）。

1. 前端 RPC 太慢（R1）：讀者角色呼叫的 RPC，這段期間平均 >1 秒。
   被 statement_timeout 取消的呼叫不進 pg_stat_statements，所以平均是下限。
2. 預算表沒人讀（R2）：pg_cron 的 refresh 函式寫入的表（從 cron.job 的 command 找 refresh_*，
   再從函式本體找 INSERT／UPDATE 目標），
   a. 沒有任何讀者角色可呼叫的函式讀它、讀者角色也不能直接 SELECT → 「沒有讀者」；
   b. 有讀者函式，但這段期間讀者角色呼叫 0 次 → 「一週沒人讀」。
   能直接 SELECT 的表只看授權、不計次數（分辨不出 refresh 自己的讀取）。
   （不用 pg_stat_user_tables 的 seq_scan＋idx_scan：refresh 自己的 DELETE／UPDATE 也會累加，
   2026-11-09 實測沒讀者的 youbike_h3_daily idx_scan 仍有 231，訊號被蓋掉。）
3. 單一 SQL 每天磁碟讀 >1 GB（shared_blks_read 差值 ÷ 天數）。

自己也要很輕（R4）：只查統計檢視與系統目錄，唯讀交易、statement_timeout 15s，絕不掃資料表。
- 數字一律用 pg_stat_statements(false)（不載查詢文字，實測 ~7 ms、零 temp）。
- 查詢文字只在「有超標或沒見過的 RPC queryid」時才取一次（pg_stat_statements(true) 會把整份文字
  物化到 temp：2026-11-09 文字 233 MB 時實測 4.5 秒、216 MB temp），解析出的 RPC 名存進狀態檔，
  同一 queryid 之後不再取。收集器批次 SQL 改固定形狀（R5）後文字會小很多。
- 差值基準：狀態檔裡上次的累計值（不建新表）。stats_reset 變了就作廢重來；queryid 被 dealloc
  擠掉又出現（累計變小）就當「自重新出現起」。第一次執行以 stats_reset 起算。
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import config

log = logging.getLogger(__name__)

TAIPEI_TZ = timezone(timedelta(hours=8))
READER_ROLES = ("anon", "authenticated", "cloud_agent_ro")  # 前端＋分析倉庫／MCP 唯讀角色
APP_SCHEMAS = ("public", "live", "analytics", "spatial", "metadata", "reference", "intel")
SLOW_RPC_MS = 1000.0
DAILY_READ_BYTES = 1024 ** 3
BLOCK = 8192
STATEMENT_TIMEOUT = "15s"
MAX_LINE_ITEMS = 3

_WRITE_TARGET = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(?:\s+TABLE)?)\s+(?:ONLY\s+)?"
    r"\"?(\w+)\"?\.\"?(\w+)\"?", re.I)
_REFRESH_CALL = re.compile(r"\b(?:\"?(\w+)\"?\.)?\"?(refresh_\w+)\"?\s*\(", re.I)
_RPC_IN_TEXT = (
    re.compile(r"\"(\w+)\"\.\"(\w+)\"\s*\("),  # PostgREST：FROM "public"."get_x"(...)
    re.compile(r"\b(public|live|analytics|spatial|metadata|reference|intel)\.(\w+)\s*\(", re.I),
)
_STATE_TABLE = re.compile(r"_(watermark|state|log|runs?|queue|lock)$", re.I)  # refresh 自己的水位／狀態表，不是預算表
_INTERNAL_PREFIX = ("refresh_", "cleanup_", "manage_", "sync_", "compute_", "check_")  # 維運函式，不算讀者


def state_path() -> Path:
    return Path(getattr(config, "LOCAL_DATA_DIR", Path("./data"))) / "db_efficiency_audit.json"


def load_state() -> dict:
    p = state_path()
    try:
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception as exc:  # 壞檔就重來，不擋日報
        log.warning("db_efficiency_audit 狀態檔讀取失敗，重新開始: %s", exc)
    return {}


def save_state(state: dict) -> None:
    p = state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, p)


# ── DB（只讀統計檢視與系統目錄）─────────────────────────────────────────────────

def _connect():
    import psycopg2
    conn = psycopg2.connect(config.SUPABASE_DB_URL, connect_timeout=15)
    conn.set_session(readonly=True, autocommit=False)
    cur = conn.cursor()
    cur.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    return conn, cur


def fetch_numbers(cur) -> dict:
    """pg_stat_statements 數字（不含文字）＋ refresh 目標表與讀者函式（系統目錄）。"""
    cur.execute("SELECT stats_reset, dealloc FROM pg_stat_statements_info")
    stats_reset, dealloc = cur.fetchone()
    cur.execute("SELECT oid::bigint, rolname FROM pg_roles WHERE rolname = ANY(%s)", (list(READER_ROLES),))
    reader_oids = {int(o): n for o, n in cur.fetchall()}
    cur.execute("""
        SELECT userid::bigint, queryid, calls, total_exec_time, shared_blks_read
        FROM pg_stat_statements(false)
        WHERE calls > 0 AND queryid IS NOT NULL
    """)
    stmts = {f"{u}:{q}": [int(c), float(t), int(b)] for u, q, c, t, b in cur.fetchall()}

    cur.execute("SELECT jobname, schedule, command FROM cron.job WHERE active LIMIT 1000")
    jobs = [{"jobname": j, "schedule": s, "command": c} for j, s, c in cur.fetchall()]

    # 應用 schema 的函式（名稱、本體、前端能不能呼叫）；只讀 pg_proc
    roles = list(reader_oids)
    cur.execute("""
        SELECT n.nspname, p.proname, p.prosrc,
               coalesce(bool_or(has_function_privilege(r.oid, p.oid, 'EXECUTE')), false) AS frontend
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        LEFT JOIN pg_roles r ON r.oid = ANY(%s::oid[])
        WHERE n.nspname = ANY(%s) AND p.prokind IN ('f', 'p')
        GROUP BY n.nspname, p.proname, p.prosrc
        LIMIT 5000
    """, (roles, list(APP_SCHEMAS)))
    funcs = [{"schema": s, "name": n, "src": src or "", "frontend": bool(fe)} for s, n, src, fe in cur.fetchall()]

    cur.execute("""
        SELECT n.nspname || '.' || c.relname,
               coalesce(bool_or(has_table_privilege(r.oid, c.oid, 'SELECT')), false)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_roles r ON r.oid = ANY(%s::oid[])
        WHERE c.relkind IN ('r', 'p', 'm') AND n.nspname = ANY(%s)
        GROUP BY 1
        LIMIT 20000
    """, (roles, list(APP_SCHEMAS)))
    tables = {t: bool(sel) for t, sel in cur.fetchall()}
    return {"stats_reset": stats_reset, "dealloc": int(dealloc or 0), "reader_oids": reader_oids,
            "stmts": stmts, "jobs": jobs, "funcs": funcs, "tables": tables}


def fetch_texts(cur, queryids: list[int]) -> dict[str, str]:
    """只在需要時呼叫：pg_stat_statements(true) 會把整份查詢文字物化到 temp。"""
    if not queryids:
        return {}
    cur.execute("""
        SELECT userid::bigint, queryid, left(query, 400)
        FROM pg_stat_statements(true) WHERE queryid = ANY(%s) LIMIT 2000
    """, (queryids,))
    return {f"{u}:{q}": t for u, q, t in cur.fetchall()}


# ── 純函式（可測）─────────────────────────────────────────────────────────────

_INSERT_INTO = re.compile(r"^\s*(?:INSERT\s+INTO|COPY)\s+\"?(\w+)\"?\.\"?(\w+)\"?", re.I)


def name_from_text(text: str) -> str:
    """查詢文字 → RPC／函式名（收集器寫入回 "INSERT schema.table"）；認不出來回空字串。"""
    m = _INSERT_INTO.match(text or "")
    if m:
        return f"INSERT {m.group(1).lower()}.{m.group(2).lower()}"
    for rx in _RPC_IN_TEXT:
        for schema, fn in rx.findall(text or ""):
            if schema.lower() in APP_SCHEMAS and not fn.startswith("pg_"):
                return fn
    return ""


def short_sql(text: str, limit: int = 60) -> str:
    s = " ".join((text or "").split())
    return s if len(s) <= limit else s[:limit - 1] + "…"


def diff_stmts(prev: Optional[dict], cur: dict) -> dict[str, list]:
    """累計值差值。prev 沒有、或累計變小（被 dealloc 擠掉後重新出現）→ 用目前累計值。"""
    out = {}
    for key, (c, t, b) in cur.items():
        p = (prev or {}).get(key)
        if p and c >= p[0]:
            dc, dt, db = c - p[0], t - p[1], b - p[2]
        else:
            dc, dt, db = c, t, b
        if dc > 0:
            out[key] = [dc, max(dt, 0.0), max(db, 0)]
    return out


def refresh_targets(jobs: list[dict], funcs: list[dict]) -> dict[str, dict]:
    """pg_cron refresh 函式寫入的表 → {table: {jobs, functions}}。"""
    by_name = {}
    for f in funcs:
        by_name.setdefault(f["name"].lower(), []).append(f)
    targets: dict[str, dict] = {}
    for job in jobs:
        for _, fn in _REFRESH_CALL.findall(job.get("command") or ""):
            for f in by_name.get(fn.lower(), []):
                for schema, table in _WRITE_TARGET.findall(f["src"]):
                    if schema.lower() in ("pg_temp", "cron") or table.lower().startswith("tmp") \
                            or _STATE_TABLE.search(table):
                        continue
                    key = f"{schema.lower()}.{table.lower()}"
                    t = targets.setdefault(key, {"jobs": [], "functions": [], "schedules": []})
                    if job["jobname"] not in t["jobs"]:
                        t["jobs"].append(job["jobname"])
                        t["schedules"].append(job.get("schedule") or "")
                    if f["name"] not in t["functions"]:
                        t["functions"].append(f["name"])
    return targets


def _mentions(src: str, table: str) -> bool:
    schema, name = table.split(".", 1)
    return re.search(rf"(?<![\w.])(?:\"?{re.escape(schema)}\"?\.)?\"?{re.escape(name)}\"?(?!\w)", src, re.I) is not None


def readers_of(table: str, funcs: list[dict], writers: set[str]) -> list[str]:
    """前端可呼叫、會讀這張表的函式（含經一層內部函式轉手）。refresh／cleanup 等維運函式不算。"""
    def is_internal(f):
        return f["name"].lower() in writers or f["name"].lower().startswith(_INTERNAL_PREFIX)

    direct = [f for f in funcs if not is_internal(f) and _mentions(f["src"], table)]
    readers = {f["name"] for f in direct if f["frontend"]}
    middles = [f["name"] for f in direct if not f["frontend"]]
    for mid in middles:
        rx = re.compile(rf"(?<!\w){re.escape(mid)}\s*\(", re.I)
        readers |= {f["name"] for f in funcs if f["frontend"] and not is_internal(f) and rx.search(f["src"])}
    return sorted(readers)


def _labels() -> dict[str, str]:
    try:
        from tasks import monitoring
        return {f"{t['schema']}.{t['table']}": t["label"] for t in monitoring.load_realtime_tables()
                if t.get("label")}
    except Exception as exc:
        log.warning("讀 realtime_tables label 失敗: %s", exc)
        return {}


_GENERIC_TOKENS = {"get", "refresh", "daily", "day", "hourly", "latest", "incremental", "current", "full",
                   "public", "live", "analytics", "v2", "24h", "7d", "14d", "total", "batch", "agg", "preagg"}


def _tokens(name: str) -> set[str]:
    out = set()
    for t in re.split(r"[_.]+", name.lower()):
        if t and t not in _GENERIC_TOKENS and not t.isdigit():
            out.add(t[:-1] if t.endswith("s") and len(t) > 3 else t)
    return out


def best_label(name: str, src: str, labels: dict) -> str:
    """函式／表的中文名：在本體有提到的已登記表裡，挑跟名稱共用字最多的那張的 label；沒共用字就不借。"""
    mine = _tokens(name)
    best, score = "", 0
    for raw, label in labels.items():
        if not _mentions(src, raw):
            continue
        sc = len(mine & _tokens(raw))
        if sc > score:
            best, score = label, sc
    return best


def display_table(table: str, targets: dict, funcs_by_name: dict, labels: dict) -> str:
    """表的顯示名：「中文 label（表名）」；預算表沒 label 就借它 refresh 函式讀的原始表 label。"""
    code = table.split(".", 1)[1] if table.startswith("live.") else table
    if table in labels:
        return f"{labels[table]}（{code}）"
    for fn in targets.get(table, {}).get("functions", []):
        for f in funcs_by_name.get(fn.lower(), []):
            label = best_label(table, f["src"], labels)
            if label:
                return f"{label}預算表（{code}）"
    return code


def display_fn(fn: str, funcs_by_name: dict, labels: dict) -> str:
    if fn.startswith("INSERT "):
        table = fn[7:]
        label = labels.get(table)
        return f"收集器寫入 {label}（{table}）" if label else f"收集器寫入 {table}"
    for f in funcs_by_name.get(fn.lower(), []):
        label = best_label(fn, f["src"], labels)
        if label:
            return f"{label}（{fn}）"
    return fn


def _days_label(days: float) -> str:
    return f"{days:.0f} 天" if days >= 1.5 else f"{days * 24:.0f} 小時"


def evaluate(numbers: dict, delta: dict, names: dict, window_days: float, labels: dict,
             baseline: bool = False) -> dict:
    """三項判斷。回傳 {slow_rpcs, unread_tables, heavy_sql}，每項是已排序的 dict 清單。

    baseline=True（第一次、只有基準沒有差值）：只做不靠差值的「沒有讀者」。"""
    reader_oids = {str(o) for o in numbers["reader_oids"]}
    funcs = numbers["funcs"]
    funcs_by_name = {}
    for f in funcs:
        funcs_by_name.setdefault(f["name"].lower(), []).append(f)

    # 1. 前端 RPC 太慢（依 RPC 名彙總；認不出名的用 queryid）
    per_rpc: dict[str, list] = {}
    for key, (dc, dt, _) in delta.items():
        if key.split(":", 1)[0] not in reader_oids:
            continue
        nm = names.get(key) or names.get(f"text:{key}") or f"queryid {key.split(':', 1)[1]}"
        agg = per_rpc.setdefault(nm, [0, 0.0])
        agg[0] += dc
        agg[1] += dt
    slow = [{"name": n, "display": display_fn(n, funcs_by_name, labels), "calls": c, "mean_ms": t / c}
            for n, (c, t) in per_rpc.items() if c and t / c > SLOW_RPC_MS]
    slow.sort(key=lambda x: -x["mean_ms"] * x["calls"])

    # 2. 預算表沒人讀
    targets = refresh_targets(numbers["jobs"], funcs)
    writers = {fn.lower() for t in targets.values() for fn in t["functions"]}
    called = {n for n, (c, _) in per_rpc.items() if c > 0}
    unread = []
    for table, info in sorted(targets.items()):
        readers = readers_of(table, funcs, writers)
        direct = numbers["tables"].get(table, False)
        if not readers and not direct:
            reason = "沒有讀者（前端與分析倉庫都沒有函式讀它，也不能直接讀）"
        elif readers and not direct and not baseline and not (set(readers) & called):
            reason = f"讀者 {'、'.join(readers[:2])} {_days_label(window_days)}內 0 次呼叫"
        else:
            continue
        unread.append({"table": table, "display": display_table(table, targets, funcs_by_name, labels),
                       "reason": reason, "jobs": info["jobs"], "schedules": info["schedules"],
                       "readers": readers})

    # 3. 單一 SQL 每天磁碟讀 >1 GB
    heavy = []
    for key, (dc, _, db) in delta.items():
        if (names.get(key) or "").startswith("INSERT "):
            continue  # 收集器必要寫入（索引維護）；改善走 R5 固定形狀與多餘索引盤點，不每週重複報
        per_day = db * BLOCK / max(window_days, 1e-6)
        if per_day > DAILY_READ_BYTES:
            nm = names.get(key) or ""
            disp = display_fn(nm, funcs_by_name, labels) if nm else names.get(f"text:{key}", f"queryid {key.split(':', 1)[1]}")
            heavy.append({"key": key, "name": nm, "display": disp, "gb_per_day": per_day / DAILY_READ_BYTES,
                          "calls": dc})
    heavy.sort(key=lambda x: -x["gb_per_day"])
    return {"slow_rpcs": slow, "unread_tables": unread, "heavy_sql": heavy}


def queryids_needing_text(numbers: dict, delta: dict, names: dict, window_days: float) -> list[int]:
    """要取文字的 queryid：沒見過的前端 queryid（判斷讀者有沒有被呼叫要用）＋超過磁碟讀門檻的。"""
    reader_oids = {str(o) for o in numbers["reader_oids"]}
    need = set()
    for key, (_, _, db) in delta.items():
        if key in names:
            continue
        if key.split(":", 1)[0] in reader_oids or db * BLOCK / max(window_days, 1e-6) > DAILY_READ_BYTES:
            need.add(int(key.split(":", 1)[1]))
    return sorted(need)


def report_lines(findings: dict, window_days: float) -> list[str]:
    """日報用：每類最多一行、前 3 項；全綠回空清單。"""
    win = _days_label(window_days)
    lines = []

    def items(rows, fmt):
        shown = "；".join(fmt(r) for r in rows[:MAX_LINE_ITEMS])
        more = f"；另 {len(rows) - MAX_LINE_ITEMS} 項" if len(rows) > MAX_LINE_ITEMS else ""
        return shown + more

    if findings.get("slow_rpcs"):
        rows = findings["slow_rpcs"]
        lines.append(f"🟡 DB 體檢：前端 RPC 平均超過 1 秒 {len(rows)} 支（{win}）："
                     + items(rows, lambda r: f"{r['display']} {r['mean_ms'] / 1000:.1f} 秒×{r['calls']} 次"))
    if findings.get("unread_tables"):
        rows = findings["unread_tables"]
        lines.append(f"🟡 DB 體檢：預算表一直在算卻沒人讀 {len(rows)} 張："
                     + items(rows, lambda r: f"{r['display']} {r['reason']}"))
    if findings.get("heavy_sql"):
        rows = findings["heavy_sql"]
        lines.append(f"🟡 DB 體檢：單一 SQL 每天讀硬碟超過 1 GB {len(rows)} 支："
                     + items(rows, lambda r: f"{r['display']} {r['gb_per_day']:.1f} GB/天"))
    return lines


# ── 任務 ──────────────────────────────────────────────────────────────────────

def run(now: Optional[datetime] = None) -> dict:
    """跑一次體檢、寫狀態檔，回傳本次結果（含自身成本）。"""
    now = now or datetime.now(TAIPEI_TZ)
    state = load_state()
    t0 = time.monotonic()
    conn, cur = _connect()
    text_fetched = 0
    try:
        numbers = fetch_numbers(cur)
        stats_reset = numbers["stats_reset"]
        prev = state.get("snapshot") or {}
        same_epoch = prev.get("stats_reset") == str(stats_reset) and prev.get("taken_at")
        if same_epoch:
            since = datetime.fromisoformat(prev["taken_at"])
            delta = diff_stmts(prev.get("stmts"), numbers["stmts"])
        else:
            # 第一次（或 stats_reset 變了）只建基準：累計值含已修好的舊問題，拿來報會是假警報。
            # 不需要差值的「預算表沒有讀者」（結構判斷）照常報。
            since, delta = now, {}
        window_days = max((now - since).total_seconds() / 86400, 1 / 24)

        names = {k: v for k, v in (state.get("names") or {}).items() if k in numbers["stmts"] or k.startswith("text:")}
        need = queryids_needing_text(numbers, delta, names, window_days)
        if need:
            texts = fetch_texts(cur, need)
            text_fetched = len(need)
            for key, text in texts.items():
                names[key] = name_from_text(text)
                if not names[key]:
                    names[f"text:{key}"] = short_sql(text)
        conn.rollback()
    finally:
        conn.close()
    elapsed_ms = (time.monotonic() - t0) * 1000

    findings = evaluate(numbers, delta, names, window_days, _labels(), baseline=not same_epoch)
    result = {
        "run_date": now.date().isoformat(),
        "ran_at": now.isoformat(),
        "window_from": since.isoformat(),
        "window_days": round(window_days, 2),
        "baseline_only": not same_epoch,
        "cost": {"elapsed_ms": round(elapsed_ms), "text_queryids": text_fetched,
                 "statements": len(numbers["stmts"]), "dealloc": numbers["dealloc"]},
        "findings": findings,
        "lines": report_lines(findings, window_days),
    }
    keep_names = {k: v for k, v in names.items()
                  if k in numbers["stmts"] or (k.startswith("text:") and k[5:] in numbers["stmts"])}
    save_state({
        "snapshot": {"stats_reset": str(stats_reset), "taken_at": now.isoformat(), "stmts": numbers["stmts"]},
        "names": keep_names,
        "last": result,
    })
    log.info("DB 體檢完成：%s ms、取文字 %s 筆、發現 %s 行",
             result["cost"]["elapsed_ms"], text_fetched, len(result["lines"]))
    print(f"🩺 DB 體檢：{result['cost']}；" + ("全綠" if not result["lines"] else "\n" + "\n".join(result["lines"])))
    return result


def report_lines_for_today(now: Optional[datetime] = None) -> list[str]:
    """日報用：今天有跑過體檢才回傳發現（全綠或今天沒跑→空清單）。"""
    now = now or datetime.now(TAIPEI_TZ)
    last = load_state().get("last") or {}
    if last.get("run_date") != now.date().isoformat():
        return []
    return list(last.get("lines") or [])
