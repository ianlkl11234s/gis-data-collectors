"""每週 DB 效能體檢（tasks/db_efficiency_audit.py）：判斷邏輯、差值、輕量約束、日報併入。"""
from datetime import datetime, timedelta, timezone

import pytest

from tasks import db_efficiency_audit as audit
from tasks import daily_report, monitoring

TPE = timezone(timedelta(hours=8))
MON = datetime(2026, 11, 16, 7, 30, tzinfo=TPE)  # 週一
RESET = datetime(2026, 11, 5, 15, 55, tzinfo=timezone.utc)
ANON, POSTGRES = 16484, 16388
GB_BLOCKS = 1024 ** 3 // 8192

FUNCS = [
    {"schema": "public", "name": "refresh_foo_daily_incremental", "frontend": False,
     "src": "INSERT INTO live.foo_daily SELECT ... FROM live.foo_raw; UPDATE live.foo_daily_watermark SET x = 1;"},
    {"schema": "public", "name": "get_foo_day", "frontend": True, "src": "SELECT * FROM live.foo_daily WHERE day = d"},
    {"schema": "public", "name": "refresh_bar_daily", "frontend": False,
     "src": "DELETE FROM live.bar_daily WHERE day = d; INSERT INTO live.bar_daily SELECT 1"},
    {"schema": "public", "name": "refresh_baz_hourly", "frontend": False, "src": "INSERT INTO live.baz_hourly SELECT 1"},
    {"schema": "public", "name": "_baz_rows", "frontend": False, "src": "SELECT * FROM live.baz_hourly"},
    {"schema": "public", "name": "get_baz", "frontend": True, "src": "RETURN QUERY SELECT * FROM public._baz_rows();"},
    {"schema": "public", "name": "get_slow", "frontend": True, "src": "SELECT * FROM live.foo_raw"},
]
JOBS = [
    {"jobname": "refresh-foo", "schedule": "5,35 * * * *",
     "command": "SELECT public.refresh_foo_daily_incremental(current_date)"},
    {"jobname": "refresh-bar", "schedule": "7 * * * *", "command": "SELECT public.refresh_bar_daily(current_date)"},
    {"jobname": "refresh-baz", "schedule": "9 * * * *", "command": "SELECT public.refresh_baz_hourly()"},
]


def _numbers(stmts):
    return {"stats_reset": RESET, "dealloc": 0, "reader_oids": {ANON: "anon"}, "stmts": stmts,
            "jobs": JOBS, "funcs": FUNCS, "tables": {"live.foo_daily": False, "live.bar_daily": False}}


# ── 純函式 ───────────────────────────────────────────────────────────────────

def test_name_from_text():
    assert audit.name_from_text('WITH pgrst_source AS (SELECT "pgrst_call".* FROM "public"."get_foo_day"($1) '
                                'pgrst_call) SELECT 1') == "get_foo_day"
    assert audit.name_from_text("SELECT public.refresh_bar_daily((now() AT TIME ZONE $1)::date)") == "refresh_bar_daily"
    assert audit.name_from_text("INSERT INTO live.ship_positions (a, b) VALUES ($1, $2)") == "INSERT live.ship_positions"
    assert audit.name_from_text("SELECT section_uid FROM live.road_sections_current") == ""


def test_diff_handles_new_dealloc_and_growth():
    prev = {"1:a": [10, 100.0, 5], "1:b": [50, 500.0, 50]}
    cur = {"1:a": [15, 160.0, 8], "1:b": [3, 30.0, 2], "1:c": [4, 40.0, 1], "1:d": [0, 0.0, 0]}
    d = audit.diff_stmts(prev, cur)
    assert d["1:a"] == [5, 60.0, 3]
    assert d["1:b"] == [3, 30.0, 2]   # 累計變小＝被擠掉後重新出現 → 用目前值
    assert d["1:c"] == [4, 40.0, 1]   # 新出現
    assert "1:d" not in d


def test_refresh_targets_skip_state_tables():
    t = audit.refresh_targets(JOBS, FUNCS)
    assert set(t) == {"live.foo_daily", "live.bar_daily", "live.baz_hourly"}
    assert t["live.foo_daily"]["jobs"] == ["refresh-foo"]


def test_readers_direct_and_via_internal_function():
    writers = {"refresh_foo_daily_incremental", "refresh_bar_daily", "refresh_baz_hourly"}
    assert audit.readers_of("live.foo_daily", FUNCS, writers) == ["get_foo_day"]
    assert audit.readers_of("live.baz_hourly", FUNCS, writers) == ["get_baz"]
    assert audit.readers_of("live.bar_daily", FUNCS, writers) == []


def test_evaluate_three_checks():
    delta = {
        f"{ANON}:1": [10, 25000.0, 0],                 # get_slow 平均 2.5 秒
        f"{ANON}:2": [100, 500.0, 0],                  # get_foo_day 快
        f"{POSTGRES}:3": [7, 70.0, 7 * 2 * GB_BLOCKS],  # 7 天讀 14 GB → 2 GB/天
        f"{POSTGRES}:4": [7, 70.0, 70 * GB_BLOCKS],     # 收集器寫入，不報
    }
    names = {f"{ANON}:1": "get_slow", f"{ANON}:2": "get_foo_day", f"{POSTGRES}:3": "refresh_bar_daily",
             f"{POSTGRES}:4": "INSERT live.ship_positions"}
    labels = {"live.foo_raw": "Foo 原始"}
    f = audit.evaluate(_numbers({}), delta, names, 7.0, labels)
    assert [r["name"] for r in f["slow_rpcs"]] == ["get_slow"]
    assert f["slow_rpcs"][0]["display"] == "get_slow"  # 名稱跟原始表沒有共用字 → 不借 label
    unread = {r["table"]: r["reason"] for r in f["unread_tables"]}
    assert "沒有讀者" in unread["live.bar_daily"]
    assert "get_baz" in unread["live.baz_hourly"] and "0 次" in unread["live.baz_hourly"]
    assert "live.foo_daily" not in unread  # get_foo_day 有被呼叫
    assert [r["name"] for r in f["heavy_sql"]] == ["refresh_bar_daily"]
    assert round(f["heavy_sql"][0]["gb_per_day"], 1) == 2.0
    lines = audit.report_lines(f, 7.0)
    assert len(lines) == 3 and all(x.startswith("🟡 DB 體檢") for x in lines)


def test_baseline_only_reports_structural():
    f = audit.evaluate(_numbers({}), {}, {}, 1 / 24, {}, baseline=True)
    assert f["slow_rpcs"] == [] and f["heavy_sql"] == []
    assert [r["table"] for r in f["unread_tables"]] == ["live.bar_daily"]  # baz 的「0 次」要差值，基準週不報


def test_all_green_has_no_lines():
    assert audit.report_lines({"slow_rpcs": [], "unread_tables": [], "heavy_sql": []}, 7) == []


def test_label_prefers_shared_tokens():
    labels = {"live.earthquake_events": "地震事件", "live.disaster_alerts": "災害示警"}
    funcs = {"get_alert_series_24h": [{"src": "FROM live.earthquake_events e, live.disaster_alerts a"}]}
    assert audit.display_fn("get_alert_series_24h", funcs, labels) == "災害示警（get_alert_series_24h）"
    assert audit.display_fn("INSERT live.earthquake_events", funcs, labels) == "收集器寫入 地震事件（live.earthquake_events）"


# ── run()：輕量約束、只在需要時取文字、狀態檔 ──────────────────────────────────

class _Cur:
    def __init__(self, stmts, texts):
        self.stmts, self.texts, self.sql, self._rows = stmts, texts, [], []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        s = " ".join(sql.split())
        if "pg_stat_statements_info" in s:
            self._rows = [(RESET, 3)]
        elif "FROM pg_roles" in s:
            self._rows = [(ANON, "anon")]
        elif "pg_stat_statements(false)" in s:
            self._rows = [(int(k.split(":")[0]), int(k.split(":")[1]), *v) for k, v in self.stmts.items()]
        elif "pg_stat_statements(true)" in s:
            self._rows = [(ANON, q, self.texts[q]) for q in params[0]]
        elif "FROM cron.job" in s:
            self._rows = [(j["jobname"], j["schedule"], j["command"]) for j in JOBS]
        elif "FROM pg_proc" in s:
            self._rows = [(f["schema"], f["name"], f["src"], f["frontend"]) for f in FUNCS]
        elif "FROM pg_class" in s:
            self._rows = [("live.foo_daily", False)]
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


class _Conn:
    def rollback(self):
        pass

    def close(self):
        pass


def test_run_baseline_then_weekly(monkeypatch, tmp_path):
    monkeypatch.setattr(audit.config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(audit, "_labels", lambda: {})
    texts = {1: 'SELECT 1 FROM "public"."get_slow"($1)', 2: 'SELECT 1 FROM "public"."get_foo_day"($1)'}
    cur = _Cur({f"{ANON}:1": [5, 5.0, 0], f"{ANON}:2": [5, 5.0, 0]}, texts)
    monkeypatch.setattr(audit, "_connect", lambda: (_Conn(), cur))

    first = audit.run(now=MON)
    assert first["baseline_only"] is True and first["cost"]["text_queryids"] == 0
    assert not any("pg_stat_statements(true)" in s for s in cur.sql)  # 基準週不取文字
    assert [x for x in first["lines"] if "平均超過 1 秒" in x] == []
    assert any("bar_daily 沒有讀者" in x for x in first["lines"])  # 結構判斷基準週照報

    # 一週後：get_slow 多 10 次、共 30 秒 → 平均 3 秒
    cur.stmts = {f"{ANON}:1": [15, 30005.0, 0], f"{ANON}:2": [9, 9.0, 0]}
    cur.sql.clear()
    second = audit.run(now=MON + timedelta(days=7))
    assert second["baseline_only"] is False and second["window_days"] == 7.0
    assert second["cost"]["text_queryids"] == 2
    assert any("get_slow 3.0 秒×10 次" in x for x in second["lines"])
    # 所有查詢只碰統計檢視／系統目錄，且第一句就設 statement_timeout（由 _connect 負責，這裡檢查不掃資料表）
    assert not any(" live." in s or " analytics." in s for s in cur.sql)

    # 名稱已快取：第三週不再取文字
    cur.stmts = {f"{ANON}:1": [16, 30006.0, 0], f"{ANON}:2": [10, 10.0, 0]}
    cur.sql.clear()
    third = audit.run(now=MON + timedelta(days=14))
    assert third["cost"]["text_queryids"] == 0
    assert not any("pg_stat_statements(true)" in s for s in cur.sql)


def test_stats_reset_change_restarts_baseline(monkeypatch, tmp_path):
    monkeypatch.setattr(audit.config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(audit, "_labels", lambda: {})
    cur = _Cur({f"{ANON}:1": [5, 5.0, 0]}, {1: '"public"."get_slow"('})
    monkeypatch.setattr(audit, "_connect", lambda: (_Conn(), cur))
    audit.run(now=MON)
    state = audit.load_state()
    state["snapshot"]["stats_reset"] = "1999-01-01 00:00:00+00:00"
    audit.save_state(state)
    again = audit.run(now=MON + timedelta(days=7))
    assert again["baseline_only"] is True


def test_connect_is_readonly_with_timeout(monkeypatch):
    calls = {}

    class C:
        def set_session(self, **kw):
            calls["session"] = kw

        def cursor(self):
            class K:
                def execute(self, sql, params=None):
                    calls.setdefault("sql", []).append(sql)
            return K()

    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: C())
    monkeypatch.setattr(audit.config, "SUPABASE_DB_URL", "postgresql://x", raising=False)
    audit._connect()
    assert calls["session"]["readonly"] is True
    assert "statement_timeout" in calls["sql"][0]


# ── 日報併入 ─────────────────────────────────────────────────────────────────

def _report(monkeypatch, tmp_path, now, lines, run_date):
    from tests.test_daily_report_compact import _setup
    task = _setup(monkeypatch, tmp_path, problems=False)
    monkeypatch.setattr(audit.config, "LOCAL_DATA_DIR", tmp_path)
    audit.save_state({"last": {"run_date": run_date, "lines": lines}})
    return task._build_compact_report(now=now)


def test_daily_report_monday_with_findings(monkeypatch, tmp_path):
    now = datetime(2026, 11, 8, 8, 0, tzinfo=TPE)
    line = "🟡 DB 體檢：前端 RPC 平均超過 1 秒 1 支（7 天）：get_slow 3.0 秒×10 次"
    report = _report(monkeypatch, tmp_path, now, [line], "2026-11-08")
    assert f"• {line}" in report
    assert "整體：🟡 有 1 件要處理" in report


def test_daily_report_monday_all_green(monkeypatch, tmp_path):
    now = datetime(2026, 11, 8, 8, 0, tzinfo=TPE)
    report = _report(monkeypatch, tmp_path, now, [], "2026-11-08")
    assert "DB 體檢" not in report and "✅ 一切正常" in report


def test_daily_report_other_day_ignores_old_result(monkeypatch, tmp_path):
    now = datetime(2026, 11, 17, 8, 0, tzinfo=TPE)
    report = _report(monkeypatch, tmp_path, now, ["🟡 DB 體檢：x"], "2026-11-08")
    assert "DB 體檢" not in report
