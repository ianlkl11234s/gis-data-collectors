"""Supabase 備份強化（2026-11-07）：失敗告警、沒時間欄的表改整表快照、auth 對照、新 schema。"""
import gzip
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

import config
from tasks import backup_watch
from tasks.backup_supabase import BackupSupabaseTask

ROOT = Path(__file__).resolve().parents[1]
TPE = ZoneInfo("Asia/Taipei")
MANIFEST = yaml.safe_load((ROOT / "config" / "backup_manifest.yaml").read_text(encoding="utf-8"))

# S3 缺口盤點 #8：推測不可重算、以前每天被 skipped_no_time_column 跳過的 9 張
IRREPLACEABLE_NO_TIME = [
    "satellite_tle_history", "satellite_passes", "vessel_watch_registry",
    "pla_activity_items", "signals_hourly", "source_health",
    "aisstream_ingest_runs", "aisstream_archive_manifests", "gfw_hourly_publish_runs",
]
# 2026-11-07 pg_table_size 實測（bytes 取整）
SIZES = {"satellite_tle_history": 512 << 20, "satellite_passes": 25 << 20,
         "gfw_hourly_publish_runs": 24 << 20}


def _ok_result(**over):
    r = {
        "db_connected": True, "s3_ready": True,
        "static": {"uploaded": 1, "skipped": 300, "failed": 0},
        "realtime": {"uploaded": 20, "skipped": 5, "failed": 0, "empty": 3},
        "reconcile": {"ok": 50, "info": 0, "warn": 0, "critical": 2},
        "auth": {"enabled": True, "uploaded": 2, "failed": 0},
    }
    r.update(over)
    return r


@pytest.fixture
def watch_env(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "BACKUP_ENABLED", True)
    sent = []
    return sent


# ── 成敗判斷 ───────────────────────────────────────────────

def test_evaluate_success_ignores_reconcile_critical():
    ok, problems = backup_watch.evaluate(_ok_result())
    assert ok and problems == []


@pytest.mark.parametrize("over, needle", [
    ({"db_connected": False}, "DB"),
    ({"s3_ready": False}, "S3"),
    ({"realtime": {"error": "boom"}}, "B 即時"),
    ({"static": {"uploaded": 0, "failed": 3}}, "A 靜態 失敗 3"),
    ({"auth": {"uploaded": 0, "failed": 1}}, "D auth"),
])
def test_evaluate_flags_silent_skips_and_failures(over, needle):
    ok, problems = backup_watch.evaluate(_ok_result(**over))
    assert not ok
    assert any(needle in p for p in problems)


def test_evaluate_rejects_missing_result():
    assert backup_watch.evaluate(None)[0] is False


# ── 失敗告警與冷卻 ─────────────────────────────────────────

def test_failure_alerts_once_per_day_then_recovery(watch_env):
    sent = watch_env
    t1 = datetime(2026, 11, 8, 3, 40, tzinfo=TPE)
    backup_watch.record_run(_ok_result(db_connected=False), now=t1, notify=sent.append)
    backup_watch.record_run(error="IsolatedTaskError: 逾時", now=t1 + timedelta(hours=1), notify=sent.append)
    assert len(sent) == 1 and "備份失敗" in sent[0]
    st = backup_watch.load_state()
    assert st["last_status"] == "failed" and st["consecutive_failures"] == 2

    backup_watch.record_run(_ok_result(), now=t1 + timedelta(days=1), notify=sent.append)
    assert len(sent) == 2 and "恢復" in sent[1]
    st = backup_watch.load_state()
    assert st["last_success_date"] == "2026-11-09" and st["consecutive_failures"] == 0


def test_success_without_prior_failure_is_silent(watch_env):
    sent = watch_env
    backup_watch.record_run(_ok_result(), now=datetime(2026, 11, 8, 3, 40, tzinfo=TPE), notify=sent.append)
    assert sent == []


def test_error_text_is_redacted(watch_env):
    sent = watch_env
    backup_watch.record_run(
        error="connect postgresql://user:hunter2@db.example:5432/postgres failed",
        now=datetime(2026, 11, 8, 3, 40, tzinfo=TPE), notify=sent.append)
    assert "hunter2" not in sent[0]
    assert "hunter2" not in json.dumps(backup_watch.load_state())


# ── 當天沒跑的 watchdog ─────────────────────────────────────

def test_watchdog_alerts_when_no_run_today_once(watch_env):
    sent = watch_env
    now = datetime(2026, 11, 8, 6, 30, tzinfo=TPE)
    backup_watch.save_state({"last_run_at": "2026-11-06T03:35:00+08:00",
                             "last_success_date": "2026-11-06", "last_status": "ok"})
    assert backup_watch.check_today(now=now, notify=sent.append) == "alerted"
    assert backup_watch.check_today(now=now, notify=sent.append) == "already_alerted"
    assert len(sent) == 1 and "2026-11-06" in sent[0]


def test_watchdog_skips_when_today_succeeded_or_running(watch_env):
    sent = watch_env
    now = datetime(2026, 11, 8, 6, 30, tzinfo=TPE)
    backup_watch.save_state({"last_success_date": "2026-11-08", "last_run_at": "x"})
    assert backup_watch.check_today(now=now, notify=sent.append) == "ok"
    backup_watch.save_state({"last_run_at": "x", "running_since": (now - timedelta(minutes=30)).isoformat()})
    assert backup_watch.check_today(now=now, notify=sent.append) == "running"
    assert sent == []


def test_watchdog_stale_running_marker_still_alerts(watch_env):
    sent = watch_env
    now = datetime(2026, 11, 8, 9, 0, tzinfo=TPE)
    # 主程序 03:30 被殺，running_since 殘留
    backup_watch.save_state({"last_run_at": "x", "running_since": "2026-11-08T03:30:00+08:00"})
    assert backup_watch.check_today(now=now, notify=sent.append) == "alerted"


def test_watchdog_after_failed_run_does_not_double_alert(watch_env):
    sent = watch_env
    t = datetime(2026, 11, 8, 3, 40, tzinfo=TPE)
    backup_watch.record_run(_ok_result(s3_ready=False), now=t, notify=sent.append)
    assert backup_watch.check_today(now=t.replace(hour=6, minute=30), notify=sent.append) == "already_alerted"
    assert len(sent) == 1


def test_watchdog_fresh_host_falls_back_to_audit_log(watch_env):
    sent = watch_env
    now = datetime(2026, 11, 8, 6, 30, tzinfo=TPE)
    assert backup_watch.check_today(now=now, notify=sent.append, audit_fallback=lambda d: True) == "ok"
    assert backup_watch.check_today(now=now, notify=sent.append, audit_fallback=lambda d: None) == "alerted"


def test_watchdog_disabled_instance_never_alerts(watch_env, monkeypatch):
    monkeypatch.setattr(config, "BACKUP_ENABLED", False)
    assert backup_watch.check_today(notify=watch_env.append) == "disabled"
    assert watch_env == []


def test_health_status_shows_last_success(watch_env):
    backup_watch.record_run(_ok_result(), now=datetime(2026, 11, 8, 3, 40, tzinfo=TPE), notify=watch_env.append)
    h = backup_watch.health_status()
    assert h["enabled"] is True
    assert h["last_success_date"] == "2026-11-08"
    assert h["last_status"] == "ok"


def test_health_endpoint_includes_backup(watch_env, monkeypatch):
    pytest.importorskip("flask")
    from api import server
    backup_watch.record_run(_ok_result(), now=datetime(2026, 11, 8, 3, 40, tzinfo=TPE), notify=watch_env.append)
    app = server.create_app()
    app.testing = True
    body = app.test_client().get("/health").get_json()
    assert body["backup"]["last_success_date"] == "2026-11-08"


# ── 沒有時間欄的表：整表快照 ─────────────────────────────────

class _StateCursor:
    """只回應 _get_backup_state 的 SELECT。"""

    def __init__(self, state_row):
        self.state_row = state_row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return self.state_row


def _plan_task(manifest=MANIFEST, time_cols=None, sizes=None, state_row=None):
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task.manifest = manifest
    task._overrides = manifest.get("overrides", {})
    task._find_time_column = lambda s, t: (time_cols or {}).get(t)
    task._table_data_bytes = lambda s, t: (sizes or {}).get(t, 1 << 20)
    task._cursor = lambda: _StateCursor(state_row)
    return task


def test_irreplaceable_no_time_tables_all_get_backed_up():
    task = _plan_task(sizes=SIZES)
    for t in IRREPLACEABLE_NO_TIME:
        q = f"live.{t}"
        assert q not in MANIFEST["exclude"] and q not in MANIFEST["archive_py_covered"], q
        plan = task._plan_realtime_table("live", t)
        assert plan["mode"] == "full_table", (t, plan)
    assert task._plan_realtime_table("live", "satellite_tle_history")["interval_days"] == 7
    assert task._plan_realtime_table("live", "source_health")["interval_days"] == 1


def test_plan_prefers_time_column_and_override():
    task = _plan_task(time_cols={"weather_x": "observed_at"})
    assert task._plan_realtime_table("live", "weather_x") == {"mode": "daily", "time_col": "observed_at"}
    plan = task._plan_realtime_table("live", "ship_last_seen")
    assert plan["mode"] == "full_table" and plan["interval_days"] == 1 and plan["source"] == "override"


def test_plan_policy_skip_restores_old_behaviour():
    manifest = {**MANIFEST, "no_time_column": {"action": "skip"}}
    assert _plan_task(manifest=manifest)._plan_realtime_table("live", "x")["mode"] == "skip"


def test_weekly_table_due_only_after_interval():
    now = datetime.now(timezone.utc)
    assert _plan_task(state_row=None)._full_table_due("live", "big", 7) is True
    recent = ("fp", "k", 1, now - timedelta(days=2))
    assert _plan_task(state_row=recent)._full_table_due("live", "big", 7) is False
    old = ("fp", "k", 1, now - timedelta(days=6, hours=13))
    assert _plan_task(state_row=old)._full_table_due("live", "big", 7) is True
    assert _plan_task(state_row=recent)._full_table_due("live", "small", 1) is True


def test_reconcile_freshness_uses_interval():
    now = datetime.now(timezone.utc)
    four_days = ("fp", "k", 1, now - timedelta(days=4))
    assert _plan_task(state_row=four_days)._full_table_fresh("live", "big", 7) is True
    assert _plan_task(state_row=four_days)._full_table_fresh("live", "small", 1) is False
    assert _plan_task(state_row=None)._full_table_fresh("live", "big", 7) is False


def test_weekly_table_not_due_is_skipped_without_upload():
    task = _plan_task(sizes={"big": 600 << 20}, state_row=("fp", "k", 1, datetime.now(timezone.utc)))
    task.audit_buf = []
    task._process_realtime_full_table = lambda *a, **k: pytest.fail("不應上傳")
    assert task._process_realtime_table("live", "big", date(2026, 11, 7)) == "skipped"
    # DB CHECK 只允許既有 code
    assert task.audit_buf[-1]["code"] == "skipped_unchanged"


# ── 新 schema 與 auth 對照 ─────────────────────────────────

def test_manifest_adds_unbacked_schemas():
    static = set(MANIFEST["classification"]["static_schemas"])
    for s in ("intel", "tourism", "urban_open_space", "civic_facilities",
              "agriculture", "air_tickets", "cross_refs"):
        assert s in static


def test_auth_export_has_no_secrets():
    cfg = MANIFEST["auth_export"]
    assert cfg["enabled"] and cfg["s3_prefix"].startswith("supabase-snapshots/")
    for table, cols in cfg["tables"].items():
        assert "id" in cols
        for c in cols:
            assert not BackupSupabaseTask._is_forbidden_auth_column(c), (table, c)
    assert "provider_id" in cfg["tables"]["auth.identities"]


class _AuthCursor:
    def __init__(self, task):
        self.task = task

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.params = params

    def fetchall(self):
        return [(c,) for c in ("id", "email", "encrypted_password", "recovery_token", "created_at")]

    def copy_expert(self, sql, fh):
        self.task.copy_sql.append(sql)
        fh.write(b"id,email\n1,a@b\n")


class _S3:
    def __init__(self):
        self.uploads = []

    def upload_path(self, path, key, *, storage_class=None, content_type=None, content_encoding=None):
        self.uploads.append((key, gzip.decompress(Path(path).read_bytes()), storage_class))
        return True


def test_auth_snapshot_selects_only_allowed_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task.manifest = {"auth_export": {
        "enabled": True, "s3_prefix": "supabase-snapshots/auth", "storage_class": "GLACIER_IR",
        # 誤列 encrypted_password／token 也要被擋下
        "tables": {"auth.users": ["id", "email", "encrypted_password", "recovery_token", "phone"]},
    }}
    task.dry_run = False
    task.db_conn = object()
    task.s3 = _S3()
    task.static_storage_class = "GLACIER_IR"
    task.audit_buf = []
    task.copy_sql = []
    task._cursor = lambda: _AuthCursor(task)
    task._flush_audit = lambda: 0

    stats = task.run_auth_snapshot(run_date=date(2026, 11, 8))
    assert stats == {"enabled": True, "uploaded": 1, "failed": 0}
    sql = task.copy_sql[0]
    assert '"id", "email"' in sql
    assert "password" not in sql and "token" not in sql
    key, _, sc = task.s3.uploads[0]
    assert key == "supabase-snapshots/auth/2026-11-08/auth.users.csv.gz" and sc == "GLACIER_IR"
    details = json.loads(task.audit_buf[-1]["details"])
    assert details["missing_columns"] == ["phone"]


def test_auth_snapshot_disabled_or_unready():
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    task.manifest = {}
    assert task.run_auth_snapshot() == {"enabled": False, "uploaded": 0, "failed": 0}
    task.manifest = {"auth_export": {"enabled": True, "tables": {"auth.users": ["id"]}}}
    task.db_conn, task.s3 = None, None
    assert task.run_auth_snapshot()["failed"] == 1


# ── CDC VM：完整快照＋每日歸檔 ───────────────────────────────

_had_db_url = "SUPABASE_DB_URL" in os.environ
os.environ.setdefault("SUPABASE_DB_URL", "postgresql://test")
sys.path.insert(0, str(ROOT / "external" / "vm_common"))
sys.path.insert(0, str(ROOT / "external" / "cdc_public_health_weekly_vm"))
try:
    import archive_cdc_public_health_weekly as cdc_archive  # noqa: E402
    import cdc_public_health_weekly_collect as cdc_vm  # noqa: E402
finally:
    if not _had_db_url:
        os.environ.pop("SUPABASE_DB_URL", None)


def test_cdc_snapshot_keeps_full_data(tmp_path, monkeypatch):
    monkeypatch.setattr(cdc_vm, "DATA_DIR", tmp_path)
    rows = [{"disease_code": "influenza", "iso_week": i} for i in range(120)]
    ts = datetime(2026, 11, 5, 11, 0, tzinfo=timezone(timedelta(hours=8)))
    fp = cdc_vm.save_snapshot({"total": 120, "data": rows}, ts)
    saved = json.loads(fp.read_text(encoding="utf-8"))
    assert len(saved["data"]) == 120
    assert fp.parent == tmp_path / "cdc_public_health_weekly" / "2026" / "11" / "05"


class _FakeS3Client:
    def __init__(self, existing=None):
        self.objects = dict(existing or {})
        self.puts = []

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body
        self.puts.append(Key)


def _day_dir(root, day):
    d = root / "cdc_public_health_weekly" / day.strftime("%Y/%m/%d")
    d.mkdir(parents=True)
    (d / "cdc_public_health_weekly_1100.json").write_text('{"data": []}', encoding="utf-8")
    return d


def test_cdc_archive_uploads_then_deletes(tmp_path, monkeypatch):
    monkeypatch.setattr(cdc_archive, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cdc_archive, "BUCKET", "bucket")
    day = datetime(2026, 11, 5, tzinfo=cdc_archive.TAIPEI_TZ)
    d = _day_dir(tmp_path, day)
    s3 = _FakeS3Client()
    msg = cdc_archive.archive_one_day(s3, day)
    assert "上傳 1 檔" in msg
    assert s3.puts == ["cdc_public_health_weekly/archives/2026-11-05.tar.gz"]
    assert not d.exists()


def test_cdc_archive_existing_key_is_not_overwritten(tmp_path, monkeypatch):
    monkeypatch.setattr(cdc_archive, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cdc_archive, "BUCKET", "bucket")
    day = datetime(2026, 11, 5, tzinfo=cdc_archive.TAIPEI_TZ)
    d = _day_dir(tmp_path, day)
    s3 = _FakeS3Client({"cdc_public_health_weekly/archives/2026-11-05.tar.gz": b"x"})
    assert "已存在" in cdc_archive.archive_one_day(s3, day)
    assert s3.puts == [] and not d.exists()


def test_cdc_archive_size_mismatch_keeps_local(tmp_path, monkeypatch):
    monkeypatch.setattr(cdc_archive, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cdc_archive, "BUCKET", "bucket")
    day = datetime(2026, 11, 5, tzinfo=cdc_archive.TAIPEI_TZ)
    d = _day_dir(tmp_path, day)

    class Truncating(_FakeS3Client):
        def put_object(self, Bucket, Key, Body, **kw):
            self.objects[Key] = Body[:-1]

    with pytest.raises(RuntimeError):
        cdc_archive.archive_one_day(Truncating(), day)
    assert d.exists()


def test_cross_layer_map_lists_cdc_archive_prefix():
    cmap = yaml.safe_load((ROOT / "config" / "cross_layer_map.yaml").read_text(encoding="utf-8"))
    prefixes = [p["prefix"] for p in cmap["cdc_public_health_weekly"]["s3_prefixes"]]
    assert prefixes == ["cdc_public_health_weekly/archives/"]


# 2026-11-08：03:00 archive freshness 誤報（VM 搬遷、已停用、季節性）
def test_archive_lag_skips_disabled_vm_and_seasonal(monkeypatch):
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    for prefix in ("ANIMAL_SHELTER_OUTCOMES", "IMMIGRATION_APIS_AIRPORT", "ISOHE_PORT_MARINE",
                   "SHIP_TDX", "VD", "FOOD_PRICES"):
        monkeypatch.setattr(config, f"{prefix}_ENABLED", False, raising=False)
    for name in ("animal_shelter_outcomes", "immigration_apis_airport", "isohe_port_marine",
                 "ship_tdx", "vd", "food_prices"):
        lag, reason = task._archive_expected_lag(name)
        assert lag is None and reason == "本機未啟用", name

    # 明列者照表：VM 自行歸檔的 ship_ais 仍檢查、flight_fr24 永久略過
    assert task._archive_expected_lag("ship_ais") == (8, None)
    assert task._archive_expected_lag("flight_fr24") == (None, "停用")
    # registry 查不到的 prefix 維持預設
    assert task._archive_expected_lag("some_unknown_prefix") == (2, None)

    monkeypatch.setattr(config, "JAPAN_ENV_WBGT_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "JAPAN_ENV_WBGT_SEASON_MONTHS", "4,5,6,7,8,9,10")
    import collectors.global_climate.env_jp_wbgt as wbgt
    monkeypatch.setattr(wbgt, "in_season", lambda now, spec: False)
    assert task._archive_expected_lag("env_jp_wbgt") == (None, "季節外")
    monkeypatch.setattr(wbgt, "in_season", lambda now, spec: True)
    assert task._archive_expected_lag("env_jp_wbgt")[0] == 2


def test_archive_lag_widens_for_long_interval(monkeypatch):
    task = BackupSupabaseTask.__new__(BackupSupabaseTask)
    monkeypatch.setattr(config, "ANIMAL_SHELTER_PRESSURE_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ANIMAL_SHELTER_PRESSURE_INTERVAL", 43200, raising=False)
    assert task._archive_expected_lag("animal_shelter_pressure") == (31, None)
    monkeypatch.setattr(config, "YOUBIKE_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "YOUBIKE_INTERVAL", 10, raising=False)
    assert task._archive_expected_lag("youbike") == (2, None)


def test_production_notify_routes_through_alerting(watch_env, monkeypatch):
    """notify=None（主機實際走的路）：失敗 🔴 一則、隔天仍失敗不重送、成功 ✅ 一則。"""
    from utils import alerting
    sent = []
    monkeypatch.setattr(alerting, "_sender", lambda text: sent.append(text) or True)
    t1 = datetime(2026, 11, 8, 3, 40, tzinfo=TPE)
    backup_watch.record_run(_ok_result(db_connected=False), now=t1)
    backup_watch.check_today(now=t1.replace(hour=6, minute=30))
    backup_watch.record_run(_ok_result(s3_ready=False), now=t1 + timedelta(days=1))
    assert len(sent) == 1 and sent[0].startswith("🔴") and "Supabase 每日備份失敗" in sent[0]
    backup_watch.record_run(_ok_result(), now=t1 + timedelta(days=2))
    assert len(sent) == 2 and sent[1].startswith("✅")
