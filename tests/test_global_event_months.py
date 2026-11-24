"""全球重大事件每月期別檔（tasks/global_event_months.py）：月份規劃、唯讀查詢、上傳、告警。"""
import json
from datetime import date

import pytest

from tasks import global_event_months as gem
from utils import alerting, notify


def test_file_name_is_frontend_contract():
    # mini src/data/globalEventMonths.ts globalEventMonthFile 的檔名
    assert gem.file_name("2026-10") == "get_global_event_month_202610.json"


def test_scheduled_months_prev_and_current():
    assert gem.scheduled_months(date(2026, 11, 24)) == ["2026-10", "2026-11"]
    assert gem.scheduled_months(date(2027, 1, 2)) == ["2026-12", "2027-01"]


def test_months_between_crosses_year():
    assert gem.months_between("2026-11", "2027-02") == ["2026-11", "2026-12", "2027-01", "2027-02"]


def test_final_after_three_days_taipei():
    assert gem.is_final("2026-10", date(2026, 11, 3)) is False
    assert gem.is_final("2026-10", date(2026, 11, 4)) is True
    assert gem.is_final("2026-11", date(2026, 11, 24)) is False


def test_plan_increment_only():
    today = date(2026, 11, 24)
    # 已有定版檔 → 跳過；本月一律重產（未完結）
    assert gem.plan(["2026-10", "2026-11"], today, {"2026-10": False, "2026-11": True}) == [
        ("2026-10", False, "skip:已有定版檔"), ("2026-11", True, "run")]
    # 上個月的檔還是 partial（月中產的）→ 要補一次定版（Codex review mini #1225 P2）
    assert gem.plan(["2026-10"], today, {"2026-10": True}) == [("2026-10", False, "run")]
    # 沒有檔 → 產定版；--force 也重產
    assert gem.plan(["2026-10"], today, {}) == [("2026-10", False, "run")]
    assert gem.plan(["2026-10"], today, {"2026-10": False}, force=True) == [("2026-10", False, "run")]
    # 月底後未滿 3 天的上個月仍是未完結、每天重產
    assert gem.plan(["2026-10"], date(2026, 11, 2), {"2026-10": True}) == [("2026-10", True, "run")]


def test_plan_refuses_months_older_than_retention_margin():
    # 保留月數放大時，150 天保護仍擋住候選表可能已殘缺的月份
    action = gem.plan(["2026-09"], date(2027, 3, 1), {}, keep=12)[0][2]
    assert action.startswith("skip:早於 150 天")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.sql.append(sql.strip().split("\n")[0])
        if params:
            self.conn.params.append(params)

    def fetchone(self):
        p = self.conn.params[-1]
        month = p["month_start"][:7]
        if month in self.conn.fail:
            raise RuntimeError("statement timeout")
        return (json.dumps({"format": "global-events-month-v1", "month": month, "partial": p["partial"],
                            "cols": ["id", "t", "cat", "sev", "title", "pts"], "rows": []}),)


class FakeConn:
    def __init__(self, fail=()):
        self.sql, self.params, self.fail, self.rollbacks = [], [], set(fail), 0

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1


class FakeBoto:
    def __init__(self, heads=None):
        self.heads, self.puts, self.deleted, self.fail_delete = dict(heads or {}), [], [], set()

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError
        if Key not in self.heads:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"Metadata": self.heads[Key]}

    def put_object(self, **kw):
        self.puts.append(kw)
        self.heads[kw["Key"]] = kw.get("Metadata", {})

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.heads if k.startswith(Prefix))
        # 兩頁，驗分頁
        half = (len(keys) + 1) // 2
        if ContinuationToken is None and len(keys) > 1:
            return {"Contents": [{"Key": k} for k in keys[:half]], "IsTruncated": True, "NextContinuationToken": "t"}
        rest = keys[half:] if ContinuationToken else keys
        return {"Contents": [{"Key": k} for k in rest], "IsTruncated": False}

    def delete_object(self, Bucket, Key):
        if Key in self.fail_delete:
            raise RuntimeError("AccessDenied")
        self.deleted.append(Key)
        self.heads.pop(Key, None)


class FakeStore:
    def __init__(self, heads=None):
        self.s3, self.bucket = FakeBoto(heads), "bucket"


def test_run_read_only_writes_s3_and_r2_with_partial_metadata():
    conn = FakeConn()
    s3 = FakeStore({"deploy-assets/static-rpc/get_global_event_month_202610.json": {"partial": "false"}})
    r2 = FakeStore({"site/static-rpc/get_global_event_month_202610.json": {"partial": "false"}})
    stats = gem.run(today=date(2026, 11, 24), conn=conn, s3=s3, r2=r2)
    assert stats["ok"] == 1 and stats["skipped"] == 1 and stats["failed"] == 0 and stats["r2"] == "on"
    assert conn.sql[:3] == ["SET TRANSACTION READ ONLY", "SET LOCAL statement_timeout = '120s'", "SET LOCAL TimeZone = 'Asia/Taipei'"]
    assert conn.params[0] == {"month_start": "2026-11-01T00:00:00+08:00", "partial": True}
    assert conn.rollbacks >= 1  # 唯讀交易一律 rollback，不 commit
    (s3put,), (r2put,) = s3.s3.puts, r2.s3.puts
    assert s3put["Key"] == "deploy-assets/static-rpc/get_global_event_month_202611.json"
    assert s3put["Metadata"]["partial"] == "true"
    assert r2put["Key"] == "site/static-rpc/get_global_event_month_202611.json"
    assert r2put["CacheControl"] == gem.CACHE_PARTIAL
    assert json.loads(s3put["Body"])["month"] == "2026-11"


def test_final_month_rerun_when_r2_missing_even_if_s3_final():
    key = "get_global_event_month_202610.json"
    s3, r2 = FakeStore({"deploy-assets/static-rpc/" + key: {"partial": "false"}}), FakeStore()
    stats = gem.run(["2026-10"], today=date(2026, 11, 24), conn=FakeConn(), s3=s3, r2=r2)
    assert stats["ok"] == 1 and [p["Key"] for p in r2.s3.puts] == ["site/static-rpc/" + key]
    # 兩邊都有定版檔才跳過
    s3, r2 = FakeStore({"deploy-assets/static-rpc/" + key: {"partial": "false"}}), FakeStore({"site/static-rpc/" + key: {"partial": "false"}})
    stats = gem.run(["2026-10"], today=date(2026, 11, 24), conn=FakeConn(), s3=s3, r2=r2)
    assert stats["skipped"] == 1 and stats["ok"] == 0


def test_run_failure_isolated(monkeypatch):
    monkeypatch.setattr("storage.r2.get_r2_storage", lambda: None)
    conn, s3 = FakeConn(fail={"2026-10"}), FakeStore()
    stats = gem.run(["2026-10", "2026-11"], today=date(2026, 11, 24), conn=conn, s3=s3)
    assert stats["failed"] == 1 and stats["ok"] == 1 and stats["r2"] == "off"
    assert "2026-10: statement timeout" in stats["failures"][0]
    assert [p["Key"] for p in s3.s3.puts] == ["deploy-assets/static-rpc/get_global_event_month_202611.json"]


def test_run_local_only_writes_out_dir_and_never_touches_storage(tmp_path):
    stats = gem.run(["2026-10"], today=date(2026, 11, 24), conn=FakeConn(), upload=False, out_dir=str(tmp_path))
    assert stats["ok"] == 1 and stats["r2"] == "off"
    data = json.loads((tmp_path / "get_global_event_month_202610.json").read_text())
    assert data["partial"] is False


@pytest.fixture
def sent(monkeypatch):
    out = []
    monkeypatch.setattr(alerting, "record_routine", lambda name, ok, summary="", **kw: out.append(("routine", name, ok, summary)))
    monkeypatch.setattr(alerting, "raise_alert", lambda key, level, text, **kw: out.append(("raise", key, level, text)))
    monkeypatch.setattr(alerting, "clear_alert", lambda key, text=None, **kw: out.append(("clear", key)))
    return out


def test_notify_success_clears_failure_yellow(sent):
    notify.notify_global_event_months({"ok": 2, "failed": 0})
    assert sent == [("routine", "global_event_months", True, "2 個月"), ("clear", "global_event_months")]
    sent.clear()
    notify.notify_global_event_months({"ok": 1, "failed": 1, "failures": ["2026-10: timeout"]})
    assert sent[0][:3] == ("routine", "global_event_months", False)
    assert sent[1][:3] == ("raise", "global_event_months", alerting.YELLOW) and "2026-10: timeout" in sent[1][3]


R2K = "site/static-rpc/get_global_event_month_{}.json"


def test_earliest_kept_and_range_cross_year():
    assert gem.earliest_kept(date(2026, 11, 24), 6) == "2026-06"
    assert gem.earliest_kept(date(2027, 3, 1), 6) == "2026-10"
    assert gem.earliest_kept(date(2027, 1, 31), 6) == "2026-08"
    assert gem.kept_range(date(2027, 1, 31), 6) == "2026-08～2027-01"


def test_prune_keeps_sixth_month_deletes_seventh_and_ignores_other_names():
    today = date(2027, 3, 1)  # 保留 2026-10～2027-03
    r2 = FakeStore({R2K.format(m): {} for m in ("202609", "202610", "202611", "202703")})
    for other in ("site/static-rpc/get_global_event_month_202608.json.bak", "site/static-rpc/get_global_event_month_2026.json",
                  "site/static-rpc/get_global_event_month_202613.json", "site/static-rpc/get_fire_events_by_year_114.json",
                  "site/other/get_global_event_month_202601.json"):
        r2.s3.heads[other] = {}
    out = gem.prune_r2(r2, today, 6)
    assert out["targets"] == [R2K.format("202609")] and out["deleted"] == [R2K.format("202609")]
    assert R2K.format("202610") in r2.s3.heads  # 剛好第 6 個月不刪
    assert "site/static-rpc/get_global_event_month_202608.json.bak" in r2.s3.heads
    assert "site/static-rpc/get_global_event_month_202613.json" in r2.s3.heads


def test_prune_dry_run_lists_but_deletes_nothing():
    r2 = FakeStore({R2K.format(m): {} for m in ("202512", "202601", "202606")})
    out = gem.prune_r2(r2, date(2026, 11, 24), 6, dry_run=True)
    assert out["targets"] == [R2K.format("202512"), R2K.format("202601")] and out["deleted"] == []
    assert r2.s3.deleted == []


def test_run_prunes_after_generating_and_delete_failure_alerts():
    s3 = FakeStore()
    r2 = FakeStore({R2K.format("202604"): {}, R2K.format("202605"): {}})
    r2.s3.fail_delete = {R2K.format("202605")}
    stats = gem.run(["2026-11"], today=date(2026, 11, 24), conn=FakeConn(), s3=s3, r2=r2, keep=6)
    assert stats["kept_range"] == "2026-06～2026-11"
    assert stats["pruned"] == [R2K.format("202604")]
    assert stats["failed"] == 1 and "AccessDenied" in stats["failures"][0]


def test_run_dry_run_writes_and_deletes_nothing():
    s3, r2 = FakeStore(), FakeStore({R2K.format("202601"): {}})
    stats = gem.run(["2026-11"], today=date(2026, 11, 24), conn=FakeConn(), s3=s3, r2=r2, dry_run=True)
    assert s3.s3.puts == [] and r2.s3.puts == [] and r2.s3.deleted == []
    assert stats["prune_targets"] == [R2K.format("202601")]


def test_plan_skips_months_outside_public_retention():
    assert gem.plan(["2026-05", "2026-11"], date(2026, 11, 24), {}, keep=6) == [
        ("2026-05", False, "skip:超過公開保留期（最近 6 個月）"), ("2026-11", True, "run")]


def test_notify_summary_includes_kept_range(sent):
    notify.notify_global_event_months({"ok": 1, "failed": 0, "kept_range": "2026-06～2026-11"})
    assert sent[0] == ("routine", "global_event_months", True, "1 個月（公開保留 2026-06～2026-11）")
