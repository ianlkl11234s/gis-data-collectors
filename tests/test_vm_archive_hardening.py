"""HiCloud VM 整理（2026-11-08）：歸檔掃本機所有日期、immigration 不覆寫、health_report 告警。"""
import importlib.util
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
TZ = timezone(timedelta(hours=8))

ARCHIVE_SCRIPTS = {
    "ship_ais": "ship_ais_vm/archive_ship_ais.py",
    "waste_positions": "waste_positions_vm/archive_waste_positions.py",
    "isohe_port_marine": "isohe_port_marine_vm/archive_isohe_port_marine.py",
    "immigration_apis_airport": "immigration_apis_airport_vm/archive_immigration_apis_airport.py",
    "cdc_public_health_weekly": "cdc_public_health_weekly_vm/archive_cdc_public_health_weekly.py",
    "template": "_vm_template/archive_template.py",
}


def _load(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "external" / rel)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _mkday(root: Path, day: str, files=("a.json",)) -> Path:
    d = root / day.replace("-", "/")
    d.mkdir(parents=True)
    for f in files:
        (d / f).write_text("{}")
    return d


@pytest.mark.parametrize("collector,rel", ARCHIVE_SCRIPTS.items())
def test_local_due_dates_covers_dates_older_than_30_days(tmp_path, collector, rel):
    mod = _load(rel, f"archive_{collector}")
    root = tmp_path / collector
    for day in ("2026-08-23", "2026-11-01", "2026-11-05", "2026-11-08"):
        _mkday(root, day)
    (root / "2026" / "11" / "xx").mkdir()  # 非日期目錄忽略
    today = datetime(2026, 11, 8, tzinfo=TZ)
    got = [d.strftime("%Y-%m-%d") for d in mod.local_due_dates(root, today, 7)]
    assert got == ["2026-08-23", "2026-11-01"]


class FakeS3:
    def __init__(self, existing=None):
        self.objects = dict(existing or {})
        self.uploads = []

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": self.objects[Key]}

    def upload_file(self, path, bucket, key):
        self.uploads.append(key)
        self.objects[key] = Path(path).stat().st_size


def _immigration(monkeypatch, tmp_path, s3):
    mod = _load(ARCHIVE_SCRIPTS["immigration_apis_airport"], "archive_iaa_test")
    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mod, "BUCKET", "bucket")
    monkeypatch.setattr(mod, "s3_client", lambda: s3)
    return mod


def test_immigration_archives_every_overdue_date(monkeypatch, tmp_path):
    s3 = FakeS3()
    mod = _immigration(monkeypatch, tmp_path, s3)
    root = tmp_path / "immigration_apis_airport"
    now = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    old = [(now - timedelta(days=n)).strftime("%Y-%m-%d") for n in (40, 9)]
    recent = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    for day in old + [recent]:
        _mkday(root, day)
    assert mod.main() == 0
    assert sorted(s3.uploads) == sorted(f"immigration_apis_airport/archives/{d}.tar.gz" for d in old)
    assert not (root / old[0].replace("-", "/")).exists()
    assert (root / recent.replace("-", "/")).exists()


def test_immigration_never_overwrites_existing_archive(monkeypatch, tmp_path):
    key_day = "2026-06-16"
    s3 = FakeS3({f"immigration_apis_airport/archives/{key_day}.tar.gz": 317})
    mod = _immigration(monkeypatch, tmp_path, s3)
    day_dir = _mkday(tmp_path / "immigration_apis_airport", key_day)
    ok = mod.archive_one_day(datetime(2026, 6, 16, tzinfo=TZ), logging.getLogger("t"), s3)
    assert ok is False
    assert s3.uploads == []
    assert day_dir.exists()


@pytest.fixture
def health():
    return _load("_shared/health_report.py", "health_report_test")


def test_health_disk_alert_above_80(health):
    _, alerts = health.run_checks({"disk_used_pct": 81, "disk_avail": "7G"}, targets=[])
    assert any("磁碟使用率 81%" in a for a in alerts)
    _, alerts = health.run_checks({"disk_used_pct": 80, "disk_avail": "7G"}, targets=[])
    assert alerts == []


def test_health_stale_date_dirs(health, tmp_path):
    ship_root, moa_root = tmp_path / "ship", tmp_path / "moa"
    for day in ("2026-08-23", "2026-10-29", "2026-11-01", "2026-11-07"):
        _mkday(ship_root / "ship_ais", day)
    (moa_root / "buffer").mkdir(parents=True)
    _mkday(moa_root / "buffer", "2026-01-01")  # 非 collector 目錄不檢查
    _mkday(moa_root / "food_prices", "2026-10-23")
    _mkday(moa_root / "animal_adoption", "2026-11-02")
    targets = [
        {"app": "nonexistent-app", "root": str(ship_root), "collectors": ["ship_ais"], "default_retention": 7},
        {"app": "nonexistent-app", "root": str(moa_root), "collectors": "*", "default_retention": 7, "keep_extra": 1},
    ]
    checks, alerts = health.run_checks({"disk_used_pct": 30}, targets=targets, today=datetime(2026, 11, 8, tzinfo=TZ))
    stale = checks["stale_date_dirs"]
    # 保留 7 天 + 寬限 2 天：age > 9 才算滯留（10/29 age 10 算，11/01 age 7 不算）
    assert stale["ship_ais"] == {"keep_days": 7, "dates": ["2026-08-23", "2026-10-29"]}
    assert stale["food_prices"] == {"keep_days": 8, "dates": ["2026-10-23"]}
    assert set(stale) == {"ship_ais", "food_prices"}
    assert len(alerts) == 2


def test_daily_report_shows_vm_alerts(monkeypatch):
    from tasks import monitoring
    from tasks.daily_report import DailyReportTask

    snap = {"system": {"disk_used_pct": 85}, "egress_ip": "1.2.3.4", "collectors": {}, "outbound_health": {},
            "alerts": ["磁碟使用率 85% > 80%（剩 5G）"]}
    monkeypatch.setattr(monitoring, "list_vm_health_snapshots", lambda **_: [
        {"host": "vm1", "is_lost": False, "snapshot": snap, "age_hours": 1}])
    text = DailyReportTask([])._section_external_vm_health()
    assert "🔴 `vm1`" in text
    assert "🔴 磁碟使用率 85%" in text
