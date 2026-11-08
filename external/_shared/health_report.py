#!/usr/bin/env python3
"""VM health snapshot — 每天 07:00 跑，把本機健康度推 S3 給 Zeabur daily_report 撈。

設計目標：
  - VM 自己看自己，產 JSON 推 s3://<BUCKET>/_external_vm_health/<HOST>/YYYY-MM-DD.json
  - Zeabur 端 tasks/daily_report.py 的 _section_external_vm_health 撈這份 JSON
  - 若 VM 死透無法 push，Zeabur 端用 snapshot age > 26h 反推「VM 失聯」

統計範圍：
  - 各 collector log 過去 24h: runs / success / last_success / last_count
  - VM uptime / load avg / disk usage / data dir 各子目錄大小
  - Outbound 健檢：對 ship_ais / waste_positions 的來源 API 各 ping 一次
    （提早發現「IP 又被擋」這種大事）
  - 告警檢查（2026-11-08 加，結果放 snapshot["alerts"]，daily_report 會在 Telegram 日報標 🔴）：
      · 根目錄磁碟使用率 > DISK_ALERT_PCT（80%）
      · 有日期目錄超過「保留天數 + 2 天」仍留在本機（歸檔卡住）

部署：
  - 程式位置 : /opt/external-health/health_report.py
  - cron     : 0 7 * * * 跑（早 1 小時於 Zeabur daily_report，08:00 撈得到）
  - .env     : 共用 /opt/ship-ais/.env 的 S3 key（或自己一份）

可由 ENV `EXTERNAL_VM_HEALTH_CONFIG` 指定 yaml 自訂監測 collector 清單；
無指定時 fallback 到下方 DEFAULT_TARGETS（ship_ais + waste_positions）。
"""
from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import time as _time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

try:
    import yaml  # 選用，沒裝就走 DEFAULT_TARGETS
except ImportError:
    yaml = None  # type: ignore

import boto3
from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parent
# 共用 ship_ais 的 .env（同一 VM、同一組 S3 key）；不存在就讀自己目錄
for candidate in (Path("/opt/ship-ais/.env"), APP_DIR / ".env"):
    if candidate.exists():
        load_dotenv(candidate)
        break

TAIPEI_TZ = timezone(timedelta(hours=8))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
S3_PREFIX = "_external_vm_health"

# fallback 監測清單（與 external/*_vm/ 對齊）
DEFAULT_TARGETS: list[dict] = [
    {
        "name": "ship_ais",
        "log_path": "/var/log/ship-ais/collect.log",
        "data_dir": "/var/lib/ship-ais/data/ship_ais",
        "outbound": [
            {"label": "mpbais", "host": "mpbais.motcmpb.gov.tw", "path": "/aismpb/tools/geojsonais.ashx"},
        ],
        "success_pattern": r"Supabase 寫入: history=(\d+)",
        "expected_interval_min": 10,
    },
    {
        "name": "waste_positions",
        "log_path": "/var/log/waste-positions/collect.log",
        "data_dir": "/var/lib/waste-positions/data/waste_positions",
        "outbound": [
            {"label": "kcg",       "host": "openapi.kcg.gov.tw",  "path": "/"},
            {"label": "ntpc",      "host": "data.ntpc.gov.tw",    "path": "/"},
            {"label": "tainan",    "host": "soa.tainan.gov.tw",   "path": "/"},
        ],
        "success_pattern": r"Supabase 寫入: (\d+) 筆",
        "expected_interval_min": 2,
    },
    {
        "name": "isohe_port_marine",
        "log_path": "/var/log/isohe-port-marine/collect.log",
        "data_dir": "/var/lib/isohe-port-marine/data/isohe_port_marine",
        "outbound": [
            {"label": "isohe_tp_wave", "host": "isohe.ihmt.gov.tw", "path": "/opendata/Wave?port=TP&format=JSON"},
        ],
        "success_pattern": r"Supabase 寫入: stations=(\d+) readings=\d+",
        "expected_interval_min": 10,
    },
]


DISK_ALERT_PCT = int(os.environ.get("VM_DISK_ALERT_PCT", "80"))
STALE_GRACE_DAYS = 2

# 歸檔滯留檢查：每支 app 的資料根目錄、保留天數來源（app .env 的 ARCHIVE_RETENTION_DAYS）
# keep_extra：archive 腳本實際多留的天數（immigration 保留 N+1 天）
ARCHIVE_TARGETS: list[dict] = [
    {"app": "ship-ais", "root": "/var/lib/ship-ais/data", "collectors": ["ship_ais"], "default_retention": 7},
    {"app": "waste-positions", "root": "/var/lib/waste-positions/data", "collectors": ["waste_positions"], "default_retention": 7},
    {"app": "isohe-port-marine", "root": "/var/lib/isohe-port-marine/data", "collectors": ["isohe_port_marine"], "default_retention": 3},
    {"app": "immigration-apis-airport", "root": "/var/lib/immigration-apis-airport/data",
     "collectors": ["immigration_apis_airport"], "default_retention": 7, "keep_extra": 1},
    {"app": "cdc-public-health", "root": "/var/lib/cdc-public-health/data",
     "collectors": ["cdc_public_health_weekly"], "default_retention": 1},
    # moa-opendata：data/ 底下每個子目錄都是一支 collector（buffer、.archive-receipts 除外）
    {"app": "moa-opendata", "root": "/var/lib/moa-opendata/data", "collectors": "*", "default_retention": 7},
]
_NON_COLLECTOR_DIRS = {"buffer", "state", ".archive-receipts", "weather_raw"}


def app_retention_days(app: str, default: int) -> int:
    env_file = Path(f"/opt/{app}/.env")
    try:
        from dotenv import dotenv_values
        value = dotenv_values(env_file).get("ARCHIVE_RETENTION_DAYS") if env_file.exists() else None
        return int(value) if value else default
    except (ValueError, OSError):
        return default


def stale_date_dirs(collector_dir: Path, keep_days: int, today: datetime) -> list[str]:
    """回傳超過 keep_days + STALE_GRACE_DAYS 天仍在本機的日期（YYYY-MM-DD，舊到新）。"""
    out = []
    for day_dir in collector_dir.glob("[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]"):
        if not day_dir.is_dir():
            continue
        try:
            day = datetime.strptime("/".join(day_dir.parts[-3:]), "%Y/%m/%d").date()
        except ValueError:
            continue
        if (today.date() - day).days > keep_days + STALE_GRACE_DAYS:
            out.append(day.isoformat())
    return sorted(out)


def run_checks(system: dict, targets: list[dict] | None = None, today: datetime | None = None) -> tuple[dict, list[str]]:
    """磁碟與歸檔滯留檢查；回傳 (checks 明細, alerts 文字清單)。"""
    today = today or datetime.now(TAIPEI_TZ)
    alerts: list[str] = []
    disk = system.get("disk_used_pct")
    if disk is None:
        alerts.append("無法讀取磁碟使用率")
    elif disk > DISK_ALERT_PCT:
        alerts.append(f"磁碟使用率 {disk}% > {DISK_ALERT_PCT}%（剩 {system.get('disk_avail')}）")

    stale: dict[str, dict] = {}
    for t in targets if targets is not None else ARCHIVE_TARGETS:
        root = Path(t["root"])
        if not root.exists():
            continue
        keep = app_retention_days(t["app"], t["default_retention"]) + t.get("keep_extra", 0)
        if t["collectors"] == "*":
            names = sorted(p.name for p in root.iterdir() if p.is_dir() and p.name not in _NON_COLLECTOR_DIRS)
        else:
            names = t["collectors"]
        for name in names:
            dates = stale_date_dirs(root / name, keep, today)
            if dates:
                stale[name] = {"keep_days": keep, "dates": dates}
                shown = ", ".join(dates[:5]) + (f" …共 {len(dates)} 天" if len(dates) > 5 else "")
                alerts.append(f"{name} 有日期目錄超過保留 {keep}+{STALE_GRACE_DAYS} 天未清：{shown}")
    return {"disk_alert_pct": DISK_ALERT_PCT, "stale_date_dirs": stale}, alerts


def load_targets() -> list[dict]:
    cfg_path = os.environ.get("EXTERNAL_VM_HEALTH_CONFIG")
    if cfg_path and yaml is not None and Path(cfg_path).exists():
        with open(cfg_path, encoding="utf-8") as f:
            return yaml.safe_load(f) or DEFAULT_TARGETS
    return DEFAULT_TARGETS


# ────────────────────────────────────────────────────────────────────
# 系統指標
# ────────────────────────────────────────────────────────────────────
def system_metrics() -> dict:
    out: dict = {"hostname": socket.gethostname()}
    try:
        with open("/proc/uptime", encoding="utf-8") as f:
            up_sec = float(f.read().split()[0])
        out["uptime_days"] = round(up_sec / 86400, 2)
    except Exception:
        out["uptime_days"] = None
    try:
        load1, _load5, _load15 = os.getloadavg()
        out["load_avg_1m"] = round(load1, 2)
    except Exception:
        out["load_avg_1m"] = None
    try:
        res = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=5)
        line = res.stdout.splitlines()[1].split()
        out["disk_used_pct"] = int(line[4].rstrip("%"))
        out["disk_avail"] = line[3]
    except Exception:
        out["disk_used_pct"] = None
    return out


# ────────────────────────────────────────────────────────────────────
# Collector log 分析（過去 24h）
# ────────────────────────────────────────────────────────────────────
LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s")


def analyze_log(log_path: str, success_pattern: str) -> dict:
    """tail 過去 24h 行數，統計 runs / success / last_success / last_count"""
    p = Path(log_path)
    if not p.exists():
        return {"error": "log file not found"}

    cutoff = datetime.now(TAIPEI_TZ) - timedelta(hours=24)
    runs = 0
    successes = 0
    last_success_at = None
    last_count = None
    success_re = re.compile(success_pattern)
    started_marker = re.compile(r"\[INFO\] (抓到|高雄市|新北市|臺南市|開始)")

    try:
        with open(p, encoding="utf-8", errors="replace") as f:
            # 簡單 tail：log 一天 < 100KB，直接讀全檔
            lines = f.readlines()
        for line in lines:
            m = LOG_TS_RE.match(line)
            if not m:
                continue
            try:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TAIPEI_TZ)
            except ValueError:
                continue
            if ts < cutoff:
                continue
            if started_marker.search(line):
                runs += 1
            sm = success_re.search(line)
            if sm:
                successes += 1
                last_success_at = ts
                try:
                    last_count = int(sm.group(1))
                except (IndexError, ValueError):
                    last_count = None
    except Exception as exc:
        return {"error": f"log parse failed: {exc}"}

    return {
        "runs_24h": runs,
        "success_24h": successes,
        "last_success_at": last_success_at.isoformat() if last_success_at else None,
        "last_count": last_count,
    }


# ────────────────────────────────────────────────────────────────────
# 資料目錄大小
# ────────────────────────────────────────────────────────────────────
def dir_size_mb(path: str) -> int | None:
    try:
        res = subprocess.run(["du", "-sm", path], capture_output=True, text=True, timeout=30)
        return int(res.stdout.split()[0])
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────────
# Outbound 健檢
# ────────────────────────────────────────────────────────────────────
def check_outbound(host: str, path: str = "/", timeout: int = 10) -> dict:
    out: dict = {"host": host}
    try:
        t = _time.time()
        s = socket.create_connection((host, 443), timeout=timeout)
        out["tcp_ms"] = int((_time.time() - t) * 1000)
        s.close()
    except Exception as exc:
        out["tcp_ms"] = None
        out["error"] = f"tcp: {exc}"
        return out
    try:
        req = urllib.request.Request(f"https://{host}{path}", method="HEAD",
                                     headers={"User-Agent": "Mozilla/5.0"})
        r = urllib.request.urlopen(req, timeout=timeout)
        out["http_status"] = r.status
    except urllib.error.HTTPError as e:
        out["http_status"] = e.code  # 4xx/5xx 也算有回應
    except Exception as exc:
        out["http_status"] = None
        out["error"] = f"http: {exc}"
    return out


# ────────────────────────────────────────────────────────────────────
# Egress IP
# ────────────────────────────────────────────────────────────────────
def egress_ip() -> str | None:
    try:
        return urllib.request.urlopen("https://api.ipify.org", timeout=10).read().decode().strip()
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────────
# 組裝 + 上傳
# ────────────────────────────────────────────────────────────────────
def build_snapshot() -> dict:
    targets = load_targets()
    collectors_report: dict[str, dict] = {}
    outbound_report: dict[str, dict] = {}
    for t in targets:
        c = analyze_log(t["log_path"], t["success_pattern"])
        c["snapshot_dir_mb"] = dir_size_mb(t["data_dir"])
        c["expected_interval_min"] = t.get("expected_interval_min")
        collectors_report[t["name"]] = c
        for ob in t.get("outbound", []):
            label = ob.get("label", ob["host"])
            outbound_report[label] = check_outbound(ob["host"], ob.get("path", "/"))

    system = system_metrics()
    checks, alerts = run_checks(system)
    return {
        "host": os.environ.get("VM_HOSTNAME") or socket.gethostname(),
        "generated_at": datetime.now(TAIPEI_TZ).isoformat(),
        "egress_ip": egress_ip(),
        "system": system,
        "collectors": collectors_report,
        "outbound_health": outbound_report,
        "checks": checks,
        "alerts": alerts,
    }


def upload_snapshot(snapshot: dict) -> str:
    if not BUCKET:
        raise RuntimeError("S3_BUCKET 未設定")
    today = datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d")
    host = snapshot["host"]
    key = f"{S3_PREFIX}/{host}/{today}.json"
    s3 = boto3.client("s3", region_name=REGION,
                      aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
                      aws_secret_access_key=os.environ.get("S3_SECRET_KEY"))
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8"),
                  ContentType="application/json")
    return f"s3://{BUCKET}/{key}"


def main() -> int:
    snapshot = build_snapshot()
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    for alert in snapshot["alerts"]:
        print(f"⚠️ ALERT: {alert}")
    if not snapshot["alerts"]:
        print("✓ checks: 磁碟與歸檔滯留皆正常")
    try:
        s3_path = upload_snapshot(snapshot)
        print(f"\n✓ uploaded → {s3_path}")
        return 0
    except Exception as exc:
        print(f"\n✗ upload failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
