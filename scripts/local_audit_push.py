#!/usr/bin/env python3
"""本機 GIS 三 repo 健康度 + 推 S3，給 Zeabur daily_report 撈。

用同樣的 _external_vm_health/<host>/YYYY-MM-DD.json 格式，
讓 daily_report 的 _section_external_vm_health 自動涵蓋本機，不必另寫 section。

範圍：
  - 三個 GIS repo 的 git status / 最新 commit
  - data/raw 與 data/processed 容量（若存在）
  - 磁碟用量

執行：
  python3 scripts/local_audit_push.py

  推 S3 用本機 ~/.config/.gis-audit-env 的 key（與 Zeabur 同一組）。
  fallback 環境變數 S3_ACCESS_KEY / S3_SECRET_KEY / S3_BUCKET。

launchd 範本見 scripts/com.gis.local_audit.plist。
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

TAIPEI_TZ = timezone(timedelta(hours=8))
S3_PREFIX = "_external_vm_health"

# GIS 三個 repo（依使用者本機路徑）
GIS_REPOS = {
    "data-collectors": Path("/Users/migu/Desktop/資料庫/gen_ai_try/ichef_工作用/GIS/data-collectors"),
    "gis-platform":    Path("/Users/migu/Desktop/資料庫/gen_ai_try/ichef_工作用/GIS/gis-platform"),
    "taipei-gis-analytics": Path("/Users/migu/Desktop/資料庫/gen_ai_try/ichef_工作用/GIS/taipei-gis-analytics"),
}

# 載入本機 audit 專用 env（保護 access key）
_env_path = Path.home() / ".config" / ".gis-audit-env"
if _env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_path)
    except ImportError:
        for line in _env_path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def git_status(repo: Path) -> dict:
    if not (repo / ".git").exists():
        return {"error": "not a git repo"}
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), "status", "--porcelain=v1"],
            capture_output=True, text=True, timeout=10,
        )
        dirty_lines = [l for l in out.stdout.splitlines() if l.strip()]
        log = subprocess.run(
            ["git", "-C", str(repo), "log", "-1", "--format=%h %s (%ar)"],
            capture_output=True, text=True, timeout=5,
        )
        branch = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        ahead = subprocess.run(
            ["git", "-C", str(repo), "rev-list", "--count", "@{upstream}..HEAD"],
            capture_output=True, text=True, timeout=5,
        )
        return {
            "branch": branch.stdout.strip(),
            "dirty_files": len(dirty_lines),
            "ahead_of_remote": int(ahead.stdout.strip()) if ahead.returncode == 0 else None,
            "last_commit": log.stdout.strip(),
        }
    except Exception as exc:
        return {"error": str(exc)}


def dir_size_mb(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        out = subprocess.run(["du", "-sm", str(path)], capture_output=True, text=True, timeout=60)
        return int(out.stdout.split()[0])
    except Exception:
        return None


def disk_usage() -> dict:
    try:
        out = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, timeout=5)
        line = out.stdout.splitlines()[1].split()
        return {"used_pct": int(line[4].rstrip("%")), "avail": line[3]}
    except Exception:
        return {}


def run_taipei_gis_audit() -> dict | None:
    """跑 taipei-gis-analytics 的 audit.py --format json，拿 summary（compliance/coverage 等）"""
    audit_path = GIS_REPOS["taipei-gis-analytics"] / ".claude/skills/data-catalog-audit/audit.py"
    if not audit_path.exists():
        return None
    try:
        res = subprocess.run(
            ["python3", str(audit_path), "--format", "json"],
            capture_output=True, text=True, timeout=300,
            cwd=str(GIS_REPOS["taipei-gis-analytics"]),
        )
        data = json.loads(res.stdout)
        return {
            "summary": data.get("summary", {}),
            "fatal_count": len(data.get("fatal", [])),
            "warn_count": len(data.get("warn", [])),
            "info_count": len(data.get("info", [])),
            "exit_code": res.returncode,
        }
    except Exception as exc:
        return {"error": str(exc)}


def build_snapshot() -> dict:
    collectors_report: dict[str, dict] = {}
    audit_result = run_taipei_gis_audit()
    for name, repo in GIS_REPOS.items():
        info: dict = git_status(repo)
        # 對 data-collectors 多看 data/ 目錄大小，幫助理解本機 raw 容量
        if name == "data-collectors":
            info["snapshot_dir_mb"] = dir_size_mb(repo / "data")
        # taipei-gis-analytics 嵌入 audit summary（compliance/coverage 等）
        if name == "taipei-gis-analytics" and audit_result:
            info["audit"] = audit_result
        # 補一個假的 24h 統計欄位，與 VM snapshot 結構對齊（避免 daily_report 解析錯）
        info.setdefault("runs_24h", 0)
        info.setdefault("success_24h", 0)
        info["last_success_at"] = datetime.now(TAIPEI_TZ).isoformat()
        collectors_report[name] = info

    return {
        "host": os.environ.get("VM_HOSTNAME") or f"local-{socket.gethostname()}",
        "generated_at": datetime.now(TAIPEI_TZ).isoformat(),
        "egress_ip": None,  # 本機不暴露
        "system": {
            "hostname": socket.gethostname(),
            "uptime_days": None,
            "load_avg_1m": round(os.getloadavg()[0], 2) if hasattr(os, "getloadavg") else None,
            "disk_used_pct": disk_usage().get("used_pct"),
            "disk_avail": disk_usage().get("avail"),
        },
        "collectors": collectors_report,
        "outbound_health": {},
    }


def upload_snapshot(snapshot: dict) -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET 未設定（檢查 ~/.config/.gis-audit-env）")
    today = datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d")
    host = snapshot["host"]
    key = f"{S3_PREFIX}/{host}/{today}.json"
    import boto3
    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("S3_REGION", "ap-southeast-2"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )
    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8"),
                  ContentType="application/json")
    return f"s3://{bucket}/{key}"


def main() -> int:
    snap = build_snapshot()
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    try:
        path = upload_snapshot(snap)
        print(f"\n✓ uploaded → {path}")
        return 0
    except Exception as exc:
        print(f"\n✗ upload failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
