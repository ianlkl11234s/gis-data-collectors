#!/usr/bin/env python3
"""Archive ISOHE raw snapshots to S3 after the three-day local buffer."""
from __future__ import annotations

import io
import os
import shutil
import sys
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parent
load_dotenv(APP_DIR / ".env")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/isohe-port-marine/data"))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "3"))
TZ = timezone(timedelta(hours=8))


def local_due_dates(collector_dir: Path, today: datetime, keep_days: int) -> list[datetime]:
    """本機所有 YYYY/MM/DD 日期目錄中，日期 <= today - keep_days 者（舊到新）。

    不再只回掃最近 30 天：VM 停機或 S3 失敗超過一個月的日期也會被補處理，
    不會永遠卡在本機（2026-11-08 清出 ship_ais／waste_positions 8/23–8/27 共約 1.8GB）。
    """
    cutoff = today - timedelta(days=keep_days)
    out = []
    for day_dir in collector_dir.glob("[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]"):
        if not day_dir.is_dir():
            continue
        try:
            day = datetime.strptime("/".join(day_dir.parts[-3:]), "%Y/%m/%d").replace(tzinfo=today.tzinfo)
        except ValueError:
            continue
        if day <= cutoff:
            out.append(day)
    return sorted(out)


def s3_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )


def pack(date_dir: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as archive:
        for path in sorted(date_dir.glob("*.json")):
            archive.add(path, arcname=path.name)
    return buf.getvalue()


def archive_day(client, target: datetime) -> str:
    date_text = target.strftime("%Y-%m-%d")
    date_dir = DATA_DIR / "isohe_port_marine" / target.strftime("%Y/%m/%d")
    files = list(date_dir.glob("*.json")) if date_dir.exists() else []
    if not files:
        return f"{date_text}: no local snapshots"
    key = f"isohe_port_marine/archives/{date_text}.tar.gz"
    try:
        head = client.head_object(Bucket=BUCKET, Key=key)
        if int(head.get("ContentLength", 0)) <= 0:
            raise RuntimeError(f"existing archive is empty: {key}")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        body = pack(date_dir)
        if not body:
            raise RuntimeError(f"generated archive is empty: {date_text}")
        client.put_object(Bucket=BUCKET, Key=key, Body=body)
        head = client.head_object(Bucket=BUCKET, Key=key)
        if int(head.get("ContentLength", 0)) != len(body):
            raise RuntimeError(f"S3 size verification failed: {key}")
    shutil.rmtree(date_dir)
    return f"{date_text}: archived {len(files)} snapshots to {key}"


def main() -> int:
    if not BUCKET:
        print("S3_BUCKET is required", file=sys.stderr)
        return 1
    client = s3_client()
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    failed = False
    for target in local_due_dates(DATA_DIR / "isohe_port_marine", today, RETENTION_DAYS):
        try:
            print(archive_day(client, target))
        except Exception as exc:
            failed = True
            print(f"archive failed: {exc}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
