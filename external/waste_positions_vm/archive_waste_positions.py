#!/usr/bin/env python3
"""每日歸檔：把昨天的 waste_positions JSON 打包成 tar.gz 上傳 S3，再刪本地。

跟 ship_ais archiver 同模子：
  - 路徑來源 : <DATA_DIR>/waste_positions/YYYY/MM/DD/*.json
  - S3 目的地: s3://<BUCKET>/waste_positions/archives/YYYY-MM-DD.tar.gz

排程：每天凌晨 03:05 由 cron 觸發（與 ship_ais 03:00 錯開）
"""
from __future__ import annotations

import io
import logging
import os
import shutil
import sys
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

APP_DIR = Path(__file__).parent
load_dotenv(APP_DIR / ".env")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/waste-positions/data"))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "7"))
COLLECTOR_NAME = "waste_positions"

TAIPEI_TZ = timezone(timedelta(hours=8))


def s3_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )


def pack_date(date_dir: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for fp in sorted(date_dir.glob("*.json")):
            tar.add(fp, arcname=fp.name)
    return buf.getvalue()


def s3_object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def archive_one_day(s3, date: datetime) -> str:
    date_str = date.strftime("%Y-%m-%d")
    date_dir = DATA_DIR / COLLECTOR_NAME / date.strftime("%Y/%m/%d")
    if not date_dir.exists():
        return f"{date_str}: 本地無資料，跳過"
    json_files = list(date_dir.glob("*.json"))
    if not json_files:
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: 空目錄，已刪"

    s3_key = f"{COLLECTOR_NAME}/archives/{date_str}.tar.gz"
    if s3_object_exists(s3, s3_key):
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: S3 已存在 ({s3_key})，本地刪除"

    tar_bytes = pack_date(date_dir)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=tar_bytes)
    shutil.rmtree(date_dir, ignore_errors=True)
    return f"{date_str}: 上傳 {len(json_files)} 檔 → {s3_key} ({len(tar_bytes):,} bytes)，本地刪除"


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    log = logging.getLogger("archive")
    if not BUCKET:
        log.error("S3_BUCKET 未設定")
        return 1

    s3 = s3_client()
    today = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    failed = 0
    for delta in range(RETENTION_DAYS, RETENTION_DAYS + 30):
        target = today - timedelta(days=delta)
        try:
            log.info(archive_one_day(s3, target))
        except Exception as exc:
            log.error(f"{target.date()}: 失敗 — {exc}")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
