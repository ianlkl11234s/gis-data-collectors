#!/usr/bin/env python3
"""每日歸檔：把昨天的 ship_ais JSON 打包成 tar.gz 上傳 S3，再刪本地。

跟主 repo tasks/archive.py 的行為一致：
  - 路徑來源 : <DATA_DIR>/ship_ais/YYYY/MM/DD/*.json
  - S3 目的地: s3://<BUCKET>/ship_ais/archives/YYYY-MM-DD.tar.gz
  - 完成後刪除本地該日目錄

排程：每天凌晨 03:00 由 cron 觸發（在 setup_vm.sh 內安裝）
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

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/ship-ais/data"))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "7"))  # 保留近 N 天本地（含今天）

TAIPEI_TZ = timezone(timedelta(hours=8))


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


def pack_date(date_dir: Path) -> bytes:
    """把目錄底下所有 .json 打包成 tar.gz bytes"""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for json_file in sorted(date_dir.glob("*.json")):
            tar.add(json_file, arcname=json_file.name)
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
    """歸檔指定日期（date 為 timezone-aware datetime 的當天），回傳狀態字串"""
    date_str = date.strftime("%Y-%m-%d")
    date_dir = DATA_DIR / "ship_ais" / date.strftime("%Y/%m/%d")
    if not date_dir.exists():
        return f"{date_str}: 本地無資料，跳過"

    json_files = list(date_dir.glob("*.json"))
    if not json_files:
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: 空目錄，已刪"

    s3_key = f"ship_ais/archives/{date_str}.tar.gz"
    if s3_object_exists(s3, s3_key):
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: S3 已存在 ({s3_key})，本地刪除"

    tar_bytes = pack_date(date_dir)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=tar_bytes)
    shutil.rmtree(date_dir, ignore_errors=True)
    return f"{date_str}: 上傳 {len(json_files)} 檔 → {s3_key} ({len(tar_bytes):,} bytes)，本地刪除"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("archive")

    if not BUCKET:
        log.error("S3_BUCKET 未設定，跳過")
        return 1

    s3 = s3_client()
    today = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    # 歸檔本機所有早於保留期的日期目錄（容錯：補處理被遺漏的歷史日，不限回掃天數）
    # 最近 RETENTION_DAYS 天保留本地不動
    failed = 0
    for target in local_due_dates(DATA_DIR / "ship_ais", today, RETENTION_DAYS):
        try:
            msg = archive_one_day(s3, target)
            log.info(msg)
        except Exception as exc:
            log.error(f"{target.date()}: 失敗 — {exc}")
            failed += 1

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
