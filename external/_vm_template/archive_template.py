#!/usr/bin/env python3
"""<name> 每日歸檔範本：本機所有早於保留期的日期目錄 → S3 tar.gz → 核對 → 刪本地。

  - 來源 : <DATA_DIR>/<name>/YYYY/MM/DD/*.json
  - 目的 : s3://<BUCKET>/<name>/archives/YYYY-MM-DD.tar.gz
  - 掃本機所有日期目錄（不只最近 30 天），漏跑或 S3 失敗的日期下次自動補
  - S3 已有同名物件：不覆寫、本地保留並記錯誤（health_report 會以滯留告警）
  - 上傳後 head_object 大小一致才刪本地
"""
from __future__ import annotations

import gzip
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

APP_DIR = Path(__file__).resolve().parent
load_dotenv(APP_DIR / ".env")

COLLECTOR = "<name>"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/<app>/data"))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "7"))  # 本機保留最近 N 天（含今天）
TAIPEI_TZ = timezone(timedelta(hours=8))


def s3_client():
    return boto3.client("s3", region_name=REGION,
                        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
                        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"))


def local_due_dates(collector_dir: Path, today: datetime, keep_days: int) -> list[datetime]:
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


def s3_object_size(s3, key: str) -> int | None:
    try:
        return int(s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"])
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def archive_one_day(s3, day: datetime) -> str:
    date_str = day.strftime("%Y-%m-%d")
    date_dir = DATA_DIR / COLLECTOR / day.strftime("%Y/%m/%d")
    files = sorted(date_dir.glob("*.json"))
    if not files:
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: 空目錄，已刪"
    key = f"{COLLECTOR}/archives/{date_str}.tar.gz"
    existing = s3_object_size(s3, key)
    if existing is not None:
        raise RuntimeError(f"S3 已有 {key}（{existing:,} B），不覆寫；本地保留待人工處理")
    tmp = DATA_DIR / f".tmp_{COLLECTOR}_{date_str}.tar.gz"
    try:
        # 串流寫檔、不整包放記憶體（VM 只有約 1GB RAM）；mtime=0 讓同內容產生同檔
        with tmp.open("wb") as out, gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as gz, \
                tarfile.open(fileobj=gz, mode="w") as tar:
            for f in files:
                tar.add(str(f), arcname=f.name)
        size = tmp.stat().st_size
        s3.upload_file(str(tmp), BUCKET, key, ExtraArgs={"ContentType": "application/gzip"})
        remote = s3_object_size(s3, key)
        if remote != size:
            raise RuntimeError(f"上傳後大小不符（本地 {size}、S3 {remote}），保留本地")
    finally:
        tmp.unlink(missing_ok=True)
    shutil.rmtree(date_dir)
    return f"{date_str}: 上傳 {len(files)} 檔 → {key} ({size:,} bytes)，本地刪除"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    log = logging.getLogger("archive")
    if not BUCKET:
        log.error("S3_BUCKET 未設定")
        return 1
    s3 = s3_client()
    today = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    targets = local_due_dates(DATA_DIR / COLLECTOR, today, RETENTION_DAYS)
    if not targets:
        log.info("無到期日期")
    failed = 0
    for day in targets:
        try:
            log.info(archive_one_day(s3, day))
        except Exception as exc:
            log.error(f"{day.date()}: 失敗 — {exc}")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
