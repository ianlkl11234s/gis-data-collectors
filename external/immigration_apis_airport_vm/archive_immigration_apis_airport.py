#!/usr/bin/env python3
"""每日歸檔：把 N 天前的 immigration_apis_airport JSON 打包成 tar.gz 上 S3，再刪本地。

  - 路徑來源 : <DATA_DIR>/immigration_apis_airport/YYYY/MM/DD/*.json
  - S3 目的地: s3://<BUCKET>/immigration_apis_airport/archives/YYYY-MM-DD.tar.gz

排程：每天 03:15 由 cron 觸發（在 setup_cron.sh 內安裝）
"""
from __future__ import annotations

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

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/immigration-apis-airport/data"))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "7"))

TAIPEI_TZ = timezone(timedelta(hours=8))


def s3_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def archive_one_day(target_day: datetime, log) -> bool:
    date_dir = DATA_DIR / "immigration_apis_airport" / target_day.strftime("%Y/%m/%d")
    if not date_dir.exists():
        log.info(f"  {date_dir} 不存在，跳過")
        return False

    files = list(date_dir.glob("*.json"))
    if not files:
        log.info(f"  {date_dir} 無 JSON，刪空目錄")
        shutil.rmtree(date_dir, ignore_errors=True)
        return False

    archive_name = target_day.strftime("%Y-%m-%d") + ".tar.gz"
    archive_path = date_dir.parent / archive_name

    with tarfile.open(archive_path, "w:gz") as tar:
        for f in files:
            tar.add(f, arcname=f.name)
    log.info(f"  打包 {len(files)} 檔 → {archive_path} ({archive_path.stat().st_size:,} B)")

    if BUCKET:
        s3_key = f"immigration_apis_airport/archives/{archive_name}"
        try:
            s3_client().upload_file(str(archive_path), BUCKET, s3_key)
            log.info(f"  ✓ S3 上傳: s3://{BUCKET}/{s3_key}")
            archive_path.unlink()
            shutil.rmtree(date_dir, ignore_errors=True)
            return True
        except ClientError as e:
            log.error(f"  ✗ S3 上傳失敗: {e}")
            return False
    else:
        log.warning("  S3_BUCKET 未設定，本地保留 tar.gz")
        return False


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("archive_iaa")

    today = datetime.now(TAIPEI_TZ).date()
    target = today - timedelta(days=RETENTION_DAYS + 1)
    target_dt = datetime(target.year, target.month, target.day, tzinfo=TAIPEI_TZ)
    log.info(f"歸檔目標日: {target_dt.strftime('%Y-%m-%d')} (保留近 {RETENTION_DAYS+1} 天)")
    archive_one_day(target_dt, log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
