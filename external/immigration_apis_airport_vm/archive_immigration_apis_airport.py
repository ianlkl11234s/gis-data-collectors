#!/usr/bin/env python3
"""每日歸檔：把本機所有超過保留期的 immigration_apis_airport JSON 打包成 tar.gz 上 S3，再刪本地。

  - 路徑來源 : <DATA_DIR>/immigration_apis_airport/YYYY/MM/DD/*.json
  - S3 目的地: s3://<BUCKET>/immigration_apis_airport/archives/YYYY-MM-DD.tar.gz
  - 掃本機所有日期目錄（不只 today-(N+1) 那一天），漏跑的日期下次會補上
  - S3 已有同名物件不覆寫，本地保留並記錯誤（由 health_report 的滯留檢查告警）
  - 上傳後 head_object 核對大小一致才刪本地

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


def local_due_dates(collector_dir: Path, today: datetime, keep_days: int) -> list[datetime]:
    """本機所有 YYYY/MM/DD 日期目錄中，日期 <= today - keep_days 者（舊到新）。"""
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


def s3_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def archive_one_day(target_day: datetime, log, s3=None) -> bool:
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
    s3_key = f"immigration_apis_airport/archives/{archive_name}"
    if BUCKET:
        s3 = s3 or s3_client()
        existing = s3_object_size(s3, s3_key)
        if existing is not None:
            log.error(f"  ✗ S3 已有 {s3_key}（{existing:,} B），不覆寫；本地保留待人工處理")
            return False

    with tarfile.open(archive_path, "w:gz") as tar:
        for f in files:
            tar.add(f, arcname=f.name)
    log.info(f"  打包 {len(files)} 檔 → {archive_path} ({archive_path.stat().st_size:,} B)")

    if BUCKET:
        try:
            s3.upload_file(str(archive_path), BUCKET, s3_key)
            remote = s3_object_size(s3, s3_key)
            if remote != archive_path.stat().st_size:
                log.error(f"  ✗ 上傳後大小不符（本地 {archive_path.stat().st_size}、S3 {remote}），保留本地")
                return False
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

    today = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    targets = local_due_dates(DATA_DIR / "immigration_apis_airport", today, RETENTION_DAYS + 1)
    log.info(f"歸檔目標日: {[t.strftime('%Y-%m-%d') for t in targets] or '無'} (保留近 {RETENTION_DAYS+1} 天)")
    s3 = s3_client() if BUCKET else None
    failed = 0
    for target_dt in targets:
        try:
            if not archive_one_day(target_dt, log, s3) and (DATA_DIR / "immigration_apis_airport" / target_dt.strftime("%Y/%m/%d")).exists():
                failed += 1
        except Exception as exc:
            log.error(f"{target_dt.date()}: 失敗 — {exc}")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
