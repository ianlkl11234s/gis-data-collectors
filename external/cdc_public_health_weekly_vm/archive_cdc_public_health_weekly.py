#!/usr/bin/env python3
"""每日歸檔：把 CDC 公衛週報 VM 的快照 JSON 打包成 tar.gz 上傳 S3，再刪本地。

寫法照 external/ship_ais_vm/archive_ship_ais.py（與主 repo tasks/archive.py 同格式）：
  - 路徑來源 : <DATA_DIR>/cdc_public_health_weekly/YYYY/MM/DD/*.json
  - S3 目的地: s3://<BUCKET>/cdc_public_health_weekly/archives/YYYY-MM-DD.tar.gz
    （接續 2026-06-16 搬上 VM 前 Zeabur 版留下的同一前綴）
  - S3 已有同名物件就不覆寫，直接刪本地
  - 上傳成功（head_object 驗證大小一致）才刪本地

collector 每週四跑一次，所以只有週四有日期目錄；預設 ARCHIVE_RETENTION_DAYS=1
＝隔天（D+1）就上傳。每次往回掃 30 天，補處理之前失敗或漏跑的日期。

排程：每天 03:20（台北）由 cron 觸發（setup_cron.sh）。
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

COLLECTOR = "cdc_public_health_weekly"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/cdc-public-health/data"))
BUCKET = os.environ.get("S3_BUCKET")
REGION = os.environ.get("S3_REGION", "ap-southeast-2")
RETENTION_DAYS = max(1, int(os.environ.get("ARCHIVE_RETENTION_DAYS", "1")))  # 1＝隔天上傳
SCAN_DAYS = 30

TAIPEI_TZ = timezone(timedelta(hours=8))


def s3_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY"),
    )


def pack_date(date_dir: Path) -> bytes:
    """把目錄底下所有 .json 打包成 tar.gz bytes（週報快照約數 MB，放記憶體即可）"""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for json_file in sorted(date_dir.glob("*.json")):
            tar.add(json_file, arcname=json_file.name)
    return buf.getvalue()


def s3_object_size(s3, key: str) -> int | None:
    try:
        return int(s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"])
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def archive_one_day(s3, day: datetime) -> str:
    """歸檔指定日期，回傳狀態字串；上傳失敗會丟例外（本地保留，下次再試）。"""
    date_str = day.strftime("%Y-%m-%d")
    date_dir = DATA_DIR / COLLECTOR / day.strftime("%Y/%m/%d")
    if not date_dir.exists():
        return f"{date_str}: 本地無資料，跳過"

    json_files = list(date_dir.glob("*.json"))
    if not json_files:
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: 空目錄，已刪"

    s3_key = f"{COLLECTOR}/archives/{date_str}.tar.gz"
    if s3_object_size(s3, s3_key) is not None:
        shutil.rmtree(date_dir, ignore_errors=True)
        return f"{date_str}: S3 已存在 ({s3_key})，本地刪除"

    tar_bytes = pack_date(date_dir)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=tar_bytes, ContentType="application/gzip")
    remote = s3_object_size(s3, s3_key)
    if remote != len(tar_bytes):
        raise RuntimeError(f"上傳後大小不符（本地 {len(tar_bytes)}、S3 {remote}），保留本地")
    shutil.rmtree(date_dir, ignore_errors=True)
    return f"{date_str}: 上傳 {len(json_files)} 檔 → {s3_key} ({len(tar_bytes):,} bytes)，本地刪除"


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("archive_cdc")

    if not BUCKET:
        log.error("S3_BUCKET 未設定，跳過")
        return 1

    s3 = s3_client()
    today = datetime.now(TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    failed = 0
    for delta in range(RETENTION_DAYS, RETENTION_DAYS + SCAN_DAYS):
        target = today - timedelta(days=delta)
        try:
            log.info(archive_one_day(s3, target))
        except Exception as exc:
            log.error(f"{target.date()}: 失敗 — {exc}")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
