"""
AWS S3 儲存

適用於生產環境，支援 AWS S3 和相容服務（如 MinIO）。
提供資料同步、歸檔和讀取功能。
"""

import io
import json
from datetime import datetime
from pathlib import Path

import config


class S3Storage:
    """AWS S3 儲存"""

    def __init__(self):
        if not config.S3_BUCKET:
            raise ValueError("S3_BUCKET 未設定")

        try:
            import boto3
            from botocore.exceptions import ClientError
            self.ClientError = ClientError
        except ImportError:
            raise ImportError("請安裝 boto3: pip install boto3")

        # 建立 S3 客戶端
        client_kwargs = {
            'region_name': config.S3_REGION,
        }

        if config.S3_ACCESS_KEY and config.S3_SECRET_KEY:
            client_kwargs['aws_access_key_id'] = config.S3_ACCESS_KEY
            client_kwargs['aws_secret_access_key'] = config.S3_SECRET_KEY

        if config.S3_ENDPOINT:
            client_kwargs['endpoint_url'] = config.S3_ENDPOINT

        self.s3 = boto3.client('s3', **client_kwargs)
        self.bucket = config.S3_BUCKET

    def save(self, collector_name: str, data: dict, timestamp: datetime = None) -> str:
        """儲存資料到 S3

        Args:
            collector_name: 收集器名稱（作為前綴）
            data: 要儲存的資料
            timestamp: 時間戳記

        Returns:
            str: S3 物件 key
        """
        timestamp = timestamp or datetime.now()
        date_str = timestamp.strftime('%Y/%m/%d')
        time_str = timestamp.strftime('%H%M')

        # 上傳資料
        key = f"{collector_name}/{date_str}/{collector_name}_{time_str}.json"
        body = json.dumps(data, ensure_ascii=False, indent=2)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode('utf-8'),
            ContentType='application/json'
        )

        # 更新 latest
        latest_key = f"{collector_name}/latest.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=latest_key,
            Body=body.encode('utf-8'),
            ContentType='application/json'
        )

        return key

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """上傳本地檔案到 S3

        Args:
            local_path: 本地檔案路徑
            s3_key: S3 物件 key

        Returns:
            bool: 是否成功
        """
        try:
            self.s3.upload_file(
                str(local_path),
                self.bucket,
                s3_key,
                ExtraArgs={'ContentType': 'application/json'}
            )
            return True
        except Exception as e:
            print(f"   ✗ 上傳失敗 {s3_key}: {e}")
            return False

    def file_exists(self, s3_key: str) -> bool:
        """檢查 S3 檔案是否存在

        Args:
            s3_key: S3 物件 key

        Returns:
            bool: 是否存在
        """
        try:
            self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except self.ClientError:
            return False

    def sync_directory(self, local_dir: Path, s3_prefix: str, skip_existing: bool = True) -> dict:
        """同步本地目錄到 S3

        Args:
            local_dir: 本地目錄
            s3_prefix: S3 前綴
            skip_existing: 是否跳過已存在的檔案

        Returns:
            dict: 同步結果統計
        """
        stats = {'uploaded': 0, 'skipped': 0, 'failed': 0}

        if not local_dir.exists():
            return stats

        for local_file in local_dir.glob('**/*.json'):
            if local_file.name == 'latest.json':
                continue

            # 計算 S3 key
            rel_path = local_file.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{rel_path}"

            # 檢查是否已存在
            if skip_existing and self.file_exists(s3_key):
                stats['skipped'] += 1
                continue

            # 上傳
            if self.upload_file(local_file, s3_key):
                stats['uploaded'] += 1
            else:
                stats['failed'] += 1

        return stats

    def list_files(self, prefix: str, max_keys: int = 1000) -> list:
        """列出 S3 上的檔案

        Args:
            prefix: 前綴路徑
            max_keys: 最大數量

        Returns:
            list: 檔案資訊列表
        """
        files = []
        paginator = self.s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, PaginationConfig={'MaxItems': max_keys}):
            for obj in page.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'modified': obj['LastModified'].isoformat()
                })

        return files

    def get_file(self, s3_key: str) -> bytes:
        """取得 S3 檔案內容

        Args:
            s3_key: S3 物件 key

        Returns:
            bytes: 檔案內容
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            return response['Body'].read()
        except self.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise

    def get_json(self, s3_key: str) -> dict:
        """取得 S3 JSON 檔案並解析

        Args:
            s3_key: S3 物件 key

        Returns:
            dict: 解析後的 JSON 資料
        """
        content = self.get_file(s3_key)
        if content:
            return json.loads(content.decode('utf-8'))
        return None

    def get_latest(self, collector_name: str) -> dict:
        """取得最新資料"""
        return self.get_json(f"{collector_name}/latest.json")

    def list_dates(self, collector_name: str) -> list:
        """列出某個收集器有資料的日期

        Args:
            collector_name: 收集器名稱

        Returns:
            list: 日期列表 (YYYY-MM-DD 格式)
        """
        dates = set()
        prefix = f"{collector_name}/"

        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter='/'):
            for cp in page.get('CommonPrefixes', []):
                # 格式: collector/YYYY/
                year_prefix = cp['Prefix']
                # 繼續列出月份
                for month_page in paginator.paginate(Bucket=self.bucket, Prefix=year_prefix, Delimiter='/'):
                    for month_cp in month_page.get('CommonPrefixes', []):
                        month_prefix = month_cp['Prefix']
                        # 繼續列出日期
                        for day_page in paginator.paginate(Bucket=self.bucket, Prefix=month_prefix, Delimiter='/'):
                            for day_cp in day_page.get('CommonPrefixes', []):
                                # 格式: collector/YYYY/MM/DD/
                                parts = day_cp['Prefix'].rstrip('/').split('/')
                                if len(parts) >= 4:
                                    date_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
                                    dates.add(date_str)

        return sorted(dates, reverse=True)

    def list_files_by_date(self, collector_name: str, date: str) -> list:
        """列出某日期的所有檔案

        Args:
            collector_name: 收集器名稱
            date: 日期 (YYYY-MM-DD)

        Returns:
            list: 檔案列表
        """
        try:
            parsed_date = datetime.strptime(date, '%Y-%m-%d')
            prefix = f"{collector_name}/{parsed_date.strftime('%Y/%m/%d')}/"
        except ValueError:
            return []

        return self.list_files(prefix)
