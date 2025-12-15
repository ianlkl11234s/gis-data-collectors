"""
AWS S3 儲存

適用於生產環境，支援 AWS S3 和相容服務（如 MinIO）。
"""

import io
import json
from datetime import datetime

import config


class S3Storage:
    """AWS S3 儲存"""

    def __init__(self):
        if not config.S3_BUCKET:
            raise ValueError("S3_BUCKET 未設定")

        try:
            import boto3
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

    def save_append(self, collector_name: str, records: list, timestamp: datetime = None) -> str:
        """追加儲存到 S3（下載-追加-上傳）

        Args:
            collector_name: 收集器名稱
            records: 要追加的記錄列表
            timestamp: 時間戳記

        Returns:
            str: S3 物件 key
        """
        timestamp = timestamp or datetime.now()
        date_str = timestamp.strftime('%Y/%m/%d')
        filename = f"{collector_name}_{timestamp.strftime('%Y%m%d')}.jsonl"
        key = f"{collector_name}/{date_str}/{filename}"

        # 嘗試取得現有內容
        existing_content = ""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            existing_content = response['Body'].read().decode('utf-8')
        except self.s3.exceptions.NoSuchKey:
            pass
        except Exception:
            pass

        # 追加新記錄
        new_lines = '\n'.join(json.dumps(r, ensure_ascii=False) for r in records)
        if existing_content and not existing_content.endswith('\n'):
            existing_content += '\n'
        combined = existing_content + new_lines + '\n'

        # 上傳
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=combined.encode('utf-8'),
            ContentType='application/x-ndjson'
        )

        return key

    def get_latest(self, collector_name: str) -> dict:
        """取得最新資料"""
        try:
            key = f"{collector_name}/latest.json"
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception:
            return None
