"""
Cloudflare R2 影像儲存（CDN 雙寫）

AR-11 read-path-cdn：影像 frame 除了寫 DB bytea，另雙寫一份到 R2，
前端改吃 CDN URL（DB egress 歸零 + edge cache 共享）。

刻意與 storage/s3.py 的 S3Storage 分離、不共用建構子，避免影響既有 AWS S3
歸檔路徑。R2 走 S3 相容 API（boto3 + endpoint_url）。

憑證走 config.R2_*（4 個），任一未設 → get_r2_storage() 回 None，
呼叫端據此跳過上傳（best-effort，不因 R2 壞掉丟資料）。
"""

import config

# 同一 (dataset, observed_at) 的影像永不變 → 可 immutable 長快取
IMMUTABLE_CACHE_CONTROL = 'public, max-age=31536000, immutable'


def r2_enabled() -> bool:
    """4 個憑證都齊全才算啟用。"""
    return bool(
        config.R2_ACCESS_KEY_ID
        and config.R2_SECRET_ACCESS_KEY
        and config.R2_ENDPOINT_URL
        and config.R2_BUCKET
    )


class R2Storage:
    """Cloudflare R2 物件儲存（S3 相容，boto3）。"""

    def __init__(self):
        if not r2_enabled():
            raise ValueError(
                "R2 憑證未設定（需 R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / "
                "R2_ENDPOINT_URL / R2_BUCKET）"
            )
        import boto3

        self.bucket = config.R2_BUCKET
        self.s3 = boto3.client(
            's3',
            endpoint_url=config.R2_ENDPOINT_URL,
            aws_access_key_id=config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=config.R2_SECRET_ACCESS_KEY,
            region_name='auto',
        )

    def upload_image(
        self,
        key: str,
        data: bytes,
        content_type: str,
        cache_control: str = IMMUTABLE_CACHE_CONTROL,
    ) -> None:
        """上傳影像 bytes（帶 ContentType + Cache-Control）。失敗會 raise。"""
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl=cache_control,
        )


def get_r2_storage():
    """回傳 R2Storage 實例；憑證未設回 None（呼叫端跳過上傳）。"""
    if not r2_enabled():
        return None
    return R2Storage()
