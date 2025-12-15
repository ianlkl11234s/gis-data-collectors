"""儲存後端模組"""

from .local import LocalStorage
from .s3 import S3Storage

__all__ = ['LocalStorage', 'S3Storage', 'get_storage']


def get_storage():
    """取得儲存後端（自動選擇）"""
    import config

    if config.S3_BUCKET:
        return S3Storage()
    else:
        return LocalStorage()
