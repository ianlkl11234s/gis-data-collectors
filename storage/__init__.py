"""儲存後端模組"""

from .local import LocalStorage
from .s3 import S3Storage

__all__ = ['LocalStorage', 'S3Storage', 'get_storage']

# SupabaseWriter 在 collectors/base.py 中延遲載入，不在此匯入


def get_storage():
    """取得儲存後端

    收集器永遠使用 LocalStorage 寫入本地，
    S3 歸檔由 ArchiveTask 以 tar.gz 批次上傳。
    """
    return LocalStorage()
