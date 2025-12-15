"""
收集器基底類別

所有收集器都應繼承此類別。
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import config
from storage import get_storage
from utils.notify import notify_error, notify_success


class BaseCollector(ABC):
    """收集器基底類別"""

    # 子類別必須覆寫
    name: str = "base"
    interval_minutes: int = 60

    def __init__(self):
        self.storage = get_storage()
        self.last_run: Optional[datetime] = None
        self.run_count: int = 0
        self.error_count: int = 0

    @abstractmethod
    def collect(self) -> dict:
        """執行資料收集

        Returns:
            dict: 收集結果，包含 data 和統計資訊
        """
        pass

    def run(self) -> dict:
        """執行收集並儲存"""
        self.run_count += 1
        timestamp = datetime.now()

        print(f"\n[{self.name}] 開始收集 ({timestamp.strftime('%H:%M:%S')})")

        try:
            # 執行收集
            result = self.collect()

            # 儲存資料
            if 'data' in result:
                filepath = self.storage.save(self.name, result, timestamp)
                print(f"[{self.name}] ✓ 已儲存: {filepath}")

            # 統計
            stats = {
                'timestamp': timestamp.isoformat(),
                'run_count': self.run_count,
                **{k: v for k, v in result.items() if k != 'data'}
            }

            self.last_run = timestamp
            notify_success(self.name, stats)

            return stats

        except Exception as e:
            self.error_count += 1
            error_msg = str(e)
            print(f"[{self.name}] ✗ 錯誤: {error_msg}")
            notify_error(self.name, error_msg)

            return {
                'timestamp': timestamp.isoformat(),
                'error': error_msg,
                'error_count': self.error_count
            }

    def get_status(self) -> dict:
        """取得收集器狀態"""
        return {
            'name': self.name,
            'interval_minutes': self.interval_minutes,
            'run_count': self.run_count,
            'error_count': self.error_count,
            'last_run': self.last_run.isoformat() if self.last_run else None,
        }
