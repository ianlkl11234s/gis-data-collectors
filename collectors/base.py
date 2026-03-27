"""
收集器基底類別

所有收集器都應繼承此類別。
"""

import gc
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import config
from storage import get_storage
from utils.notify import notify_error, notify_success


# Supabase writer 單例（所有 collector 共用同一條連線）
_supabase_writer = None


def get_supabase_writer():
    """取得 SupabaseWriter 單例"""
    global _supabase_writer
    if _supabase_writer is None and config.SUPABASE_ENABLED and config.SUPABASE_DB_URL:
        try:
            from storage.supabase_writer import SupabaseWriter
            _supabase_writer = SupabaseWriter(config.SUPABASE_DB_URL)
            print("✓ Supabase writer 已初始化")
        except Exception as e:
            print(f"✗ Supabase writer 初始化失敗: {e}")
    return _supabase_writer


class BaseCollector(ABC):
    """收集器基底類別"""

    # 子類別必須覆寫
    name: str = "base"
    interval_minutes: int = 60

    def __init__(self):
        self.storage = get_storage()
        self.supabase_writer = get_supabase_writer()
        self.last_run: Optional[datetime] = None
        self.run_count: int = 0
        self.error_count: int = 0
        self.consecutive_errors: int = 0

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

                # 旁路寫入 Supabase（不影響本地儲存流程）
                if self.supabase_writer:
                    try:
                        self.supabase_writer.write(self.name, result, timestamp)
                    except Exception as sb_err:
                        print(f"[{self.name}] ⚠ Supabase 寫入異常: {sb_err}")

            # 統計
            stats = {
                'timestamp': timestamp.isoformat(),
                'run_count': self.run_count,
                **{k: v for k, v in result.items() if k != 'data'}
            }

            self.last_run = timestamp
            self.consecutive_errors = 0  # 成功則重置連續錯誤
            notify_success(self.name, stats)

            return stats

        except Exception as e:
            self.error_count += 1
            self.consecutive_errors += 1
            error_msg = str(e)
            print(f"[{self.name}] ✗ 錯誤: {error_msg}")
            notify_error(self.name, error_msg, self.consecutive_errors)

            return {
                'timestamp': timestamp.isoformat(),
                'error': error_msg,
                'error_count': self.error_count
            }

        finally:
            # 每次收集後觸發 GC，避免記憶體累積
            gc.collect()

    def get_status(self) -> dict:
        """取得收集器狀態"""
        return {
            'name': self.name,
            'interval_minutes': self.interval_minutes,
            'run_count': self.run_count,
            'error_count': self.error_count,
            'consecutive_errors': self.consecutive_errors,
            'last_run': self.last_run.isoformat() if self.last_run else None,
        }
