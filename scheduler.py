"""
CollectorScheduler — 取代原本的單線程 schedule 執行方式

核心機制：
1. ThreadPoolExecutor：每個 collector 在獨立線程執行，互不阻塞
2. Skip-if-running：同一 collector 上一輪還沒跑完，新一輪 skip
3. 線程名標準化：便於 log 追蹤（thread name = "collector-{name}"）
4. Timeout 觀察：超過 COLLECT_TIMEOUT 記 warning（不強制中斷，避免資料不一致）

與 schedule 庫的關係：
- schedule 庫仍負責「何時觸發」（cron-like 時間排程）
- CollectorScheduler 負責「怎麼執行」（平行、保護、追蹤）
- 用法：schedule.every(N).minutes.do(scheduler.submit, collector)
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class CollectorScheduler:
    """Collector 執行調度器

    Usage:
        scheduler = CollectorScheduler(max_workers=10)
        scheduler.register(bus_collector)
        # 交給 schedule 庫觸發：
        import schedule
        schedule.every(2).minutes.do(scheduler.submit, bus_collector)
    """

    def __init__(self, max_workers: int = 10):
        """
        Args:
            max_workers: 同時最多幾個 collector 在跑。避免線程爆炸。
                         建議值 = 常用 collector 數量的 50%-80%
        """
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix='collector',
        )
        # name -> 當前 Future（若 None 或 done() 代表閒置）
        self._running: Dict[str, Future] = {}
        # name -> 最後一次 submit 的時間戳（用於觀察延遲）
        self._last_submit: Dict[str, float] = {}
        # name -> 統計：skip 次數
        self._skip_count: Dict[str, int] = {}
        # name -> collector instance（供 get_status 用）
        self._collectors: Dict[str, object] = {}
        self._lock = threading.Lock()

    def register(self, collector) -> None:
        """註冊 collector（供狀態查詢用，不會立即執行）"""
        self._collectors[collector.name] = collector
        self._skip_count.setdefault(collector.name, 0)

    def submit(self, collector) -> Optional[Future]:
        """提交 collector 到 thread pool

        若該 collector 上一輪還在跑，skip 本次並記錄 warning。
        這是避免同 collector 疊加執行、導致資料重複或 API 配額爆炸的保護機制。

        Returns:
            Future 物件（若 skip 則回傳 None）
        """
        name = collector.name

        # 確保已註冊（允許外部在啟動時忘記 register）
        if name not in self._collectors:
            self.register(collector)

        with self._lock:
            prev = self._running.get(name)

            # 上一輪還在跑 → skip
            if prev is not None and not prev.done():
                elapsed = time.time() - self._last_submit.get(name, 0)
                self._skip_count[name] = self._skip_count.get(name, 0) + 1
                logger.warning(
                    f"[scheduler] {name} 上一輪已跑 {elapsed:.1f}s 仍未結束，"
                    f"skip 本次（累計 skip {self._skip_count[name]} 次）"
                )
                return None

            # 上一輪跑完了 → 檢查有無未捕捉的 exception（防漏掉錯誤）
            if prev is not None and prev.done():
                exc = prev.exception()
                if exc is not None:
                    logger.error(
                        f"[scheduler] {name} 上一輪有未捕捉的 exception: {exc!r}"
                    )

            # 提交新任務
            future = self.executor.submit(self._safe_run, collector)
            self._running[name] = future
            self._last_submit[name] = time.time()
            return future

    def _safe_run(self, collector) -> Optional[dict]:
        """在 thread pool 內的執行包裝

        1. 設定線程名稱為 collector-{name}，方便 log 追蹤
        2. 捕捉所有 exception（避免污染 ThreadPoolExecutor 的內部狀態）
        3. 觀察超時（不強制中斷，只記 warning）
        """
        thread = threading.current_thread()
        # ThreadPoolExecutor 的 thread_name_prefix 只在初始化時設定
        # 這裡改用 collector 名稱，讓每次執行 log 都看得出是誰
        original_name = thread.name
        thread.name = f"collector-{collector.name}"

        timeout = getattr(collector, 'COLLECT_TIMEOUT', 300)
        start = time.time()
        try:
            result = collector.run()
            elapsed = time.time() - start
            if elapsed > timeout:
                logger.warning(
                    f"[{collector.name}] 執行耗時 {elapsed:.1f}s "
                    f"超過 COLLECT_TIMEOUT={timeout}s（僅告警，不中斷）"
                )
            return result
        except Exception as e:
            # BaseCollector.run() 已經做了一層 try/except
            # 這裡是「保險絲」，避免 ThreadPoolExecutor 內部因 exception 崩潰
            logger.exception(
                f"[{collector.name}] 意外的 uncaught exception（scheduler 層）: {e!r}"
            )
            return None
        finally:
            # 恢復線程名（pool 會 reuse thread，避免殘留名稱誤導下次 log）
            thread.name = original_name

    def get_status(self) -> dict:
        """取得調度器狀態（給 /health endpoint 或 daily report 用）"""
        status = {
            'max_workers': self.max_workers,
            'registered': len(self._collectors),
            'collectors': {},
        }
        with self._lock:
            for name in self._collectors:
                future = self._running.get(name)
                is_running = future is not None and not future.done()
                status['collectors'][name] = {
                    'running': is_running,
                    'last_submit': self._last_submit.get(name),
                    'skip_count': self._skip_count.get(name, 0),
                }
        return status

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        """關閉 thread pool

        Args:
            wait: 等所有 in-flight 任務跑完再關閉
            cancel_futures: 取消尚未開始的任務（僅對 pending 有效，已在跑的不會停）
        """
        try:
            self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        except TypeError:
            # Python < 3.9 沒有 cancel_futures 參數
            self.executor.shutdown(wait=wait)


# ============================================================
# 全域 scheduler 單例
# ============================================================

_scheduler: Optional[CollectorScheduler] = None


def get_scheduler(max_workers: int = 10) -> CollectorScheduler:
    """取得全域 scheduler 單例

    Args:
        max_workers: 第一次建立時的 worker 數上限。之後呼叫忽略此參數。
    """
    global _scheduler
    if _scheduler is None:
        _scheduler = CollectorScheduler(max_workers=max_workers)
    return _scheduler
