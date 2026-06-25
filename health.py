"""
進程存活心跳（watchdog 用）

主迴圈每輪呼叫 heartbeat()，/health 端點據此判斷主迴圈是否還活著。
2026-06-24 起：解決「主迴圈卡死、進程靜默 14 小時、/health 仍回 healthy 不被重啟」的事故。
"""

import os
import threading
import time

_last_main_tick: float | None = None  # monotonic


def heartbeat() -> None:
    """主迴圈每輪呼叫一次，記錄存活時間戳。"""
    global _last_main_tick
    _last_main_tick = time.monotonic()


def seconds_since_heartbeat() -> float | None:
    """距上次 heartbeat 幾秒；None 表示主迴圈尚未開始。"""
    if _last_main_tick is None:
        return None
    return time.monotonic() - _last_main_tick


def start_watchdog(max_silence: int, check_interval: int = 30,
                   on_trigger=None, exit_fn=None) -> threading.Thread:
    """背景 watchdog：心跳超過 max_silence 秒 → exit_fn(1) 讓進程崩潰 → 平台重啟。

    daemon thread；典型卡死（網路 I/O / 鎖等待）會釋放 GIL，故 watchdog 仍能運行。
    exit_fn 預設 os._exit（硬退出、不跑 cleanup，因卡死時 graceful shutdown 不可信）。
    """
    _exit = exit_fn or os._exit

    def _loop():
        while True:
            time.sleep(check_interval)
            since = seconds_since_heartbeat()
            if since is not None and since > max_silence:
                if on_trigger:
                    try:
                        on_trigger(since)
                    except Exception:
                        pass
                _exit(1)
                return

    t = threading.Thread(target=_loop, daemon=True, name="watchdog")
    t.start()
    return t
