#!/usr/bin/env python3
"""
Data Collectors 主程式

統一管理所有資料收集器的排程執行，並提供 HTTP API 下載資料。
支援 S3 歸檔與資料生命週期管理。
"""

import logging
import signal
import sys
import time
import threading
from datetime import datetime
from types import SimpleNamespace

import schedule

import config
from scheduler import get_scheduler


def _as_task(name: str, fn, timeout: int = 300):
    """把 callable 包成「偽 collector」物件，丟進 CollectorScheduler.submit。

    這樣這些原本掛在主迴圈跑的 daily/buffer task 都跑在 worker thread，不會
    堵主迴圈，避免 watchdog 因主迴圈靜默 > 120s 誤殺整個進程。
    免費取得 skip-if-running、uncaught exception logging、超時 warning。
    """
    return SimpleNamespace(name=name, run=fn, COLLECT_TIMEOUT=timeout)
from collectors.registry import COLLECTOR_REGISTRY
from tasks import ArchiveTask, BackupSupabaseTask, DailyReportTask, MiniTaipeiPublishTask
from utils.notify import notify_archive_complete


def _init_collector_from_entry(entry, first: bool) -> "BaseCollector | None":
    """依 registry entry 初始化單一 collector，並沿用原本 main.py 的啟動訊息格式。

    Returns:
        collector 實例（成功時）或 None（停用、缺 key、初始化失敗）
    """
    prefix = entry.config_prefix
    display = entry.display_name
    enabled = getattr(config, f"{prefix}_ENABLED", False)
    lead = "\n" if first else ""

    if not enabled:
        print(f"{lead}⏸️  {display}已停用 ({prefix}_ENABLED=false)")
        return None

    missing = [k for k in entry.required_env if not getattr(config, k, None)]
    if missing:
        # 目前所有 entry 只會有 1 個 required_env，但保留逗號串接以利擴充
        print(f"{lead}⚠️  {', '.join(missing)} 未設定，跳過 {display}")
        return None

    try:
        c = entry.cls()
        print(f"{lead}✓ {display} (每 {c.interval_minutes} 分鐘)")
        return c
    except Exception as e:
        print(f"{lead}✗ {display}初始化失敗: {e}")
        return None


def run_collectors():
    """依 COLLECTOR_REGISTRY 初始化所有 collector，並交給 CollectorScheduler 排程"""
    collectors = []
    for idx, entry in enumerate(COLLECTOR_REGISTRY):
        c = _init_collector_from_entry(entry, first=(idx == 0))
        if c is not None:
            collectors.append(c)

    if not collectors:
        print("\n❌ 沒有可用的收集器")
        return []

    # ============================================================
    # 統一透過 CollectorScheduler 調度（Phase 1 升級）
    # 每個 collector 在獨立線程執行，互不阻塞
    # Skip-if-running 保護避免同 collector 疊加
    # ============================================================
    # max_workers 預設為 collector 數量 + 緩衝（避免所有 collector 都撞同一 tick 時排隊）
    # +5 留給 5 個 task（archive/backup/daily_report/mini_taipei/sb_flush）共用 pool
    max_workers = max(10, len(collectors) + 7)
    sched = get_scheduler(max_workers=max_workers)

    print("\n" + "=" * 60)
    print(f"🚀 初始執行（共 {len(collectors)} 個 collector，pool max_workers={max_workers}）")
    print("=" * 60)

    # 註冊並立即提交一次（異步執行，不阻塞啟動流程）
    for collector in collectors:
        sched.register(collector)
        sched.submit(collector)

    # 設定排程（schedule 庫只負責觸發，實際執行交給 CollectorScheduler）
    for collector in collectors:
        schedule.every(collector.interval_minutes).minutes.do(sched.submit, collector)

    # 顯示下次執行時間
    next_run = schedule.next_run()
    if next_run:
        print(f"\n⏰ 下次排程觸發: {next_run.strftime('%H:%M:%S')}")

    return collectors


def run_api_server_thread():
    """在背景執行 API Server"""
    if not config.API_KEY:
        print("\n⚠️  API_KEY 未設定，API Server 不會啟動")
        print("   設定 API_KEY 環境變數以啟用 HTTP API")
        return None

    from api import run_api_server

    # 在背景執行 Flask
    def start_server():
        # 使用 werkzeug 內建 server，關閉 reloader 以避免多執行緒問題
        from api.server import create_app
        app = create_app()

        # 關閉 Flask 的輸出
        import logging
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.WARNING)

        print(f"\n{'=' * 60}")
        print(f"🌐 API Server 已啟動")
        print(f"{'=' * 60}")
        print(f"   URL: http://0.0.0.0:{config.API_PORT}")
        print(f"   認證: X-API-Key header 或 api_key 參數")
        print(f"{'=' * 60}")

        app.run(
            host='0.0.0.0',
            port=config.API_PORT,
            threaded=True,
            use_reloader=False
        )

    thread = threading.Thread(target=start_server, daemon=True)
    thread.start()
    return thread


def run_archive_task(daily_report_task=None):
    """設定歸檔任務排程"""
    if not config.ARCHIVE_ENABLED:
        print("\n⚠️  歸檔功能已停用 (ARCHIVE_ENABLED=false)")
        return None

    if not config.S3_BUCKET:
        print("\n⚠️  S3_BUCKET 未設定，歸檔功能停用")
        return None

    try:
        archive_task = ArchiveTask()
        print(f"\n✓ 歸檔任務已設定 (每日 {config.ARCHIVE_TIME})")

        # 包裝歸檔任務，加入 Telegram 通知和結果記錄
        def archive_with_notify():
            result = archive_task.run()
            if result:
                notify_archive_complete(result)
                # 將結果傳給每日報告
                if daily_report_task:
                    daily_report_task.last_archive_result = result
            return result

        # 設定每日排程（worker thread，不堵主迴圈避免觸發 watchdog）
        sched = get_scheduler()
        schedule.every().day.at(config.ARCHIVE_TIME).do(
            sched.submit, _as_task("archive", archive_with_notify, timeout=900))

        return archive_task
    except Exception as e:
        print(f"\n✗ 歸檔任務初始化失敗: {e}")
        return None


def run_daily_report_task(collectors: list, archive_task=None):
    """設定每日報告排程"""
    if not config.DAILY_REPORT_ENABLED:
        print("\n⚠️  每日報告已停用 (DAILY_REPORT_ENABLED=false)")
        return None

    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("\n⚠️  TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 未設定，每日報告停用")
        return None

    try:
        daily_report = DailyReportTask(collectors, archive_task)
        sched = get_scheduler()
        job = schedule.every().day.at(config.DAILY_REPORT_TIME).do(
            sched.submit, _as_task("daily_report", daily_report.run, timeout=600))
        print(f"✓ 每日報告已設定 (每日 {config.DAILY_REPORT_TIME})")
        print(f"   下次觸發: {job.next_run}")
        print(f"   收集器數量: {len(collectors)}")

        return daily_report
    except Exception as e:
        print(f"\n✗ 每日報告初始化失敗: {e}")
        import traceback
        traceback.print_exc()
        return None


def run_mini_taipei_publish_task():
    """設定 Mini Taipei 每日時刻表發布任務"""
    if not getattr(config, 'MINI_TAIPEI_PUBLISH_ENABLED', False):
        print("\n⏸️  Mini Taipei 發布已停用 (MINI_TAIPEI_PUBLISH_ENABLED=false)")
        return None

    if not config.S3_BUCKET:
        print("\n⚠️  S3_BUCKET 未設定，Mini Taipei 發布功能停用")
        return None

    try:
        publish_task = MiniTaipeiPublishTask()
        publish_time = getattr(config, 'MINI_TAIPEI_PUBLISH_TIME', '07:00')
        print(f"\n✓ Mini Taipei 發布任務已設定 (每日 {publish_time})")

        sched = get_scheduler()
        schedule.every().day.at(publish_time).do(
            sched.submit, _as_task("mini_taipei_publish", publish_task.run, timeout=180))

        return publish_task
    except Exception as e:
        print(f"\n✗ Mini Taipei 發布任務初始化失敗: {e}")
        return None


def run_backup_task():
    """設定 Supabase → S3 備份任務排程（每日 03:30 UTC，archive.py 之後執行）"""
    if not config.BACKUP_ENABLED:
        print("\n⏸️  Supabase 備份已停用 (BACKUP_ENABLED=false)")
        return None

    if not config.S3_BUCKET:
        print("\n⚠️  S3_BUCKET 未設定，Supabase 備份功能停用")
        return None

    try:
        backup_task = BackupSupabaseTask()
        sched = get_scheduler()
        schedule.every().day.at("03:30").do(
            sched.submit, _as_task("backup_supabase", backup_task.run, timeout=1200))
        print("\n✓ Supabase 備份任務已設定 (每日 03:30 UTC)")
        return backup_task
    except Exception as e:
        print(f"\n✗ Supabase 備份任務初始化失敗: {e}")
        return None


def _setup_logging():
    """初始化 logging（讓 scheduler 與其他模組的 logger 輸出）"""
    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(threadName)s] %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )
    # werkzeug 預設 INFO 太吵
    logging.getLogger('werkzeug').setLevel(logging.WARNING)


def main():
    """主程式"""
    _setup_logging()

    print("=" * 60)
    print("📡 Data Collectors")
    print("=" * 60)

    # 驗證設定
    if not config.validate_config():
        sys.exit(1)

    config.print_config()

    # 啟動 API Server（背景執行緒）
    api_thread = run_api_server_thread()

    # 啟動收集器
    collectors = run_collectors()

    # 設定每日報告（先建立，讓歸檔任務可以回傳結果）
    daily_report_task = run_daily_report_task(collectors)

    # 設定歸檔任務（傳入 daily_report_task 以記錄結果）
    archive_task = run_archive_task(daily_report_task)

    # 設定 Mini Taipei 發布任務
    run_mini_taipei_publish_task()

    # 設定 Supabase → S3 備份任務
    run_backup_task()

    # Supabase buffer flush 排程
    if config.SUPABASE_ENABLED and config.SUPABASE_DB_URL:
        from collectors.base import get_supabase_writer
        sb_writer = get_supabase_writer()
        if sb_writer:
            sched = get_scheduler()
            schedule.every(config.SUPABASE_BUFFER_INTERVAL).minutes.do(
                sched.submit, _as_task("sb_flush", sb_writer.flush_buffer, timeout=60))
            print(f"\n✓ Supabase buffer flush (每 {config.SUPABASE_BUFFER_INTERVAL} 分鐘)")

    # 將歸檔任務回傳給每日報告
    if daily_report_task and archive_task:
        daily_report_task.archive_task = archive_task

    if not collectors:
        # 如果沒有收集器但有 API，繼續執行
        if not api_thread:
            sys.exit(1)
        print("\n⚠️  沒有收集器，僅執行 API Server")

    # 設定 graceful shutdown
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        print(f"\n\n🛑 收到停止信號，正在結束...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("\n" + "=" * 60)
    print("📡 等待排程執行... (按 Ctrl+C 停止)")
    print("=" * 60)

    # 主迴圈 + watchdog
    import health
    import logging as _logging
    health.heartbeat()  # 啟動即先記一次，避免 watchdog 在第一輪前誤判

    if config.HEALTH_WATCHDOG_ENABLED:
        def _on_hang(since):
            msg = (f"主迴圈 {since:.0f}s 無心跳（>{config.HEALTH_MAX_LOOP_SILENCE}s），"
                   f"watchdog 強制重啟進程")
            _logging.error(msg)
            try:
                from utils.notify import send_telegram, _instance_tag
                send_telegram(f"🔁 *Watchdog 重啟進程*{_instance_tag()}\n\n{msg}")
            except Exception:
                pass
        health.start_watchdog(config.HEALTH_MAX_LOOP_SILENCE, on_trigger=_on_hang)
        print(f"\n✓ Watchdog 啟用（主迴圈靜默 > {config.HEALTH_MAX_LOOP_SILENCE}s 自動重啟）")

    # 自我測試開關（staging 驗證用）：啟動 90s 後停止心跳，模擬主迴圈卡死
    # → watchdog 應在 HEALTH_MAX_LOOP_SILENCE 後 os._exit → 觀察平台是否重啟。測完移除 env。
    import os as _os
    _selftest = _os.getenv('WATCHDOG_SELFTEST', '').lower() in ('true', '1', 'yes')
    _loop_start = time.monotonic()
    _selftest_announced = False

    while running:
        if _selftest and time.monotonic() - _loop_start > 90:
            if not _selftest_announced:
                print("🧪 WATCHDOG_SELFTEST：停止心跳，模擬主迴圈卡死…")
                _selftest_announced = True
            # 故意不打心跳
        else:
            health.heartbeat()  # watchdog 心跳：主迴圈卡住 → watchdog 自殺 → 平台重啟
        schedule.run_pending()
        time.sleep(1)

    # 結束
    if collectors:
        print("\n📊 執行統計:")
        for collector in collectors:
            status = collector.get_status()
            print(f"   [{status['name']}] "
                  f"執行 {status['run_count']} 次 | "
                  f"錯誤 {status['error_count']} 次")

    print("\n👋 已停止")


if __name__ == '__main__':
    main()
