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

import schedule

import config
from scheduler import get_scheduler
from collectors.registry import COLLECTOR_REGISTRY
from tasks import ArchiveTask, DailyReportTask, MiniTaipeiPublishTask
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
    max_workers = max(10, len(collectors) + 2)
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

        # 設定每日排程
        schedule.every().day.at(config.ARCHIVE_TIME).do(archive_with_notify)

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
        job = schedule.every().day.at(config.DAILY_REPORT_TIME).do(daily_report.run)
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

        schedule.every().day.at(publish_time).do(publish_task.run)

        return publish_task
    except Exception as e:
        print(f"\n✗ Mini Taipei 發布任務初始化失敗: {e}")
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

    # Supabase buffer flush 排程
    if config.SUPABASE_ENABLED and config.SUPABASE_DB_URL:
        from collectors.base import get_supabase_writer
        sb_writer = get_supabase_writer()
        if sb_writer:
            schedule.every(config.SUPABASE_BUFFER_INTERVAL).minutes.do(sb_writer.flush_buffer)
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

    # 主迴圈
    while running:
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
