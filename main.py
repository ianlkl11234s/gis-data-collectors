#!/usr/bin/env python3
"""
Data Collectors 主程式

統一管理所有資料收集器的排程執行，並提供 HTTP API 下載資料。
支援 S3 歸檔與資料生命週期管理。
"""

import logging
import random
import signal
import sys
import time
import threading
from datetime import datetime, timedelta
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
from tasks import (
    ArchiveTask,
    BackupSupabaseTask,
    DailyReportTask,
    GFWHourlyPublishTask,
    MiniTaipeiPublishTask,
)
from utils.notify import notify_archive_complete, notify_trails_export


def _init_collector_from_entry(entry, first: bool) -> "BaseCollector | None":
    """依 registry entry 初始化單一 collector，並沿用原本 main.py 的啟動訊息格式。

    Returns:
        collector 實例（成功時）或 None（停用、缺 key、初始化失敗）
    """
    # persistent provider 由自己的 worker 啟動，不應建立一個會被 interval
    # scheduler 重複觸發的 shim collector。
    if getattr(entry, 'persistent', False):
        return None
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
    # +6 留給 6 個 task（archive/backup/daily_report/mini_taipei/sb_flush/trails_export）共用 pool
    max_workers = max(10, len(collectors) + 8)
    sched = get_scheduler(max_workers=max_workers)

    print("\n" + "=" * 60)
    print(f"🚀 初始執行（共 {len(collectors)} 個 collector，pool max_workers={max_workers}）")
    print("=" * 60)

    # 註冊並立即提交一次（異步執行）。每個 collector 間隔 1-3 秒 submit，
    # 錯開首輪全量齊發時的 DB 連線尖峰（曾釀 pool borrow timeout，見 config.py 註解）
    for idx, collector in enumerate(collectors):
        if idx > 0:
            time.sleep(random.uniform(1.0, 3.0))
        sched.register(collector)
        sched.submit(collector)

    # 設定排程（schedule 庫只負責觸發，實際執行交給 CollectorScheduler）。
    # 對 next_run 加一次性隨機 offset（0 ~ min(interval, 5 分鐘)），錯開同 interval
    # collector 全撞同一 tick 的連線尖峰；schedule 之後以實際執行時間推下一輪，
    # offset 自然保留，interval 語義不變。
    for collector in collectors:
        job = schedule.every(collector.interval_minutes).minutes.do(sched.submit, collector)
        offset_sec = random.uniform(0, min(collector.interval_minutes * 60, 300))
        job.next_run += timedelta(seconds=offset_sec)
        print(f"   ⏱ [{collector.name}] 排程 jitter +{offset_sec:.0f}s"
              f"（首次觸發 {job.next_run.strftime('%H:%M:%S')}）")

    # 顯示下次執行時間
    next_run = schedule.next_run()
    if next_run:
        print(f"\n⏰ 下次排程觸發: {next_run.strftime('%H:%M:%S')}")

    return collectors


def run_aisstream_worker():
    """啟動 AISStream 長駐 worker；預設 disabled，與排程 collectors 分離。"""
    if not getattr(config, 'AISSTREAM_ENABLED', False):
        print("\n⏸️  AISStream 長駐收集器已停用 (AISSTREAM_ENABLED=false)")
        return None
    if not config.AISSTREAM_API_KEY:
        print("\n⚠️  AISSTREAM_API_KEY 未設定，跳過 AISStream 長駐收集器")
        return None
    try:
        from workers.aisstream import AISStreamWorker

        worker = AISStreamWorker()
        # DB run ledger and S3 cold archive are hard gates.  Prepare on the
        # main thread so a migration/credential failure cannot kill only the
        # daemon thread while Zeabur still looks healthy.
        worker.prepare()
        thread = threading.Thread(target=worker.run, daemon=True, name='aisstream-worker')
        thread.start()
        print(f"\n✓ AISStream 長駐收集器已啟動（{len(worker.boxes)} 個 bbox，campaign {config.AISSTREAM_CAMPAIGN_DAYS} 天）")
        return worker
    except Exception as exc:
        print(f"\n✗ AISStream 長駐收集器初始化失敗: {exc}")
        raise


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


def run_trails_export_task():
    """設定每日軌跡凍結匯出排程（AR-14）

    Supabase 的 live.*_trails_daily 是滾動視窗（bus 3 天 / ship 7 天 / flight 7~9 天），
    每天把「昨天」凍結成 S3 靜態檔才留得住歷史。時間預設 02:00 為容器本地時間
    （Dockerfile 已設 TZ=Asia/Taipei）—— summary 表 refreshed_at 顯示每日資料
    01:00~01:20 才定版，早於此會匯出到半成品。
    """
    if not config.TRAILS_EXPORT_ENABLED:
        print("\n⏸️  軌跡凍結匯出已停用 (TRAILS_EXPORT_ENABLED=false)")
        return None
    if not config.S3_BUCKET:
        print("\n⚠️  S3_BUCKET 未設定，軌跡凍結匯出停用")
        return None

    if not config.SUPABASE_DB_URL:
        print("\n⚠️  SUPABASE_DB_URL 未設定，軌跡凍結匯出停用")
        return None

    try:
        from scripts.export_daily_trails import export_range

        def export_with_notify():
            try:
                # 不帶 end_date → 在「執行當下」才解析昨天。排程是啟動時註冊的，
                # 這裡若把日期算死，之後每晚都會重匯同一天。
                stats = export_range()
            except Exception as e:
                # 整個任務中止（DB 連不上／設定缺漏）也要走同一條 Telegram 告警路徑；
                # 靜默失敗到隔天才發現的話，bus 只有 3 天保留期，來不及補。
                notify_trails_export({
                    'ok': 0, 'failed': 1, 'rows': 0, 'bytes': 0,
                    'dates': [], 'failures': [f'任務中止: {e}'],
                })
                raise
            notify_trails_export(stats)
            return stats

        # 全 4 個 dataset 單日約 100MB / 3 分鐘（本機實測），timeout 抓 1200s 留餘裕
        sched = get_scheduler()
        schedule.every().day.at(config.TRAILS_EXPORT_TIME).do(
            sched.submit, _as_task("trails_export", export_with_notify, timeout=1200))
        print(f"\n✓ 軌跡凍結匯出已設定 (每日 {config.TRAILS_EXPORT_TIME} Asia/Taipei)")
        return True
    except Exception as e:
        print(f"\n✗ 軌跡凍結匯出初始化失敗: {e}")
        return None


def run_gfw_hourly_publish_task():
    """Set the fixed-time unified GFW hourly release job without an immediate run."""
    if not getattr(config, 'GFW_HOURLY_PUBLISH_ENABLED', False):
        print("\n⏸️  GFW hourly unified publish 已停用 (GFW_HOURLY_PUBLISH_ENABLED=false)")
        return None
    if not getattr(config, 'GFW_HOURLY_REDISTRIBUTION_APPROVED', False):
        print("\n⚠️  GFW hourly publish 缺 redistribution approval，停用")
        return None
    try:
        task = GFWHourlyPublishTask()
        publish_time = config.GFW_HOURLY_PUBLISH_TIME
        sched = get_scheduler()
        schedule.every().day.at(publish_time).do(
            sched.submit,
            _as_task("gfw_hourly_publish", task.run, timeout=7200),
        )
        print(
            f"\n✓ GFW hourly unified publish 已設定 "
            f"(每日 {publish_time} Asia/Taipei；不在 deploy 時立即執行)"
        )
        return task
    except Exception as exc:
        print(f"\n✗ GFW hourly unified publish 初始化失敗: {exc}")
        raise


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

    # AISStream 是長駐 WebSocket，刻意不掛入 interval scheduler，避免被當成 polling job。
    aisstream_worker = run_aisstream_worker()

    # 設定每日報告（先建立，讓歸檔任務可以回傳結果）
    daily_report_task = run_daily_report_task(collectors)

    # 設定歸檔任務（傳入 daily_report_task 以記錄結果）
    archive_task = run_archive_task(daily_report_task)

    # 設定 Mini Taipei 發布任務
    run_mini_taipei_publish_task()

    # 設定 Supabase → S3 備份任務
    run_backup_task()

    # 設定每日軌跡凍結匯出任務
    run_trails_export_task()

    # GFW AIS grid/tracks + SAR unmatched 共用單一 root manifest；僅固定每日排程。
    gfw_hourly_task = run_gfw_hourly_publish_task()

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
        if not api_thread and not aisstream_worker and not gfw_hourly_task:
            sys.exit(1)
        if aisstream_worker:
            print("\n⚠️  沒有排程型收集器，僅執行 AISStream 長駐 worker" + (" + API Server" if api_thread else ""))
        else:
            print("\n⚠️  沒有收集器，僅執行 API Server")

    # 設定 graceful shutdown
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        print(f"\n\n🛑 收到停止信號，正在結束...")
        if aisstream_worker:
            aisstream_worker.stop()
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
