#!/usr/bin/env python3
"""
Data Collectors 主程式

統一管理所有資料收集器的排程執行，並提供 HTTP API 下載資料。
支援 S3 歸檔與資料生命週期管理。
"""

import os

# pyarrow 預設用自帶 jemalloc 池，不受 compose 的 MALLOC_* 控制；改用系統 malloc。
# 必須在任何模組 import pyarrow 之前設定（子行程繼承環境變數）。compose 有設時以 compose 為準。
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

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


def _as_task(name: str, fn, timeout: int = 300, heavy: bool = False):
    """把 callable 包成「偽 collector」物件，丟進 CollectorScheduler.submit。

    這樣這些原本掛在主迴圈跑的 daily/buffer task 都跑在 worker thread，不會
    堵主迴圈，避免 watchdog 因主迴圈靜默 > 120s 誤殺整個進程。
    免費取得 skip-if-running、uncaught exception logging、超時 warning。
    heavy=True → 走 scheduler 的「重任務道」，同時只跑一個（排隊等，不 skip）。
    """
    return SimpleNamespace(name=name, run=fn, COLLECT_TIMEOUT=timeout, HEAVY_TASK=heavy)
from collectors.registry import COLLECTOR_REGISTRY
from tasks import (
    ArchiveTask,
    DailyReportTask,
    GFWHourlyPublishTask,
    MiniTaipeiPublishTask,
)
from tasks.gfw_hourly_publish import run_in_subprocess as run_gfw_hourly_publish_in_subprocess
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


def _schedule_collector(collector, sched) -> None:
    """註冊 collector 的週期觸發。

    預設 every(interval) + 一次性 jitter；collector 有 fixed_minutes 時改成每小時
    固定分鐘觸發（例如海象避開 DB cron 的 :14/:15/:40，見
    docs/investigations/2026-10-12-marine-write-stall.md）。
    """
    fixed = tuple(getattr(collector, 'fixed_minutes', ()) or ())
    if fixed:
        for minute in fixed:
            schedule.every().hour.at(f":{int(minute):02d}").do(sched.submit, collector)
        print(f"   ⏱ [{collector.name}] 固定分鐘 {','.join(f':{int(m):02d}' for m in fixed)}")
        return
    job = schedule.every(collector.interval_minutes).minutes.do(sched.submit, collector)
    offset_sec = random.uniform(0, min(collector.interval_minutes * 60, 300))
    job.next_run += timedelta(seconds=offset_sec)
    print(f"   ⏱ [{collector.name}] 排程 jitter +{offset_sec:.0f}s"
          f"（首次觸發 {job.next_run.strftime('%H:%M:%S')}）")


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
    # max_workers：ThreadPoolExecutor 依需求長執行緒，每條執行緒都可能綁一個 glibc
    # arena 留下碎片，所以設上限。舊公式 max(10, N+8) 在 83 個 collector 時是 91，
    # 但 skip-if-running 讓每個 collector 同時最多佔 1 條，實際同時在跑的遠少於 N
    # （舊主機 10 天後整個行程只有 44 條執行緒，含 Flask／長駐 worker／內層 pool）。
    # 上限 32 的計算：≤5 分鐘的高頻 collector 23 個（registry 全數，假設全開且同時在跑）
    # ＋ 3 個長時 collector（jma_raster 3600s、ocean_env／global_events 1800s）
    # ＋ 7 個 task 槽（trails/archive/backup/gfw 四個重任務排隊時各佔一條，
    #   加 daily_report/mini_taipei/sb_flush）＝ 33，取 32；collector 少時仍用 N+8。
    # pool 滿時新工作排隊（不丟），scheduler 會記「排隊 Ns 才開始」warning，
    # get_status()['pending'] 可看排隊數；需要時用 COLLECTOR_POOL_MAX_WORKERS 覆寫。
    max_workers = config.COLLECTOR_POOL_MAX_WORKERS or min(max(10, len(collectors) + 8), 32)
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
        _schedule_collector(collector, sched)

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


def run_ripe_ris_live_worker():
    """Start the bounded RIPE RIS Live worker outside the interval scheduler."""
    if not getattr(config, 'RIPE_RIS_LIVE_ENABLED', False):
        print("\n⏸️  RIPE RIS Live 長駐收集器已停用 (RIPE_RIS_LIVE_ENABLED=false)")
        return None
    try:
        from workers.ripe_ris_live import RipeRisLiveWorker

        worker = RipeRisLiveWorker()
        # Roster review, replicas=1, process lock, DB and private S3 are hard
        # startup gates.  Fail on the main thread instead of leaving a dead
        # daemon while the container health endpoint still looks alive.
        worker.prepare()
        thread = threading.Thread(target=worker.run, daemon=True, name='ripe-ris-live-worker')
        thread.start()
        print(f"\n✓ RIPE RIS Live 長駐收集器已啟動（{len(worker._subscriptions)} bounded subscriptions）")
        return worker
    except Exception as exc:
        print(f"\n✗ RIPE RIS Live 長駐收集器初始化失敗: {exc}")
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
        print(f"   認證: X-API-Key header")
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
            sched.submit, _as_task("archive", archive_with_notify, timeout=900, heavy=True))

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


def run_db_efficiency_audit_task():
    """每週一 DB 效能體檢（規則見 docs/handbook/operations.md §8）；結果由當天日報讀檔併入。"""
    if not (config.DB_AUDIT_ENABLED and config.SUPABASE_ENABLED and config.SUPABASE_DB_URL):
        print("\n⏸️  DB 效能體檢停用（DB_AUDIT_ENABLED=false 或 Supabase 未設定）")
        return
    from tasks import db_efficiency_audit
    sched = get_scheduler()
    schedule.every().monday.at(config.DB_AUDIT_TIME).do(
        sched.submit, _as_task("db_efficiency_audit", db_efficiency_audit.run, timeout=120))
    print(f"\n✓ DB 效能體檢 (每週一 {config.DB_AUDIT_TIME}，結果併進當天日報)")


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
    """設定 Supabase → S3 備份任務排程（每日 03:30 容器時間＝台北，Dockerfile TZ=Asia/Taipei；archive.py 之後執行）"""
    if not config.BACKUP_ENABLED:
        print("\n⏸️  Supabase 備份已停用 (BACKUP_ENABLED=false)")
        return None

    if not config.S3_BUCKET:
        print("\n⚠️  S3_BUCKET 未設定，Supabase 備份功能停用")
        return None

    try:
        from tasks import backup_watch
        from utils.isolated_task import run_isolated

        def backup_in_subprocess():
            # 子行程：COPY／gzip 尖峰不墊高主程序 RSS；每次都在子行程內新建 DB／S3 連線。
            # 成敗由 backup_watch 判斷並記錄（DB／S3 沒連上也算失敗），失敗發 Telegram。
            backup_watch.mark_started()
            try:
                result = run_isolated(
                    "backup_supabase", "tasks.backup_supabase:run_backup_once",
                    timeout=config.BACKUP_KILL_TIMEOUT_SEC)
            except Exception as exc:
                backup_watch.record_run(error=f"{type(exc).__name__}: {exc}")
                raise
            backup_watch.record_run(result)
            return result

        sched = get_scheduler()
        schedule.every().day.at("03:30").do(
            sched.submit,
            _as_task("backup_supabase", backup_in_subprocess, timeout=1200, heavy=True))
        # 「今天沒成功」檢查：沒跑、卡在重任務佇列、或失敗都會在這個時間告警（同日只發一次）
        schedule.every().day.at(config.BACKUP_WATCHDOG_TIME).do(
            sched.submit, _as_task("backup_watchdog", backup_watch.check_today, timeout=120))
        print(f"\n✓ Supabase 備份任務已設定 (每日 03:30 台北，子行程；"
              f"{config.BACKUP_WATCHDOG_TIME} 檢查今日是否成功)")
        return True
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
        from utils.isolated_task import run_isolated

        def export_with_notify():
            try:
                # 不帶 end_date → 在「執行當下」才解析昨天。排程是啟動時註冊的，
                # 這裡若把日期算死，之後每晚都會重匯同一天。
                # 子行程跑（Arrow／整日列的尖峰不墊高主程序 RSS）。
                stats = run_isolated(
                    "trails_export", "scripts.export_daily_trails:export_range",
                    timeout=config.TRAILS_EXPORT_KILL_TIMEOUT_SEC)
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
            sched.submit, _as_task("trails_export", export_with_notify, timeout=1200, heavy=True))
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
            # Child process: the fetch peak (GBs) must not OOM-kill every collector.
            _as_task("gfw_hourly_publish", run_gfw_hourly_publish_in_subprocess, timeout=7200,
                     heavy=True),
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

    # RIS Live 同樣是單一常駐 WebSocket；replicas=1 是 enable hard gate。
    ripe_ris_live_worker = run_ripe_ris_live_worker()

    # 設定每日報告（先建立，讓歸檔任務可以回傳結果）
    daily_report_task = run_daily_report_task(collectors)
    run_db_efficiency_audit_task()

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

    # 本機磁碟護欄：量用量、清可丟的輔助檔、分兩級告警（不刪未歸檔原始檔）
    if config.DISK_GUARD_ENABLED and config.DISK_GUARD_INTERVAL_MIN > 0:
        from utils.disk_guard import DiskGuard
        disk_guard = DiskGuard()
        sched = get_scheduler()
        schedule.every(config.DISK_GUARD_INTERVAL_MIN).minutes.do(
            sched.submit, _as_task("disk_guard", disk_guard.run, timeout=300))
        sched.submit(_as_task("disk_guard", disk_guard.run, timeout=300))
        print(f"\n✓ 本機磁碟護欄 (每 {config.DISK_GUARD_INTERVAL_MIN} 分鐘)")

    # 收集器「太久沒新資料」即時告警（失敗本身不發；規則見 docs/handbook/operations.md §1）
    if config.STALE_ALERT_ENABLED and config.STALE_CHECK_INTERVAL_MIN > 0:
        from tasks.staleness_watch import StalenessWatch
        persistent = [n for n, w in (("aisstream", aisstream_worker),
                                     ("ripe_ris_live", ripe_ris_live_worker)) if w]
        staleness = StalenessWatch(collectors, persistent)
        sched = get_scheduler()
        schedule.every(config.STALE_CHECK_INTERVAL_MIN).minutes.do(
            sched.submit, _as_task("staleness_watch", staleness.run, timeout=120))
        print(f"\n✓ 收集器停更告警 (每 {config.STALE_CHECK_INTERVAL_MIN} 分鐘)")

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
        if not api_thread and not aisstream_worker and not ripe_ris_live_worker and not gfw_hourly_task:
            sys.exit(1)
        if aisstream_worker or ripe_ris_live_worker:
            workers = []
            if aisstream_worker:
                workers.append("AISStream")
            if ripe_ris_live_worker:
                workers.append("RIPE RIS Live")
            print("\n⚠️  沒有排程型收集器，僅執行 " + " + ".join(workers) + " 長駐 worker" + (" + API Server" if api_thread else ""))
        else:
            print("\n⚠️  沒有收集器，僅執行 API Server")

    # 設定 graceful shutdown
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        print(f"\n\n🛑 收到停止信號，正在結束...")
        if aisstream_worker:
            aisstream_worker.stop()
        if ripe_ris_live_worker:
            ripe_ris_live_worker.stop()
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
            # 單次自癒 🟡（進日報）；1 小時內第 2 次以上才 🔴。後面緊接 os._exit，
            # 所以同步寫狀態／送出，不走 pending flush。
            try:
                from utils import alerting
                now = datetime.now(alerting.TAIPEI_TZ)
                recent = alerting.recent_events("watchdog", now - timedelta(hours=1))
                level = alerting.RED if recent else alerting.YELLOW
                text = (f"Watchdog 1 小時內第 {len(recent) + 1} 次重啟進程：{msg}"
                        if recent else f"Watchdog 重啟進程：{msg}")
                alerting.event("watchdog", level, text, now=now)
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

    # 定期把 glibc arena 的空頁還給 OS（非 Linux 安靜跳過）
    from utils.memory import malloc_trim
    _trim_every = config.MALLOC_TRIM_INTERVAL_SEC
    _last_trim = time.monotonic()

    while running:
        if _trim_every > 0 and time.monotonic() - _last_trim >= _trim_every:
            _last_trim = time.monotonic()
            malloc_trim()
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
