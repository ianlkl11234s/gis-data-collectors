#!/usr/bin/env python3
"""
Data Collectors 主程式

統一管理所有資料收集器的排程執行，並提供 HTTP API 下載資料。
支援 S3 歸檔與資料生命週期管理。
"""

import signal
import sys
import time
import threading
from datetime import datetime

import schedule

import config
from collectors import (
    YouBikeCollector,
    WeatherCollector,
    VDCollector,
    TemperatureGridCollector,
    ParkingCollector,
    TRATrainCollector,
    TRAStaticCollector,
    RailTimetableCollector,
    ShipTDXCollector,
    ShipAISCollector,
    FlightFR24Collector,
    FlightFR24ZoneCollector,
    FlightOpenSkyCollector,
    BusCollector,
    FreewayVDCollector,
    EarthquakeCollector,
    SatelliteCollector,
    LaunchCollector,
    NCDRAlertsCollector,
    CWASatelliteCollector,
    FoursquarePOICollector,
)
from tasks import ArchiveTask, DailyReportTask, MiniTaipeiPublishTask
from utils.notify import notify_archive_complete


def run_collectors():
    """執行收集器排程"""
    # 初始化收集器
    collectors = []

    # YouBike 收集器
    if config.YOUBIKE_ENABLED:
        try:
            youbike = YouBikeCollector()
            collectors.append(youbike)
            print(f"\n✓ YouBike 收集器 (每 {youbike.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"\n✗ YouBike 收集器初始化失敗: {e}")
    else:
        print("\n⏸️  YouBike 收集器已停用 (YOUBIKE_ENABLED=false)")

    # Weather 收集器
    if config.WEATHER_ENABLED and config.CWA_API_KEY:
        try:
            weather = WeatherCollector()
            collectors.append(weather)
            print(f"✓ Weather 收集器 (每 {weather.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Weather 收集器初始化失敗: {e}")
    elif not config.WEATHER_ENABLED:
        print("⏸️  Weather 收集器已停用 (WEATHER_ENABLED=false)")
    else:
        print("⚠️  CWA_API_KEY 未設定，跳過 Weather 收集器")

    # VD 車輛偵測器收集器
    if config.VD_ENABLED:
        try:
            vd = VDCollector()
            collectors.append(vd)
            print(f"✓ VD 收集器 (每 {vd.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ VD 收集器初始化失敗: {e}")
    else:
        print("⏸️  VD 收集器已停用 (VD_ENABLED=false)")

    # 國道即時車流 + 壅塞收集器 (需要 TDX API)
    if config.FREEWAY_VD_ENABLED:
        try:
            freeway_vd = FreewayVDCollector()
            collectors.append(freeway_vd)
            print(f"✓ Freeway VD 收集器 (每 {freeway_vd.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Freeway VD 收集器初始化失敗: {e}")
    else:
        print("⏸️  Freeway VD 收集器已停用 (FREEWAY_VD_ENABLED=false)")

    # 溫度網格收集器 (需要 CWA API Key)
    if config.TEMPERATURE_ENABLED and config.CWA_API_KEY:
        try:
            temperature = TemperatureGridCollector()
            collectors.append(temperature)
            print(f"✓ Temperature Grid 收集器 (每 {temperature.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Temperature Grid 收集器初始化失敗: {e}")
    elif not config.TEMPERATURE_ENABLED:
        print("⏸️  Temperature Grid 收集器已停用 (TEMPERATURE_ENABLED=false)")

    # 路邊停車收集器 (需要 TDX API)
    if config.PARKING_ENABLED:
        try:
            parking = ParkingCollector()
            collectors.append(parking)
            print(f"✓ Parking 收集器 (每 {parking.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Parking 收集器初始化失敗: {e}")
    else:
        print("⏸️  Parking 收集器已停用 (PARKING_ENABLED=false)")

    # 公車即時位置收集器 (需要 TDX API)
    if config.BUS_ENABLED:
        try:
            bus = BusCollector()
            collectors.append(bus)
            print(f"✓ Bus 收集器 (每 {bus.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Bus 收集器初始化失敗: {e}")
    else:
        print("⏸️  Bus 收集器已停用 (BUS_ENABLED=false)")

    # 台鐵即時列車位置收集器 (需要 TDX API)
    if config.TRA_TRAIN_ENABLED:
        try:
            tra_train = TRATrainCollector()
            collectors.append(tra_train)
            print(f"✓ TRA Train 收集器 (每 {tra_train.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ TRA Train 收集器初始化失敗: {e}")
    else:
        print("⏸️  TRA Train 收集器已停用 (TRA_TRAIN_ENABLED=false)")

    # 台鐵靜態資料收集器 (需要 TDX API，每日一次)
    if config.TRA_STATIC_ENABLED:
        try:
            tra_static = TRAStaticCollector()
            collectors.append(tra_static)
            print(f"✓ TRA Static 收集器 (每 {tra_static.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ TRA Static 收集器初始化失敗: {e}")
    else:
        print("⏸️  TRA Static 收集器已停用 (TRA_STATIC_ENABLED=false)")

    # 台鐵 + 高鐵每日時刻表歸檔（需要 TDX API）
    if config.RAIL_TIMETABLE_ENABLED:
        try:
            rail_tt = RailTimetableCollector()
            collectors.append(rail_tt)
            print(f"✓ Rail Timetable 收集器 (每 {rail_tt.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Rail Timetable 收集器初始化失敗: {e}")
    else:
        print("⏸️  Rail Timetable 收集器已停用 (RAIL_TIMETABLE_ENABLED=false)")

    # TDX 國內航線船位收集器 (需要 TDX API)
    if config.SHIP_TDX_ENABLED:
        try:
            ship_tdx = ShipTDXCollector()
            collectors.append(ship_tdx)
            print(f"✓ Ship TDX 收集器 (每 {ship_tdx.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Ship TDX 收集器初始化失敗: {e}")
    else:
        print("⏸️  Ship TDX 收集器已停用 (SHIP_TDX_ENABLED=false)")

    # 航港局 AIS 船位收集器
    if config.SHIP_AIS_ENABLED:
        try:
            ship_ais = ShipAISCollector()
            collectors.append(ship_ais)
            print(f"✓ Ship AIS 收集器 (每 {ship_ais.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Ship AIS 收集器初始化失敗: {e}")
    else:
        print("⏸️  Ship AIS 收集器已停用 (SHIP_AIS_ENABLED=false)")

    # FlightRadar24 航班軌跡收集器
    if config.FLIGHT_FR24_ENABLED:
        try:
            flight_fr24 = FlightFR24Collector()
            collectors.append(flight_fr24)
            print(f"✓ Flight FR24 收集器 (每 {flight_fr24.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Flight FR24 收集器初始化失敗: {e}")
    else:
        print("⏸️  Flight FR24 收集器已停用 (FLIGHT_FR24_ENABLED=false)")

    # FR24 Zone 空域快照收集器
    if config.FLIGHT_FR24_ZONE_ENABLED:
        try:
            fr24_zone = FlightFR24ZoneCollector()
            collectors.append(fr24_zone)
            print(f"✓ FR24 Zone 收集器 (每 {fr24_zone.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ FR24 Zone 收集器初始化失敗: {e}")
    else:
        print("⏸️  FR24 Zone 收集器已停用 (FLIGHT_FR24_ZONE_ENABLED=false)")

    # 地震報告收集器 (需要 CWA API Key，每日一次)
    if config.EARTHQUAKE_ENABLED and config.CWA_API_KEY:
        try:
            earthquake = EarthquakeCollector()
            collectors.append(earthquake)
            print(f"✓ Earthquake 收集器 (每 {earthquake.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Earthquake 收集器初始化失敗: {e}")
    elif not config.EARTHQUAKE_ENABLED:
        print("⏸️  Earthquake 收集器已停用 (EARTHQUAKE_ENABLED=false)")
    else:
        print("⚠️  CWA_API_KEY 未設定，跳過 Earthquake 收集器")

    # OpenSky 空域快照收集器
    if config.FLIGHT_OPENSKY_ENABLED:
        try:
            opensky_collector = FlightOpenSkyCollector()
            collectors.append(opensky_collector)
            print(f"✓ OpenSky 收集器 (每 {opensky_collector.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ OpenSky 收集器初始化失敗: {e}")
    else:
        print("⏸️  OpenSky 收集器已停用 (FLIGHT_OPENSKY_ENABLED=false)")

    # 衛星軌道追蹤收集器（CelesTrak GP + SGP4，免註冊）
    if config.SATELLITE_ENABLED:
        try:
            satellite = SatelliteCollector()
            collectors.append(satellite)
            print(f"✓ Satellite 收集器 (每 {satellite.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Satellite 收集器初始化失敗: {e}")
    else:
        print("⏸️  Satellite 收集器已停用 (SATELLITE_ENABLED=false)")

    # 太空發射收集器（Launch Library 2，免費 API）
    if config.LAUNCH_ENABLED:
        try:
            launch = LaunchCollector()
            collectors.append(launch)
            print(f"✓ Launch 收集器 (每 {launch.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Launch 收集器初始化失敗: {e}")
    else:
        print("⏸️  Launch 收集器已停用 (LAUNCH_ENABLED=false)")

    # CWA 衛星雲圖 + 雷達 PNG 收集器（需 CWA API Key）
    if config.CWA_SATELLITE_ENABLED and config.CWA_API_KEY:
        try:
            cwa_sat = CWASatelliteCollector()
            collectors.append(cwa_sat)
            print(f"✓ CWA Satellite 影像收集器 (每 {cwa_sat.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ CWA Satellite 收集器初始化失敗: {e}")
    elif not config.CWA_SATELLITE_ENABLED:
        print("⏸️  CWA Satellite 收集器已停用 (CWA_SATELLITE_ENABLED=false)")
    else:
        print("⚠️  CWA_API_KEY 未設定，跳過 CWA Satellite 收集器")

    # NCDR 災害示警收集器（CAP feed，無需 API key）
    if config.NCDR_ALERTS_ENABLED:
        try:
            ncdr = NCDRAlertsCollector()
            collectors.append(ncdr)
            print(f"✓ NCDR Alerts 收集器 (每 {ncdr.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ NCDR Alerts 收集器初始化失敗: {e}")
    else:
        print("⏸️  NCDR Alerts 收集器已停用 (NCDR_ALERTS_ENABLED=false)")

    # Foursquare OS Places POI 收集器（每月一次，背景 thread 執行）
    if config.FOURSQUARE_POI_ENABLED and config.HF_TOKEN:
        try:
            foursquare_poi = FoursquarePOICollector()
            collectors.append(foursquare_poi)
            print(f"✓ Foursquare POI 收集器 (每 {foursquare_poi.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Foursquare POI 收集器初始化失敗: {e}")
    elif not config.FOURSQUARE_POI_ENABLED:
        print("⏸️  Foursquare POI 收集器已停用 (FOURSQUARE_POI_ENABLED=false)")
    else:
        print("⚠️  HF_TOKEN 未設定，跳過 Foursquare POI 收集器")

    if not collectors:
        print("\n❌ 沒有可用的收集器")
        return []

    # ============================================================
    # 背景 thread collector：跑時間較長，不阻塞主排程
    # 與主 schedule loop 共用 SupabaseWriter（writer 內部已加鎖）
    # ============================================================
    BACKGROUND_COLLECTORS = {'flight_fr24', 'foursquare_poi', 'satellite'}

    bg_collectors = [c for c in collectors if c.name in BACKGROUND_COLLECTORS]
    fg_collectors = [c for c in collectors if c.name not in BACKGROUND_COLLECTORS]

    # 立即執行一次（前景 collector 在主 thread 依序跑）
    print("\n" + "=" * 60)
    print("🚀 初始執行")
    print("=" * 60)

    for collector in fg_collectors:
        collector.run()

    # 設定前景排程
    for collector in fg_collectors:
        schedule.every(collector.interval_minutes).minutes.do(collector.run)

    # 啟動背景 thread collector（各跑各的 interval）
    def _bg_loop(collector):
        print(f"[{collector.name}] 背景 thread 啟動 (每 {collector.interval_minutes} 分鐘)")
        # 啟動時先跑一次
        try:
            collector.run()
        except Exception as e:
            print(f"[{collector.name}] 背景初次執行錯誤: {e}")
        interval_sec = collector.interval_minutes * 60
        while True:
            time.sleep(interval_sec)
            try:
                collector.run()
            except Exception as e:
                print(f"[{collector.name}] 背景執行錯誤: {e}")

    for collector in bg_collectors:
        t = threading.Thread(target=_bg_loop, args=(collector,), daemon=True, name=f"bg-{collector.name}")
        t.start()
        print(f"✓ {collector.name} 已切換為背景 thread 模式（不阻塞主排程）")

    # 顯示下次執行時間
    next_run = schedule.next_run()
    if next_run:
        print(f"\n⏰ 下次執行: {next_run.strftime('%H:%M:%S')}")

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
        print(f"✓ 每日報告已設定 (每日 {config.DAILY_REPORT_TIME})")

        schedule.every().day.at(config.DAILY_REPORT_TIME).do(daily_report.run)

        return daily_report
    except Exception as e:
        print(f"\n✗ 每日報告初始化失敗: {e}")
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


def main():
    """主程式"""
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
