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
    ShipTDXCollector,
    ShipAISCollector,
    FlightFR24Collector,
)
from tasks import ArchiveTask


def run_collectors():
    """執行收集器排程"""
    # 初始化收集器
    collectors = []

    # YouBike 收集器
    try:
        youbike = YouBikeCollector()
        collectors.append(youbike)
        print(f"\n✓ YouBike 收集器 (每 {youbike.interval_minutes} 分鐘)")
    except Exception as e:
        print(f"\n✗ YouBike 收集器初始化失敗: {e}")

    # Weather 收集器
    if config.CWA_API_KEY:
        try:
            weather = WeatherCollector()
            collectors.append(weather)
            print(f"✓ Weather 收集器 (每 {weather.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Weather 收集器初始化失敗: {e}")
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

    # 溫度網格收集器 (需要 CWA API Key)
    if config.CWA_API_KEY:
        try:
            temperature = TemperatureGridCollector()
            collectors.append(temperature)
            print(f"✓ Temperature Grid 收集器 (每 {temperature.interval_minutes} 分鐘)")
        except Exception as e:
            print(f"✗ Temperature Grid 收集器初始化失敗: {e}")

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

    # 台鐵即時列車位置收集器 (需要 TDX API)
    try:
        tra_train = TRATrainCollector()
        collectors.append(tra_train)
        print(f"✓ TRA Train 收集器 (每 {tra_train.interval_minutes} 分鐘)")
    except Exception as e:
        print(f"✗ TRA Train 收集器初始化失敗: {e}")

    # 台鐵靜態資料收集器 (需要 TDX API，每日一次)
    try:
        tra_static = TRAStaticCollector()
        collectors.append(tra_static)
        print(f"✓ TRA Static 收集器 (每 {tra_static.interval_minutes} 分鐘)")
    except Exception as e:
        print(f"✗ TRA Static 收集器初始化失敗: {e}")

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

    if not collectors:
        print("\n❌ 沒有可用的收集器")
        return []

    # 立即執行一次所有收集器
    print("\n" + "=" * 60)
    print("🚀 初始執行")
    print("=" * 60)

    for collector in collectors:
        collector.run()

    # 設定排程
    for collector in collectors:
        schedule.every(collector.interval_minutes).minutes.do(collector.run)

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


def run_archive_task():
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

        # 設定每日排程
        schedule.every().day.at(config.ARCHIVE_TIME).do(archive_task.run)

        return archive_task
    except Exception as e:
        print(f"\n✗ 歸檔任務初始化失敗: {e}")
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

    # 設定歸檔任務
    archive_task = run_archive_task()

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
