#!/usr/bin/env python3
"""
Data Collectors 主程式

統一管理所有資料收集器的排程執行。
"""

import signal
import sys
import time
from datetime import datetime

import schedule

import config
from collectors import YouBikeCollector


def main():
    """主程式"""
    print("=" * 60)
    print("📡 Data Collectors")
    print("=" * 60)

    # 驗證設定
    if not config.validate_config():
        sys.exit(1)

    config.print_config()

    # 初始化收集器
    collectors = []

    # YouBike 收集器
    try:
        youbike = YouBikeCollector()
        collectors.append(youbike)
        print(f"\n✓ YouBike 收集器 (每 {youbike.interval_minutes} 分鐘)")
    except Exception as e:
        print(f"\n✗ YouBike 收集器初始化失敗: {e}")

    # TODO: 未來可加入其他收集器
    # weather = WeatherCollector()
    # collectors.append(weather)

    if not collectors:
        print("\n❌ 沒有可用的收集器")
        sys.exit(1)

    # 設定 graceful shutdown
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        print(f"\n\n🛑 收到停止信號，正在結束...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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

    print("\n" + "=" * 60)
    print("📡 等待排程執行... (按 Ctrl+C 停止)")
    print("=" * 60)

    # 主迴圈
    while running:
        schedule.run_pending()
        time.sleep(1)

    # 結束
    print("\n📊 執行統計:")
    for collector in collectors:
        status = collector.get_status()
        print(f"   [{status['name']}] "
              f"執行 {status['run_count']} 次 | "
              f"錯誤 {status['error_count']} 次")

    print("\n👋 已停止")


if __name__ == '__main__':
    main()
