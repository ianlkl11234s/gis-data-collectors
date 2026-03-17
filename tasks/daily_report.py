"""
每日報告任務

每天早上發送 Telegram 訊息，彙整昨日資料收集狀態、檔案統計、歸檔結果與系統資訊。
同時執行靜默檢測與磁碟空間檢查。
"""

from datetime import datetime, timedelta
from pathlib import Path

import config
from utils.notify import (
    send_telegram,
    notify_disk_alert,
    notify_silence_alert,
)


class DailyReportTask:
    """每日報告任務"""

    def __init__(self, collectors: list, archive_task=None):
        """
        Args:
            collectors: 所有啟用的收集器實例
            archive_task: 歸檔任務實例（可選）
        """
        self.collectors = collectors
        self.archive_task = archive_task
        self.last_archive_result = None  # 由外部設定最近一次歸檔結果
        self._start_time = datetime.now()

    def run(self):
        """產生並發送每日報告"""
        print(f"\n{'=' * 60}")
        print(f"📊 產生每日報告")
        print(f"{'=' * 60}")

        report = self._build_report()
        send_telegram(report)
        print(f"✓ 每日報告已發送")

        # 順帶執行健康檢查
        self._check_silence()
        self._check_disk_usage()

    def _build_report(self) -> str:
        """組裝報告訊息"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        tag = f" [{config.INSTANCE_NAME}]" if config.INSTANCE_NAME else ""
        lines = [f"📊 *資料收集日報{tag} — {yesterday}*\n"]

        # 收集狀態
        lines.append(self._section_collector_status())

        # 檔案統計（本地）
        lines.append(self._section_file_stats())

        # S3 統計
        lines.append(self._section_s3_stats())

        # 歸檔結果
        if self.archive_task:
            lines.append(self._section_archive())

        # 系統資訊
        lines.append(self._section_system_info())

        return '\n'.join(lines)

    def _section_collector_status(self) -> str:
        """收集器狀態區塊"""
        normal = []
        has_errors = []
        silent = []

        now = datetime.now()

        for c in self.collectors:
            status = c.get_status()
            name = status['name']
            run_count = status['run_count']
            error_count = status['error_count']
            last_run = status['last_run']

            # 檢查是否靜默（超過預期間隔 2 倍）
            is_silent = False
            if last_run:
                last_dt = datetime.fromisoformat(last_run)
                silence_threshold = timedelta(minutes=c.interval_minutes * 2)
                if now - last_dt > silence_threshold:
                    is_silent = True

            if is_silent:
                last_str = datetime.fromisoformat(last_run).strftime('%m-%d %H:%M')
                silent.append(f"  `{name}`: 最後執行 {last_str}")
            elif error_count > 0:
                rate = (error_count / run_count * 100) if run_count > 0 else 0
                has_errors.append(f"  `{name}`: {run_count}次, {error_count}次錯誤 ({rate:.1f}%)")
            else:
                normal.append(f"`{name}` {run_count}次")

        total = len(self.collectors)
        parts = []

        if normal:
            parts.append(f"✅ *正常運作* ({len(normal)}/{total})")
            # 正常的用一行顯示，節省空間
            parts.append(f"  {' | '.join(normal)}")

        if has_errors:
            parts.append(f"\n⚠️ *有錯誤* ({len(has_errors)}/{total})")
            parts.extend(has_errors)

        if silent:
            parts.append(f"\n❌ *疑似停止* ({len(silent)}/{total})")
            parts.extend(silent)

        return '\n'.join(parts)

    def _section_file_stats(self) -> str:
        """本地檔案統計區塊"""
        data_dir = config.LOCAL_DATA_DIR
        if not data_dir.exists():
            return "\n📁 *本地檔案*\n  資料目錄不存在"

        total_files = 0
        total_size = 0
        today_files = 0
        collector_stats = []
        today_str = datetime.now().strftime('%Y/%m/%d')

        for collector_dir in sorted(data_dir.iterdir()):
            if not collector_dir.is_dir():
                continue

            # 計算所有 JSON（排除 latest.json）
            files = [f for f in collector_dir.glob('**/*.json') if f.name != 'latest.json']
            size = sum(f.stat().st_size for f in files)
            count = len(files)

            # 計算今日檔案
            today_dir = collector_dir / today_str
            today_count = len(list(today_dir.glob('*.json'))) if today_dir.exists() else 0

            total_files += count
            total_size += size
            today_files += today_count

            if count > 0:
                collector_stats.append(f"`{collector_dir.name}` {count}")

        size_mb = total_size / (1024 * 1024)

        parts = [
            f"\n📁 *本地檔案*",
            f"  總計: *{total_files}* 個 ({size_mb:.1f} MB)",
            f"  今日新增: *{today_files}* 個",
        ]
        if collector_stats:
            parts.append(f"  {' | '.join(collector_stats)}")

        return '\n'.join(parts)

    def _section_s3_stats(self) -> str:
        """S3 儲存統計區塊"""
        if not config.S3_BUCKET:
            return "\n☁️ *S3 儲存*\n  未設定"

        try:
            from storage.s3 import S3Storage
            s3 = S3Storage()
            stats = s3.get_bucket_stats()
        except Exception as e:
            return f"\n☁️ *S3 儲存*\n  查詢失敗: {e}"

        total_objects = stats['total_objects']
        total_gb = stats['total_size_bytes'] / (1024 ** 3)
        estimated_cost = total_gb * config.S3_PRICE_PER_GB

        parts = [
            f"\n☁️ *S3 儲存* ({config.S3_BUCKET})",
            f"  總計: *{total_objects}* 個物件 ({total_gb:.2f} GB)",
            f"  估算月費: *${estimated_cost:.2f}* USD",
        ]

        # 按收集器顯示（只顯示前幾大的）
        by_collector = stats['by_collector']
        if by_collector:
            sorted_collectors = sorted(
                by_collector.items(),
                key=lambda x: x[1]['size_bytes'],
                reverse=True
            )
            top_items = []
            for name, info in sorted_collectors[:5]:
                size_mb = info['size_bytes'] / (1024 ** 2)
                top_items.append(f"`{name}` {size_mb:.0f}MB")
            parts.append(f"  {' | '.join(top_items)}")

        return '\n'.join(parts)

    def _section_archive(self) -> str:
        """歸檔結果區塊"""
        parts = [f"\n📦 *歸檔結果* ({config.ARCHIVE_TIME})"]

        if self.last_archive_result:
            archive = self.last_archive_result.get('archive', {})
            cleanup = self.last_archive_result.get('cleanup', {})
            uploaded = archive.get('uploaded', 0)
            skipped = archive.get('skipped', 0)
            failed = archive.get('failed', 0)
            deleted = cleanup.get('deleted', 0)

            parts.append(f"  上傳: {uploaded} 個 | 跳過: {skipped} 個 | 失敗: {failed} 個")
            parts.append(f"  清理: 刪除 {deleted} 個本地目錄")
        else:
            parts.append(f"  昨日無歸檔記錄")

        return '\n'.join(parts)

    def _section_system_info(self) -> str:
        """系統資訊區塊"""
        uptime = datetime.now() - self._start_time
        days = uptime.days
        hours = uptime.seconds // 3600

        # 磁碟使用
        data_dir = config.LOCAL_DATA_DIR
        used_mb = 0
        if data_dir.exists():
            used_mb = sum(
                f.stat().st_size for f in data_dir.glob('**/*') if f.is_file()
            ) / (1024 * 1024)

        parts = [
            f"\n⚙️ *系統資訊*",
            f"  運行時間: {days}天{hours}小時",
            f"  本地磁碟: {used_mb:.0f} MB",
        ]

        return '\n'.join(parts)

    def _check_silence(self):
        """檢查收集器是否靜默（即時告警）"""
        now = datetime.now()

        for c in self.collectors:
            status = c.get_status()
            last_run = status['last_run']

            if not last_run:
                continue

            last_dt = datetime.fromisoformat(last_run)
            silence_threshold = timedelta(minutes=c.interval_minutes * 2)

            if now - last_dt > silence_threshold:
                last_str = last_dt.strftime('%m-%d %H:%M')
                notify_silence_alert(c.name, last_str, c.interval_minutes)

    def _check_disk_usage(self):
        """檢查磁碟使用量"""
        data_dir = config.LOCAL_DATA_DIR
        if not data_dir.exists():
            return

        used_bytes = sum(f.stat().st_size for f in data_dir.glob('**/*') if f.is_file())
        used_mb = used_bytes / (1024 * 1024)

        if used_mb > config.DISK_ALERT_THRESHOLD_MB:
            notify_disk_alert(used_mb, config.DISK_ALERT_THRESHOLD_MB)
