"""
歸檔任務

負責將本地資料同步到 S3，並清理過期的本地資料。
"""

import gc
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import config


class ArchiveTask:
    """歸檔任務管理器"""

    def __init__(self):
        self.s3 = None
        self._init_s3()

    def _init_s3(self):
        """初始化 S3 儲存"""
        if not config.S3_BUCKET:
            print("⚠️  S3_BUCKET 未設定，歸檔功能停用")
            return

        try:
            from storage.s3 import S3Storage
            self.s3 = S3Storage()
            print(f"✓ S3 儲存已連接: {config.S3_BUCKET}")
        except Exception as e:
            print(f"✗ S3 儲存初始化失敗: {e}")
            self.s3 = None

    def run(self):
        """執行歸檔任務"""
        if not config.ARCHIVE_ENABLED:
            print("⚠️  歸檔功能已停用 (ARCHIVE_ENABLED=false)")
            return

        if not self.s3:
            print("⚠️  S3 未設定，跳過歸檔")
            return

        print(f"\n{'=' * 60}")
        print(f"📦 開始歸檔任務")
        print(f"{'=' * 60}")
        print(f"   時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   保留天數: {config.ARCHIVE_RETENTION_DAYS}")
        print(f"   S3 Bucket: {config.S3_BUCKET}")

        # 步驟 1: 同步所有資料到 S3
        sync_stats = self._sync_to_s3()

        # 步驟 2: 清理過期的本地資料
        cleanup_stats = self._cleanup_local()

        # 觸發 GC
        gc.collect()

        print(f"\n{'=' * 60}")
        print(f"📊 歸檔完成")
        print(f"{'=' * 60}")
        print(f"   同步: 上傳 {sync_stats['uploaded']} | 跳過 {sync_stats['skipped']} | 失敗 {sync_stats['failed']}")
        print(f"   清理: 刪除 {cleanup_stats['deleted']} 個檔案 | 保留 {cleanup_stats['kept']} 個檔案")
        print(f"{'=' * 60}")

        return {
            'sync': sync_stats,
            'cleanup': cleanup_stats
        }

    def _sync_to_s3(self) -> dict:
        """同步本地資料到 S3"""
        print(f"\n📤 同步資料到 S3...")

        total_stats = {'uploaded': 0, 'skipped': 0, 'failed': 0}

        if not config.LOCAL_DATA_DIR.exists():
            print("   ⚠️  本地資料目錄不存在")
            return total_stats

        # 遍歷所有收集器目錄
        for collector_dir in config.LOCAL_DATA_DIR.iterdir():
            if not collector_dir.is_dir():
                continue

            collector_name = collector_dir.name

            # 同步該收集器的資料
            stats = self.s3.sync_directory(
                local_dir=collector_dir,
                s3_prefix=collector_name,
                skip_existing=True
            )

            total_stats['uploaded'] += stats['uploaded']
            total_stats['skipped'] += stats['skipped']
            total_stats['failed'] += stats['failed']

            if stats['uploaded'] > 0:
                print(f"   ✓ {collector_name}: 上傳 {stats['uploaded']} 個檔案")
            elif stats['skipped'] > 0:
                print(f"   - {collector_name}: 已同步 ({stats['skipped']} 個檔案)")

        return total_stats

    def _cleanup_local(self) -> dict:
        """清理過期的本地資料"""
        print(f"\n🗑️  清理過期資料 (>{config.ARCHIVE_RETENTION_DAYS} 天)...")

        stats = {'deleted': 0, 'kept': 0}
        cutoff_date = datetime.now() - timedelta(days=config.ARCHIVE_RETENTION_DAYS)

        if not config.LOCAL_DATA_DIR.exists():
            return stats

        # 遍歷所有收集器目錄
        for collector_dir in config.LOCAL_DATA_DIR.iterdir():
            if not collector_dir.is_dir():
                continue

            collector_name = collector_dir.name

            # 遍歷年/月/日目錄結構
            deleted_count = 0
            for json_file in collector_dir.glob('**/*.json'):
                # 跳過 latest.json
                if json_file.name == 'latest.json':
                    stats['kept'] += 1
                    continue

                # 檢查檔案修改時間
                file_mtime = datetime.fromtimestamp(json_file.stat().st_mtime)

                if file_mtime < cutoff_date:
                    # 確認 S3 上已有此檔案
                    rel_path = json_file.relative_to(collector_dir)
                    s3_key = f"{collector_name}/{rel_path}"

                    if self.s3.file_exists(s3_key):
                        json_file.unlink()
                        deleted_count += 1
                        stats['deleted'] += 1
                    else:
                        stats['kept'] += 1
                else:
                    stats['kept'] += 1

            if deleted_count > 0:
                print(f"   ✓ {collector_name}: 刪除 {deleted_count} 個過期檔案")

                # 清理空目錄
                self._cleanup_empty_dirs(collector_dir)

        return stats

    def _cleanup_empty_dirs(self, base_dir: Path):
        """清理空目錄"""
        for dir_path in sorted(base_dir.glob('**/*'), reverse=True):
            if dir_path.is_dir() and not any(dir_path.iterdir()):
                dir_path.rmdir()

    def get_archive_status(self) -> dict:
        """取得歸檔狀態"""
        status = {
            'enabled': config.ARCHIVE_ENABLED,
            's3_configured': self.s3 is not None,
            's3_bucket': config.S3_BUCKET,
            'retention_days': config.ARCHIVE_RETENTION_DAYS,
            'archive_time': config.ARCHIVE_TIME,
            'local_data_dir': str(config.LOCAL_DATA_DIR),
            'collectors': []
        }

        if not config.LOCAL_DATA_DIR.exists():
            return status

        # 統計各收集器的資料
        for collector_dir in config.LOCAL_DATA_DIR.iterdir():
            if not collector_dir.is_dir():
                continue

            files = list(collector_dir.glob('**/*.json'))
            files = [f for f in files if f.name != 'latest.json']

            total_size = sum(f.stat().st_size for f in files)

            collector_status = {
                'name': collector_dir.name,
                'local_files': len(files),
                'local_size_mb': round(total_size / (1024 * 1024), 2)
            }

            # 如果 S3 可用，統計 S3 上的資料
            if self.s3:
                s3_files = self.s3.list_files(collector_dir.name)
                s3_size = sum(f['size'] for f in s3_files)
                collector_status['s3_files'] = len(s3_files)
                collector_status['s3_size_mb'] = round(s3_size / (1024 * 1024), 2)

            status['collectors'].append(collector_status)

        return status
