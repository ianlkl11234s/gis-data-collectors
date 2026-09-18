"""
歸檔任務

負責將本地資料壓成 tar.gz 上傳到 S3，並清理過期的本地資料。

新流程：
1. 遍歷各收集器的 YYYY/MM/DD 日期目錄
2. 跳過今天（還在收集中）
3. 將該日所有 JSON 壓成 collector/archives/YYYY-MM-DD.tar.gz
4. 上傳 1 個 PUT 到 S3
5. 清理：驗證封存內容 receipt 與遠端 identity 後，才刪除符合 retention 的日期目錄
"""

import gc
import gzip
import hashlib
import json
import shutil
import tarfile
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

        # 步驟 1: 壓縮並上傳 tar.gz 到 S3
        archive_stats = self._archive_to_s3()

        # 步驟 2: 清理過期的本地資料
        cleanup_stats = self._cleanup_local()

        # 觸發 GC
        gc.collect()

        print(f"\n{'=' * 60}")
        print(f"📊 歸檔完成")
        print(f"{'=' * 60}")
        print(f"   歸檔: 上傳 {archive_stats['uploaded']} | 跳過 {archive_stats['skipped']} | 失敗 {archive_stats['failed']}")
        print(f"   清理: 刪除 {cleanup_stats['deleted']} 個目錄")
        print(f"{'=' * 60}")

        return {
            'archive': archive_stats,
            'cleanup': cleanup_stats
        }

    def _find_date_dirs(self, collector_dir: Path) -> list:
        """找出收集器目錄下所有日期目錄（YYYY/MM/DD 結構）

        Args:
            collector_dir: 收集器目錄（如 data/weather/）

        Returns:
            list: [(date_str, date_dir_path), ...] 如 [('2026-02-27', Path('data/weather/2026/02/27'))]
        """
        date_dirs = []
        today = datetime.now().strftime('%Y-%m-%d')

        for year_dir in collector_dir.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                for day_dir in month_dir.iterdir():
                    if not day_dir.is_dir():
                        continue
                    date_str = f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
                    # 跳過今天（還在收集中）
                    if date_str == today:
                        continue
                    date_dirs.append((date_str, day_dir))

        return sorted(date_dirs)

    def _member_manifest(self, date_dir: Path) -> list[dict]:
        members = []
        for json_file in sorted(date_dir.glob('*.json')):
            digest, size = hashlib.sha256(), 0
            with json_file.open('rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block); size += len(block)
            members.append({'name': json_file.name, 'sha256': digest.hexdigest(), 'bytes': size})
        return members

    def _create_tar_gz(self, date_dir: Path, destination: Path) -> None:
        """Stream a deterministic tar.gz to disk; do not retain the archive in RAM."""
        with destination.open('wb') as output:
            with gzip.GzipFile(fileobj=output, mode='wb', mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode='w') as tar:
                    for json_file in sorted(date_dir.glob('*.json')):
                        tar.add(str(json_file), arcname=json_file.name)

    def _has_archive_space(self, json_files: list[Path]) -> bool:
        return shutil.disk_usage(config.LOCAL_DATA_DIR).free >= sum(item.stat().st_size for item in json_files) + 1024 * 1024

    def _has_only_manifest_files(self, date_dir: Path, members: list[dict]) -> bool:
        expected = {member['name'] for member in members}
        entries = list(date_dir.iterdir())
        return len(entries) == len(expected) and all(entry.name in expected and entry.is_file() and not entry.is_symlink() for entry in entries)

    def _receipt_path(self, collector_name: str, date_str: str) -> Path:
        return config.LOCAL_DATA_DIR / '.archive-receipts' / collector_name / f'{date_str}.json'

    def _write_receipt(self, collector_name: str, date_str: str, s3_key: str, members: list[dict], identity: dict) -> bool:
        receipt = self._receipt_path(collector_name, date_str)
        try:
            receipt.parent.mkdir(parents=True, exist_ok=True)
            temporary = receipt.with_suffix('.json.tmp')
            temporary.write_text(json.dumps({'s3_key': s3_key, 'members': members, 'remote_identity': identity, 'checked_at': datetime.now().isoformat()}))
            temporary.replace(receipt)
            return True
        except OSError:
            return False

    def _receipt_matches(self, collector_name: str, date_str: str, s3_key: str, members: list[dict]) -> dict | None:
        try:
            receipt = json.loads(self._receipt_path(collector_name, date_str).read_text())
            return receipt if receipt.get('s3_key') == s3_key and receipt.get('members') == members and isinstance(receipt.get('remote_identity'), dict) else None
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    def _archive_to_s3(self) -> dict:
        """壓縮並上傳 tar.gz 到 S3"""
        print(f"\n📤 壓縮並上傳 tar.gz 到 S3...")

        stats = {'uploaded': 0, 'skipped': 0, 'failed': 0}

        if not config.LOCAL_DATA_DIR.exists():
            print("   ⚠️  本地資料目錄不存在")
            return stats

        for collector_dir in config.LOCAL_DATA_DIR.iterdir():
            if not collector_dir.is_dir():
                continue

            collector_name = collector_dir.name
            if collector_name == 'gfw_vessel_presence' and not config.GFW_RAW_ARCHIVE_ENABLED:
                # Provider raw retention is license-gated.  Skip even if stale
                # local files exist from a prior build or interrupted run.
                continue
            date_dirs = self._find_date_dirs(collector_dir)

            for date_str, date_dir in date_dirs:
                s3_key = f"{collector_name}/archives/{date_str}.tar.gz"

                # 確認目錄中有 JSON 檔案
                json_files = list(date_dir.glob('*.json'))
                if not json_files:
                    continue
                # 壓縮
                try:
                    members = self._member_manifest(date_dir)
                except Exception as e:
                    print(f"   ✗ {collector_name}/{date_str}: 壓縮失敗 - {e}")
                    stats['failed'] += 1
                    continue
                receipt = self._receipt_matches(collector_name, date_str, s3_key, members)
                if receipt is not None:
                    identity = self.s3.archive_identity(s3_key)
                    if identity['status'] == 'present' and identity['identity'] == receipt['remote_identity']:
                        stats['skipped'] += 1
                        continue

                # 寫入臨時檔案再上傳
                tmp_path = config.LOCAL_DATA_DIR / f".tmp_{collector_name}_{date_str}.tar.gz"
                try:
                    verification = self.s3.verify_archive(s3_key, members)
                    uploaded = False
                    if verification['status'] == 'missing':
                        if not self._has_archive_space(json_files):
                            stats['failed'] += 1
                            continue
                        self._create_tar_gz(date_dir, tmp_path)
                        if not self.s3.upload_archive(tmp_path, s3_key):
                            stats['failed'] += 1
                            continue
                        uploaded = True
                        verification = self.s3.verify_archive(s3_key, members)
                    if verification['status'] != 'verified' or not self._write_receipt(collector_name, date_str, s3_key, members, verification['identity']):
                        stats['failed'] += 1
                    elif uploaded:
                        stats['uploaded'] += 1
                        print(f"   ✓ {collector_name}/{date_str}: {len(json_files)} 個檔案 → tar.gz ({tmp_path.stat().st_size} bytes)")
                    else:
                        stats['skipped'] += 1
                finally:
                    tmp_path.unlink(missing_ok=True)

            if stats['uploaded'] > 0 or stats['skipped'] > 0:
                uploaded = stats['uploaded']
                skipped = stats['skipped']
                if uploaded > 0:
                    print(f"   {collector_name}: 上傳 {uploaded} 個歸檔")

        return stats

    def _cleanup_local(self) -> dict:
        """清理過期的本地資料（已歸檔到 S3 的日期目錄）

        每個 collector 可透過 {NAME}_ARCHIVE_RETENTION_DAYS 覆寫全域天數。
        """
        overrides = config.COLLECTOR_RETENTION_OVERRIDES
        if overrides:
            print(f"\n🗑️  清理過期資料 (預設 >{config.ARCHIVE_RETENTION_DAYS} 天；"
                  f"特例：{overrides}) ...")
        else:
            print(f"\n🗑️  清理過期資料 (>{config.ARCHIVE_RETENTION_DAYS} 天)...")

        stats = {'deleted': 0}
        now_ts = datetime.now()

        if not config.LOCAL_DATA_DIR.exists():
            return stats

        for collector_dir in config.LOCAL_DATA_DIR.iterdir():
            if not collector_dir.is_dir():
                continue

            collector_name = collector_dir.name
            retention_days = config.get_retention_days(collector_name)
            cutoff_date = now_ts - timedelta(days=retention_days)

            date_dirs = self._find_date_dirs(collector_dir)
            deleted_count = 0

            for date_str, date_dir in date_dirs:
                # 檢查是否超過保留天數
                try:
                    dir_date = datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError:
                    continue

                if dir_date >= cutoff_date:
                    continue

                s3_key = f"{collector_name}/archives/{date_str}.tar.gz"
                json_files = list(date_dir.glob('*.json'))
                if not json_files:
                    continue
                members = self._member_manifest(date_dir)
                if not self._has_only_manifest_files(date_dir, members):
                    continue
                receipt = self._receipt_matches(collector_name, date_str, s3_key, members)
                if receipt is None:
                    continue
                identity = self.s3.archive_identity(s3_key)
                if identity['status'] != 'present' or identity['identity'] != receipt['remote_identity']:
                    continue

                # 刪除整個日期目錄
                shutil.rmtree(date_dir)
                deleted_count += 1
                stats['deleted'] += 1

            if deleted_count > 0:
                print(f"   ✓ {collector_name} ({retention_days} 天): 刪除 {deleted_count} 個過期日期目錄")

                # 清理空的年/月目錄
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

            # 如果 S3 可用，統計歸檔數量
            if self.s3:
                archive_prefix = f"{collector_dir.name}/archives/"
                archive_files = self.s3.list_files(archive_prefix)
                collector_status['s3_archives'] = len(archive_files)
                s3_size = sum(f['size'] for f in archive_files)
                collector_status['s3_size_mb'] = round(s3_size / (1024 * 1024), 2)

            status['collectors'].append(collector_status)

        return status
