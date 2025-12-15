"""
本地檔案儲存

適用於開發測試環境。
"""

import json
from datetime import datetime
from pathlib import Path

import config


class LocalStorage:
    """本地檔案儲存"""

    def __init__(self, base_dir: Path = None):
        self.base_dir = base_dir or config.LOCAL_DATA_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, collector_name: str, data: dict, timestamp: datetime = None) -> Path:
        """儲存資料

        Args:
            collector_name: 收集器名稱（作為子目錄）
            data: 要儲存的資料
            timestamp: 時間戳記（預設為現在）

        Returns:
            Path: 儲存的檔案路徑
        """
        timestamp = timestamp or datetime.now()
        date_str = timestamp.strftime('%Y/%m/%d')
        time_str = timestamp.strftime('%H%M')

        # 建立目錄
        output_dir = self.base_dir / collector_name / date_str
        output_dir.mkdir(parents=True, exist_ok=True)

        # 儲存資料
        filename = f"{collector_name}_{time_str}.json"
        filepath = output_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # 更新 latest
        latest_dir = self.base_dir / collector_name
        latest_file = latest_dir / 'latest.json'
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return filepath

    def save_append(self, collector_name: str, records: list, timestamp: datetime = None) -> Path:
        """追加儲存（JSONL 格式）

        Args:
            collector_name: 收集器名稱
            records: 要追加的記錄列表
            timestamp: 時間戳記

        Returns:
            Path: 儲存的檔案路徑
        """
        timestamp = timestamp or datetime.now()
        date_str = timestamp.strftime('%Y/%m/%d')

        # 建立目錄
        output_dir = self.base_dir / collector_name / date_str
        output_dir.mkdir(parents=True, exist_ok=True)

        # 追加資料
        filename = f"{collector_name}_{timestamp.strftime('%Y%m%d')}.jsonl"
        filepath = output_dir / filename

        with open(filepath, 'a', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')

        return filepath

    def get_latest(self, collector_name: str) -> dict:
        """取得最新資料"""
        latest_file = self.base_dir / collector_name / 'latest.json'
        if latest_file.exists():
            with open(latest_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
