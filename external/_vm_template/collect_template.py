#!/usr/bin/env python3
"""<name> 收集器範本：抓一輪 → 寫本機日期目錄 JSON（歸檔交給 archive_<name>.py）。"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parent
load_dotenv(APP_DIR / ".env")

COLLECTOR = "<name>"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/<app>/data"))
TAIPEI_TZ = timezone(timedelta(hours=8))


def fetch() -> list[dict]:
    """TODO：呼叫來源 API，回傳資料列。失敗就丟例外（cron log 會留下 traceback）。"""
    raise NotImplementedError


def write_snapshot(rows: list[dict], now: datetime) -> Path:
    date_dir = DATA_DIR / COLLECTOR / now.strftime("%Y/%m/%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    path = date_dir / f"{COLLECTOR}_{now.strftime('%H%M')}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"data": rows, "row_count": len(rows), "collected_at": now.isoformat()},
                              ensure_ascii=False))
    tmp.replace(path)  # 原子寫入，避免歸檔打包到半個檔
    return path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    now = datetime.now(TAIPEI_TZ)
    rows = fetch()
    path = write_snapshot(rows, now)
    logging.info(f"抓到 {len(rows)} 筆 → {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
