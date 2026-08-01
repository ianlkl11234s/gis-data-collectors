#!/usr/bin/env python3
"""
圖片版通報轉錄 — 步驟 1／3：備料。

背景：國防部通報 2025-02-02 以前為「圖片版」，網頁內文為空，架次/共艦等數值
只存在於「臺海周邊海、空域活動」JPG（中英雙語通報全文，印刷體）。
本流程讓 Claude Code subagent **看圖逐字轉錄**（走訂閱額度，不打 API），
轉錄結果再餵既有 `parse_pla_detail()` 產生數值 —— LLM 只抄字、不判讀數字。

本腳本：從 DB 撈待轉錄清單 → 下載圖到本地 → 切批次清單供 subagent 認領。

用法：
    python3 scripts/pla_ocr_prepare.py --limit 200 --batch-size 25
    python3 scripts/pla_ocr_prepare.py --status          # 只看進度
"""
import os
import sys
import json
import time
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import urllib3
import psycopg2

import config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

WORK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data", "raw", "pla_ocr")
IMG_DIR = os.path.join(WORK_DIR, "images")
TXT_DIR = os.path.join(WORK_DIR, "transcripts")
BATCH_DIR = os.path.join(WORK_DIR, "batches")

PENDING_SQL = """
    SELECT report_date, activity_chart_url
      FROM live.pla_activity_daily
     WHERE activity_chart_url IS NOT NULL
       AND raw_text IS NULL
     ORDER BY report_date DESC
     LIMIT %s
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--batch-size", type=int, default=25)
    ap.add_argument("--status", action="store_true", help="只顯示進度")
    args = ap.parse_args()

    for d in (IMG_DIR, TXT_DIR, BATCH_DIR):
        os.makedirs(d, exist_ok=True)

    conn = psycopg2.connect(config.SUPABASE_DB_URL)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FILTER (WHERE activity_chart_url IS NOT NULL AND raw_text IS NULL),
                   count(*) FILTER (WHERE activity_chart_url IS NOT NULL),
                   count(*)
              FROM live.pla_activity_daily
        """)
        pending, img_era, total = cur.fetchone()
    done_txt = len([f for f in os.listdir(TXT_DIR) if f.endswith(".txt")])
    logger.info("DB 共 %d 列 / 圖片版 %d 列 / 待轉錄 %d 列；本地已轉錄 %d",
                total, img_era, pending, done_txt)
    if args.status:
        conn.close()
        return

    with conn.cursor() as cur:
        cur.execute(PENDING_SQL, (args.limit,))
        rows = cur.fetchall()
    conn.close()

    sess = requests.Session()
    sess.verify = False
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0)",
        "Accept": "image/*,*/*",          # mnd 對 text/html Accept 回 406
    })

    todo = []
    for report_date, url in rows:
        d = report_date.isoformat()
        if os.path.exists(os.path.join(TXT_DIR, f"{d}.txt")):
            continue                       # 已轉錄，冪等跳過
        path = os.path.join(IMG_DIR, f"{d}.jpg")
        if not os.path.exists(path):
            try:
                r = sess.get(url, timeout=config.REQUEST_TIMEOUT)
                r.raise_for_status()
                with open(path, "wb") as f:
                    f.write(r.content)
                time.sleep(0.4)
            except requests.RequestException as e:
                logger.warning("下載失敗 %s: %s", d, e)
                continue
        todo.append({"date": d, "image": path,
                     "out": os.path.join(TXT_DIR, f"{d}.txt")})

    batches = [todo[i:i + args.batch_size] for i in range(0, len(todo), args.batch_size)]
    for i, b in enumerate(batches, 1):
        p = os.path.join(BATCH_DIR, f"batch_{i:03d}.json")
        with open(p, "w") as f:
            json.dump(b, f, ensure_ascii=False, indent=1)
    logger.info("✅ 已備妥 %d 張圖 / %d 批 → %s", len(todo), len(batches), BATCH_DIR)


if __name__ == "__main__":
    main()
