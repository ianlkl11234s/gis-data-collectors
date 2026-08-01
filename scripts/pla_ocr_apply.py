#!/usr/bin/env python3
"""
圖片版通報轉錄 — 步驟 3／3：套用。

讀 subagent 產出的轉錄檔（data/raw/pla_ocr/transcripts/{date}.txt），
餵既有 `parse_pla_detail()` 產生數值 → UPSERT live.pla_activity_daily。

⚠ 設計原則：LLM 只負責抄字，**數值一律由確定性 regex 解析器產生**，
與文字版共用同一套邏輯與 9 個單元測試（含「無括號子句 = crossed 0 非 NULL」等規則）。

英文交叉驗證：圖上自帶等義英文段（"12 PLA aircraft, 5 PLAN vessels and 1
official ship..."）。中英數字不一致者不靜默採用，標 source_lang='zh?' 待複核。

用法：
    python3 scripts/pla_ocr_apply.py --dry-run
    python3 scripts/pla_ocr_apply.py
"""
import os
import re
import sys
import glob
import argparse
import logging
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_batch

import config
from collectors.pla_activity_daily import parse_pla_detail

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

TXT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "raw", "pla_ocr", "transcripts")

# 英文段數字（交叉驗證用）："12 PLA aircraft, 5 PLAN vessels and 1 official ship"
#                          "9 of the aircraft crossed the median line"
_EN_AIRCRAFT = re.compile(r"(\d+)\s+PLA\s+aircraft", re.I)
_EN_VESSELS = re.compile(r"(\d+)\s+PLAN\s+vessel", re.I)
_EN_OFFICIAL = re.compile(r"(\d+)\s+official\s+ship", re.I)
_EN_CROSSED = re.compile(r"(\d+)\s+of\s+the\s+aircraft\s+crossed", re.I)

UPSERT_SQL = """
UPDATE live.pla_activity_daily SET
    aircraft_sorties        = %(aircraft_sorties)s,
    crossed_median_line_cnt = %(crossed_median_line_cnt)s,
    plan_vessels            = %(plan_vessels)s,
    official_ships          = %(official_ships)s,
    adiz_north              = %(adiz_north)s,
    adiz_central            = %(adiz_central)s,
    adiz_southwestern       = %(adiz_southwestern)s,
    adiz_eastern            = %(adiz_eastern)s,
    period_start            = %(period_start)s,
    period_end              = %(period_end)s,
    raw_text                = %(raw_text)s,
    source_lang             = %(source_lang)s,
    updated_at              = now()
WHERE report_date = %(report_date)s
"""


def cross_check(parsed: dict, text: str) -> tuple[bool, list[str]]:
    """中英數字比對。回傳 (一致, 不符欄位清單)。英文段缺漏不算不一致。"""
    pairs = [
        ("aircraft_sorties", _EN_AIRCRAFT),
        ("plan_vessels", _EN_VESSELS),
        ("official_ships", _EN_OFFICIAL),
        ("crossed_median_line_cnt", _EN_CROSSED),
    ]
    bad = []
    for key, pat in pairs:
        m = pat.search(text)
        if not m:
            continue
        if parsed.get(key) is not None and int(m.group(1)) != parsed[key]:
            bad.append(f"{key}: zh={parsed[key]} en={m.group(1)}")
    return (not bad), bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(TXT_DIR, "*.txt")))
    logger.info("轉錄檔 %d 份", len(files))

    rows, stats = [], {"ok": 0, "unparsed": 0, "mismatch": 0, "date_mismatch": 0}
    for path in files:
        stem = os.path.splitext(os.path.basename(path))[0]
        with open(path, encoding="utf-8") as f:
            text = " ".join(f.read().split())
        if not text:
            continue
        p = parse_pla_detail(text)
        if not p:
            stats["unparsed"] += 1
            logger.warning("  ✗ 解析失敗 %s（轉錄品質不足或版型特殊）", stem)
            continue
        # 檔名日期（來自 DB report_date）應與轉錄內文日期一致 → 不一致代表轉錯圖
        if p["report_date"] != stem:
            stats["date_mismatch"] += 1
            logger.warning("  ✗ 日期不符 %s vs 內文 %s — 跳過", stem, p["report_date"])
            continue
        ok, bad = cross_check(p, text)
        if not ok:
            stats["mismatch"] += 1
            logger.warning("  ⚠ 中英不符 %s: %s → 標記待複核", stem, "; ".join(bad))
        p["source_lang"] = "zh" if ok else "zh?"
        p["raw_text"] = text[:4000]
        stats["ok"] += 1
        rows.append(p)

    logger.info("解析結果：%s", stats)
    if args.dry_run:
        for p in rows[:5]:
            logger.info("  %s sorties=%s crossed=%s vessels=%s lang=%s",
                        p["report_date"], p["aircraft_sorties"],
                        p["crossed_median_line_cnt"], p["plan_vessels"], p["source_lang"])
        logger.info("dry-run：不寫 DB")
        return

    conn = psycopg2.connect(config.SUPABASE_DB_URL)
    with conn:
        with conn.cursor() as cur:
            execute_batch(cur, UPSERT_SQL, rows, page_size=100)
    conn.close()
    logger.info("✅ 更新 %d 列（%d 列標記待複核）", len(rows), stats["mismatch"])


if __name__ == "__main__":
    main()
