#!/usr/bin/env python3
"""
回填 live.pla_activity_daily — 重爬 mnd.gov.tw 歷史通報。

背景（2026-08-01 稽核，見 gis-platform migration 326 檔頭）：
  1. 解析三 bug：crossed_median_line_cnt 全 NULL、ADIZ 只標最後一區、
     report_date 語意誤註。解析器已修（collectors/pla_activity_daily.py）。
  2. raw_text 舊版只存頁面 chrome → 無法從 DB 重解析，必須重爬。
  3. 航跡示意圖是機型明細的唯一載體（PT-0 向量化計畫 Phase 0）→ 一併下載存 S3。

列表頁路徑式翻頁：https://www.mnd.gov.tw/news/plaactlist/{page}（1 頁 9 則，
共 216 頁 ≈ 1,940 則，最舊 2020-09-17）。近兩年 ≈ 730 則 ≈ 82 頁。

用法：
    python3 scripts/backfill_pla_activity.py --dry-run --pages 2      # 只看解析結果
    python3 scripts/backfill_pla_activity.py --days 730               # 近兩年（預設）
    python3 scripts/backfill_pla_activity.py --days 730 --resume      # 中斷後續跑
    python3 scripts/backfill_pla_activity.py --days 730 --no-s3       # 不上傳圖
    python3 scripts/backfill_pla_activity.py --all                    # 全量 5 年
"""
import os
import sys
import time
import argparse
import logging
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import urllib3
import psycopg2
from psycopg2.extras import execute_batch

import config
from collectors.pla_activity_daily import LIST_URL, DETAIL_URL, parse_pla_page, _RE_LIST_ITEM

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

LIST_PAGE_URL = LIST_URL + "/{page}"
REQ_DELAY = 0.5          # 溫柔速率
S3_PREFIX = "pla/track_charts"
S3_ACTIVITY_PREFIX = "pla/activity_charts"   # 圖片版時代的數字表格圖（待 OCR）

UPSERT_SQL = """
INSERT INTO live.pla_activity_daily (
    report_date, period_start, period_end,
    aircraft_sorties, crossed_median_line_cnt, plan_vessels, official_ships,
    adiz_north, adiz_central, adiz_southwestern, adiz_eastern,
    raw_text, track_chart_url, activity_chart_url,
    source_lang, source_url, collected_at, updated_at
) VALUES (%(report_date)s, %(period_start)s, %(period_end)s,
    %(aircraft_sorties)s, %(crossed_median_line_cnt)s, %(plan_vessels)s, %(official_ships)s,
    %(adiz_north)s, %(adiz_central)s, %(adiz_southwestern)s, %(adiz_eastern)s,
    %(raw_text)s, %(track_chart_url)s, %(activity_chart_url)s,
    %(source_lang)s, %(source_url)s, now(), now())
ON CONFLICT (report_date) DO UPDATE SET
    period_start            = EXCLUDED.period_start,
    period_end              = EXCLUDED.period_end,
    -- 圖片版數值全 NULL，不得覆蓋既有已解析的文字版數值 → COALESCE 保留舊值
    aircraft_sorties        = COALESCE(EXCLUDED.aircraft_sorties, live.pla_activity_daily.aircraft_sorties),
    crossed_median_line_cnt = COALESCE(EXCLUDED.crossed_median_line_cnt, live.pla_activity_daily.crossed_median_line_cnt),
    plan_vessels            = COALESCE(EXCLUDED.plan_vessels, live.pla_activity_daily.plan_vessels),
    official_ships          = COALESCE(EXCLUDED.official_ships, live.pla_activity_daily.official_ships),
    adiz_north              = COALESCE(EXCLUDED.adiz_north, live.pla_activity_daily.adiz_north),
    adiz_central            = COALESCE(EXCLUDED.adiz_central, live.pla_activity_daily.adiz_central),
    adiz_southwestern       = COALESCE(EXCLUDED.adiz_southwestern, live.pla_activity_daily.adiz_southwestern),
    adiz_eastern            = COALESCE(EXCLUDED.adiz_eastern, live.pla_activity_daily.adiz_eastern),
    raw_text                = COALESCE(EXCLUDED.raw_text, live.pla_activity_daily.raw_text),
    track_chart_url         = COALESCE(EXCLUDED.track_chart_url, live.pla_activity_daily.track_chart_url),
    activity_chart_url      = COALESCE(EXCLUDED.activity_chart_url, live.pla_activity_daily.activity_chart_url),
    source_lang             = EXCLUDED.source_lang,
    source_url              = EXCLUDED.source_url,
    updated_at              = now()
"""


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml",
        "Accept-Language": "zh-TW,zh;q=0.9",
    })
    s.verify = False
    return s


def fetch_list_page(sess: requests.Session, page: int) -> list[int]:
    resp = sess.get(LIST_PAGE_URL.format(page=page), timeout=config.REQUEST_TIMEOUT)
    resp.raise_for_status()
    ids, seen = [], set()
    for m in _RE_LIST_ITEM.finditer(resp.text):
        nid = int(m.group(1))
        if nid not in seen:
            seen.add(nid)
            ids.append(nid)
    return ids


def upload_chart(s3, sess: requests.Session, url: str, report_date: str,
                 prefix: str = S3_PREFIX) -> str | None:
    """下載圖並上傳 S3，回傳 object key（失敗回 None）。已存在則跳過下載。"""
    y, m = report_date[:4], report_date[5:7]
    ext = os.path.splitext(url.split("?")[0])[1] or ".jpg"
    key = f"{prefix}/{y}/{m}/{report_date}{ext}"
    try:
        s3.s3.head_object(Bucket=s3.bucket, Key=key)
        return key                      # 已備份，冪等跳過
    except Exception:
        pass
    try:
        # ⚠ 必須覆寫 Accept — session 預設只收 text/html，抓 JPG 會被 mnd 回 406
        r = sess.get(url, timeout=config.REQUEST_TIMEOUT,
                     headers={"Accept": "image/*,*/*"})
        r.raise_for_status()
        s3.s3.put_object(Bucket=s3.bucket, Key=key, Body=r.content,
                         ContentType=r.headers.get("Content-Type", "image/jpeg"))
        return key
    except Exception as e:
        logger.warning("  ⚠ 圖上傳失敗 %s: %s", url[:80], e)
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=730, help="回填最近 N 天（預設 730 = 近兩年）")
    ap.add_argument("--all", action="store_true", help="全量（忽略 --days）")
    ap.add_argument("--pages", type=int, help="只掃前 N 頁（測試用）")
    ap.add_argument("--dry-run", action="store_true", help="只解析不寫 DB / 不上傳")
    ap.add_argument("--no-s3", action="store_true", help="不上傳航跡圖")
    ap.add_argument("--resume", action="store_true",
                    help="跳過已由新版解析器處理過的 nid（period_end 或 activity_chart_url 非空）")
    args = ap.parse_args()

    cutoff = None if args.all else (date.today() - timedelta(days=args.days))
    sess = make_session()

    s3 = None
    if not args.no_s3 and not args.dry_run:
        from storage.s3 import S3Storage
        s3 = S3Storage()
        logger.info("S3 bucket=%s prefix=%s", s3.bucket, S3_PREFIX)

    conn = None
    done_nids: set[int] = set()
    if not args.dry_run:
        conn = psycopg2.connect(config.SUPABASE_DB_URL)
        if args.resume:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source_url FROM live.pla_activity_daily
                     WHERE source_url IS NOT NULL
                       AND (period_end IS NOT NULL OR activity_chart_url IS NOT NULL)
                """)
                for (u,) in cur.fetchall():
                    m = re.search(r"/plaact/(\d+)", u or "")
                    if m:
                        done_nids.add(int(m.group(1)))
            logger.info("resume：跳過 %d 個已處理 nid", len(done_nids))

    def flush(batch: list[dict]) -> None:
        """每頁即時寫入 — 中斷不丟進度（原本累積到最後才寫，被 kill 就全丟）。"""
        if not batch or conn is None:
            return
        for r in batch:
            r.setdefault("track_chart_url", None)
            r.setdefault("activity_chart_url", None)
        with conn:
            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_SQL, batch, page_size=100)

    rows, seen_dates = [], set()
    stats = {"pages": 0, "nid": 0, "text": 0, "image": 0, "skip_old": 0, "unparsed": 0,
             "charts": 0, "activity_charts": 0, "no_chart": 0}
    page, stop = 1, False
    consec_old = 0          # 連續多少則早於 cutoff（列表順序未必嚴格單調 → 連 2 頁才停）

    while not stop:
        if args.pages and page > args.pages:
            break
        try:
            nids = fetch_list_page(sess, page)
        except requests.RequestException as e:
            logger.warning("列表頁 %d 失敗: %s", page, e)
            break
        if not nids:
            logger.info("列表頁 %d 無項目，結束", page)
            break
        stats["pages"] += 1

        page_rows: list[dict] = []
        for nid in nids:
            stats["nid"] += 1
            if nid in done_nids:
                stats["resumed"] = stats.get("resumed", 0) + 1
                continue
            time.sleep(REQ_DELAY)
            try:
                r = sess.get(DETAIL_URL.format(nid=nid), timeout=config.REQUEST_TIMEOUT)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.warning("  抓 %s 失敗: %s", nid, e)
                continue
            p = parse_pla_page(r.text)
            if not p:
                stats["unparsed"] += 1
                logger.debug("  nid=%s 無法解析（可能為早期機型格式）", nid)
                continue
            rd = p["report_date"]
            if cutoff and date.fromisoformat(rd) < cutoff:
                stats["skip_old"] += 1
                consec_old += 1
                if consec_old >= 18:            # 連 2 頁全過期才停（列表順序未必單調）
                    stop = True
                    break
                continue
            consec_old = 0
            if rd in seen_dates:
                continue
            seen_dates.add(rd)
            p["source_url"] = DETAIL_URL.format(nid=nid)
            p.setdefault("activity_chart_url", None)
            stats["image" if p.get("needs_ocr") else "text"] += 1

            chart = p.get("track_chart_url")
            if chart:
                stats["charts"] += 1
                if s3:
                    upload_chart(s3, sess, chart, rd)
            else:
                stats["no_chart"] += 1
            act = p.get("activity_chart_url")     # 圖片版的數字表格圖（OCR 原料）
            if act:
                stats["activity_charts"] += 1
                if s3:
                    upload_chart(s3, sess, act, rd, prefix=S3_ACTIVITY_PREFIX)
            rows.append(p)
            page_rows.append(p)

        if not args.dry_run:
            flush(page_rows)          # 每頁落地，被 kill 也不丟已爬的部分
        logger.info("page %-3d text=%d image=%d（最舊 %s）", page, stats["text"],
                    stats["image"], min(seen_dates) if seen_dates else "-")
        page += 1

    logger.info("爬取結束：%s", stats)

    if args.dry_run:
        for p in rows[:5] + rows[-3:]:
            logger.info("  %s %s sorties=%s crossed=%s adiz=%s chart=%s act=%s",
                        p["report_date"], "IMG" if p.get("needs_ocr") else "TXT",
                        p["aircraft_sorties"], p["crossed_median_line_cnt"],
                        [k[5:] for k in ("adiz_north", "adiz_central", "adiz_southwestern",
                                         "adiz_eastern") if p[k]],
                        bool(p.get("track_chart_url")), bool(p.get("activity_chart_url")))
        logger.info("dry-run：不寫 DB（共 %d 列）", len(rows))
        return

    logger.info("✅ 已 upsert %d 列進 live.pla_activity_daily（每頁即時落地）", len(rows))
    conn.close()


if __name__ == "__main__":
    main()
