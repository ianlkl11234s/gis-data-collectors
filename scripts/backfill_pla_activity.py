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

UPSERT_SQL = """
INSERT INTO live.pla_activity_daily (
    report_date, period_start, period_end,
    aircraft_sorties, crossed_median_line_cnt, plan_vessels, official_ships,
    adiz_north, adiz_central, adiz_southwestern, adiz_eastern,
    raw_text, track_chart_url, source_lang, source_url, collected_at, updated_at
) VALUES (%(report_date)s, %(period_start)s, %(period_end)s,
    %(aircraft_sorties)s, %(crossed_median_line_cnt)s, %(plan_vessels)s, %(official_ships)s,
    %(adiz_north)s, %(adiz_central)s, %(adiz_southwestern)s, %(adiz_eastern)s,
    %(raw_text)s, %(track_chart_url)s, %(source_lang)s, %(source_url)s, now(), now())
ON CONFLICT (report_date) DO UPDATE SET
    period_start            = EXCLUDED.period_start,
    period_end              = EXCLUDED.period_end,
    aircraft_sorties        = EXCLUDED.aircraft_sorties,
    crossed_median_line_cnt = EXCLUDED.crossed_median_line_cnt,
    plan_vessels            = EXCLUDED.plan_vessels,
    official_ships          = EXCLUDED.official_ships,
    adiz_north              = EXCLUDED.adiz_north,
    adiz_central            = EXCLUDED.adiz_central,
    adiz_southwestern       = EXCLUDED.adiz_southwestern,
    adiz_eastern            = EXCLUDED.adiz_eastern,
    raw_text                = EXCLUDED.raw_text,
    track_chart_url         = EXCLUDED.track_chart_url,
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


def upload_chart(s3, sess: requests.Session, url: str, report_date: str) -> str | None:
    """下載航跡圖並上傳 S3，回傳 object key（失敗回 None）。已存在則跳過下載。"""
    y, m = report_date[:4], report_date[5:7]
    ext = os.path.splitext(url.split("?")[0])[1] or ".jpg"
    key = f"{S3_PREFIX}/{y}/{m}/{report_date}{ext}"
    try:
        s3.s3.head_object(Bucket=s3.bucket, Key=key)
        return key                      # 已備份，冪等跳過
    except Exception:
        pass
    try:
        r = sess.get(url, timeout=config.REQUEST_TIMEOUT)
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
    args = ap.parse_args()

    cutoff = None if args.all else (date.today() - timedelta(days=args.days))
    sess = make_session()

    s3 = None
    if not args.no_s3 and not args.dry_run:
        from storage.s3 import S3Storage
        s3 = S3Storage()
        logger.info("S3 bucket=%s prefix=%s", s3.bucket, S3_PREFIX)

    conn = None
    if not args.dry_run:
        conn = psycopg2.connect(config.SUPABASE_DB_URL)

    rows, seen_dates = [], set()
    stats = {"pages": 0, "nid": 0, "parsed": 0, "skip_old": 0, "unparsed": 0,
             "charts": 0, "no_chart": 0}
    page, stop = 1, False

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

        for nid in nids:
            stats["nid"] += 1
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
                stop = True                     # 列表新→舊，越過 cutoff 即可停
                break
            if rd in seen_dates:
                continue
            seen_dates.add(rd)
            p["source_url"] = DETAIL_URL.format(nid=nid)
            chart = p.get("track_chart_url")
            if chart:
                stats["charts"] += 1
                if s3:
                    upload_chart(s3, sess, chart, rd)
            else:
                stats["no_chart"] += 1
            stats["parsed"] += 1
            rows.append(p)

        logger.info("page %-3d 累計 parsed=%d（最舊 %s）", page, stats["parsed"],
                    min(seen_dates) if seen_dates else "-")
        page += 1

    logger.info("爬取結束：%s", stats)

    if args.dry_run:
        for p in rows[:5]:
            logger.info("  %s sorties=%s crossed=%s adiz=%s chart=%s", p["report_date"],
                        p["aircraft_sorties"], p["crossed_median_line_cnt"],
                        [k[5:] for k in ("adiz_north", "adiz_central", "adiz_southwestern",
                                         "adiz_eastern") if p[k]],
                        bool(p.get("track_chart_url")))
        logger.info("dry-run：不寫 DB（共 %d 列）", len(rows))
        return

    for p in rows:
        p.setdefault("track_chart_url", None)
    with conn:
        with conn.cursor() as cur:
            execute_batch(cur, UPSERT_SQL, rows, page_size=100)
    logger.info("✅ upsert %d 列進 live.pla_activity_daily", len(rows))
    conn.close()


if __name__ == "__main__":
    main()
