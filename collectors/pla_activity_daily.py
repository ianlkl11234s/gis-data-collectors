"""
中共解放軍臺海周邊海、空域動態 — 每日通報收集器

資料來源：mnd.gov.tw 中文官網（每日 06:00 (UTC+8) 截止，約 08-10 點發布）
  列表頁：https://www.mnd.gov.tw/news/plaactlist  → 含最近 ~10 個 news/plaact/{id}
  詳細頁：https://www.mnd.gov.tw/news/plaact/{id}
  格式公式化（2026-08 現行句型；舊句型以 fallback regex 支援）：
    「一、日期：中華民國{Y}年{M}月{D}日（星期X）0600時至{Y}年{M}月{D+1}日…0600時止。」
    「二、活動動態：迄0600時止，偵獲共機27架次（逾越中線進入北部、中部、西南及
       東部空域22架次）、共艦9艘及公務船2艘…」← 括號數字在句尾；
       未逾越中線時寫「（進入西南及東部空域5架次）」（無「逾越中線」字樣 = 0 逾越）
    「三、上述期間未偵獲共機，故無提供航跡圖。」（共機為 0 時）
  report_date = 起算日（活動主要發生日）；起訖另存 period_start / period_end。

寫入：
  - live.pla_activity_daily（PK = report_date，UPSERT by date）

⚠ 政治敏感：用「中共解放軍臺海周邊海、空域動態」官方語彙，不用「擾台」「侵擾」。
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# mnd.gov.tw 同 NHI / CDC 憑證缺 SKI，verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LIST_URL   = "https://www.mnd.gov.tw/news/plaactlist"
DETAIL_URL = "https://www.mnd.gov.tw/news/plaact/{nid}"

# 從列表頁找 detail 連結
_RE_LIST_ITEM = re.compile(r'<a\s+href="news/plaact/(\d+)"', re.IGNORECASE)

# 詳細頁內文容器（避免 raw_text 存到頁面 chrome；抓不到時 fallback 全頁）
_RE_MAINCONTENT = re.compile(r'<div class="maincontent">(.*?)</div>', re.S | re.I)

# 詳細頁解析
_RE_DATE_ROC = re.compile(
    r"中華民國\s*(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日"
)
# 一、日期：「中華民國115年7月31日（星期五）0600時至115年8月1日（星期六）0600時止」
# → 起訖兩個 ROC 日期（第二個日期通常不帶「中華民國」前綴）
_RE_PERIOD = re.compile(
    r"中華民國\s*(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日[^0-9]{0,12}?0600\s*時至"
    r"(?:中華民國)?\s*(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日"
)
# ⚠ 單架時國防部寫「1架」不是「1架次」，同一句還會混用
#   （「偵獲共機3架次（逾越中線進入西南空域1架）」）→ 一律吃「架」「架次」
_RE_AIRCRAFT = re.compile(r"偵獲\s*共機\s*(\d+)\s*架")
_RE_VESSELS  = re.compile(r"(?:偵獲\s*)?共艦\s*(\d+)\s*艘")
_RE_OFFICIAL = re.compile(r"公務船\s*(\d+)\s*艘")
# 現行句型：架次後接括號子句，數字在句尾 —
#   「偵獲共機27架次（逾越中線進入北部、中部、西南及東部空域22架次）」
#   「偵獲共機5架次（進入西南及東部空域5架次）」← 未提逾越中線 = 當日 0 逾越
_RE_AIR_CLAUSE = re.compile(r"偵獲\s*共機\s*\d+\s*架(?:次)?\s*[（(]([^（）()]{0,120})[)）]")
_RE_CLAUSE_CNT = re.compile(r"(\d+)\s*架")
# 舊句型 fallback：「其中共機 N 架次逾越海峽中線…」（數字在前）
_RE_CROSSED_OLD = re.compile(r"(\d+)\s*架(?:次)?\s*逾越.{0,10}中線")
# 航跡圖（臺海周邊海、空域活動示意圖）
_RE_TRACK_IMG = re.compile(r'src="(https?://www\.mnd\.gov\.tw/NewUpload/[^"]+)"', re.I)

# ADIZ 分區：括號子句內以子字串逐區判斷（頓號列舉「北部、中部、西南及東部空域」
# 舊版 adjacency regex 只會命中緊貼「空域」的最後一區 → 系統性漏標）
_ADIZ_SUBSTR = {
    "adiz_north":        "北部",
    "adiz_central":      "中部",
    "adiz_southwestern": "西南",
    "adiz_eastern":      "東部",
}
# 無括號子句時的 fallback（舊句型）
_ADIZ_KEYWORDS = {
    "adiz_north":         re.compile(r"我?\s*北部\s*空域"),
    "adiz_central":       re.compile(r"我?\s*中部\s*空域"),
    "adiz_southwestern":  re.compile(r"我?\s*西南(?:部)?\s*空域"),
    "adiz_eastern":       re.compile(r"我?\s*東部\s*空域"),
}

# 將「未偵獲共機」明示為 0
_RE_NO_AIRCRAFT = re.compile(r"未偵獲\s*共機")


def _int_match(m: re.Match | None) -> Optional[int]:
    if m is None:
        return None
    try:
        return int(m.group(1))
    except (ValueError, IndexError):
        return None


def _strip_html(html: str) -> str:
    """非常輕量 HTML 移除（不依賴 BeautifulSoup）"""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>",  " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;",  "<", text)
    text = re.sub(r"&gt;",  ">", text)
    text = re.sub(r"\s+", " ", text)
    return text


def parse_pla_detail(text: str) -> dict | None:
    """從詳細頁文字解析結構化欄位。回傳 None 表示不是通報內容。"""
    if "中共解放軍臺海周邊" not in text and "區域動態" not in text:
        return None
    if "活動動態" not in text:
        # 不是通報、可能是其他類型新聞
        return None

    # 起訖日：「M/D 0600 時至 M+1/D 0600 時止」。report_date = 起算日
    # （活動主要發生日；與既有資料一致，migration 326 已更正欄位註解）
    pm = _RE_PERIOD.search(text)
    if pm:
        period_start = date(int(pm.group(1)) + 1911, int(pm.group(2)), int(pm.group(3)))
        period_end   = date(int(pm.group(4)) + 1911, int(pm.group(5)), int(pm.group(6)))
    else:
        m = _RE_DATE_ROC.search(text)
        if not m:
            return None
        period_start = date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        period_end = None
    report_date = period_start

    sorties  = _int_match(_RE_AIRCRAFT.search(text))
    if sorties is None and _RE_NO_AIRCRAFT.search(text):
        sorties = 0
    vessels  = _int_match(_RE_VESSELS.search(text))
    official = _int_match(_RE_OFFICIAL.search(text))

    # 逾越中線（官方合併語意「逾越中線及進入空域」架次）＋ ADIZ 分區
    clause_m = _RE_AIR_CLAUSE.search(text)
    clause = clause_m.group(1) if clause_m else None
    crossed: Optional[int] = None
    if clause:
        n = _int_match(_RE_CLAUSE_CNT.search(clause))
        # 括號內有「逾越…中線」→ 句尾數字；只寫「進入…空域」→ 當日 0 逾越
        crossed = n if ("逾越" in clause and "中線" in clause) else 0
    if crossed is None:
        crossed = _int_match(_RE_CROSSED_OLD.search(text))
    if crossed is None and sorties == 0:
        crossed = 0

    if clause:
        adiz = {k: (kw in clause) for k, kw in _ADIZ_SUBSTR.items()}
    else:
        adiz = {k: bool(pat.search(text)) for k, pat in _ADIZ_KEYWORDS.items()}

    return {
        "report_date":             report_date.isoformat(),
        "period_start":            period_start.isoformat(),
        "period_end":              period_end.isoformat() if period_end else None,
        "aircraft_sorties":        sorties,
        "plan_vessels":            vessels,
        "official_ships":          official,
        "crossed_median_line_cnt": crossed,
        **adiz,
        "raw_text":                text[:4000],
        "source_lang":             "zh",
    }


def parse_pla_page(html: str) -> dict | None:
    """整頁 HTML → 抽 maincontent 內文 → 解析（含航跡圖 URL）。

    raw_text 只存內文（舊版存整頁 strip 前 2000 字全是導覽 chrome，回填不出
    任何欄位 → 一律以內文為準，未來加欄位可直接從 DB 重解析）。
    """
    m = _RE_MAINCONTENT.search(html)
    content_html = m.group(1) if m else html
    parsed = parse_pla_detail(_strip_html(content_html))
    if parsed is None and m:
        # maincontent 抓到但 gate 沒過（版型變動保險）→ 全頁降級再試
        parsed = parse_pla_detail(_strip_html(html))
    if parsed:
        img = _RE_TRACK_IMG.search(content_html)
        parsed["track_chart_url"] = img.group(1) if img else None
    return parsed


class PlaActivityDailyCollector(BaseCollector):
    """共機通報每日收集器 — 抓 mnd.gov.tw 中文列表 → 詳細頁解析 → UPSERT by report_date

    每 30 min 抓最近 5 則（最新通報通常每天 1 則）。
    """

    name = "pla_activity_daily"
    interval_minutes = config.PLA_ACTIVITY_DAILY_INTERVAL

    DETAIL_LIMIT = 5  # 每次最多解析最新幾則

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "zh-TW,zh;q=0.9",
        })
        self._session.verify = False  # mnd.gov.tw SSL SKI 缺失

    def _fetch_list_ids(self) -> list[int]:
        resp = self._session.get(LIST_URL, timeout=config.REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        ids = []
        seen: set[int] = set()
        for m in _RE_LIST_ITEM.finditer(resp.text):
            nid = int(m.group(1))
            if nid not in seen:
                seen.add(nid)
                ids.append(nid)
        return ids[: self.DETAIL_LIMIT]

    def _fetch_detail(self, nid: int) -> dict | None:
        url = DETAIL_URL.format(nid=nid)
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[{self.name}] ⚠ 抓 {nid} 失敗: {e}")
            return None
        parsed = parse_pla_page(resp.text)
        if parsed:
            parsed["source_url"] = url
        return parsed

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        try:
            nids = self._fetch_list_ids()
        except requests.RequestException as e:
            print(f"[{self.name}] ⚠ 抓列表失敗: {e}")
            nids = []

        # UPSERT by report_date：同日多則保留最新（list 順序新→舊，先寫新後寫舊）
        records: dict[str, dict] = {}
        for nid in nids:
            parsed = self._fetch_detail(nid)
            if not parsed:
                continue
            parsed["collected_at"] = now.isoformat()
            rd = parsed["report_date"]
            # 第一個（最新）保留
            if rd not in records:
                records[rd] = parsed

        rows = list(records.values())
        return {
            "data":          rows,
            "list_ids":      nids,
            "parsed_count":  len(rows),
            "collected_at":  now.isoformat(),
        }
