"""
中共解放軍臺海周邊海、空域動態 — 每日通報收集器

資料來源：mnd.gov.tw 中文官網（每日 06:00 (UTC+8) 截止，約 08-10 點發布）
  列表頁：https://www.mnd.gov.tw/news/plaactlist  → 含最近 ~10 個 news/plaact/{id}
  詳細頁：https://www.mnd.gov.tw/news/plaact/{id}
  格式公式化：
    「一、日期：中華民國{ROC_YEAR}年{M}月{D}日（星期X）0600時至…0600時止。」
    「二、活動動態：迄0600時止，偵獲共機 {N} 架次、共艦 {N} 艘、公務船 {N} 艘…
       其中共機 {N} 架次逾越海峽中線及進入我西南、東部空域…」
    「三、上述期間未偵獲共機，故無提供航跡圖。」（共機為 0 時）

寫入：
  - realtime.pla_activity_daily（PK = report_date，UPSERT by date）

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

# 詳細頁解析
_RE_DATE_ROC = re.compile(
    r"中華民國\s*(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日"
)
_RE_AIRCRAFT = re.compile(r"偵獲\s*共機\s*(\d+)\s*架次")
_RE_VESSELS  = re.compile(r"(?:偵獲\s*)?共艦\s*(\d+)\s*艘")
_RE_OFFICIAL = re.compile(r"公務船\s*(\d+)\s*艘")
_RE_CROSSED  = re.compile(r"(\d+)\s*架次\s*逾越.{0,10}中線")

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

    # ROC 年月日 → 西元日（截止當日為 report_date）
    m = _RE_DATE_ROC.search(text)
    if not m:
        return None
    roc_y, mon, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    report_date = date(roc_y + 1911, mon, day)

    sorties  = _int_match(_RE_AIRCRAFT.search(text))
    if sorties is None and _RE_NO_AIRCRAFT.search(text):
        sorties = 0
    vessels  = _int_match(_RE_VESSELS.search(text))
    official = _int_match(_RE_OFFICIAL.search(text))
    crossed  = _int_match(_RE_CROSSED.search(text))

    adiz = {k: bool(pat.search(text)) for k, pat in _ADIZ_KEYWORDS.items()}

    return {
        "report_date":             report_date.isoformat(),
        "aircraft_sorties":        sorties,
        "plan_vessels":            vessels,
        "official_ships":          official,
        "crossed_median_line_cnt": crossed,
        **adiz,
        "raw_text":                text[:2000],
        "source_lang":             "zh",
    }


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
        text = _strip_html(resp.text)
        parsed = parse_pla_detail(text)
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
