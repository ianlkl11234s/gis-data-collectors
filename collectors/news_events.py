"""
新聞事件收集器（news_events）

長期收集台灣即時新聞 RSS，經 URL 正規化 + simhash 跨媒體去重後，
用 Gemini Flash-Lite（預設）或 OpenRouter batch（20 則 packing）抽取「正規化地點（縣市+鄉鎮）+
分類 + 摘要」，寫入 live.news_events。每 20 分鐘執行一次。

資料來源（URL 來自 mini-taiwan-pulse/docs/research/news-layer-revival-2026-06.md）：
    - 中央社 CNA：feedburner rsscna/{local,social,lifehealth}
    - 自由時報 LTN：news.ltn.com.tw/rss/{all,society,local}.xml
    - ETtoday：feedburner ettoday/realtime
    - Google News geo feed × 22 縣市（自帶縣市標籤 → 當 LLM 縣市 hint）

設計重點：
    - 每個 feed 之間 sleep 2~3 秒（Google News 有 429 限流）
    - 單一 feed 失敗不影響其他 feed（log + continue）
    - URL 去 tracking params；Google News redirect 嘗試離線 base64 解碼，
      解不出（新格式 AU_yqL…）就用 Google News URL 本身當 url_norm
    - 去重兩層：
        (a) url_norm 對 DB 近 7 天集合 + 同批內
        (b) 標題 64-bit simhash（中文 2-gram）hamming distance <= 3 視為跨媒體重複
    - LLM **絕不輸出座標**：只輸出清單內的正規化 county/township，
      回來再過 368 鄉鎮白名單驗證（township 不合法 → 降級 county-only；
      county 也不合法 → 地點欄位留 NULL，照樣入庫）
    - geom 完全不寫：DB trigger（migration 162）由 admin_code 查 township centroid
    - title_simhash 以 signed 64-bit int 寫入（pg BIGINT 範圍）
    - 368 鄉鎮清單啟動時查 spatial.township_boundaries 一次，
      cache 到本地 json（DB 不可用時 fallback 讀檔）
    - 記錄每輪 LLM token 用量與估算成本（驗收月成本 $1–5 USD）

手動執行：
    離線試跑（抓 feed + 去重，不打 LLM、不寫 DB）：
        python3 -m collectors.news_events --dry-run
    完整單次執行（會寫 DB、打 LLM）：
        NEWS_EVENTS_ENABLED=true SUPABASE_ENABLED=true python3 -c \\
            "from collectors.news_events import NewsEventsCollector; NewsEventsCollector().run()"
"""

import base64
import hashlib
import json
import logging
import re
import time
import urllib.parse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import config
from .base import BaseCollector, TAIPEI_TZ

logger = logging.getLogger(__name__)

# feedparser / google-genai 延遲匯入：registry 會在啟動時 import 本模組，
# 套件缺漏不應讓整個 main.py 掛掉，只在實際執行時報錯。
try:
    import feedparser
except ImportError:  # pragma: no cover
    feedparser = None


# ============================================================
# Feed 清單（URL 依研究報告 news-layer-revival-2026-06.md，皆 2026-06-12 實測過）
# ============================================================

GNEWS_COUNTIES = (
    '臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市',
    '基隆市', '新竹市', '嘉義市',
    '新竹縣', '苗栗縣', '彰化縣', '南投縣', '雲林縣', '嘉義縣',
    '屏東縣', '宜蘭縣', '花蓮縣', '臺東縣',
    '澎湖縣', '金門縣', '連江縣',
)


def _gnews_geo_url(county: str) -> str:
    return (
        "https://news.google.com/rss/headlines/section/geo/"
        f"{urllib.parse.quote(county)}?hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    )


def build_feed_list() -> list[dict]:
    """回傳 feed 設定：source / url / county_hint"""
    feeds = [
        {'source': 'cna', 'url': 'https://feeds.feedburner.com/rsscna/local', 'county_hint': None},
        {'source': 'cna', 'url': 'https://feeds.feedburner.com/rsscna/social', 'county_hint': None},
        {'source': 'cna', 'url': 'https://feeds.feedburner.com/rsscna/lifehealth', 'county_hint': None},
        {'source': 'ltn', 'url': 'https://news.ltn.com.tw/rss/all.xml', 'county_hint': None},
        {'source': 'ltn', 'url': 'https://news.ltn.com.tw/rss/society.xml', 'county_hint': None},
        {'source': 'ltn', 'url': 'https://news.ltn.com.tw/rss/local.xml', 'county_hint': None},
        {'source': 'ettoday', 'url': 'https://feeds.feedburner.com/ettoday/realtime', 'county_hint': None},
    ]
    for county in GNEWS_COUNTIES:
        feeds.append({'source': 'gnews', 'url': _gnews_geo_url(county), 'county_hint': county})
    return feeds


# ============================================================
# URL 正規化
# ============================================================

# 常見 tracking 參數（完整比對 + utm_ 前綴）
_TRACKING_PARAMS = {
    'fbclid', 'gclid', 'dclid', 'msclkid', 'twclid', 'igshid', 'mc_cid', 'mc_eid',
    'ref', 'referer', 'referrer', 'from', 'feature', 'spm', 'share', 'sfnsn',
    'oc',  # Google News redirect 附帶
}


def _is_tracking_param(key: str) -> bool:
    return key.lower().startswith('utm_') or key.lower() in _TRACKING_PARAMS


def decode_google_news_url(url: str) -> Optional[str]:
    """嘗試離線解出 Google News redirect 的真實 URL。

    舊格式 article id 是 urlsafe-base64 的 protobuf，內含
    b'\\x08\\x13\\x22' + len-prefix + 原始 URL（結尾常見 b'\\xd2\\x01\\x00'）。
    新格式（AU_yqL… 開頭）需打 Google 內部 API 才解得出 → 回傳 None，
    呼叫端 fallback 用 Google News URL 本身當 url_norm（不 hard fail）。
    """
    m = re.match(r'https?://news\.google\.com/(?:rss/)?articles/([^?/#]+)', url)
    if not m:
        return None
    encoded = m.group(1)
    try:
        # 補 padding
        decoded = base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4))
    except Exception:
        return None

    prefix = b'\x08\x13\x22'
    if not decoded.startswith(prefix):
        return None  # 新格式（無法離線解碼）
    decoded = decoded[len(prefix):]
    suffix = b'\xd2\x01\x00'
    if decoded.endswith(suffix):
        decoded = decoded[:-len(suffix)]
    if not decoded:
        return None
    # 長度前綴：< 0x80 一個 byte；>= 0x80 為兩 byte varint
    length = decoded[0]
    if length >= 0x80:
        if len(decoded) < 2:
            return None
        length = (length & 0x7F) | (decoded[1] << 7)
        payload = decoded[2:2 + length]
    else:
        payload = decoded[1:1 + length]
    try:
        real_url = payload.decode('utf-8')
    except UnicodeDecodeError:
        return None
    if real_url.startswith('http://') or real_url.startswith('https://'):
        return real_url
    return None


def normalize_url(url: str) -> str:
    """URL 正規化：解 Google News redirect、去 tracking params、去 fragment。

    解不出 redirect 時用 Google News URL 本身（去 query）當正規化結果。
    """
    if not url:
        return ''
    url = url.strip()

    if 'news.google.com' in url:
        real = decode_google_news_url(url)
        if real:
            url = real
        else:
            # 解不出 → 用 articles/<id> 路徑本身當穩定鍵（去 query/fragment）
            parts = urllib.parse.urlsplit(url)
            return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))

    parts = urllib.parse.urlsplit(url)
    query_pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    kept = [(k, v) for k, v in query_pairs if not _is_tracking_param(k)]
    new_query = urllib.parse.urlencode(kept)
    netloc = parts.netloc.lower()
    path = parts.path.rstrip('/') or '/'
    return urllib.parse.urlunsplit((parts.scheme.lower() or 'https', netloc, path, new_query, ''))


# ============================================================
# Simhash（64-bit，中文 2-gram）
# ============================================================

_NON_WORD_RE = re.compile(r'[\s\W_]+', re.UNICODE)
# Google News 標題尾巴媒體名（「標題 - 自由時報」），去重前先剝掉
_TITLE_SOURCE_SUFFIX_RE = re.compile(r'\s+[-|｜–—]\s*[^-|｜–—]{1,20}$')


def clean_title(title: str) -> str:
    """供 simhash 用的標題清洗：去媒體名尾巴、去標點空白、轉小寫、台→臺"""
    t = (title or '').strip()
    t = _TITLE_SOURCE_SUFFIX_RE.sub('', t)
    t = _NON_WORD_RE.sub('', t)
    return t.lower().replace('台', '臺')


def simhash64(text: str) -> int:
    """標準 64-bit simhash，中文用 2-gram 切詞。回傳 unsigned (0 ~ 2^64-1)。"""
    if not text:
        return 0
    if len(text) == 1:
        grams = [text]
    else:
        grams = [text[i:i + 2] for i in range(len(text) - 1)]

    v = [0] * 64
    for g in grams:
        h = int.from_bytes(hashlib.md5(g.encode('utf-8')).digest()[:8], 'big')
        for i in range(64):
            v[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(64):
        if v[i] > 0:
            out |= (1 << i)
    return out


def to_signed_64(u: int) -> int:
    """unsigned 64-bit → signed 64-bit（pg BIGINT 可存範圍）"""
    return u - (1 << 64) if u >= (1 << 63) else u


def to_unsigned_64(s: int) -> int:
    """signed 64-bit（DB 讀回）→ unsigned 64-bit（hamming 比對用）"""
    return s + (1 << 64) if s < 0 else s


def hamming_distance(a: int, b: int) -> int:
    return (a ^ b).bit_count()


SIMHASH_DUP_THRESHOLD = 3  # hamming distance <= 3 視為跨媒體重複


# ============================================================
# Location evidence POC（純 contract；尚未接 production DB）
# ============================================================

LOCATION_EVIDENCE_METHOD_VERSION = 'news-location-evidence-poc/v1'
LOCATION_SCOPES = ('taiwan_local', 'taiwan_multi', 'taiwan_national', 'foreign', 'unknown')
LOCATION_STATUSES = ('accepted', 'ambiguous', 'unresolved', 'rejected')
LOCATION_PRECISIONS = (
    'address', 'poi', 'intersection', 'road_segment', 'village', 'township',
    'county', 'country', 'none',
)
LOCATION_ROLES = (
    'event_site', 'affected_area', 'reporting_location', 'organization_location', 'background',
)

# 這是 POC 的小型明示詞表，不是完整 gazetteer，也不應取代既有 DB 白名單。
_TAIWAN_COUNTIES = (
    '臺北市', '新北市', '桃園市', '臺中市', '臺南市', '高雄市', '基隆市', '新竹市', '嘉義市',
    '新竹縣', '苗栗縣', '彰化縣', '南投縣', '雲林縣', '嘉義縣', '屏東縣', '宜蘭縣', '花蓮縣',
    '臺東縣', '澎湖縣', '金門縣', '連江縣',
)
_TOWNSHIP_COUNTY_HINTS = {
    # 鹿草是此 POC 必須保護的反例：Google News 的雲林 hint 不得覆寫文內地點。
    '鹿草鄉': '嘉義縣',
    '鹿草': '嘉義縣',
}
_FOREIGN_COUNTRIES = ('日本', '美國', '中國', '韓國', '南韓', '北韓', '香港', '澳洲', '英國', '法國')
_NATIONAL_MARKERS = ('全國', '全臺', '各縣市')


@dataclass(frozen=True)
class LocationEvidence:
    """新聞地點 evidence 的可序列化 POC contract。

    county_hint 僅作候選線索；規則明確命中文內地點時，resolved_county 必以文內結果為準。
    此 contract 是純函式輸出，尚未加入 live.news_events 寫入 payload。
    """

    location_scope: str
    location_status: str
    location_precision: str
    evidence_text: str
    evidence_field: str
    location_role: str
    resolver: str
    method_version: str
    resolved_county: Optional[str] = None
    county_hint: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ArticleRelationCandidate:
    """尚未寫入 DB 的 simhash 同稿候選；供後續人工或 resolver 判讀。"""

    left_article_key: str
    right_article_key: str
    relation_type: str
    hamming_distance: int
    resolver: str = 'simhash64-title-2gram'
    method_version: str = 'news-article-relation-poc/v1'

    def to_dict(self) -> dict:
        return asdict(self)


def _norm_taiwan_name(text: str) -> str:
    return (text or '').replace('台', '臺')


def _first_text_match(title: str, summary: str, candidates: tuple[str, ...]) -> tuple[str, str]:
    """依 title 優先回傳 (matched text, source field)。"""
    for field, text in (('title', title), ('summary', summary)):
        for candidate in candidates:
            if candidate in text:
                return candidate, field
    return '', ''


def build_location_evidence(article: dict) -> LocationEvidence:
    """以文章明示文字建立 location evidence，完全離線且不修改輸入 article。

    這是保守 POC：未命中文內地點時，不將 feed 的 county_hint 升格為已接受位置。
    """
    title = _norm_taiwan_name(str(article.get('title') or ''))
    summary = _norm_taiwan_name(str(article.get('summary') or ''))
    county_hint = _norm_taiwan_name(str(article.get('county_hint') or '')) or None
    corpus = f'{title} {summary}'

    foreign, foreign_field = _first_text_match(title, summary, _FOREIGN_COUNTRIES)
    matched_township, township_field = _first_text_match(
        title, summary, tuple(_TOWNSHIP_COUNTY_HINTS)
    )
    matched_counties = []
    county_field = ''
    for county in _TAIWAN_COUNTIES:
        if county in corpus:
            matched_counties.append(county)
            if not county_field:
                county_field = 'title' if county in title else 'summary'

    # 文內台灣地點的證據優先於國外背景詞（例如「日本旅客在嘉義縣…」）。
    if matched_township:
        county = _TOWNSHIP_COUNTY_HINTS[matched_township]
        return LocationEvidence(
            'taiwan_local', 'accepted', 'township', matched_township, township_field,
            'event_site', 'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION,
            county, county_hint,
        )

    if matched_counties:
        unique_counties = tuple(dict.fromkeys(matched_counties))
        if len(unique_counties) > 1:
            return LocationEvidence(
                'taiwan_multi', 'accepted', 'county', '、'.join(unique_counties), county_field,
                'affected_area', 'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION,
                None, county_hint,
            )
        return LocationEvidence(
            'taiwan_local', 'accepted', 'county', unique_counties[0], county_field,
            'event_site', 'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION,
            unique_counties[0], county_hint,
        )

    national, national_field = _first_text_match(title, summary, _NATIONAL_MARKERS)
    if national:
        return LocationEvidence(
            'taiwan_national', 'accepted', 'none', national, national_field,
            'affected_area', 'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION,
            None, county_hint,
        )
    if foreign:
        return LocationEvidence(
            'foreign', 'accepted', 'country', foreign, foreign_field,
            'event_site', 'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION,
            None, county_hint,
        )
    if county_hint:
        return LocationEvidence(
            'unknown', 'unresolved', 'none', county_hint, 'county_hint',
            'background', 'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION,
            None, county_hint,
        )
    return LocationEvidence(
        'unknown', 'unresolved', 'none', '', '', 'background',
        'news_location_rules', LOCATION_EVIDENCE_METHOD_VERSION, None, None,
    )


def location_evidence_json(article: dict) -> str:
    """供 fixture / downstream contract test 使用的穩定 JSON 表示。"""
    return json.dumps(build_location_evidence(article).to_dict(), ensure_ascii=False, sort_keys=True)


def _article_relation_key(article: dict, index: int) -> str:
    return str(article.get('article_id') or article.get('url_norm') or article.get('url') or index)


def build_article_relation_candidates(
        articles: list[dict], threshold: int = SIMHASH_DUP_THRESHOLD) -> list[ArticleRelationCandidate]:
    """列出 simhash 相近稿候選，不做捨棄、不寫入 production DB。"""
    candidates = []
    hashes = [
        to_unsigned_64(int(article['title_simhash']))
        if article.get('title_simhash') is not None
        else simhash64(clean_title(str(article.get('title') or '')))
        for article in articles
    ]
    article_keys = [_article_relation_key(article, index) for index, article in enumerate(articles)]
    for left in range(len(articles)):
        for right in range(left + 1, len(articles)):
            # 相同穩定身份表示輸入集重複同一篇文章，不是可供審查的文章間關聯。
            if article_keys[left] == article_keys[right]:
                continue
            distance = hamming_distance(hashes[left], hashes[right])
            if distance <= threshold:
                candidates.append(ArticleRelationCandidate(
                    article_keys[left], article_keys[right],
                    'same_story_candidate', distance,
                ))
    return candidates


def article_relation_candidates_json(articles: list[dict]) -> str:
    return json.dumps(
        [candidate.to_dict() for candidate in build_article_relation_candidates(articles)],
        ensure_ascii=False, sort_keys=True,
    )


# ============================================================
# 368 鄉鎮 gazetteer（白名單驗證）
# ============================================================

class TownshipGazetteer:
    """368 鄉鎮白名單 + 正規化查表。

    rows 來自 spatial.township_boundaries（code = 8 碼鄉鎮代碼，
    name = 「苗栗縣頭屋鄉」格式，縣市名固定 3 字前綴）。
    """

    def __init__(self, rows: list[dict]):
        # (county, township) -> admin_code
        self.township_codes: dict[tuple[str, str], str] = {}
        # county -> 5 碼縣市代碼（取轄下任一鄉鎮 code 前 5 碼）
        self.county_codes: dict[str, str] = {}
        for r in rows:
            code = str(r.get('code') or '')
            name = (r.get('name') or '').strip()
            if not code or len(name) < 4:
                continue
            county, township = name[:3], name[3:]
            self.township_codes[(county, township)] = code
            self.county_codes.setdefault(county, code[:5])

    @staticmethod
    def _norm(s: Optional[str]) -> str:
        return (s or '').strip().replace('台', '臺')

    def is_empty(self) -> bool:
        return not self.township_codes

    def county_list(self) -> list[str]:
        return sorted(self.county_codes)

    def prompt_lines(self) -> list[str]:
        """system prompt 用的完整清單（admin_code 縣市 鄉鎮）"""
        return [
            f"{code} {county} {township}"
            for (county, township), code in sorted(
                self.township_codes.items(), key=lambda kv: kv[1]
            )
        ]

    def validate(self, county: Optional[str], township: Optional[str]) -> dict:
        """白名單驗證 + 正規化。

        Returns:
            dict(county, township, admin_code, location_name)
            - township 合法 → admin_code 為 8 碼鄉鎮代碼
            - township 不合法但 county 合法 → 降級 county-only（5 碼縣市代碼）
            - county 也不合法 → 全 None（照樣入庫，只是沒地點）
        """
        c = self._norm(county)
        t = self._norm(township)

        if c not in self.county_codes:
            return {'county': None, 'township': None, 'admin_code': None, 'location_name': None}

        if t and (c, t) in self.township_codes:
            return {
                'county': c,
                'township': t,
                'admin_code': self.township_codes[(c, t)],
                'location_name': f'{c}{t}',
            }

        # township 缺 / 不在白名單 → 降級 county-only
        return {
            'county': c,
            'township': None,
            'admin_code': self.county_codes[c],
            'location_name': c,
        }


# ============================================================
# LLM（Gemini）設定
# ============================================================

LLM_BATCH_SIZE = 15  # v2 output 變多（多 3 欄），降 batch 防超時
LLM_BATCH_SLEEP = 0.5  # batch 間隔秒數
OPENROUTER_RETRY_DELAYS = (2.0, 5.0)  # 429 / 5xx / 連線 / read timeout 重試 2 次（backoff 秒數）

CATEGORY_ENUM = ('accident', 'crime', 'disaster', 'traffic', 'health', 'policy', 'other')


class NewsAnnotationError(RuntimeError):
    """新聞 annotation 不完整時中止整輪，避免未分類資料寫入。"""


# Gemini Flash-Lite 標準（非 batch API）單價，USD / 1M tokens（2026-06）
GEMINI_PRICE_INPUT_PER_MTOK = 0.10
GEMINI_PRICE_OUTPUT_PER_MTOK = 0.40

# v2 prompt（2026-06-13）：加 gis_relevance / severity / is_event 三維度，
# 讓前端能篩掉政績宣傳 / 純政治發言 / 體育娛樂 / 個人事件。
SYSTEM_PROMPT_HEADER = """你是台灣新聞地點抽取器 + GIS 相關性評估器。輸入是一批台灣新聞（每則含 idx、標題、摘要、可能的縣市提示 county_hint）。

對每一則新聞輸出一個 JSON 物件，全部組成 JSON array（嚴格 JSON，不要 markdown code block）：
{"idx": <輸入的 idx>, "county": "<縣市或 null>", "township": "<鄉鎮市區或 null>", "category": "<分類>", "summary": "<30 字內中文摘要>", "confidence": <0~1>, "gis_relevance": <0-3>, "severity": <0-3>, "is_event": <true|false>}

規則：
1. county / township 必須一字不差取自下方「行政區清單」中的名稱；不在清單內就回 null。
2. 只能判斷到縣市級時 township 給 null；完全沒有台灣地點時 county 與 township 都給 null。
3. 有 county_hint 時優先在該縣市內消歧（例如「中山區」依 hint 判斷屬於哪個縣市）。
4. category 必須是其中之一：accident, crime, disaster, traffic, health, policy, other。
5. 禁止輸出任何座標（經緯度）。
6. confidence：地點明確 0.9 以上、需推測 0.5~0.8、僅縣市級 0.5 左右、無地點 0。
7. 每一則輸入都要有對應輸出，idx 不可遺漏或重複。
8. gis_relevance（地理影響程度，0-3）：
   0 = 與地理空間無關（純政治發言／質詢／聲明、體育、娛樂、影劇、個人就醫、弊案調查、純政績宣傳、人物特寫）
   1 = 提到地點但事件本身不影響當地（座談會、紀念活動、人事任命、政策宣布、地方文化介紹）
   2 = 地方事件，影響有限（單一交通事故、個案治安、地方建設動工、地方節慶開幕、社區活動）
   3 = 重大地方事件，明顯影響當地（火災、氣爆、群聚感染≥50 人、大規模停水電、人員傷亡≥1、土石流、淹水）
9. severity（傷亡或影響規模，0-3）：
   0 = 無傷亡、無公共服務中斷
   1 = 個案（1 人受傷、輕微影響）
   2 = 區域（社區或街道規模、影響 < 100 人）
   3 = 大規模（縣市規模、影響 > 100 人或有死亡）
10. is_event：是否為發生於物理空間的「事件」
    true  = 火災、事故、群聚、活動舉辦、開幕、抗議、災害、犯罪行為
    false = 聲明、發言、質詢、評論、政策說明、回顧報導、人事任命、人物特寫

行政區清單（admin_code 縣市 鄉鎮市區）：
"""


# ============================================================
# Collector
# ============================================================

class NewsEventsCollector(BaseCollector):
    """新聞事件收集器（每 20 分鐘）"""

    name = "news_events"
    interval_minutes = getattr(config, 'NEWS_EVENTS_INTERVAL', 20)
    # 29 feeds × ~2.5s sleep + 抓取 + LLM batch，放寬到 10 分鐘
    COLLECT_TIMEOUT = 600

    FEED_SLEEP_SECONDS = 2.5  # 每個 feed 之間 sleep（Google News 429 限流）
    DEDUP_WINDOW_DAYS = 7
    SUMMARY_MAX_CHARS = 300   # 餵給 LLM / 入庫的摘要截斷長度

    def __init__(self, dry_run: bool = False):
        super().__init__()
        self.dry_run = dry_run
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (compatible; taipei-gis-analytics/news-collector; '
                '+https://github.com/mini-taiwan-pulse)'
            ),
            'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
        })
        self._gazetteer: Optional[TownshipGazetteer] = None
        self._system_prompt: Optional[str] = None
        self._llm_client = None

    # ------------------------------------------------------------
    # Gazetteer（368 鄉鎮清單，DB 查一次 + 本地 json cache）
    # ------------------------------------------------------------

    @property
    def _gazetteer_cache_path(self):
        return config.LOCAL_DATA_DIR / 'news_events' / 'township_gazetteer.json'

    def _load_gazetteer(self) -> TownshipGazetteer:
        if self._gazetteer is not None and not self._gazetteer.is_empty():
            return self._gazetteer

        rows: list[dict] = []

        # 1) 先試 DB（每個 process 只查一次）
        if self.supabase_writer:
            try:
                with self.supabase_writer.with_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT code, name FROM spatial.township_boundaries "
                            "ORDER BY code LIMIT 500"
                        )
                        rows = [{'code': r[0], 'name': r[1]} for r in cur.fetchall()]
                if rows:
                    try:
                        self._gazetteer_cache_path.parent.mkdir(parents=True, exist_ok=True)
                        self._gazetteer_cache_path.write_text(
                            json.dumps(rows, ensure_ascii=False), encoding='utf-8'
                        )
                    except OSError as e:
                        logger.warning(f"[{self.name}] gazetteer cache 寫入失敗: {e}")
            except Exception as e:
                logger.warning(f"[{self.name}] 讀取 township_boundaries 失敗，改用本地 cache: {e}")

        # 2) DB 不可用 → 本地 cache
        if not rows and self._gazetteer_cache_path.exists():
            try:
                rows = json.loads(self._gazetteer_cache_path.read_text(encoding='utf-8'))
            except (OSError, json.JSONDecodeError) as e:
                logger.warning(f"[{self.name}] gazetteer cache 讀取失敗: {e}")

        self._gazetteer = TownshipGazetteer(rows)
        if self._gazetteer.is_empty():
            logger.warning(
                f"[{self.name}] gazetteer 為空（DB 與本地 cache 皆不可用），"
                f"有新新聞時本輪將中止，避免未分類資料入庫"
            )
        return self._gazetteer

    # ------------------------------------------------------------
    # Fetch feeds
    # ------------------------------------------------------------

    def _fetch_feed(self, feed: dict) -> list[dict]:
        """抓取單一 RSS/Atom feed → 統一 item dicts；失敗 raise 由上層 catch"""
        if feedparser is None:
            raise RuntimeError("feedparser 未安裝（pip3 install feedparser）")

        resp = self._session.get(feed['url'], timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)

        items = []
        for entry in parsed.entries:
            title = (entry.get('title') or '').strip()
            link = (entry.get('link') or '').strip()
            if not title or not link:
                continue
            items.append({
                'source': feed['source'],
                'url': link,
                'title': title,
                'summary': self._strip_html(entry.get('summary', ''))[:self.SUMMARY_MAX_CHARS],
                'published_ts': self._entry_published_iso(entry),
                'county_hint': feed.get('county_hint'),
            })
        return items

    @staticmethod
    def _strip_html(text: str) -> str:
        text = re.sub(r'<[^>]+>', ' ', text or '')
        return re.sub(r'\s+', ' ', text).strip()

    @staticmethod
    def _entry_published_iso(entry) -> str:
        tp = entry.get('published_parsed') or entry.get('updated_parsed')
        if tp:
            dt = datetime(*tp[:6], tzinfo=timezone.utc).astimezone(TAIPEI_TZ)
        else:
            dt = datetime.now(TAIPEI_TZ)
        return dt.isoformat()

    def _fetch_all_feeds(self) -> tuple[list[dict], int, int]:
        """逐一抓取所有 feed（之間 sleep），單一失敗不影響其他"""
        all_items: list[dict] = []
        ok = failed = 0
        feeds = build_feed_list()
        # source_health 先收集、抓完整批一次 upsert（一次 borrow 一條 conn）。
        # 舊版每個 feed 各借一條連線 upsert，數十 feed = 數十次 borrow + pre-ping，
        # 是 pool 壓力來源之一。抓取期間有 sleep，不能邊抓邊持有連線。
        health_updates: list[tuple[dict, bool, Optional[str]]] = []
        for i, feed in enumerate(feeds):
            if i > 0:
                time.sleep(self.FEED_SLEEP_SECONDS)
            try:
                items = self._fetch_feed(feed)
                all_items.extend(items)
                ok += 1
                health_updates.append((feed, True, None))
            except Exception as e:
                failed += 1
                label = feed.get('county_hint') or feed['source']
                print(f"   ⚠ feed 抓取失敗 [{feed['source']}/{label}]: {e}")
                health_updates.append((feed, False, str(e)[:500]))
        self._upsert_source_health_batch(health_updates)
        return all_items, ok, failed

    def _upsert_source_health_batch(
            self, updates: list[tuple[dict, bool, Optional[str]]]) -> None:
        """整批更新 live.source_health，共用一條連線（失敗不影響主流程）。

        連線為 autocommit（pool borrow 預設），每筆 upsert 各自成交易；
        單筆失敗只記 log 繼續下一筆，維持舊版「每 feed 獨立 best-effort」語義。
        """
        if not self.supabase_writer or not updates:
            return
        try:
            with self.supabase_writer.with_conn() as conn:
                for feed, success, error in updates:
                    try:
                        with conn.cursor() as cur:
                            if success:
                                cur.execute(
                                    """
                                    INSERT INTO live.source_health
                                        (feed_url, source, county_hint, last_success_at, last_attempt_at,
                                         last_error, consecutive_fail, updated_at)
                                    VALUES (%s, %s, %s, now(), now(), NULL, 0, now())
                                    ON CONFLICT (feed_url) DO UPDATE SET
                                        last_success_at  = EXCLUDED.last_success_at,
                                        last_attempt_at  = EXCLUDED.last_attempt_at,
                                        last_error       = NULL,
                                        consecutive_fail = 0,
                                        source           = EXCLUDED.source,
                                        county_hint      = EXCLUDED.county_hint,
                                        updated_at       = now();
                                    """,
                                    (feed['url'], feed['source'], feed.get('county_hint')),
                                )
                            else:
                                cur.execute(
                                    """
                                    INSERT INTO live.source_health
                                        (feed_url, source, county_hint, last_attempt_at,
                                         last_error, consecutive_fail, updated_at)
                                    VALUES (%s, %s, %s, now(), %s, 1, now())
                                    ON CONFLICT (feed_url) DO UPDATE SET
                                        last_attempt_at  = EXCLUDED.last_attempt_at,
                                        last_error       = EXCLUDED.last_error,
                                        consecutive_fail = live.source_health.consecutive_fail + 1,
                                        source           = EXCLUDED.source,
                                        county_hint      = EXCLUDED.county_hint,
                                        updated_at       = now();
                                    """,
                                    (feed['url'], feed['source'], feed.get('county_hint'), error),
                                )
                    except Exception as e:
                        logger.warning(
                            f"[{self.name}] source_health upsert 失敗"
                            f"（feed={feed['url']}，不影響主流程）: {e}"
                        )
        except Exception as e:
            logger.warning(f"[{self.name}] source_health 批次 upsert 失敗（不影響主流程）: {e}")

    # ------------------------------------------------------------
    # 去重
    # ------------------------------------------------------------

    def _fetch_recent_keys(self) -> tuple[set, list[int]]:
        """查 DB 近 7 天 url_norm 集合 + simhash（unsigned）清單。

        DB 不可用 → 回空集合（degraded：url upsert do_nothing 仍擋 URL 重複）。
        """
        if not self.supabase_writer:
            return set(), []
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT url_norm, title_simhash FROM live.news_events "
                        "WHERE published_ts >= now() - make_interval(days => %s) "
                        "LIMIT 100000",
                        (self.DEDUP_WINDOW_DAYS,),
                    )
                    rows = cur.fetchall()
            url_set = {r[0] for r in rows if r[0]}
            hashes = [to_unsigned_64(int(r[1])) for r in rows if r[1] is not None]
            return url_set, hashes
        except Exception as e:
            logger.warning(f"[{self.name}] 讀取近 {self.DEDUP_WINDOW_DAYS} 天去重集合失敗: {e}")
            return set(), []

    def _dedup(self, items: list[dict]) -> tuple[list[dict], dict]:
        """兩層去重：url_norm（DB + 同批）→ simhash hamming <= 3（DB + 同批）"""
        seen_urls, seen_hashes = self._fetch_recent_keys()
        stats = {'dup_url': 0, 'dup_simhash': 0}

        fresh: list[dict] = []
        for it in items:
            url_norm = normalize_url(it['url'])
            if not url_norm:
                continue
            if url_norm in seen_urls:
                stats['dup_url'] += 1
                continue
            seen_urls.add(url_norm)

            sh = simhash64(clean_title(it['title']))
            if any(hamming_distance(sh, h) <= SIMHASH_DUP_THRESHOLD for h in seen_hashes):
                stats['dup_simhash'] += 1
                continue
            seen_hashes.append(sh)

            it['url_norm'] = url_norm
            it['title_simhash'] = to_signed_64(sh)
            fresh.append(it)
        return fresh, stats

    # ------------------------------------------------------------
    # LLM 地點抽取（Gemini，20 則 packing）
    # ------------------------------------------------------------

    def _init_llm(self):
        if self._llm_client is not None:
            return self._llm_client
        api_key = getattr(config, 'GEMINI_API_KEY', None)
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY 未設定")
        from google import genai  # 延遲匯入（pip3 install google-genai）
        self._llm_client = genai.Client(api_key=api_key)
        return self._llm_client

    @staticmethod
    def _llm_provider() -> str:
        provider = str(getattr(config, 'NEWS_EVENTS_LLM_PROVIDER', 'gemini') or '').strip().lower()
        if provider not in ('gemini', 'openrouter'):
            raise RuntimeError("NEWS_EVENTS_LLM_PROVIDER must be gemini or openrouter")
        return provider

    def _validate_llm_config(self) -> str:
        """驗證選定 provider 的 key；絕不將 key 帶入例外或 log。"""
        provider = self._llm_provider()
        key_name = 'GEMINI_API_KEY' if provider == 'gemini' else 'OPENROUTER_API_KEY'
        if not getattr(config, key_name, None):
            raise NewsAnnotationError(
                f"{key_name} unavailable; refusing to write unclassified news records"
            )
        return provider

    def _build_system_prompt(self, gaz: TownshipGazetteer) -> str:
        """固定 system prompt（>1024 tokens → 觸發 Gemini implicit prompt cache）"""
        if self._system_prompt is None:
            self._system_prompt = SYSTEM_PROMPT_HEADER + '\n'.join(gaz.prompt_lines())
        return self._system_prompt

    @staticmethod
    def _parse_llm_json(raw: str) -> list[dict]:
        raw = (raw or '').strip()
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1] if '\n' in raw else raw
            raw = raw.rsplit('```', 1)[0]
        parsed = json.loads(raw)
        if isinstance(parsed, dict):  # 容忍包一層 {"results": [...]}
            for v in parsed.values():
                if isinstance(v, list):
                    return v
            return []
        return parsed if isinstance(parsed, list) else []

    @staticmethod
    def _openrouter_post(send):
        """呼叫 OpenRouter；429／5xx／連線錯誤／read timeout 重試 2 次，其他 4xx 直接拋出。"""
        for attempt in range(len(OPENROUTER_RETRY_DELAYS) + 1):
            try:
                response = send()
                response.raise_for_status()
                return response
            except (requests.HTTPError, requests.ConnectionError, requests.ReadTimeout) as exc:
                status = getattr(getattr(exc, 'response', None), 'status_code', None)
                if isinstance(exc, requests.HTTPError):
                    retryable = isinstance(status, int) and (status == 429 or status >= 500)
                else:
                    retryable = True
                if not retryable or attempt >= len(OPENROUTER_RETRY_DELAYS):
                    raise
            time.sleep(OPENROUTER_RETRY_DELAYS[attempt])

    def _llm_extract_batch(self, batch: list[dict], gaz: TownshipGazetteer) -> tuple[dict, dict]:
        """單一 batch（<=20 則）→ {idx: annotation}，回傳 (annotations, usage)"""
        lines = []
        for i, it in enumerate(batch):
            payload = {'idx': i, 'title': it['title'], 'summary': it['summary']}
            if it.get('county_hint'):
                payload['county_hint'] = it['county_hint']
            lines.append(json.dumps(payload, ensure_ascii=False))

        provider = self._llm_provider()
        if provider == 'gemini':
            from google.genai import types

            client = self._init_llm()
            response = client.models.generate_content(
                model=getattr(config, 'GEMINI_MODEL', 'gemini-3.1-flash-lite-preview'),
                contents='\n'.join(lines),
                config=types.GenerateContentConfig(
                    system_instruction=self._build_system_prompt(gaz),
                    response_mime_type='application/json',
                    temperature=0.1,
                ),
            )
            raw = response.text
            usage = {'input': 0, 'output': 0, 'cached': 0}
            meta = getattr(response, 'usage_metadata', None)
            if meta:
                usage['input'] = meta.prompt_token_count or 0
                usage['output'] = meta.candidates_token_count or 0
                usage['cached'] = getattr(meta, 'cached_content_token_count', 0) or 0
        else:
            key = getattr(config, 'OPENROUTER_API_KEY', None)
            if not key:
                raise RuntimeError("OPENROUTER_API_KEY unavailable")
            response = self._openrouter_post(lambda: self._session.post(
                'https://openrouter.ai/api/v1/chat/completions',
                headers={
                    'Authorization': f'Bearer {key}',
                    'Content-Type': 'application/json',
                },
                json={
                    'model': getattr(config, 'NEWS_EVENTS_OPENROUTER_MODEL', 'qwen/qwen3.7-flash'),
                    'temperature': 0.1,
                    'max_tokens': max(
                        1, int(getattr(config, 'NEWS_EVENTS_OPENROUTER_MAX_TOKENS', 4096))
                    ),
                    'reasoning': {
                        'enabled': bool(getattr(
                            config, 'NEWS_EVENTS_OPENROUTER_REASONING_ENABLED', False
                        ))
                    },
                    'messages': [
                        {'role': 'system', 'content': self._build_system_prompt(gaz)},
                        {'role': 'user', 'content': '\n'.join(lines)},
                    ],
                },
                timeout=max(1, int(getattr(config, 'NEWS_EVENTS_OPENROUTER_TIMEOUT', 60))),
            ))
            payload = response.json()
            choices = payload.get('choices') if isinstance(payload, dict) else None
            choice = choices[0] if isinstance(choices, list) and choices else None
            message = choice.get('message') if isinstance(choice, dict) else None
            raw = message.get('content') if isinstance(message, dict) else None
            if not isinstance(raw, str) or not raw.strip():
                raise ValueError("OpenRouter response missing message content")
            usage_payload = payload.get('usage') if isinstance(payload, dict) else {}
            usage_payload = usage_payload if isinstance(usage_payload, dict) else {}
            usage = {
                'input': usage_payload.get('prompt_tokens', usage_payload.get('input_tokens', 0)) or 0,
                'output': usage_payload.get('completion_tokens', usage_payload.get('output_tokens', 0)) or 0,
                'cached': usage_payload.get('cached_tokens', 0) or 0,
            }

        annotations: dict[int, dict] = {}
        for obj in self._parse_llm_json(raw):
            if not isinstance(obj, dict):
                continue
            idx = obj.get('idx')
            if type(idx) is int and 0 <= idx < len(batch):
                if idx in annotations:
                    raise NewsAnnotationError(
                        "LLM annotation batch has duplicate idx; refusing to write this run"
                    )
                annotations[idx] = obj
        return annotations, usage

    @staticmethod
    def _validate_annotation(ann: dict) -> None:
        """單則 annotation 必須欄位齊全且型別／範圍正確，否則 fail-closed。

        url_norm upsert 為 do_nothing，寫入的錯誤標註無法事後被覆蓋，故寧可中止整輪。
        """
        def _ok_level(v):
            return type(v) is int and 0 <= v <= 3

        valid = (
            isinstance(ann, dict)
            and ann.get('category') in CATEGORY_ENUM
            and isinstance(ann.get('is_event'), bool)
            and _ok_level(ann.get('gis_relevance'))
            and _ok_level(ann.get('severity'))
            and 'county' in ann
            and 'township' in ann
        )
        if not valid:
            raise NewsAnnotationError(
                "LLM annotation object invalid; refusing to write this run"
            )

    def _annotate_items(self, items: list[dict], gaz: TownshipGazetteer) -> dict:
        """完整 annotation 所有新項目；任一 batch 異常即中止整輪。"""
        total_usage = {'input': 0, 'output': 0, 'cached': 0, 'batches': 0, 'failed_batches': 0}

        for start in range(0, len(items), LLM_BATCH_SIZE):
            batch = items[start:start + LLM_BATCH_SIZE]
            if total_usage['batches'] > 0:
                time.sleep(LLM_BATCH_SLEEP)
            try:
                annotations, usage = self._llm_extract_batch(batch, gaz)
                total_usage['batches'] += 1
                for k in ('input', 'output', 'cached'):
                    total_usage[k] += usage[k]
            except Exception:
                total_usage['failed_batches'] += 1
                raise NewsAnnotationError(
                    f"LLM annotation batch failed ({len(batch)} records); refusing to write this run"
                ) from None

            expected_indexes = set(range(len(batch)))
            if set(annotations) != expected_indexes:
                total_usage['failed_batches'] += 1
                raise NewsAnnotationError(
                    "LLM annotation batch incomplete; refusing to write this run"
                )
            for ann in annotations.values():
                try:
                    self._validate_annotation(ann)
                except NewsAnnotationError:
                    total_usage['failed_batches'] += 1
                    raise

            for i, it in enumerate(batch):
                ann = annotations.get(i) or {}
                loc = gaz.validate(ann.get('county'), ann.get('township'))
                it.update(loc)

                category = ann.get('category')
                it['category'] = category if category in CATEGORY_ENUM else 'other'

                llm_summary = (ann.get('summary') or '').strip()
                if llm_summary:
                    it['summary'] = llm_summary[:self.SUMMARY_MAX_CHARS]

                conf = ann.get('confidence')
                try:
                    it['confidence'] = max(0.0, min(1.0, float(conf))) if conf is not None else None
                except (TypeError, ValueError):
                    it['confidence'] = None
                if it['location_name'] is None:
                    it['confidence'] = 0.0 if it['confidence'] is None else it['confidence']

                # v2 三維度：gis_relevance / severity / is_event（不合法值留 NULL，DB 允許）
                def _int_in_range(v, lo, hi):
                    # bool 是 int 子類，True/False 不可被當成 1/0
                    if type(v) is not int:
                        try:
                            if isinstance(v, bool):
                                return None
                            n = int(v)
                        except (TypeError, ValueError):
                            return None
                    else:
                        n = v
                    return n if lo <= n <= hi else None
                it['gis_relevance'] = _int_in_range(ann.get('gis_relevance'), 0, 3)
                it['severity']      = _int_in_range(ann.get('severity'), 0, 3)
                raw_event = ann.get('is_event')
                it['is_event'] = bool(raw_event) if isinstance(raw_event, bool) else None
        return total_usage

    @staticmethod
    def _no_location_defaults(items: list[dict]):
        """dry-run 預覽的欄位補齊（不會入庫）。"""
        for it in items:
            it.setdefault('county', None)
            it.setdefault('township', None)
            it.setdefault('admin_code', None)
            it.setdefault('location_name', None)
            it.setdefault('category', 'other')
            it.setdefault('confidence', None)
            it.setdefault('gis_relevance', None)
            it.setdefault('severity', None)
            it.setdefault('is_event', None)

    # ------------------------------------------------------------
    # Collect
    # ------------------------------------------------------------

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)

        print(f"   抓取 {len(build_feed_list())} 個 news feeds（每個間隔 {self.FEED_SLEEP_SECONDS}s）...")
        all_items, feeds_ok, feeds_failed = self._fetch_all_feeds()
        print(f"   feed 完成: {feeds_ok} 成功 / {feeds_failed} 失敗，共 {len(all_items)} 則")

        fresh, dedup_stats = self._dedup(all_items)
        print(
            f"   去重後新項目 {len(fresh)} 則 "
            f"(url 重複 {dedup_stats['dup_url']} / simhash 重複 {dedup_stats['dup_simhash']})"
        )

        gaz = self._load_gazetteer()
        usage = {'input': 0, 'output': 0, 'cached': 0, 'batches': 0, 'failed_batches': 0}
        llm_provider: Optional[str] = None
        llm_model: Optional[str] = None

        if self.dry_run:
            print("   [dry-run] 跳過 LLM 地點抽取與 DB 寫入")
            self._no_location_defaults(fresh)
        elif not fresh:
            pass
        elif gaz.is_empty():
            raise NewsAnnotationError(
                "township gazetteer unavailable; refusing to write unclassified news records"
            )
        else:
            llm_provider = self._validate_llm_config()
            llm_model = (
                getattr(config, 'GEMINI_MODEL', 'gemini-3.1-flash-lite-preview')
                if llm_provider == 'gemini'
                else getattr(config, 'NEWS_EVENTS_OPENROUTER_MODEL', 'qwen/qwen3.7-flash')
            )
            usage = self._annotate_items(fresh, gaz)
            cost_text = ""
            if llm_provider == 'gemini':
                cost = (
                    usage['input'] / 1e6 * GEMINI_PRICE_INPUT_PER_MTOK
                    + usage['output'] / 1e6 * GEMINI_PRICE_OUTPUT_PER_MTOK
                )
                cost_text = f" | 約 ${cost:.5f} USD/輪"
            print(
                f"   LLM ({llm_provider}/{llm_model}): "
                f"{usage['batches']} batch (失敗 {usage['failed_batches']}) | "
                f"tokens in {usage['input']} (cached {usage['cached']}) / out {usage['output']}"
                f"{cost_text}"
            )

        records = []
        for it in fresh:
            records.append({
                'source': it['source'],
                'url': it['url'],
                'url_norm': it['url_norm'],
                'title': it['title'],
                'summary': it.get('summary') or None,
                'category': it.get('category') or 'other',
                'location_name': it.get('location_name'),
                'county': it.get('county'),
                'admin_code': it.get('admin_code'),
                'published_ts': it['published_ts'],
                'confidence': it.get('confidence'),
                'title_simhash': it['title_simhash'],
                # v2（2026-06-13）：LLM 評的 GIS 相關性 / 嚴重度 / 是否為事件
                'gis_relevance': it.get('gis_relevance'),
                'severity': it.get('severity'),
                'is_event': it.get('is_event'),
            })

        located = sum(1 for r in records if r['admin_code'])
        township_level = sum(1 for r in records if r['admin_code'] and len(r['admin_code']) == 8)
        print(f"   入庫 {len(records)} 則（有地點 {located}，鄉鎮級 {township_level}）")

        result = {
            'fetch_time': fetch_time.isoformat(),
            'feeds_ok': feeds_ok,
            'feeds_failed': feeds_failed,
            'fetched_total': len(all_items),
            'new_items': len(records),
            'dup_url': dedup_stats['dup_url'],
            'dup_simhash': dedup_stats['dup_simhash'],
            'located': located,
            'township_level': township_level,
            'llm_batches': usage['batches'],
            'llm_failed_batches': usage['failed_batches'],
            'llm_tokens_input': usage['input'],
            'llm_tokens_cached': usage['cached'],
            'llm_tokens_output': usage['output'],
            'llm_provider': llm_provider,
            'llm_model': llm_model,
            # Gemini uses the documented local estimate. OpenRouter pricing is
            # model/provider specific, so do not report a Gemini-derived cost.
            'llm_cost_usd': (
                round(
                    usage['input'] / 1e6 * GEMINI_PRICE_INPUT_PER_MTOK
                    + usage['output'] / 1e6 * GEMINI_PRICE_OUTPUT_PER_MTOK, 6,
                )
                if llm_provider == 'gemini'
                else None
            ),
        }
        # dry-run 不帶 data → base.run() 不會存檔 / 寫 DB
        if not self.dry_run:
            result['data'] = records
        else:
            result['dry_run_preview'] = records[:5]
        return result


if __name__ == "__main__":
    # 離線試跑（不寫 DB、不打 LLM）：python3 -m collectors.news_events --dry-run
    import argparse
    from unittest.mock import MagicMock

    parser = argparse.ArgumentParser(description="news_events collector 手動執行")
    parser.add_argument('--dry-run', action='store_true',
                        help='只抓 feed + 去重，不打 LLM、不寫 DB')
    args = parser.parse_args()

    if args.dry_run:
        c = NewsEventsCollector.__new__(NewsEventsCollector)
        c.storage = MagicMock()
        c.supabase_writer = None
        c.dry_run = True
        c._session = requests.Session()
        c._session.headers.update({'User-Agent': 'Mozilla/5.0 (news-events-dry-run)'})
        c._gazetteer = None
        c._system_prompt = None
        c._llm_client = None
        out = c.collect()
        print("\n=== dry-run 結果 ===")
        for k, v in out.items():
            if k == 'dry_run_preview':
                continue
            print(f"  {k}: {v}")
        for r in out.get('dry_run_preview', []):
            print(f"  · [{r['source']}] {r['title'][:50]} → {r['url_norm'][:80]}")
    else:
        # 完整執行（需 SUPABASE_ENABLED + 選定 provider 的 API key）
        NewsEventsCollector().run()
