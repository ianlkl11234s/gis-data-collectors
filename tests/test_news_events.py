"""
news_events collector 的純函式單元測試（不打網路、不碰 DB）

涵蓋：
1. URL 正規化（tracking params / fragment / Google News redirect 解碼與 fallback）
2. 64-bit simhash（確定性、相似標題 hamming <= 3、不相似 > 3、signed 轉換）
3. TownshipGazetteer 白名單驗證（合法鄉鎮 / 降級 county-only / 全無效 / 台臺正規化）
"""

import base64

import pytest

from collectors.news_events import (
    SIMHASH_DUP_THRESHOLD,
    TownshipGazetteer,
    clean_title,
    decode_google_news_url,
    hamming_distance,
    normalize_url,
    simhash64,
    to_signed_64,
    to_unsigned_64,
)


# ============================================================
# URL 正規化
# ============================================================

class TestNormalizeUrl:

    def test_strips_utm_params(self):
        url = "https://news.ltn.com.tw/news/society/breakingnews/123?utm_source=rss&utm_medium=feed"
        assert normalize_url(url) == "https://news.ltn.com.tw/news/society/breakingnews/123"

    def test_strips_fbclid_and_fragment(self):
        url = "https://www.ettoday.net/news/20260612/123.htm?fbclid=abc#top"
        assert normalize_url(url) == "https://www.ettoday.net/news/20260612/123.htm"

    def test_keeps_meaningful_query(self):
        url = "https://example.com/article?id=42&utm_campaign=x"
        assert normalize_url(url) == "https://example.com/article?id=42"

    def test_lowercases_host_and_strips_trailing_slash(self):
        assert normalize_url("https://News.CNA.com.tw/news/aSOC/202606120123.aspx/") == \
            "https://news.cna.com.tw/news/aSOC/202606120123.aspx"

    def test_empty_url(self):
        assert normalize_url("") == ""
        assert normalize_url(None) == ""

    def test_same_article_different_tracking_collapses(self):
        a = normalize_url("https://example.com/a/1?utm_source=fb&fbclid=x1")
        b = normalize_url("https://example.com/a/1?gclid=zzz")
        assert a == b


class TestGoogleNewsDecode:

    @staticmethod
    def _encode_old_format(real_url: str) -> str:
        """組出舊格式 Google News article id（解碼器的反向操作）"""
        payload = real_url.encode("utf-8")
        assert len(payload) < 0x80  # 測試 URL 保持單 byte 長度前綴
        raw = b"\x08\x13\x22" + bytes([len(payload)]) + payload + b"\xd2\x01\x00"
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def test_decodes_old_format(self):
        real = "https://udn.com/news/story/7320/123456"
        gn_url = f"https://news.google.com/rss/articles/{self._encode_old_format(real)}?oc=5"
        assert decode_google_news_url(gn_url) == real

    def test_normalize_url_resolves_old_format(self):
        real = "https://udn.com/news/story/7320/123456"
        gn_url = f"https://news.google.com/rss/articles/{self._encode_old_format(real)}?oc=5"
        assert normalize_url(gn_url) == real

    def test_new_format_returns_none(self):
        # 新格式（AU_yqL…）離線解不出 → None
        gn_url = "https://news.google.com/rss/articles/AU_yqLNotDecodableXXXX?oc=5"
        assert decode_google_news_url(gn_url) is None

    def test_new_format_fallback_to_google_url(self):
        # 解不出 → 用 articles/<id> 路徑當 url_norm（去 query），不 hard fail
        gn_url = "https://news.google.com/rss/articles/AU_yqLNotDecodableXXXX?oc=5&hl=zh-TW"
        norm = normalize_url(gn_url)
        assert norm == "https://news.google.com/rss/articles/AU_yqLNotDecodableXXXX"

    def test_non_google_url_returns_none(self):
        assert decode_google_news_url("https://example.com/articles/abc") is None

    def test_garbage_base64_returns_none(self):
        assert decode_google_news_url("https://news.google.com/rss/articles/!!!not-b64!!!") is None


# ============================================================
# Simhash
# ============================================================

class TestSimhash:

    def test_deterministic(self):
        t = clean_title("高雄市鼓山區民宅火警 消防局出動 30 人搶救")
        assert simhash64(t) == simhash64(t)

    def test_identical_titles_distance_zero(self):
        a = simhash64(clean_title("台南永康工廠大火 延燒 3 小時"))
        b = simhash64(clean_title("台南永康工廠大火 延燒 3 小時"))
        assert hamming_distance(a, b) == 0

    def test_cross_media_similar_titles_within_threshold(self):
        # 同一事件、不同媒體的小幅改寫（含媒體名尾巴），應視為重複
        a = simhash64(clean_title("高雄鼓山民宅火警 消防出動30人搶救 - 自由時報"))
        b = simhash64(clean_title("高雄鼓山民宅火警　消防出動30人搶救 ｜ ETtoday"))
        assert hamming_distance(a, b) <= SIMHASH_DUP_THRESHOLD

    def test_different_news_beyond_threshold(self):
        a = simhash64(clean_title("高雄市鼓山區民宅火警 消防局出動 30 人搶救"))
        b = simhash64(clean_title("立法院三讀通過交通安全修法 提高罰鍰上限"))
        assert hamming_distance(a, b) > SIMHASH_DUP_THRESHOLD

    def test_empty_text(self):
        assert simhash64("") == 0

    def test_clean_title_normalizes_tai_variant(self):
        assert clean_title("台南市") == clean_title("臺南市")

    def test_clean_title_strips_media_suffix(self):
        assert clean_title("某某新聞標題 - 中央社") == clean_title("某某新聞標題")


class TestSigned64:

    def test_roundtrip_high_bit(self):
        u = (1 << 63) | 12345  # 最高位為 1 → signed 為負
        s = to_signed_64(u)
        assert s < 0
        assert to_unsigned_64(s) == u

    def test_roundtrip_low_value(self):
        assert to_signed_64(42) == 42
        assert to_unsigned_64(42) == 42

    def test_within_pg_bigint_range(self):
        for u in (0, 1, (1 << 63) - 1, 1 << 63, (1 << 64) - 1):
            s = to_signed_64(u)
            assert -(1 << 63) <= s <= (1 << 63) - 1

    def test_simhash_output_fits_after_conversion(self):
        u = simhash64(clean_title("測試標題轉換為資料庫可存的整數"))
        s = to_signed_64(u)
        assert -(1 << 63) <= s <= (1 << 63) - 1


# ============================================================
# TownshipGazetteer 白名單驗證
# ============================================================

@pytest.fixture
def gazetteer():
    rows = [
        {'code': '63000050', 'name': '臺北市中正區'},
        {'code': '63000010', 'name': '臺北市松山區'},
        {'code': '64000110', 'name': '高雄市鼓山區'},
        {'code': '64000010', 'name': '高雄市鹽埕區'},
        {'code': '10014010', 'name': '臺東縣臺東市'},
    ]
    return TownshipGazetteer(rows)


class TestGazetteerValidate:

    def test_valid_township(self, gazetteer):
        out = gazetteer.validate('臺北市', '中正區')
        assert out == {
            'county': '臺北市', 'township': '中正區',
            'admin_code': '63000050', 'location_name': '臺北市中正區',
        }

    def test_tai_variant_normalized(self, gazetteer):
        out = gazetteer.validate('台北市', '中正區')
        assert out['admin_code'] == '63000050'
        assert out['county'] == '臺北市'

    def test_invalid_township_downgrades_to_county(self, gazetteer):
        out = gazetteer.validate('臺北市', '不存在區')
        assert out['county'] == '臺北市'
        assert out['township'] is None
        assert out['admin_code'] == '63000'  # 5 碼縣市代碼
        assert out['location_name'] == '臺北市'

    def test_township_belongs_to_other_county_downgrades(self, gazetteer):
        # 鼓山區屬高雄，配臺北市 → 降級 county-only（不可錯掛代碼）
        out = gazetteer.validate('臺北市', '鼓山區')
        assert out['admin_code'] == '63000'
        assert out['township'] is None

    def test_county_only(self, gazetteer):
        out = gazetteer.validate('高雄市', None)
        assert out['county'] == '高雄市'
        assert out['admin_code'] == '64000'
        assert out['location_name'] == '高雄市'

    def test_invalid_county_all_null(self, gazetteer):
        out = gazetteer.validate('東京都', '新宿區')
        assert out == {
            'county': None, 'township': None,
            'admin_code': None, 'location_name': None,
        }

    def test_none_inputs(self, gazetteer):
        out = gazetteer.validate(None, None)
        assert out['admin_code'] is None

    def test_empty_gazetteer(self):
        gaz = TownshipGazetteer([])
        assert gaz.is_empty()
        out = gaz.validate('臺北市', '中正區')
        assert out['admin_code'] is None

    def test_prompt_lines_format(self, gazetteer):
        lines = gazetteer.prompt_lines()
        assert '63000050 臺北市 中正區' in lines
        assert len(lines) == 5
