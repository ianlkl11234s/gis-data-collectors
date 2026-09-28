"""
news_events collector 的純函式單元測試（不打網路、不碰 DB）

涵蓋：
1. URL 正規化（tracking params / fragment / Google News redirect 解碼與 fallback）
2. 64-bit simhash（確定性、相似標題 hamming <= 3、不相似 > 3、signed 轉換）
3. TownshipGazetteer 白名單驗證（合法鄉鎮 / 降級 county-only / 全無效 / 台臺正規化）
"""

import base64
import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

import config
import collectors.news_events as news_events
from collectors.news_events import (
    LOCATION_EVIDENCE_METHOD_VERSION,
    LOCATION_PRECISIONS,
    LOCATION_ROLES,
    LOCATION_SCOPES,
    LOCATION_STATUSES,
    NewsAnnotationError,
    NewsEventsCollector,
    SIMHASH_DUP_THRESHOLD,
    TownshipGazetteer,
    article_relation_candidates_json,
    build_article_relation_candidates,
    build_location_evidence,
    clean_title,
    decode_google_news_url,
    hamming_distance,
    location_evidence_json,
    normalize_url,
    simhash64,
    to_signed_64,
    to_unsigned_64,
)


# ============================================================
# Location evidence POC（純 contract；不接 DB）
# ============================================================

class TestLocationEvidencePoc:

    def test_explicit_lucao_overrides_conflicting_yunlin_hint(self):
        evidence = build_location_evidence({
            'title': '鹿草鄉農路事故造成交通受阻',
            'summary': '雲林縣鄰近民眾協助通報。',
            'county_hint': '雲林縣',
        })

        assert evidence.location_scope == 'taiwan_local'
        assert evidence.location_status == 'accepted'
        assert evidence.resolved_county == '嘉義縣'
        assert evidence.county_hint == '雲林縣'
        assert evidence.evidence_text == '鹿草鄉'
        assert evidence.evidence_field == 'title'
        assert evidence.location_precision == 'township'
        assert evidence.location_role == 'event_site'

    def test_national_news_is_not_forced_to_feed_county(self):
        evidence = build_location_evidence({
            'title': '立法院三讀通過全國交通安全修法',
            'summary': '',
            'county_hint': '高雄市',
        })

        assert evidence.location_scope == 'taiwan_national'
        assert evidence.location_status == 'accepted'
        assert evidence.location_precision == 'none'
        assert evidence.resolved_county is None
        assert evidence.evidence_text == '全國'

    def test_multiple_explicit_counties_are_retained_as_multi_scope(self):
        evidence = build_location_evidence({
            'title': '臺北市與高雄市同步舉行防災演練', 'summary': '',
        })

        assert evidence.location_scope == 'taiwan_multi'
        assert evidence.location_status == 'accepted'
        assert evidence.location_precision == 'county'
        assert evidence.evidence_text == '臺北市、高雄市'
        assert evidence.resolved_county is None

    def test_foreign_news_is_an_accepted_country_event_site(self):
        evidence = build_location_evidence({'title': '日本北海道發生強震', 'summary': ''})

        assert evidence.location_scope == 'foreign'
        assert evidence.location_status == 'accepted'
        assert evidence.location_precision == 'country'
        assert evidence.location_role == 'event_site'

    def test_legislative_yuan_protest_is_not_inferred_as_national(self):
        evidence = build_location_evidence({'title': '立法院外抗議要求政府回應', 'summary': ''})

        assert evidence.location_scope == 'unknown'
        assert evidence.location_status == 'unresolved'
        assert evidence.location_precision == 'none'

    def test_missing_location_is_unknown_and_hint_stays_unresolved(self):
        evidence = build_location_evidence({
            'title': '市場價格波動引發討論', 'summary': '', 'county_hint': '雲林縣',
        })

        assert evidence.location_scope == 'unknown'
        assert evidence.location_status == 'unresolved'
        assert evidence.location_precision == 'none'
        assert evidence.evidence_field == 'county_hint'
        assert evidence.resolved_county is None

    def test_contract_has_required_provenance_and_json_is_pure(self):
        article = {'title': '鹿草鄉公園旁事故', 'summary': '', 'county_hint': '雲林縣'}
        before = dict(article)
        evidence = build_location_evidence(article)
        payload = json.loads(location_evidence_json(article))

        assert article == before
        assert evidence.method_version == LOCATION_EVIDENCE_METHOD_VERSION
        assert payload == evidence.to_dict()
        assert payload['location_scope'] in LOCATION_SCOPES
        assert payload['location_status'] in LOCATION_STATUSES
        assert payload['location_precision'] in LOCATION_PRECISIONS
        assert payload['location_role'] in LOCATION_ROLES
        for key in ('evidence_text', 'evidence_field', 'location_role', 'resolver', 'method_version'):
            assert payload[key]


class TestArticleRelationCandidates:

    def test_simhash_near_duplicate_is_retained_as_candidate(self):
        articles = [
            {'article_id': 'cna-1', 'title': '高雄鼓山民宅火警 消防出動30人搶救'},
            {'article_id': 'ltn-2', 'title': '高雄鼓山民宅火警　消防出動30人搶救 - 自由時報'},
            {'article_id': 'other-3', 'title': '立法院三讀通過交通安全修法'},
        ]

        candidates = build_article_relation_candidates(articles)
        assert len(candidates) == 1
        candidate = candidates[0]
        assert candidate.left_article_key == 'cna-1'
        assert candidate.right_article_key == 'ltn-2'
        assert candidate.relation_type == 'same_story_candidate'
        assert candidate.hamming_distance <= SIMHASH_DUP_THRESHOLD
        assert json.loads(article_relation_candidates_json(articles)) == [candidate.to_dict()]


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


# ============================================================
# LLM annotation fail-closed
# ============================================================

@pytest.fixture
def annotation_items():
    return [
        {'title': '臺北市中正區火警', 'summary': '測試摘要一'},
        {'title': '高雄市鼓山區事故', 'summary': '測試摘要二'},
    ]


def _annotation(idx=0):
    return {
        'idx': idx,
        'county': '臺北市',
        'township': '中正區',
        'category': 'accident',
        'summary': '火警摘要',
        'confidence': 0.9,
        'gis_relevance': 3,
        'severity': 2,
        'is_event': True,
    }


def _usage():
    return {'input': 1, 'output': 1, 'cached': 0}


class TestAnnotationFailClosed:

    def test_all_batches_failing_raises_without_defaults(self, monkeypatch, gazetteer, annotation_items):
        collector = NewsEventsCollector.__new__(NewsEventsCollector)
        monkeypatch.setattr(news_events, 'LLM_BATCH_SIZE', 2)
        collector._llm_extract_batch = Mock(side_effect=RuntimeError('provider unavailable'))

        with pytest.raises(NewsAnnotationError, match='batch failed'):
            collector._annotate_items(annotation_items, gazetteer)

        assert 'category' not in annotation_items[0]

    def test_partial_batch_failure_aborts_whole_run(self, monkeypatch, gazetteer, annotation_items):
        collector = NewsEventsCollector.__new__(NewsEventsCollector)
        monkeypatch.setattr(news_events, 'LLM_BATCH_SIZE', 1)
        monkeypatch.setattr(news_events, 'LLM_BATCH_SLEEP', 0)
        collector._llm_extract_batch = Mock(side_effect=[({0: _annotation()}, _usage()), RuntimeError('timeout')])

        with pytest.raises(NewsAnnotationError, match='refusing to write this run'):
            collector._annotate_items(annotation_items, gazetteer)

        assert collector._llm_extract_batch.call_count == 2

    def test_missing_response_index_raises(self, monkeypatch, gazetteer, annotation_items):
        collector = NewsEventsCollector.__new__(NewsEventsCollector)
        monkeypatch.setattr(news_events, 'LLM_BATCH_SIZE', 2)
        collector._llm_extract_batch = Mock(return_value=({0: _annotation()}, _usage()))

        with pytest.raises(NewsAnnotationError, match='batch incomplete'):
            collector._annotate_items(annotation_items, gazetteer)

        assert 'category' not in annotation_items[0]

    def test_dry_run_remains_offline_without_annotation(self, monkeypatch, annotation_items):
        collector = NewsEventsCollector.__new__(NewsEventsCollector)
        collector.dry_run = True
        collector._fetch_all_feeds = Mock(return_value=(annotation_items, 1, 0))
        collector._dedup = Mock(return_value=(annotation_items, {'dup_url': 0, 'dup_simhash': 0}))
        collector._load_gazetteer = Mock(return_value=TownshipGazetteer([]))
        collector._annotate_items = Mock(side_effect=AssertionError('dry-run must not call LLM'))
        for index, item in enumerate(annotation_items):
            item.update({
                'source': 'test',
                'url': f'https://example.test/{index}',
                'url_norm': f'https://example.test/{index}',
                'published_ts': '2026-09-28T00:00:00+08:00',
                'title_simhash': index,
            })

        result = collector.collect()

        assert 'data' not in result
        assert len(result['dry_run_preview']) == 2
        assert result['dry_run_preview'][0]['category'] == 'other'
        collector._annotate_items.assert_not_called()


class TestLlmProviders:

    @staticmethod
    def _collector():
        collector = NewsEventsCollector.__new__(NewsEventsCollector)
        collector._system_prompt = None
        collector._llm_client = None
        collector._session = Mock()
        return collector

    def test_openrouter_success_uses_existing_json_contract(self, monkeypatch, gazetteer):
        collector = self._collector()
        response = Mock()
        response.json.return_value = {
            'choices': [{'message': {'content': json.dumps([_annotation()])}}],
            'usage': {'prompt_tokens': 11, 'completion_tokens': 7},
        }
        collector._session.post.return_value = response
        monkeypatch.setattr(config, 'NEWS_EVENTS_LLM_PROVIDER', 'openrouter')
        monkeypatch.setattr(config, 'OPENROUTER_API_KEY', 'test-openrouter-key')
        monkeypatch.setattr(config, 'NEWS_EVENTS_OPENROUTER_MODEL', 'qwen/qwen3.7-flash')
        monkeypatch.setattr(config, 'NEWS_EVENTS_OPENROUTER_TIMEOUT', 23)
        monkeypatch.setattr(config, 'NEWS_EVENTS_OPENROUTER_MAX_TOKENS', 2048)
        monkeypatch.setattr(config, 'NEWS_EVENTS_OPENROUTER_REASONING_ENABLED', False)

        annotations, usage = collector._llm_extract_batch(
            [{'title': '測試標題', 'summary': '測試摘要'}], gazetteer
        )

        assert annotations == {0: _annotation()}
        assert usage == {'input': 11, 'output': 7, 'cached': 0}
        response.raise_for_status.assert_called_once()
        _, kwargs = collector._session.post.call_args
        assert kwargs['json']['model'] == 'qwen/qwen3.7-flash'
        assert kwargs['json']['max_tokens'] == 2048
        assert kwargs['json']['reasoning'] == {'enabled': False}
        assert kwargs['json']['messages'][0]['role'] == 'system'
        assert kwargs['timeout'] == 23

    def test_openrouter_http_failure_propagates_to_fail_closed_gate(self, monkeypatch, gazetteer):
        collector = self._collector()
        response = Mock()
        response.raise_for_status.side_effect = requests.HTTPError('401')
        collector._session.post.return_value = response
        monkeypatch.setattr(config, 'NEWS_EVENTS_LLM_PROVIDER', 'openrouter')
        monkeypatch.setattr(config, 'OPENROUTER_API_KEY', 'test-openrouter-key')

        with pytest.raises(NewsAnnotationError, match='refusing to write this run') as exc:
            collector._annotate_items([{'title': '測試', 'summary': ''}], gazetteer)

        assert '401' not in str(exc.value)

    def test_openrouter_malformed_response_is_rejected(self, monkeypatch, gazetteer):
        collector = self._collector()
        response = Mock()
        response.json.return_value = {'choices': [{'message': {'content': 'not-json'}}]}
        collector._session.post.return_value = response
        monkeypatch.setattr(config, 'NEWS_EVENTS_LLM_PROVIDER', 'openrouter')
        monkeypatch.setattr(config, 'OPENROUTER_API_KEY', 'test-openrouter-key')

        with pytest.raises(json.JSONDecodeError):
            collector._llm_extract_batch([{'title': '測試', 'summary': ''}], gazetteer)

    def test_openrouter_missing_key_is_rejected_before_annotation(self, monkeypatch):
        collector = self._collector()
        monkeypatch.setattr(config, 'NEWS_EVENTS_LLM_PROVIDER', 'openrouter')
        monkeypatch.setattr(config, 'OPENROUTER_API_KEY', '')

        with pytest.raises(NewsAnnotationError, match='OPENROUTER_API_KEY unavailable'):
            collector._validate_llm_config()

    def test_gemini_path_remains_default_provider(self, monkeypatch, gazetteer):
        collector = self._collector()
        response = Mock()
        response.text = json.dumps([_annotation()])
        response.usage_metadata = SimpleNamespace(
            prompt_token_count=13, candidates_token_count=5, cached_content_token_count=2
        )
        client = Mock()
        client.models.generate_content.return_value = response
        collector._llm_client = client
        fake_module = ModuleType('google.genai')
        fake_module.types = SimpleNamespace(GenerateContentConfig=lambda **kwargs: kwargs)
        monkeypatch.setitem(sys.modules, 'google.genai', fake_module)
        monkeypatch.setattr(config, 'NEWS_EVENTS_LLM_PROVIDER', 'gemini')
        monkeypatch.setattr(config, 'GEMINI_MODEL', 'gemini-regression-model')

        annotations, usage = collector._llm_extract_batch(
            [{'title': '測試標題', 'summary': '測試摘要'}], gazetteer
        )

        assert annotations == {0: _annotation()}
        assert usage == {'input': 13, 'output': 5, 'cached': 2}
        assert client.models.generate_content.call_args.kwargs['model'] == 'gemini-regression-model'
