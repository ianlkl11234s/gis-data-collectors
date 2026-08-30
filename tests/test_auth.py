"""
utils.auth 測試

背景：2026-08-30 TDX 端 secret 被重置，token 端點回 401，16 支 collector
每 2 分鐘各自重打 token 端點連打 15 小時（retry storm 連鎖 429），且
`raise_for_status()` 丟掉 response body，log 裡從沒出現關鍵的錯誤訊息。

涵蓋範圍：
1. Token 端點失敗時，例外訊息要含 response body（截斷 200 字內）
2. 全域 token-fetch 失敗 backoff（class-level、跨 instance／thread 共用）
3. 資料 API 401 時 TDXSession 自動換新 token 並 retry 恰好一次（utils.tdx_session）

用 mock/假 Response 攔截，不打真網路。
"""

import re
import time
from unittest.mock import MagicMock

import pytest
import requests

import config
from utils import auth as auth_module
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession


@pytest.fixture(autouse=True)
def _reset_tdx_auth_state(monkeypatch):
    """重置 TDXAuth 的 class-level backoff 狀態，並把 backoff 參數換成極小值

    backoff 狀態是 class-level（跨 instance 共用），測試間必須重置避免互相
    汙染；參數換成極小值讓「backoff 期間 fail fast → 時間過後可再試」這種
    測試不用真的等 60~600 秒。
    """
    monkeypatch.setattr(config, 'TDX_APP_ID', 'test-app-id')
    monkeypatch.setattr(config, 'TDX_APP_KEY', 'test-app-key')
    monkeypatch.setattr(auth_module, '_TOKEN_BACKOFF_INITIAL_SECONDS', 0.05)
    monkeypatch.setattr(auth_module, '_TOKEN_BACKOFF_MAX_SECONDS', 0.2)

    TDXAuth._backoff_until = 0.0
    TDXAuth._backoff_seconds = 0.0
    yield
    TDXAuth._backoff_until = 0.0
    TDXAuth._backoff_seconds = 0.0


def _make_http_error_response(status_code, body_text, headers=None):
    """建立真的 requests.Response，讓 raise_for_status() 丟出真的 HTTPError"""
    response = requests.Response()
    response.status_code = status_code
    response.reason = 'Error'
    response._content = body_text.encode('utf-8')
    if headers:
        response.headers.update(headers)
    return response


class TestTokenFailureMessage:
    """任務 1：token 端點失敗時，例外訊息要含 response body"""

    def test_401_error_message_includes_response_body(self):
        session = MagicMock()
        session.post.return_value = _make_http_error_response(
            401, 'Invalid client secret',
        )
        auth = TDXAuth(session=session)

        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            auth.get_access_token()

        assert 'Invalid client secret' in str(exc_info.value)
        # 型別與 .response 要保留，既有 16 支 collector 多半用
        # `except requests.exceptions.HTTPError as e: e.response.status_code` 接
        assert exc_info.value.response.status_code == 401

    def test_body_is_truncated_to_200_chars(self):
        session = MagicMock()
        long_body = 'x' * 500
        session.post.return_value = _make_http_error_response(500, long_body)
        auth = TDXAuth(session=session)

        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            auth.get_access_token()

        message = str(exc_info.value)
        body_in_message = message.split('response body: ', 1)[1]
        assert len(body_in_message) <= 200


class TestTokenBackoff:
    """任務 2：全域 token-fetch 失敗 backoff"""

    def test_failure_triggers_backoff_and_blocks_other_instances(self):
        session = MagicMock()
        session.post.return_value = _make_http_error_response(401, 'bad secret')
        auth = TDXAuth(session=session)

        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_access_token()
        assert session.post.call_count == 1

        # backoff 生效中：另一個 instance（模擬另一支 collector）也要 fail
        # fast，完全不打網路
        another_session = MagicMock()
        another_auth = TDXAuth(session=another_session)
        with pytest.raises(RuntimeError, match='backoff'):
            another_auth.get_access_token()
        assert another_session.post.call_count == 0

    def test_backoff_message_includes_deadline(self):
        session = MagicMock()
        session.post.return_value = _make_http_error_response(401, 'bad secret')
        auth = TDXAuth(session=session)
        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_access_token()

        with pytest.raises(RuntimeError) as exc_info:
            auth.get_access_token()
        assert re.search(r'backoff 至 \d{2}:\d{2}:\d{2}', str(exc_info.value))

    def test_backoff_expires_and_doubles_on_next_failure(self):
        session = MagicMock()
        session.post.return_value = _make_http_error_response(401, 'bad secret')
        auth = TDXAuth(session=session)

        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_access_token()
        first_backoff = TDXAuth._backoff_seconds
        assert first_backoff == pytest.approx(auth_module._TOKEN_BACKOFF_INITIAL_SECONDS)

        # backoff 期間 fail fast，不打網路
        with pytest.raises(RuntimeError):
            auth.get_access_token()
        assert session.post.call_count == 1

        time.sleep(first_backoff + 0.02)  # 模擬時間過後，backoff 已過期

        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_access_token()
        assert session.post.call_count == 2
        assert TDXAuth._backoff_seconds == pytest.approx(first_backoff * 2)

    def test_backoff_capped_at_max(self):
        TDXAuth._backoff_seconds = auth_module._TOKEN_BACKOFF_MAX_SECONDS
        TDXAuth._record_failure()
        assert TDXAuth._backoff_seconds == auth_module._TOKEN_BACKOFF_MAX_SECONDS

    def test_success_resets_backoff_globally(self):
        session = MagicMock()
        session.post.return_value = _make_http_error_response(401, 'bad secret')
        auth = TDXAuth(session=session)
        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_access_token()
        assert TDXAuth._backoff_seconds > 0

        time.sleep(TDXAuth._backoff_seconds + 0.02)

        ok_response = MagicMock()
        ok_response.raise_for_status.return_value = None
        ok_response.json.return_value = {'access_token': 'tok-1', 'expires_in': 3600}
        ok_session = MagicMock()
        ok_session.post.return_value = ok_response
        ok_auth = TDXAuth(session=ok_session)

        token = ok_auth.get_access_token()

        assert token == 'tok-1'
        assert TDXAuth._backoff_seconds == 0.0
        assert TDXAuth._backoff_until == 0.0

    def test_429_with_retry_after_uses_max(self):
        session = MagicMock()
        session.post.return_value = _make_http_error_response(
            429, 'slow down', headers={'Retry-After': '5'},
        )
        auth = TDXAuth(session=session)

        with pytest.raises(requests.exceptions.HTTPError):
            auth.get_access_token()

        # Retry-After(5s) 遠大於初始 backoff(0.05s)，應該取 Retry-After
        assert TDXAuth._backoff_seconds == pytest.approx(5.0)


class TestSessionRetryAfter401:
    """任務 3：資料 API 401 → invalidate → 重取 token → retry 恰好一次"""

    def test_retries_once_after_401_with_new_token(self, monkeypatch):
        monkeypatch.setattr(
            'utils.tdx_session.get_tdx_rate_limiter', lambda: MagicMock(),
        )

        session = TDXSession()
        auth = TDXAuth(session=session)

        invalidate_calls = []
        monkeypatch.setattr(auth, 'invalidate', lambda: invalidate_calls.append(1))
        monkeypatch.setattr(auth, 'get_access_token', lambda: 'new-token')

        responses = [MagicMock(status_code=401), MagicMock(status_code=200)]
        call_log = []

        def fake_request(self, method, url, **kwargs):
            call_log.append(kwargs.get('headers'))
            return responses[len(call_log) - 1]

        monkeypatch.setattr(requests.Session, 'request', fake_request)

        result = session.get(
            'https://tdx.transportdata.tw/api/basic/v2/some/data',
            headers={'authorization': 'Bearer old-token'},
        )

        assert result.status_code == 200
        assert len(call_log) == 2
        assert invalidate_calls == [1]
        assert call_log[0]['authorization'] == 'Bearer old-token'
        assert call_log[1]['authorization'] == 'Bearer new-token'

    def test_second_401_is_returned_as_is(self, monkeypatch):
        monkeypatch.setattr(
            'utils.tdx_session.get_tdx_rate_limiter', lambda: MagicMock(),
        )
        session = TDXSession()
        auth = TDXAuth(session=session)
        monkeypatch.setattr(auth, 'invalidate', lambda: None)
        monkeypatch.setattr(auth, 'get_access_token', lambda: 'still-bad-token')

        call_count = [0]

        def fake_request(self, method, url, **kwargs):
            call_count[0] += 1
            return MagicMock(status_code=401)

        monkeypatch.setattr(requests.Session, 'request', fake_request)

        result = session.get(
            'https://tdx.transportdata.tw/api/basic/v2/some/data',
            headers={'authorization': 'Bearer old-token'},
        )

        assert result.status_code == 401
        assert call_count[0] == 2  # 原本 1 次 + retry 1 次，恰好 retry 一次

    def test_no_retry_when_request_has_no_authorization_header(self, monkeypatch):
        """token 端點本身的 401 不帶 authorization header，不應觸發 retry

        （否則會遞迴呼叫 auth.get_access_token()，該方法本身又是打同一個
        session，無限迴圈）。
        """
        monkeypatch.setattr(
            'utils.tdx_session.get_tdx_rate_limiter', lambda: MagicMock(),
        )
        session = TDXSession()
        auth = TDXAuth(session=session)
        get_token_calls = []
        monkeypatch.setattr(
            auth, 'get_access_token',
            lambda: get_token_calls.append(1) or 'token',
        )

        call_count = [0]

        def fake_request(self, method, url, **kwargs):
            call_count[0] += 1
            return MagicMock(status_code=401)

        monkeypatch.setattr(requests.Session, 'request', fake_request)

        result = session.post(
            config.TDX_AUTH_URL,
            headers={'content-type': 'application/x-www-form-urlencoded'},
        )

        assert result.status_code == 401
        assert call_count[0] == 1
        assert get_token_calls == []

    def test_no_retry_without_linked_auth(self, monkeypatch):
        """TDXSession 沒掛 TDXAuth 時，401 行為與原本相同（不 retry）"""
        monkeypatch.setattr(
            'utils.tdx_session.get_tdx_rate_limiter', lambda: MagicMock(),
        )
        session = TDXSession()

        call_count = [0]

        def fake_request(self, method, url, **kwargs):
            call_count[0] += 1
            return MagicMock(status_code=401)

        monkeypatch.setattr(requests.Session, 'request', fake_request)

        result = session.get(
            'https://tdx.transportdata.tw/api/basic/v2/some/data',
            headers={'authorization': 'Bearer old-token'},
        )

        assert result.status_code == 401
        assert call_count[0] == 1
