"""
TDXSession 整合測試

驗證 TDXSession 是否正確觸發 rate limiter。
用 monkeypatch 攔截 requests 底層呼叫，避免真的打 TDX API。
"""

import time
from unittest.mock import patch, MagicMock

import pytest
import requests

from utils.rate_limiter import RateLimiter, get_tdx_rate_limiter
from utils.tdx_session import TDXSession


class TestTDXSession:
    """TDXSession 行為驗證"""

    def test_tdx_session_is_requests_session_subclass(self):
        """TDXSession 必須是 requests.Session 的子類（drop-in 相容）"""
        assert issubclass(TDXSession, requests.Session)

    def test_session_request_triggers_rate_limiter(self, monkeypatch):
        """每個 request 都應該 acquire 一次 rate limiter"""
        call_count = [0]

        def fake_acquire():
            call_count[0] += 1
            return 0.0

        # 替換 get_tdx_rate_limiter 的回傳為 mock
        fake_limiter = MagicMock()
        fake_limiter.acquire = fake_acquire

        # 攔截 requests.Session.request 避免真正送 HTTP
        def fake_request(self, method, url, **kwargs):
            return MagicMock(status_code=200)

        monkeypatch.setattr(
            'utils.tdx_session.get_tdx_rate_limiter',
            lambda: fake_limiter,
        )
        monkeypatch.setattr(requests.Session, 'request', fake_request)

        session = TDXSession()
        session.get('https://example.com')
        session.get('https://example.com')
        session.post('https://example.com')

        assert call_count[0] == 3, f"預期 3 次 acquire，實際 {call_count[0]}"

    def test_parallel_session_requests_serialize_through_limiter(self, monkeypatch):
        """多個 TDXSession 實例共用同一個 limiter，request 應被節流"""
        # 用真的 RateLimiter，但指向測試專用 instance（避免汙染 singleton）
        test_limiter = RateLimiter(rate_per_sec=10)  # 0.1s interval

        monkeypatch.setattr(
            'utils.tdx_session.get_tdx_rate_limiter',
            lambda: test_limiter,
        )

        # mock 掉真實 HTTP
        def fake_request(self, method, url, **kwargs):
            return MagicMock(status_code=200)

        monkeypatch.setattr(requests.Session, 'request', fake_request)

        # 兩個不同的 session 實例（模擬不同 collector）
        session_a = TDXSession()
        session_b = TDXSession()

        start = time.monotonic()
        session_a.get('https://example.com/a')
        session_b.get('https://example.com/b')
        session_a.get('https://example.com/c')
        session_b.get('https://example.com/d')
        elapsed = time.monotonic() - start

        # 4 次 request = 3 個間隔 × 0.1s = 0.3s
        assert 0.25 < elapsed < 0.5, (
            f"4 個跨 session request 耗時 {elapsed:.2f}s（預期 ~0.3s，代表節流生效）"
        )
