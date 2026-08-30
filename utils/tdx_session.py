"""
TDXSession — 自動節流的 requests.Session 子類

用途：任何使用 TDX API 的 collector 只要把 `requests.Session()` 換成
`TDXSession()`，所有 HTTP 請求就會自動經過 TDX rate limiter。

設計原則：
1. 完全相容 requests.Session 介面（drop-in replacement）
2. 節流對 caller 透明（不需 caller 改任何 HTTP 呼叫程式碼）
3. 保留 session-level keep-alive 與 connection pooling

為何不用 HTTPAdapter？
- HTTPAdapter 的 send() 較底層，處理 redirect / retry 時可能重複觸發
- 在 request() 層級 intercept 比較乾淨，且語意單純
"""

import requests

from .rate_limiter import get_tdx_rate_limiter


class TDXSession(requests.Session):
    """自動節流的 requests.Session，所有請求都先 acquire 全域 TDX rate limiter

    資料 API 回 401（token 可能已失效或被撤銷）時，會嘗試換新 token 並 retry
    一次：呼叫掛在自己身上的 TDXAuth.invalidate() 清快取 → 重新取 token →
    重新 acquire rate limiter → 用新 token retry 該請求一次。只 retry 一次，
    第二次仍 401 就照原本行為把 response 原樣回傳（由 caller 的
    raise_for_status() 處理）。
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 由 TDXAuth.__init__ 掛上（同一個 session 給 TDXAuth 拿去打
        # token 端點，也給 collector 拿去打資料 API）。未掛 TDXAuth 時
        # 維持 None，401 時不做任何特殊處理，行為與原本相同。
        self._tdx_auth = None

    def request(self, method, url, **kwargs):
        # 在每個請求送出前 acquire，確保全 TDX IP 出口合計不超過 rate limit
        get_tdx_rate_limiter().acquire()
        response = super().request(method, url, **kwargs)

        if response.status_code == 401:
            response = self._retry_once_after_401(method, url, response, **kwargs)

        return response

    def _retry_once_after_401(self, method, url, response, **kwargs):
        """資料 API 401 時換新 token 並 retry 一次

        用「這次請求本來有沒有帶 authorization header」判斷是否要處理：
        TDXAuth 打 token 端點的 POST 不帶 authorization header，天然排除
        遞迴（否則 token 端點本身 401 時會無限迴圈呼叫 get_access_token()）。
        """
        auth = self._tdx_auth
        headers = kwargs.get('headers') or {}
        auth_keys = [k for k in headers if k.lower() == 'authorization']

        if auth is None or not auth_keys:
            return response

        auth.invalidate()
        new_token = auth.get_access_token()  # 換 token 失敗就直接往外拋

        new_headers = dict(headers)
        for k in auth_keys:
            del new_headers[k]
        new_headers['authorization'] = f'Bearer {new_token}'
        kwargs['headers'] = new_headers

        get_tdx_rate_limiter().acquire()
        return super().request(method, url, **kwargs)
