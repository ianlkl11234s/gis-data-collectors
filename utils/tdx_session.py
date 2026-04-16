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
    """自動節流的 requests.Session，所有請求都先 acquire 全域 TDX rate limiter"""

    def request(self, method, url, **kwargs):
        # 在每個請求送出前 acquire，確保全 TDX IP 出口合計不超過 rate limit
        get_tdx_rate_limiter().acquire()
        return super().request(method, url, **kwargs)
