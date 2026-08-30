"""
API 認證模組

提供 TDX 和 CWA API 的認證功能。
"""

import logging
import threading
import time
import requests
import config
from .tdx_session import TDXSession

logger = logging.getLogger(__name__)

# TDX token 端點連續失敗時的全域 backoff 參數（模組常數，刻意不走環境變數）。
# 首次失敗 backoff 60 秒，之後每次連續失敗倍增，上限 600 秒；任一 instance
# 取得 token 成功即全域歸零。倍增的上限套用在這裡；429 帶 Retry-After 時
# 直接尊重伺服器要求的等待時間，不受這個上限限制（見 _record_failure）。
_TOKEN_BACKOFF_INITIAL_SECONDS = 60.0
_TOKEN_BACKOFF_MAX_SECONDS = 600.0


class TDXAuth:
    """TDX API 認證管理器（使用 Session 重用連線）

    預設使用 TDXSession，讓 token refresh 也自動經過 TDX rate limiter，
    避免在 burst 場景下 auth 端點被 429 擋住（token refresh 失敗會連鎖影響所有 TDX 請求）。

    若 caller 傳入自己的 session，建議也使用 TDXSession 以保證節流生效。

    Token cache（`_access_token`/`_token_expiry`）是 per-instance（每個
    collector 各自一份，不共用）。但 token 端點連續失敗時的 backoff 狀態是
    class-level、跨所有 instance／thread 共用：任一 instance 取新 token 失敗
    都會讓「所有」instance 在 backoff 期間直接 fail fast，避免 16 支
    collector 各自重打壞掉的 token 端點造成 retry storm。
    """

    # class-level backoff 狀態（跨所有 instance／thread 共用），thread-safe。
    _backoff_lock = threading.Lock()
    _backoff_until = 0.0     # time.monotonic() 時間戳，0 代表目前不在 backoff
    _backoff_seconds = 0.0   # 目前連續失敗對應的 backoff 秒數，0 代表尚未失敗過

    def __init__(self, session: requests.Session = None):
        if not config.TDX_APP_ID or not config.TDX_APP_KEY:
            raise ValueError("TDX_APP_ID 和 TDX_APP_KEY 未設定")

        self._session = session or TDXSession()
        # 讓 TDXSession 在資料 API 收到 401 時，能回頭呼叫這個 TDXAuth 換新 token。
        self._session._tdx_auth = self
        self._access_token = None
        self._token_expiry = 0

    @classmethod
    def _check_backoff(cls):
        """backoff 生效中則直接 raise，不打網路"""
        with cls._backoff_lock:
            until = cls._backoff_until
        now = time.monotonic()
        if until > now:
            remaining = until - now
            eta = time.strftime('%H:%M:%S', time.localtime(time.time() + remaining))
            raise RuntimeError(
                f"TDX token 端點連續失敗，全域 backoff 至 {eta}"
                f"（尚餘 {remaining:.0f} 秒）才會再嘗試，本次不發送網路請求"
            )

    @classmethod
    def _record_failure(cls, status_code=None, retry_after_header=None):
        """記錄一次 token 端點失敗，更新全域 backoff（class-level，跨 instance）"""
        with cls._backoff_lock:
            doubled = (
                _TOKEN_BACKOFF_INITIAL_SECONDS
                if cls._backoff_seconds <= 0
                else cls._backoff_seconds * 2
            )
            doubled = min(doubled, _TOKEN_BACKOFF_MAX_SECONDS)

            if status_code == 429 and retry_after_header:
                try:
                    retry_after_seconds = float(retry_after_header)
                except (TypeError, ValueError):
                    retry_after_seconds = 0.0
                # 尊重伺服器要求的等待時間：取 max，且不受 600s 上限限制
                # （伺服器明確要求等更久時，比它更早重試正是本次要消滅的行為）。
                candidate = max(doubled, retry_after_seconds)
            else:
                candidate = doubled

            cls._backoff_seconds = candidate
            cls._backoff_until = time.monotonic() + candidate

    @classmethod
    def _record_success(cls):
        """任一 instance 取得 token 成功，全域 backoff 歸零"""
        with cls._backoff_lock:
            cls._backoff_seconds = 0.0
            cls._backoff_until = 0.0

    def invalidate(self):
        """清掉目前快取的 token，強制下次 get_access_token() 重新取號

        供 TDXSession 收到資料 API 401 時呼叫：清快取後緊接著呼叫
        get_access_token() 取得新 token 並 retry 該次請求。
        """
        self._access_token = None
        self._token_expiry = 0

    def get_access_token(self) -> str:
        """取得 Access Token（自動快取）"""
        if self._access_token and time.time() < self._token_expiry - 300:
            return self._access_token

        self._check_backoff()

        try:
            response = self._session.post(
                config.TDX_AUTH_URL,
                headers={'content-type': 'application/x-www-form-urlencoded'},
                data={
                    'grant_type': 'client_credentials',
                    'client_id': config.TDX_APP_ID,
                    'client_secret': config.TDX_APP_KEY
                },
                timeout=config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            resp = e.response
            status_code = resp.status_code if resp is not None else None
            body = (resp.text or '')[:200] if resp is not None else ''
            retry_after = resp.headers.get('Retry-After') if resp is not None else None
            # 保證不管上層 caller 抓到這個例外後怎麼處理（多數 collector 只印
            # e.response.status_code），log 裡都留得下 Keycloak 的 error body。
            logger.error(
                "TDX token 端點回應錯誤 status=%s retry_after=%s body=%s",
                status_code, retry_after, body,
            )
            TDXAuth._record_failure(status_code=status_code, retry_after_header=retry_after)
            # 保留原本的例外型別與 .response（既有 collector 多半用
            # `except requests.exceptions.HTTPError as e: e.response.status_code`
            # 接這個例外，型別換掉會漏接），只把 response body 併入訊息。
            e.args = (f"{e.args[0] if e.args else str(e)} | response body: {body}",) + tuple(e.args[1:])
            raise
        except requests.exceptions.RequestException as e:
            logger.error("TDX token 端點請求失敗（無 HTTP 回應）：%s", e)
            TDXAuth._record_failure()
            raise

        auth_data = response.json()
        self._access_token = auth_data['access_token']
        self._token_expiry = time.time() + auth_data.get('expires_in', 86400)
        TDXAuth._record_success()

        return self._access_token

    def get_auth_header(self) -> dict:
        """取得認證 Header"""
        return {
            'authorization': f'Bearer {self.get_access_token()}',
            'Accept-Encoding': 'gzip'
        }


class CWAAuth:
    """CWA 氣象局 API 認證管理器"""

    def __init__(self):
        if not config.CWA_API_KEY:
            raise ValueError("CWA_API_KEY 未設定")

        self.api_key = config.CWA_API_KEY

    def get_auth_params(self) -> dict:
        """取得認證參數"""
        return {'Authorization': self.api_key}
