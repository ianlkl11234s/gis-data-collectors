"""
API 認證模組

提供 TDX 和 CWA API 的認證功能。
"""

import time
import requests
import config


class TDXAuth:
    """TDX API 認證管理器"""

    def __init__(self):
        if not config.TDX_APP_ID or not config.TDX_APP_KEY:
            raise ValueError("TDX_APP_ID 和 TDX_APP_KEY 未設定")

        self._access_token = None
        self._token_expiry = 0

    def get_access_token(self) -> str:
        """取得 Access Token（自動快取）"""
        if self._access_token and time.time() < self._token_expiry - 300:
            return self._access_token

        response = requests.post(
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

        auth_data = response.json()
        self._access_token = auth_data['access_token']
        self._token_expiry = time.time() + auth_data.get('expires_in', 86400)

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
