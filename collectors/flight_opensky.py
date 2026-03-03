"""
OpenSky 台灣空域即時快照收集器

每 5 分鐘對台灣附近空域做 bounding box 快照，
記錄所有飛機的即時位置（state vectors）。

認證優先順序：
1. OAuth2 Client Credentials（新帳號，4,000 credits/天）
2. Basic Auth（舊帳號）
3. 匿名（400 credits/天）

API 文件：https://openskynetwork.github.io/opensky-api/rest.html
"""

import time as _time
from datetime import datetime, timezone

import requests

import config
from collectors.base import BaseCollector


# OpenSky state vector 欄位索引對照
# https://openskynetwork.github.io/opensky-api/rest.html#all-state-vectors
STATE_FIELDS = [
    "icao24",           # 0
    "callsign",         # 1
    "origin_country",   # 2
    "time_position",    # 3
    "last_contact",     # 4
    "longitude",        # 5
    "latitude",         # 6
    "baro_altitude",    # 7
    "on_ground",        # 8
    "velocity",         # 9
    "true_track",       # 10
    "vertical_rate",    # 11
    "sensors",          # 12
    "geo_altitude",     # 13
    "squawk",           # 14
    "spi",              # 15
    "position_source",  # 16
    "category",         # 17
]

# OAuth2 token endpoint
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)


class FlightOpenSkyCollector(BaseCollector):
    """OpenSky 台灣空域即時快照收集器"""

    name = "flight_opensky"
    interval_minutes = config.FLIGHT_OPENSKY_INTERVAL

    OPENSKY_URL = "https://opensky-network.org/api/states/all"
    BBOX = {
        "lamin": 21, "lamax": 27,
        "lomin": 117, "lomax": 123,
    }

    def __init__(self):
        super().__init__()
        self.auth = None
        self._access_token = None
        self._token_expires_at = 0

        # 認證：OAuth2 > Basic Auth > 匿名
        if config.FLIGHT_OPENSKY_CLIENT_ID and config.FLIGHT_OPENSKY_CLIENT_SECRET:
            self._oauth2 = True
            print(f"[{self.name}] 使用 OAuth2 ({config.FLIGHT_OPENSKY_CLIENT_ID})")
        elif config.FLIGHT_OPENSKY_USERNAME and config.FLIGHT_OPENSKY_PASSWORD:
            self._oauth2 = False
            self.auth = (config.FLIGHT_OPENSKY_USERNAME, config.FLIGHT_OPENSKY_PASSWORD)
            print(f"[{self.name}] 使用 Basic Auth ({config.FLIGHT_OPENSKY_USERNAME})")
        else:
            self._oauth2 = False
            print(f"[{self.name}] 匿名模式（10 秒解析度，400 credits/天）")

    # ------------------------------------------------------------------
    # OAuth2 token 管理
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str | None:
        """取得或刷新 OAuth2 access token（30 分鐘過期，提前 60 秒刷新）

        若 token endpoint 連線失敗，回傳 None 讓 collect() 降級為匿名查詢。
        """
        now = _time.time()
        if self._access_token and now < self._token_expires_at - 60:
            return self._access_token

        try:
            resp = requests.post(
                OPENSKY_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": config.FLIGHT_OPENSKY_CLIENT_ID,
                    "client_secret": config.FLIGHT_OPENSKY_CLIENT_SECRET,
                },
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            token_data = resp.json()

            self._access_token = token_data["access_token"]
            self._token_expires_at = now + token_data.get("expires_in", 1800)
            return self._access_token
        except Exception as e:
            print(f"[{self.name}] OAuth2 token 取得失敗，降級為匿名: {e}")
            self._access_token = None
            return None

    # ------------------------------------------------------------------
    # collect
    # ------------------------------------------------------------------

    def collect(self) -> dict:
        fetch_time = datetime.now(timezone.utc)

        # 組裝 request kwargs
        kwargs = {
            "params": self.BBOX,
            "timeout": config.REQUEST_TIMEOUT,
        }
        if self._oauth2:
            token = self._get_access_token()
            if token:
                kwargs["headers"] = {"Authorization": f"Bearer {token}"}
        elif self.auth:
            kwargs["auth"] = self.auth

        resp = requests.get(self.OPENSKY_URL, **kwargs)
        resp.raise_for_status()

        payload = resp.json()
        api_time = payload.get("time", 0)
        states = payload.get("states") or []

        # 轉換為 dict 列表
        aircraft_list = []
        for s in states:
            record = {}
            for i, field in enumerate(STATE_FIELDS):
                record[field] = s[i] if i < len(s) else None
            # 清理 callsign 空白
            if record.get("callsign"):
                record["callsign"] = record["callsign"].strip()
            aircraft_list.append(record)

        print(f"[{self.name}] 取得 {len(aircraft_list)} 架飛機")

        return {
            "fetch_time": fetch_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "api_time": api_time,
            "aircraft_count": len(aircraft_list),
            "bbox": self.BBOX,
            "data": aircraft_list,
        }
