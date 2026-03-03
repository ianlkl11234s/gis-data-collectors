"""
FlightRadar24 台灣空域即時快照收集器（Zone Feed）

使用 FR24 公開 feed endpoint 做 bounding box 快照，
每 5 分鐘抓取台灣空域內所有飛機的即時位置。

與其他飛機收集器的互補關係：
- flight_fr24: 台灣起降航班的完整軌跡（trail）
- flight_opensky: 精確高度、垂直速率
- flight_fr24_zone（本收集器）: 最多飛機數、含 origin/destination

三者都有 icao24，分析時可直接 merge。

資料來源：FlightRadar24（非官方，僅供教育用途）
"""

import random
from datetime import datetime, timezone

import requests

import config
from .base import BaseCollector


class FlightFR24ZoneCollector(BaseCollector):
    """FlightRadar24 台灣空域即時快照收集器"""

    name = "flight_fr24_zone"
    interval_minutes = config.FLIGHT_FR24_ZONE_INTERVAL

    FEED_URL = "https://data-cloud.flightradar24.com/zones/fcgi/feed.js"

    # 獨立 User-Agent 池（與 flight_fr24 不同，避免同時被封）
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]

    # feed.js 回傳的 array 欄位索引對照
    # [icao24, lat, lng, track, alt_ft, speed_kts, squawk,
    #  radar, aircraft_type, registration, timestamp, origin, destination,
    #  flight_number, on_ground, vertical_speed, callsign, ?, airline_icao]
    IDX_ICAO24 = 0
    IDX_LAT = 1
    IDX_LNG = 2
    IDX_TRACK = 3
    IDX_ALT_FT = 4
    IDX_SPEED_KTS = 5
    IDX_SQUAWK = 6
    IDX_RADAR = 7
    IDX_AIRCRAFT_TYPE = 8
    IDX_REGISTRATION = 9
    IDX_TIMESTAMP = 10
    IDX_ORIGIN = 11
    IDX_DESTINATION = 12
    IDX_FLIGHT_NUMBER = 13
    IDX_ON_GROUND = 14
    IDX_VERTICAL_SPEED = 15
    IDX_CALLSIGN = 16

    def __init__(self):
        super().__init__()
        self._session = requests.Session()

    def _get_headers(self) -> dict:
        """產生隨機瀏覽器 headers"""
        return {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "application/json",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Origin": "https://www.flightradar24.com",
            "Referer": "https://www.flightradar24.com/",
        }

    def _safe_get(self, arr, idx, default=None):
        """安全取得 array 元素"""
        try:
            val = arr[idx]
            return val if val is not None else default
        except (IndexError, TypeError):
            return default

    def collect(self) -> dict:
        """抓取台灣空域內所有飛機的即時位置"""
        fetch_time = datetime.now(timezone.utc)

        bbox = {
            "lamin": config.FLIGHT_FR24_ZONE_LAMIN,
            "lamax": config.FLIGHT_FR24_ZONE_LAMAX,
            "lomin": config.FLIGHT_FR24_ZONE_LOMIN,
            "lomax": config.FLIGHT_FR24_ZONE_LOMAX,
        }

        params = {
            "bounds": f"{bbox['lamax']},{bbox['lamin']},{bbox['lomin']},{bbox['lomax']}",
            "faa": "1",
            "satellite": "1",
            "mlat": "1",
            "flarm": "0",      # 排除滑翔機
            "adsb": "1",
            "gnd": "0",        # 排除地面車輛
            "air": "1",
            "vehicles": "0",   # 排除地面車輛
            "estimated": "1",
            "gliders": "0",    # 排除滑翔機
            "stats": "0",
        }

        resp = self._session.get(
            self.FEED_URL,
            params=params,
            headers=self._get_headers(),
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        raw = resp.json()

        # feed.js 回傳格式：頂層 key 為 fr24_id（hex 字串），
        # 值為 array；另有 "full_count", "version" 等 metadata key
        aircraft_list = []
        for key, value in raw.items():
            # 跳過 metadata keys（非飛機資料）
            if not isinstance(value, list):
                continue

            icao24_raw = self._safe_get(value, self.IDX_ICAO24, "")
            lat = self._safe_get(value, self.IDX_LAT)
            lng = self._safe_get(value, self.IDX_LNG)

            # 跳過沒有座標的記錄
            if lat is None or lng is None:
                continue

            aircraft_list.append({
                "icao24": icao24_raw.lower() if isinstance(icao24_raw, str) else "",
                "callsign": (self._safe_get(value, self.IDX_CALLSIGN, "") or "").strip(),
                "registration": self._safe_get(value, self.IDX_REGISTRATION, ""),
                "aircraft_type": self._safe_get(value, self.IDX_AIRCRAFT_TYPE, ""),
                "latitude": lat,
                "longitude": lng,
                "altitude_ft": self._safe_get(value, self.IDX_ALT_FT, 0),
                "speed_kts": self._safe_get(value, self.IDX_SPEED_KTS, 0),
                "track": self._safe_get(value, self.IDX_TRACK, 0),
                "origin_iata": self._safe_get(value, self.IDX_ORIGIN, ""),
                "destination_iata": self._safe_get(value, self.IDX_DESTINATION, ""),
                "vertical_speed": self._safe_get(value, self.IDX_VERTICAL_SPEED, 0),
                "on_ground": bool(self._safe_get(value, self.IDX_ON_GROUND, 0)),
                "squawk": self._safe_get(value, self.IDX_SQUAWK, ""),
                "timestamp": self._safe_get(value, self.IDX_TIMESTAMP, 0),
                "fr24_id": key,
            })

        print(f"[{self.name}] 取得 {len(aircraft_list)} 架飛機")

        return {
            "fetch_time": fetch_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "aircraft_count": len(aircraft_list),
            "bbox": {
                "lamin": bbox["lamin"],
                "lamax": bbox["lamax"],
                "lomin": bbox["lomin"],
                "lomax": bbox["lomax"],
            },
            "data": aircraft_list,
        }
