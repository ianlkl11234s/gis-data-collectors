"""
airtw 空氣品質色階圖 PNG 收集器

每小時抓取環境部 airtw.moenv.gov.tw 發布的全台空品色階圖，
資料結構與 CWA 衛星雲圖一致（base64 傳輸、bytea 入庫），
PRIMARY KEY (product_type, observed_at) 天然去重。

端點:
    https://airtw.moenv.gov.tw/ModelSimulate/{YYYYMMDD}/output_{TYPE}_{YYYYMMDDHH}0000.png

產品類型: AQI / PM25 / PM10 / O3 / NO2 （airtw 首頁實際發布的 5 種）

寫入: realtime.aqi_imagery_frames (PK: product_type, observed_at)
"""

import base64
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import requests

import config
from collectors.base import BaseCollector

TAIPEI_TZ = timezone(timedelta(hours=8))

BASE_URL = "https://airtw.moenv.gov.tw/ModelSimulate"

DEFAULT_PRODUCTS = ["AQI", "PM25", "PM10", "O3", "NO2"]


class AirQualityImageryCollector(BaseCollector):
    """airtw 全台空品色階圖 PNG 收集器"""

    name = "air_quality_imagery"
    interval_minutes = config.AIR_QUALITY_IMAGERY_INTERVAL

    def __init__(self):
        super().__init__()
        self.products = config.AIR_QUALITY_IMAGERY_PRODUCTS or DEFAULT_PRODUCTS
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (air-quality-imagery)",
        })

    def _build_url(self, product: str, dt: datetime) -> str:
        date_str = dt.strftime("%Y%m%d")
        stamp = dt.strftime("%Y%m%d%H")
        return f"{BASE_URL}/{date_str}/output_{product}_{stamp}0000.png"

    def _fetch_png(self, url: str) -> Optional[tuple[bytes, str]]:
        """回傳 (bytes, last_modified_header) 或 None (miss)"""
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        except requests.RequestException as e:
            print(f"[{self.name}]   ✗ {url} -> {e}")
            return None
        if resp.status_code != 200 or not resp.content.startswith(b"\x89PNG"):
            return None
        return resp.content, resp.headers.get("Last-Modified") or ""

    def _find_latest_hour(self, now: datetime, max_lookback: int = 3) -> Optional[datetime]:
        """從 now 往前探測最近可用整點（以 AQI 為探針）。"""
        for i in range(max_lookback + 1):
            candidate = (now - timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
            if self._fetch_png(self._build_url("AQI", candidate)) is not None:
                return candidate
        return None

    def _parse_observed_at(self, last_modified: str, fallback: datetime) -> datetime:
        if last_modified:
            try:
                return parsedate_to_datetime(last_modified)
            except (TypeError, ValueError):
                pass
        return fallback

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)
        target = self._find_latest_hour(fetch_time)
        if target is None:
            print(f"[{self.name}]   ⚠ 前 3 小時都無 AQI PNG，跳過")
            return {
                "fetch_time": fetch_time.isoformat(),
                "frame_count": 0,
                "total_bytes": 0,
                "data": [],
            }

        # target 是 naive datetime (來自 strftime/replace)，補上台灣時區
        if target.tzinfo is None:
            target = target.replace(tzinfo=TAIPEI_TZ)

        frames: list[dict] = []
        total_bytes = 0

        for product in self.products:
            url = self._build_url(product, target)
            got = self._fetch_png(url)
            if got is None:
                print(f"[{self.name}]   ⚠ miss: {product} @ {target.strftime('%Y-%m-%d %H:00')}")
                continue
            png, last_modified = got
            observed_at = self._parse_observed_at(last_modified, target)

            frames.append({
                "product_type": product,
                "observed_at": observed_at.isoformat(),
                "image_b64": base64.b64encode(png).decode("ascii"),
                "image_size": len(png),
                "mime_type": "image/png",
                "product_url": url,
            })
            total_bytes += len(png)
            print(f"[{self.name}]   ✓ {product:5s} {len(png)/1024:5.1f} KB")

        return {
            "fetch_time": fetch_time.isoformat(),
            "target_hour": target.isoformat(),
            "frame_count": len(frames),
            "total_bytes": total_bytes,
            "products": self.products,
            "data": frames,
        }
