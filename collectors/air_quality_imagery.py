"""
airtw 空氣品質色階圖 PNG 收集器

每小時抓取環境部 airtw.moenv.gov.tw 發布的全台空品色階圖，
資料結構與 CWA 衛星雲圖一致（base64 傳輸、bytea 入庫），
PRIMARY KEY (product_type, observed_at) 天然去重。

端點:
    https://airtw.moenv.gov.tw/ModelSimulate/{YYYYMMDD}/output_{TYPE}_{YYYYMMDDHH}0000.png

產品類型: AQI / PM25 / PM10 / O3 / NO2 （airtw 首頁實際發布的 5 種）

寫入: live.aqi_imagery_frames (PK: product_type, observed_at)
"""

import base64
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import requests

import config
from collectors.base import BaseCollector
from storage.r2 import get_r2_storage

TAIPEI_TZ = timezone(timedelta(hours=8))

BASE_URL = "https://airtw.moenv.gov.tw/ModelSimulate"

DEFAULT_PRODUCTS = ["AQI", "PM25", "PM10", "O3", "NO2"]

# R2 CDN object key 副檔名對照（AR-11 read-path-cdn，比照 cwa_satellite）
# airtw 只發布 PNG，但沿用 mime→ext 對照表以與 cwa 範式一致
_EXT_BY_MIME = {
    'image/png': 'png',
}


def _ext_from_mime(mime_type: str) -> str:
    """由 MIME 判定副檔名（png），未知回 'bin'。"""
    return _EXT_BY_MIME.get((mime_type or '').lower(), 'bin')


def imagery_r2_key(product_type: str, observed_at, mime_type: str) -> str:
    """R2 object key 規約：imagery/aqi/{product_type}/{YYYYMMDD}/{HHMMSS}.{ext}

    時間一律取 observed_at 的 **UTC**。observed_at 可為 aware datetime 或
    ISO 字串（backfill 走字串 / DB datetime；collector 走 datetime）。
    naive datetime 視為 UTC。（規則與 cwa_satellite.imagery_r2_key 一致）
    """
    if isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    utc = observed_at.astimezone(timezone.utc)
    ext = _ext_from_mime(mime_type)
    return (
        f"imagery/aqi/{product_type}/"
        f"{utc.strftime('%Y%m%d')}/{utc.strftime('%H%M%S')}.{ext}"
    )


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
        # R2 雙寫（AR-11）：憑證未設 → None → 跳過上傳（image_key=None，DB 照寫）
        self._r2 = get_r2_storage()

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

    def _upload_to_r2(self, frame: dict, data: bytes) -> Optional[str]:
        """雙寫影像到 R2 CDN，回傳 object key。

        best-effort：R2 未設定或上傳失敗 → 回 None（image_key=None，DB 照寫），
        絕不因 CDN 失敗丟資料或 crash。
        """
        if self._r2 is None:
            return None
        key = imagery_r2_key(frame["product_type"], frame["observed_at"], frame["mime_type"])
        try:
            self._r2.upload_image(key, data, frame["mime_type"])
            return key
        except Exception as e:
            print(f"[{self.name}]   ⚠️ R2 上傳失敗 {key}: {e}")
            return None

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

            frame = {
                "product_type": product,
                "observed_at": observed_at,
                "image_b64": base64.b64encode(png).decode("ascii"),
                "image_size": len(png),
                "mime_type": "image/png",
                "product_url": url,
            }
            # R2 雙寫（best-effort，需在 observed_at 轉字串前算 key）
            frame["image_key"] = self._upload_to_r2(frame, png)
            frame["observed_at"] = observed_at.isoformat()

            frames.append(frame)
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
