"""
CWA 衛星雲圖 / 雷達回波圖 PNG 收集器

每 10 分鐘抓取 CWA Open Data 的影像類資料集，將 PNG bytes + metadata
寫入 Supabase 供前端時間軸動畫使用。

預設蒐集兩個資料集：
- O-C0042-004: 向日葵衛星真實色雲圖（台灣區域）
- O-A0058-005: 雷達整合回波圖（較大範圍、透明底）

每個資料集每 10 分鐘更新一次，PNG 大小約 100-500 KB。
存入 realtime.cwa_imagery_frames（PRIMARY KEY: dataset_id, observed_at）
天然去重，重複觀測不會重複寫入。

CWA 影像類資料集 metadata 端點:
    https://opendata.cwa.gov.tw/api/v1/rest/datastore/{dataset_id}?Authorization={key}
回傳 JSON 內含 cwaopendata.dataset.resource.ProductURL（PNG S3 連結）
與 cwaopendata.dataset.DateTime（觀測時間）
以及 parameterSet.LongitudeRange / LatitudeRange / ImageDimension（bbox + 解析度）
"""

import base64
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector
from storage.r2 import get_r2_storage

TAIPEI_TZ = timezone(timedelta(hours=8))

CWA_FILEAPI_URL = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/{dataset_id}"

# 預設蒐集的影像資料集
DEFAULT_DATASETS = [
    "O-C0042-004",  # 紅外線黑白衛星雲圖（台灣區域，~105 KB JPG）
    "O-A0058-005",  # 雷達回波圖（較大範圍、透明底，~180 KB PNG）
]

# R2 CDN object key 副檔名對照（AR-11 read-path-cdn）
_EXT_BY_MIME = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
}


def _mime_from_url(url: str) -> str:
    u = (url or '').lower()
    if u.endswith('.png'):
        return 'image/png'
    if u.endswith('.jpg') or u.endswith('.jpeg'):
        return 'image/jpeg'
    return 'application/octet-stream'


def _ext_from_mime(mime_type: str) -> str:
    """由 MIME 判定副檔名（png/jpg），未知回 'bin'。"""
    return _EXT_BY_MIME.get((mime_type or '').lower(), 'bin')


def imagery_r2_key(dataset_id: str, observed_at, mime_type: str) -> str:
    """R2 object key 規約：imagery/cwa/{dataset_id}/{YYYYMMDD}/{HHMMSS}.{ext}

    時間一律取 observed_at 的 **UTC**。observed_at 可為 aware datetime 或
    ISO 字串（backfill 走字串 / DB datetime；collector 走 datetime）。
    naive datetime 視為 UTC。
    """
    if isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    utc = observed_at.astimezone(timezone.utc)
    ext = _ext_from_mime(mime_type)
    return (
        f"imagery/cwa/{dataset_id}/"
        f"{utc.strftime('%Y%m%d')}/{utc.strftime('%H%M%S')}.{ext}"
    )


def _parse_range(s: str) -> tuple[float, float] | tuple[None, None]:
    """解析 '115.00-126.50' → (115.00, 126.50)"""
    if not s:
        return (None, None)
    try:
        a, b = s.split("-")
        return (float(a), float(b))
    except (ValueError, AttributeError):
        return (None, None)


def _parse_dim(s: str) -> tuple[int, int] | tuple[None, None]:
    """解析 '3600x3600' → (3600, 3600)"""
    if not s:
        return (None, None)
    try:
        w, h = s.lower().split("x")
        return (int(w), int(h))
    except (ValueError, AttributeError):
        return (None, None)


def _parse_iso(s: str) -> datetime | None:
    """解析 '2026-04-07T16:10:00+08:00' 為 timezone-aware datetime"""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


class CWASatelliteCollector(BaseCollector):
    """CWA 衛星雲圖 + 雷達 PNG 影像收集器"""

    name = "cwa_satellite"
    interval_minutes = config.CWA_SATELLITE_INTERVAL

    def __init__(self):
        super().__init__()
        if not config.CWA_API_KEY:
            raise ValueError("CWA_API_KEY 未設定，無法使用 cwa_satellite collector")

        self.datasets = config.CWA_SATELLITE_DATASETS or DEFAULT_DATASETS
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (cwa-imagery)",
        })
        # R2 雙寫（AR-11）：憑證未設 → None → 跳過上傳（image_key=None，DB 照寫）
        self._r2 = get_r2_storage()

    def _fetch_metadata(self, dataset_id: str) -> Optional[dict]:
        """從 CWA File API 取得影像 metadata（含 ProductURL）"""
        url = CWA_FILEAPI_URL.format(dataset_id=dataset_id)
        resp = self._session.get(
            url,
            params={"Authorization": config.CWA_API_KEY, "format": "JSON"},
            timeout=config.REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
        return resp.json()

    def _extract_frame(self, dataset_id: str, payload: dict) -> Optional[dict]:
        """從 CWA JSON 抽取單張影像的 metadata。
        相容兩種 schema：
        - 雷達 (O-A0058)：dataset.datasetInfo.parameterSet + dataset.DateTime + dataset.resource
        - 衛星雲圖 (O-C0042)：dataset.GeoInfo + dataset.ObsTime.Datetime + dataset.Resource
        """
        try:
            cwa = payload.get("cwaopendata", {})
            ds = cwa.get("dataset", {})

            # ── Resource block (大小寫並存) ──
            resource = ds.get("resource") or ds.get("Resource") or {}
            product_url = resource.get("ProductURL")
            resource_desc = resource.get("resourceDesc") or resource.get("ResourceDesc")

            # ── 觀測時間 ──
            observed_at_str = (
                ds.get("DateTime")
                or (ds.get("ObsTime") or {}).get("Datetime")
                or (ds.get("ObsTime") or {}).get("DateTime")
                or cwa.get("sent")
            )
            observed_at = _parse_iso(observed_at_str)

            # ── BBox：兩種放法 ──
            geo_info = ds.get("GeoInfo") or {}
            param_set = (ds.get("datasetInfo") or {}).get("parameterSet") or {}
            lon_range_str = geo_info.get("LongitudeRange") or param_set.get("LongitudeRange", "")
            lat_range_str = geo_info.get("LatitudeRange") or param_set.get("LatitudeRange", "")
            lon_min, lon_max = _parse_range(lon_range_str)
            lat_min, lat_max = _parse_range(lat_range_str)

            # ── ImageDimension：可能不存在 ──
            width, height = _parse_dim(param_set.get("ImageDimension", ""))

            if not product_url or not observed_at:
                print(f"[{self.name}]   ⚠️ {dataset_id} metadata 不完整，跳過")
                return None

            return {
                "dataset_id": dataset_id,
                "observed_at": observed_at,
                "product_url": product_url,
                "mime_type": _mime_from_url(product_url),
                "lon_min": lon_min,
                "lon_max": lon_max,
                "lat_min": lat_min,
                "lat_max": lat_max,
                "width": width,
                "height": height,
                "resource_desc": resource_desc,
            }
        except (AttributeError, KeyError, TypeError) as e:
            print(f"[{self.name}]   ✗ {dataset_id} metadata 解析失敗: {e}")
            return None

    def _download_png(self, url: str) -> Optional[bytes]:
        """下載 PNG bytes"""
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT * 2)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            print(f"[{self.name}]   ✗ 下載失敗 {url}: {e}")
            return None

    def _upload_to_r2(self, frame: dict, data: bytes) -> Optional[str]:
        """雙寫影像到 R2 CDN，回傳 object key。

        best-effort：R2 未設定或上傳失敗 → 回 None（image_key=None，DB 照寫），
        絕不因 CDN 失敗丟資料或 crash。
        """
        if self._r2 is None:
            return None
        key = imagery_r2_key(frame["dataset_id"], frame["observed_at"], frame["mime_type"])
        try:
            self._r2.upload_image(key, data, frame["mime_type"])
            return key
        except Exception as e:
            print(f"[{self.name}]   ⚠️ R2 上傳失敗 {key}: {e}")
            return None

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)
        frames: list[dict] = []
        total_bytes = 0

        for dataset_id in self.datasets:
            try:
                payload = self._fetch_metadata(dataset_id)
            except Exception as e:
                print(f"[{self.name}]   ✗ {dataset_id} metadata 抓取失敗: {e}")
                continue

            frame = self._extract_frame(dataset_id, payload)
            if not frame:
                continue

            png = self._download_png(frame["product_url"])
            if not png:
                continue

            # R2 雙寫（best-effort，需在 observed_at 轉字串前算 key）
            frame["image_key"] = self._upload_to_r2(frame, png)

            # JSON-safe：image_b64 給 local storage / supabase transformer 使用
            # observed_at 也要轉 ISO string（datetime 不可 JSON 序列化）
            frame["observed_at"] = frame["observed_at"].isoformat()
            frame["image_b64"] = base64.b64encode(png).decode("ascii")
            frame["image_size"] = len(png)
            total_bytes += len(png)
            frames.append(frame)
            print(
                f"[{self.name}]   ✓ {dataset_id} @ {frame['observed_at'][11:16]} "
                f"({len(png) / 1024:.0f} KB)"
            )

        return {
            "fetch_time": fetch_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "frame_count": len(frames),
            "total_bytes": total_bytes,
            "datasets": self.datasets,
            "data": frames,
        }
