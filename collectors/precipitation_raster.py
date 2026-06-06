"""
水利署 IoT 累積雨量柵格圖 collector（PNG raster）

資料來源：經濟部水利署 IoT 水資源物聯網
  https://iot.wra.gov.tw/
  端點：GET /rasterMap/precipitation?cumulativeHours=N （N=1~24）
  認證：OAuth2 client_credentials → Bearer JWT（30 min TTL，共用 USWG token）

特性：
  - 全台單一 PNG，bbox 約 (25.34, 119.98) ~ (21.86, 122.04)
  - 解析度 426m × 773m
  - PNG 二進位 → base64 transit → DB bytea（仿 cwa_satellite / aqi_imagery 模式）
  - TimeStamp 從 HTTP Header `RasterMepMetaData` 解析（注意官方 typo "Mep" 而非 "Map"）
  - 多 cumulativeHours：1=最新 1 小時雨量、3=最新 3 小時、6/12/24 ...

寫入：
  - realtime.precipitation_raster_frames（PK: cumulative_hours, observed_at）
  - ON CONFLICT DO NOTHING：同 ts × ch 不重寫

Standalone usage（dry-run，不寫 DB）：
  cd data-collectors
  export IOW_CLIENT_ID=...
  export IOW_CLIENT_SECRET=...
  python3 -m collectors.precipitation_raster --dry-run
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# 政府憑證缺 SKI（同 USWG / NHI ER 坑）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

IOW_BASE_URL  = "https://iot.wra.gov.tw"
IOW_TOKEN_URL = f"{IOW_BASE_URL}/Oauth2/token"
TOKEN_TTL_SEC = 25 * 60   # 30min 官方，提前 5min 刷新

# 預設抓哪些 cumulativeHours（多筆累積時長）
# 1/3/6 對應 CWA 短時/中時/長時雨量分析；24 累積給 24h 強降雨判定
DEFAULT_CH_LIST = [1, 3, 6, 24]


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:19], fmt)
            except ValueError:
                continue
        return None


def _extract_meta_header(headers) -> Optional[dict]:
    """RasterMepMetaData header（官方 typo: Mep 不是 Map）— 大小寫不敏感找"""
    for k in headers:
        if 'meta' in k.lower():
            try:
                return json.loads(headers[k])
            except (json.JSONDecodeError, TypeError):
                continue
    return None


class PrecipitationRasterCollector(BaseCollector):
    """水利署累積雨量 PNG raster 收集器（每 60 分鐘）"""

    name = "precipitation_raster"
    interval_minutes = getattr(config, "PRECIPITATION_RASTER_INTERVAL", 60)
    COLLECT_TIMEOUT: int = 120

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (precipitation-raster)",
            "Accept": "application/octet-stream, application/json",
        })
        self._token: Optional[str] = None
        self._token_obtained_at: float = 0.0

        # 共用 USWG 的 OAuth2 credentials
        self._client_id     = os.environ.get("IOW_CLIENT_ID")
        self._client_secret = os.environ.get("IOW_CLIENT_SECRET")
        if not self._client_id or not self._client_secret:
            print("[precipitation_raster] ⚠ 缺 IOW_CLIENT_ID / IOW_CLIENT_SECRET")

        # 從 env 讀取 ch list，預設 1,3,6,24
        ch_env = os.environ.get("PRECIPITATION_RASTER_CH_LIST", "")
        if ch_env:
            try:
                self.ch_list = [int(x) for x in ch_env.split(",") if x.strip()]
            except ValueError:
                self.ch_list = DEFAULT_CH_LIST
        else:
            self.ch_list = DEFAULT_CH_LIST

    # ------------------------------------------------------------
    # OAuth2 token
    # ------------------------------------------------------------
    def _get_token(self) -> str:
        if self._token and (time.time() - self._token_obtained_at) < TOKEN_TTL_SEC:
            return self._token

        resp = requests.post(
            IOW_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=15,
            verify=False,
        )
        resp.raise_for_status()
        tok = resp.json().get("access_token")
        if not tok:
            raise RuntimeError(f"token response missing access_token: {resp.text[:200]}")
        self._token = tok
        self._token_obtained_at = time.time()
        return tok

    # ------------------------------------------------------------
    # Fetch single raster
    # ------------------------------------------------------------
    def _fetch_raster(self, ch: int) -> Optional[dict]:
        """抓單一 cumulativeHours 的 PNG + metadata（Header）"""
        token = self._get_token()
        url = f"{IOW_BASE_URL}/rasterMap/precipitation?cumulativeHours={ch}"
        resp = self._session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=config.REQUEST_TIMEOUT if hasattr(config, "REQUEST_TIMEOUT") else 30,
            verify=False,
        )
        if not resp.ok:
            print(f"[precipitation_raster]   ✗ ch={ch}: {resp.status_code} {resp.text[:150]}")
            return None

        meta = _extract_meta_header(resp.headers)
        if not meta:
            print(f"[precipitation_raster]   ✗ ch={ch}: 找不到 RasterMepMetaData header")
            return None

        ts = _parse_dt(meta.get("TimeStamp"))
        if not ts:
            print(f"[precipitation_raster]   ✗ ch={ch}: TimeStamp 解析失敗 ({meta.get('TimeStamp')})")
            return None

        is_empty = bool(meta.get("IsEmptyRasterMap"))
        png = resp.content
        if is_empty or len(png) < 100:
            # 空圖（API 還沒生成此 ch）— 仍記 metadata 但 image_bytes 標 NULL
            return {
                "cumulative_hours": ch,
                "observed_at": ts.isoformat(),
                "image_bytes_b64": None,
                "image_size": 0,
                "is_empty": True,
                "ul_lat": meta.get("ULLatitude"),
                "ul_lng": meta.get("ULLongitude"),
                "br_lat": meta.get("BRLatitude"),
                "br_lng": meta.get("BRLongitude"),
                "width_m": meta.get("Width"),
                "height_m": meta.get("Height"),
                "source_url": url,
            }

        return {
            "cumulative_hours": ch,
            "observed_at": ts.isoformat(),
            "image_bytes_b64": base64.b64encode(png).decode("ascii"),
            "image_size": len(png),
            "is_empty": False,
            "ul_lat": meta.get("ULLatitude"),
            "ul_lng": meta.get("ULLongitude"),
            "br_lat": meta.get("BRLatitude"),
            "br_lng": meta.get("BRLongitude"),
            "width_m": meta.get("Width"),
            "height_m": meta.get("Height"),
            "source_url": url,
        }

    # ------------------------------------------------------------
    # collect()
    # ------------------------------------------------------------
    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        frames: list[dict] = []
        total_bytes = 0
        errors: list[str] = []

        for ch in self.ch_list:
            try:
                frame = self._fetch_raster(ch)
                if frame:
                    frames.append(frame)
                    total_bytes += frame.get("image_size", 0) or 0
                    ts_short = frame["observed_at"][11:16]
                    sz_kb = (frame.get("image_size") or 0) / 1024
                    empty_tag = " [EMPTY]" if frame.get("is_empty") else ""
                    print(
                        f"[precipitation_raster]   ✓ ch={ch:>2} @ {ts_short} "
                        f"({sz_kb:.0f} KB){empty_tag}"
                    )
            except Exception as e:
                err = f"ch={ch}: {e}"
                errors.append(err)
                print(f"[precipitation_raster]   ✗ {err}")

        return {
            "data":          frames,
            "fetch_time":    now.isoformat(),
            "frame_count":   len(frames),
            "total_bytes":   total_bytes,
            "ch_list":       self.ch_list,
            "errors":        errors,
        }


# ============================================================
# Standalone dry-run
# ============================================================
def _dry_run() -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    print("=" * 60)
    print("Precipitation Raster Collector — DRY RUN（不寫 DB）")
    print("=" * 60)

    coll = PrecipitationRasterCollector()
    coll.supabase_writer = None   # 強制不走 DB

    t0 = time.time()

    print(f"\n[1/3] OAuth2 token …")
    try:
        tok = coll._get_token()
        print(f"      ✅ token len={len(tok)}")
    except Exception as e:
        print(f"      ❌ {e}")
        return 1

    print(f"\n[2/3] Fetch cumulativeHours={coll.ch_list}")
    frames = []
    for ch in coll.ch_list:
        f = coll._fetch_raster(ch)
        if f:
            frames.append(f)

    print(f"\n[3/3] Summary")
    print(f"      frames: {len(frames)}")
    print(f"      total_bytes: {sum(f.get('image_size',0) for f in frames):,}")
    for f in frames:
        sz = f.get("image_size", 0)
        e = f.get("is_empty")
        sha = hashlib.sha256(
            base64.b64decode(f["image_bytes_b64"]) if f.get("image_bytes_b64") else b""
        ).hexdigest()[:12]
        print(f"      ch={f['cumulative_hours']:>2}  ts={f['observed_at']}  "
              f"sz={sz/1024:>5.1f}KB  empty={e}  sha={sha}  "
              f"bbox=({f['ul_lat']:.2f},{f['ul_lng']:.2f})~({f['br_lat']:.2f},{f['br_lng']:.2f})")

    print(f"\n[done] 耗時 {time.time() - t0:.1f}s")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(_dry_run())
    print("Precipitation Raster Collector. Use --dry-run to test.")
    sys.exit(0)
