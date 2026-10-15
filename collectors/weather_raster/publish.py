"""JMA 發布幀（publish frame）：T2 網格 → 8-bit LA PNG → R2，給前端時間軸用。

契約：analytics docs/topic-research/japan_opendata/jma-frontend-contract.md（§3 R2、§4 編碼、§5 產品）。
R2 是可由 T2 重算的快取層（保留 90 天）；T2（S3）才是真本。發布失敗絕不影響 T2／T3。

編碼（PNG 不帶顏色，等級→顏色由前端 jmaTypes.ts 決定）：
  level_la_v1  L＝等級、A＝255；等級 0 與畫布外 → (0,0) 透明；缺測 255 → (255,255)。
               sea_ice：254 陸地 → 透明；255 未分析 → 缺測。開水面 0 依契約為透明。
  sst_la_v1    L＝clamp(round_half_up((°C＋3)×7), 1, 254)（前端：°C ≈ L/7 − 3）、A＝255；
               陸地 -32768 → A＝0；海冰 -32767 → (0,255)；無法解析 -32766 → (255,255)。
畫布 EPSG:3857、row 0＝北。z7 產品以 decode.pool_max 2×2 降到 z6（255 只在 4 格全缺時保留）。
經緯度網格（sst_him、sea_ice）以 nearest 轉 3857，寬＝min(原欄數, 2048)，不放大。

決定性：同一環境同輸入 → 同 bytes（固定 compress_level、無 pnginfo／時間戳）。
不同機器的 zlib／Pillow 版本可能讓 bytes 不同，冪等靠「R2 已有就跳過」保證，不靠重算 bytes 相同。
"""

from __future__ import annotations

import io
import math
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from PIL import Image

from collectors.weather_raster.decode import MISSING, pool_max

ENC_LEVEL = "level_la_v1"
ENC_SST = "sst_la_v1"
PNG_COMPRESS_LEVEL = 9
MAX_WIDTH = 2048
CONTENT_TYPE = "image/png"

SST_LAND, SST_ICE, SST_BAD = -32768, -32767, -32766
ICE_LAND = 254

# product → 發布規格。pool：tile 網格 k×k 取最大；kind：tile（已是 3857 網格）／lonlat（需轉 3857）／sst
PUBLISH_PRODUCTS: dict[str, dict] = {
    "radar": {"kind": "tile", "pool": 2},
    "rasrf": {"kind": "tile", "pool": 2},
    "risk_land": {"kind": "tile", "pool": 2},
    "risk_inund": {"kind": "tile", "pool": 2},
    "snow_depth": {"kind": "tile", "pool": 1},
    "uv_anal": {"kind": "tile", "pool": 1},
    "sea_ice": {"kind": "lonlat", "transparent": (0, ICE_LAND)},
    "sst_him": {"kind": "sst"},
}

IMAGE_FIELDS = ("image_key", "image_bytes", "image_width", "image_height",
                "lon_min", "lon_max", "lat_min", "lat_max", "encoding", "published_at")


# ───────────────────────── 純函式：編碼 ─────────────────────────

def png_la(L: np.ndarray, A: np.ndarray) -> bytes:
    """8-bit LA PNG（決定性：固定壓縮等級、不寫任何 metadata）。"""
    arr = np.ascontiguousarray(np.dstack([L.astype(np.uint8), A.astype(np.uint8)]))
    bio = io.BytesIO()
    Image.fromarray(arr, "LA").save(bio, format="PNG", compress_level=PNG_COMPRESS_LEVEL, optimize=False)
    return bio.getvalue()


def level_la(levels: np.ndarray, transparent=(0,)) -> tuple[np.ndarray, np.ndarray]:
    """等級網格 → (L, A)。transparent 內的值 → (0,0)；其餘 L＝等級（含 255 缺測）、A＝255。"""
    lv = np.asarray(levels, np.uint8)
    clear = np.isin(lv, np.asarray(transparent, np.uint8))
    L = np.where(clear, 0, lv).astype(np.uint8)
    A = np.where(clear, 0, 255).astype(np.uint8)
    return L, A


def sst_la(sst_dc: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """int16 0.1℃ 網格 → (L, A)，見模組說明 sst_la_v1。"""
    dc = np.asarray(sst_dc).astype(np.int32)
    # (°C+3)×7 ＝ (dc+30)×7/10；整數 round half up：floor((v+5)/10)
    L = np.clip(np.floor_divide((dc + 30) * 7 + 5, 10), 1, 254)
    A = np.full(dc.shape, 255, np.int32)
    land, ice, bad = dc == SST_LAND, dc == SST_ICE, dc == SST_BAD
    L[land], A[land] = 0, 0
    L[ice] = 0
    L[bad] = 255
    return L.astype(np.uint8), A.astype(np.uint8)


def decode_png_la(data: bytes) -> tuple[np.ndarray, np.ndarray]:
    """測試／驗證用：PNG → (L, A)。"""
    im = Image.open(io.BytesIO(data))
    if im.mode != "LA":
        raise ValueError(f"mode {im.mode} ≠ LA")
    a = np.asarray(im)
    return a[..., 0], a[..., 1]


# ───────────────────────── 純函式：經緯度 → 3857 ─────────────────────────

def _merc_y(lat: float) -> float:
    return math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


def _inv_merc_y(y: np.ndarray) -> np.ndarray:
    return np.degrees(2 * np.arctan(np.exp(y)) - math.pi / 2)


def lonlat_edges(georef: dict) -> list[float]:
    """GridProduct georef → 格邊界 [w, s, e, n]。point 註冊（值在格點上）外擴半格，nearest 時每格點代表 ±step/2。"""
    w, s, e, n = georef["bounds_lonlat"]
    if georef.get("registration") == "point":
        h = georef["step_deg"] / 2
        return [w - h, s - h, e + h, n + h]
    return [w, s, e, n]


def reproject_lonlat_to_3857(grid: np.ndarray, edges: list[float], max_width: int = MAX_WIDTH
                             ) -> tuple[np.ndarray, list[float]]:
    """規則經緯度網格（row 0＝北、col 0＝西，edges＝格邊界 [w,s,e,n]）→ 3857 畫布（nearest）。

    寬＝min(欄數, max_width)；高由 Mercator y 跨度以同一 m/px 決定。回 (畫布, [lon_min, lat_min, lon_max, lat_max])，
    畫布四角恰為 edges（無畫布外區域）。
    """
    nrows, ncols = grid.shape
    w, s, e, n = edges
    W = min(ncols, max_width)
    x_span = math.radians(e - w)
    y0, y1 = _merc_y(s), _merc_y(n)
    H = max(1, int(round((y1 - y0) / (x_span / W))))
    yc = y1 - (np.arange(H) + 0.5) * (y1 - y0) / H
    lat = _inv_merc_y(yc)
    rows = np.clip(np.floor((n - lat) / ((n - s) / nrows)).astype(np.int64), 0, nrows - 1)
    lon = w + (np.arange(W) + 0.5) * (e - w) / W
    cols = np.clip(np.floor((lon - w) / ((e - w) / ncols)).astype(np.int64), 0, ncols - 1)
    return grid[rows[:, None], cols[None, :]], [w, s, e, n]


# ───────────────────────── 產品 → PNG ─────────────────────────

def render(product: str, grid: np.ndarray, *, bounds_lonlat: Optional[list] = None,
           georef: Optional[dict] = None) -> dict:
    """依產品規格產 PNG。tile 產品給 bounds_lonlat（spool.georef 的 [w,s,e,n]，池化不改四角）；
    lonlat／sst 產品給 GridProduct 的 georef。回 {png, width, height, bounds, encoding}。"""
    spec = PUBLISH_PRODUCTS[product]
    kind = spec["kind"]
    if kind == "tile":
        g = pool_max(grid, spec["pool"]) if spec["pool"] > 1 else np.asarray(grid, np.uint8)
        L, A = level_la(g)
        bounds, enc = list(bounds_lonlat), ENC_LEVEL
    else:
        canvas, bounds = reproject_lonlat_to_3857(grid, lonlat_edges(georef))
        if kind == "sst":
            L, A = sst_la(canvas)
            enc = ENC_SST
        else:
            L, A = level_la(canvas, spec.get("transparent", (0,)))
            enc = ENC_LEVEL
    png = png_la(L, A)
    return {"png": png, "width": int(L.shape[1]), "height": int(L.shape[0]),
            "bounds": [float(b) for b in bounds], "encoding": enc}


def r2_key(product: str, valid_time: datetime) -> str:
    t = valid_time.astimezone(timezone.utc)
    return f"imagery/jma/{product}/{t:%Y%m%d}/{t:%H%M%S}.png"


def image_fields(key: str, r: dict, published_at: datetime) -> dict:
    """frames 列的 image 欄（皆為 Python 原生型別，可直接寫 state.json）。"""
    lon_min, lat_min, lon_max, lat_max = r["bounds"]
    return {"image_key": key, "image_bytes": len(r["png"]), "image_width": r["width"], "image_height": r["height"],
            "lon_min": lon_min, "lon_max": lon_max, "lat_min": lat_min, "lat_max": lat_max,
            "encoding": r["encoding"], "published_at": published_at.isoformat()}


# ───────────────────────── 發布（有副作用；失敗只 log）─────────────────────────

_r2_cache: dict = {}
_warned: set = set()


def _warn_once(tag: str, msg: str) -> None:
    if tag not in _warned:
        _warned.add(tag)
        print(msg)


def get_r2():
    """R2 未設定回 None（只 log 一次）。測試可直接傳 r2 參數覆寫。"""
    if "r2" not in _r2_cache:
        try:
            from storage.r2 import get_r2_storage
            _r2_cache["r2"] = get_r2_storage()
        except Exception as e:  # noqa: BLE001
            print(f"[jma-imagery] ✗ R2 初始化失敗：{e}")
            _r2_cache["r2"] = None
    return _r2_cache["r2"]


def enabled() -> bool:
    import config
    return bool(getattr(config, "JAPAN_JMA_IMAGERY_PUBLISH", True))


def publish(product: str, valid_time: datetime, grid: np.ndarray, *, bounds_lonlat=None, georef=None,
            r2=None, log_prefix: str = "[jma-imagery]") -> Optional[dict]:
    """編碼＋上傳一幀；成功回 image 欄 dict，任何失敗／停用／R2 未設定回 None（絕不 raise）。"""
    if product not in PUBLISH_PRODUCTS or not enabled():
        return None
    try:
        store = r2 if r2 is not None else get_r2()
        if store is None:
            _warn_once("no_r2", f"{log_prefix} R2 未設定，略過發布幀（T2/T3 照常）")
            return None
        r = render(product, grid, bounds_lonlat=bounds_lonlat, georef=georef)
        key = r2_key(product, valid_time)
        store.upload_image(key, r["png"], CONTENT_TYPE)
        return image_fields(key, r, datetime.now(timezone.utc))
    except Exception as e:  # noqa: BLE001
        print(f"{log_prefix} ✗ 發布幀失敗 {product} {valid_time.isoformat()}：{e}（T2/T3 不受影響）")
        return None
