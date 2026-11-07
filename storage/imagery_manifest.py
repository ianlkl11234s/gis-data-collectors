"""
影像 R2 manifest（每日幀清單）

目的：網站改從 R2 讀「當天有哪些影像幀」，不再經 Supabase RPC，
之後 DB 影像表（cwa_imagery_frames / aqi_imagery_frames / precipitation_raster_frames）
可以退役。契約見 docs/IMAGERY_R2_MANIFEST.md。

物件：
- imagery/<source>/manifest-<YYYY-MM-DD>.json   該 UTC 日的全部幀（依 product、observed_at 排序）
- imagery/<source>/latest.json                  每個 product 最新一幀（前端不依賴，給監控／除錯）

日期分桶一律 UTC，與影像 key（imagery/<source>/<product>/<YYYYMMDD>/<HHMMSS>.<ext>）一致。

寫法：read-merge-write。以 (product, observed_at) 去重（同一張最新幀每輪會再抓到），
內容沒變就不寫。每支 collector 只有單一實例在跑，所以不處理並發寫入。
best-effort：任何失敗只印警告、回傳 False，絕不影響 DB 寫入與 local 歸檔。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

MANIFEST_VERSION = 1
# 當天的 manifest 會一直被改寫 → 短快取；影像本體才用 immutable。
MANIFEST_CACHE_CONTROL = 'public, max-age=60'


def _to_utc(observed_at) -> datetime:
    if isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at.replace('Z', '+00:00'))
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return observed_at.astimezone(timezone.utc)


def utc_iso(observed_at) -> str:
    """統一成 'YYYY-MM-DDTHH:MM:SSZ'（秒精度）。"""
    return _to_utc(observed_at).strftime('%Y-%m-%dT%H:%M:%SZ')


def manifest_key(source: str, day: str) -> str:
    """day 為 UTC 'YYYY-MM-DD'。"""
    return f'imagery/{source}/manifest-{day}.json'


def latest_key(source: str) -> str:
    return f'imagery/{source}/latest.json'


def frame_entry(
    product: str,
    observed_at,
    key: str,
    mime_type: str,
    image_size: Optional[int],
    bbox: tuple[float, float, float, float],
) -> dict:
    """一筆幀。bbox = (lon_min, lat_min, lon_max, lat_max)，WGS84。"""
    lon_min, lat_min, lon_max, lat_max = bbox
    return {
        'product': str(product),
        'observed_at': utc_iso(observed_at),
        'key': key,
        'mime_type': mime_type,
        'image_size': int(image_size) if image_size is not None else None,
        'bbox': [float(lon_min), float(lat_min), float(lon_max), float(lat_max)],
    }


def _sort_key(f: dict):
    return (f['product'], f['observed_at'])


def merge_frames(existing: Iterable[dict], new: Iterable[dict]) -> tuple[list[dict], bool]:
    """合併幀清單：(product, observed_at) 相同時以新的為準。回傳 (排序後清單, 是否有變)。"""
    by_id: dict[tuple[str, str], dict] = {}
    for f in existing or []:
        by_id[_sort_key(f)] = f
    changed = False
    for f in new:
        k = _sort_key(f)
        if by_id.get(k) != f:
            by_id[k] = f
            changed = True
    return sorted(by_id.values(), key=_sort_key), changed


def group_by_day(entries: Iterable[dict]) -> dict[str, list[dict]]:
    days: dict[str, list[dict]] = {}
    for e in entries:
        days.setdefault(e['observed_at'][:10], []).append(e)
    return days


def build_manifest(source: str, day: str, frames: list[dict], updated_at: str) -> dict:
    return {
        'version': MANIFEST_VERSION,
        'source': source,
        'date': day,
        'date_basis': 'UTC',
        'updated_at': updated_at,
        'frames': frames,
    }


def merge_latest(existing: Optional[dict], entries: Iterable[dict]) -> tuple[dict, bool]:
    """latest.json 的 products 只在 observed_at 更新時前進（回填舊資料不會倒退）。"""
    products = dict((existing or {}).get('products') or {})
    changed = False
    for e in entries:
        cur = products.get(e['product'])
        if cur is None or e['observed_at'] > cur['observed_at']:
            products[e['product']] = e
            changed = True
    return {'products': dict(sorted(products.items()))}, changed


def publish_frames(r2, source: str, entries: list[dict], log_prefix: str = '') -> bool:
    """把新幀併進當日 manifest 與 latest.json。成功（含無變動）回 True，任何失敗回 False。"""
    if r2 is None or not entries:
        return True
    now = utc_iso(datetime.now(timezone.utc))
    ok = True
    for day, day_entries in sorted(group_by_day(entries).items()):
        key = manifest_key(source, day)
        try:
            existing = r2.get_json(key) or {}
            frames, changed = merge_frames(existing.get('frames') or [], day_entries)
            if changed:
                r2.put_json(key, build_manifest(source, day, frames, now), MANIFEST_CACHE_CONTROL)
        except Exception as e:  # noqa: BLE001 — best-effort
            ok = False
            print(f'{log_prefix}⚠️ manifest 更新失敗 {key}: {e}')
    try:
        lkey = latest_key(source)
        latest, changed = merge_latest(r2.get_json(lkey), entries)
        if changed:
            latest = {'version': MANIFEST_VERSION, 'source': source, 'updated_at': now, **latest}
            r2.put_json(lkey, latest, MANIFEST_CACHE_CONTROL)
    except Exception as e:  # noqa: BLE001 — best-effort
        ok = False
        print(f'{log_prefix}⚠️ latest.json 更新失敗 {source}: {e}')
    return ok
