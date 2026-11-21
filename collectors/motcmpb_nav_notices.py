"""航港局航船布告（data.gov.tw 179037）→ R2 有效布告快照（不寫 Supabase）。

B8 N03（2026-11-21）。aton.motcmpb.gov.tw 擋境外 IP（美國雲端 45 秒逾時）→ 正式環境跑 HiCloud VM
（external/tw_live_snapshots_vm/），pulse-host 的 MOTCMPB_NAV_NOTICES_ENABLED 一律維持 false。
本檔是 schema 正本；解析在 utils/tw_live_snapshots.py（合併重複列、還原對調的時段欄、Circle／Sector 推算面、
遮蔽電話、東亞寬框外座標不畫）。

端點每次即時產生約 9 MB；新布告工作日每天數則 → 每小時抓一次。
放哪（ADR-0026 §5.1 Q2＝否）：R2 `live/tw/nav_notices/latest.geojson`（約 6 MB、gzip 約 0.5 MB）；
正本：只在內容變動時把原檔存本機 → 每日 S3 `motcmpb_nav_notices/archives/`（不變的輪次不重存，避免一天 200 MB）。
健康：非 200、不是非空 FeatureCollection → raise。
"""
from __future__ import annotations

from datetime import datetime

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.cdc_dengue_cluster import _NoStrictTls
from storage.r2 import get_r2_storage
from utils.tw_live_snapshots import NAV_URL, content_hash, nav_normalise

R2_KEY = "live/tw/nav_notices/latest.geojson"
R2_STATUS_KEY = "live/tw/nav_notices/status.json"
R2_CACHE_CONTROL = "public, max-age=600"


class MotcmpbNavNoticesCollector(BaseCollector):
    name = "motcmpb_nav_notices"
    interval_minutes = config.MOTCMPB_NAV_NOTICES_INTERVAL
    heartbeat_without_writer = True

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.mount("https://", _NoStrictTls())
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (motcmpb-nav-notices)"})
        self._r2 = get_r2_storage()
        self._last_hash: str | None = None

    def collect(self) -> dict:
        now = datetime.now(TAIPEI_TZ).replace(microsecond=0)
        r = self._session.get(NAV_URL, timeout=max(config.REQUEST_TIMEOUT, 120))
        r.raise_for_status()
        src = r.json()
        latest, _all, status = nav_normalise(src, now)
        h = content_hash(latest)
        result = {"feature_count": status["active_features"], "active_notices": status["active_notices"],
                  "unlocated_notices": status["unlocated_notices"], "suspect_coords": len(status["suspect_coords_excluded"]),
                  "_upstream_max_at": status["max_issued_at"], "r2_enabled": self._r2 is not None, "r2_published": False}
        if h == self._last_hash:
            return result  # 沒變：不存檔、不重寫 R2（成功心跳由 base 補）
        if self._r2 is not None:
            self._r2.put_json(R2_KEY, latest, R2_CACHE_CONTROL)
            self._r2.put_json(R2_STATUS_KEY, status, R2_CACHE_CONTROL)
            result["r2_published"] = True
        self._last_hash = h
        result["data"] = [{"_type": "source", "geojson": src, "status": status}]
        return result
