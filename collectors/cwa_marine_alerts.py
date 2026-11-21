"""氣象署大潮監測（O-B0069-001）＋長浪監測（O-B0070-001）警戒狀態 → R2 最新快照（不寫 Supabase）。

B8 N16（2026-11-21）。來源走氣象署公開 S3（不需 CWA_API_KEY，美國 IP 實測 200 → 放 pulse-host）：
  https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Observation/<DataID>.json
每小時更新，內容是「過去 8 小時」各監測點最大潮高／浪高與警戒字串；兩份都沒有站號與座標，座標依 O-B0076-001
名稱對照（utils/tw_live_snapshots.py，對不上的不畫）。

放哪（ADR-0026 §5.1 Q2＝否）：每個人看都一樣的最新快照 → R2 `live/tw/cwa_tide_swell_alerts/latest.geojson`
（短快取）；正本是本機 JSON → 每日 archive 任務上傳 S3 `cwa_marine_alerts/archives/`。內容沒變就不重寫 R2。
前端可用 station_uid 疊到既有 marineObservationCwa 的測站上（警戒時脈動）。

健康：三份來源任一非 200／解析不到監測點 → raise；監測點全對不到座標 → raise；遇到沒看過的警戒字串只記
unknown_status（alert_level=None），不中斷（2026-11-21 實測只看過「低於警戒」）。
"""
from __future__ import annotations

from datetime import datetime

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.r2 import get_r2_storage
from utils.tw_live_snapshots import CWA_S3, MARINE_IDS, content_hash, marine_alerts_build

R2_KEY = "live/tw/cwa_tide_swell_alerts/latest.geojson"
R2_STATUS_KEY = "live/tw/cwa_tide_swell_alerts/status.json"
R2_CACHE_CONTROL = "public, max-age=300"


class CwaMarineAlertsCollector(BaseCollector):
    name = "cwa_marine_alerts"
    interval_minutes = config.CWA_MARINE_ALERTS_INTERVAL
    fixed_minutes = config.CWA_MARINE_ALERTS_MINUTES
    heartbeat_without_writer = True  # 不寫 DB；抓取／解析失敗一律 raise

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (cwa-marine-alerts)"})
        self._r2 = get_r2_storage()
        self._last_hash: str | None = None

    def _get(self, data_id: str) -> dict:
        r = self._session.get(CWA_S3 + f"{data_id}.json", timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()["cwaopendata"]

    def collect(self) -> dict:
        retrieved_at = datetime.now(TAIPEI_TZ).replace(microsecond=0).isoformat()
        docs = {k: self._get(v) for k, v in MARINE_IDS.items()}
        fc, status = marine_alerts_build(docs["spring"], docs["swell"], docs["stations"], retrieved_at)
        if not fc["features"]:
            raise RuntimeError(f"cwa_marine_alerts: 0 located sites (unlocated={len(status['unlocated'])})")
        h = content_hash(fc)
        published = False
        if self._r2 is not None and h != self._last_hash:
            self._r2.put_json(R2_KEY, fc, R2_CACHE_CONTROL)
            self._r2.put_json(R2_STATUS_KEY, {**status, "retrieved_at": retrieved_at, "content_sha256": h}, R2_CACHE_CONTROL)
            self._last_hash = h
            published = True
        return {
            "data": fc["features"],
            "feature_count": status["feature_count"], "unlocated": status["unlocated"],
            "unknown_status": status["unknown_status"], "alert_sites": status["alert_sites"],
            "r2_published": published, "r2_enabled": self._r2 is not None,
            # 上游凍結但仍回 200 時靠這個抓：觀測視窗結束時間（每小時往前）
            "_upstream_max_at": max((f["properties"]["obs_end"] or "") for f in fc["features"]) or None,
        }
