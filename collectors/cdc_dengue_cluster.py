"""疾管署近兩週本土登革熱病例聚集區（data.gov.tw 179291）→ R2 最新快照（不寫 Supabase）。

B8 N02（2026-11-21）。od.cdc.gov.tw 擋境外 IP（美國雲端 45 秒逾時）→ 正式環境跑 HiCloud VM
（external/tw_live_snapshots_vm/），pulse-host 的 CDC_DENGUE_CLUSTER_ENABLED 一律維持 false，避免雙跑。
本檔是 schema 正本；解析邏輯在 utils/tw_live_snapshots.py（VM 版共用同一份）。

來源：22 縣市檔＋DengueCluster_all.json（data.gov.tw 只列 7 檔），每日更新，非流行季全是空 FeatureCollection。
放哪（ADR-0026 §5.1 Q2＝否）：R2 `live/tw/dengue_cluster/latest.geojson`；正本：本機 JSON → 每日 S3
`cdc_dengue_cluster/archives/`（來源只給近兩週、沒有歷史，S3 是唯一的歷史正本，空檔日也要留）。
健康：不適用「0 筆＝失敗」。判準是 23 檔都 200、都是合法 FeatureCollection、_all 與縣市聯集一致。
"""
from __future__ import annotations

import ssl
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from storage.r2 import get_r2_storage
from utils.tw_live_snapshots import DENGUE_BASE, DENGUE_CITIES, dengue_normalise

R2_KEY = "live/tw/dengue_cluster/latest.geojson"
R2_STATUS_KEY = "live/tw/dengue_cluster/status.json"
R2_CACHE_CONTROL = "public, max-age=600"


class _NoStrictTls(HTTPAdapter):
    """保留 CA／主機名驗證，只放寬 Python 3.13 起預設的 VERIFY_X509_STRICT（政府站憑證常缺 SKI）。"""

    def __init__(self):
        self._ctx = ssl.create_default_context()
        if hasattr(ssl, "VERIFY_X509_STRICT"):
            self._ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
        super().__init__()

    def init_poolmanager(self, connections, maxsize, block=False, **kw):
        kw["ssl_context"] = self._ctx
        return super().init_poolmanager(connections, maxsize, block=block, **kw)


class CdcDengueClusterCollector(BaseCollector):
    name = "cdc_dengue_cluster"
    interval_minutes = config.CDC_DENGUE_CLUSTER_INTERVAL
    heartbeat_without_writer = True

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.mount("https://", _NoStrictTls())
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (cdc-dengue-cluster)"})
        self._r2 = get_r2_storage()

    def collect(self) -> dict:
        now = datetime.now(TAIPEI_TZ).replace(microsecond=0)
        files, last_modified = {}, {}
        for name in DENGUE_CITIES + ["all"]:
            r = self._session.get(f"{DENGUE_BASE}DengueCluster_{name}.json", timeout=config.REQUEST_TIMEOUT)
            r.raise_for_status()
            files[name] = r.json()
            last_modified[name] = r.headers.get("Last-Modified")
        fc, status = dengue_normalise(files, now.strftime("%Y%m%d"), now.isoformat())
        if status["issues"].get("bad_geometry") or status["all_matches_city_union"] is False:
            raise RuntimeError(f"dengue cluster QA failed: {status['issues']} all_match={status['all_matches_city_union']}")
        published = False
        if self._r2 is not None:  # 每日一次、KB 級：每輪都重寫，status 的 snapshot_date 讓前端判斷是否過期
            self._r2.put_json(R2_KEY, fc, R2_CACHE_CONTROL)
            self._r2.put_json(R2_STATUS_KEY, {**status, "source_last_modified": last_modified}, R2_CACHE_CONTROL)
            published = True
        return {"data": [{"_type": "snapshot", "geojson": fc, "status": status, "source_last_modified": last_modified}],
                "feature_count": status["feature_count"], "snapshot_empty": status["snapshot_empty"],
                "r2_published": published, "r2_enabled": self._r2 is not None,
                "_source_date": now.strftime("%Y-%m-%d")}
