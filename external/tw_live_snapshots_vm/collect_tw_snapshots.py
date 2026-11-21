#!/usr/bin/env python3
"""HiCloud VM：疾管署登革熱聚集區（dengue）與航港局航船布告（nav）即時快照 → 本機 JSON（S3 正本）＋R2 最新快照。

兩個來源都擋境外 IP（2026-11-21 美國雲端 45 秒逾時），所以放台灣 IP 的 VM。主 repo 的 schema 正本是
collectors/cdc_dengue_cluster.py、collectors/motcmpb_nav_notices.py；解析邏輯是 utils/tw_live_snapshots.py，
部署時把那支檔案 scp 到本目錄（同名 tw_live_snapshots.py），不要另抄。

  python3 collect_tw_snapshots.py dengue   # cron 每日 1 次
  python3 collect_tw_snapshots.py nav      # cron 每小時

本機：/var/lib/tw-live-snapshots/data/<collector>/YYYY/MM/DD/<collector>_HHMM.json（archive_tw_snapshots.py 每日上 S3）。
  - dengue 每輪都存（KB 級，空檔日也要留：來源只給近兩週，S3 是唯一的歷史正本）。
  - nav 只在有效布告內容變動時存原檔（約 9 MB），狀態檔 state/nav_last_hash 記上次雜湊。
R2：環境檔有 R2_ACCESS_KEY_ID／R2_SECRET_ACCESS_KEY／R2_ENDPOINT_URL／R2_BUCKET 才寫（站主決定的方案 A）；
    沒有就只存本機與 S3、log 記 r2=skipped（方案 B 由別處鏡射）。
"""
from __future__ import annotations

import json
import logging
import os
import ssl
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter

APP_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(APP_DIR))
load_dotenv(APP_DIR / ".env")

import tw_live_snapshots as T  # noqa: E402  部署時從主 repo utils/ scp 過來

DATA_DIR = Path(os.environ.get("DATA_DIR", "/var/lib/tw-live-snapshots/data"))
STATE_DIR = Path(os.environ.get("STATE_DIR", "/var/lib/tw-live-snapshots/state"))
NAMES = {"dengue": "cdc_dengue_cluster", "nav": "motcmpb_nav_notices"}
R2_KEYS = {"dengue": "live/tw/dengue_cluster/", "nav": "live/tw/nav_notices/"}
R2_CACHE = "public, max-age=600"
UA = "GIS-DataCollectors/1.0 (tw-live-snapshots-vm)"


class _NoStrictTls(HTTPAdapter):
    """保留 CA／主機名驗證，只放寬 VERIFY_X509_STRICT（政府站憑證常缺 SKI）。"""

    def __init__(self):
        self._ctx = ssl.create_default_context()
        if hasattr(ssl, "VERIFY_X509_STRICT"):
            self._ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
        super().__init__()

    def init_poolmanager(self, connections, maxsize, block=False, **kw):
        kw["ssl_context"] = self._ctx
        return super().init_poolmanager(connections, maxsize, block=block, **kw)


def session() -> requests.Session:
    s = requests.Session()
    s.mount("https://", _NoStrictTls())
    s.headers.update({"User-Agent": UA})
    return s


def r2_client():
    keys = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL", "R2_BUCKET")
    if not all(os.environ.get(k) for k in keys):
        return None
    import boto3
    return boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT_URL"], region_name="auto",
                        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])


def r2_put(r2, key: str, obj) -> None:
    r2.put_object(Bucket=os.environ["R2_BUCKET"], Key=key, CacheControl=R2_CACHE,
                  ContentType="application/json; charset=utf-8",
                  Body=json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def write_local(collector: str, now: datetime, payload: dict) -> Path:
    d = DATA_DIR / collector / now.strftime("%Y/%m/%d")
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{collector}_{now.strftime('%H%M')}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False))
    tmp.replace(path)  # 原子寫入，避免歸檔打包到半個檔
    return path


def run_dengue(s, now):
    files, lm = {}, {}
    for name in T.DENGUE_CITIES + ["all"]:
        r = s.get(f"{T.DENGUE_BASE}DengueCluster_{name}.json", timeout=60)
        r.raise_for_status()
        files[name], lm[name] = r.json(), r.headers.get("Last-Modified")
    fc, st = T.dengue_normalise(files, now.strftime("%Y%m%d"), now.isoformat())
    if st["issues"].get("bad_geometry") or st["all_matches_city_union"] is False:
        raise RuntimeError(f"QA failed: {st['issues']} all_match={st['all_matches_city_union']}")
    st["source_last_modified"] = lm
    path = write_local(NAMES["dengue"], now, {"data": [{"geojson": fc, "status": st}], "row_count": st["feature_count"],
                                              "collected_at": now.isoformat()})
    return fc, st, path


def run_nav(s, now):
    r = s.get(T.NAV_URL, timeout=180)
    r.raise_for_status()
    src = r.json()
    latest, _all, st = T.nav_normalise(src, now)
    h = T.content_hash(latest)
    hp = STATE_DIR / "nav_last_hash"
    if hp.exists() and hp.read_text().strip() == h:
        return None, st, None  # 沒變：不存 9 MB、不重寫 R2
    path = write_local(NAMES["nav"], now, {"data": [{"geojson": src, "status": st}], "row_count": st["active_features"],
                                           "collected_at": now.isoformat()})
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    hp.write_text(h)
    return latest, st, path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    which = sys.argv[1] if len(sys.argv) > 1 else ""
    if which not in NAMES:
        logging.error("用法：collect_tw_snapshots.py dengue|nav")
        return 2
    now = datetime.now(T.TPE).replace(microsecond=0)
    fc, st, path = (run_dengue if which == "dengue" else run_nav)(session(), now)
    r2 = r2_client()
    r2_state = "skipped(no R2 keys)" if r2 is None else "unchanged"
    if r2 is not None and fc is not None:
        r2_put(r2, R2_KEYS[which] + "latest.geojson", fc)
        r2_put(r2, R2_KEYS[which] + "status.json", st)
        r2_state = "published"
    logging.info(f"{which}: features={st.get('feature_count', st.get('active_features'))} local={path} r2={r2_state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
