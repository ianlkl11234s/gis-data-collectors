"""
英國 Met Office 國家惡劣天氣警報（NSWWS）收集器 — 只記警報版本變動（gis-platform migration 562）

來源：Weather DataHub NSWWS v1.1（header `apikey`，環境變數 METOFFICE_WARNINGS_API_KEY；免費 20,000 次/日）
  GET {API_BASE}/objects/feed  → Atom（約 0.7 KB）：
      <link rel="related" href=".../objects/issued/<uuid>/">   目前全部「已發布」警報（不含 CANCELLED／EXPIRED）
      <updated>                                                最後異動時間
      <entry><id>urn:uuid:…</id><link rel="alternate" href=".../objects/updated/<uuid>/"/></entry>
          ← 單一變動事件（含 ISSUED／CANCELLED／EXPIRED），只在 feed 留 24 小時
  GET issued／updated href → GeoJSON FeatureCollection；geometry 一律 MultiPolygon。
  欄位依官方 v1.0 文件（https://metoffice.github.io/nswws-public-api/issued.html、updated.html）：
    warningId、warningVersion、warningStatus、warningLevel（RED/AMBER/YELLOW）、weatherType[]、
    warningLikelihood、warningImpact、validFromDate、validToDate、issuedDate、modifiedDate、
    warningHeadline、whatToExpect[]、warningFurtherDetails、warningUpdateDescription、affectedAreas[]。
  ⚠ 2026-11-20 實測當下沒有警報（issued 0 features、feed 沒有 entry），v1.1 真實警報的欄位尚未驗證：
    解析一律寬鬆（缺欄 → NULL），完整 properties 原樣存 jsonb。

每 10 分鐘：
  1. 抓 feed；related href 與 <updated> 都沒變、也沒有新 entry → 不抓其他東西（no_change，成功心跳）。
  2. related href 或 <updated> 變了 → 抓 issued；每個沒看過的 entry → 抓一次 updated（拿得到取消／過期）。
  3. 只送「沒見過的版本」進 DB：自然鍵 (modified_at, warning_id, warning_version, warning_status)，
     DO NOTHING。warning_status 必須在鍵裡：官方範例 EXPIRED 與 ISSUED 共用 version 2.0。
  有效與否用 valid_from／valid_to 與最新版本的 warning_status 判斷；事件列永不刪改（保留期另計）。

金鑰保護：只對以 API_BASE 開頭的 href 送 apikey；feed 給的連結不在這個網域就略過並警告。
狀態：LOCAL_DATA_DIR/state/metoffice_warnings_state.json，只在 DB 寫入成功後前進（run()）。
  遺失時只是把目前的警報再送一次，DO NOTHING 冪等，不必從 DB 重建。
"""

from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import requests
from shapely.geometry import MultiPolygon, shape  # import 失敗要大聲失敗，不可讓每個多邊形默默變 NULL

import config
from collectors.base import BaseCollector

API_BASE = "https://data.hub.api.metoffice.gov.uk/nswws/v1.1"
FEED_URL = f"{API_BASE}/objects/feed"
ATOM = "{http://www.w3.org/2005/Atom}"
# 看過的 entry／版本在 state 留多久（entry 只在 feed 留 24h；版本留到 valid_to 之後 7 天）
SEEN_ENTRY_TTL = timedelta(days=3)
SEEN_VERSION_GRACE = timedelta(days=7)
UNKNOWN_VERSION = "unknown"


def _parse_ts(v) -> Optional[datetime]:
    if not v or not isinstance(v, str):
        return None
    try:
        dt = datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _int(v) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == int(f) else None


def _text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _str_list(v) -> Optional[list[str]]:
    if v is None:
        return None
    items = v if isinstance(v, list) else [v]
    out = [str(x).strip() for x in items if x is not None and str(x).strip()]
    return out or None


def _uuid_from_href(href: str) -> Optional[str]:
    parts = [p for p in (href or "").split("/") if p]
    return parts[-1] if parts else None


def parse_feed(xml_text: str) -> dict:
    """Atom → {feed_updated, related_href, entries:[{id, href, updated}]}。"""
    root = ET.fromstring(xml_text)
    related = None
    for link in root.findall(f"{ATOM}link"):
        if link.get("rel") == "related":
            related = link.get("href")
    entries = []
    for e in root.findall(f"{ATOM}entry"):
        href = None
        for link in e.findall(f"{ATOM}link"):
            if link.get("rel") == "alternate":
                href = link.get("href")
        eid = _text(e.findtext(f"{ATOM}id")) or _uuid_from_href(href or "")
        if href and eid:
            entries.append({"id": eid, "href": href,
                            "updated": _iso(_parse_ts(e.findtext(f"{ATOM}updated")))})
    return {
        "feed_updated": _iso(_parse_ts(root.findtext(f"{ATOM}updated"))),
        "related_href": related,
        "entries": entries,
    }


def _to_multipolygon_ewkt(geometry) -> Optional[str]:
    """GeoJSON geometry → 'SRID=4326;MULTIPOLYGON(...)'；不是 (Multi)Polygon 或無效 → None。"""
    if not isinstance(geometry, dict) or geometry.get("type") not in ("Polygon", "MultiPolygon"):
        return None
    try:
        g = shape(geometry)
        if g.is_empty:
            return None
        if g.geom_type == "Polygon":
            g = MultiPolygon([g])
        return f"SRID=4326;{g.wkt}"
    except Exception:
        return None


def normalize_feature(feat: dict, *, source: str, source_uuid: Optional[str],
                      feed_updated: Optional[str], collected_at: str) -> Optional[dict]:
    """GeoJSON Feature → live.metoffice_warning_events 一列；沒有 warningId 回 None。

    鍵欄位的退路（都要 NOT NULL）：modified_at ← modifiedDate → issuedDate → feed <updated>；
    warning_version 缺 → 'unknown'；warning_status 缺 → issued 端點視為 ISSUED（官方：issued 只含已發布），
    updated 端點視為 UNKNOWN。
    """
    props = (feat or {}).get("properties") or {}
    wid = _text(props.get("warningId"))
    if not wid:
        return None
    modified = (_parse_ts(props.get("modifiedDate")) or _parse_ts(props.get("issuedDate"))
                or _parse_ts(feed_updated))
    if modified is None:
        return None
    status = _text(props.get("warningStatus"))
    status = status.upper() if status else ("ISSUED" if source == "issued" else "UNKNOWN")
    level = _text(props.get("warningLevel"))
    areas = props.get("affectedAreas") if isinstance(props.get("affectedAreas"), list) else None
    geometry = (feat or {}).get("geometry")
    return {
        "modified_at": modified.isoformat(),
        "warning_id": wid,
        "warning_version": _text(props.get("warningVersion")) or UNKNOWN_VERSION,
        "warning_status": status,
        "warning_level": level.upper() if level else None,
        "weather_types": _str_list(props.get("weatherType")),
        "warning_likelihood": _int(props.get("warningLikelihood")),
        "warning_impact": _int(props.get("warningImpact")),
        "valid_from": _iso(_parse_ts(props.get("validFromDate"))),
        "valid_to": _iso(_parse_ts(props.get("validToDate"))),
        "issued_at": _iso(_parse_ts(props.get("issuedDate"))),
        "headline": _text(props.get("warningHeadline")),
        "update_description": _text(props.get("warningUpdateDescription")),
        "region_codes": _str_list([a.get("regionCode") for a in areas or [] if isinstance(a, dict)]),
        "affected_areas": areas,
        "properties": props,
        "geom": _to_multipolygon_ewkt(geometry),
        "geometry_type": _text((geometry or {}).get("type")) if isinstance(geometry, dict) else None,
        "source": source,
        "source_uuid": source_uuid,
        "feed_updated_at": feed_updated,
        "collected_at": collected_at,
    }


def version_key(row: dict) -> str:
    return "|".join((row["modified_at"], row["warning_id"], row["warning_version"], row["warning_status"]))


class MetofficeWarningsCollector(BaseCollector):
    """NSWWS：feed 有變才抓 issued／新 entry 才抓 updated，只寫沒見過的警報版本。"""

    name = "metoffice_warnings"
    interval_minutes = config.METOFFICE_WARNINGS_INTERVAL
    heartbeat_without_writer = True  # 沒有變動的輪次不帶 data，也要有成功心跳；抓取失敗一律 raise

    def __init__(self, now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        super().__init__()
        self._now = now
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "metoffice_warnings_state.json"
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (metoffice_warnings)"})
        if config.METOFFICE_WARNINGS_API_KEY:
            self._session.headers["apikey"] = config.METOFFICE_WARNINGS_API_KEY

    def require_db_write(self) -> bool:
        # state 只在 DB 寫入成功後前進
        return True

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_state(self._pending_state)
        self._pending_state = None
        return stats

    def _load_state(self) -> dict:
        if self._state is None:
            try:
                st = json.loads(self.state_path.read_text())
                self._state = st if isinstance(st, dict) else None
            except (OSError, ValueError):
                self._state = None
        return self._state or {}

    def _save_state(self, st: dict) -> None:
        self._state = st
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(st, ensure_ascii=False))
            tmp.replace(self.state_path)
        except OSError as e:
            # 遺失只會讓目前警報重送一次（DO NOTHING 冪等），不擋
            print(f"[{self.name}] ⚠ state 檔寫入失敗: {e}")

    def _get(self, url: str) -> requests.Response:
        if not url.startswith(API_BASE + "/"):
            raise ValueError(f"拒絕對 API 網域外的連結送金鑰: {url[:80]}")
        resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp

    def _features(self, url: str) -> list:
        body = self._get(url).json()
        if not isinstance(body, dict) or not isinstance(body.get("features"), list):
            raise RuntimeError(f"NSWWS 回傳不是 FeatureCollection: {str(body)[:120]}")
        return body["features"]

    def collect(self) -> dict:
        if not config.METOFFICE_WARNINGS_API_KEY:
            raise RuntimeError("METOFFICE_WARNINGS_API_KEY 未設定")
        now = self._now()
        collected_at = now.isoformat()
        calls = 1
        feed = parse_feed(self._get(FEED_URL).text)
        if not feed["related_href"]:
            raise RuntimeError("NSWWS feed 沒有 rel=related（issued）連結，格式可能改了")

        prev = self._load_state()
        seen_entries: dict = dict(prev.get("seen_entries") or {})
        seen_versions: dict = dict(prev.get("seen_versions") or {})
        new_entries = [e for e in feed["entries"] if e["id"] not in seen_entries]
        issued_changed = (feed["related_href"] != prev.get("related_href")
                          or feed["feed_updated"] != prev.get("feed_updated"))

        if not issued_changed and not new_entries:
            print(f"[{self.name}] no change（feed updated={feed['feed_updated']}）")
            return {"no_change": True, "feed_updated": feed["feed_updated"], "api_calls": calls,
                    "collected_at": collected_at}

        rows: list[dict] = []
        skipped = 0
        issued_count = None

        def take(features: list, source: str, uuid: Optional[str]) -> None:
            nonlocal skipped
            for f in features:
                r = normalize_feature(f, source=source, source_uuid=uuid,
                                      feed_updated=feed["feed_updated"], collected_at=collected_at)
                if r is None:
                    skipped += 1
                else:
                    rows.append(r)

        raw: dict = {"feed": feed, "issued": None, "updated": {}}
        if issued_changed:
            feats = self._features(feed["related_href"])
            calls += 1
            issued_count = len(feats)
            raw["issued"] = feats
            take(feats, "issued", _uuid_from_href(feed["related_href"]))
        entry_failed = 0
        for e in new_entries:
            calls += 1
            try:
                feats = self._features(e["href"])
            except requests.HTTPError as err:
                # updated 連結只留 24 小時；404 代表已過期，記成看過、不再重試。其他錯誤下輪再試。
                entry_failed += 1
                status = getattr(getattr(err, "response", None), "status_code", None)
                print(f"[{self.name}] ⚠ updated entry {e['id']} 抓取失敗（HTTP {status}）")
                if status == 404:
                    seen_entries[e["id"]] = collected_at
                continue
            raw["updated"][e["id"]] = feats
            take(feats, "updated", _uuid_from_href(e["href"]))
            seen_entries[e["id"]] = collected_at  # 看到的時間（不是 entry 的 updated）

        # 同一版本在 issued 與 updated 都出現時只留一列；已寫過的版本不再送
        fresh: dict[str, dict] = {}
        for r in rows:
            k = version_key(r)
            if k not in seen_versions and k not in fresh:
                fresh[k] = r
        bad_geom = sum(1 for r in fresh.values() if r["geom"] is None)
        if skipped:
            print(f"[{self.name}] ⚠ {skipped} 個 feature 沒有 warningId／時間，略過")
        if bad_geom:
            print(f"[{self.name}] ⚠ {bad_geom} 個版本的 geometry 不是有效 (Multi)Polygon，geom 寫 NULL")

        # 清理 state：entry 自看到起留 3 天；版本留到 max(valid_to, 看到時間)＋7 天
        for k, v in list(seen_entries.items()):
            ts = _parse_ts(v)
            if ts is None or now - ts > SEEN_ENTRY_TTL:
                seen_entries.pop(k)
        for k, r in fresh.items():
            ends = _parse_ts(r["valid_to"])
            seen_versions[k] = max(ends, now).isoformat() if ends else now.isoformat()
        for k, v in list(seen_versions.items()):
            ts = _parse_ts(v)
            if ts is None or now - ts > SEEN_VERSION_GRACE:
                seen_versions.pop(k)

        self._pending_state = {
            "related_href": feed["related_href"], "feed_updated": feed["feed_updated"],
            "seen_entries": seen_entries, "seen_versions": seen_versions,
        }
        data = list(fresh.values())
        print(f"[{self.name}]   ✓ issued {issued_count if issued_count is not None else '-'} 筆、"
              f"新 entry {len(new_entries)}；新版本 {len(data)} 列，API {calls} 次")
        return {
            "data": data,
            "feed_updated": feed["feed_updated"],
            "issued_features": issued_count,
            "new_entries": len(new_entries),
            "new_versions": len(data),
            "skipped_features": skipped,
            "failed_entries": entry_failed,
            "invalid_geometry": bad_geom,
            "api_calls": calls,
            "collected_at": collected_at,
            # 上游原始 FeatureCollection（v1.1 欄位未驗證，S3 才是正本）只進本地 JSON → 每日歸檔 S3
            "raw_payload": raw,
        }
