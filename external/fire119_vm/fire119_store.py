"""即時 119／警廣：組列、逐列驗證（對齊 gis-platform migration 555 的 CHECK）、本機差異狀態、DB 寫入 SQL。

寫入契約（live.fire119_incidents、live.pbs_road_events，mig 555）：
  - geom 與 geom_precision 一致：exact／fuzzy／road 必有點；district／none 必為 NULL。
    不合者逐列降級（road 對不到 → district、geom NULL），不讓整批失敗。
  - dest_geom 只給新北（送醫目的醫院）。
  - incident_key 原生 id 帶來源前綴（ntpc:、tainan:）；status_history 是 [[ts, status_raw], ...]。
  - 只在「新案件」或「內容變化（狀態、分隊、地點…）」時 upsert；last_seen_at 另外每 30 分鐘批次更新。

DB 寫入可重放（vm_buffer 補寫可能晚到或重複）：
  - ON CONFLICT 時只在 status_raw 真的不同才 append status_history；units 取聯集；
    first_seen_at 不動；last_seen_at 取 GREATEST；整列用 WHERE EXCLUDED.updated_at > t.updated_at 擋舊資料。
  - 同一批同 key 只留最後一筆（ON CONFLICT DO UPDATE 不允許同批重複 key）。
  - SQL 形狀固定（vm_common/bulk.py，鐵則 R5）；只用交易內 SET LOCAL（Supavisor transaction pool）。
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime, timedelta
from pathlib import Path

from fire119_parse import TZ, incident_key, map_category, map_status

CATEGORIES = {"fire", "ems", "rescue", "other"}
STATUSES = {"received", "dispatched", "arrived", "transporting", "returned", "other"}
PRECISIONS = {"exact", "fuzzy", "road", "district", "none"}
PBS_STATUSES = {"reported", "ongoing", "cleared"}
# 台灣含離島（金門 118.2E、馬祖 26.4N、東沙、蘭嶼）的寬鬆外框；框外座標視為雜訊
LON_RANGE = (116.5, 123.0)
LAT_RANGE = (20.4, 26.6)


def _valid_point(lon, lat) -> bool:
    try:
        lon, lat = float(lon), float(lat)
    except (TypeError, ValueError):
        return False
    return (math.isfinite(lon) and math.isfinite(lat)
            and LON_RANGE[0] <= lon <= LON_RANGE[1] and LAT_RANGE[0] <= lat <= LAT_RANGE[1])


def _iso(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=TZ)
        return v.astimezone(TZ).isoformat(timespec="seconds")
    return str(v)


# ── 119：解析結果 → 寫入列 ───────────────────────────────────────────
def build_incident(source: str, county: str, r: dict, seen: datetime, gazetteer=None) -> dict | None:
    """一筆解析結果 → 符合 mig 555 的列（尚未驗證）。location_text 以「測試」開頭的回 None（丟掉）。"""
    loc = (r.get("location_text") or "").strip()
    if loc.startswith("測試"):
        return None
    category = map_category(r.get("category_raw"))
    lon, lat, prec = r.get("lon"), r.get("lat"), r.get("precision") or "none"
    road_key = town_code = None
    # 路名對照：臺北的精確／模糊點也查（要 road_key 讓前端高亮整條路、town_code 給行政區面量）；
    # 新北沒有事發地文字，不查。精度只從對照結果來（road 一定帶中點），不從文字猜。
    if gazetteer is not None and loc and source != "new_taipei":
        try:
            m = gazetteer.match(county, loc)
        except Exception:  # noqa: BLE001 — 對照表壞掉不影響收集，當對不到
            m = None
        if m:
            town_code = m.get("town_code")
            if m.get("precision") == "road":
                road_key = m.get("road_key")
            if prec not in ("exact", "fuzzy"):
                if m.get("precision") == "road" and m.get("lon") is not None and m.get("lat") is not None:
                    lon, lat, prec = m["lon"], m["lat"], "road"
                elif m.get("precision") == "district":
                    lon = lat = None
                    prec = "district"
    seen_iso = _iso(seen)
    status_raw = r.get("status_raw") or ""
    dest_ok = source == "new_taipei" and _valid_point(r.get("dest_lon"), r.get("dest_lat"))
    return dict(
        incident_key=incident_key(source, r), source=source, county=county,
        category=category, category_raw=r.get("category_raw") or None,
        status=map_status(status_raw, category), status_raw=status_raw or None,
        status_history=[[seen_iso, status_raw]],
        received_at=_iso(r.get("received_at")), location_text=loc or None,
        lon=lon, lat=lat, geom_precision=prec, road_key=road_key, town_code=town_code,
        units=list(dict.fromkeys(u for u in (r.get("units") or []) if u)),
        hospital=r.get("hospital") or None,
        dest_lon=r.get("dest_lon") if dest_ok else None, dest_lat=r.get("dest_lat") if dest_ok else None,
        first_seen_at=seen_iso, last_seen_at=seen_iso, updated_at=seen_iso)


def validate_incident(row: dict) -> tuple[dict | None, list[str]]:
    """逐列對齊 mig 555 的 CHECK；能降級就降級，回 (row 或 None, 降級說明)。"""
    notes: list[str] = []
    if not row.get("incident_key") or not row.get("source") or not row.get("county"):
        return None, ["缺 incident_key／source／county，丟棄"]
    if row.get("category") not in CATEGORIES:
        notes.append(f"category {row.get('category')!r}→other")
        row["category"] = "other"
    if row.get("status") not in STATUSES:
        notes.append(f"status {row.get('status')!r}→other")
        row["status"] = "other"
    prec = row.get("geom_precision")
    if prec not in PRECISIONS:
        notes.append(f"precision {prec!r}→none")
        prec = "none"
    has_pt = row.get("lon") is not None or row.get("lat") is not None
    if has_pt and not _valid_point(row.get("lon"), row.get("lat")):
        notes.append("座標不合法或在台灣外框外，移除")
        row["lon"] = row["lat"] = None
        has_pt = False
    if prec in ("exact", "fuzzy", "road") and not has_pt:
        new = "district" if row.get("town_code") else "none"
        notes.append(f"{prec} 沒有點→{new}")
        prec = new
    elif prec in ("district", "none") and has_pt:
        notes.append(f"{prec} 卻有點→移除點")
        row["lon"] = row["lat"] = None
    row["geom_precision"] = prec
    if row.get("source") != "new_taipei" or not _valid_point(row.get("dest_lon"), row.get("dest_lat")):
        if row.get("dest_lon") is not None or row.get("dest_lat") is not None:
            notes.append("dest_geom 只給新北且須合法，移除")
        row["dest_lon"] = row["dest_lat"] = None
    if not isinstance(row.get("status_history"), list):
        row["status_history"] = []
    if row.get("last_seen_at") and row.get("first_seen_at") and row["last_seen_at"] < row["first_seen_at"]:
        row["last_seen_at"] = row["first_seen_at"]
    return row, notes


def content_hash(row: dict) -> str:
    """會觸發 upsert 的內容（不含狀態、分隊、時間戳）：類別、地點、座標、精度、醫院。"""
    keys = ("category_raw", "location_text", "lon", "lat", "geom_precision", "road_key", "town_code",
            "hospital", "dest_lon", "dest_lat", "received_at")
    return hashlib.sha1(json.dumps([row.get(k) for k in keys], ensure_ascii=False).encode()).hexdigest()[:12]


# ── 警廣 ──────────────────────────────────────────────────────────────
def build_pbs(r: dict, seen: datetime) -> dict:
    seen_iso = _iso(seen)
    ok = _valid_point(r.get("lon"), r.get("lat"))
    return dict(uid=r["uid"], roadtype=r.get("roadtype"), category=r.get("category") or "other",
                comment=r.get("comment"), area=r.get("area"), direction=r.get("direction"),
                happened_at=_iso(r.get("happened_at")),
                lon=r.get("lon") if ok else None, lat=r.get("lat") if ok else None,
                status=r.get("status") if r.get("status") in PBS_STATUSES else "reported",
                mod_dttm=_iso(r.get("mod_dttm")),
                status_history=[[seen_iso, _iso(r.get("mod_dttm"))]],
                first_seen_at=seen_iso, last_seen_at=seen_iso, updated_at=seen_iso)


# ── 本機差異狀態 ───────────────────────────────────────────────────────
class SeenState:
    """{key: [status_raw|mod, units, content_hash, last_seen_iso]}，JSON 檔。

    - diff()：回傳需要寫 DB 的列（新 key、或 status／分隊聯集／內容有變）。
    - commit()：只有在列已「落地」（DB commit 或 buffer 存檔成功）後才呼叫，否則下一輪會重送。
    - touch()：記錄本輪看到（給 last_seen 批次更新）。
    - 超過 prune_hours 沒看到的 key 刪掉（清單上的舊案每輪都會被看到，不會被誤刪）。
    """

    def __init__(self, path: Path, prune_hours: int = 12):
        self.path = Path(path)
        self.prune_hours = prune_hours
        try:
            self.data: dict[str, list] = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self.data = {}

    def changed(self, key: str, marker, units: list[str], chash: str) -> bool:
        cur = self.data.get(key)
        if cur is None:
            return True
        return cur[0] != marker or not set(units) <= set(cur[1]) or cur[2] != chash

    def commit(self, key: str, marker, units: list[str], chash: str, seen_iso: str) -> None:
        cur = self.data.get(key)
        merged = list(dict.fromkeys((cur[1] if cur else []) + list(units)))
        self.data[key] = [marker, merged, chash, seen_iso]

    def touch(self, key: str, seen_iso: str) -> None:
        if key in self.data:
            self.data[key][3] = seen_iso

    def seen_since(self, since_iso: str | None) -> list[dict]:
        return [{"k": k, "ts": v[3]} for k, v in self.data.items() if since_iso is None or v[3] > since_iso]

    def prune(self, now: datetime) -> int:
        cutoff = _iso(now - timedelta(hours=self.prune_hours))
        old = [k for k, v in self.data.items() if v[3] < cutoff]
        for k in old:
            del self.data[k]
        return len(old)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, self.path)


def dedupe(rows: list[dict], key: str) -> list[dict]:
    """同一批同 key 只留最後一筆（避免 ON CONFLICT DO UPDATE 的 cardinality violation）。"""
    out: dict[str, dict] = {}
    for r in rows:
        out[r[key]] = r
    return list(out.values())


# ── SQL ──────────────────────────────────────────────────────────────
INCIDENT_COLS = ["incident_key", "source", "county", "category", "category_raw", "status", "status_raw",
                 "status_history", "received_at", "location_text", "geom", "geom_precision", "road_key",
                 "town_code", "units", "hospital", "dest_geom", "first_seen_at", "last_seen_at", "updated_at"]

UPSERT_INCIDENTS_SQL = (
    f"INSERT INTO live.fire119_incidents AS t ({', '.join(INCIDENT_COLS)}) VALUES %s "
    "ON CONFLICT (incident_key) DO UPDATE SET "
    "category = EXCLUDED.category, category_raw = EXCLUDED.category_raw, "
    "status = EXCLUDED.status, status_raw = EXCLUDED.status_raw, "
    "status_history = CASE WHEN EXCLUDED.status_raw IS DISTINCT FROM t.status_raw "
    "THEN t.status_history || EXCLUDED.status_history ELSE t.status_history END, "
    "received_at = COALESCE(EXCLUDED.received_at, t.received_at), "
    "location_text = EXCLUDED.location_text, geom = EXCLUDED.geom, geom_precision = EXCLUDED.geom_precision, "
    "road_key = EXCLUDED.road_key, town_code = EXCLUDED.town_code, "
    "units = ARRAY(SELECT u FROM unnest(t.units || EXCLUDED.units) WITH ORDINALITY AS x(u, o) "
    "GROUP BY u ORDER BY min(o)), "
    "hospital = COALESCE(EXCLUDED.hospital, t.hospital), dest_geom = COALESCE(EXCLUDED.dest_geom, t.dest_geom), "
    "last_seen_at = GREATEST(t.last_seen_at, EXCLUDED.last_seen_at), updated_at = EXCLUDED.updated_at "
    "WHERE EXCLUDED.updated_at > t.updated_at"
)

PBS_COLS = ["uid", "roadtype", "category", "comment", "area", "direction", "happened_at", "geom", "status",
            "mod_dttm", "status_history", "first_seen_at", "last_seen_at", "updated_at"]

UPSERT_PBS_SQL = (
    f"INSERT INTO live.pbs_road_events AS t ({', '.join(PBS_COLS)}) VALUES %s "
    "ON CONFLICT (uid) DO UPDATE SET "
    "roadtype = EXCLUDED.roadtype, category = EXCLUDED.category, comment = EXCLUDED.comment, "
    "area = EXCLUDED.area, direction = EXCLUDED.direction, happened_at = EXCLUDED.happened_at, "
    "geom = EXCLUDED.geom, status = EXCLUDED.status, mod_dttm = EXCLUDED.mod_dttm, "
    "status_history = CASE WHEN EXCLUDED.mod_dttm IS DISTINCT FROM t.mod_dttm "
    "THEN t.status_history || EXCLUDED.status_history ELSE t.status_history END, "
    "last_seen_at = GREATEST(t.last_seen_at, EXCLUDED.last_seen_at), updated_at = EXCLUDED.updated_at "
    "WHERE EXCLUDED.updated_at > t.updated_at"
)

LAST_SEEN_SQL = {
    "fire119": ("UPDATE live.fire119_incidents AS t SET last_seen_at = v.ts "
                "FROM json_to_recordset(%s::json) AS v(k text, ts timestamptz) "
                "WHERE t.incident_key = v.k AND t.last_seen_at < v.ts"),
    "pbs": ("UPDATE live.pbs_road_events AS t SET last_seen_at = v.ts "
            "FROM json_to_recordset(%s::json) AS v(k text, ts timestamptz) "
            "WHERE t.uid = v.k AND t.last_seen_at < v.ts"),
}


def _ewkt(lon, lat) -> str | None:
    return f"SRID=4326;POINT({lon} {lat})" if lon is not None and lat is not None else None


def incident_tuple(r: dict) -> tuple:
    return (r["incident_key"], r["source"], r["county"], r["category"], r.get("category_raw"), r["status"],
            r.get("status_raw"), r["status_history"], r.get("received_at"), r.get("location_text"),
            _ewkt(r.get("lon"), r.get("lat")), r["geom_precision"], r.get("road_key"), r.get("town_code"),
            r.get("units") or [], r.get("hospital"), _ewkt(r.get("dest_lon"), r.get("dest_lat")),
            r["first_seen_at"], r["last_seen_at"], r["updated_at"])


def pbs_tuple(r: dict) -> tuple:
    return (r["uid"], r.get("roadtype"), r["category"], r.get("comment"), r.get("area"), r.get("direction"),
            r.get("happened_at"), _ewkt(r.get("lon"), r.get("lat")), r["status"], r.get("mod_dttm"),
            r["status_history"], r["first_seen_at"], r["last_seen_at"], r["updated_at"])


def write_payload(conn, payload: dict, execute_values) -> dict:
    """寫一包（本輪或 buffer 補寫）：{"fire119": [...], "pbs": [...]}。整包一個交易。"""
    inc = dedupe(payload.get("fire119") or [], "incident_key")
    pbs = dedupe(payload.get("pbs") or [], "uid")
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '30s'")
        if inc:
            execute_values(cur, UPSERT_INCIDENTS_SQL, [incident_tuple(r) for r in inc], page_size=500)
        if pbs:
            execute_values(cur, UPSERT_PBS_SQL, [pbs_tuple(r) for r in pbs], page_size=500)
    conn.commit()
    return {"fire119": len(inc), "pbs": len(pbs)}


def write_last_seen(conn, batches: dict, execute_json) -> dict:
    """{"fire119": [{"k","ts"}], "pbs": [...]} → 批次 UPDATE last_seen_at（只往後推）。"""
    out = {}
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '30s'")
        for kind, rows in batches.items():
            if rows:
                execute_json(cur, LAST_SEEN_SQL[kind], rows, page_size=1000)
            out[kind] = len(rows)
    conn.commit()
    return out
