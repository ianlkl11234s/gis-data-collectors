"""飛機航跡來源（aviation-src-<D>.json，schema pulse-sound-aviation-src-v1）。

每日檔：原樣搬自 mini-taiwan-pulse `scripts/preprocess/build-sound-scenes.py build_aviation`（S3 模式）＋
`build-sound-grid-sources.py build_aviation_src／make_segment_hit`。輸入是 S3 `trails/flights/<D>.json.gz`
（data-collectors scripts/export_daily_trails.py 每日 02:00 匯出 live.flight_trails_daily）。

即時：每次讀 live.flight_positions 一個時間窗（有界：collected_at 窗＋altitude ≥ 100，同 refresh_flight_trails_daily
的條件），航跡點字串格式與 flight_trails_daily 相同（lat,lng,alt,unix），所以分片合併後與每日檔的同一航班點逐字相同。
**注意**：飛機網格包 `aviation-<D>.class.bin` 由前端 TS kernel 算（scripts/preprocess/sound/build-sound-grid-packs.ts），
本管線不產生（見 DESIGN §2.6 待決）。
"""
from __future__ import annotations

import gzip
import json
from typing import Callable, Iterable, Optional

from .common import bbox_hit, day_window, expand_bbox, in_bbox, now_tpe_iso, shift

SCHEMA = "pulse-sound-aviation-src-v1"
AVIATION_PAD_KM = 23.0
FLIGHT_COLS = ("flight_id", "callsign", "aircraft_type", "origin", "destination", "trail")
# 前端 AVIATION.maxInterpolationGapS：段內兩點 > 600 s 不插值。即時分片為了補上「進入範圍那一段」，
# 新進範圍的航班要附上窗前最後一點（≤ 這個秒數內）。
LOOKBACK_S = 600


def trail_points(trail: str):
    for p in trail.split(";"):
        parts = p.split(",")
        if len(parts) < 4:
            continue
        yield float(parts[0]), float(parts[1]), float(parts[3])  # lat, lng, ts


def segment_hit(row: dict, bbox: list[float], window=None) -> bool:
    """航跡點落在 bbox，或相鄰兩點連線外框碰到 bbox（build-sound-grid-sources.py make_segment_hit）"""
    prev = None
    for lat, lng, ts in trail_points(row["trail"]):
        in_win = window is None or window[0] <= ts < window[1]
        if in_win and in_bbox(lng, lat, bbox):
            return True
        if prev is not None:
            plat, plng, pts = prev
            if (in_win or window is None or window[0] <= pts < window[1]) and bbox_hit(
                [min(plng, lng), min(plat, lat), max(plng, lng), max(plat, lat)], bbox
            ):
                return True
        prev = (lat, lng, ts)
    return False


def filter_bbox(grid_bbox: list[float]) -> list[float]:
    return expand_bbox(grid_bbox, AVIATION_PAD_KM)


def build_daily(date: str, grid_bbox: list[float], fetch: Callable[[str], Optional[list[dict]]], origin_fmt: str = "s3://<bucket>/trails/flights/{date}.json.gz",
                generated_at: Optional[str] = None) -> dict:
    """fetch(date) → 該日 trails 列（None＝讀不到）。同 build_aviation(sources.aviation=s3) ＋ build_aviation_src。"""
    fb = filter_bbox(grid_bbox)
    notes: list[str] = []
    origin = origin_fmt.format(date=date)

    def doc(origin_s: str, health: str, rows: list) -> dict:
        return {"schema": SCHEMA, "date_taiwan": date, "generated_at": generated_at or now_tpe_iso(), "origin": origin_s, "health": health,
                "grid_bbox": grid_bbox, "filter_bbox": fb,
                "filter_rule": f"北北網格外框外擴 {AVIATION_PAD_KM:g} km（候選半徑 20 km×1.15）；任一航跡點或相鄰兩點連線外框碰到即整條保留；D±1 檔只補 D 檔沒有、且在臺北日 D 內飛經的航班",
                "notes_zh": notes, "rows": rows}

    try:
        rows = fetch(date)
    except Exception as e:  # noqa: BLE001
        rows = None
        notes.append(f"航跡：{origin} 讀取失敗（{type(e).__name__}），飛機聲源整日 unknown。")
        return doc(origin, "error", [])
    if rows is None:
        notes.append(f"航跡：{origin} 讀取失敗，飛機聲源整日 unknown。")
        return doc(origin, "error", [])
    if not rows:
        notes.append(f"航跡：{origin} 回 0 筆（該日無資料或已過保留），飛機聲源整日 unknown。")
        return doc(origin, "empty", [])
    kept = {r["flight_id"]: {k: r.get(k) for k in FLIGHT_COLS} for r in rows if segment_hit(r, fb)}
    window = day_window(date)
    merged: list[str] = []
    neighbor_errors: list[str] = []
    all_ids = {r["flight_id"] for r in rows}
    for nd in (shift(date, -1), shift(date, 1)):
        try:
            nrows = fetch(nd)
        except Exception as e:  # noqa: BLE001
            nrows = None
            neighbor_errors.append(f"{nd}: {type(e).__name__}")
        if nrows is None:
            if not neighbor_errors or not neighbor_errors[-1].startswith(nd):
                neighbor_errors.append(f"{nd}: 檔案不存在（尚未匯出）")
            continue
        for r in nrows:
            if r["flight_id"] in all_ids or r["flight_id"] in kept:
                continue
            if segment_hit(r, fb, window):
                kept[r["flight_id"]] = {k: r.get(k) for k in FLIGHT_COLS}
                merged.append(f"{nd}:{r['flight_id']}")
    if merged:
        notes.append(f"航跡：從鄰日檔併入 {len(merged)} 個 D 檔缺漏、但在臺北日 {date} 飛經 bbox 的航班。")
    else:
        notes.append(f"航跡：已比對 {shift(date, -1)}／{shift(date, 1)} 鄰日檔，沒有 D 檔缺漏的跨午夜航班。")
    if neighbor_errors:
        notes.append("航跡：鄰日檔讀取失敗（只影響跨午夜補漏）：" + "；".join(neighbor_errors))
    out_rows = [kept[k] for k in sorted(kept)]
    notes.append(f"航跡：該日全台 {len(rows)} 個 flight_id，任一點落在 bbox 的 {len(out_rows)} 個（整條保留）。")
    return doc(origin, "ok", out_rows)


def trails_from_gz(raw: bytes) -> list[dict]:
    return json.loads(gzip.decompress(raw))


# ── 即時：位置點 → 航跡片段 ──────────────────────────────────────

POSITIONS_SQL = """
SELECT flight_id,
       callsign, aircraft_type, origin, destination,
       lat::text || ',' || lng::text || ',' || COALESCE(altitude, 0)::text || ',' || EXTRACT(EPOCH FROM collected_at)::bigint::text AS pt,
       EXTRACT(EPOCH FROM collected_at)::bigint AS ts,
       lat, lng
FROM live.flight_positions
WHERE collected_at >= to_timestamp(%(t0)s) AND collected_at < to_timestamp(%(t1)s)
  AND altitude IS NOT NULL AND altitude >= 100
ORDER BY collected_at, flight_id
LIMIT %(limit)s
"""
# 點字串的寫法與 refresh_flight_trails_daily（gis-platform migration 312）相同，所以片段點＝每日航跡點（逐字）。
POSITIONS_LIMIT = 20_000  # 一個 10 分窗全台約 1–3 千點；超過＝異常，截斷並標 truncated


class AviationLive:
    """即時航跡片段：每次給一個時間窗的位置點（依 collected_at），回本窗要發布的片段。

    - 本窗有點的航班，用「上一個已知點＋本窗各點」判斷是否碰到外擴 23 km 外框（同 segment_hit）；
    - 碰到 → 輸出本窗各點；若這是該航班今天第一次輸出，再附上窗前最後一點（≤ LOOKBACK_S 秒），補上進入範圍的那一段；
    - 片段欄位：flight_id、callsign 等取本窗非空值的最大（合併時再取最大＝flight_trails_daily 的 MAX()）、trail＝點字串。
    """

    def __init__(self, date: str, grid_bbox: list[float]):
        self.date = date
        self.fb = filter_bbox(grid_bbox)
        self.last_pt: dict[str, tuple[float, str, float, float]] = {}   # flight_id → (ts, pt, lat, lng)
        self.emitted: set[str] = set()

    def window(self, rows: Iterable[dict]) -> list[dict]:
        by: dict[str, list[dict]] = {}
        for r in rows:
            by.setdefault(r["flight_id"], []).append(r)
        out = []
        for fid, pts in sorted(by.items()):
            pts.sort(key=lambda r: r["ts"])
            prev = self.last_pt.get(fid)
            chain = ([prev[1]] if prev and pts[0]["ts"] - prev[0] <= LOOKBACK_S else []) + [p["pt"] for p in pts]
            hit = segment_hit({"trail": ";".join(chain)}, self.fb)
            if hit:
                trail_pts = [p["pt"] for p in pts]
                if fid not in self.emitted and prev and pts[0]["ts"] - prev[0] <= LOOKBACK_S:
                    trail_pts = [prev[1]] + trail_pts
                self.emitted.add(fid)
                frag = {"flight_id": fid}
                for c in ("callsign", "aircraft_type", "origin", "destination"):
                    vals = [p.get(c) for p in pts if p.get(c) is not None]
                    frag[c] = max(vals) if vals else None
                frag["trail"] = ";".join(trail_pts)
                out.append(frag)
            last = pts[-1]
            self.last_pt[fid] = (last["ts"], last["pt"], last.get("lat"), last.get("lng"))
        # 久未出現的航班不再當「上一點」
        newest = max((p[0] for p in self.last_pt.values()), default=0)
        for fid in [f for f, p in self.last_pt.items() if newest - p[0] > 2 * LOOKBACK_S]:
            del self.last_pt[fid]
        return out


def merge_fragments(frags: Iterable[dict]) -> list[dict]:
    """前端合併規則（參考實作）：同 flight_id 的點依時間排序、同字串去重；文字欄取非空最大值，沒有＝''（同 COALESCE(MAX(x),'')）"""
    by: dict[str, dict] = {}
    for f in frags:
        cur = by.setdefault(f["flight_id"], {"flight_id": f["flight_id"], "_pts": {}, **{c: None for c in ("callsign", "aircraft_type", "origin", "destination")}})
        for c in ("callsign", "aircraft_type", "origin", "destination"):
            v = f.get(c)
            if v is not None and (cur[c] is None or v > cur[c]):
                cur[c] = v
        for p in f["trail"].split(";"):
            if p:
                cur["_pts"][p] = float(p.split(",")[3])
    out = []
    for fid in sorted(by):
        cur = by[fid]
        pts = sorted(cur.pop("_pts").items(), key=lambda kv: kv[1])
        row = {"flight_id": fid, **{c: cur[c] if cur[c] is not None else "" for c in ("callsign", "aircraft_type", "origin", "destination")},
               "trail": ";".join(p for p, _ in pts)}
        out.append(row)
    return out
