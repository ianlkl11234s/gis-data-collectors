"""自然三族來源：風（weather）、浪（cwa_marine_observation）、河川水位（river_water_level）。

規則原樣搬自 mini-taiwan-pulse `scripts/preprocess/build-sound-scenes.py` 的 build_wind／build_wave／build_river
（W5，pulse-sound-natural-v1），每日檔外框搬自 `build-sound-grid-sources.py build_natural_full`。
輸入是 collector 原始檔（本機目錄或 S3 archive，內容相同）。

為了讓即時分片與每日檔共用同一份邏輯，風與河川寫成「累加器」：依抵達順序 feed 原始檔，
每日檔＝全部 feed 完的結果；即時分片＝每次 feed 之後新增或改變的部分（`drain_delta`）。
"""
from __future__ import annotations

import io
import json
import math
from typing import Callable, Iterable, Optional

from .common import RawSource, day_window, expand_bbox, in_bbox, now_tpe_iso, num, shift, unix_of, contract_json

NATURAL_SCHEMA = "pulse-sound-natural-v1"
GRID_DOC_SCHEMA = "pulse-sound-natural-grid-v1"
WIND_STATION_PAD_KM = 8.0
RIVER_STATION_PAD_KM = 5.0
RIVER_HISTORY_DAYS = 60
RIVER_QUANT_PROBS = [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
WAVE_METRICS = ("wave_height", "wave_period")


# ── 風 ──────────────────────────────────────────────────────────

class WindAccumulator:
    """build_wind 的逐檔版：站序＝第一次出現順序；同 (站, 整點) 後到的覆寫先到的（同原本 dict 指派）。"""

    def __init__(self, date: str, grid_bbox: list[float]):
        self.start, self.end = day_window(date)
        self.sb = expand_bbox(grid_bbox, WIND_STATION_PAD_KM)
        self.stations: dict[str, int] = {}
        self.st_list: list[dict] = []
        self.obs: dict[tuple[int, int], list] = {}
        self.total = 0
        self._emitted_st = 0
        self._emitted_obs: dict[tuple[int, int], list] = {}

    def feed(self, doc: dict) -> None:
        for r in doc.get("data", []) or []:
            lon, lat = num(r.get("longitude")), num(r.get("latitude"))
            if lon is None or lat is None or not r.get("obs_time"):
                continue
            t = unix_of(r["obs_time"])
            if not (self.start <= t < self.end):
                continue
            self.total += 1
            if not in_bbox(lon, lat, self.sb):
                continue
            sid = r["station_id"]
            if sid not in self.stations:
                self.stations[sid] = len(self.st_list)
                self.st_list.append({"id": sid, "name": r.get("station_name"), "lon": lon, "lat": lat, "alt_m": num(r.get("altitude"))})
            ws = num(r.get("wind_speed"))
            gust = num(r.get("gust_speed"))
            i = self.stations[sid]
            self.obs[(i, t)] = [i, t, ws if ws is None or ws >= 0 else None, gust if gust is None or gust >= 0 else None]

    def block(self, origin: str) -> dict:
        return {"origin": origin, "health": "ok" if self.total else "empty", "bbox": None, "stations": self.st_list,
                "obs": sorted(self.obs.values(), key=lambda x: (x[0], x[1]))}

    def drain_delta(self) -> dict:
        """上次 drain 之後新增的站與新增／改變的觀測（站用 id 指，不用索引，分片可獨立合併）"""
        new_st = self.st_list[self._emitted_st:]
        self._emitted_st = len(self.st_list)
        changed = []
        for k, v in self.obs.items():
            if self._emitted_obs.get(k) != v:
                changed.append([self.st_list[v[0]]["id"], v[1], v[2], v[3]])
                self._emitted_obs[k] = list(v)
        changed.sort(key=lambda x: (x[0], x[1]))
        return {"stations": new_st, "obs": changed}


def build_wind(date: str, grid_bbox: list[float], files: Optional[Iterable], origin: str, notes: list[str]) -> dict:
    if files is None:
        notes.append(f"風：{origin} 缺檔 → health=error（前端 unknown）。")
        return {"origin": origin, "health": "error", "bbox": grid_bbox, "stations": [], "obs": []}
    acc = WindAccumulator(date, grid_bbox)
    for f in files:
        acc.feed(f.json())
    b = acc.block(origin)
    b["bbox"] = grid_bbox
    notes.append(f"風：{origin}，全台 {acc.total} 筆整點觀測；收 bbox 外擴 {WIND_STATION_PAD_KM:g} km 內 {len(acc.st_list)} 站。風速≠聲級。")
    return b


# ── 浪 ──────────────────────────────────────────────────────────

def marine_items(raw: bytes) -> Iterable[dict]:
    """cwa_marine_observation 原始檔（約 27 MB）的 data 陣列逐項產出；有 ijson 就串流，不整份載入。"""
    try:
        import ijson  # requirements.txt 已有（yajl2_c）
    except ImportError:  # pragma: no cover - 本機沒裝時退回整份解析
        yield from json.loads(raw).get("data", [])
        return
    yield from ijson.items(io.BytesIO(raw), "data.item", use_float=True)


def wave_values(items: Iterable[dict], date: str) -> tuple[dict[str, dict], dict[tuple[str, int], dict]]:
    """(站 meta, {(站, t): {metric: 值}})；同 build_wave（後出現的覆寫）"""
    start, end = day_window(date)
    st_meta: dict[str, dict] = {}
    vals: dict[tuple[str, int], dict] = {}
    for r in items:
        typ = r.get("_type")
        if typ == "station":
            st_meta[r["source_station_id"]] = r
            continue
        if typ != "reading" or r.get("metric_code") not in WAVE_METRICS:
            continue
        t = unix_of(r["observed_at"])
        if not (start <= t < end):
            continue
        v = num(r.get("value_numeric"))
        if r.get("is_missing") or r.get("is_valid") is False or (v is not None and v < 0):
            v = None
        vals.setdefault((r["source_station_id"], t), {})[r["metric_code"]] = v
    return st_meta, vals


def wave_station(meta: dict, sid: str) -> dict:
    return {"id": sid, "name": meta.get("name_zh"), "type": meta.get("station_type"), "lon": meta["longitude"], "lat": meta["latitude"]}


def build_wave_block(st_meta: dict, vals: dict, origin: str) -> dict:
    idx: dict[str, int] = {}
    st_list: list[dict] = []
    obs = []
    for (sid, t), m in sorted(vals.items()):
        meta = st_meta.get(sid)
        if not meta:
            continue
        if sid not in idx:
            idx[sid] = len(st_list)
            st_list.append(wave_station(meta, sid))
        obs.append([idx[sid], t, m.get("wave_height"), m.get("wave_period")])
    return {"origin": origin, "health": "ok" if obs else "empty", "bbox": None, "stations": st_list, "obs": obs}


def build_wave(date: str, grid_bbox: list[float], raw: Optional[bytes], origin: str, notes: list[str]) -> dict:
    """raw＝D+1 第一個 cwa_marine_observation 原始檔（滾動 48h，涵蓋 D 全天）"""
    if raw is None:
        notes.append(f"浪：{origin} 讀取失敗 → health=error。")
        return {"origin": origin, "health": "error", "bbox": grid_bbox, "stations": [], "obs": []}
    st_meta, vals = wave_values(marine_items(raw), date)
    b = build_wave_block(st_meta, vals, origin)
    b["bbox"] = grid_bbox
    notes.append(f"浪：{origin}（CWA O-B0075 滾動 48h，取 D+1 第一個檔即涵蓋 D 全天），{len(b['stations'])} 個有浪高的浮標／測站全收（共 {len(b['obs'])} 小時列）；浪高或週期缺值保持 null。")
    return b


# ── 河川水位 ────────────────────────────────────────────────────

RiverTuple = tuple  # (station_id, observed unix, water_level_m|None)


def river_rows(files: Optional[Iterable]) -> Optional[list[RiverTuple]]:
    """原始檔 → 依抵達順序的 (站, observed unix, 水位) 精簡列（沒有 observed_at 的列略過，同 _hourly_last 的第一個判斷）。

    不留原始 dict：60 天 × 每天約 5 萬列，整份 dict 留在記憶體會到 GB 級。
    """
    if files is None:
        return None
    out: list[RiverTuple] = []
    for f in files:
        for r in f.json().get("data", []) or []:
            if not r.get("observed_at"):
                continue
            out.append((r["station_id"], unix_of(r["observed_at"]), num(r.get("water_level_m"))))
    return out


def hourly_last(rows: Iterable[RiverTuple], start: int, end: int) -> dict[tuple[str, int], Optional[float]]:
    """同 build-sound-scenes.py _hourly_last：每站每小時取 observed_at 最晚；同時刻先到的為準"""
    best: dict[tuple[str, int], tuple[int, Optional[float]]] = {}
    for sid, t, v in rows:
        if not (start <= t < end):
            continue
        k = (sid, t - (t - start) % 3600)
        if k not in best or best[k][0] < t:
            best[k] = (t, v)
    return {k: v for k, (_, v) in best.items()}


def quantiles(values: list[float]) -> Optional[dict]:
    if len(values) < 2:
        return None
    v = sorted(values)
    out = []
    for p in RIVER_QUANT_PROBS:
        x = p * (len(v) - 1)
        lo = int(math.floor(x))
        hi = min(lo + 1, len(v) - 1)
        out.append(round(v[lo] + (v[hi] - v[lo]) * (x - lo), 4))
    return {"probs": RIVER_QUANT_PROBS, "values": out, "n": len(v)}


def river_coords() -> dict[str, tuple[list[float], Optional[str]]]:
    return {sid: ([v[0], v[1]], v[2]) for sid, v in contract_json("river_stations.json").items()}


class RiverRowCache:
    """河川精簡列的小快取：分位數依序讀前 60 天，每一天會被「當天」與「前一天的隔日」各要一次，留最近 keep 天即可。"""

    def __init__(self, src: RawSource, keep: int = 4):
        self.src = src
        self.keep = keep
        self._c: dict[str, Optional[list[RiverTuple]]] = {}

    def get(self, date: str) -> Optional[list[RiverTuple]]:
        if date not in self._c:
            self._c[date] = river_rows(self.src.files("river_water_level", date))
            while len(self._c) > self.keep:
                del self._c[next(iter(self._c))]
        return self._c[date]


def river_baseline(date: str, grid_bbox: list[float], rows_of: Callable[[str], Optional[list[RiverTuple]]],
                   station_ids: Optional[Iterable[str]] = None) -> dict:
    """D 之前 60 天（不含 D）每小時最後一筆的分位數。同 build_river 的 hist 段（含 k=1 不讀 D 的行為）。

    station_ids 給了就只算這些站（每日檔＝當日有觀測的站）；沒給＝外擴 5 km 內所有有座標的站（即時段的每日基準檔）。
    """
    coords = river_coords()
    sb = expand_bbox(grid_bbox, RIVER_STATION_PAD_KM)
    ids = sorted(station_ids) if station_ids is not None else sorted(i for i, (c, _n) in coords.items() if in_bbox(c[0], c[1], sb))
    hist: dict[str, list[float]] = {i: [] for i in ids}
    hist_days = 0
    for k in range(RIVER_HISTORY_DAYS, 0, -1):
        d = shift(date, -k)
        rows = rows_of(d)
        nxt = rows_of(shift(d, 1)) if k > 1 else None
        if rows is None:
            continue
        hist_days += 1
        s0, e0 = day_window(d)
        hv = hourly_last(rows + (nxt or []), s0, e0)
        for (sid, _), v in hv.items():
            if sid in hist and v is not None:
                hist[sid].append(v)
    basis = f"{shift(date, -RIVER_HISTORY_DAYS)}..{shift(date, -1)}（{hist_days}/{RIVER_HISTORY_DAYS} 天有 archive），每小時最後一筆"
    return {"basis": basis, "hist_days": hist_days, "q": {sid: quantiles(hist[sid]) for sid in ids}}


def river_today(date: str, grid_bbox: list[float], rows: list[RiverTuple]) -> tuple[dict, list[str], set[str]]:
    """(today {(站, 整點): 值}, 站清單（外擴 5 km 內、有座標）, 全部有觀測的站)"""
    coords = river_coords()
    sb = expand_bbox(grid_bbox, RIVER_STATION_PAD_KM)
    start, end = day_window(date)
    today = hourly_last(rows, start, end)
    ids_all = {sid for sid, _ in today}
    ids = sorted(i for i in ids_all if i in coords and in_bbox(coords[i][0][0], coords[i][0][1], sb))
    return today, ids, ids_all


def build_river(date: str, grid_bbox: list[float], cache: RiverRowCache, origin: str, notes: list[str]) -> dict:
    day_rows = []
    missing = []
    for d in (date, shift(date, 1)):
        rows = cache.get(d)
        if rows is None:
            missing.append(d)
        else:
            day_rows.extend(rows)
    if date in missing:
        notes.append(f"河川：{date} 水位 archive 缺檔 → health=error。")
        return {"origin": origin, "health": "error", "bbox": grid_bbox, "stations": [], "obs": []}
    coords = river_coords()
    today, ids, ids_all = river_today(date, grid_bbox, day_rows)
    unplaced = sorted(i for i in ids_all if i not in coords)
    base = river_baseline(date, grid_bbox, cache.get, ids)
    idx = {sid: n for n, sid in enumerate(ids)}
    st_list = [{"id": sid, "name": coords[sid][1], "lon": coords[sid][0][0], "lat": coords[sid][0][1], "q": base["q"][sid]} for sid in ids]
    obs = sorted([[idx[sid], t, v] for (sid, t), v in today.items() if sid in idx], key=lambda x: (x[0], x[1]))
    notes.append(
        f"河川：{origin}；收 bbox 外擴 {RIVER_STATION_PAD_KM:g} km 內 {len(ids)} 站（全台 {len(ids_all)} 站中 {len(unplaced)} 站在 water_monitor_stations 找不到座標、不放）。"
        f"分位基準 {base['basis']}。水位≠流量≠聲音；感潮河段反映潮汐。"
    )
    return {"origin": origin, "health": "ok" if obs else "empty", "bbox": grid_bbox, "history_basis": base["basis"], "stations": st_list, "obs": obs}


# ── 每日 natural 檔 ─────────────────────────────────────────────

def first_marine_raw(src: RawSource, date: str) -> Optional[bytes]:
    files = src.files("cwa_marine_observation", date)
    if files is None:
        return None
    for f in files:
        return f.loader()
    return None


def build_daily(date: str, grid_bbox: list[float], src: RawSource, static: Optional[dict], river_cache: Optional[RiverRowCache] = None,
                generated_at: Optional[str] = None) -> dict:
    notes: list[str] = []
    nat: dict = {"schema": NATURAL_SCHEMA}
    nat["wind"] = build_wind(date, grid_bbox, src.files("weather", date), f"weather/archives/{date}", notes)
    nat["wave"] = build_wave(date, grid_bbox, first_marine_raw(src, shift(date, 1)), f"cwa_marine_observation/archives/{shift(date, 1)}#first-member", notes)
    nat["river"] = build_river(date, grid_bbox, river_cache or RiverRowCache(src), f"river_water_level/archives/{{{date},{shift(date, 1)}}}", notes)
    if static and static.get("coast"):
        nat["coast"] = static["coast"]
    if static and static.get("river_network"):
        nat["river_network"] = static["river_network"]
    else:
        notes.append("河道：靜態契約 natural-static-v1 不在 → 不放 river_network（河川整片無法判斷）。")
    return {"schema": GRID_DOC_SCHEMA, "date_taiwan": date, "generated_at": generated_at or now_tpe_iso(), "grid_bbox": grid_bbox,
            "notes_zh": notes, "natural": nat}
