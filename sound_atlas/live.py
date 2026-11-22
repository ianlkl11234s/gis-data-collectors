"""即時段：每 10 分一片不可變的分片（slot shard）＋每小時合併片（hour rollup）。

分片內容＝「這次執行新到的資料」（依抵達切，不依觀測時間切），所以晚到的資料自然落在後面的分片；
前端把當天已發布的分片全部合併（合併規則見 `merge_day`，前端照這份參考實作寫 TS）。
每一族都附 coverage，前端只把 coverage 內的時間當「已知」，缺片＝無法判斷，不可當 0。

各族的「即時看不到未來資料」處理（逐條見 DESIGN §3.3）：
  雷達   幀 k 雜波看不到 k+1 → 前一幀條件成立就先標 255（`radar.live_clutter_flag`）
  VD     整日全零、瞬斷看不到之後的列 → 先給 None（`freeway_vd.clean_vd_live`）
  雨量站 每站每整點取「到目前為止」最晚一筆（合併時取最晚＝每日 RPC 規則）
  風     同 (站, 整點) 後到覆寫先到 → 分片送新增或改變的值，合併時後片覆寫前片
  浪     滾動 48h 檔會回修 → 每小時解析最新檔，送新增或改變的值，合併時後片覆寫前片
  河川   每站每小時取最晚；同時刻先到為準 → 送第一次看到的讀數，合併時同規則
  飛機   航跡片段（位置點＝flight_trails_daily 的點字串）；合併時依時間排序去重
"""
from __future__ import annotations

import base64
import copy
import gzip
import pickle
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

import numpy as np

from . import aviation, freeway_vd, natural, radar, rain_gauge
from .common import (GRID, GRID_CELLS, GRID_SHA256, SLOT_S, TPE, UNKNOWN, RawFile, RawSource, contract_bytes, day_window,
                     grid_bbox, now_tpe_iso, shift, slot_of, slot_start_utc)

SLOT_SCHEMA = "pulse-sound-live-slot-v1"
HOUR_SCHEMA = "pulse-sound-live-hour-v1"
BASELINE_SCHEMA = "pulse-sound-river-baseline-v1"
RADAR_MISSING_AFTER_S = 3600     # 槽起點後這麼久還抓不到 → 宣告缺幀（之後才到也不補，每日檔為準）
WAVE_PARSE_EVERY_S = 3600        # 浪是逐時資料；海象原始檔約 27 MB，每小時最多解析一次
AVIATION_INSERT_LAG_S = 120      # flight_positions 寫入延遲（buffer）緩衝：只讀到 now − lag
RADAR_MAX_FRAMES_PER_RUN = 18     # 冷啟動補幀上限（每次最多 3 小時份，舊的先補；其餘下一次）
RADAR_KEEP_CODES = RADAR_MISSING_AFTER_S // 600 + 2  # 幀可能晚到 1 小時：前一幀的碼至少留這麼多槽
AVIATION_MAX_WINDOW_S = 3600     # 冷啟動補讀上限：每次最多讀 1 小時的位置點
PARSE_RETRIES = 3                # 原始檔解析失敗（collector 寫到一半）先不標已讀，連續失敗這麼多次才放棄並回報
STATE_VERSION = 1
TAIL_SLOTS = 6                   # 收尾片：D+1 00:00–01:00 繼續替 D 跑（slot 144–149），補 D 末尾才到的雷達幀、雨量、河川、浪、航跡


def _b64gz(b: bytes) -> str:
    return base64.b64encode(gzip.compress(b, compresslevel=9, mtime=0)).decode("ascii")


def decode_b64gz(s: str) -> bytes:
    return gzip.decompress(base64.b64decode(s))


def vd_ids_sha() -> str:
    import hashlib
    return hashlib.sha256(contract_bytes("freeway_vd_ids.json")).hexdigest()


@dataclass
class LiveInputs:
    """即時段的輸入（正式＝本機 /data 原始檔＋公開雷達網址＋DB 位置點；測試＝本機原始匯出模擬）"""
    raw: RawSource
    radar_fetcher: radar.FrameFetcher
    # (t0, t1) unix → 位置點列 [{flight_id, callsign, ..., pt, ts, lat, lng}]；None＝讀取失敗
    positions: Optional[Callable[[int, int], Optional[list[dict]]]] = None
    # 列出某 collector 某日「抵達時間 ≤ now」的原始檔（模擬用；正式＝目錄裡現有的檔）
    visible: Callable[[RawFile, float], bool] = lambda f, now: True


@dataclass
class LiveDay:
    """一個臺灣日的即時狀態（每次執行後 pickle 到本機 state 目錄；重啟後接續）"""
    date: str
    bbox: list[float] = field(default_factory=grid_bbox)
    seen_files: dict[str, set] = field(default_factory=dict)
    rain_seen: set = field(default_factory=set)
    vd_rows: dict[str, list] = field(default_factory=dict)
    vd_slots_done: set = field(default_factory=set)
    radar_done: set = field(default_factory=set)          # 已發布的幀
    radar_missing: set = field(default_factory=set)       # 已宣告缺的幀
    radar_codes: dict[int, bytes] = field(default_factory=dict)  # 幀 k 的 dBZ 碼（給 k+1 判雜波；只留最近幾幀）
    wind: Optional[natural.WindAccumulator] = None
    wave_emitted: dict = field(default_factory=dict)
    wave_parsed_at: float = 0.0
    wave_last_file: str = ""
    river_seen: set = field(default_factory=set)
    river_ids: Optional[set] = None
    aviation: Optional[aviation.AviationLive] = None
    aviation_watermark: int = 0
    aviation_after: set = field(default_factory=set)      # 已補過跨午夜終點的航班
    hours_published: set = field(default_factory=set)
    slots_published: set = field(default_factory=set)
    baseline_published: bool = False
    parse_fail: dict = field(default_factory=dict)        # 檔案 key → 解析失敗次數
    bad_files: list = field(default_factory=list)         # 放棄的檔（回報用）
    version: int = STATE_VERSION

    def __post_init__(self):
        if self.wind is None:
            self.wind = natural.WindAccumulator(self.date, self.bbox)
        if self.aviation is None:
            self.aviation = aviation.AviationLive(self.date, self.bbox)
        if not self.aviation_watermark:
            self.aviation_watermark = day_window(self.date)[0]

    # ── 持久化 ──
    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".part")
        tmp.write_bytes(pickle.dumps(self, protocol=pickle.HIGHEST_PROTOCOL))
        tmp.replace(path)

    @staticmethod
    def load(path: Path) -> Optional["LiveDay"]:
        """讀不到、壞檔或版本不符 → None（呼叫端從頭建；冷啟動補讀有上限，見各族）"""
        if not path.exists():
            return None
        try:
            day = pickle.loads(path.read_bytes())
        except Exception:  # noqa: BLE001 — 部署改了類別、寫到一半
            return None
        if not isinstance(day, LiveDay) or getattr(day, "version", None) != STATE_VERSION:
            return None
        return day

    # ── 共用：新到的原始檔 ──
    def _dates(self, now: float) -> list[str]:
        """D；過了午夜（收尾片）才加 D+1。之前不列 D+1，免得每次都去 archive 來源找一個還不存在的日期。"""
        return [self.date, shift(self.date, 1)] if now >= day_window(self.date)[1] else [self.date]

    def _new_files(self, inputs: LiveInputs, collector: str, now: float, dates: Iterable[str]) -> Optional[list[RawFile]]:
        """還沒處理過的原始檔（不標已讀；由 _parsed 在處理完後才標）"""
        seen = self.seen_files.setdefault(collector, set())
        out: list[RawFile] = []
        found = False
        for d in dates:
            files = inputs.raw.files(collector, d)
            if files is None:
                continue
            found = True
            for f in files:
                if f"{d}/{f.name}" in seen or not inputs.visible(f, now):
                    continue
                out.append(f)
        return out if found else None

    def _parsed(self, collector: str, files: list[RawFile]):
        """逐檔解析並交給呼叫端處理；呼叫端處理完（取下一檔）才標已讀。

        collector 寫檔不是原子操作，:x9 讀到寫一半的檔 → 解析失敗先跳過、下次再讀；
        連續 PARSE_RETRIES 次才放棄並記在 bad_files。呼叫端處理到一半丟例外 → 該檔不標已讀，下次重做
        （各族的合併規則冪等，重做不會重複計）。
        """
        seen = self.seen_files.setdefault(collector, set())
        for f in files:
            key = f"{f.date}/{f.name}"
            try:
                doc = f.json()
            except Exception:  # noqa: BLE001
                n = self.parse_fail.get(f"{collector}/{key}", 0) + 1
                self.parse_fail[f"{collector}/{key}"] = n
                if n >= PARSE_RETRIES:
                    seen.add(key)
                    self.bad_files.append(f"{collector}/{key}")
                continue
            yield doc
            seen.add(key)

    # ── 各族 ──
    # 各族會在執行中改的狀態欄位（run_slot 失敗還原用）；seen_files 以 collector 名取子集
    _FAMILY_STATE = {
        "radar": (("radar_done", "radar_missing", "radar_codes"), ()),
        "freeway_vd": (("vd_rows", "vd_slots_done"), ("freeway_vd",)),
        "rain_gauge": (("rain_seen",), ("rain_gauge_realtime",)),
        "wind": (("wind",), ("weather",)),
        "wave": (("wave_emitted", "wave_parsed_at", "wave_last_file"), ()),
        "river": (("river_seen",), ("river_water_level",)),
        "aviation": (("aviation", "aviation_watermark", "aviation_after"), ()),
    }

    def snapshot_family(self, family: str) -> dict:
        attrs, collectors = self._FAMILY_STATE[family]
        return {"attrs": {a: copy.deepcopy(getattr(self, a)) for a in attrs},
                "seen": {c: set(self.seen_files.get(c, set())) for c in collectors}}

    def restore_family(self, snap: dict) -> None:
        """族執行丟例外時還原：輸出整族丟棄，狀態就不能先前進（否則該段資料永久漏掉）。"""
        for a, v in snap["attrs"].items():
            setattr(self, a, v)
        for c, v in snap["seen"].items():
            self.seen_files[c] = v

    def run_radar(self, inputs: LiveInputs, now: float, decoder: radar.FrameDecoder) -> dict:
        k_now = min(slot_of(self.date, now), 143)
        frames_out, missing_out = [], []
        pending = [k for k in range(0, k_now + 1) if k not in self.radar_done and k not in self.radar_missing]
        errors = []
        decode_errors: list[int] = []  # 解碼失敗的幀：即使其他幀成功也要讓監控看得到
        fetched = 0
        for k in pending:
            t = slot_start_utc(self.date, k)
            if fetched >= RADAR_MAX_FRAMES_PER_RUN:
                break  # 冷啟動：其餘下一次補（不宣告缺）
            st, body = inputs.radar_fetcher.get(radar.frame_key(t))
            if st == "fetch_error":
                errors.append(k)
                break  # 影像主機連不上：不逐幀重試拖長這次執行；下一次再來（1 小時後才宣告缺）
            if st == "ok":
                fetched += 1
            if st != "ok":
                if now - t.timestamp() >= RADAR_MISSING_AFTER_S:
                    self.radar_missing.add(k)
                    missing_out.append(k)
                continue
            try:
                codes, bad, _seen = decoder.decode(body)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001 — 單幀解碼失敗：不動狀態、留待下次，不拖累已處理的前幀
                errors.append(k)
                decode_errors.append(k)
                continue
            if bad:
                # 表外顏色：每日包整日報錯；即時這一幀當缺（不硬配）
                self.radar_missing.add(k)
                missing_out.append(k)
                continue
            cls = radar.codes_to_class(codes)
            prev = self.radar_codes.get(k - 1)
            # 前一幀還沒到也還沒宣告缺，或已發布但碼不在了（重啟／清掉）→ 當未知
            prev_state_unknown = k > 0 and (k - 1) not in self.radar_missing and (prev is None)
            if prev_state_unknown:
                # 前一幀還沒到也還沒宣告缺：每日包可能會判 → 強回波先標 255
                flag = (codes != UNKNOWN) & (codes != 0) & (codes.astype(float) - 1 >= radar.CLUTTER_MIN_DBZ) if 0 < k < 143 else np.zeros(GRID_CELLS, bool)
            else:
                prev_arr = np.frombuffer(prev, dtype=np.uint8) if prev is not None else None
                flag = radar.live_clutter_flag(codes, prev_arr, k)
            out = cls.copy()
            out[flag] = UNKNOWN
            self.radar_done.add(k)
            self.radar_codes[k] = codes.tobytes()
            frames_out.append({"k": k, "utc": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "class_b64gz": _b64gz(out.tobytes()),
                               "clutter_live_cells": int(flag.sum())})
        for old in [x for x in self.radar_codes if x < k_now - RADAR_KEEP_CODES]:
            del self.radar_codes[old]
        if errors and not frames_out:
            status = "error"
        elif decode_errors:
            status = "partial"  # 有幀解碼失敗：保留成功的幀，但不算恢復（_family_alerts 視同 error 累計）
        else:
            status = "ok" if frames_out or missing_out else "no_new_data"
        doc = {"status": status, "encoding": "gzip+base64 uint8[cells]", "frames": frames_out, "missing": missing_out}
        if decode_errors:
            doc["decode_errors"] = decode_errors
        return doc

    def run_vd(self, inputs: LiveInputs, now: float) -> dict:
        files = self._new_files(inputs, "freeway_vd", now, [self.date])
        if files is None:
            return {"status": "no_new_data", "slots": {}}
        ids = freeway_vd.used_vd_ids()
        wanted = set(ids)
        new_slots: set[int] = set()
        fetch: dict[int, float] = {}
        for doc in self._parsed("freeway_vd", files):
            slot, t, rows = freeway_vd.rows_from_doc(doc, self.date, wanted)
            if slot is None:
                continue
            new_slots.add(slot)
            fetch[slot] = min(t, fetch.get(slot, t))  # type: ignore[type-var]
            for vid, r in rows.items():
                self.vd_rows.setdefault(vid, []).append(r)
        slots_out: dict[str, dict] = {}
        reasons: dict[str, int] = {}
        for s in sorted(new_slots):
            vals = []
            for vid in ids:
                v, why = freeway_vd.clean_vd_live(self.vd_rows.get(vid, []), [s])[s]
                vals.append(v)
                if why:
                    reasons[why] = reasons.get(why, 0) + 1
            slots_out[str(s)] = {"fetch_unix": int(fetch[s]), "v": vals}
            self.vd_slots_done.add(s)
        return {"status": "ok" if slots_out else "no_new_data", "vd_ids_sha256": vd_ids_sha(), "slots": slots_out, "null_reasons": reasons}

    def run_rain(self, inputs: LiveInputs, now: float) -> dict:
        files = self._new_files(inputs, "rain_gauge_realtime", now, self._dates(now))
        if files is None:
            return {"status": "no_new_data", "rows": []}
        sb = rain_gauge.station_bbox(self.bbox)
        rows = rain_gauge.first_seen_readings(self._parsed("rain_gauge_realtime", files), self.date, sb, self.rain_seen)
        return {"status": "ok" if files else "no_new_data", "files": len(files), "rows": rows}

    def run_wind(self, inputs: LiveInputs, now: float) -> dict:
        files = self._new_files(inputs, "weather", now, [self.date])
        if not files:
            return {"status": "no_new_data", "stations": [], "obs": []}
        for doc in self._parsed("weather", files):
            self.wind.feed(doc)  # type: ignore[union-attr]
        return {"status": "ok", "files": len(files), **self.wind.drain_delta()}  # type: ignore[union-attr]

    def run_wave(self, inputs: LiveInputs, now: float) -> dict:
        if now - self.wave_parsed_at < WAVE_PARSE_EVERY_S:
            return {"status": "no_new_data", "stations": [], "obs": []}
        latest = None
        for d in self._dates(now):  # 滾動 48h 檔：D+1 的檔也含 D 的值（每日檔取 D+1 第一檔）
            files = inputs.raw.files("cwa_marine_observation", d)
            for f in files or []:
                if inputs.visible(f, now):
                    latest = f
        if latest is None or f"{latest.date}/{latest.name}" == self.wave_last_file:
            return {"status": "no_new_data", "stations": [], "obs": []}
        st_meta, vals = natural.wave_values(natural.marine_items(latest.loader()), self.date)
        self.wave_last_file, self.wave_parsed_at = f"{latest.date}/{latest.name}", now
        stations, obs = [], []
        known_st = {sid for sid, _t in self.wave_emitted}
        for (sid, t), m in sorted(vals.items()):
            meta = st_meta.get(sid)
            if not meta:
                continue
            v = [m.get("wave_height"), m.get("wave_period")]
            if self.wave_emitted.get((sid, t)) == v:
                continue
            if sid not in known_st:
                stations.append(natural.wave_station(meta, sid))
                known_st.add(sid)
            self.wave_emitted[(sid, t)] = v
            obs.append([sid, t, v[0], v[1]])
        return {"status": "ok", "file": latest.name, "stations": stations, "obs": obs}

    def run_river(self, inputs: LiveInputs, now: float, baseline_ids: set[str]) -> dict:
        files = self._new_files(inputs, "river_water_level", now, self._dates(now))
        if not files:
            return {"status": "no_new_data", "obs": []}
        start, end = day_window(self.date)
        obs = []
        for doc in self._parsed("river_water_level", files):
            for r in doc.get("data", []) or []:
                if not r.get("observed_at") or r.get("station_id") not in baseline_ids:
                    continue
                t = natural.unix_of(r["observed_at"])
                k = (r["station_id"], t)
                if not (start <= t < end) or k in self.river_seen:
                    continue
                self.river_seen.add(k)
                obs.append([r["station_id"], t, natural.num(r.get("water_level_m"))])
        return {"status": "ok", "files": len(files), "obs": obs}

    def run_aviation(self, inputs: LiveInputs, now: float) -> dict:
        start, end = day_window(self.date)
        t0 = self.aviation_watermark
        # 讀到 end＋LOOKBACK：跨午夜那一段（D 最後一點 → D+1 第一點）每日檔也算在 D，收尾片要補上
        t1 = int(min(now - AVIATION_INSERT_LAG_S, end + aviation.LOOKBACK_S, t0 + AVIATION_MAX_WINDOW_S))
        if inputs.positions is None:
            return {"status": "disabled", "rows": [], "window": [t0, t0]}
        if t1 <= t0:
            return {"status": "no_new_data", "rows": [], "window": [t0, t0]}
        # 第一次（或重啟後）往前多讀一個 lookback，讓「上一點」有東西可接
        q0 = t0 - aviation.LOOKBACK_S if t0 == start else t0
        rows = inputs.positions(q0, t1)
        if rows is None:
            return {"status": "error", "rows": [], "window": [t0, t0]}
        truncated = len(rows) >= aviation.POSITIONS_LIMIT
        if truncated:
            # 達 LIMIT：只信到最後一個時間點之前（同一秒可能沒讀完）；其餘下一次讀，不宣稱完整
            cut = rows[-1]["ts"]
            rows = [r for r in rows if r["ts"] < cut]
            t1 = int(cut)
            if t1 <= t0:
                return {"status": "error", "error": "truncated", "rows": [], "window": [t0, t0]}
        if q0 < t0:
            self.aviation.window([r for r in rows if r["ts"] < t0])  # type: ignore[union-attr]  # 只建「上一點」，不發布
            self.aviation.emitted.clear()  # type: ignore[union-attr]
            rows = [r for r in rows if r["ts"] >= t0]
        if any(r["ts"] >= end for r in rows):
            # 午夜後的點只當「跨午夜那一段」的終點：每個航班取第一點，且距該航班 D 最後一點 ≤ LOOKBACK
            last_d = {fid: p[0] for fid, p in self.aviation.last_pt.items()}  # type: ignore[union-attr]
            for r in rows:
                if r["ts"] < end:
                    last_d[r["flight_id"]] = max(last_d.get(r["flight_id"], 0), r["ts"])
            first_after: dict[str, dict] = {}
            for r in rows:
                fid = r["flight_id"]
                if (r["ts"] >= end and fid not in first_after and fid not in self.aviation_after
                        and r["ts"] - last_d.get(fid, -10**12) <= aviation.LOOKBACK_S):
                    first_after[fid] = r
            self.aviation_after |= set(first_after)
            rows = [r for r in rows if r["ts"] < end] + list(first_after.values())
        frags = self.aviation.window(rows)  # type: ignore[union-attr]
        self.aviation_watermark = t1
        complete = [k for k in range((t0 - start) // SLOT_S, (t1 - start) // SLOT_S) if 0 <= k < 144]
        return {"status": "ok", "window": [t0, t1], "truncated": truncated, "slots_complete": complete, "rows": frags}


def river_baseline_doc(date: str, bbox: list[float], rows_of: Callable[[str], Optional[list]]) -> dict:
    coords = natural.river_coords()
    base = natural.river_baseline(date, bbox, rows_of)
    return {"schema": BASELINE_SCHEMA, "date_taiwan": date, "generated_at": now_tpe_iso(), "history_basis": base["basis"],
            "stations": [{"id": sid, "name": coords[sid][1], "lon": coords[sid][0][0], "lat": coords[sid][0][1], "q": q} for sid, q in base["q"].items()]}


def run_slot(day: LiveDay, inputs: LiveInputs, now: float, baseline_ids: set[str], decoder: radar.FrameDecoder,
             families: Optional[set[str]] = None) -> dict:
    """一次即時執行 → 一個分片（dict）。各族失敗互不影響；失敗族標 error（前端該族該段無法判斷）。"""
    fam = families or {"radar", "freeway_vd", "rain_gauge", "wind", "wave", "river", "aviation"}
    k = min(max(slot_of(day.date, now), 0), 143 + TAIL_SLOTS)
    timings: dict[str, float] = {}
    out: dict = {"schema": SLOT_SCHEMA, "date_taiwan": day.date, "slot": k,
                 "run_at": _iso_tpe(now), "generated_at": now_tpe_iso(),
                 "grid": {"id": GRID["id"], "sha256": GRID_SHA256}, "sources": {}}
    runners = {
        "radar": lambda: day.run_radar(inputs, now, decoder),
        "freeway_vd": lambda: day.run_vd(inputs, now),
        "rain_gauge": lambda: day.run_rain(inputs, now),
        "wind": lambda: day.run_wind(inputs, now),
        "wave": lambda: day.run_wave(inputs, now),
        "river": lambda: day.run_river(inputs, now, baseline_ids),
        "aviation": lambda: day.run_aviation(inputs, now),
    }
    for name, fn in runners.items():
        if name not in fam:
            out["sources"][name] = {"status": "disabled"}
            continue
        t = time.perf_counter()
        snap = day.snapshot_family(name)
        try:
            out["sources"][name] = fn()
        except Exception as e:  # noqa: BLE001 — 單族失敗只記錄；該族本片標 error
            day.restore_family(snap)   # 輸出丟了，狀態不可先前進
            from utils.redact import redact_secrets  # 分片會公開發布：錯誤訊息先遮金鑰
            out["sources"][name] = {"status": "error", "error": f"{type(e).__name__}: {redact_secrets(e)[:200]}"}
        timings[name] = round(time.perf_counter() - t, 3)
    out["timings_s"] = timings
    day.slots_published.add(k)
    return out


def _iso_tpe(unix: float) -> str:
    import datetime as dt
    return dt.datetime.fromtimestamp(unix, TPE).replace(microsecond=0).isoformat()


# ── 每小時合併片 ─────────────────────────────────────────────────

def rollup(date: str, hour: int, shards: list[dict]) -> dict:
    """同一小時的 slot 分片依序串接（不重算）。缺的片記在 slots_missing；前端該段照缺片處理。"""
    shards = sorted(shards, key=lambda s: s["slot"])
    present = [s["slot"] for s in shards]
    return {"schema": HOUR_SCHEMA, "date_taiwan": date, "hour": hour, "generated_at": now_tpe_iso(),
            "grid": {"id": GRID["id"], "sha256": GRID_SHA256},
            "slots": present, "slots_missing": [k for k in range(hour * 6, hour * 6 + 6) if k not in present],
            "shards": [{"slot": s["slot"], "run_at": s["run_at"], "sources": s["sources"]} for s in shards]}


def flatten(docs: Iterable[dict]) -> list[dict]:
    """slot 分片與 hour 合併片 → 依 slot 排序的 [{slot, sources}]（同 slot 只留一份）"""
    by: dict[int, dict] = {}
    for d in docs:
        if d.get("schema") == HOUR_SCHEMA:
            for s in d["shards"]:
                by.setdefault(s["slot"], s)
        else:
            by.setdefault(d["slot"], d)
    return [by[k] for k in sorted(by)]


# ── 參考合併（前端照這個寫）────────────────────────────────────────

def merge_day(date: str, docs: Iterable[dict], baseline: Optional[dict], static: Optional[dict]) -> dict:
    """當天已發布的分片 → 與每日檔同形的內容（測試拿來和每日檔比；前端 TS 照同規則寫）。

    回 {radar: {frames: {k: class bytes}, missing: set}, freeway_vd: {vd: [144]}, vd_slots: set,
        rain_gauge_rows, natural, aviation_rows, aviation_slots: set}
    """
    shards = flatten(docs)
    radar_frames: dict[int, bytes] = {}
    radar_missing: set[int] = set()
    vd_ids = freeway_vd.used_vd_ids()
    vd: dict[str, list] = {v: [None] * 144 for v in vd_ids}
    vd_slots: set[int] = set()
    rain_rows: list[dict] = []
    wind_st: dict[str, dict] = {}
    wind_order: list[str] = []
    wind_obs: dict[tuple[str, int], list] = {}
    wave_st: dict[str, dict] = {}
    wave_obs: dict[tuple[str, int], list] = {}
    river_obs: list = []
    frags: list[dict] = []
    av_slots: set[int] = set()
    for s in shards:
        src = s["sources"]
        r = src.get("radar") or {}
        for f in r.get("frames", []):
            radar_frames[f["k"]] = decode_b64gz(f["class_b64gz"])
        radar_missing |= set(r.get("missing", []))
        v = src.get("freeway_vd") or {}
        for k, row in (v.get("slots") or {}).items():
            k = int(k)
            vd_slots.add(k)
            for vid, x in zip(vd_ids, row["v"]):
                if x is not None and (vd[vid][k] is None or x > vd[vid][k]):
                    vd[vid][k] = x
        rain_rows.extend((src.get("rain_gauge") or {}).get("rows", []))
        w = src.get("wind") or {}
        for st in w.get("stations", []):
            if st["id"] not in wind_st:
                wind_st[st["id"]] = st
                wind_order.append(st["id"])
        for sid, t, ws, gust in w.get("obs", []):
            wind_obs[(sid, t)] = [sid, t, ws, gust]
        wv = src.get("wave") or {}
        for st in wv.get("stations", []):
            wave_st.setdefault(st["id"], st)
        for sid, t, h, p in wv.get("obs", []):
            wave_obs[(sid, t)] = [h, p]
        river_obs.extend((src.get("river") or {}).get("obs", []))
        a = src.get("aviation") or {}
        frags.extend(a.get("rows", []))
        av_slots |= set(a.get("slots_complete", []))

    # 雨量站：每站每整點取最晚
    rain = rain_gauge.hourly_latest(rain_rows)
    # 風：站序＝第一次出現；觀測後片覆寫前片
    idx = {sid: i for i, sid in enumerate(wind_order)}
    wind = {"stations": [wind_st[s] for s in wind_order],
            "obs": sorted(([idx[sid], t, ws, g] for (sid, t), (_s, _t, ws, g) in wind_obs.items()), key=lambda x: (x[0], x[1]))}
    # 浪：同 build_wave_block
    wave = natural.build_wave_block({sid: {"name_zh": st["name"], "station_type": st["type"], "longitude": st["lon"], "latitude": st["lat"]}
                                     for sid, st in wave_st.items()},
                                    {k: {"wave_height": v[0], "wave_period": v[1]} for k, v in wave_obs.items()}, "live")
    # 河川：每站每小時取最晚（同時刻先到為準）；站＝基準檔中今天有觀測的站
    start, end = day_window(date)
    best: dict[tuple[str, int], tuple[int, object]] = {}
    for sid, t, val in river_obs:
        k = (sid, t - (t - start) % 3600)
        if k not in best or best[k][0] < t:
            best[k] = (t, val)
    river = None
    if baseline is not None:
        st_by = {st["id"]: st for st in baseline["stations"]}
        ids = sorted({sid for sid, _ in best if sid in st_by})
        ridx = {sid: n for n, sid in enumerate(ids)}
        river = {"history_basis": baseline["history_basis"], "stations": [st_by[sid] for sid in ids],
                 "obs": sorted(([ridx[sid], t, v] for (sid, t), (_t0, v) in best.items() if sid in ridx), key=lambda x: (x[0], x[1]))}
    return {"radar": {"frames": radar_frames, "missing": radar_missing}, "freeway_vd": vd, "vd_slots": vd_slots,
            "rain_gauge_rows": rain, "natural": {"wind": wind, "wave": wave, "river": river,
                                                  "coast": (static or {}).get("coast"), "river_network": (static or {}).get("river_network")},
            "aviation_rows": aviation.merge_fragments(frags), "aviation_slots": av_slots}
