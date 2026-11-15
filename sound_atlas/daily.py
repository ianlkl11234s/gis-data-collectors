"""每日段：過了臺灣日 D 之後，從原始檔重建與前端現有格式相同的每日檔（正本）。

產物（檔名與 mini-taiwan-pulse public/sound-atlas/grid/ 相同，前端只換網址前綴）：
  radar-rain-<D>.class.bin＋radar-rain-<D>.meta.json
  freeway-vd-<D>.json
  rain-gauge-<D>.json
  natural-<D>.json
  aviation-src-<D>.json
不產 aviation-<D>.class.bin（前端 TS kernel，見 DESIGN §2.6）。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional

from . import aviation, freeway_vd, natural, radar, rain_gauge
from .common import RawSource, dumps_compact, grid_bbox, shift


RAIN_NEXT_DAY_UNTIL_HHMM = 300


@dataclass
class DailyInputs:
    raw: RawSource
    radar_fetcher: radar.FrameFetcher
    trails: Callable[[str], Optional[list[dict]]]   # date → trails/flights/<date>.json.gz 內容；None＝沒有
    static: Optional[dict]                          # natural-static-v1（coast＋river_network）
    vd_available: Optional[set[str]] = None         # 只在測試用：重現舊 DB 匯出 CSV 的 VD 範圍（正式＝None）


def build_all(date: str, inputs: DailyInputs, families: Optional[set[str]] = None,
              generated_at: Optional[dict] = None) -> tuple[dict[str, bytes], dict]:
    """回 ({檔名: 位元組}, 報告)。generated_at 可指定各檔的 generated_at（測試比對用）。"""
    fam = families or {"radar", "freeway_vd", "rain_gauge", "natural", "aviation"}
    ga = generated_at or {}
    gb = grid_bbox()
    out: dict[str, bytes] = {}
    report: dict = {"date": date, "timings_s": {}, "errors": {}}

    def timed(name: str, fn):
        t = time.perf_counter()
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — 一族失敗不擋其他族；報告列出
            report["errors"][name] = f"{type(e).__name__}: {str(e)[:300]}"
        report["timings_s"][name] = round(time.perf_counter() - t, 2)

    if "radar" in fam:
        def do_radar():
            payload, meta = radar.build_daily(date, inputs.radar_fetcher, generated_at=ga.get("radar"))
            out[f"radar-rain-{date}.class.bin"] = payload
            out[f"radar-rain-{date}.meta.json"] = radar.meta_bytes(meta)
            report["radar_frames_missing"] = len(meta["frames_missing"])
        timed("radar", do_radar)
    if "freeway_vd" in fam:
        def do_vd():
            files = inputs.raw.files("freeway_vd", date)
            if files is None:
                raise FileNotFoundError(f"freeway_vd {date} 原始檔不存在")
            doc = freeway_vd.build_daily(date, (f.json() for f in files), f"freeway_vd/archives/{date}.tar.gz", generated_at=ga.get("freeway_vd"),
                                       available=inputs.vd_available)
            out[f"freeway-vd-{date}.json"] = dumps_compact(doc)
        timed("freeway_vd", do_vd)
    if "rain_gauge" in fam:
        def do_rain():
            docs = []
            for d in (date, shift(date, 1)):  # D+1 前段：D 末尾的讀數在午夜後才收到
                files = inputs.raw.files("rain_gauge_realtime", d)
                if files is None:
                    if d == date:
                        raise FileNotFoundError(f"rain_gauge_realtime {date} 原始檔不存在")
                    report.setdefault("warnings", []).append(f"rain_gauge_realtime {d} 不存在：D 最後一小時可能不完整")
                    continue
                # D+1 只看凌晨前段（收集間隔 10 分；D 的讀數最晚約 00:10 收到，留到 03:00 的餘裕）
                docs.append(files if d == date else (f for f in files if 0 <= f.hhmm < RAIN_NEXT_DAY_UNTIL_HHMM))
            def gen():
                for files in docs:
                    for f in files:
                        yield f.json()
            doc = rain_gauge.build_daily(date, gen(), gb, f"rain_gauge_realtime/archives/{{{date},{shift(date, 1)}}}", generated_at=ga.get("rain_gauge"))
            out[f"rain-gauge-{date}.json"] = dumps_compact(doc)
        timed("rain_gauge", do_rain)
    if "natural" in fam:
        def do_nat():
            doc = natural.build_daily(date, gb, inputs.raw, inputs.static, generated_at=ga.get("natural"))
            out[f"natural-{date}.json"] = dumps_compact(doc)
        timed("natural", do_nat)
    if "aviation" in fam:
        def do_av():
            doc = aviation.build_daily(date, gb, inputs.trails, generated_at=ga.get("aviation"))
            out[f"aviation-src-{date}.json"] = dumps_compact(doc)
            report["aviation_health"] = doc["health"]
        timed("aviation", do_av)
    report["bytes"] = {k: len(v) for k, v in out.items()}
    return out, report


def transient_gaps(files: dict[str, bytes], report: dict) -> dict[str, str]:
    """哪些族的輸入是「暫時」拿不到（重跑可能補齊）→ {檔名前綴: 理由}。每日檔不可覆寫，這些族先不發布。

    真正沒有資料（上游 404、該日無檔）照發布：每日檔會寫 unknown／health，前端照缺處理。
    """
    import json

    hold: dict[str, str] = {}
    for name, b in files.items():
        if name.startswith("radar-rain-") and name.endswith(".meta.json"):
            errs = [f["index"] for f in json.loads(b).get("frames_missing", []) if f.get("status") == "fetch_error"]
            if errs:
                hold["radar-rain-"] = f"{len(errs)} 幀抓取失敗（非 404）"
        elif name.startswith("aviation-src-"):
            if json.loads(b).get("health") == "error":
                hold["aviation-src-"] = "航跡檔讀取失敗"
        elif name.startswith("natural-"):
            nat = json.loads(b).get("natural", {})
            bad = [k for k in ("wind", "wave", "river") if (nat.get(k) or {}).get("health") == "error"]
            if bad:
                hold["natural-"] = "讀取失敗：" + "、".join(bad)
    for w in report.get("warnings", []):
        if w.startswith("rain_gauge_realtime"):
            hold["rain-gauge-"] = w
    return hold
