"""日本氣象廳 府縣天氣預報＋早期注意情報收集器（C5，is_multi_table）

資料來源：JMA bosai（免認證、非公式 JSON）
  forecast/data/forecast/map.json              → 56 筆（每府縣一個代表區）的短期／週間 reportDatetime；當變動偵測器
  forecast/data/forecast/{office}.json         → 該 office 全部區域、全部時間序列（map.json 的超集）
  probability/data/probability/r8/map.json     → 早期注意情報（警報級の可能性），56 府縣 × 短期／週間
  common/const/area.json                       → class10 → office 對照（每 24h 更新一次）

寫入（gis-platform migration 440）：
  live.jma_forecasts               PK (report_datetime, area_code, target_date, series)  DO NOTHING；每次發表都保留
  live.jma_forecast_probabilities  PK (report_datetime, area_code, series, prob_type)    DO NOTHING

## 為什麼抓 {office}.json 而不只 map.json
map.json 每府縣只有一個代表區、沒有天氣文字／風／浪／週間氣溫上下限。office JSON 全國 56 個合計約 280 KB，
有變才抓，所以「保留細節」的成本很低。office 014030（十勝）、460040（奄美）的 JSON 404，
它們的區域分別併在 014100、460100 裡，map.json 的 class10 用 area.json 的 parent 對到可抓的 office。

## 變動偵測（只記變動）
state 鍵：fc|{office}|{short|week}、pr|{府縣首區 code}|{short|week} → reportDatetime。
短期（05/11/17 時）與週間（11/17 時）各自更新，不能用 office 一個鍵。state 只在 DB 寫入成功後前進。

## 缺值
- 上游空字串 "" → NULL（例如週間第一天的 pops／tempsMin），不補 0。
- 短期 temps 的 timeDefines 是 [今日09時, 今日00時, 明日00時, 明日09時]：00 時槽＝最低、09 時槽＝最高。
  2026-10-12 實測 05 時發表 171/171 站「今日最低」都等於「今日最高」（JMA 網頁顯示「-」），
  11 時發表也是（docs/api-platforms/jma/gotchas.md）。所以 target_date＝發表日且最低＝最高時，temp_min 存 NULL。
- 早期注意的 "" 不是缺值，是「未達中／高」：levels 照原字串存（含 ""），不轉 NULL。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import (
    JmaFetchError, fetch, iso, new_jma_session, parse_dt, to_float,
)

URL_MAP = "forecast/data/forecast/map.json"
URL_OFFICE = "forecast/data/forecast/{office}.json"
URL_PROB = "probability/data/probability/r8/map.json"
URL_AREA = "common/const/area.json"

SERIES = ("short", "week")
CONST_REFRESH = timedelta(hours=24)
JST = timezone(timedelta(hours=9))

_TEXT_FIELDS = {"weatherCodes": "weather_code", "weathers": "weather_text",
                "winds": "wind_text", "waves": "wave_text", "reliabilities": "reliability"}
_WEEK_TEMP_FIELDS = {"tempsMin": "temp_min", "tempsMinUpper": "temp_min_upper",
                     "tempsMinLower": "temp_min_lower", "tempsMax": "temp_max",
                     "tempsMaxUpper": "temp_max_upper", "tempsMaxLower": "temp_max_lower"}

FORECAST_COLS = [
    "report_datetime", "series", "office_code", "area_code", "area_name", "area_kind", "target_date",
    "weather_code", "weather_text", "wind_text", "wave_text", "pops_6h", "pop", "reliability",
    "temp_min", "temp_max", "temp_min_upper", "temp_min_lower", "temp_max_upper", "temp_max_lower",
    "collected_at",
]
PROB_COLS = ["report_datetime", "series", "office_code", "area_code", "prob_type",
             "time_defines", "levels", "collected_at"]


def _blank_to_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _pop(v: Any) -> Optional[int]:
    f = to_float(_blank_to_none(v))
    return int(f) if f is not None else None


def build_office_index(area: dict) -> dict[str, str]:
    """area.json → {class10 或 office code: 可抓的 office code}。"""
    offices = (area or {}).get("offices") or {}
    out = {code: code for code in offices}
    for code, c10 in ((area or {}).get("class10s") or {}).items():
        parent = (c10 or {}).get("parent")
        if parent:
            out[code] = parent
    return out


def map_report_times(fmap: Any, office_of: dict[str, str]) -> dict[str, dict[str, str]]:
    """map.json → {office: {'short': rdt, 'week': rdt}}；代表區 code 以 parent 對到 office。"""
    out: dict[str, dict[str, str]] = {}
    for entry in fmap or []:
        if not isinstance(entry, list) or not entry:
            continue
        try:
            code = entry[0]["timeSeries"][0]["areas"][0]["area"]["code"]
        except (KeyError, IndexError, TypeError):
            continue
        office = office_of.get(code, code)
        rec = out.setdefault(office, {})
        for i, blk in enumerate(entry[:2]):
            rdt = iso((blk or {}).get("reportDatetime"))
            if rdt:
                rec[SERIES[i]] = rdt
    return out


def parse_forecast_block(block: dict, series: str, office: str, collected_at: str) -> list[dict]:
    """office JSON 的一個區塊（短期或週間）→ 每 (area_code, target_date) 一列。"""
    rdt_dt = parse_dt((block or {}).get("reportDatetime"))
    if rdt_dt is None:
        return []
    rdt = rdt_dt.isoformat()
    report_date = rdt_dt.astimezone(JST).date()
    rows: dict[tuple[str, str], dict] = {}

    def row(area: dict, date) -> dict:
        key = (str(area.get("code")), date.isoformat())
        if key not in rows:
            rows[key] = {c: None for c in FORECAST_COLS}
            rows[key].update({
                "report_datetime": rdt, "series": series, "office_code": office,
                "area_code": key[0], "area_name": area.get("name"), "target_date": key[1],
                "collected_at": collected_at,
            })
        return rows[key]

    for ts in block.get("timeSeries") or []:
        times = [parse_dt(t) for t in ts.get("timeDefines") or []]
        for a in ts.get("areas") or []:
            area = a.get("area") or {}
            if not area.get("code"):
                continue
            for i, t in enumerate(times):
                if t is None:
                    continue
                tj = t.astimezone(JST)
                r = row(area, tj.date())
                for src, dst in _TEXT_FIELDS.items():
                    vals = a.get(src)
                    if isinstance(vals, list) and i < len(vals):
                        r[dst] = _blank_to_none(vals[i])
                        r["area_kind"] = r["area_kind"] or "area"
                pops = a.get("pops")
                if isinstance(pops, list) and i < len(pops):
                    r["area_kind"] = r["area_kind"] or "area"
                    if series == "short":
                        r["pops_6h"] = r["pops_6h"] or {}
                        r["pops_6h"][tj.strftime("%H")] = _pop(pops[i])
                    else:
                        r["pop"] = _pop(pops[i])
                temps = a.get("temps")
                if isinstance(temps, list) and i < len(temps):
                    r["area_kind"] = "amedas"
                    v = to_float(_blank_to_none(temps[i]))
                    if tj.hour == 0:
                        r["temp_min"] = v
                    elif tj.hour == 9:
                        r["temp_max"] = v
                for src, dst in _WEEK_TEMP_FIELDS.items():
                    vals = a.get(src)
                    if isinstance(vals, list) and i < len(vals):
                        r["area_kind"] = "amedas"
                        r[dst] = to_float(_blank_to_none(vals[i]))

    out = list(rows.values())
    for r in out:
        # 今日最低＝今日最高 → 上游佔位值（見模組說明），存 NULL
        if (series == "short" and r["target_date"] == report_date.isoformat()
                and r["temp_min"] is not None and r["temp_min"] == r["temp_max"]):
            r["temp_min"] = None
    return out


def parse_probability(pmap: Any, office_of: dict[str, str], collected_at: str
                      ) -> dict[tuple[str, str], tuple[str, list[dict]]]:
    """r8/map.json → {(府縣鍵, series): (reportDatetime, rows)}；府縣鍵＝該府縣首區 code。"""
    out: dict[tuple[str, str], tuple[str, list[dict]]] = {}
    for entry in pmap or []:
        if not isinstance(entry, list):
            continue
        for i, blk in enumerate(entry[:2]):
            series = SERIES[i]
            rdt = iso((blk or {}).get("reportDatetime"))
            if not rdt:
                continue
            rows: list[dict] = []
            seen: set[tuple[str, str]] = set()
            pref_key = None
            for ts in blk.get("timeSeries") or []:
                times = [iso(t) for t in ts.get("timeDefines") or []]
                for a in ts.get("areas") or []:
                    code = a.get("code")
                    if not code:
                        continue
                    pref_key = pref_key or str(code)
                    for prop in a.get("properties") or []:
                        ptype = prop.get("type")
                        levels = prop.get("probabilities")
                        if not ptype or not isinstance(levels, list) or (str(code), ptype) in seen:
                            continue
                        seen.add((str(code), ptype))
                        rows.append({
                            "report_datetime": rdt, "series": series,
                            "office_code": office_of.get(str(code), str(code)),
                            "area_code": str(code), "prob_type": ptype,
                            "time_defines": times[:len(levels)],
                            # "" ＝未達「中」，不是缺值 → 原字串保留
                            "levels": ["" if v is None else str(v) for v in levels],
                            "collected_at": collected_at,
                        })
            if pref_key:
                out[(pref_key, series)] = (rdt, rows)
    return out


class JmaForecastCollector(BaseCollector):
    """府縣天氣預報（全區域）＋早期注意；只在 reportDatetime 變動時寫入。"""

    name = "jma_forecast"
    interval_minutes = config.JAPAN_JMA_FORECAST_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-forecast")
        self._office_of: dict[str, str] = {}
        self._area_loaded_at: Optional[datetime] = None
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "jma_forecast_seen.json"
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None

    def require_db_write(self) -> bool:
        return True

    def _load_state(self) -> dict:
        if self._state is None:
            try:
                self._state = dict(json.loads(self.state_path.read_text()).get("seen") or {})
            except (OSError, ValueError, AttributeError, TypeError):
                self._state = {}
        return self._state

    def _save_state(self, seen: dict) -> None:
        self._state = seen
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"seen": seen}, sort_keys=True))
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[{self.name}] ⚠ state 寫入失敗（重啟後重送一輪，DB DO NOTHING 擋重複）: {e}")

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_state(self._pending_state)
        self._pending_state = None
        return stats

    def _office_index(self) -> dict[str, str]:
        now = datetime.now(timezone.utc)
        if not self._office_of or self._area_loaded_at is None or now - self._area_loaded_at > CONST_REFRESH:
            try:
                self._office_of = build_office_index(fetch(self._session, URL_AREA))
                self._area_loaded_at = now
            except JmaFetchError as e:
                print(f"[{self.name}] ⚠ area.json 抓取失敗（沿用舊對照）: {e}")
        return self._office_of

    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        seen = dict(self._load_state())
        office_of = self._office_index()
        records: list[dict] = []
        raw: dict[str, Any] = {}
        failures: dict[str, str] = {}
        changed = {"forecast_blocks": 0, "probability_blocks": 0}

        # ── 預報：map.json 偵測 → 有變的 office 才抓 ──
        try:
            rtimes = map_report_times(fetch(self._session, URL_MAP), office_of)
        except JmaFetchError as e:
            rtimes = {}
            failures["map"] = str(e)
        for office, blocks in sorted(rtimes.items()):
            todo = [s for s, rdt in blocks.items() if seen.get(f"fc|{office}|{s}") != rdt]
            if not todo:
                continue
            try:
                data = fetch(self._session, URL_OFFICE.format(office=office))
            except JmaFetchError as e:
                failures[f"office:{office}"] = str(e)
                continue
            raw[office] = data
            for i, blk in enumerate((data or [])[:2]):
                series = SERIES[i]
                rdt = iso((blk or {}).get("reportDatetime"))
                if series not in todo or not rdt or seen.get(f"fc|{office}|{series}") == rdt:
                    continue
                records.extend({"_type": "forecast", **r}
                               for r in parse_forecast_block(blk, series, office, collected_at))
                seen[f"fc|{office}|{series}"] = rdt
                changed["forecast_blocks"] += 1

        # ── 早期注意 ──
        try:
            pmap = fetch(self._session, URL_PROB)
            for (pref, series), (rdt, rows) in parse_probability(pmap, office_of, collected_at).items():
                if seen.get(f"pr|{pref}|{series}") == rdt:
                    continue
                records.extend({"_type": "probability", **r} for r in rows)
                seen[f"pr|{pref}|{series}"] = rdt
                changed["probability_blocks"] += 1
            if changed["probability_blocks"]:
                raw["probability"] = pmap
        except JmaFetchError as e:
            failures["probability"] = str(e)

        if "map" in failures and "probability" in failures:
            raise RuntimeError(f"jma_forecast: map 與 probability 皆失敗 {failures}")

        self._pending_state = seen
        out = {"changed": changed, "source_failures": failures, "collected_at": collected_at,
               "row_counts": {"forecast": sum(1 for r in records if r["_type"] == "forecast"),
                              "probability": sum(1 for r in records if r["_type"] == "probability")}}
        if failures:
            print(f"[{self.name}] ⚠ 部分來源失敗（未成功的區塊 state 不前進，下輪重試）: {list(failures)}")
        if not records:
            out["no_change"] = True
            return out
        out["data"] = records
        out["raw_payload"] = raw   # 只進本地 JSON → 每日歸檔 S3（含 tempAverage／precipAverage 等未入庫欄）
        return out


if __name__ == "__main__":
    c = JmaForecastCollector.__new__(JmaForecastCollector)
    c.name = "jma_forecast"
    c._session = new_jma_session("jma-forecast-test")
    c._office_of, c._area_loaded_at = {}, None
    c._state, c._pending_state = {}, None
    out = c.collect()
    print({k: v for k, v in out.items() if k not in ("data", "raw_payload")})
