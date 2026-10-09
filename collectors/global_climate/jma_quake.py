"""日本氣象廳 地震・津波・火山 情報收集器（三來源同一 collector，is_multi_table）

資料來源：JMA bosai（免認證、非公式 JSON）
  https://www.jma.go.jp/bosai/quake/data/list.json      → 地震情報清單（約 200+ 筆；json=檔名當 PK）
  https://www.jma.go.jp/bosai/tsunami/data/list.json    → 津波情報清單
  https://www.jma.go.jp/bosai/volcano/data/warning.json → 噴火警報・予報現況（每座有警報的火山最新公報）
  https://www.jma.go.jp/bosai/volcano/const/volcano_list.json → 火山座標（latlon 為字串）

寫入（gis-platform migration 435）：
  live.jma_quake_reports     PK json_id                     DO NOTHING；永久
  live.jma_tsunami_reports   PK json_id                     DO NOTHING；永久
  live.jma_volcano_warnings  PK (volcano_code, report_time) DO NOTHING；永久
    → report_time 缺值的火山項目跳過並 log（PK NOT NULL）

quake 欄位：cod 為 ISO 6709 '+32.4+130.5-10000/'（深度單位公尺、負號向下）→ depth_km = |深度|/1000；
  cod 空（震度速報）或 mag 非數字（'Ｍ不明' 等）→ NULL，不補 0。intensity_by_pref = list.json 的 int 原樣。
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import (
    JmaFetchError, fetch, iso, new_jma_session, to_float,
)

URL_QUAKE_LIST = "quake/data/list.json"
URL_TSUNAMI_LIST = "tsunami/data/list.json"
URL_VOLCANO_WARNING = "volcano/data/warning.json"
URL_VOLCANO_LIST = "volcano/const/volcano_list.json"

CONST_REFRESH = timedelta(hours=24)

_COD_RE = re.compile(r"^([+-]\d+(?:\.\d+)?)([+-]\d+(?:\.\d+)?)([+-]\d+(?:\.\d+)?)?/?")

VOLCANO_TYPE_TARGET = "噴火警報・予報（対象火山）"
VOLCANO_TYPE_MUNICIPALITY = "噴火警報・予報（対象市町村等）"


def parse_cod(cod: Any) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """'+32.4+130.5-10000/' → (32.4, 130.5, 10.0)；無法解析 → (None, None, None)。"""
    if not isinstance(cod, str):
        return None, None, None
    m = _COD_RE.match(cod.strip())
    if not m:
        return None, None, None
    lat, lon = to_float(m.group(1)), to_float(m.group(2))
    depth_m = to_float(m.group(3)) if m.group(3) else None
    depth_km = round(abs(depth_m) / 1000.0, 3) if depth_m is not None else None
    return lat, lon, depth_km


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def parse_quake_list(items: list, collected_at: str) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    for it in items or []:
        if not isinstance(it, dict):
            continue
        json_id = _text(it.get("json"))
        if not json_id or json_id in seen:
            continue
        seen.add(json_id)
        lat, lon, depth_km = parse_cod(it.get("cod"))
        rows.append({
            "_type": "quake",
            "json_id": json_id,
            "event_id": _text(it.get("eid")),
            "report_time": iso(it.get("rdt")),
            "origin_time": iso(it.get("at")),
            "title": _text(it.get("ttl")),
            "hypocenter_name": _text(it.get("anm")),
            "lat": lat,
            "lon": lon,
            "depth_km": depth_km,
            "magnitude": to_float(it.get("mag")),
            "max_intensity": _text(it.get("maxi")),
            "intensity_by_pref": it.get("int") if isinstance(it.get("int"), list) else None,
            "collected_at": collected_at,
        })
    return rows


def parse_tsunami_list(items: list, collected_at: str) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    for it in items or []:
        if not isinstance(it, dict):
            continue
        json_id = _text(it.get("json"))
        if not json_id or json_id in seen:
            continue
        seen.add(json_id)
        rows.append({
            "_type": "tsunami",
            "json_id": json_id,
            "event_id": _text(it.get("eid")),
            "report_time": iso(it.get("rdt")),
            "title": _text(it.get("ttl")),
            "raw": it,
            "collected_at": collected_at,
        })
    return rows


def build_volcano_index(items: list) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for v in items or []:
        if not isinstance(v, dict) or not v.get("code"):
            continue
        latlon = v.get("latlon") or [None, None]
        out[str(v["code"])] = {
            "name": v.get("name_jp"),
            "lat": to_float(latlon[0]) if len(latlon) > 0 else None,
            "lon": to_float(latlon[1]) if len(latlon) > 1 else None,
        }
    return out


def parse_volcano_warning(items: list, volcanoes: dict[str, dict], collected_at: str,
                          ) -> tuple[list[dict], int]:
    """warning.json → 每座火山一列。回傳 (rows, skipped_no_report_time)。"""
    rows: list[dict] = []
    skipped = 0
    seen: set[tuple[str, str]] = set()
    for entry in items or []:
        if not isinstance(entry, dict):
            continue
        report_time = iso(entry.get("reportDatetime"))
        infos = entry.get("volcanoInfos") or []
        warning_kind = None
        for info in infos:
            if info.get("type") == VOLCANO_TYPE_MUNICIPALITY:
                names = [i.get("name") for i in info.get("items") or [] if i.get("name")]
                warning_kind = names[0] if names else None
                break
        for info in infos:
            if info.get("type") != VOLCANO_TYPE_TARGET:
                continue
            for item in info.get("items") or []:
                for a in item.get("areas") or []:
                    code = _text(a.get("code"))
                    if not code:
                        continue
                    if not report_time:
                        skipped += 1
                        continue
                    key = (code, report_time)
                    if key in seen:
                        continue
                    seen.add(key)
                    v = volcanoes.get(code) or {}
                    rows.append({
                        "_type": "volcano",
                        "volcano_code": code,
                        "volcano_name": _text(a.get("name")) or v.get("name"),
                        "lat": v.get("lat"),
                        "lon": v.get("lon"),
                        "level_code": _text(item.get("code")),
                        "level_name": _text(item.get("name")),
                        "warning_kind": warning_kind,
                        "report_time": report_time,
                        "raw": entry,
                        "collected_at": collected_at,
                    })
    return rows, skipped


class JmaQuakeCollector(BaseCollector):
    """地震・津波・火山三清單；全部 DO NOTHING，重複抓無副作用。"""

    name = "jma_quake"
    interval_minutes = config.JAPAN_JMA_QUAKE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-quake")
        self._volcanoes: dict[str, dict] = {}
        self._volcanoes_loaded_at: Optional[datetime] = None

    def _volcano_index(self) -> dict[str, dict]:
        now = datetime.now(timezone.utc)
        if (not self._volcanoes or self._volcanoes_loaded_at is None
                or now - self._volcanoes_loaded_at > CONST_REFRESH):
            try:
                self._volcanoes = build_volcano_index(fetch(self._session, URL_VOLCANO_LIST))
                self._volcanoes_loaded_at = now
            except JmaFetchError as e:
                print(f"[{self.name}] ⚠ volcano_list 抓取失敗（座標本輪可能 NULL）: {e}")
        return self._volcanoes

    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        failures: dict[str, str] = {}
        records: list[dict] = []
        counts = {"quake": 0, "tsunami": 0, "volcano": 0}

        try:
            q = parse_quake_list(fetch(self._session, URL_QUAKE_LIST), collected_at)
            records.extend(q)
            counts["quake"] = len(q)
        except JmaFetchError as e:
            failures["quake"] = str(e)

        try:
            t = parse_tsunami_list(fetch(self._session, URL_TSUNAMI_LIST), collected_at)
            records.extend(t)
            counts["tsunami"] = len(t)
        except JmaFetchError as e:
            failures["tsunami"] = str(e)

        skipped = 0
        try:
            v, skipped = parse_volcano_warning(fetch(self._session, URL_VOLCANO_WARNING),
                                               self._volcano_index(), collected_at)
            records.extend(v)
            counts["volcano"] = len(v)
        except JmaFetchError as e:
            failures["volcano"] = str(e)
        if skipped:
            print(f"[{self.name}] ⚠ volcano 有 {skipped} 項缺 reportDatetime，已跳過（PK 不可為 NULL）")

        if len(failures) == 3:
            raise RuntimeError(f"jma_quake: 三來源全部失敗 {failures}")

        result = {
            "data": records,
            "counts": counts,
            "volcano_skipped_no_report_time": skipped,
            "source_failures": failures,
            "collected_at": collected_at,
        }
        if failures:
            # 已抓到的照寫，但本輪標失敗讓告警看得到
            result["_collector_error"] = f"jma_quake: 部分來源失敗 {failures}"
        return result


if __name__ == "__main__":
    c = JmaQuakeCollector.__new__(JmaQuakeCollector)
    c.name = "jma_quake"
    c._session = new_jma_session("jma-quake-test")
    c._volcanoes, c._volcanoes_loaded_at = {}, None
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
