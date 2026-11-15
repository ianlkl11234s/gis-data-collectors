"""國道 VD 每 10 分流量（freeway-vd-<D>.json，schema pulse-sound-freeway-vd-v1）。

品質規則 `clean_vd_day` 原樣搬自 mini-taiwan-pulse `scripts/preprocess/sound/road_grid_lib.py`；
每日檔組裝搬自 `scripts/preprocess/build-sound-road-grid.py cmd_vd`。
輸入改成 collector 原始檔（freeway_vd 本機 JSON／S3 archive；欄位與寫進 live.freeway_vd_traffic 的列相同，
抓取時間用檔內 fetch_time＝臺北時間 naive），不再用 DB 匯出 CSV。

即時（`clean_vd_live`）看不到後面的資料：
  - 整日全零（day_all_zero）改成「到目前為止 status 0 的列全為 0」→ None；
  - 瞬斷（前後都 ≥10 的全零列）後一筆未知 → 只要前一筆有效值 ≥10 就先給 None。
所以即時值只會等於每日值或是 None（無法判斷），不會把每日檔的 None 當成數字。
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Optional

from .common import SLOT_S, TPE, contract_json, day_window

SCHEMA = "pulse-sound-freeway-vd-v1"
MAX_VEH_PER_MIN_PER_LANE = 50
GLITCH_NEIGHBOR_MIN = 10

REASON_STATUS = "status_nonzero"
REASON_LANE = "implausible_per_lane"
REASON_DAY_ZERO = "day_all_zero"
REASON_GLITCH = "isolated_zero_glitch"
REASON_LIVE_DAY_ZERO = "live_all_zero_so_far"
REASON_LIVE_GLITCH = "live_zero_after_busy_next_unknown"


@dataclass
class VdRow:
    slot: int
    total: int
    speed: float
    occ: float
    lanes: int
    status: int


def clean_vd_day(rows: list[VdRow]) -> tuple[list[Optional[int]], dict[str, int], str]:
    """一支 VD 一天的列（可亂序、可同槽重複）→ 144 槽值（每分鐘輛數；None＝無法判斷）、各原因筆數、整日狀態。

    規則（依序）：
      1. status ≠ 0 → None（status 1 的列量速佔全為 0，是收集器把 -99 壓成 0，不是沒車）
      2. 每車道每分鐘 > MAX_VEH_PER_MIN_PER_LANE → None
      3. 整日 status 0 的列量全為 0 → 整日 None（故障偵測器；主線不可能整天沒車）
      4. 全零列且時間上前後兩筆有效值都 ≥ GLITCH_NEIGHBOR_MIN → None（瞬斷）
      其餘全零列保留為 0（夜間 1 分鐘內真的可能沒車）。同槽多筆取有效值的最大。
    """
    reasons: dict[str, int] = {}

    def bump(k: str) -> None:
        reasons[k] = reasons.get(k, 0) + 1

    ok0 = [r for r in rows if r.status == 0]
    if ok0 and max(r.total for r in ok0) == 0:
        reasons[REASON_DAY_ZERO] = len(rows)
        return [None] * 144, reasons, REASON_DAY_ZERO
    rows = sorted(rows, key=lambda r: r.slot)
    vals: list[Optional[int]] = []
    for r in rows:
        if r.status != 0:
            bump(REASON_STATUS); vals.append(None); continue
        if r.lanes > 0 and r.total / r.lanes > MAX_VEH_PER_MIN_PER_LANE:
            bump(REASON_LANE); vals.append(None); continue
        vals.append(r.total)
    for i, r in enumerate(rows):
        if vals[i] != 0 or not (r.speed == 0 and r.occ == 0):
            continue
        prev = next((vals[j] for j in range(i - 1, -1, -1) if vals[j] is not None), None)
        nxt = next((vals[j] for j in range(i + 1, len(rows)) if vals[j] is not None), None)
        if prev is not None and nxt is not None and prev >= GLITCH_NEIGHBOR_MIN and nxt >= GLITCH_NEIGHBOR_MIN:
            vals[i] = None; bump(REASON_GLITCH)
    out: list[Optional[int]] = [None] * 144
    for r, v in zip(rows, vals):
        if not 0 <= r.slot < 144 or v is None:
            continue
        cur = out[r.slot]
        out[r.slot] = v if cur is None else max(cur, v)
    return out, reasons, "ok"


def clean_vd_live(rows_so_far: list[VdRow], new_slots: Iterable[int]) -> dict[int, tuple[Optional[int], Optional[str]]]:
    """即時：用到目前為止的列，決定 new_slots 各槽的值（後面的列一律當未知）。回 {slot: (值, None 原因)}。

    以 clean_vd_day 的規則 1、2、4 依序跑，差別：
      - 規則 3：到目前為止 status 0 的列全為 0 → 這些槽一律 None（REASON_LIVE_DAY_ZERO）；
      - 規則 4：本槽之後的有效值未知 → 前一筆有效值 ≥ 門檻就 None（REASON_LIVE_GLITCH）。
    """
    want = sorted(set(new_slots))
    ok0 = [r for r in rows_so_far if r.status == 0]
    if not ok0 or max(r.total for r in ok0) == 0:
        reason = REASON_LIVE_DAY_ZERO if ok0 else None
        res: dict[int, tuple[Optional[int], Optional[str]]] = {}
        for s in want:
            st = [r for r in rows_so_far if r.slot == s]
            res[s] = (None, reason if st and any(r.status == 0 for r in st) else (REASON_STATUS if st else "no_row"))
        return res
    rows = sorted(rows_so_far, key=lambda r: r.slot)
    vals: list[Optional[int]] = []
    why: list[Optional[str]] = []
    for r in rows:
        if r.status != 0:
            vals.append(None); why.append(REASON_STATUS); continue
        if r.lanes > 0 and r.total / r.lanes > MAX_VEH_PER_MIN_PER_LANE:
            vals.append(None); why.append(REASON_LANE); continue
        vals.append(r.total); why.append(None)
    for i, r in enumerate(rows):
        if vals[i] != 0 or not (r.speed == 0 and r.occ == 0):
            continue
        prev = next((vals[j] for j in range(i - 1, -1, -1) if vals[j] is not None), None)
        nxt = next((vals[j] for j in range(i + 1, len(rows)) if vals[j] is not None), None)
        if prev is None or prev < GLITCH_NEIGHBOR_MIN:
            continue
        if nxt is None:
            vals[i] = None; why[i] = REASON_LIVE_GLITCH  # 後一筆未知（尚未到）→ 先當無法判斷
        elif nxt >= GLITCH_NEIGHBOR_MIN:
            vals[i] = None; why[i] = REASON_GLITCH
    out: dict[int, tuple[Optional[int], Optional[str]]] = {}
    for r, v, w in zip(rows, vals, why):
        if r.slot not in want:
            continue
        cur = out.get(r.slot)
        if v is None:
            if cur is None:
                out[r.slot] = (None, w)
            continue
        out[r.slot] = (v, None) if cur is None or cur[0] is None else (max(cur[0], v), None)
    for s in want:
        out.setdefault(s, (None, "no_row"))
    return out


# ── 原始檔 → 列 ────────────────────────────────────────────────────

def used_vd_ids() -> list[str]:
    """road-network-v1 國道路段用到的 VD（契約 freeway_vd_ids.json）"""
    return contract_json("freeway_vd_ids.json")


def fetch_unix(doc: dict) -> Optional[float]:
    """檔內 fetch_time 是臺北時間 naive（collector datetime.now()；容器 TZ=Asia/Taipei）"""
    ft = doc.get("fetch_time")
    if not ft:
        return None
    t = dt.datetime.fromisoformat(ft)
    if t.tzinfo is None:
        t = t.replace(tzinfo=TPE)
    return t.timestamp()


def rows_from_doc(doc: dict, date: str, wanted: set[str]) -> tuple[Optional[int], Optional[float], dict[str, VdRow]]:
    """一份 freeway_vd 原始檔 → (槽, 抓取 unix, {vd_id: VdRow})；不在臺灣日 date 內 → (None, t, {})。

    欄位對應 storage/supabase_writer._transform_freeway_vd（寫進 live.freeway_vd_traffic 的同一份值）。
    """
    t = fetch_unix(doc)
    if t is None:
        return None, None, {}
    start, _ = day_window(date)
    slot = int((t - start) // SLOT_S)
    if not 0 <= slot < 144:
        return None, t, {}
    out: dict[str, VdRow] = {}
    for r in (doc.get("data") or {}).get("vd", []) or []:
        vid = r.get("VDID", "")
        if vid not in wanted:
            continue
        out[vid] = VdRow(slot, int(r.get("TotalVolume") or 0), float(r.get("AvgSpeed") or 0), float(r.get("AvgOccupancy") or 0),
                         int(r.get("LaneCount") or 0), int(r.get("Status") if r.get("Status") is not None else -1))
    return slot, t, out


def _iso_utc_seconds(t: float) -> str:
    """同 build-sound-road-grid.py：pandas Timestamp(UTC).isoformat(timespec='seconds') → 'YYYY-MM-DDTHH:MM:SS+00:00'"""
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).replace(microsecond=0).isoformat()


def build_daily(date: str, docs: Iterable[dict], csv_name: str, generated_at: Optional[str] = None,
                available: Optional[set[str]] = None) -> dict:
    """當日全部 freeway_vd 原始檔（依抓取順序）→ freeway-vd-<D>.json 文件。

    available：只採用這些 VD 的列（測試用：重現舊 DB 匯出 CSV 的空間範圍；正式＝None，全部採用）。
    """
    used = used_vd_ids()
    wanted = set(used) if available is None else set(used) & available
    by_vd: dict[str, list[VdRow]] = defaultdict(list)
    lanes_first: dict[str, int] = {}
    fetch_by_slot: dict[int, float] = {}
    for doc in docs:
        slot, t, rows = rows_from_doc(doc, date, wanted)
        if slot is None:
            continue
        if slot not in fetch_by_slot or t < fetch_by_slot[slot]:  # type: ignore[operator]
            fetch_by_slot[slot] = t  # type: ignore[assignment]
        for vid, r in rows.items():
            by_vd[vid].append(r)
            lanes_first.setdefault(vid, r.lanes)
    reasons_total: dict[str, int] = defaultdict(int)
    vds = {}
    for vid in used:
        rows = by_vd.get(vid)
        if not rows:
            vds[vid] = {"lanes": None, "day": "not_in_export", "v": [None] * 144}
            reasons_total["not_in_export"] += 1
            continue
        vals, reasons, day = clean_vd_day(rows)
        for k, n in reasons.items():
            reasons_total[k] += n
        vds[vid] = {"lanes": lanes_first[vid], "day": day, "v": vals}
    missing = [k for k in range(144) if k not in fetch_by_slot]
    known = sum(1 for v in vds.values() for x in v["v"] if x is not None)
    midnight_utc = dt.datetime.fromtimestamp(day_window(date)[0], dt.timezone.utc)
    return {
        "schema": SCHEMA,
        "date_taiwan": date,
        "generated_at": generated_at or dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "source": {"table": "live.freeway_vd_traffic（data-collectors freeway_vd，TDX /v2/Road/Traffic/Live/VD/Freeway）",
                   "export": csv_name, "license": "政府資料開放授權條款-第1版"},
        "unit": "veh_per_min",
        "unit_note": "每 10 分抓一次即時值，每個值是該次的 1 分鐘流量（上游 UpdateInterval=60），不是 10 分鐘累計；輛/時＝值×60",
        "frames": {"count": 144, "step_s": 600, "first_slot_utc": midnight_utc.isoformat(),
                   "slot_rule": "slot k＝[first_slot+600k, +600)，依抓取時間落槽；同槽多筆取有效最大；沒抓到＝null（不回補）",
                   "slots_missing": missing, "slot_fetch_utc": [_iso_utc_seconds(fetch_by_slot[k]) if k in fetch_by_slot else None for k in range(144)]},
        "qa": {"rules": clean_vd_day.__doc__.strip(), "null_reasons": dict(reasons_total),
               "known_values": known, "total_values": len(vds) * 144},
        "vds": vds,
    }
