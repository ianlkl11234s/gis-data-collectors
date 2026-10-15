"""JMA 分析圖（analysis maps）：由 T1 每日彙總累積出「季／月至今」的每日一張圖，給 Pulse 時間軸拖曳看累積。

文件：docs/JMA_RASTER_CONTRACT.md「分析圖」節。編碼與發布沿用 publish.py（LA PNG、R2 imagery/jma/…、frames image 欄）。

產品（每 JST 日 D 一張；valid_time＝D 00:00 JST；**內容含 D 當日**的 T1，即「截至 D 日結束」）：
  ana_snow_first        積雪季（10/1–5/31）首次「解析積雪深 ≥5 cm」的日序（10/1＝1 … 5/31＝243；0＝至今未達）
  ana_snow_days         積雪季至今「≥5 cm」日數（某日任一小時解析值 ≥5 cm 即算一日）
  ana_rain_days         當月（JST）至今「解析雨量 1 小時 ≥1 mm」至少一次的日數
  ana_risk_land_hours   當月至今 土砂キキクル「警戒（level 3）以上」時數（無條件進位到整小時）
  ana_risk_inund_hours  當月至今 浸水キキクル「警戒以上」時數

門檻（皆可由現有 T1 精確推得）：
  積雪 ≥5 cm   ＝ snow_depth max_level ≥ 2（解碼表 level 1＝<5 cm、level 2＝5–20 cm；兩級色票已以實磚驗證）。
                level 1 涵蓋 0–5 cm 的薄雪／融雪殘值，不當作「有積雪日」。T1 沒有 count_ge_2，但兩張雪圖都是日粒度，
                日最大等級 ≥2 即精確等於「當日至少一小時 ≥5 cm」。
  雨日 ≥1 mm   ＝ rasrf max_level ≥ 2（1 小時解析雨量 ≥1 mm）。選 rasrf 不選 radar：解析雨量以雨量計校正、是累積量；
                nowc hrpns 是瞬間回波強度，level 1（<1 mm/h）含非降水回波／海面雜波，「≥1 mm」也是雨日的通用定義。
                代價：rasrf 上游只留 12h（radar 24h），collector 長時間中斷較易缺幀（以覆蓋率表達，不當作無雨）。
                注意：日雨量 ≥1 mm 但每小時都 <1 mm 的毛毛雨日不會算到（偏保守）。
  キキクル警戒 ＝ count_ge_3（level 3＝警戒【警戒レベル３相当】）。土砂／浸水**分開兩張**：T1 只有各自的幀計數，
                兩者聯集（任一達警戒的時數）無法由日計數推得；兩者也是不同災害，前端可各自開關。

時數換算不可寫死 /2：取樣間隔曾變更（risk_* 2026-10-11 由 10 分改 30 分，當日 frames_expected=144），
  每日以 meta.frames_expected 換算 step＝1440/frames_expected 分鐘，累積「分鐘」整數。

覆蓋率（不得把缺資料當成 0）：
  日粒度（雪、雨）：像素某日「有觀測」＝該日 T1 存在且（有事件 或 有效幀 ≥ frames_expected 的一半）。
  分鐘粒度（キキクル）：有效幀×step 累積成觀測分鐘。
  像素覆蓋率＝觀測量／自有效起日（max(期間起日, EPOCH)）以來經過量；< min_coverage（預設 0.5）→ L＝255（未涵蓋）。
  T1 缺日（例：snow_depth 10-13、10-14）＝該日全部像素無觀測，只降低覆蓋率。
  frame 列 status：期間內每日 T1 都在且未被 EPOCH 截斷 → ok；有缺日或被截斷 → partial；完全沒有 T1 → missing（不出圖）。

解析度：radar／rasrf／risk_* 的 T1 是 z7（4096×3072），先 2×2 聚合到 z6（2048×1536）再累積
  （事件取任一、計數與有效幀取最大，與發布幀 pool_max 一致）；snow_depth 本來就是 z6。四角經緯度不變。

狀態（可由 T1 重算、冪等）：
  S3 weather-daily/jma/_analysis/{family}/{period_id}/{YYYYMMDD}.npz ＝ 截至 D 的累積陣列＋meta；
  物件 metadata `sig`＝sha256(演算法版本、family、D、有效起日、納入的 T1 日清單)。
  算 D 時往回找最近一個 sig 正確的快照，再往前逐日補算（遲到的 T1 會讓後面所有日的 sig 改變 → 自動重算＋重發）。
  任何快照遺失都能從 T1 重建；--recompute 可強制全部重算。
  四張圖都與日序無關（min／sum），但仍逐日依序累積以保留每日快照。

encoding `count_la_v1`（8-bit LA PNG，EPSG:3857，row 0＝北，z6 2048×1536）：
  L＝值 clamp 0–254（254＝≥254；時數月上限 744h，實務上警戒以上不會連續超過 254h）、A＝255；
  值 0 → (0,0) 透明；像素覆蓋率不足 → (255,255)（前端畫「未涵蓋」灰）。值的單位依 product（日序／日／小時）。
  ana_snow_first 的 L 是日序（10/1＝1），不是計數，但編碼規則相同。

旁檔：每張圖同 key 換副檔名 .json（期間、有效起日、缺 T1 日、覆蓋率、門檻、狀態），前端可選讀。
R2 Cache-Control 用短快取（遲到 T1 會重發同 key），不是 immutable。
"""

from __future__ import annotations

import hashlib
import io
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Callable, Optional

import numpy as np

from collectors.weather_raster import publish as pub

SOURCE = "jma"
JST = timezone(timedelta(hours=9))
ALGO_VERSION = "jma-analysis-v1"
ENC_COUNT = "count_la_v1"
CACHE_CONTROL = "public, max-age=3600"
STATE_PREFIX = f"weather-daily/{SOURCE}/_analysis"
Z6_SHAPE = (1536, 2048)
SNOW_SEASON_MONTHS = frozenset({10, 11, 12, 1, 2, 3, 4, 5})
DEFAULT_GRACE_DAYS = 3


@dataclass(frozen=True)
class Family:
    name: str
    source_product: str        # T1 來源
    period: str                # snow_season / month
    outputs: tuple[str, ...]
    unit: str


FAMILIES: dict[str, Family] = {f.name: f for f in (
    Family("snow", "snow_depth", "snow_season", ("ana_snow_first", "ana_snow_days"), "day"),
    Family("rain", "rasrf", "month", ("ana_rain_days",), "day"),
    Family("risk_land", "risk_land", "month", ("ana_risk_land_hours",), "minute"),
    Family("risk_inund", "risk_inund", "month", ("ana_risk_inund_hours",), "minute"),
)}
OUTPUT_FAMILY = {o: f for f in FAMILIES.values() for o in f.outputs}

OUTPUT_META = {
    "ana_snow_first": {"value": "積雪季日序（10/1＝1）", "unit": "day_of_season",
                       "threshold": "snow_depth level≥2（≥5 cm）"},
    "ana_snow_days": {"value": "積雪季至今 ≥5 cm 日數", "unit": "days", "threshold": "snow_depth level≥2（≥5 cm）"},
    "ana_rain_days": {"value": "當月至今雨日數", "unit": "days", "threshold": "rasrf level≥2（1h ≥1 mm）"},
    "ana_risk_land_hours": {"value": "當月至今土砂キキクル警戒以上時數（進位）", "unit": "hours",
                            "threshold": "risk_land level≥3（警戒）"},
    "ana_risk_inund_hours": {"value": "當月至今浸水キキクル警戒以上時數（進位）", "unit": "hours",
                             "threshold": "risk_inund level≥3（警戒）"},
}


# ───────────────────────── 日期／期間 ─────────────────────────

def ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def period_of(family: Family, d: date) -> Optional[tuple[str, date]]:
    """(period_id, 期間起日)；不在期間內（積雪季外）回 None。"""
    if family.period == "month":
        return f"{d:%Y%m}", d.replace(day=1)
    if d.month not in SNOW_SEASON_MONTHS:
        return None
    y = d.year if d.month >= 10 else d.year - 1
    return f"season{y}", date(y, 10, 1)


def day_of_season(d: date) -> int:
    y = d.year if d.month >= 10 else d.year - 1
    return (d - date(y, 10, 1)).days + 1


def valid_time(d: date) -> datetime:
    return datetime.combine(d, dtime(0), JST)


def t1_key(product: str, day: str) -> str:
    return f"weather-daily/{SOURCE}/{product}/{day[:4]}/{day}.npz"


def state_key(family: Family, period_id: str, day: str) -> str:
    return f"{STATE_PREFIX}/{family.name}/{period_id}/{day}.npz"


def signature(family: Family, day: str, eff_start: str, applied: list[str]) -> str:
    body = json.dumps({"v": ALGO_VERSION, "family": family.name, "day": day, "eff_start": eff_start,
                       "applied": sorted(applied)}, sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()


# ───────────────────────── 純函式：T1 → 單日貢獻（z6）─────────────────────────

def pool2_any(a: np.ndarray) -> np.ndarray:
    h, w = a.shape
    return a.reshape(h // 2, 2, w // 2, 2).any(axis=(1, 3))


def pool2_max(a: np.ndarray) -> np.ndarray:
    h, w = a.shape
    return a.reshape(h // 2, 2, w // 2, 2).max(axis=(1, 3))


def pool2_min(a: np.ndarray) -> np.ndarray:
    h, w = a.shape
    return a.reshape(h // 2, 2, w // 2, 2).min(axis=(1, 3))


def _to_z6(a: np.ndarray, how: str) -> np.ndarray:
    """z7 → z6（2×2）；已是 z6 原樣。how：any／max／min。"""
    if a.shape == Z6_SHAPE:
        return a
    if a.shape != (Z6_SHAPE[0] * 2, Z6_SHAPE[1] * 2):
        raise ValueError(f"T1 shape {a.shape} 不是 z6／z7 日本畫布")
    return {"any": pool2_any, "max": pool2_max, "min": pool2_min}[how](a)


def day_contribution(family: Family, t1_bytes: bytes, warn: Callable[[str], None] = print) -> dict:
    """單日 T1 → z6 貢獻。只讀需要的陣列，且先在原生型別（uint8／uint16）2×2 聚合再運算，控制記憶體
    （z7 每面 uint16 25 MB）。回 {event/observed（bool）或 warn_min/obs_min（int32 分鐘）, frames_expected, meta}。

    像素有效幀 v＝valid_frames − missing_count；2×2 聚合取 v 最大（＝missing 最小），事件取任一、計數取最大。
    """
    with np.load(io.BytesIO(t1_bytes)) as z:
        meta = json.loads(str(z["meta"]))
        fe_meta = int(meta.get("frames_expected") or 0)
        vf = int(z["valid_frames"])
        # 分母＝max(frames_expected, valid_frames)：
        #   取樣間隔變更日（risk_* 10-12：expected 48、valid 77）多出的非格點幀也在計數裡，每幀代表的時間要縮小；
        #   季節開關變更日（snow_depth 10-12：expected 0、valid 8）expected 不可信，以實收幀為準。
        fe = max(fe_meta, vf)
        if fe <= 0:
            raise ValueError("T1 frames_expected 與 valid_frames 皆為 0")
        if vf > fe_meta:
            warn(f"valid_frames {vf} > frames_expected {fe_meta}，每幀時間以 1440/{fe} 分換算")
        miss = _to_z6(z["missing_count"], "min").astype(np.int32)
        v = np.clip(vf - miss, 0, fe)
        if family.unit == "day":
            ml = z["max_level"]
            event = _to_z6((ml >= 2) & (ml != 255), "any")
            observed = event | (v * 2 >= fe)
            return {"event": event, "observed": observed, "frames_expected": fe, "meta": meta}
        step = 1440.0 / fe
        cnt = np.minimum(_to_z6(z["count_ge_3"], "max").astype(np.int32), fe)
        return {"warn_min": np.rint(cnt * step).astype(np.int32), "obs_min": np.rint(v * step).astype(np.int32),
                "frames_expected": fe, "meta": meta}


# ───────────────────────── 純函式：累積 ─────────────────────────

def empty_state(family: Family) -> dict:
    if family.name == "snow":
        return {"first": np.zeros(Z6_SHAPE, np.uint8), "days": np.zeros(Z6_SHAPE, np.uint16),
                "obs_days": np.zeros(Z6_SHAPE, np.uint16)}
    if family.unit == "day":
        return {"days": np.zeros(Z6_SHAPE, np.uint16), "obs_days": np.zeros(Z6_SHAPE, np.uint16)}
    return {"warn_min": np.zeros(Z6_SHAPE, np.uint32), "obs_min": np.zeros(Z6_SHAPE, np.uint32)}


def apply_day(family: Family, state: dict, d: date, contrib: Optional[dict]) -> dict:
    """回新 state（不改輸入）。contrib None＝該日無 T1（只增加經過量，不增加觀測）。"""
    s = {k: v.copy() for k, v in state.items()}
    if contrib is None:
        return s
    if family.unit == "day":
        ev = contrib["event"]
        s["days"] += ev.astype(np.uint16)
        s["obs_days"] += contrib["observed"].astype(np.uint16)
        if "first" in s:
            s["first"] = np.where((s["first"] == 0) & ev, np.uint8(day_of_season(d)), s["first"]).astype(np.uint8)
    else:
        s["warn_min"] += contrib["warn_min"].astype(np.uint32)
        s["obs_min"] += contrib["obs_min"].astype(np.uint32)
    return s


def output_values(output: str, state: dict, elapsed_days: int) -> tuple[np.ndarray, np.ndarray]:
    """(值, 像素覆蓋率 0–1)。"""
    if output.endswith("_hours"):
        val = np.ceil(state["warn_min"].astype(np.float64) / 60.0).astype(np.int64)
        cov = state["obs_min"].astype(np.float64) / (elapsed_days * 1440.0)
    else:
        val = (state["first"] if output == "ana_snow_first" else state["days"]).astype(np.int64)
        cov = state["obs_days"].astype(np.float64) / float(elapsed_days)
    return val, np.clip(cov, 0.0, 1.0)


def count_la(values: np.ndarray, coverage: np.ndarray, min_coverage: float) -> tuple[np.ndarray, np.ndarray]:
    """count_la_v1：L＝clamp(值, 0, 254)、A＝255；值 0 → (0,0)；覆蓋率 < min_coverage → (255,255)。"""
    v = np.asarray(values, np.int64)
    L = np.clip(v, 0, 254).astype(np.uint8)
    A = np.where(v > 0, 255, 0).astype(np.uint8)
    bad = np.asarray(coverage) < min_coverage
    L[bad] = 255
    A[bad] = 255
    return L, A


def state_to_bytes(state: dict, meta: dict) -> bytes:
    bio = io.BytesIO()
    np.savez_compressed(bio, meta=np.array(json.dumps(meta, ensure_ascii=False, sort_keys=True)), **state)
    return bio.getvalue()


def state_from_bytes(data: bytes) -> tuple[dict, dict]:
    with np.load(io.BytesIO(data)) as z:
        meta = json.loads(str(z["meta"]))
        return {k: z[k] for k in z.files if k != "meta"}, meta


# ───────────────────────── 儲存介面 ─────────────────────────

class S3StateStore:
    """S3Storage 的薄包裝（測試用 dict 假物件取代）：get／put（帶 sig metadata）／head_sig／list_keys。"""

    def __init__(self, s3, read_only: bool = False):
        self.s3 = s3
        self.read_only = read_only
        self._mem: dict[str, tuple[bytes, str]] = {}  # dry-run 的寫入只留記憶體

    def get(self, key: str) -> Optional[bytes]:
        if key in self._mem:
            return self._mem[key][0]
        return self.s3.get_file(key)

    def head_sig(self, key: str) -> Optional[str]:
        if key in self._mem:
            return self._mem[key][1]
        h = self.s3.head(key)
        return None if h is None else (h.get("Metadata") or {}).get("sig")

    def put(self, key: str, data: bytes, sig: str) -> None:
        if self.read_only:
            self._mem[key] = (data, sig)
            return
        self.s3.s3.put_object(Bucket=self.s3.bucket, Key=key, Body=data, StorageClass="STANDARD",
                              ContentType="application/octet-stream", Metadata={"sig": sig})

    def list_keys(self, prefix: str) -> list[str]:
        return [f["key"] for f in self.s3.list_files(prefix, max_keys=5000)]


# ───────────────────────── 引擎 ─────────────────────────

class AnalysisEngine:
    """計算＋發布。collector（每輪增量）與 scripts/backfill_jma_analysis.py（區間回補）共用。

    publish_cache：{output: {YYYYMMDD: pub_sig}}，相同 pub_sig 已發布就不重發（collector 用本機 state；回補傳 None＝一律發）。
    """

    def __init__(self, store, r2=None, *, epoch: date, min_coverage: float = 0.5,
                 grace_days: int = DEFAULT_GRACE_DAYS, warn: Callable[[str], None] = print,
                 log_prefix: str = "[jma-analysis]"):
        self.store = store
        self.r2 = r2
        self.epoch = epoch
        self.min_coverage = min_coverage
        self.grace_days = grace_days
        self.warn = warn
        self.log = log_prefix
        self._t1_days: dict[str, set[str]] = {}
        self._mem: dict[str, tuple[str, str, dict, dict]] = {}   # family → (day, sig, state, meta) 最近一個
        self.stats = {"t1_read": 0, "states_computed": 0, "states_reused": 0, "published": 0, "publish_failed": 0}

    # T1 可用日（每個來源每輪 LIST 一次；年份跨越時列兩年）
    def t1_days(self, product: str, years: set[int]) -> set[str]:
        have = self._t1_days.setdefault(product, set())
        listed = self._t1_days.setdefault(f"__years__{product}", set())
        for y in sorted(years - {int(x) for x in listed}):
            for k in self.store.list_keys(f"weather-daily/{SOURCE}/{product}/{y}/"):
                name = k.rsplit("/", 1)[-1]
                if name.endswith(".npz") and name[:8].isdigit():
                    have.add(name[:8])
            listed.add(str(y))
        return have

    def eligible(self, family: Family, d: date, today: date) -> bool:
        """D 可以出圖：在期間內、≥ EPOCH、早於今天（JST），且 T1 已在 S3 或已過寬限期（之後缺日視為永久缺）。"""
        if d < self.epoch or d >= today or period_of(family, d) is None:
            return False
        have = self.t1_days(family.source_product, {d.year})
        return ymd(d) in have or today >= d + timedelta(days=1 + self.grace_days)

    def _plan(self, family: Family, d: date) -> tuple[str, date, list[str], list[str]]:
        pid, pstart = period_of(family, d)
        eff = max(pstart, self.epoch)
        have = self.t1_days(family.source_product, {eff.year, d.year})
        days = [ymd(eff + timedelta(days=i)) for i in range((d - eff).days + 1)]
        return pid, eff, [x for x in days if x in have], [x for x in days if x not in have]

    def ensure_state(self, family: Family, d: date, recompute: bool = False) -> tuple[dict, dict, str]:
        """截至 D 的累積 state（必要時從最近一個有效快照往前補算，並寫回快照）。"""
        pid, eff, applied, _absent = self._plan(family, d)
        target_sig = signature(family, ymd(d), ymd(eff), applied)
        mem = self._mem.get(family.name)
        if mem and mem[0] == ymd(d) and mem[1] == target_sig:
            return mem[2], mem[3], target_sig
        chain: list[date] = []
        base_state, base_meta = None, None
        x = d
        while x >= eff:
            _p, _e, ap_x, _ab = self._plan(family, x)
            sig_x = signature(family, ymd(x), ymd(eff), ap_x)
            if mem and mem[0] == ymd(x) and mem[1] == sig_x:
                base_state, base_meta = mem[2], mem[3]
                break
            key = state_key(family, pid, ymd(x))
            if not recompute and self.store.head_sig(key) == sig_x:
                data = self.store.get(key)
                if data:
                    base_state, base_meta = state_from_bytes(data)
                    self.stats["states_reused"] += 1
                    break
            chain.append(x)
            x -= timedelta(days=1)
        state = base_state if base_state is not None else empty_state(family)
        for x in reversed(chain):
            dx = ymd(x)
            contrib = None
            if dx in self.t1_days(family.source_product, {x.year}):
                data = self.store.get(t1_key(family.source_product, dx))
                self.stats["t1_read"] += 1
                if data:
                    try:
                        contrib = day_contribution(family, data, lambda m, dx=dx: self.warn(
                            f"{self.log} {family.source_product} {dx}: {m}"))
                    except Exception as e:  # noqa: BLE001
                        self.warn(f"{self.log} ✗ {family.source_product} {dx} T1 無法讀取，當作缺日：{e}")
                        contrib = None
            state = apply_day(family, state, x, contrib)
            _p, _e, ap_x, ab_x = self._plan(family, x)
            if contrib is None and dx in ap_x:   # T1 列得到但讀不到 → 不可記成已納入
                ap_x = [a for a in ap_x if a != dx]
                ab_x = sorted(ab_x + [dx])
            sig_x = signature(family, dx, ymd(eff), ap_x)
            base_meta = {"version": ALGO_VERSION, "family": family.name, "source_product": family.source_product,
                         "period_id": pid, "period_start": period_of(family, x)[1].isoformat(),
                         "effective_start": eff.isoformat(), "as_of": x.isoformat(),
                         "elapsed_days": (x - eff).days + 1, "t1_days": ap_x, "t1_absent_days": ab_x,
                         "grid": "z6 2048x1536 EPSG:3857 (z7 sources pooled 2x2)", "sig": sig_x}
            self.store.put(state_key(family, pid, dx), state_to_bytes(state, base_meta), sig_x)
            self.stats["states_computed"] += 1
            self._mem[family.name] = (dx, sig_x, state, base_meta)
        if not chain:
            self._mem[family.name] = (ymd(d), base_meta.get("sig", target_sig), state, base_meta)
        return state, base_meta, base_meta.get("sig", target_sig)

    def render(self, output: str, state: dict, meta: dict) -> dict:
        val, cov = output_values(output, state, int(meta["elapsed_days"]))
        L, A = count_la(val, cov, self.min_coverage)
        png = pub.png_la(L, A)
        observed = cov > 0
        return {"data": png, "png": png, "width": int(L.shape[1]), "height": int(L.shape[0]),
                "encoding": ENC_COUNT, "ext": "png", "content_type": pub.CONTENT_TYPE,
                "nonzero_pixels": int(((val > 0) & (cov >= self.min_coverage)).sum()),
                "coverage_mean": round(float(cov[observed].mean()), 4) if observed.any() else 0.0,
                "insufficient_pixels": int((cov < self.min_coverage).sum()),
                "max_value": int(val.max()) if val.size else 0}

    @staticmethod
    def frame_status(meta: dict) -> str:
        if not meta["t1_days"]:
            return "missing"
        if meta["t1_absent_days"] or meta["effective_start"] != meta["period_start"]:
            return "partial"
        return "ok"

    def sidecar(self, output: str, d: date, meta: dict, r: Optional[dict], status: str) -> dict:
        return {"product": output, "as_of_date": d.isoformat(), "valid_time": valid_time(d).isoformat(),
                "includes_as_of_day": True, "encoding": ENC_COUNT, **OUTPUT_META[output],
                "source_product": meta["source_product"], "period_start": meta["period_start"],
                "effective_start": meta["effective_start"], "elapsed_days": meta["elapsed_days"],
                "t1_days": len(meta["t1_days"]), "t1_absent_days": meta["t1_absent_days"],
                "min_coverage": self.min_coverage, "status": status,
                "coverage_mean": r["coverage_mean"] if r else 0.0,
                "insufficient_pixels": r["insufficient_pixels"] if r else None,
                "algorithm": ALGO_VERSION, "license": "気象庁ホームページ（PDL1.0）を加工"}

    def pub_sig(self, state_sig: str, output: str) -> str:
        return hashlib.sha256(f"{state_sig}|{output}|{self.min_coverage}|{ENC_COUNT}".encode()).hexdigest()[:32]

    def publish_day(self, family: Family, d: date, *, publish_cache: Optional[dict] = None,
                    recompute: bool = False, dry_run: bool = False) -> list[dict]:
        """算 D 的 state、出 family 的每張圖。回 frame 列（含 image 欄；missing 時無 image）。
        publish_cache 命中（同 sig 已發布）的圖先略過，全部命中就連 state 都不讀（collector 每輪的常態）。"""
        _pid, eff, applied, _ab = self._plan(family, d)
        planned = signature(family, ymd(d), ymd(eff), applied)
        todo = [o for o in family.outputs
                if publish_cache is None or publish_cache.get(o, {}).get(ymd(d)) != self.pub_sig(planned, o)]
        if not todo:
            return []
        state, meta, sig = self.ensure_state(family, d, recompute=recompute)
        status = self.frame_status(meta)
        rows = []
        vt = valid_time(d)
        bounds = list(_bounds_lonlat())
        for output in todo:
            pub_sig = self.pub_sig(sig, output)
            row = {"_type": "frame", "source": SOURCE, "product": output, "valid_time": vt.isoformat(),
                   "status": status, "_pub_sig": pub_sig, "_day": ymd(d)}
            if status == "missing":
                rows.append(row)
                continue
            r = self.render(output, state, meta)
            r["bounds"] = bounds
            key = pub.r2_key(output, vt, "png")
            row["nonzero_pixels"] = r["nonzero_pixels"]
            row["_render"] = {k: r[k] for k in ("coverage_mean", "insufficient_pixels", "max_value")}
            row["_bytes"] = len(r["data"])
            if not dry_run:
                if self.r2 is None:
                    self.warn(f"{self.log} R2 未設定，略過 {output} {ymd(d)}")
                    continue
                try:
                    self.r2.upload_image(key, r["data"], pub.CONTENT_TYPE, cache_control=CACHE_CONTROL)
                    side = json.dumps(self.sidecar(output, d, meta, r, status), ensure_ascii=False,
                                      sort_keys=True).encode("utf-8")
                    self.r2.upload_image(key[:-4] + ".json", side, "application/json", cache_control=CACHE_CONTROL)
                except Exception as e:  # noqa: BLE001
                    self.stats["publish_failed"] += 1
                    self.warn(f"{self.log} ✗ R2 {key}：{e}（下輪重試）")
                    continue
            row.update(pub.image_fields(key, r, datetime.now(timezone.utc)))
            self.stats["published"] += 1
            rows.append(row)
        return rows


def _bounds_lonlat() -> list[float]:
    """日本畫布 z6 四角（與 T1 meta.georef.bounds_lonlat 相同；z7 同範圍）。"""
    from collectors.weather_raster import spool as sp
    return sp.georef(6, 52, 22, 2048, 1536)["bounds_lonlat"]


def db_row(row: dict) -> dict:
    """去掉內部欄位（_pub_sig、_day、_render、_bytes），留 writer 需要的欄。"""
    return {k: v for k, v in row.items() if not k.startswith("_") or k == "_type"}


def target_days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def sync(engine: AnalysisEngine, families: list[Family], today: date, lookback_days: int,
         publish_cache: dict, deadline: Optional[float] = None) -> list[dict]:
    """collector 每輪：近 lookback_days 天（且 ≥ EPOCH）每個 family 每日確保 state 並發布有變動的圖。"""
    import time as _t
    rows: list[dict] = []
    start = max(engine.epoch, today - timedelta(days=lookback_days))
    for family in families:
        for d in target_days(start, today - timedelta(days=1)):
            if deadline is not None and _t.monotonic() > deadline:
                engine.warn(f"{engine.log} 時間預算用完，剩下的下輪再做")
                return rows
            if not engine.eligible(family, d, today):
                continue
            rows.extend(engine.publish_day(family, d, publish_cache=publish_cache))
    return rows
