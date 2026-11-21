"""
英國 Met Office Land Observations 逐時測站觀測收集器（Weather DataHub，免費版每日 360 次）

來源：https://data.hub.api.metoffice.gov.uk/observation-land/1（header `apikey`，環境變數 METOFFICE_OBS_API_KEY）
  - GET /nearest?lat=&lon=  → [{"geohash":"gcvw5v","area":"Edinburgh","region":"dg","country":"Scotland",
                                "olson_time_zone":"Europe/London"}]
    ⚠ lat／lon 最多 2 位小數，3 位會回 400（2026-11-20 實測，官方文件沒寫）。
  - GET /{geohash}          → 該站近 48 筆逐時觀測 [{"datetime":"…Z","temperature":…,…}]；
    9 個參數：temperature、humidity、mslp、pressure_tendency（文字）、visibility、weather_code、
    wind_speed、wind_gust、wind_direction（16 方位文字）。站不同、參數不同（倫敦 gcpvj0 只有溫濕度）。

沒有「列出全部測站」的端點，所以收集器自己維護站表：
  1. 首次掃描：config/metoffice_obs_grid.json 的 581 個英國陸地網格點依序打 /nearest，累積 geohash
     （每點只查一次，同一 geohash 不重複記）。每輪最多 METOFFICE_OBS_DISCOVERY_PER_RUN 點。
  2. 掃完後每週重掃 METOFFICE_OBS_RESCAN_PER_WEEK 點（網格點加半格偏移，輪流），補漏站與新站。
  3. 量測：每個已知站每 UTC 日打 1 次 /{geohash}（一次拿 48 小時，重疊部分由 DB DO NOTHING 去重）。
     各站依 geohash 雜湊分到 0–23 時的時段，平均分散在一天中，表的新鮮度因此每小時都會前進。

每日呼叫上限（硬性守門，fail closed）：
  - 每 UTC 日 ≤ METOFFICE_OBS_DAILY_CAP（預設 330；免費額度 360，留 30 給人工）。
  - 計數存在 LOCAL_DATA_DIR/state/metoffice_obs_quota.json；**每次呼叫前先 +1 並寫檔成功才打**，
    寫檔失敗就不打（不可能在不知情下超額）。錯誤回應（4xx／5xx／逾時）一樣算一次。
  - 探站一律先保留今天還沒抓的已知站所需次數，剩下的額度才給 /nearest。
  - 額度檔遺失時：若進度檔或 DB 站表已有資料（＝不是第一次部署），保守地把今天當作已用滿，隔天 UTC 0 點恢復；
    兩者都沒有才當第一次部署、從 0 開始。

進度檔（LOCAL_DATA_DIR/state/metoffice_obs_state.json）：站表、網格掃描進度、各站最後成功抓取日。
  只在 DB 寫入成功後才前進（run()，同 jma_warnings）。遺失時從 DB live.metoffice_obs_stations 重建站表，
  並直接視為首次掃描已完成（只靠每週重掃補站），不重新燒額度掃全網格。

寫入（gis-platform migration 562）：
  - live.metoffice_obs_measurements：自然鍵 (observed_at, geohash)，ON CONFLICT DO NOTHING；缺值 NULL 不填 0
  - live.metoffice_obs_stations：站表，geom＝geohash 格中心（**近似位置**，6 碼約 1.2 × 0.6 km，不是精確站址），有變才 UPDATE
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import requests

import config
from collectors.base import BaseCollector

API_BASE = "https://data.hub.api.metoffice.gov.uk/observation-land/1"
GRID_PATH = Path(__file__).resolve().parent.parent / "config" / "metoffice_obs_grid.json"

NUMERIC_FIELDS = ("temperature", "humidity", "mslp", "visibility", "wind_speed", "wind_gust")
INT_FIELDS = ("weather_code",)
TEXT_FIELDS = ("wind_direction", "pressure_tendency")
KNOWN_FIELDS = frozenset(("datetime",) + NUMERIC_FIELDS + INT_FIELDS + TEXT_FIELDS)
STATION_FIELDS = ("area", "region", "country", "olson_time_zone")

_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


HTTP_MAX_ATTEMPTS = 3   # 每個請求最多嘗試次數（逾時／連線錯誤／5xx）
HTTP_BACKOFF_SEC = 2.0  # 第 n 次失敗後等 n×2 秒


class QuotaExhausted(Exception):
    """今天的呼叫額度用完（或額度檔無法寫入，fail closed）。"""


def geohash_center(gh: str) -> Optional[tuple[float, float]]:
    """geohash → 格中心 (lon, lat)。非法字元回 None。"""
    if not gh:
        return None
    lat_lo, lat_hi, lon_lo, lon_hi = -90.0, 90.0, -180.0, 180.0
    even = True
    for ch in gh.lower():
        idx = _GEOHASH_BASE32.find(ch)
        if idx < 0:
            return None
        for bit in (16, 8, 4, 2, 1):
            on = bool(idx & bit)
            if even:
                mid = (lon_lo + lon_hi) / 2
                lon_lo, lon_hi = (mid, lon_hi) if on else (lon_lo, mid)
            else:
                mid = (lat_lo + lat_hi) / 2
                lat_lo, lat_hi = (mid, lat_hi) if on else (lat_lo, mid)
            even = not even
    return (lon_lo + lon_hi) / 2, (lat_lo + lat_hi) / 2


def station_slot(gh: str) -> int:
    """站的每日抓取時段（UTC 0–23），由 geohash 雜湊決定、跨重啟穩定。"""
    return int(hashlib.sha1(gh.encode()).hexdigest(), 16) % 24


def _num(v) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if (math.isnan(f) or math.isinf(f)) else f


def _int(v) -> Optional[int]:
    f = _num(v)
    return None if f is None or f != int(f) else int(f)


def _text(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _parse_ts(v) -> Optional[datetime]:
    if not v or not isinstance(v, str):
        return None
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_station(item: dict) -> Optional[dict]:
    """/nearest 的一筆 → live.metoffice_obs_stations 一列（座標＝geohash 格中心）。"""
    gh = _text((item or {}).get("geohash"))
    center = geohash_center(gh) if gh else None
    if not gh or center is None:
        return None
    return {"geohash": gh, **{f: _text(item.get(f)) for f in STATION_FIELDS},
            "lon": round(center[0], 6), "lat": round(center[1], 6)}


def normalize_obs(records: list, geohash: str, collected_at: str) -> tuple[list[dict], int]:
    """/{geohash} 回應 → 量測列。缺的參數存 NULL（不是 0）；沒見過的欄位放 extra。

    Returns: (rows, 沒有 datetime 而丟掉的筆數)
    """
    rows, bad = [], 0
    seen = set()
    for r in records or []:
        if not isinstance(r, dict):
            bad += 1
            continue
        ts = _parse_ts(r.get("datetime"))
        if ts is None:
            bad += 1
            continue
        key = ts.isoformat()
        if key in seen:
            continue
        seen.add(key)
        extra = {k: v for k, v in r.items() if k not in KNOWN_FIELDS}
        rows.append({
            "observed_at": key,
            "geohash": geohash,
            **{f: _num(r.get(f)) for f in NUMERIC_FIELDS},
            **{f: _int(r.get(f)) for f in INT_FIELDS},
            **{f: _text(r.get(f)) for f in TEXT_FIELDS},
            "extra": extra or None,
            "collected_at": collected_at,
        })
    return rows, bad


def load_grid(path: Path = GRID_PATH) -> list[tuple[float, float]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [(float(lon), float(lat)) for lon, lat in data["points"]]


def rescan_point(pt: tuple[float, float]) -> tuple[float, float]:
    """每週重掃用：網格點加半格偏移（補首次掃描漏掉的密集區測站）。"""
    return pt[0] + 0.165, pt[1] + 0.1


class MetofficeObsCollector(BaseCollector):
    """Met Office 英國陸地測站逐時觀測（約 150 站），每日呼叫硬上限。"""

    name = "metoffice_obs"
    interval_minutes = config.METOFFICE_OBS_INTERVAL
    heartbeat_without_writer = True  # 沒事做的輪次（額度用完、這小時沒有站到時段）也要有成功心跳
    COLLECT_TIMEOUT = 900

    def __init__(self, sleep: Callable[[float], None] = time.sleep,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        super().__init__()
        self._sleep = sleep
        self._now = now
        self._last_call = 0.0
        self.daily_cap = config.METOFFICE_OBS_DAILY_CAP
        self.discovery_per_run = config.METOFFICE_OBS_DISCOVERY_PER_RUN
        self.rescan_per_week = config.METOFFICE_OBS_RESCAN_PER_WEEK
        self.min_interval = config.METOFFICE_OBS_MIN_INTERVAL_SEC
        state_dir = Path(config.LOCAL_DATA_DIR) / "state"
        self.quota_path = state_dir / "metoffice_obs_quota.json"
        self.state_path = state_dir / "metoffice_obs_state.json"
        self.grid = load_grid()
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "GIS-DataCollectors/1.0 (metoffice_obs)",
                                      "Accept": "application/json"})
        if config.METOFFICE_OBS_API_KEY:
            self._session.headers["apikey"] = config.METOFFICE_OBS_API_KEY

    def require_db_write(self) -> bool:
        # 「今天已抓過」只在 DB 寫入成功後記下；寫入失敗要讓本輪失敗，下輪重抓
        return True

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_json(self.state_path, self._pending_state)
            self._state = self._pending_state
        self._pending_state = None
        return stats

    # ── 檔案 ────────────────────────────────────────────
    @staticmethod
    def _read_json(path: Path) -> Optional[dict]:
        try:
            d = json.loads(path.read_text())
            return d if isinstance(d, dict) else None
        except (OSError, ValueError):
            return None

    @staticmethod
    def _save_json(path: Path, data: dict) -> None:
        """原子寫入；失敗丟 OSError（呼叫端決定要不要擋）。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False))
        tmp.replace(path)

    # ── 額度 ────────────────────────────────────────────
    def _load_quota(self, today: str, not_first_deploy: bool) -> dict:
        q = self._read_json(self.quota_path)
        if q is None or not isinstance(q.get("calls"), int) or not q.get("day"):
            # 額度檔遺失：不是第一次部署就保守當作今天已用滿（fail closed）
            q = {"day": today, "calls": self.daily_cap if not_first_deploy else 0,
                 "reset_reason": "quota_file_missing" if not_first_deploy else "first_run"}
            self._save_json(self.quota_path, q)  # 寫不進去 → OSError → 本輪失敗、不打任何 API
        if q["day"] != today:
            q = {"day": today, "calls": 0}
            self._save_json(self.quota_path, q)
        return q

    def _spend(self) -> None:
        """呼叫前先記一次；超過上限或寫檔失敗就不打。

        排程器本身 skip-if-running 不會疊跑；仍以檔案鎖＋重讀額度檔，讓容器內手動跑一輪時也不會少算。
        """
        try:
            self.quota_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.quota_path.with_suffix(".lock"), "w") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                on_disk = self._read_json(self.quota_path) or {}
                q = self._quota
                today = self._now().date().isoformat()
                if q["day"] != today:
                    # 跨 UTC 午夜：改記新的一天（優先採磁碟上新日的計數），不再記在舊日期
                    q = {"day": today, "calls": 0}
                    if on_disk.get("day") == today and isinstance(on_disk.get("calls"), int):
                        q["calls"] = on_disk["calls"]
                    self._quota = q
                if on_disk.get("day") == q["day"] and isinstance(on_disk.get("calls"), int):
                    q["calls"] = max(q["calls"], on_disk["calls"])
                if q["calls"] >= self.daily_cap:
                    raise QuotaExhausted(f"今日額度 {q['calls']}/{self.daily_cap} 已用完")
                q["calls"] += 1
                try:
                    self._save_json(self.quota_path, q)
                except OSError:
                    q["calls"] -= 1
                    raise
        except QuotaExhausted:
            raise
        except OSError as e:
            raise QuotaExhausted(f"額度檔讀寫失敗，停止呼叫（fail closed）: {e}") from e
        self.calls += 1

    def _remaining(self) -> int:
        return self.daily_cap - self._quota["calls"]

    # ── HTTP ────────────────────────────────────────────
    def _get(self, path: str, params: Optional[dict] = None):
        """節流 → 記額度 → 打 API；逾時／連線錯誤／5xx 有界重試（每次嘗試都過額度守門）。"""
        last_exc: Optional[Exception] = None
        for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
            wait = self.min_interval - (time.monotonic() - self._last_call)
            if wait > 0:
                self._sleep(wait)  # 先等再記額度，跨午夜時記到實際送出那天
            self._spend()
            self._last_call = time.monotonic()
            try:
                resp = self._session.get(f"{API_BASE}{path}", params=params, timeout=config.REQUEST_TIMEOUT)
            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
            else:
                if resp.status_code == 429:
                    # 被限流就整輪停，不再把額度燒在注定失敗的呼叫上
                    raise QuotaExhausted("上游回 429（限流／額度用完），本輪停止")
                if resp.status_code < 500:
                    resp.raise_for_status()
                    return resp.json()
                try:
                    resp.raise_for_status()
                except requests.HTTPError as e:
                    last_exc = e
            if attempt < HTTP_MAX_ATTEMPTS:
                self._sleep(HTTP_BACKOFF_SEC * attempt)
        raise last_exc  # type: ignore[misc]

    # ── 進度 ────────────────────────────────────────────
    def _db_stations(self) -> Optional[dict]:
        """DB 站表（唯讀），進度檔遺失時重建用。無 writer 回 None。"""
        writer = getattr(self, "supabase_writer", None)
        if writer is None:
            return None
        with writer.with_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT geohash, area, region, country, olson_time_zone, lon, lat "
                            "FROM live.metoffice_obs_stations")
                cols = [d[0] for d in cur.description]
                return {r[0]: dict(zip(cols, r)) for r in cur.fetchall()}

    def _load_state(self) -> tuple[dict, bool]:
        """回傳 (state, not_first_deploy)。"""
        st = self._state or self._read_json(self.state_path)
        if st is not None and isinstance(st.get("stations"), dict):
            return st, True
        stations = self._db_stations() or {}
        st = {"stations": stations, "grid_next": len(self.grid) if stations else 0,
              "fetched": {}, "rescan": {"week": None, "count": 0, "cursor": 0}}
        if stations:
            print(f"[{self.name}] 進度檔遺失 → 從 DB 重建站表 {len(stations)} 站，首次掃描視為完成")
        return st, bool(stations)

    # ── 主流程 ──────────────────────────────────────────
    def collect(self) -> dict:
        if not config.METOFFICE_OBS_API_KEY:
            raise RuntimeError("METOFFICE_OBS_API_KEY 未設定")
        now = self._now()
        today = now.date().isoformat()
        iso = now.isocalendar()
        week = f"{iso[0]}-W{iso[1]:02d}"
        collected_at = now.isoformat()
        self.calls = 0

        prev, not_first = self._load_state()
        self._quota = self._load_quota(today, not_first)

        st = json.loads(json.dumps(prev))  # 深拷貝，成功才換掉
        stations: dict = st["stations"]
        fetched: dict = {g: d for g, d in st.get("fetched", {}).items() if g in stations}
        st["fetched"] = fetched
        rescan = st.setdefault("rescan", {"week": None, "count": 0, "cursor": 0})
        if rescan.get("week") != week:
            rescan.update(week=week, count=0)

        rows: list[dict] = []
        new_stations: list[dict] = []
        obs_ok = obs_failed = nearest_ok = nearest_failed = bad_records = 0
        quota_hit = False

        def due() -> list[str]:
            return sorted(g for g in stations
                          if fetched.get(g) != today and station_slot(g) <= now.hour)

        def unfetched_today() -> int:
            return sum(1 for g in stations if fetched.get(g) != today)

        def fetch_obs(gs: list[str]) -> None:
            nonlocal obs_ok, obs_failed, bad_records
            for g in gs:
                try:
                    body = self._get(f"/{g}")
                except QuotaExhausted:
                    raise
                except Exception as e:  # 單站失敗不擋整輪；今天仍算已試過（不重燒額度）
                    obs_failed += 1
                    fetched[g] = today
                    print(f"[{self.name}]   ⚠ {g} 觀測失敗: {type(e).__name__}")
                    continue
                r, bad = normalize_obs(body if isinstance(body, list) else [], g, collected_at)
                rows.extend(r)
                bad_records += bad
                obs_ok += 1
                fetched[g] = today

        try:
            # 1. 到時段的已知站
            fetch_obs(due())

            # 2. 探站：首次掃描或每週重掃；永遠先保留今天其餘已知站需要的次數
            for _ in range(self.discovery_per_run):
                if self._remaining() - unfetched_today() < 1:
                    break
                prev_progress = (st["grid_next"], rescan["cursor"], rescan["count"])
                if st["grid_next"] < len(self.grid):
                    lon, lat = self.grid[st["grid_next"]]
                    st["grid_next"] += 1
                elif rescan["count"] < self.rescan_per_week and self.grid:
                    lon, lat = rescan_point(self.grid[rescan["cursor"] % len(self.grid)])
                    rescan["cursor"] = (rescan["cursor"] + 1) % len(self.grid)
                    rescan["count"] += 1
                else:
                    break
                try:
                    body = self._get("/nearest", {"lat": f"{lat:.2f}", "lon": f"{lon:.2f}"})
                except QuotaExhausted:
                    # 沒真的查到就不算掃過：還原進度，下輪從同一點重來
                    st["grid_next"], rescan["cursor"], rescan["count"] = prev_progress
                    raise
                except Exception as e:
                    nearest_failed += 1
                    print(f"[{self.name}]   ⚠ nearest({lat:.2f},{lon:.2f}) 失敗: {type(e).__name__}")
                    continue
                nearest_ok += 1
                for item in body if isinstance(body, list) else []:
                    s = normalize_station(item)
                    if s and s["geohash"] not in stations:
                        stations[s["geohash"]] = {k: v for k, v in s.items() if k != "geohash"}
                        new_stations.append(s)

            # 3. 新站若已過時段，本輪就抓
            fetch_obs(due())
        except QuotaExhausted as e:
            quota_hit = True
            print(f"[{self.name}] ⏸ {e}")

        attempted = obs_ok + obs_failed + nearest_ok + nearest_failed
        if attempted and not (obs_ok or nearest_ok):
            raise RuntimeError(f"Met Office 觀測 API 本輪全部失敗（{attempted} 次）")

        if new_stations and self.supabase_writer:
            self.supabase_writer._upsert_metoffice_obs_stations(new_stations)

        self._pending_state = st
        grid_done = st["grid_next"] >= len(self.grid)
        summary = {
            "fetch_time": collected_at,
            "api_calls": self.calls,
            "api_calls_today": self._quota["calls"],
            "daily_cap": self.daily_cap,
            "quota_exhausted": quota_hit,
            "stations_known": len(stations),
            "stations_new": len(new_stations),
            "stations_fetched": obs_ok,
            "stations_failed": obs_failed,
            "stations_unfetched_today": unfetched_today(),
            "grid_progress": f"{min(st['grid_next'], len(self.grid))}/{len(self.grid)}",
            "grid_done": grid_done,
            "rescan_this_week": rescan["count"],
            "nearest_calls": nearest_ok + nearest_failed,
            "bad_records": bad_records,
            "total_readings": len(rows),
        }
        print(f"[{self.name}]   ✓ 站 {len(stations)}（新 {len(new_stations)}），抓 {obs_ok} 站／失敗 {obs_failed}，"
              f"nearest {nearest_ok + nearest_failed} 次，網格 {summary['grid_progress']}；"
              f"{len(rows)} 筆；今日 API {self._quota['calls']}/{self.daily_cap}")
        if not attempted:
            return {**summary, "no_work": True}
        # S3 歸檔正本：站表（近似座標）＋本輪量測
        return {**summary,
                "stations": [{"geohash": g, **v} for g, v in sorted(stations.items())],
                "data": rows}
