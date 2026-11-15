"""城市聲景排程任務（設計見 docs/sound-atlas-live/DESIGN.md）。

- 即時（SOUND_ATLAS_LIVE_ENABLED）：每 10 分在 :x9 跑一次，產生當天的 slot 分片；每整點收尾時再產一份 hour 合併片。
  每天第一次跑時先發布河川基準檔（前 60 天分位數）。00:00–01:00 另替前一天跑收尾片（slot 144–149）。
- 每日（SOUND_ATLAS_DAILY_ENABLED）：每天 02:40 在子行程重建前一天的每日檔（正本）。

兩者預設只寫本機 <LOCAL_DATA_DIR>/sound_atlas/out/（dry-run）；SOUND_ATLAS_UPLOAD_ENABLED=true 才上傳 R2。
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Optional

import config
from sound_atlas import aviation, daily, live, natural, publish, radar
from sound_atlas.common import TPE, RawSource, contract_json, dumps_compact, grid_bbox, natural_static, shift, slot_of

log = logging.getLogger(__name__)

TASK_LIVE = "sound_atlas_live"
TASK_DAILY = "sound_atlas_daily"


def _root() -> Path:
    return Path(config.LOCAL_DATA_DIR) / "sound_atlas"


def _s3_fetch():
    """archive key → bytes|None（S3 回補用；沒有 S3 設定就不回補）"""
    if not config.S3_BUCKET:
        return None
    from storage.s3 import S3Storage
    s3 = S3Storage()
    return s3.get_file


def _raw_source() -> RawSource:
    return RawSource(local_dir=str(config.LOCAL_DATA_DIR), archive_fetch=_s3_fetch())


def _static() -> Optional[dict]:
    """coast＋river_network 靜態檔：本機快取 → 公開網址（驗 sha256 後才快取）"""
    d = _root() / "static"
    got = natural_static(str(d))
    if got is not None:
        return got
    meta = contract_json("manifest.json").get("natural_static") or {}
    sha = meta.get("sha256")
    if not sha:
        return None
    url = f"https://data.itsmigu.com/{publish.static_key('natural-static-v1.json', sha)}"
    st, body = radar.http_get(url)
    if st != "ok" or body is None or hashlib.sha256(body).hexdigest() != sha:
        log.warning("[sound_atlas] 靜態幾何取不到或 sha 不符（%s）：自然族 coast／river_network 缺", st)
        return None
    d.mkdir(parents=True, exist_ok=True)
    (d / "natural-static-v1.json").write_bytes(body)
    return json.loads(body)


def _positions(t0: int, t1: int) -> Optional[list[dict]]:
    """live.flight_positions 唯讀一個時間窗（有界：collected_at 窗＋LIMIT）；失敗回 None（飛機族本片 error）"""
    if not config.SUPABASE_DB_URL:
        return None
    from storage.db import connect_supabase
    conn = None
    try:
        conn = connect_supabase(autocommit=True)
        with conn.cursor() as cur:
            cur.execute("BEGIN READ ONLY")
            cur.execute("SET LOCAL statement_timeout = '20s'")
            cur.execute(aviation.POSITIONS_SQL, {"t0": t0, "t1": t1, "limit": aviation.POSITIONS_LIMIT})
            cols = [c.name for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute("COMMIT")
        for r in rows:
            r["lat"], r["lng"] = float(r["lat"]), float(r["lng"])
        if len(rows) >= aviation.POSITIONS_LIMIT:
            log.warning("[sound_atlas] flight_positions 窗 %s–%s 達上限 %s 列（截斷）", t0, t1, aviation.POSITIONS_LIMIT)
        return rows
    except Exception as e:  # noqa: BLE001
        log.warning("[sound_atlas] flight_positions 讀取失敗：%s", type(e).__name__)
        return None
    finally:
        if conn is not None:
            conn.close()


def _today(now: float) -> str:
    return dt.datetime.fromtimestamp(now, TPE).date().isoformat()


class SoundAtlasLive:
    """主程序內常駐（輕量：模擬實測每次 0.2 s 平均、浪解析那次約 0.4–0.9 s）"""

    def __init__(self, sink: Optional[publish.Sink] = None, inputs: Optional[live.LiveInputs] = None,
                 root: Optional[Path] = None):
        self.root = root or _root()
        self.sink = sink or publish.make_sink(config.SOUND_ATLAS_UPLOAD_ENABLED, str(self.root / "out"))
        self.inputs = inputs or live.LiveInputs(
            raw=_raw_source(),
            radar_fetcher=radar.FrameFetcher(base_url=config.SOUND_ATLAS_RADAR_BASE_URL),
            positions=_positions if config.SOUND_ATLAS_AVIATION_LIVE_ENABLED else None,
        )
        self.decoder = radar.FrameDecoder()
        self._days: dict[str, live.LiveDay] = {}
        self._baseline: dict[str, dict] = {}

    # ── 狀態 ──
    def _state_path(self, date: str) -> Path:
        return self.root / "state" / f"live-{date}.pkl"

    def _spool(self, date: str) -> Path:
        return self.root / "state" / f"spool-{date}"

    def _day(self, date: str) -> live.LiveDay:
        if date not in self._days:
            self._days[date] = live.LiveDay.load(self._state_path(date)) or live.LiveDay(date)
        for old in [d for d in self._days if d < shift(date, -1)]:
            del self._days[old]
        return self._days[date]

    def _baseline_doc(self, date: str) -> dict:
        if date in self._baseline:
            return self._baseline[date]
        p = self.root / "state" / f"river-baseline-{date}.json"
        if p.exists():
            doc = json.loads(p.read_bytes())
        else:
            cache = natural.RiverRowCache(self.inputs.raw)
            doc = live.river_baseline_doc(date, grid_bbox(), cache.get)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(dumps_compact(doc))
        self.sink.put_json(publish.live_baseline_key(date), doc)
        self._baseline = {date: doc, **{d: v for d, v in self._baseline.items() if d >= shift(date, -1)}}
        return doc

    # ── 一次執行 ──
    def run(self, now: Optional[float] = None) -> dict:
        now = time.time() if now is None else now
        today = _today(now)
        out = {}
        y = shift(today, -1)
        if slot_of(y, now) < 144 + live.TAIL_SLOTS:
            out[y] = self.run_day(y, now)
        out[today] = self.run_day(today, now)
        try:
            from utils import alerting
            alerting.mark_success(TASK_LIVE)
        except Exception:  # noqa: BLE001
            pass
        return out

    def run_day(self, date: str, now: float) -> dict:
        day = self._day(date)
        k = min(max(slot_of(date, now), 0), 143 + live.TAIL_SLOTS)
        if k in day.slots_published or self.sink.exists(publish.live_slot_key(date, k)):
            day.slots_published.add(k)
            return {"slot": k, "skipped": "slot 已發布（同槽重跑或重啟；新資料留給下一槽）"}
        try:
            base = self._baseline_doc(date)
            base_ids = {s["id"] for s in base["stations"]}
        except Exception as e:  # noqa: BLE001 — 基準檔失敗：河川族本日 error，其他族照跑
            log.warning("[sound_atlas] 河川基準檔失敗：%s", type(e).__name__)
            base_ids = set()
        doc = live.run_slot(day, self.inputs, now, base_ids, self.decoder)
        if not base_ids:
            doc["sources"]["river"] = {"status": "error", "error": "river baseline unavailable"}
        size = self.sink.put_json(publish.live_slot_key(date, k), doc)
        sp = self._spool(date)
        sp.mkdir(parents=True, exist_ok=True)
        (sp / f"slot-{k:03d}.json").write_bytes(dumps_compact(doc))
        if k % 6 == 5:
            shards = [json.loads((sp / f"slot-{j:03d}.json").read_bytes()) for j in range(k - 5, k + 1)
                      if (sp / f"slot-{j:03d}.json").exists()]
            self.sink.put_json(publish.live_hour_key(date, k // 6), live.rollup(date, k // 6, shards))
        day.save(self._state_path(date))  # 發布成功後才存狀態：上傳失敗 → 下一槽重送（合併規則冪等）
        return {"slot": k, "bytes": size, "status": {n: s.get("status") for n, s in doc["sources"].items()},
                "timings_s": doc["timings_s"]}


_LIVE: Optional[SoundAtlasLive] = None


def run_live() -> dict:
    global _LIVE
    if _LIVE is None:
        _LIVE = SoundAtlasLive()
    return _LIVE.run()


def run_daily(date: Optional[str] = None, families: Optional[list[str]] = None) -> dict:
    """子行程進入點（utils.isolated_task）：重建臺灣日 date（預設昨天）的每日檔並發布。"""
    date = date or shift(_today(time.time()), -1)
    root = _root()
    sink = publish.make_sink(config.SOUND_ATLAS_UPLOAD_ENABLED, str(root / "out"))
    s3 = None
    if config.S3_BUCKET:
        from storage.s3 import S3Storage
        s3 = S3Storage()

    def trails(d: str):
        if s3 is None:
            return None
        b = s3.get_file(f"trails/flights/{d}.json.gz")
        return aviation.trails_from_gz(b) if b else None

    inputs = daily.DailyInputs(raw=RawSource(local_dir=str(config.LOCAL_DATA_DIR), archive_fetch=s3.get_file if s3 else None),
                               radar_fetcher=radar.FrameFetcher(base_url=config.SOUND_ATLAS_RADAR_BASE_URL),
                               trails=trails, static=_static())
    files, report = daily.build_all(date, inputs, set(families) if families else None)
    written = {}
    for name, body in files.items():
        kind = "bin" if name.endswith(".bin") else "json"
        written[name] = sink.put(publish.daily_key(date, name), body, kind)
    manifest = {"schema": "pulse-sound-daily-manifest-v1", "date_taiwan": date,
                "contract": contract_json("manifest.json"),
                "files": {n: {"bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()} for n, b in files.items()},
                "report": report}
    sink.put_json(publish.daily_key(date, "manifest.json"), manifest)
    return {"date": date, "sink": publish.describe(sink), "written": written, "errors": report["errors"],
            "timings_s": report["timings_s"]}


def run_daily_isolated() -> dict:
    from utils.isolated_task import run_isolated
    return run_isolated(TASK_DAILY, "tasks.sound_atlas:run_daily", timeout=config.SOUND_ATLAS_DAILY_KILL_TIMEOUT_SEC)


def live_schedule_minutes() -> list[int]:
    off = config.SOUND_ATLAS_LIVE_MINUTE_OFFSET % 10
    return [m * 10 + off for m in range(6)]
