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
import re
import shutil
import time
from pathlib import Path
from typing import Optional

import config
from sound_atlas import aviation, daily, live, natural, publish, radar
from sound_atlas.common import TPE, RawSource, contract_json, dumps_compact, grid_bbox, natural_static, now_tpe_iso, shift, slot_of

log = logging.getLogger(__name__)

TASK_LIVE = "sound_atlas_live"
TASK_DAILY = "sound_atlas_daily"
KEEP_LOCAL_DAYS = 3   # state／spool／本機 dry-run 輸出只留最近幾天（正本在 S3 原始檔；每日檔可重建）


def _root() -> Path:
    return Path(config.LOCAL_DATA_DIR) / "sound_atlas"


def _s3_fetch():
    """archive key → bytes|None（S3 回補用；沒有 S3 設定就不回補）"""
    if not config.S3_BUCKET:
        return None
    from storage.s3 import S3Storage
    s3 = S3Storage()
    return s3.get_file


def _raw_source(with_s3: bool = True) -> RawSource:
    return RawSource(local_dir=str(config.LOCAL_DATA_DIR), archive_fetch=_s3_fetch() if with_s3 else None)


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
                 root: Optional[Path] = None, history: Optional[RawSource] = None):
        self.root = root or _root()
        self.sink = sink or publish.make_sink(config.SOUND_ATLAS_UPLOAD_ENABLED, str(self.root / "out"))
        # 河川基準要讀前 60 天（只在 S3）；即時本身只讀本機（今天、昨天都還在本機），S3 出錯不影響即時
        self.history = history or (inputs.raw if inputs else _raw_source(with_s3=True))
        self.inputs = inputs or live.LiveInputs(
            raw=_raw_source(with_s3=False),
            # 即時用短逾時：影像主機掛掉時一次執行最多卡約 25 s（每日段用預設 3 次 × 30 s）
            radar_fetcher=radar.FrameFetcher(base_url=config.SOUND_ATLAS_RADAR_BASE_URL,
                                             getter=lambda u: radar.http_get(u, retries=2, timeout=10)),
            positions=_positions if config.SOUND_ATLAS_AVIATION_LIVE_ENABLED else None,
        )
        self.decoder = radar.FrameDecoder()
        self._err_runs: dict[str, int] = {}     # 族 → 連續 error 次數（只看今天的片）
        self._alerting: set[str] = set()
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
            cache = natural.RiverRowCache(self.history)
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
            try:
                out[y] = self.run_day(y, now)
            except Exception as e:  # noqa: BLE001 — 昨天的收尾片失敗不擋今天
                log.warning("[sound_atlas] %s 收尾片失敗：%s", y, type(e).__name__)
                out[y] = {"error": type(e).__name__}
        out[today] = self.run_day(today, now)
        self._family_alerts(out[today].get("status") or {})
        self._prune(today)
        try:
            from utils import alerting
            alerting.mark_success(TASK_LIVE)
        except Exception:  # noqa: BLE001
            pass
        return out

    def _family_alerts(self, status: dict[str, str]) -> None:
        """某一族連續 SOUND_ATLAS_FAMILY_ERROR_ALERT_RUNS 次 error（或 partial：部分幀解碼失敗）→ 🔴 一次；之後 ok／no_new_data → 清除（送過才送恢復）"""
        from utils import alerting
        need = max(1, config.SOUND_ATLAS_FAMILY_ERROR_ALERT_RUNS)
        for fam, st in status.items():
            key = f"sound_atlas_live:{fam}"
            if st in ("error", "partial"):
                n = self._err_runs[fam] = self._err_runs.get(fam, 0) + 1
                if n >= need and fam not in self._alerting:
                    alerting.raise_alert(key, alerting.RED, f"城市聲景即時：{fam} 連續 {n} 次失敗（約 {n * 10} 分鐘）")
                    self._alerting.add(fam)
            elif st in ("ok", "no_new_data"):
                self._err_runs[fam] = 0
                if fam in self._alerting:
                    alerting.clear_alert(key, f"城市聲景即時：{fam}")
                    self._alerting.discard(fam)

    def _prune(self, today: str) -> None:
        cutoff = shift(today, -KEEP_LOCAL_DAYS)
        st = self.root / "state"
        for p in list(st.glob("*")) if st.exists() else []:
            m = re.search(r"\d{4}-\d{2}-\d{2}", p.name)
            if m and m.group(0) < cutoff:
                shutil.rmtree(p) if p.is_dir() else p.unlink()
        if isinstance(self.sink, publish.LocalDirSink):
            for sub in ("live", "daily"):
                base = self.root / "out" / publish.PREFIX / sub
                for p in list(base.iterdir()) if base.exists() else []:
                    if p.name < cutoff:
                        shutil.rmtree(p)

    def run_day(self, date: str, now: float) -> dict:
        try:
            return self._run_day(date, now)
        except Exception:
            # 記憶體裡的狀態可能已前進（例如上傳失敗）：丟掉，下次從上次存檔接續 → 這批資料下一槽重送
            self._days.pop(date, None)
            raise

    def _run_day(self, date: str, now: float) -> dict:
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
        # 小時片：本槽是該小時最後一槽，或之前有小時因為那次執行沒跑而漏了 → 補
        for h in range(0, k // 6 + (1 if k % 6 == 5 else 0)):
            if h in day.hours_published:
                continue
            shards = [json.loads((sp / f"slot-{j:03d}.json").read_bytes()) for j in range(h * 6, h * 6 + 6)
                      if (sp / f"slot-{j:03d}.json").exists()]
            if shards:
                self.sink.put_json(publish.live_hour_key(date, h), live.rollup(date, h, shards))
            day.hours_published.add(h)
        day.save(self._state_path(date))  # 發布成功後才存狀態：上傳失敗 → 下一槽重送（合併規則冪等）
        return {"slot": k, "bytes": size, "status": {n: s.get("status") for n, s in doc["sources"].items()},
                "timings_s": doc["timings_s"]}


_LIVE: Optional[SoundAtlasLive] = None


def run_live() -> dict:
    global _LIVE
    if _LIVE is None:
        _LIVE = SoundAtlasLive()
    return _LIVE.run()


def run_daily(date: Optional[str] = None, families: Optional[list[str]] = None, revise: bool = False) -> dict:
    """子行程進入點（utils.isolated_task）：重建臺灣日 date（預設昨天）的每日檔並發布。

    revise=True（人工重跑修正用）：已發布的檔改發下一個修訂號（-r2、-r3…），manifest 指向最新。
    """
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
    res = publish_daily(date, inputs, sink, families, revise=revise)
    if config.SOUND_ATLAS_UPLOAD_ENABLED:
        try:
            res["prune"] = publish.prune_remote(sink, _today(time.time()), config.SOUND_ATLAS_LIVE_RETENTION_DAYS,
                                                config.SOUND_ATLAS_DAILY_RETENTION_DAYS, config.SOUND_ATLAS_PRUNE_MAX_DATES)
        except Exception as e:  # noqa: BLE001 — 清理失敗不影響發布結果
            res["prune"] = {"error": type(e).__name__}
    return res


MAX_REV_PROBE = 20   # revise 時最多往後找幾個 rev 才放棄


def publish_daily(date: str, inputs: "daily.DailyInputs", sink: publish.Sink, families: Optional[list[str]] = None,
                  revise: bool = False) -> dict:
    """建每日檔並發布；manifest 只記真的發布出去的檔（重跑沿用前次紀錄）。

    檔名規則：首版用前端同名檔；revise=True 時已發布的檔改發 -r<n>（n＝前次＋1）。
    被暫擋的族補發時還沒有首版 → 直接發首版（同一套紀錄：manifest files[name].key 指向實際檔名）。
    """
    files, report = daily.build_all(date, inputs, set(families) if families else None)
    hold = daily.transient_gaps(files, report)
    mkey = publish.daily_key(date, "manifest.json")
    prev = sink.get_json(mkey) or {}
    published: dict = dict(prev.get("files") or {})   # 只記真的在 R2 上的檔（沿用前次紀錄）
    written = {}
    for name, body in files.items():
        if any(name.startswith(prefix) for prefix in hold):
            written[name] = "held: " + hold[next(p for p in hold if name.startswith(p))]
            continue
        kind = "bin" if name.endswith(".bin") else "json"
        cur = published.get(name)
        if cur is None and sink.exists(publish.daily_key(date, name)):
            cur = published[name] = {"key": name, "rev": 1, "bytes": None, "sha256": None, "note": "已存在、無前次紀錄"}
        if cur is not None and not revise:
            written[name] = "exists (kept earlier build)"
            continue
        rev = 1 if cur is None else int(cur.get("rev", 1)) + 1
        sha = hashlib.sha256(body).hexdigest()
        # 目標 rev 的物件可能已上傳但 manifest 沒存到（中途失敗）：內容相符就採用，不符就試下一個 rev，
        # 否則會永遠卡在同一個 rev（put 回 False、manifest 不前進）
        for _ in range(MAX_REV_PROBE):
            fname = publish.revision_name(name, date, rev)
            fkey = publish.daily_key(date, fname)
            if sink.put(fkey, body, kind):
                note = f"published {fname}"
                break
            existing = sink.get_body(fkey, kind)
            if existing is not None and hashlib.sha256(existing).hexdigest() == sha:
                note = f"published {fname} (already uploaded, adopted)"
                break
            rev += 1
        else:
            written[name] = f"exists {fname} (kept; no free revision within {MAX_REV_PROBE})"
            continue
        published[name] = {"key": fname, "rev": rev, "bytes": len(body), "sha256": sha, "published_at": now_tpe_iso()}
        written[name] = note
    still_held = {p: r for p, r in hold.items() if not any(n.startswith(p) for n in published)}
    manifest = {"schema": "pulse-sound-daily-manifest-v1", "date_taiwan": date, "updated_at": now_tpe_iso(),
                "contract": contract_json("manifest.json"), "files": published, "held": still_held,
                "report": report}
    # manifest 可覆寫（短快取）：重跑補上被擋下的族之後要更新
    sink.put_json(mkey, manifest, immutable=False)
    if report["errors"] or still_held:
        log.warning("[sound_atlas] 每日 %s 未完整：errors=%s held=%s", date, list(report["errors"]), list(still_held))
    return {"date": date, "sink": publish.describe(sink), "written": written, "errors": report["errors"],
            "held": still_held, "timings_s": report["timings_s"]}


ALERT_DAILY = "sound_atlas_daily"


def daily_alert(result: Optional[dict], error: Optional[str] = None) -> None:
    """每日段失敗、或有族出錯／被暫擋 → 🔴 一次（alerting 去重）；完整發布 → 清除（送過才送恢復）。"""
    from utils import alerting
    problems = []
    if error:
        problems.append(f"任務中止：{error}")
    if result:
        problems += [f"{k} 失敗" for k in (result.get("errors") or {})]
        problems += [f"{p.rstrip('-')} 暫擋（{why}）" for p, why in (result.get("held") or {}).items()]
    date = (result or {}).get("date", "")
    alerting.record_routine(TASK_DAILY, not problems, "；".join(problems)[:120] if problems else f"{date} 已發布")
    if problems:
        alerting.raise_alert(ALERT_DAILY, alerting.RED, f"城市聲景每日檔 {date} 未完整：" + "；".join(problems))
    else:
        alerting.clear_alert(ALERT_DAILY, "城市聲景每日檔")


def run_daily_isolated() -> dict:
    from utils.isolated_task import run_isolated
    try:
        res = run_isolated(TASK_DAILY, "tasks.sound_atlas:run_daily", timeout=config.SOUND_ATLAS_DAILY_KILL_TIMEOUT_SEC)
    except Exception as e:
        daily_alert(None, type(e).__name__)
        raise
    daily_alert(res)
    return res


def live_schedule_minutes() -> list[int]:
    off = config.SOUND_ATLAS_LIVE_MINUTE_OFFSET % 10
    return [m * 10 + off for m in range(6)]
