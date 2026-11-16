#!/usr/bin/env python3
"""即時 119 案件＋警廣路況 runner — HiCloud VM 專用（cron 每分鐘觸發一次，見 run_fire119.sh）。

一輪的流程：
  1. 保護：讀 /proc/meminfo，MemAvailable < MIN_MEM_AVAILABLE_MB 就跳過本輪；
     上一輪留下「開始但沒結束」的標記 → 記一次逾時；連續 3 次自動降頻（間隔 ×2，最多 ×4）並寫告警標記。
  2. 排程：依 fire119_parse.SOURCES 的 interval／phase 挑到期來源（1 分鐘的先抓）；
     03:00–03:25 歸檔窗只抓 1 分鐘來源；超過 SOFT_DEADLINE_SEC 就把剩下的留到下一輪。
     單執行緒、來源間隔 ≥ SOURCE_GAP_SEC 秒、timeout 20 秒、一般瀏覽器 UA。
  3. 每個來源：抓 → 隱私剔除 → 原始回應 gzip 落地 RAW_DIR（只留 2–3 天、不上傳）→ 解析 → 組列＋逐列驗證。
  4. 只把「新案件或內容／狀態／分隊有變」的列寫 DB（live.fire119_incidents、live.pbs_road_events）；
     last_seen_at 每 LAST_SEEN_FLUSH_MIN 分鐘批次更新一次。DB 失敗 → vm_buffer 存檔、下輪補寫。
  5. 解析後的列寫 DATA_DIR/<name>/YYYY/MM/DD/<name>_HHMM.json，archive_fire119.py 每日打包上 S3。
  6. 寫 STATE_DIR/source_health.json（health_report 07:00 讀，日報紅黃燈）與 last_round.txt（第一天監測用）。

每輪恰好印一行「本輪結束 …」（或 FAILED／記憶體不足跳過），health_report 用它算輪數與成功率。

用法：
  python3 fire119_collect.py                 # 正式（cron）
  python3 fire119_collect.py --dry-run       # 不連 DB，寫入內容輸出成本機 JSONL
  python3 fire119_collect.py --dry-run --all # 忽略排程，所有來源各抓一次（本機驗證用）
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
for _p in (APP_DIR, APP_DIR.parent / "vm_common"):  # VM 上 vm_buffer.py／bulk.py 跟本檔同目錄
    if str(_p) not in sys.path:
        sys.path.append(str(_p))

import fire119_parse as P  # noqa: E402
import fire119_store as S  # noqa: E402

log = logging.getLogger("fire119")

DEFAULTS = dict(
    DATA_DIR="/var/lib/fire119/data",
    STATE_DIR="/var/lib/fire119/state",
    RAW_DIR="/var/lib/fire119/raw",
    GAZETTEER_PATH=str(APP_DIR / "road_gazetteer.sqlite"),
    MIN_MEM_AVAILABLE_MB="150",
    SOFT_DEADLINE_SEC="45",
    SOURCE_GAP_SEC="2",
    HTTP_TIMEOUT_SEC="20",
    LAST_SEEN_FLUSH_MIN="30",
    ARCHIVE_WINDOW="03:00-03:25",
    OVERRUN_SEC="75",
    FIRE119_DISABLED="",
)
DEGRADE_AFTER = 3        # 連續幾輪逾時就降頻
RECOVER_AFTER = 60       # 降頻後連續幾輪正常就回升一級
MAX_DEGRADE_LEVEL = 2    # 間隔最多 ×4
HEALTH_WINDOW_H = 24


def cfg(name: str) -> str:
    return os.environ.get(name, DEFAULTS[name])


# ── 保護 ──────────────────────────────────────────────────────────────
def mem_available_mb(path: str = "/proc/meminfo") -> int | None:
    """MemAvailable（MB）；讀不到（非 Linux）回 None＝不檢查。"""
    try:
        with open(path, encoding="ascii") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def peak_rss_mb() -> float | None:
    try:
        import resource
        v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(v / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)
    except Exception:  # noqa: BLE001
        return None


def in_window(now: datetime, spec: str) -> bool:
    try:
        a, b = spec.split("-")
        lo, hi = a.strip(), b.strip()
    except ValueError:
        return False
    hm = now.strftime("%H:%M")
    return lo <= hm < hi


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, path)


# ── 排程 ──────────────────────────────────────────────────────────────
def effective_interval(src: dict, level: int) -> int:
    return int(src["interval"]) * (2 ** level)


def is_due(name: str, now: datetime, last_fetch: dict, level: int) -> bool:
    """phase 對到的那一分鐘到期；另外超過「間隔＋50 秒」沒抓（漏輪、被延後）也到期；剛抓過的不重抓。"""
    src = P.SOURCES[name]
    iv = effective_interval(src, level)
    last = last_fetch.get(name)
    if last:
        age = (now - datetime.fromisoformat(last)).total_seconds()
        if age < iv * 60 - 20:
            return False
        if age >= iv * 60 + 50:
            return True
    else:
        return True
    minute = int(now.timestamp() // 60)
    return minute % iv == src["phase"] % iv


def due_sources(now: datetime, last_fetch: dict, level: int, archive_window: str,
                disabled: set[str]) -> list[str]:
    names = [n for n in P.SOURCES if n not in disabled and is_due(n, now, last_fetch, level)]
    if in_window(now, archive_window):
        names = [n for n in names if P.SOURCES[n]["interval"] == 1]

    def overdue(n):
        last = last_fetch.get(n)
        return -(now - datetime.fromisoformat(last)).total_seconds() if last else -1e9
    return sorted(names, key=lambda n: (P.SOURCES[n]["interval"] != 1, overdue(n)))


# ── 抓取 ──────────────────────────────────────────────────────────────
def fetch(name: str, session, timeout: float) -> dict:
    """回 dict(ok, http, ms, bytes, content, error)；不丟例外。"""
    s = P.SOURCES[name]
    headers = {"User-Agent": P.UA, "Accept": "*/*", "Accept-Language": "zh-TW,zh;q=0.9", **s.get("headers", {})}
    t0 = time.monotonic()
    try:
        if s.get("method") == "POST":
            r = session.post(s["url"], headers=headers, data=s.get("body"), timeout=timeout)
        else:
            r = session.get(s["url"], headers=headers, timeout=timeout)
        ms = int((time.monotonic() - t0) * 1000)
        ok = r.status_code == 200
        return dict(ok=ok, http=r.status_code, ms=ms, bytes=len(r.content), content=r.content,
                    error=None if ok else f"HTTP {r.status_code}")
    except Exception as e:  # noqa: BLE001
        return dict(ok=False, http=None, ms=int((time.monotonic() - t0) * 1000), bytes=0, content=b"",
                    error=f"{type(e).__name__}: {str(e)[:120]}")


def save_raw(raw_dir: Path, name: str, content: bytes, now: datetime) -> Path:
    """已剔除隱私欄位的原始回應 → RAW_DIR/<source>/YYYYMMDD/HHMMSS.<ext>.gz（不在 DATA_DIR，歸檔不會打包）。"""
    d = raw_dir / name / now.strftime("%Y%m%d")
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{now.strftime('%H%M%S')}.{P.RAW_EXT.get(name, 'html')}.gz"
    with gzip.open(p, "wb", compresslevel=6) as f:
        f.write(content)
    return p


# ── 健康狀態 ───────────────────────────────────────────────────────────
def _bucket(now: datetime) -> str:
    return now.strftime("%Y%m%d%H")


def record_source(health: dict, name: str, now: datetime, ok: bool, rows: int | None, error: str | None) -> None:
    s = health.setdefault("sources", {}).setdefault(name, {})
    iso = now.isoformat(timespec="seconds")
    s["last_attempt_at"] = iso
    b = s.setdefault("hourly", {}).setdefault(_bucket(now), [0, 0, 0])  # [嘗試, 失敗, 0 筆]
    b[0] += 1
    if ok:
        s["last_ok_at"] = iso
        s["fail_streak"] = 0
        s.pop("first_fail_at", None)
        s["last_rows"] = rows
        if rows:
            s["last_nonzero_at"] = iso
            s.pop("zero_since", None)
        else:
            b[2] += 1
            s.setdefault("zero_since", iso)
    else:
        b[1] += 1
        if not s.get("fail_streak"):
            s["first_fail_at"] = iso
        s["fail_streak"] = int(s.get("fail_streak") or 0) + 1
        s["last_error"] = (error or "")[:200]
        s["last_error_at"] = iso


def record_runner_event(health: dict, now: datetime, kind: str) -> None:
    r = health.setdefault("runner", {})
    b = r.setdefault("hourly", {}).setdefault(_bucket(now), {})
    b[kind] = b.get(kind, 0) + 1


def _prune_hourly(hourly: dict, now: datetime) -> dict:
    cutoff = _bucket(now - timedelta(hours=HEALTH_WINDOW_H))
    return {k: v for k, v in hourly.items() if k > cutoff}


def evaluate_health(health: dict, now: datetime, disabled: set[str] = frozenset(),
                    gazetteer_ok: bool = True) -> tuple[list[str], list[str]]:
    """回 (alerts 紅, warnings 黃)。

    紅：某來源 ≥ 6 小時沒有一次成功、runner 降頻中。
    黃：解析／HTTP 失敗（24h 失敗率 ≥ 10% 或目前連續失敗）、解析成功但 0 筆超過該來源門檻、
        記憶體不足跳過、逾時、路名對照表缺檔。宜蘭、澎湖、臺東常態 0 筆，不告警（zero_alert_hours=None）。
    """
    alerts: list[str] = []
    warnings: list[str] = []
    for name, src in P.SOURCES.items():
        if name in disabled:
            continue
        s = (health.get("sources") or {}).get(name)
        if not s:
            continue
        s["hourly"] = _prune_hourly(s.get("hourly") or {}, now)
        att = sum(v[0] for v in s["hourly"].values())
        fails = sum(v[1] for v in s["hourly"].values())
        last_ok = datetime.fromisoformat(s["last_ok_at"]) if s.get("last_ok_at") else None
        failing_h = 0.0
        if s.get("fail_streak"):
            ref = last_ok or datetime.fromisoformat(s.get("first_fail_at") or s["last_attempt_at"])
            failing_h = (now - ref).total_seconds() / 3600
        if s.get("fail_streak") and failing_h >= 6:
            alerts.append(f"{src['county'] or '警廣'}（{name}）HTTP／解析失敗已 {failing_h:.0f} 小時以上："
                          f"{s.get('last_error')}")
        elif s.get("fail_streak") or (att and fails / att >= 0.10):
            warnings.append(f"{src['county'] or '警廣'}（{name}）HTTP／解析失敗 24h {fails}/{att} 次，"
                            f"最後錯誤：{s.get('last_error')}")
        zh = src.get("zero_alert_hours")
        if zh and s.get("zero_since") and not s.get("fail_streak"):
            hours = (now - datetime.fromisoformat(s["zero_since"])).total_seconds() / 3600
            if hours >= zh:
                warnings.append(f"{src['county'] or '警廣'}（{name}）解析成功但連續 0 筆 {hours:.0f} 小時")
    r = health.setdefault("runner", {})
    r["hourly"] = _prune_hourly(r.get("hourly") or {}, now)
    totals: dict[str, int] = {}
    for v in r["hourly"].values():
        for k, n in v.items():
            totals[k] = totals.get(k, 0) + n
    if r.get("degrade_level"):
        alerts.append(f"runner 降頻中（間隔 ×{2 ** r['degrade_level']}）：連續逾時，見 /var/log/fire119/collect.log")
    if totals.get("mem_skip"):
        warnings.append(f"記憶體不足跳過 {totals['mem_skip']} 輪（24h）")
    if totals.get("overrun"):
        warnings.append(f"逾時／未正常結束 {totals['overrun']} 輪（24h）")
    if not gazetteer_ok:
        warnings.append("路名對照表 sqlite 不存在：路名級案件只能存 none（見 README 部署步驟）")
    return alerts, warnings


# ── DB ────────────────────────────────────────────────────────────────
def _db_write(payload: dict, last_seen: dict | None, buffer_dir: Path) -> tuple[str, dict]:
    """回 (db 狀態, 統計)。狀態：ok／buffered／fail。last_seen 失敗不影響本輪狀態（下次再推）。"""
    import psycopg2  # VM 才需要；dry-run／測試不 import
    from bulk import execute_json, execute_values
    from vm_buffer import connect_with_retry, flush_pending, save_batch

    db_url = os.environ.get("SUPABASE_DB_URL")
    stats: dict = {}
    conn = None
    try:
        if not db_url:
            raise RuntimeError("SUPABASE_DB_URL 未設定")
        conn = connect_with_retry(lambda: psycopg2.connect(db_url, connect_timeout=10),
                                  retries=1, wait_seconds=3, log=log)
        flush_pending(conn, buffer_dir, lambda c, p: S.write_payload(c, p, execute_values), log=log)
        if payload.get("fire119") or payload.get("pbs"):
            stats["written"] = S.write_payload(conn, payload, execute_values)
    except Exception as exc:  # noqa: BLE001
        log.error(f"DB 寫入失敗: {type(exc).__name__}: {str(exc)[:200]}")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            conn.close()
        if payload.get("fire119") or payload.get("pbs"):
            saved = save_batch(buffer_dir, "fire119", payload, log=log)
            return ("buffered" if saved else "fail"), stats
        return "fail", stats
    try:
        if last_seen:
            stats["last_seen"] = S.write_last_seen(conn, last_seen, execute_json)
    except Exception as exc:  # noqa: BLE001
        log.warning(f"last_seen 批次更新失敗（下次再推）: {type(exc).__name__}: {str(exc)[:200]}")
        stats["last_seen_error"] = True
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
    finally:
        conn.close()
    return "ok", stats


def _dry_write(out_dir: Path, now: datetime, payload: dict, last_seen: dict | None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"dryrun_{now:%Y%m%d}.jsonl").open("a", encoding="utf-8") as f:
        for kind in ("fire119", "pbs"):
            for r in payload.get(kind) or []:
                f.write(json.dumps({"kind": kind, "at": now.isoformat(timespec="seconds"), **r},
                                   ensure_ascii=False, default=str) + "\n")
        if last_seen:
            f.write(json.dumps({"kind": "last_seen", "at": now.isoformat(timespec="seconds"),
                                "counts": {k: len(v) for k, v in last_seen.items()}}, ensure_ascii=False) + "\n")


def write_snapshot(data_dir: Path, name: str, now: datetime, body: dict) -> Path:
    d = data_dir / name / now.strftime("%Y/%m/%d")
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{name}_{now.strftime('%H%M')}.json"
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(body, ensure_ascii=False, default=str), encoding="utf-8")
    os.replace(tmp, p)  # 原子寫入，歸檔不會打包到半個檔
    return p


# ── 主流程 ─────────────────────────────────────────────────────────────
def run_round(args, now: datetime | None = None, session=None, sleep=time.sleep, meminfo=mem_available_mb) -> int:
    t_start = time.monotonic()
    now = now or datetime.now(P.TZ)
    state_dir = Path(args.state_dir or cfg("STATE_DIR"))
    data_dir = Path(args.data_dir or cfg("DATA_DIR"))
    raw_dir = Path(cfg("RAW_DIR"))
    disabled = {x.strip() for x in cfg("FIRE119_DISABLED").split(",") if x.strip()}
    runner_path = state_dir / "runner.json"
    health_path = state_dir / "source_health.json"
    runner = _load_json(runner_path, {})
    health = _load_json(health_path, {})

    # 1) 記憶體門檻
    avail = meminfo()
    min_mb = int(cfg("MIN_MEM_AVAILABLE_MB"))
    if avail is not None and avail < min_mb:
        log.warning(f"記憶體不足（MemAvailable={avail} MB < {min_mb}），跳過本輪")
        record_runner_event(health, now, "mem_skip")
        health["updated_at"] = now.isoformat(timespec="seconds")
        _save_json(health_path, health)
        return 0

    # 2) 上一輪沒正常結束（被 timeout 砍掉、OOM）→ 記逾時；連續 DEGRADE_AFTER 次降頻
    level = int(runner.get("degrade_level") or 0)
    if runner.get("running"):
        runner["overrun_streak"] = int(runner.get("overrun_streak") or 0) + 1
        runner["normal_streak"] = 0
        record_runner_event(health, now, "overrun")
        log.warning(f"上一輪（{runner['running']} 開始）未正常結束，計逾時 {runner['overrun_streak']} 次")
    if int(runner.get("overrun_streak") or 0) >= DEGRADE_AFTER and level < MAX_DEGRADE_LEVEL:
        level += 1
        runner["overrun_streak"] = 0
        log.warning(f"連續 {DEGRADE_AFTER} 輪逾時，自動降頻：間隔 ×{2 ** level}")
        (state_dir / "ALERT_degraded").write_text(f"{now.isoformat()} 降頻到 ×{2 ** level}\n", encoding="utf-8")
    runner["degrade_level"] = level
    runner["running"] = now.isoformat(timespec="seconds")
    _save_json(runner_path, runner)

    # 3) 挑來源
    last_fetch = runner.setdefault("last_fetch", {})
    if args.all or args.only:
        only = {x for x in (args.only or "").split(",") if x}
        names = [n for n in P.SOURCES if (not only or n in only) and n not in disabled]
    else:
        names = due_sources(now, last_fetch, level, cfg("ARCHIVE_WINDOW"), disabled)

    gz_path = Path(cfg("GAZETTEER_PATH"))
    gazetteer = None
    if gz_path.exists() and any(n not in ("pbs", "new_taipei") for n in names):
        from match import Gazetteer
        gazetteer = Gazetteer(str(gz_path))

    inc_state = S.SeenState(state_dir / "incidents_seen.json")
    pbs_state = S.SeenState(state_dir / "pbs_seen.json", prune_hours=72)
    if session is None:
        import requests
        session = requests.Session()
    timeout = float(cfg("HTTP_TIMEOUT_SEC"))
    gap = float(cfg("SOURCE_GAP_SEC"))
    deadline = float(args.deadline if args.deadline is not None else cfg("SOFT_DEADLINE_SEC"))

    inc_rows: list[dict] = []
    pbs_rows: list[dict] = []
    per_source: dict[str, dict] = {}
    deferred: list[str] = []
    downgraded = 0
    seen_iso = S._iso(now)
    for i, name in enumerate(names):
        if time.monotonic() - t_start > deadline:
            deferred = names[i:]
            break
        if i:
            sleep(gap)
        r = fetch(name, session, timeout)
        fetched_at = datetime.now(P.TZ) if not args.fixed_now else now
        last_fetch[name] = fetched_at.isoformat(timespec="seconds")
        info = dict(http=r["http"], ms=r["ms"], bytes=r["bytes"], rows=None, error=r["error"])
        recs = None
        if r["ok"]:
            try:
                clean = P.sanitize(name, r["content"])
                save_raw(raw_dir, name, clean, fetched_at)
                recs = P.parse(name, clean)
            except P.ParseError as e:
                info["error"] = f"parse: {e}"
            except Exception as e:  # noqa: BLE001
                info["error"] = f"parse: {type(e).__name__}: {str(e)[:120]}"
        if recs is None:
            record_source(health, name, fetched_at, False, None, info["error"])
            log.warning(f"{name}: 失敗 {info['error']}")
            per_source[name] = info
            continue
        if name == "pbs":
            rows = [S.build_pbs(x, now) for x in recs]
            pbs_rows.extend(rows)
        else:
            county = P.SOURCES[name]["county"]
            rows = []
            for x in recs:
                row = S.build_incident(name, county, x, now, gazetteer)
                if row is None:
                    continue
                row, notes = S.validate_incident(row)
                if row is None:
                    continue
                downgraded += bool(notes)
                rows.append(row)
            inc_rows.extend(rows)
        info["rows"] = len(rows)
        record_source(health, name, fetched_at, True, len(rows), None)
        per_source[name] = info
    if gazetteer is not None:
        gazetteer.close()

    # 4) 差異 → 要寫的列
    inc_rows = S.dedupe(inc_rows, "incident_key")
    pbs_rows = S.dedupe(pbs_rows, "uid")
    inc_changed, pbs_changed = [], []
    for r in inc_rows:
        if inc_state.changed(r["incident_key"], r.get("status_raw"), r["units"], S.content_hash(r)):
            inc_changed.append(r)
    for r in pbs_rows:
        marker = f"{r.get('mod_dttm')}|{r['status']}"
        if pbs_state.changed(r["uid"], marker, [], S.content_hash(r)):
            pbs_changed.append(r)
    payload = {"fire119": inc_changed, "pbs": pbs_changed}

    flush_mark = runner.get("last_seen_flush_at")
    last_seen = None
    if not flush_mark or (now - datetime.fromisoformat(flush_mark)).total_seconds() >= \
            int(cfg("LAST_SEEN_FLUSH_MIN")) * 60:
        for r in inc_rows:
            inc_state.touch(r["incident_key"], seen_iso)
        for r in pbs_rows:
            pbs_state.touch(r["uid"], seen_iso)
        last_seen = {"fire119": inc_state.seen_since(flush_mark), "pbs": pbs_state.seen_since(flush_mark)}

    # 5) 寫入
    buffer_dir = data_dir / "buffer"
    dry_out = Path(args.out or (data_dir.parent / "dryrun"))
    if args.dry_run:
        _dry_write(dry_out, now, payload, last_seen)
        db_state, stats = "dry", {}
    elif inc_changed or pbs_changed or last_seen or (buffer_dir.is_dir() and any(buffer_dir.glob("*.json"))):
        db_state, stats = _db_write(payload, last_seen, buffer_dir)
    else:
        db_state, stats = "none", {}

    durable = db_state in ("ok", "buffered", "dry")
    if durable:
        for r in inc_changed:
            inc_state.commit(r["incident_key"], r.get("status_raw"), r["units"], S.content_hash(r), seen_iso)
        for r in pbs_changed:
            pbs_state.commit(r["uid"], f"{r.get('mod_dttm')}|{r['status']}", [], S.content_hash(r), seen_iso)
    for r in inc_rows:
        inc_state.touch(r["incident_key"], seen_iso)
    for r in pbs_rows:
        pbs_state.touch(r["uid"], seen_iso)
    if last_seen is not None and (db_state in ("ok", "dry") and not stats.get("last_seen_error")):
        runner["last_seen_flush_at"] = seen_iso
    inc_state.prune(now)
    pbs_state.prune(now)
    inc_state.save()
    pbs_state.save()

    # 6) 本機快照（解析後的列；119 存本輪全部列，警廣只存有變化的列）。dry-run 寫到輸出目錄，不進歸檔
    snap_dir = dry_out / "data" if args.dry_run else data_dir
    fetched_119 = [n for n in per_source if n != "pbs"]
    if fetched_119:
        write_snapshot(snap_dir, "fire119", now, {
            "collected_at": now.isoformat(timespec="seconds"),
            "sources": {n: per_source[n] for n in fetched_119}, "row_count": len(inc_rows), "data": inc_rows})
    if "pbs" in per_source:
        write_snapshot(snap_dir, "pbs_road_events", now, {
            "collected_at": now.isoformat(timespec="seconds"), "sources": {"pbs": per_source["pbs"]},
            "seen": len(pbs_rows), "row_count": len(pbs_changed), "data": pbs_changed})

    # 7) 結束：逾時判定、健康檔、單行摘要
    elapsed = round(time.monotonic() - t_start, 1)
    if elapsed > float(cfg("OVERRUN_SEC")):
        runner["overrun_streak"] = int(runner.get("overrun_streak") or 0) + 1
        runner["normal_streak"] = 0
        record_runner_event(health, now, "overrun")
    else:
        runner["overrun_streak"] = 0
        runner["normal_streak"] = int(runner.get("normal_streak") or 0) + 1
        if runner["normal_streak"] >= RECOVER_AFTER and level > 0:
            runner["degrade_level"] = level - 1
            runner["normal_streak"] = 0
            log.info(f"連續 {RECOVER_AFTER} 輪正常，回升一級：間隔 ×{2 ** (level - 1)}")
            if level - 1 == 0:
                (state_dir / "ALERT_degraded").unlink(missing_ok=True)
    runner.pop("running", None)
    _save_json(runner_path, runner)
    if db_state == "fail":
        record_runner_event(health, now, "db_fail")
    rss = peak_rss_mb()
    health.setdefault("runner", {}).update(
        degrade_level=runner.get("degrade_level", 0),
        last_round={"at": now.isoformat(timespec="seconds"), "elapsed_s": elapsed, "rss_mb": rss,
                    "sources": len(per_source), "deferred": deferred, "db": db_state,
                    "precision": _precision_counts(inc_rows)})
    alerts, warnings = evaluate_health(health, now, disabled, gazetteer_ok=gz_path.exists())
    health.update(updated_at=now.isoformat(timespec="seconds"), alerts=alerts, warnings=warnings)
    _save_json(health_path, health)
    (state_dir / "last_round.txt").write_text(
        f"{now.isoformat(timespec='seconds')} elapsed={elapsed}s rss={rss}MB sources={len(per_source)} "
        f"db={db_state}\n", encoding="utf-8")

    ok_n = sum(1 for v in per_source.values() if v["rows"] is not None)
    written = sum((stats.get("written") or {}).values()) if stats.get("written") else 0
    if db_state == "dry":
        written = len(inc_changed) + len(pbs_changed)
    log.info(f"本輪結束 sources={len(per_source)} ok={ok_n} rows119={len(inc_rows)} pbs={len(pbs_rows)} "
             f"changed={len(inc_changed)}+{len(pbs_changed)} downgraded={downgraded} deferred={len(deferred)} "
             f"db={db_state} written={written} elapsed={elapsed}s rss={rss}MB")
    return 0 if db_state != "fail" else 1


def _precision_counts(rows: list[dict]) -> dict:
    out: dict[str, int] = {}
    for r in rows:
        out[r["geom_precision"]] = out.get(r["geom_precision"], 0) + 1
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="不連 DB；寫入內容輸出成 JSONL")
    ap.add_argument("--out", help="dry-run 輸出目錄（預設 DATA_DIR 旁的 dryrun/）")
    ap.add_argument("--all", action="store_true", help="忽略排程，所有來源各抓一次")
    ap.add_argument("--only", help="只抓這些來源（逗號分隔）")
    ap.add_argument("--deadline", type=float, help="覆寫 SOFT_DEADLINE_SEC")
    ap.add_argument("--state-dir")
    ap.add_argument("--data-dir")
    ap.add_argument("--fixed-now", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    try:
        from dotenv import load_dotenv
        load_dotenv(APP_DIR / ".env")
    except ImportError:
        pass
    try:
        return run_round(args)
    except Exception as exc:  # noqa: BLE001
        log.error(f"FAILED: {type(exc).__name__}: {exc}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
