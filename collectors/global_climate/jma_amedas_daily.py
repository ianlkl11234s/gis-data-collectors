"""AMeDAS 每日 Parquet（2026-10-11 第二批；ADR-0021 S 型冷層）

每個 JST 日 D 在 D+1 01:00 之後，把該日所有 10 分鐘觀測組成一份 Parquet：每站 × 每 10 分一列，
欄位同 live.jma_amedas_observations（storage.supabase_writer._JMA_AMEDAS_COLS）。
  S3 key   weather-obs/jma/amedas/YYYY/MM/YYYYMMDD.parquet   STANDARD（全桶 lifecycle 轉 IA／GIR），永久
  防重傳   本機 receipt {LOCAL_DATA_DIR}/state/jma_amedas_parquet/{YYYYMMDD}.json ＋ S3 head（大小＋sha256）

資料來源（依序）：
  1. 本機 10 分鐘存檔 {LOCAL_DATA_DIR}/jma_amedas/YYYY/MM/DD/jma_amedas_HHMM.json 的 _type=current 列
     ⚠ 目錄日期是「台北時間」的收集時刻：JST 日 D＝台北 D-1 23:00 → D 23:00，要讀兩個台北日目錄、再以 observed_at 過濾。
     ⚠ 本機只留 2 天（ArchiveTask 台北 03:00 刪 D-1 目錄＝JST D+1 04:00），日結窗口只有約 3 小時。
  2. 缺的 10 分鐘時刻（排程漂移、重啟、窗口錯過）向上游 bosai map/{stamp}.json 補抓（2026-10-11 實測上游可回溯約 7 天）。
     補抓列 collected_at＝補抓時刻；補了哪些時刻寫進 Parquet metadata 與 receipt。
記憶體：先掃一遍檔案只記「observed_at → 檔案」索引，再逐小時讀檔、寫一個 row group，不把整天 18 萬列放進 RAM。
  哪些時刻是補抓、哪些仍缺，記在 receipt 與 S3 object metadata（檔內 metadata 只放靜態說明，避免重寫整檔）。
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

JST = timezone(timedelta(hours=9))
TAIPEI = timezone(timedelta(hours=8))
SLOT = timedelta(minutes=10)
CLOSE_AFTER = timedelta(hours=1)
S3_PREFIX = "weather-obs/jma/amedas"

FLOAT_COLS = ("lat", "lon", "alt_m", "temp", "precip10m", "precip1h", "precip3h", "precip24h",
              "wind", "gust", "humidity", "pressure", "sun1h", "snow", "snow1h", "snow6h", "snow12h", "snow24h")
TIME_COLS = ("observed_at", "source_time", "collected_at")


def s3_key(day: str) -> str:
    return f"{S3_PREFIX}/{day[:4]}/{day[4:6]}/{day}.parquet"


def receipt_s3_key(day: str) -> str:
    """日結 receipt sidecar（含 missing 明細），與 parquet 同目錄。"""
    return s3_key(day) + ".receipt.json"


def day_slots(day: str) -> list[datetime]:
    """JST 日 D 的 144 個 10 分鐘時刻（JST aware）。"""
    d0 = datetime.strptime(day, "%Y%m%d").replace(tzinfo=JST)
    return [d0 + i * SLOT for i in range(144)]


def closable_days(now: datetime, first_day: str, lookback_days: int = 6) -> list[str]:
    """可日結的 JST 日：>= first_day、now >= D+1 01:00，最多回看 lookback_days 天（上游約留 7 天）。"""
    today = now.astimezone(JST).date()
    first = datetime.strptime(first_day, "%Y%m%d").date()
    out = []
    for k in range(lookback_days, 0, -1):
        d = today - timedelta(days=k)
        if d < first:
            continue
        if now.astimezone(JST) >= datetime(d.year, d.month, d.day, tzinfo=JST) + timedelta(days=1) + CLOSE_AFTER:
            out.append(d.strftime("%Y%m%d"))
    return out


def local_files(base: Path, day: str) -> list[Path]:
    """JST 日 D 可能涵蓋的本機存檔：台北日期 D-1 與 D 兩個目錄。"""
    d = datetime.strptime(day, "%Y%m%d").date()
    files: list[Path] = []
    for dd in (d - timedelta(days=1), d):
        p = base / f"{dd:%Y}" / f"{dd:%m}" / f"{dd:%d}"
        if p.is_dir():
            files.extend(sorted(p.glob("jma_amedas_*.json")))
    return files


def _parse_ts(v) -> Optional[datetime]:
    if v is None or v == "":
        return None
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else None


def _current_rows(path: Path) -> list[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    return [r for r in (data.get("data") or []) if isinstance(r, dict) and r.get("_type") == "current"]


def index_local(files: list[Path], slots: set[datetime]) -> dict[datetime, Path]:
    """observed_at（JST 時刻）→ 第一個含該時刻 current 列的檔案。"""
    idx: dict[datetime, Path] = {}
    for f in files:
        rows = _current_rows(f)
        if not rows:
            continue
        t = _parse_ts(rows[0].get("observed_at"))
        if t is None:
            continue
        t = t.astimezone(JST)
        if t in slots and t not in idx:
            idx[t] = f
    return idx


def arrow_schema():
    import pyarrow as pa
    from storage.supabase_writer import SupabaseWriter
    fields = []
    for c in SupabaseWriter._JMA_AMEDAS_COLS:
        if c in TIME_COLS:
            fields.append(pa.field(c, pa.timestamp("s", tz="UTC")))
        elif c in FLOAT_COLS:
            fields.append(pa.field(c, pa.float64()))
        elif c == "wind_dir":
            fields.append(pa.field(c, pa.int16()))
        elif c == "has_snow_gauge":
            fields.append(pa.field(c, pa.bool_()))
        else:
            fields.append(pa.field(c, pa.string()))
    return pa.schema(fields)


def _table(rows: list[dict], schema):
    import pyarrow as pa
    cols = {}
    for f in schema:
        vals = [r.get(f.name) for r in rows]
        if f.name in TIME_COLS:
            vals = [_parse_ts(v) for v in vals]
        elif f.name == "wind_dir":
            vals = [int(v) if v is not None else None for v in vals]
        cols[f.name] = pa.array(vals, type=f.type)
    return pa.table(cols, schema=schema)


def build_day_parquet(day: str, out: Path, *, local_base: Path,
                      fetch_slot: Optional[Callable[[datetime], list[dict]]] = None,
                      max_fill: int = 144, max_fill_seconds: float = 300.0,
                      max_consecutive_fail: int = 6) -> dict:
    """組 JST 日 D 的 Parquet。fetch_slot(t) 回該時刻 current 列（上游補抓）；None＝不補。

    回傳 {rows, stations, slots_local, slots_filled, slots_missing, bytes}。0 列丟 RuntimeError（不產空檔）。
    補抓有整體時限（max_fill_seconds）與連續失敗熔斷（max_consecutive_fail）；觸發時丟 RuntimeError，
    不產檔、不寫 receipt，該日下輪可重試（上游持續掛掉時不拖長單輪）。
    """
    import pyarrow.parquet as pq
    slots = day_slots(day)
    idx = index_local(local_files(local_base, day), set(slots))
    schema = arrow_schema().with_metadata({b"jma_amedas_daily": json.dumps({
        "source": "jma_amedas", "date_jst": day, "grain": "station x 10min",
        "columns_as": "live.jma_amedas_observations",
        "provenance": "local 10-min archive; gaps refilled from bosai amedas map (see receipt)",
        "license": "気象庁ホームページ（PDL1.0）を加工"}, ensure_ascii=False).encode()})
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    filled, missing, stations = [], [], set()
    n_rows = 0
    fills_left = max_fill
    deadline = time.monotonic() + max_fill_seconds
    consec_fail = 0
    aborted = ""
    writer = pq.ParquetWriter(tmp, schema, compression="zstd")
    try:
        for h in range(24):
            hour_rows: list[dict] = []
            for t in slots[h * 6:(h + 1) * 6]:
                rows = None
                if t in idx:
                    rows = [r for r in _current_rows(idx[t])
                            if (_parse_ts(r.get("observed_at")) or t).astimezone(JST) == t]
                elif fetch_slot is not None and fills_left > 0 and not aborted:
                    if time.monotonic() > deadline:
                        aborted = f"補抓超過 {max_fill_seconds:.0f}s"
                    else:
                        fills_left -= 1
                        try:
                            rows = fetch_slot(t)
                        except Exception as e:  # noqa: BLE001
                            print(f"[jma_amedas] ⚠ 每日 Parquet 補抓 {t.isoformat()} 失敗: {e}")
                            rows = None
                            consec_fail += 1
                            if consec_fail >= max_consecutive_fail:
                                aborted = f"補抓連續失敗 {consec_fail} 次"
                        else:
                            consec_fail = 0
                    if rows:
                        filled.append(t.isoformat())
                if not rows:
                    missing.append(t.isoformat())
                    continue
                seen = set()
                for r in rows:
                    sid = r.get("station_id")
                    if not sid or sid in seen:
                        continue
                    seen.add(sid)
                    stations.add(sid)
                    hour_rows.append(r)
            if hour_rows:
                hour_rows.sort(key=lambda r: (r["observed_at"], r["station_id"]))
                writer.write_table(_table(hour_rows, schema))
                n_rows += len(hour_rows)
    finally:
        writer.close()
    if aborted:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"jma_amedas 每日 Parquet {day}: {aborted}，中止（該日下輪重試）")
    if n_rows == 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"jma_amedas 每日 Parquet {day}: 0 列（本機與上游皆無資料）")
    tmp.replace(out)
    return {"rows": n_rows, "stations": len(stations), "slots_local": len(idx), "slots_filled": len(filled),
            "slots_missing": len(missing), "missing": missing, "bytes": out.stat().st_size}
