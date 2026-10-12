"""日本氣象廳 雷ナウキャスト LIDEN 落雷收集器（C4）

資料來源：JMA bosai（免認證、非公式 JSON）
  https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N3.json   → 判斷最新可取的 liden 幀
  https://www.jma.go.jp/bosai/jmatile/data/nowc/{vt}/none/{vt}/surf/liden/data.geojson
    （Content-Encoding: gzip；curl 要加 --compressed）

寫入：live.lightning_events（source='jma'，與台電／氣象署同表；migration 440 加 source_type 欄）

## 上游特性（2026-10-12 實測）
- 每幀＝validtime 前 5 分鐘內的雷擊（obstimeJST 毫秒精度），幀與幀不重疊；validtime 為 UTC、5 分對齊
  （非對齊時間 404）。
- targetTimes_N3 只列約 3 小時，但伺服器實際留約 4–5 天（10-07 00Z 仍 200、10-06 00Z 已 404）。
  所以本 collector 自己產 5 分格點、在 JAPAN_JMA_LIGHTNING_BACKFILL_HOURS 內補抓缺幀，不靠 targetTimes 列舉。
- 無雷擊的幀是 200 + 空 FeatureCollection（71 bytes gzip），不是故障。
- properties.type：nowc.properties XML 的圖示規則是 `type === 4 ? CG : CC`，圖例文字為
  「対地放電（落雷）／雲放電」→ 4＝対地放電、其他＝雲放電。2026-10-10～11 兩日 25,400 筆實測只見
  0（7,542）、1（5,646）、4（12,212）；0 與 1 的差別官方未說明（JMA 網頁都畫成雲放電）。
  既有表 strike_type 語意是 0=雲對地 / 1=雲中（台電），所以 4→0、0／1→1、未見過的值→NULL 並 log；
  原值另存 source_type（migration 440），0／1 的區別不丟。

## 去重
- event_id：上游 id 是「幀內序號」不是雷擊 ID，不可用；比照 lightning_cwa 由 dedup_hash 合成。
- dedup_hash = md5(strike_time 毫秒 ISO | lon | lat | source_type)；UNIQUE (source, dedup_hash) DO NOTHING。
- 已完成幀記在 state，只在 DB 寫入成功後前進（jma_quake 模式）；剛出爐（< SETTLE 分鐘）的幀這輪照抓但不標完成，
  下一輪再抓一次以防上游補值，重複列由 UNIQUE 擋掉。
"""

from __future__ import annotations

import gzip
import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import JMA_BOSAI_BASE, new_jma_session, to_float
from collectors.lightning_events import _make_dedup_hash
from collectors.lightning_cwa import _synth_event_id

SOURCE = "jma"
STEP = timedelta(minutes=5)
JST = timezone(timedelta(hours=9))
URL_TARGET_TIMES = f"{JMA_BOSAI_BASE}/jmatile/data/nowc/targetTimes_N3.json"
URL_FRAME = f"{JMA_BOSAI_BASE}/jmatile/data/nowc/{{vt}}/none/{{vt}}/surf/liden/data.geojson"

# JMA type → 既有表 strike_type（0=雲對地、1=雲中）
_TYPE_MAP = {4: 0, 0: 1, 1: 1}

# 404 重試幾輪後當成上游缺幀、不再抓
MISSING_GIVE_UP = 3


def vt_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def parse_vt(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def floor_step(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return dt - timedelta(minutes=dt.minute % 5)


def frame_grid(start: datetime, end: datetime) -> list[str]:
    """[start, end] 之間 5 分對齊的 validtime（UTC 字串），舊→新。"""
    out, t = [], floor_step(start)
    if t < start:
        t += STEP
    while t <= end:
        out.append(vt_str(t))
        t += STEP
    return out


def latest_liden_vt(target_times: Any) -> Optional[datetime]:
    """targetTimes_N3.json 中含 liden 的最新 validtime。"""
    best = None
    for it in target_times or []:
        if not isinstance(it, dict) or "liden" not in (it.get("elements") or []):
            continue
        dt = parse_vt(it.get("validtime"))
        if dt and (best is None or dt > best):
            best = dt
    return best


def parse_obstime_jst(value: Any) -> Optional[datetime]:
    """'2026/10/10 18:55:28.860'（JST）→ aware datetime；無法解析回 None（不猜）。"""
    if not isinstance(value, str):
        return None
    s = value.strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=JST)
        except ValueError:
            continue
    return None


def parse_liden(geo: Any, vt: str, collected_at: str) -> tuple[list[dict], Counter, int]:
    """一幀 GeoJSON → lightning_events 列。回傳 (rows, 未知 type 計數, 丟棄筆數)。"""
    rows: list[dict] = []
    unknown: Counter = Counter()
    dropped = 0
    seen: set[str] = set()
    feats = geo.get("features") if isinstance(geo, dict) else None
    for f in feats or []:
        try:
            coords = f["geometry"]["coordinates"]
            props = f.get("properties") or {}
        except (KeyError, TypeError):
            dropped += 1
            continue
        lon = to_float(coords[0]) if isinstance(coords, list) and len(coords) >= 2 else None
        lat = to_float(coords[1]) if isinstance(coords, list) and len(coords) >= 2 else None
        when = parse_obstime_jst(props.get("obstimeJST"))
        if lon is None or lat is None or when is None:
            dropped += 1  # 表欄位 NOT NULL；缺座標／時間不補值
            continue
        raw_type = props.get("type")
        source_type = raw_type if isinstance(raw_type, int) and not isinstance(raw_type, bool) else None
        strike_type = _TYPE_MAP.get(source_type) if source_type is not None else None
        if strike_type is None:
            unknown[repr(raw_type)] += 1
        strike_iso = when.astimezone(timezone.utc).isoformat(timespec="milliseconds")
        dedup = _make_dedup_hash(strike_iso, lon, lat, source_type)
        if dedup in seen:
            continue
        seen.add(dedup)
        rows.append({
            "event_id": _synth_event_id(dedup),
            "strike_time": strike_iso,
            "lon": lon,
            "lat": lat,
            "intensity_ka": None,          # LIDEN JSON 不提供電流強度
            "strike_type": strike_type,
            "source_type": source_type,
            "dedup_hash": dedup,
            "source": SOURCE,
            "observed_at": strike_iso,
            "frame_vt": vt,
            "collected_at": collected_at,
        })
    return rows, unknown, dropped


def decode_body(body: bytes) -> Any:
    """requests 通常已依 Content-Encoding 解 gzip；保險起見 magic number 仍是 gzip 就再解一次。"""
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return json.loads(body.decode("utf-8"))


class JmaLightningCollector(BaseCollector):
    """JMA LIDEN 5 分鐘幀；自產格點補抓缺幀，state 只在 DB 寫入成功後前進。"""

    name = "jma_lightning"
    interval_minutes = config.JAPAN_JMA_LIGHTNING_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-lightning")
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "jma_lightning_frames.json"
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None

    def require_db_write(self) -> bool:
        return True

    # ---- state ----
    def _load_state(self) -> dict:
        if self._state is None:
            try:
                d = json.loads(self.state_path.read_text())
                self._state = {"done": dict(d.get("done") or {}), "miss": dict(d.get("miss") or {})}
            except (OSError, ValueError, AttributeError, TypeError):
                self._state = {"done": {}, "miss": {}}
        return self._state

    def _save_state(self, state: dict) -> None:
        self._state = state
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, sort_keys=True))
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[{self.name}] ⚠ state 寫入失敗（重啟後會重抓視窗內幀，DB DO NOTHING 擋重複）: {e}")

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_state(self._pending_state)
        self._pending_state = None
        return stats

    # ---- fetch ----
    def _fetch_frame(self, vt: str) -> tuple[str, Any]:
        """回傳 ('ok', geojson) / ('missing', None)；其他錯誤丟例外。"""
        url = URL_FRAME.format(vt=vt)
        last: Exception | None = None
        for attempt in range(2):
            try:
                resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
                if resp.status_code == 404:
                    return "missing", None
                resp.raise_for_status()
                return "ok", decode_body(resp.content)
            except Exception as e:  # noqa: BLE001
                last = e
                if attempt == 0:
                    time.sleep(2)
        raise RuntimeError(f"liden {vt}: {last}")

    def _latest_vt(self, now: datetime) -> datetime:
        try:
            resp = self._session.get(URL_TARGET_TIMES, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            latest = latest_liden_vt(resp.json())
            if latest:
                return latest
        except Exception as e:  # noqa: BLE001
            print(f"[{self.name}] ⚠ targetTimes_N3 失敗，改用 now-10 分推估最新幀: {e}")
        return floor_step(now - timedelta(minutes=10))

    def plan_frames(self, latest: datetime, now: datetime, state: dict) -> list[str]:
        """待抓幀：最新 3 幀優先，其餘缺幀舊→新，每輪上限 MAX_FRAMES。"""
        start = latest - timedelta(hours=config.JAPAN_JMA_LIGHTNING_BACKFILL_HOURS)
        grid = frame_grid(start, latest)
        todo = [vt for vt in grid if vt not in state["done"]]
        cap = max(1, config.JAPAN_JMA_LIGHTNING_MAX_FRAMES)
        newest = todo[-3:]
        rest = [vt for vt in todo if vt not in newest]
        return (list(reversed(newest)) + rest)[:cap]

    def collect(self) -> dict:
        now = datetime.now(timezone.utc)
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        state = self._load_state()
        latest = self._latest_vt(now)
        plan = self.plan_frames(latest, now, state)

        done = dict(state["done"])
        miss = dict(state["miss"])
        rows: list[dict] = []
        frames: list[dict] = []
        unknown: Counter = Counter()
        dropped = 0
        errors: dict[str, str] = {}
        settle = timedelta(minutes=config.JAPAN_JMA_LIGHTNING_SETTLE_MINUTES)
        for vt in plan:
            try:
                status, geo = self._fetch_frame(vt)
            except RuntimeError as e:
                errors[vt] = str(e)
                continue
            vt_dt = parse_vt(vt)
            if status == "missing":
                miss[vt] = int(miss.get(vt, 0)) + 1
                if miss[vt] >= MISSING_GIVE_UP:
                    done[vt] = "missing"
                frames.append({"vt": vt, "status": "missing"})
                continue
            r, unk, drp = parse_liden(geo, vt, collected_at)
            rows.extend(r)
            unknown.update(unk)
            dropped += drp
            frames.append({"vt": vt, "status": "ok", "strikes": len(r)})
            miss.pop(vt, None)
            if now - vt_dt >= settle:
                done[vt] = len(r)
            time.sleep(0.1)

        # 只保留視窗內的 state（視窗外的幀不會再被列入計畫）
        cutoff = vt_str(latest - timedelta(hours=config.JAPAN_JMA_LIGHTNING_BACKFILL_HOURS + 1))
        done = {k: v for k, v in done.items() if k >= cutoff}
        miss = {k: v for k, v in miss.items() if k >= cutoff}

        if plan and len(errors) == len(plan):
            raise RuntimeError(f"jma_lightning: 本輪 {len(plan)} 幀全部抓取失敗 {list(errors.values())[:2]}")
        if unknown:
            print(f"[{self.name}] ⚠ 未知 type 值（strike_type 存 NULL、source_type 留原值）: {dict(unknown)}")

        self._pending_state = {"done": done, "miss": miss}
        summary = {
            "latest_vt": vt_str(latest),
            "frames_planned": len(plan),
            "frames": frames,
            "strike_count": len(rows),
            "unknown_types": dict(unknown),
            "dropped": dropped,
            "frame_errors": errors,
            "collected_at": collected_at,
        }
        if errors:
            # 已抓到的照寫；失敗幀不標完成、下輪重抓。不設 _collector_error，避免單幀失敗讓整輪 state 不前進。
            print(f"[{self.name}] ⚠ {len(errors)} 幀抓取失敗，下輪重試")
        if not rows:
            summary["no_change"] = True
            return summary
        summary["data"] = rows
        return summary


if __name__ == "__main__":
    # 離線試跑（不寫 DB、不寫 state）：python3 -m collectors.global_climate.jma_lightning
    c = JmaLightningCollector.__new__(JmaLightningCollector)
    c.name = "jma_lightning"
    c._session = new_jma_session("jma-lightning-test")
    c._state = {"done": {}, "miss": {}}
    c._pending_state = None
    out = c.collect()
    print({k: v for k, v in out.items() if k not in ("data", "frames")})
