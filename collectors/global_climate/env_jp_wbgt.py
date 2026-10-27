"""環境省 熱中症予防情報サイト 暑さ指数（WBGT）收集器（C7，is_multi_table）

資料來源（免金鑰；「暑さ指数（WBGT）電子情報提供サービス」，2026 年服務期 4/22–10/21）：
  實況 https://www.wbgt.env.go.jp/est15WG/dl/wbgt_all_{yyyymm}.csv
       第 1 列 Date,Time,{841 站號}；之後每列一個時刻（當月 1 日 1:00 起到當日 24:00），值為 ℃（小數 1 位）
  預報 https://www.wbgt.env.go.jp/prev15WG/dl/yohou_all.csv
       第 1 列前兩欄空白，之後是 YYYYMMDDHH 對象時刻（3 小時一格，到翌々日 24 時）；
       之後每列：站號, 作成時刻 'YYYY/MM/DD HH:MM', 值×10（有前導空白）

寫入（gis-platform migration 440）：
  live.env_jp_wbgt_observations  PK (station_id, observed_at)  DO UPDATE（值有變才更新：環境省曾公告實況誤算出並回溯修正）
  live.env_jp_wbgt_forecasts     PK (station_id, issued_at)    DO NOTHING（每期保留，target_times[]／wbgt[] 陣列）

## 已驗證（2026-10-12）
- 編碼：純 ASCII（官方說明「ASCIIコード」）、LF；實測 utf-8 可解。
- 站號＝AMeDAS 站號：實況 841 站、預報 841 站全部在 amedastable.json（841/841），座標從 live.jma_amedas_current join。
- 時刻為 JST；'24:00'／'…24' ＝次日 00:00 JST。
- 月檔含當日未到時刻的空白列：空格不是缺測、也不是 0 → 不寫列。
- 「東祖谷」10/22 前用「京上」站號 71191 提供（官方說明）。
- 授權：tos.php 把「暑さ指数（WBGT）電子情報提供サービス」列為 PDL1.0 的適用外；服務頁要求轉載時必明記出典為環境省。

## 只記變動
- 實況：state 記每個月檔每個時刻列的 hash；hash 變了（新時刻或回溯修正）才送該列的非空格。
  首跑（該月無 state）只送最近 JAPAN_ENV_WBGT_INITIAL_LOOKBACK_DAYS 天。
- 預報：作成時刻沒變就不寫。兩者 HTTP 都帶 If-None-Match／If-Modified-Since，304 直接跳過。
- 季節月份外（JAPAN_ENV_WBGT_SEASON_MONTHS，JST）不發請求；季內 404（服務期前後幾天）不算錯。
- state 只在 DB 寫入成功後前進。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import mount_bounded_retry

BASE = "https://www.wbgt.env.go.jp"
URL_OBS = BASE + "/est15WG/dl/wbgt_all_{ym}.csv"
URL_FCST = BASE + "/prev15WG/dl/yohou_all.csv"
JST = timezone(timedelta(hours=9))


def season_months(spec: str) -> set[int]:
    out: set[int] = set()
    for tok in (spec or "").split(","):
        tok = tok.strip()
        if tok.isdigit() and 1 <= int(tok) <= 12:
            out.add(int(tok))
    return out


def in_season(now: datetime, spec: str) -> bool:
    return now.astimezone(JST).month in season_months(spec)


def _num(v: Any) -> Optional[float]:
    s = (v or "").strip() if isinstance(v, str) else v
    if s in (None, ""):
        return None
    try:
        f = float(s)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def parse_obs_time(date_s: str, time_s: str) -> Optional[datetime]:
    """('2026/10/1', '24:00') → 2026-10-02T00:00+09:00。"""
    try:
        d = datetime.strptime(date_s.strip(), "%Y/%m/%d")
        hh, mm = time_s.strip().split(":")
        return (d + timedelta(hours=int(hh), minutes=int(mm))).replace(tzinfo=JST)
    except (ValueError, AttributeError):
        return None


def parse_target_hour(s: str) -> Optional[datetime]:
    """'2026101224' → 2026-10-13T00:00+09:00。"""
    s = (s or "").strip()
    if len(s) != 10 or not s.isdigit():
        return None
    try:
        d = datetime.strptime(s[:8], "%Y%m%d")
    except ValueError:
        return None
    return (d + timedelta(hours=int(s[8:]))).replace(tzinfo=JST)


def parse_obs_csv(text: str) -> tuple[list[str], list[tuple[str, list[Optional[float]], str]]]:
    """實況月檔 → (站號清單, [(observed_at ISO, 各站值, 列 hash)])；無法解析時刻的列丟棄。"""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows or len(rows[0]) < 3 or rows[0][0].strip() != "Date":
        raise ValueError("WBGT 實況 CSV 表頭不符（預期 Date,Time,站號…）")
    stations = [s.strip() for s in rows[0][2:]]
    out = []
    for r in rows[1:]:
        if len(r) < 2:
            continue
        t = parse_obs_time(r[0], r[1])
        if t is None:
            continue
        cells = [c.strip() for c in r[2:2 + len(stations)]]
        cells += [""] * (len(stations) - len(cells))
        vals = [_num(c) for c in cells]
        h = hashlib.md5(",".join(cells).encode()).hexdigest()[:16]
        out.append((t.isoformat(), vals, h))
    return stations, out


def parse_forecast_csv(text: str) -> tuple[Optional[str], list[dict]]:
    """預報檔 → (最新作成時刻 ISO, 每站一列 {station_id, issued_at, target_times[], wbgt[]})。值 ÷10。"""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows or len(rows[0]) < 3:
        raise ValueError("WBGT 預報 CSV 表頭不符")
    times = [parse_target_hour(c) for c in rows[0][2:]]
    out: list[dict] = []
    latest: Optional[datetime] = None
    for r in rows[1:]:
        if len(r) < 3 or not r[0].strip():
            continue
        try:
            issued = datetime.strptime(r[1].strip(), "%Y/%m/%d %H:%M").replace(tzinfo=JST)
        except ValueError:
            continue
        tt, vv = [], []
        for t, c in zip(times, r[2:]):
            if t is None:
                continue
            v = _num(c)
            tt.append(t.isoformat())
            vv.append(round(v / 10.0, 1) if v is not None else None)
        latest = issued if latest is None or issued > latest else latest
        out.append({"station_id": r[0].strip(), "issued_at": issued.isoformat(),
                    "target_times": tt, "wbgt": vv})
    return (latest.isoformat() if latest else None), out


class EnvJpWbgtCollector(BaseCollector):
    """環境省 WBGT 實況（逐站×逐時，變動才寫）＋預報（每期保留）。"""

    name = "env_jp_wbgt"
    interval_minutes = config.JAPAN_ENV_WBGT_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update(
            {"User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; env-jp-wbgt)"})
        mount_bounded_retry(self._session)
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "env_jp_wbgt.json"
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None

    def require_db_write(self) -> bool:
        return True

    def _load_state(self) -> dict:
        if self._state is None:
            try:
                d = json.loads(self.state_path.read_text())
                self._state = {"rows": dict(d.get("rows") or {}), "http": dict(d.get("http") or {}),
                               "fcst_issued": d.get("fcst_issued")}
            except (OSError, ValueError, AttributeError, TypeError):
                self._state = {"rows": {}, "http": {}, "fcst_issued": None}
        return self._state

    def _save_state(self, state: dict) -> None:
        self._state = state
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, sort_keys=True))
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[{self.name}] ⚠ state 寫入失敗（重啟後重送，DB 端 PK 擋重複）: {e}")

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_state(self._pending_state)
        self._pending_state = None
        return stats

    def _get(self, url: str, http_state: dict) -> tuple[str, Optional[str], dict]:
        """回傳 (status, text, 新 header 狀態)；status ∈ ok／not_modified／missing。"""
        prev = http_state.get(url) or {}
        headers = {}
        if prev.get("etag"):
            headers["If-None-Match"] = prev["etag"]
        if prev.get("last_modified"):
            headers["If-Modified-Since"] = prev["last_modified"]
        resp = self._session.get(url, headers=headers, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code == 304:
            return "not_modified", None, prev
        if resp.status_code == 404:
            return "missing", None, prev
        resp.raise_for_status()
        new = {"etag": resp.headers.get("ETag"), "last_modified": resp.headers.get("Last-Modified")}
        return "ok", resp.content.decode("utf-8"), new

    def _obs_months(self, now_jst: datetime) -> list[str]:
        months = [now_jst.strftime("%Y%m")]
        if now_jst.day <= 2:  # 月初兩天也看上月檔（上月最後 24:00 與回溯修正）
            months.insert(0, (now_jst.replace(day=1) - timedelta(days=1)).strftime("%Y%m"))
        return months

    def collect(self) -> dict:
        now = datetime.now(timezone.utc)
        now_jst = now.astimezone(JST)
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        if not in_season(now, config.JAPAN_ENV_WBGT_SEASON_MONTHS):
            return {"no_change": True, "off_season": True, "collected_at": collected_at}

        old = self._load_state()
        state = {"rows": {k: dict(v) for k, v in old["rows"].items()},
                 "http": dict(old["http"]), "fcst_issued": old.get("fcst_issued")}
        records: list[dict] = []
        status: dict[str, str] = {}
        failures: dict[str, str] = {}

        # ── 實況 ──
        lookback = now - timedelta(days=config.JAPAN_ENV_WBGT_INITIAL_LOOKBACK_DAYS)
        for ym in self._obs_months(now_jst):
            url = URL_OBS.format(ym=ym)
            try:
                st, text, hdr = self._get(url, state["http"])
            except Exception as e:  # noqa: BLE001
                failures[f"obs:{ym}"] = str(e)
                continue
            status[f"obs:{ym}"] = st
            if st != "ok":
                continue
            try:
                stations, rows = parse_obs_csv(text)
            except ValueError as e:
                failures[f"obs:{ym}"] = str(e)
                continue
            first_time = ym not in state["rows"]
            seen = state["rows"].setdefault(ym, {})
            for obs_iso, vals, h in rows:
                if all(v is None for v in vals):
                    continue  # 尚未觀測的未來時刻
                if seen.get(obs_iso) == h:
                    continue
                seen[obs_iso] = h
                if first_time and datetime.fromisoformat(obs_iso) < lookback:
                    continue  # 首跑只送回看期內；更早的列記 hash，日後若被修正仍會送
                for sid, v in zip(stations, vals):
                    if v is None:
                        continue  # 空格＝缺測，不寫列（不當 0）
                    records.append({"_type": "obs", "station_id": sid, "observed_at": obs_iso,
                                    "wbgt": v, "collected_at": collected_at})
            state["http"][url] = hdr

        # ── 預報 ──
        try:
            st, text, hdr = self._get(URL_FCST, state["http"])
            status["forecast"] = st
            if st == "ok":
                issued, frows = parse_forecast_csv(text)
                if issued and issued != state.get("fcst_issued"):
                    for r in frows:
                        records.append({"_type": "forecast", **r, "collected_at": collected_at})
                    state["fcst_issued"] = issued
                state["http"][URL_FCST] = hdr
        except Exception as e:  # noqa: BLE001
            failures["forecast"] = str(e)

        # 只留最近兩個月的列 hash
        keep = set(self._obs_months(now_jst)) | {now_jst.strftime("%Y%m")}
        state["rows"] = {k: v for k, v in state["rows"].items() if k in keep}

        if failures and not status:
            raise RuntimeError(f"env_jp_wbgt: 所有來源失敗 {failures}")

        self._pending_state = state
        out = {"http_status": status, "source_failures": failures, "collected_at": collected_at,
               "row_counts": {"obs": sum(1 for r in records if r["_type"] == "obs"),
                              "forecast": sum(1 for r in records if r["_type"] == "forecast")},
               "forecast_issued_at": state.get("fcst_issued")}
        if failures:
            print(f"[{self.name}] ⚠ 部分來源失敗（下輪重試）: {list(failures)}")
        if not records:
            out["no_change"] = True
            return out
        out["data"] = records
        return out


if __name__ == "__main__":
    c = EnvJpWbgtCollector.__new__(EnvJpWbgtCollector)
    c.name = "env_jp_wbgt"
    c._session = requests.Session()
    c._state = {"rows": {}, "http": {}, "fcst_issued": None}
    c._pending_state = None
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
