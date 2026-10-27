"""日本氣象廳 AMeDAS 10 分鐘全國觀測收集器（約 1,286 站）

資料來源：JMA bosai（免認證、非公式 JSON）
  https://www.jma.go.jp/bosai/amedas/data/latest_time.txt        → '2026-10-09T15:40:00+09:00'
  https://www.jma.go.jp/bosai/amedas/data/map/{YYYYMMDDHHmm00}.json → {station_id: {elem: [值, 品質碼]}}
  https://www.jma.go.jp/bosai/amedas/const/amedastable.json       → 站表（lat/lon 為 [度, 分]）

寫入（gis-platform migration 435，is_multi_table）：
  live.jma_amedas_current       PK station_id，每輪全量 upsert（updated_at=now()）
  live.jma_amedas_observations  PK (station_id, observed_at)，只寫整點（分鐘=00），DO NOTHING；保留期見 metadata.retention_policies（gis-platform mig 436）

缺值語意（docs/api-platforms/jma/gotchas.md）：
  - 值為 [值, 品質碼]；品質碼 None = 缺值 → NULL（不補 0）
  - 無積雪計的站（站表 elems 第 6 碼 != '1'，約 950 站）也會回 snow1h: [0, None] → snow* 一律 NULL
整點保證：若某輪 latest_time 不是整點（例如排程漂移跳過 :00），會補抓最近整點那一份 map，
  避免 observations 漏小時（同一 process 內每個整點只補一次；DB 端 DO NOTHING 兜底重複）。
新鮮度：latest_time 落後現在 > 40 分 → print stale 警告（result['stale']=True）。
每日 Parquet（第二批 2026-10-11）：每輪結尾檢查可日結的 JST 日（D+1 01:00 後、無 receipt），每輪最多 1 天，
  組 Parquet 上傳 S3 weather-obs/jma/amedas/YYYY/MM/YYYYMMDD.parquet（見 jma_amedas_daily.py）。
  JAPAN_JMA_AMEDAS_DAILY_PARQUET=false 可關。失敗 → result['_collector_error']（通知），下輪重試。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import (
    JmaFetchError, age_minutes, fetch, new_jma_session, parse_dt, to_float,
)

URL_LATEST_TIME = "amedas/data/latest_time.txt"
URL_MAP = "amedas/data/map/{stamp}.json"
URL_TABLE = "amedas/const/amedastable.json"

STALE_MINUTES = 40
JST = timezone(timedelta(hours=9))
TABLE_REFRESH = timedelta(hours=24)

# JSON 鍵 → 表欄位
ELEMENT_FIELDS = {
    "temp": "temp",
    "precipitation10m": "precip10m",
    "precipitation1h": "precip1h",
    "precipitation3h": "precip3h",
    "precipitation24h": "precip24h",
    "wind": "wind",
    "windDirection": "wind_dir",
    "gust": "gust",
    "humidity": "humidity",
    "pressure": "pressure",          # 現地気圧（normalPressure 海面気圧不在契約內）
    "sun1h": "sun1h",
    "snow": "snow",
    "snow1h": "snow1h",
    "snow6h": "snow6h",
    "snow12h": "snow12h",
    "snow24h": "snow24h",
}
SNOW_FIELDS = ("snow", "snow1h", "snow6h", "snow12h", "snow24h")

ROW_FIELDS = (
    "station_id", "station_name", "station_name_en", "lat", "lon", "alt_m",
    "observed_at", *ELEMENT_FIELDS.values(), "has_snow_gauge", "source_time", "collected_at",
)


def _deg_min(value: Any) -> Optional[float]:
    """[度, 分] → 十進位度。"""
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        d, m = to_float(value[0]), to_float(value[1])
        if d is None or m is None:
            return None
        return round(d + m / 60.0, 6)
    return to_float(value)


def build_station_index(table: dict) -> dict[str, dict]:
    """amedastable.json → {station_id: {name, name_en, lat, lon, alt_m, has_snow_gauge}}"""
    out: dict[str, dict] = {}
    for sid, st in (table or {}).items():
        if not isinstance(st, dict):
            continue
        elems = str(st.get("elems") or "")
        out[str(sid)] = {
            "station_name": st.get("kjName"),
            "station_name_en": st.get("enName"),
            "lat": _deg_min(st.get("lat")),
            "lon": _deg_min(st.get("lon")),
            "alt_m": to_float(st.get("alt")),
            "has_snow_gauge": len(elems) > 5 and elems[5] == "1",
        }
    return out


def _value(pair: Any) -> Optional[float]:
    """[值, 品質碼] → 值；品質碼 None 或格式不符 → None。"""
    if not isinstance(pair, (list, tuple)) or len(pair) < 2:
        return None
    if pair[1] is None:
        return None
    return to_float(pair[0])


def parse_amedas_map(data: dict, stations: dict[str, dict], *, observed_at: str,
                     source_time: str, collected_at: str) -> list[dict]:
    """map/{stamp}.json → 每站一列（欄位同 ROW_FIELDS）。不在站表的站仍保留（座標 NULL）。"""
    rows: list[dict] = []
    for sid, obs in (data or {}).items():
        if not isinstance(obs, dict):
            continue
        st = stations.get(str(sid)) or {}
        has_snow = bool(st.get("has_snow_gauge"))
        row: dict[str, Any] = {
            "station_id": str(sid),
            "station_name": st.get("station_name"),
            "station_name_en": st.get("station_name_en"),
            "lat": st.get("lat"),
            "lon": st.get("lon"),
            "alt_m": st.get("alt_m"),
            "observed_at": observed_at,
            "has_snow_gauge": has_snow,
            "source_time": source_time,
            "collected_at": collected_at,
        }
        for key, field in ELEMENT_FIELDS.items():
            row[field] = _value(obs.get(key))
        if row["wind_dir"] is not None:
            row["wind_dir"] = int(row["wind_dir"])
        if not has_snow:
            for f in SNOW_FIELDS:
                row[f] = None
        rows.append(row)
    return rows


def _stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M00")


class JmaAmedasCollector(BaseCollector):
    """AMeDAS 10 分鐘全量 → current upsert + 整點 observations。"""

    name = "jma_amedas"
    interval_minutes = config.JAPAN_JMA_AMEDAS_INTERVAL
    COLLECT_TIMEOUT = 600  # 每日 Parquet 若需整天向上游補抓（144 請求）實測約 212 秒；平常走本機檔只需數秒

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-amedas")
        self._stations: dict[str, dict] = {}
        self._stations_loaded_at: Optional[datetime] = None
        self._hours_done: set[str] = set()
        self._s3 = None
        self.parquet_dir = Path(config.LOCAL_DATA_DIR) / "state" / "jma_amedas_parquet"
        self.local_base = Path(config.LOCAL_DATA_DIR) / "jma_amedas"

    def _station_index(self) -> dict[str, dict]:
        now = datetime.now(timezone.utc)
        if (not self._stations or self._stations_loaded_at is None
                or now - self._stations_loaded_at > TABLE_REFRESH):
            try:
                self._stations = build_station_index(fetch(self._session, URL_TABLE))
                self._stations_loaded_at = now
            except JmaFetchError as e:
                if not self._stations:
                    raise
                print(f"[{self.name}] ⚠ 站表更新失敗，沿用舊表: {e}")
        return self._stations

    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        latest_raw = str(fetch(self._session, URL_LATEST_TIME, as_text=True)).strip()
        latest = parse_dt(latest_raw)
        if latest is None:
            raise RuntimeError(f"jma_amedas: latest_time.txt 無法解析: {latest_raw[:60]!r}")
        source_time = latest.isoformat()

        stations = self._station_index()
        data = fetch(self._session, URL_MAP.format(stamp=_stamp(latest)))
        rows = parse_amedas_map(data, stations, observed_at=source_time,
                                source_time=source_time, collected_at=collected_at)
        if not rows:
            raise RuntimeError("jma_amedas: map JSON 0 站")

        records = [{**r, "_type": "current"} for r in rows]

        # 整點 observations：latest 本身是整點就直接用；否則補抓最近整點那份
        hour = latest.replace(minute=0, second=0, microsecond=0)
        hour_key = hour.isoformat()
        hourly_count = 0
        if latest.minute == 0:
            records.extend({**r, "_type": "observation"} for r in rows)
            hourly_count = len(rows)
            self._hours_done.add(hour_key)
        elif hour_key not in self._hours_done:
            try:
                hdata = fetch(self._session, URL_MAP.format(stamp=_stamp(hour)))
                hrows = parse_amedas_map(hdata, stations, observed_at=hour_key,
                                         source_time=source_time, collected_at=collected_at)
                records.extend({**r, "_type": "observation"} for r in hrows)
                hourly_count = len(hrows)
                if hrows:
                    self._hours_done.add(hour_key)
                else:  # 空 map 視為缺資料，不標完成，下輪重試
                    print(f"[{self.name}] ⚠ 整點 {hour_key} 回傳 0 站（下輪再試）")
            except JmaFetchError as e:
                print(f"[{self.name}] ⚠ 補抓整點 {hour_key} 失敗（下輪再試）: {e}")
        # 只留最近 48 個整點，避免 set 無限長
        if len(self._hours_done) > 48:
            self._hours_done = set(sorted(self._hours_done)[-48:])

        lag = age_minutes(latest)
        stale = lag is not None and lag > STALE_MINUTES
        if stale:
            print(f"[{self.name}] ⚠ stale: latest_time={source_time} 已落後 {lag:.0f} 分（>{STALE_MINUTES}）")

        out = {
            "data": records,
            "station_count": len(rows),
            "hourly_count": hourly_count,
            "latest_time": source_time,
            "lag_minutes": round(lag, 1) if lag is not None else None,
            "stale": stale,
            "collected_at": collected_at,
        }
        try:
            daily = self.close_daily(datetime.now(timezone.utc))
            if daily:
                out["daily_parquet"] = daily
        except Exception as e:  # noqa: BLE001 — 本輪資料照寫，日結下輪重試；但要讓告警看得到
            print(f"[{self.name}] ✗ 每日 Parquet 失敗（下輪重試）: {e}")
            out["daily_parquet_error"] = str(e)
            out["_collector_error"] = f"jma_amedas daily parquet: {e}"
        return out

    # ───────── 每日 Parquet（JST 日結）─────────
    def _s3_client(self):
        if self._s3 is None and config.S3_BUCKET:
            from storage.s3 import S3Storage
            self._s3 = S3Storage()
        return self._s3

    def _fetch_slot(self, t: datetime) -> list[dict]:
        """上游補抓單一 10 分鐘時刻（JST aware）→ current 列。"""
        data = fetch(self._session, URL_MAP.format(stamp=_stamp(t.astimezone(JST))))
        iso = t.astimezone(JST).isoformat()
        return parse_amedas_map(data, self._station_index(), observed_at=iso, source_time=iso,
                                collected_at=datetime.now(TAIPEI_TZ).isoformat())

    def close_daily(self, now: datetime, max_days: int = 1) -> Optional[dict]:
        """日結可結的 JST 日（每輪最多 max_days 天）。回本輪摘要；沒事做回 None。失敗丟例外。"""
        if not config.JAPAN_JMA_AMEDAS_DAILY_PARQUET:
            return None
        from collectors.global_climate import jma_amedas_daily as dly
        from collectors.weather_raster.spool import read_json, sha256_file, write_json_atomic
        from storage.s3 import put_verified
        first_path = self.parquet_dir / "_first_day.json"
        first = (read_json(first_path, None) or {}).get("first_day")
        if not first:
            first = now.astimezone(JST).strftime("%Y%m%d")
            write_json_atomic(first_path, {"first_day": first})
        todo = [d for d in dly.closable_days(now, first) if not (self.parquet_dir / f"{d}.json").exists()]
        if not todo:
            return None
        s3 = self._s3_client()
        if s3 is None:
            print(f"[{self.name}] ⚠ S3 未設定，每日 Parquet 無法上傳（待日結 {todo}）")
            return {"pending": todo, "skipped": "no_s3"}
        done = []
        for day in todo[:max_days]:
            local = self.parquet_dir / f"{day}.parquet"
            info = dly.build_day_parquet(day, local, local_base=self.local_base, fetch_slot=self._fetch_slot)
            sha = sha256_file(local)
            key = dly.s3_key(day)
            if not put_verified(s3, local, key, sha, "STANDARD", "application/vnd.apache.parquet"):
                raise RuntimeError(f"{key} 上傳／驗證失敗")
            receipt = {"day": day, "key": key, "sha256": sha, "storage_class": "STANDARD",
                       "verified_at": datetime.now(timezone.utc).isoformat(), **info}
            # receipt（含 missing 明細）先以 sidecar 上傳驗證，成功才寫本機 receipt（本機 receipt 代表日結完成）
            rcpt_local = self.parquet_dir / f"{day}.receipt.upload.json"
            write_json_atomic(rcpt_local, receipt)
            rkey = dly.receipt_s3_key(day)
            if not put_verified(s3, rcpt_local, rkey, sha256_file(rcpt_local), "STANDARD", "application/json"):
                raise RuntimeError(f"{rkey} 上傳／驗證失敗")
            write_json_atomic(self.parquet_dir / f"{day}.json", receipt)
            rcpt_local.unlink(missing_ok=True)
            local.unlink(missing_ok=True)
            print(f"[{self.name}] ✓ 每日 Parquet {day}: {info['rows']:,} 列 {info['stations']} 站 "
                  f"本機 {info['slots_local']}/144 補抓 {info['slots_filled']} 缺 {info['slots_missing']} "
                  f"{info['bytes']:,}B → s3://{key}")
            done.append({k: receipt[k] for k in ("day", "rows", "slots_local", "slots_filled",
                                                 "slots_missing", "bytes")})
        return {"closed": done, "pending": todo[max_days:]}


if __name__ == "__main__":
    c = JmaAmedasCollector.__new__(JmaAmedasCollector)
    c.name = "jma_amedas"
    c._session = new_jma_session("jma-amedas-test")
    c._stations, c._stations_loaded_at, c._hours_done = {}, None, set()
    c._s3, c.parquet_dir, c.local_base = None, Path("/nonexistent"), Path("/nonexistent")
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
    print(out["data"][0])
