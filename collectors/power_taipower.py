"""
台電即時電力供需收集器

資料來源：台灣電力公司 開放資料平台（service.taipower.com.tw，免金鑰）
  三支端點（皆 GET，每 10 分鐘更新）：
    1) 162595 系統供需   d006020/001.json  → records[4] 段（即時/今日預測/昨日/即時最高）
    2) 8931   各機組     d006001/001.json  → aaData（每機組裝置容量/淨發電量）
    3) 162596 區域用電   d006019/001.csv   → 北/中/南/東 4 列（發電/用電）

單位：162595 / 162596 原始為「萬瓩」→ ×10 轉 MW；8931 本身即 MW，不轉。

寫入（schema=realtime）：
  - realtime.power_system_status     (每 10 分一列，UNIQUE(observed_at) DO NOTHING)
  - realtime.power_generation_unit   (每 10 分 ×N 機組，UNIQUE(unit_name, observed_at) DO NOTHING)
  - realtime.power_region_demand     (每 10 分 ×4 區，UNIQUE(region, observed_at) DO NOTHING)

注意：base.py 只在 result 含 'data' 鍵時才 save + 寫 Supabase，
故 collect() 回傳必含 'data'（= system_status 列），多表資料另以
'generation_units' / 'region_demand' 鍵掛上，supabase_writer 走 is_multi_table 取用。
"""

from __future__ import annotations

import csv
import io
import json as _json
import re
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# 台電端點憑證缺 Subject Key Identifier（同 NHI），Python TLS 預設驗證會失敗 → verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL_SYSTEM = "https://service.taipower.com.tw/data/opendata/apply/file/d006020/001.json"
URL_UNITS = "https://service.taipower.com.tw/data/opendata/apply/file/d006001/001.json"
URL_REGION = "https://service.taipower.com.tw/data/opendata/apply/file/d006019/001.csv"

WAN_KW_TO_MW = 10.0  # 1 萬瓩 = 10 MW


def _num(v) -> Optional[float]:
    """解析數值：去 %、去空白、空值/'-'/None → None，否則 float。"""
    if v is None:
        return None
    s = str(v).strip().replace("%", "").replace(",", "")
    if s == "" or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _wan_to_mw(v) -> Optional[float]:
    n = _num(v)
    return None if n is None else n * WAN_KW_TO_MW


def _floor_10min(dt: datetime) -> datetime:
    """向下取整到 10 分鐘（秒/微秒歸零），確保 observed_at 去重穩定。"""
    return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)


def _parse_obs(s: str | None) -> Optional[datetime]:
    """解析台電觀測時間 → 補台灣時區的 tz-aware datetime。
    8931 DateTime 形如 '2026-06-05T10:30:00'；162596 時間 形如 '20260605T103000'。
    必須 tz-aware，否則寫入 timestamptz 會被 UTC session 誤判 +8h（見 base.py 註記）。"""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y%m%dT%H%M%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


class PowerTaipowerCollector(BaseCollector):
    """台電即時電力供需收集器（每 10 分鐘，對齊來源更新頻率）"""

    name = "power_taipower"
    interval_minutes = config.POWER_TAIPOWER_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; power-taipower)",
        })
        self._session.verify = False  # 台電憑證缺 SKI

    def _get(self, url: str) -> requests.Response:
        resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp

    def _get_json(self, url: str) -> dict:
        """JSON 端點帶 UTF-8 BOM → resp.json() 會炸，改用 utf-8-sig decode 後解析。"""
        text = self._get(url).content.decode("utf-8-sig")
        return _json.loads(text)

    # ------------------------------------------------------------
    # 162595 系統供需 → 單列 system_status
    # ------------------------------------------------------------
    def _fetch_system(self, observed_at: datetime, collected_at: datetime) -> dict:
        data = self._get_json(URL_SYSTEM)
        records = data.get("records") or []

        # records index 順序不保證固定 → 用欄位特徵辨識每一段
        seg_curr = next((r for r in records if isinstance(r, dict) and "curr_load" in r), {})
        seg_fore = next((r for r in records if isinstance(r, dict) and "fore_peak_dema_load" in r), {})
        seg_yday = next((r for r in records if isinstance(r, dict) and "yday_peak_dema_load" in r), {})
        seg_real = next((r for r in records if isinstance(r, dict) and "real_hr_maxi_sply_capacity" in r), {})

        return {
            "observed_at":                   observed_at,
            "curr_load_mw":                  _wan_to_mw(seg_curr.get("curr_load")),
            "curr_util_rate":                _num(seg_curr.get("curr_util_rate")),
            "fore_maxi_sply_capacity_mw":    _wan_to_mw(seg_fore.get("fore_maxi_sply_capacity")),
            "fore_peak_dema_load_mw":        _wan_to_mw(seg_fore.get("fore_peak_dema_load")),
            "fore_peak_resv_capacity_mw":    _wan_to_mw(seg_fore.get("fore_peak_resv_capacity")),
            "fore_peak_resv_rate":           _num(seg_fore.get("fore_peak_resv_rate")),
            "fore_peak_resv_indicator":      (seg_fore.get("fore_peak_resv_indicator") or None),
            "fore_peak_hour_range":          (seg_fore.get("fore_peak_hour_range") or None),
            "yday_peak_resv_rate":           _num(seg_yday.get("yday_peak_resv_rate")),
            "yday_peak_resv_indicator":      (seg_yday.get("yday_peak_resv_indicator") or None),
            "real_hr_maxi_sply_capacity_mw": _wan_to_mw(seg_real.get("real_hr_maxi_sply_capacity")),
            "real_hr_peak_time":             (seg_real.get("real_hr_peak_time") or None),
            "publish_time":                  (seg_fore.get("publish_time") or None),
            "collected_at":                  collected_at,
        }

    # ------------------------------------------------------------
    # 8931 各機組 → N 列 generation_unit（本身即 MW，不轉）
    # ------------------------------------------------------------
    def _fetch_units(self, collected_at: datetime) -> list[dict]:
        data = self._get_json(URL_UNITS)
        observed_at = _parse_obs(data.get("DateTime"))  # 頂層觀測時間 → tz-aware
        rows = data.get("aaData") or []
        out: list[dict] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            unit_name = (r.get("機組名稱") or "").strip()
            if not unit_name:
                continue
            # ⚠ aaData 混入分類「小計」加總列（機組名稱='小計'，淨值是 '13854.0(37.4%)' 字串）
            #   → 非真機組，全名相同會被 UNIQUE 塌成 1 列污染資料，必須濾除
            if "小計" in unit_name or "合計" in unit_name:
                continue
            # ⚠ 機組類型偶帶 HTML 殘留（如 '儲能負載(...)</b>'）→ 去標籤
            fuel_type = re.sub(r"<[^>]*>", "", r.get("機組類型") or "").strip() or None
            out.append({
                "observed_at":  observed_at,
                "fuel_type":    fuel_type,
                "unit_name":    unit_name,
                "capacity_mw":  _num(r.get("裝置容量(MW)")),
                "net_gen_mw":   _num(r.get("淨發電量(MW)")),
                "util_rate":    _num(r.get("淨發電量/裝置容量比(%)")),
                "note":         (r.get("備註") or "").strip() or None,
                "collected_at": collected_at,
            })
        return out

    # ------------------------------------------------------------
    # 162596 區域用電 → 4 列 region_demand（萬瓩 → MW）
    # ------------------------------------------------------------
    def _fetch_region(self, collected_at: datetime) -> list[dict]:
        # utf-8-sig 處理 CSV 開頭 BOM；逐欄位再 strip BOM 防殘留
        text = self._get(URL_REGION).content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        out: list[dict] = []
        for row in reader:
            # 欄名/值可能殘留 BOM ﻿ → 一律 strip
            clean = {(k or "").strip().lstrip("﻿"): (v or "").strip().lstrip("﻿")
                     for k, v in row.items()}
            region = clean.get("區域")
            if not region:
                continue
            out.append({
                "observed_at":     _parse_obs(clean.get("時間")),  # 形如 20260605T101000 → tz-aware
                "region":          region,
                "generation_mw":   _wan_to_mw(clean.get("發電(萬瓩)")),
                "consumption_mw":  _wan_to_mw(clean.get("用電(萬瓩)")),
                "collected_at":    collected_at,
            })
        return out

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        # 162595 無自身 10 分時間戳 → 用 collected_at 向下取整到 10 分當 observed_at
        observed_at = _floor_10min(now)
        collected_iso = now.isoformat()

        system_row = self._fetch_system(observed_at, now)
        units = self._fetch_units(now)
        regions = self._fetch_region(now)

        # 序列化 datetime（buffer fallback / JSON-safe）
        system_row["observed_at"] = system_row["observed_at"].isoformat()
        system_row["collected_at"] = collected_iso
        for u in units:
            u["observed_at"] = u["observed_at"].isoformat() if u["observed_at"] else None
            u["collected_at"] = collected_iso
        for r in regions:
            r["observed_at"] = r["observed_at"].isoformat() if r["observed_at"] else None
            r["collected_at"] = collected_iso

        return {
            "data":             [system_row],   # base.py 需要 'data' 鍵才會 save + 寫 DB
            "system_status":    [system_row],   # 顯式別名（與 supabase_writer 取用一致）
            "generation_units": units,
            "region_demand":    regions,
            "system_count":     1,
            "unit_count":       len(units),
            "region_count":     len(regions),
            "collected_at":     collected_iso,
        }


if __name__ == "__main__":
    # 離線試跑：python3 -m collectors.power_taipower
    c = PowerTaipowerCollector.__new__(PowerTaipowerCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (power-taipower-test)"})
    c._session.verify = False
    out = c.collect()
    print("system:", out["system_count"], "units:", out["unit_count"], "regions:", out["region_count"])
    print("system_row:", out["system_status"][0])
    if out["generation_units"]:
        print("unit[0]:", out["generation_units"][0])
    for r in out["region_demand"]:
        print("region:", r)
