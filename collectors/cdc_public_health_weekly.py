"""
CDC 公衛週報收集器（類流感 / 登革熱 / 腸病毒）

⚠️⚠️⚠️  Taiwan IP required — runs on external VM  ⚠️⚠️⚠️
   Zeabur 出口 IP 連 od.cdc.gov.tw timeout（2026-06-16 實證）。
   主 repo 此 collector 在 Zeabur 必設 CDC_PUBLIC_HEALTH_WEEKLY_ENABLED=false，
   實際 schedule 由 external/cdc_public_health_weekly_vm/ 在 HiCloud VM 跑。
   本檔仍保留完整實作 = schema / parser SSOT，VM 版照抄。
   詳見 docs/EXTERNAL_COLLECTORS.md。

資料來源：data.cdc.gov.tw 開放資料平台
  下載走 od.cdc.gov.tw/eic/{dataset_id}.csv（CSV 直下載，免認證）

  3 個 dataset：
    rods-influenza                類流感急診就診（年/週/年齡/縣市/就診數）
    aagstable-weekly-dengue       登革熱週確診（年/週/縣市/鄉鎮/性別/境外/年齡/病例數）
    rods-enteroviral-infection    腸病毒急診（年/週/年齡/縣市/就診數）

每週四發布上週資料。

⚠ CDC 端點同 NHI 憑證缺 SKI → verify=False 必開。

寫入：
  - realtime.public_health_weekly（UPSERT by 複合 key）
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# CDC 同 NHI 憑證缺 SKI
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CDC_CSV_BASE = "https://od.cdc.gov.tw/eic"

# (disease_code, dataset_filename, column_mapping)
# 檔名取自 data.cdc.gov.tw CKAN package_show 真實 resources[].url
DATASETS = [
    {
        "disease_code":  "influenza",
        "filename":      "RODS_Influenza_like_illness.csv",
        "dataset_id":    "rods-influenza",
        "kind":          "rods",     # 年/週/年齡/縣市/就診數/縣市碼
    },
    {
        "disease_code":  "dengue",
        "filename":      "Weekly_Age_County_Gender_061.csv",
        "dataset_id":    "aagstable-weekly-dengue",
        "kind":          "dengue",   # 多欄含鄉鎮/性別/境外
    },
    {
        "disease_code":  "enterovirus",
        "filename":      "RODS_EnteroviralInfection.csv",
        "dataset_id":    "rods-enteroviral-infection",
        "kind":          "rods",
    },
]


def _num(v) -> Optional[float]:
    try:
        if v is None or v == "" or v == "-":
            return None
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    f = _num(v)
    return int(f) if f is not None else None


def _bool(v) -> Optional[bool]:
    if v is None or v == "":
        return None
    s = str(v).strip()
    if s in ("1", "Y", "y", "是", "true", "True"):
        return True
    if s in ("0", "N", "n", "否", "false", "False"):
        return False
    return None


def _norm_field(row: dict, *keys: str) -> str | None:
    """從 row 找第一個非空欄位。CDC 表頭中英文混用，兩邊都試。"""
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() not in ("", "-"):
            return str(v).strip()
    return None


def _parse_rods(row: dict, disease_code: str, dataset_id: str) -> dict | None:
    """類流感 / 腸病毒共用解析：年/週/年齡別/縣市/{類流感|腸病毒}急診就診人次/縣市別代碼"""
    year = _int(_norm_field(row, "年", "Year", "year"))
    week = _int(_norm_field(row, "週", "Week", "week"))
    # 流感欄名「類流感急診就診人次」、腸病毒「腸病毒急診就診人次」
    cnt  = _num(_norm_field(row,
                            "類流感急診就診人次", "腸病毒急診就診人次",
                            "就診數", "Cases", "cases"))
    county_name = _norm_field(row, "縣市", "County", "county")
    county_code = _norm_field(row, "縣市別代碼", "縣市碼", "CountyCode", "county_code")
    age_group   = _norm_field(row, "年齡別", "年齡", "AgeGroup", "age")
    if not year or not week or cnt is None:
        return None
    return {
        "disease_code":    disease_code,
        "iso_year":        year,
        "iso_week":        week,
        "county_code":     county_code or "",
        "county_name":     county_name,
        "township_code":   "",          # rods 無鄉鎮粒度
        "township_name":   None,
        "age_group":       age_group or "",
        "gender":          "",
        "is_imported":     None,
        "metric_value":    cnt,
        "source_dataset":  dataset_id,
    }


def _parse_dengue(row: dict, disease_code: str, dataset_id: str) -> dict | None:
    """登革熱：確定病名/發病年份/發病週別/縣市/鄉鎮/性別/是否為境外移入/年齡層/確定病例數/縣市別代碼/鄉鎮別代碼"""
    year = _int(_norm_field(row, "發病年份", "年", "Year", "year"))
    week = _int(_norm_field(row, "發病週別", "週", "Week", "week"))
    cnt  = _num(_norm_field(row, "確定病例數", "病例數", "Cases", "cases"))
    if not year or not week or cnt is None:
        return None
    county_name   = _norm_field(row, "縣市", "County", "county")
    county_code   = _norm_field(row, "縣市別代碼", "縣市碼", "CountyCode", "county_code")
    township_name = _norm_field(row, "鄉鎮", "Township", "township")
    township_code = _norm_field(row, "鄉鎮別代碼", "鄉鎮碼", "TownshipCode", "township_code")
    gender        = _norm_field(row, "性別", "Sex", "gender")
    is_imported   = _bool(_norm_field(row, "是否為境外移入", "境外", "Imported", "imported"))
    age_group     = _norm_field(row, "年齡層", "年齡", "AgeGroup", "age")
    return {
        "disease_code":    disease_code,
        "iso_year":        year,
        "iso_week":        week,
        "county_code":     county_code or "",
        "county_name":     county_name,
        "township_code":   township_code or "",
        "township_name":   township_name,
        "age_group":       age_group or "",
        "gender":          gender or "",
        "is_imported":     is_imported,
        "metric_value":    cnt,
        "source_dataset":  dataset_id,
    }


class CdcPublicHealthWeeklyCollector(BaseCollector):
    """CDC 公衛週報 3 疾病收集器（每週四 11:00 抓一次即可）"""

    name = "cdc_public_health_weekly"
    interval_minutes = config.CDC_PUBLIC_HEALTH_WEEKLY_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (cdc-public-health-weekly)",
            "Accept": "text/csv, application/csv, */*",
        })
        self._session.verify = False  # CDC SSL SKI 缺失

    def _fetch_csv(self, filename: str) -> list[dict]:
        url = f"{CDC_CSV_BASE}/{filename}"
        resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)

    # 只保留近 N 年資料（CDC 歷史可達 20+ 年，monitor 用 2 年足夠）
    KEEP_YEARS = 2

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        cutoff_year = now.year - self.KEEP_YEARS + 1
        all_records: list[dict] = []
        dataset_stats: dict[str, int] = {}

        for ds in DATASETS:
            try:
                rows = self._fetch_csv(ds["filename"])
            except requests.RequestException as e:
                print(f"[{self.name}] ⚠ 抓 {ds['filename']} 失敗: {e}")
                dataset_stats[ds["disease_code"]] = 0
                continue
            parser = _parse_dengue if ds["kind"] == "dengue" else _parse_rods
            parsed_count = 0
            for r in rows:
                n = parser(r, ds["disease_code"], ds["dataset_id"])
                if n and n["iso_year"] >= cutoff_year:
                    all_records.append(n)
                    parsed_count += 1
            dataset_stats[ds["disease_code"]] = parsed_count

        return {
            "data":           all_records,
            "dataset_stats":  dataset_stats,
            "total_rows":     len(all_records),
            "collected_at":   now.isoformat(),
        }
