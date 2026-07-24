"""矯正機關每日收容動態收集器（法務部矯正署，免金鑰）

資料來源：prisonmuseum.moj.gov.tw/jqw_pub/today.xml
  端點：GET https://prisonmuseum.moj.gov.tw/jqw_pub/today.xml
  格式：XML，每日全國總計 1 row：
    <Table>
      <日期>115/05/15</日期>          ← 民國年/月/日
      <實際收容>64005</實際收容>
      <男>57010</男>
      <女>6995</女>
      <核定容額>60552</核定容額>
      <超收率>5.7%</超收率>
      <入監人數>139</入監人數>
      <出監人數>149</出監人數>
    </Table>

  全國總計（非分機關）。每日 1 筆，建議排程一日數次抓但寫入 UPSERT by 日期。
  data.gov.tw nid 101185 對應，本 collector 直走原 XML 端點。

寫入：
  - live.prison_population_daily (UPSERT by observed_date)
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

PRISON_XML_URL = "https://prisonmuseum.moj.gov.tw/jqw_pub/today.xml"

# prisonmuseum.moj.gov.tw 憑證鏈不完整，verify=False 後關閉警告噪音
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _int(v) -> Optional[int]:
    try:
        return int(str(v).strip().replace(",", ""))
    except (TypeError, ValueError, AttributeError):
        return None


def _pct(v) -> Optional[float]:
    """「5.7%」→ 5.7 浮點"""
    if v is None:
        return None
    s = str(v).strip().rstrip("%")
    try:
        return float(s)
    except ValueError:
        return None


def _roc_date(s: str | None) -> Optional[date]:
    """115/05/15 → 2026-05-15（民國 + 1911）"""
    if not s:
        return None
    m = re.match(r"(\d{2,3})/(\d{1,2})/(\d{1,2})", s.strip())
    if not m:
        return None
    try:
        y = int(m.group(1)) + 1911
        return date(y, int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


class CorrectionalDailySnapshotCollector(BaseCollector):
    """矯正機關每日收容動態收集器（每日 1 次足以，配 24 小時 interval）"""

    name = "correctional_daily_snapshot"
    interval_minutes = config.CORRECTIONAL_DAILY_SNAPSHOT_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (correctional-daily-snapshot)",
            "Accept": "application/xml,text/xml,*/*",
        })
        self._session.verify = False  # prisonmuseum.moj.gov.tw 憑證鏈不完整

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)

        resp = self._session.get(PRISON_XML_URL, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        # 來源宣告 utf-8，去 BOM 後再 parse
        text = resp.content.decode("utf-8-sig")
        root = ET.fromstring(text)

        table = root.find("Table")
        if table is None:
            return {"data": [], "row_count": 0, "collected_at": now.isoformat(),
                    "note": "no Table element"}

        def _t(tag): return (table.findtext(tag) or "").strip() or None

        observed_date = _roc_date(_t("日期"))
        if observed_date is None:
            return {"data": [], "row_count": 0, "collected_at": now.isoformat(),
                    "note": f"bad date: {_t('日期')}"}

        record = {
            "observed_date":      observed_date.isoformat(),
            "total_inmates":      _int(_t("實際收容")),
            "male_inmates":       _int(_t("男")),
            "female_inmates":     _int(_t("女")),
            "approved_capacity":  _int(_t("核定容額")),
            "over_capacity_pct":  _pct(_t("超收率")),
            "new_in_count":       _int(_t("入監人數")),
            "new_out_count":      _int(_t("出監人數")),
            "collected_at":       now.isoformat(),
        }

        return {
            "data":         [record],
            "row_count":    1,
            "collected_at": now.isoformat(),
        }
