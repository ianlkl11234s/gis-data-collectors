"""JTWC 颱風位置收集器（time-point decomposed）

資料來源：美軍聯合颱風警報中心（免認證）
  端點：
    https://www.metoc.navy.mil/jtwc/rss/jtwc.rss
    https://www.metoc.navy.mil/jtwc/products/wp{NN}{YY}web.txt  (西太編號)
  特性：
    - RSS 列出當前所有 active TC + 對應 .txt URL
    - ATCF 文字含 WARNING POSITION + FORECASTS（12/24/36/48/72/96/120 HR）
    - 風速 1-min sustained（JMA 是 10-min，JTWC 高 12-15%）
    - 無 TC 時 RSS 仍可拿但無 <item>

寫入：realtime.typhoon_positions (source='jtwc')
  - UNIQUE(storm_id, source, valid_at, point_type, advisory_number)
  - ON CONFLICT DO NOTHING
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

URL_JTWC_RSS = "https://www.metoc.navy.mil/jtwc/rss/jtwc.rss"

# ATCF 段提取
RE_WARNING_NR = re.compile(r"WARNING\s+NR\s+(\d+)", re.IGNORECASE)
RE_NAME = re.compile(r"\((\w+)\)")  # (MEKKHALA) capture
RE_DTG = re.compile(r"^\s*(\d{6})Z\s+---?\s+NEAR\s+([\d.]+)\s*([NS])\s+([\d.]+)\s*([EW])", re.MULTILINE)
RE_FORECAST_BLOCK = re.compile(
    r"(\d+)\s+HRS,?\s+VALID\s+AT:\s*\n\s*(\d{6})Z\s+---?\s+([\d.]+)\s*([NS])\s+([\d.]+)\s*([EW])\s*\n\s*MAX SUSTAINED WINDS\s*-\s*(\d+)\s+KT",
    re.IGNORECASE,
)
RE_WARN_POS_DTG = re.compile(
    r"WARNING POSITION:\s*\n\s*(\d{6})Z\s+---?\s+(?:NEAR\s+)?([\d.]+)\s*([NS])\s+([\d.]+)\s*([EW])",
    re.IGNORECASE,
)
RE_PRESENT_WINDS = re.compile(
    r"PRESENT WIND DISTRIBUTION:.*?MAX SUSTAINED WINDS\s*-\s*(\d+)\s+KT",
    re.DOTALL | re.IGNORECASE,
)


def _parse_dtg(dtg: str, ref_year_month: tuple[int, int]) -> Optional[datetime]:
    """ATCF DTG (e.g. '271200') = day/hour/minute, 補上參考年月轉成 UTC。"""
    if len(dtg) != 6:
        return None
    try:
        day = int(dtg[0:2])
        hour = int(dtg[2:4])
        minute = int(dtg[4:6])
        year, month = ref_year_month
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _latlon(lat_val: float, lat_dir: str, lon_val: float, lon_dir: str) -> tuple[float, float]:
    lat = lat_val * (1 if lat_dir.upper() == "N" else -1)
    lon = lon_val * (1 if lon_dir.upper() == "E" else -1)
    return lat, lon


class JtwcCollector(BaseCollector):
    """JTWC 颱風 (time-point decomposed) 收集器。無颱風 idle。"""

    name = "global_climate_jtwc"
    interval_minutes = config.GLOBAL_CLIMATE_JTWC_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; global-climate-jtwc)",
        })

    def _fetch_text(self, url: str) -> Optional[str]:
        try:
            resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return None
            return resp.text
        except Exception:
            return None

    def _list_active(self, rss_text: str) -> list[dict]:
        """從 RSS CDATA HTML 解析 active TC + .txt URL。

        典型結構：
          <description><![CDATA[
            <p><b>Tropical Storm 07W (Mekkhala) Warning #36
            <a href='https://www.metoc.navy.mil/jtwc/products/wp0726web.txt'>TC Warning Text</a>
            ...
          ]]></description>
        """
        out: list[dict] = []
        # 抓所有 wp\d{4}web.txt（西太），ep/io basin 可後續擴充
        for m in re.finditer(
            r"href='?(https?://[^'\">]*?(wp|ep|io|sh)(\d{4})web\.txt)'?",
            rss_text,
            re.IGNORECASE,
        ):
            url = m.group(1)
            basin = m.group(2).lower()  # wp / ep / io / sh
            storm_id = f"{basin}{m.group(3)}"  # e.g. wp0726
            if not any(o["storm_id"] == storm_id for o in out):
                out.append({"storm_id": storm_id, "warning_url": url, "basin": basin})
        return out

    def _parse_warning(self, storm_id: str, atcf_text: str, collected_at: datetime) -> list[dict]:
        """解 ATCF WARNING text → typhoon_position rows。"""
        rows: list[dict] = []

        # advisory number
        m_nr = RE_WARNING_NR.search(atcf_text)
        advisory_number = int(m_nr.group(1)) if m_nr else None

        # name
        m_name = RE_NAME.search(atcf_text)
        name_en = m_name.group(1).title() if m_name else None  # MEKKHALA → Mekkhala

        # 推算參考年月：用 collected_at 為基準（DTG 只給 day/hour）
        # 月底跨月會錯一天，但 collector 每 6h 跑可接受
        ref_year_month = (collected_at.year, collected_at.month)

        # 1. WARNING POSITION (現在) + PRESENT WIND
        m = RE_WARN_POS_DTG.search(atcf_text)
        m_wind = RE_PRESENT_WINDS.search(atcf_text)
        if m:
            dtg, lat_v, lat_d, lon_v, lon_d = m.groups()
            valid_at = _parse_dtg(dtg, ref_year_month)
            if valid_at:
                lat, lon = _latlon(float(lat_v), lat_d, float(lon_v), lon_d)
                max_wind = int(m_wind.group(1)) if m_wind else None
                rows.append(self._make_row(
                    storm_id, "observed", valid_at, advisory_number, collected_at,
                    name_en=name_en, lat=lat, lon=lon, max_wind_kt=max_wind,
                ))

        # 2. FORECASTS
        for m in RE_FORECAST_BLOCK.finditer(atcf_text):
            hrs, dtg, lat_v, lat_d, lon_v, lon_d, max_wind = m.groups()
            valid_at = _parse_dtg(dtg, ref_year_month)
            if not valid_at:
                continue
            lat, lon = _latlon(float(lat_v), lat_d, float(lon_v), lon_d)
            rows.append(self._make_row(
                storm_id, "forecast", valid_at, advisory_number, collected_at,
                name_en=name_en, lat=lat, lon=lon, max_wind_kt=int(max_wind),
            ))

        return rows

    def _make_row(self, storm_id, point_type, valid_at, advisory_number,
                  collected_at, *, name_en=None, lat, lon, max_wind_kt=None):
        return {
            "storm_id":            storm_id,
            "source":              "jtwc",
            "valid_at":            valid_at.isoformat(),
            "point_type":          point_type,
            "advisory_number":     advisory_number,
            "advisory_issued_at":  collected_at.isoformat(),
            "name_local":          None,
            "name_en":             name_en,
            "center_lat":          lat,
            "center_lon":          lon,
            "center_pressure_hpa": None,
            "max_wind_kt":         max_wind_kt,
            "gale_radius_km":      None,
            "storm_radius_km":     None,
            "lon":                 lon,
            "lat":                 lat,
            "raw_json":            None,
            "collected_at":        collected_at.isoformat(),
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        rss = self._fetch_text(URL_JTWC_RSS) or ""
        storms = self._list_active(rss)

        all_rows: list[dict] = []
        for s in storms:
            atcf = self._fetch_text(s["warning_url"])
            if not atcf:
                continue
            rows = self._parse_warning(s["storm_id"], atcf, now)
            all_rows.extend(rows)

        return {
            "data":           all_rows,
            "storm_count":    len(storms),
            "active_storms":  [s["storm_id"] for s in storms],
            "point_count":    len(all_rows),
            "collected_at":   now.isoformat(),
        }


if __name__ == "__main__":
    c = JtwcCollector.__new__(JtwcCollector)
    c._session = requests.Session()
    c._session.headers.update({"User-Agent": "Mozilla/5.0 (jtwc-test)"})
    out = c.collect()
    print(f"storms: {out['storm_count']}, points: {out['point_count']}")
    print(f"active: {out['active_storms']}")
    if out["data"]:
        print(f"sample: {out['data'][0]}")
