"""NCDR 各國颱風預測路徑收集器（JTWC／JMA／NMC／CWA／KMA）

資料來源：國家災害防救科技中心（國科會 NSTC 開放資料「各國颱風預測」）
  入口 https://dwfile.ncdr.nat.gov.tw/opendata/NSTC/TYRoute/TYRoute.kml 只是 NetworkLink 殼，
  實際資料在 https://satis.ncdr.nat.gov.tw/kml/multityroute.aspx（免金鑰，refreshInterval 1201 秒）。
  multityroute.aspx 沒有 Access-Control-Allow-Origin（2026-10-17 實測），前端不能直讀 → 入庫。

  KML 結構（2026-10-17 樣本，颱風薔琵）：
    每家一個 <Folder><name>JTWC</name>：6 個 Point Placemark（styleUrl #JTWC_00／_24／_48／_72／_96／_120）
    ＋ 1 條 LineString（#JTWCroute）。第一個 Point 的 name＝「JTWC發佈時間:10/17 20」。
    沒有颱風名稱與編號；發佈時間沒有年份與時區 → 推定 +08:00，年份取最接近收集時間者。
    各家 0 小時點是各家自己的分析（NMC 23 時、其他 20 時），不對齊成同一時刻。
    多顆颱風同時存在時的行為未實測：同一機構出現第二個 Folder 時 track_index 遞增，不覆蓋。

寫入：live.typhoon_agency_forecasts（gis-platform migration 476）
  UNIQUE(agency, issued_at, lead_hour, track_index) → ON CONFLICT DO NOTHING（同一版重抓不寫）
  颱風配對在 public.typhoon_agency_forecasts view 事後做，collector 不猜。

沒有颱風（KML 沒有任何機構 Folder／Point）：回傳不帶 'data'、status=no_active_storm。
HTTP／XML 錯誤、有 Folder 卻解析不出發佈時間 → raise。
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import mount_bounded_retry
from collectors.global_climate.typhoon_common import valid_position

URL_NCDR_MULTI_ROUTE = "https://satis.ncdr.nat.gov.tw/kml/multityroute.aspx"
AGENCIES = {"JTWC": "jtwc", "JMA": "jma", "NMC": "nmc", "CWA": "cwa", "KMA": "kma"}
MAX_BYTES = 2_000_000  # 樣本 23 KB；防異常大檔

_NS = re.compile(r"^\{[^}]*\}")
RE_ISSUED = re.compile(r"發佈時間\s*[:：]\s*(\d{1,2})/(\d{1,2})\s+(\d{1,2})")
RE_LEAD = re.compile(r"_(\d{1,3})$")


def _tag(el) -> str:
    return _NS.sub("", el.tag)


def _child(el, name: str):
    for c in el:
        if _tag(c) == name:
            return c
    return None


def _text(el, name: str) -> str:
    c = _child(el, name)
    return (c.text or "").strip() if c is not None and c.text else ""


def resolve_issued(month: int, day: int, hour: int, now: datetime) -> Optional[datetime]:
    """MM/DD HH（+08）→ 最接近 now 的年份（處理 12/31 → 1/1 跨年）。"""
    best = None
    for y in (now.year - 1, now.year, now.year + 1):
        try:
            cand = datetime(y, month, day, hour, tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
        if best is None or abs(cand - now) < abs(best - now):
            best = cand
    return best


def parse_multityroute(kml_text: str, now: datetime, collected_at: Optional[datetime] = None) -> dict:
    """純函式：KML 文字 → {'rows', 'agencies', 'no_active'}；結構錯誤 raise ValueError。"""
    if len(kml_text) > MAX_BYTES:
        raise ValueError(f"KML 過大：{len(kml_text)} bytes")
    if "<!ENTITY" in kml_text:
        raise ValueError("KML 含 ENTITY 宣告，拒絕解析")
    try:
        root = ET.fromstring(kml_text)
    except ET.ParseError as e:
        raise ValueError(f"KML 解析失敗：{e}") from e

    collected_iso = (collected_at or now).isoformat()
    rows: list[dict] = []
    agencies: list[str] = []
    seen: dict[str, int] = {}

    for folder in root.iter():
        if _tag(folder) != "Folder":
            continue
        agency = AGENCIES.get(_text(folder, "name").upper())
        if not agency:
            continue
        points = []
        issued = None
        issued_label = None
        for pm in folder:
            if _tag(pm) != "Placemark":
                continue
            pt = _child(pm, "Point")
            if pt is None:
                continue  # LineString 路徑線：由點重建，不另存
            for txt in (_text(pm, "name"), _text(pm, "description")):
                m = RE_ISSUED.search(txt)
                if m and issued is None:
                    mo, dd, hh = (int(x) for x in m.groups())
                    issued = resolve_issued(mo, dd, hh, now)
                    issued_label = f"{mo:02d}/{dd:02d} {hh:02d}"
            m_lead = RE_LEAD.search(_text(pm, "styleUrl"))
            coords = _text(pt, "coordinates").split(",")
            if not m_lead or len(coords) < 2:
                raise ValueError(f"{agency} Point 缺 styleUrl 時效或座標")
            try:
                lon, lat = float(coords[0]), float(coords[1])
            except ValueError as e:
                raise ValueError(f"{agency} 座標非數字：{coords[:2]}") from e
            points.append((int(m_lead.group(1)), round(lat, 5), round(lon, 5)))
        if not points:
            continue
        if issued is None:
            raise ValueError(f"{agency} 有 {len(points)} 個預報點但找不到「發佈時間」")
        track_index = seen.get(agency, 0)
        seen[agency] = track_index + 1
        agencies.append(agency)
        for lead, lat, lon in points:
            if not valid_position(lat, lon) or not (0 <= lead <= 240):
                continue
            rows.append({
                "agency":       agency,
                "track_index":  track_index,
                "issued_at":    issued.isoformat(),
                "issued_label": issued_label,
                "lead_hour":    lead,
                "valid_at":     (issued + timedelta(hours=lead)).isoformat(),
                "center_lat":   lat,
                "center_lon":   lon,
                "lat":          lat,
                "lon":          lon,
                "collected_at": collected_iso,
            })

    return {"rows": rows, "agencies": agencies, "no_active": not agencies}


class NcdrTyphoonRoutesCollector(BaseCollector):
    """NCDR 五家颱風預報路徑。無颱風時 idle（不寫列、status=no_active_storm）。"""

    name = "global_climate_ncdr_typhoon_routes"
    interval_minutes = config.GLOBAL_CLIMATE_NCDR_TYPHOON_ROUTES_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; GIS-DataCollectors/1.0; global-climate-ncdr-typhoon-routes)",
            "Accept": "application/vnd.google-earth.kml+xml, application/xml, */*",
        })
        mount_bounded_retry(self._session)

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        resp = self._session.get(URL_NCDR_MULTI_ROUTE, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        parsed = parse_multityroute(resp.text, now)
        base = {
            "agency_count": len(parsed["agencies"]),
            "agencies":     parsed["agencies"],
            "collected_at": now.isoformat(),
        }
        if parsed["no_active"]:
            return {**base, "status": "no_active_storm", "point_count": 0}
        result = {**base, "status": "ok", "data": parsed["rows"], "point_count": len(parsed["rows"])}
        if not parsed["rows"]:
            result["_collector_error"] = "有機構資料夾但 0 個點通過守門"
        return result
