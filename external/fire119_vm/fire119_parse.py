"""即時 119 案件＋警廣路況：來源註冊表、隱私剔除、解析、分類。

解析邏輯搬自 taipei-gis-analytics pipelines/realtime/realtime_incidents_probe/
（probe_119.py、normalize.py、normalize_traffic.py，2026-11-16 實測版），整理成正式模組：
  - 解析前先檢查「結構標記」（表頭、容器），找不到就丟 ParseError：
    版面改了會變成「解析失敗」告警，不會被當成「0 件」。
  - 隱私剔除（sanitize_*）在任何落地之前做：臺北 fuzzyAble≠N 的 csX/csY/csPlace、
    新北救護車軌跡（caseList.path）與目前位置（startPoint）一律不留。
  - TWD97 TM2（EPSG:3826）→ WGS84 用純 Python 公式（VM 不裝 pyproj）。
  - 只依賴標準庫＋beautifulsoup4（HTML 來源才 import）。

來源端點與頻率見 SOURCES；苗栗已改 https 無 port（11-16 實測舊 http:8080 被拒）。
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone

TZ = timezone(timedelta(hours=8))
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")


class ParseError(Exception):
    """回應結構不符預期（版面改了、回錯頁）。和「解析成功但 0 筆」分開告警。"""


# ── 來源註冊表 ─────────────────────────────────────────────────────────
# interval：分鐘；phase：在 (分鐘數 % interval) == phase 的那一分鐘到期（錯開同頻來源）。
#   1 分鐘：清單容量小（臺中 4 筆、新北 0–4 筆、雲林 1–6 筆）。
#   2 分鐘：一般。重的（位元組多）放奇數分鐘，避開垃圾車每偶數分鐘那輪。
#   5 分鐘：清單長或案少；phase 只用 1／2／4，避開 :x0（ship_ais）與 :x8（isohe，≡3 mod 5）。
# zero_alert_hours：解析成功但連續 0 筆超過幾小時亮黃燈；None＝常態為 0，不告警。
SOURCES: dict[str, dict] = {
    "taichung": dict(county="臺中市", parser="taichung", interval=1, phase=0, zero_alert_hours=3,
                     url="https://www.fire.taichung.gov.tw/"),
    "new_taipei": dict(county="新北市", parser="new_taipei", interval=1, phase=0, zero_alert_hours=12,
                       url="https://e.ntpc.gov.tw/v3/api/map/dynamic/layer/rescue"),
    "yunlin": dict(county="雲林縣", parser="dts", interval=1, phase=0, zero_alert_hours=12,
                   url="http://119.ylfire.gov.tw:8080/DTS/caselist/html"),
    # 2 分鐘，奇數分鐘
    "taipei": dict(county="臺北市", parser="taipei", interval=2, phase=1, zero_alert_hours=3,
                   method="POST", body="{}",
                   headers={"Content-Type": "application/json", "Referer": "https://service119.tfd.gov.tw/"},
                   url="https://service119.tfd.gov.tw/service119/citizenCase/caseList"),
    "kaohsiung": dict(county="高雄市", parser="dts", interval=2, phase=1, zero_alert_hours=3,
                      url="https://119dts.fdkc.gov.tw/DTS/caselist/html"),
    "tainan": dict(county="臺南市", parser="dts", interval=2, phase=1, zero_alert_hours=6,
                   url="https://119dts.tncfd.gov.tw/DTS/caselist/html"),
    "changhua": dict(county="彰化縣", parser="changhua", interval=2, phase=1, zero_alert_hours=6,
                     url="https://www.chfd.gov.tw/RealInfo/index.aspx?Parser=99,3,29"),
    "keelung": dict(county="基隆市", parser="dts", interval=2, phase=1, zero_alert_hours=12,
                    url="https://klc-dt.klfd.klcg.gov.tw/DTS/caselist/html"),
    # 2 分鐘，偶數分鐘（輕的）
    "hsinchu_city": dict(county="新竹市", parser="dts", interval=2, phase=0, zero_alert_hours=12,
                         url="https://119.hcfd.gov.tw/DTS/caselist/html"),
    "miaoli": dict(county="苗栗縣", parser="dts", interval=2, phase=0, zero_alert_hours=12,
                   url="https://119mlfire.mlfd.gov.tw/DTS/caselist/html"),
    "chiayi_county": dict(county="嘉義縣", parser="dts", interval=2, phase=0, zero_alert_hours=12,
                          url="https://dt.cycfd.gov.tw/DTS/caselist/html"),
    "chiayi_city": dict(county="嘉義市", parser="dts", interval=2, phase=0, zero_alert_hours=12,
                        url="https://dt.cyfd.gov.tw/DTS/caselist/html"),
    "taitung": dict(county="臺東縣", parser="dts", interval=2, phase=0, zero_alert_hours=None,
                    url="https://dt.ttfd.gov.tw/DTS/caselist/html"),
    # 5 分鐘
    "pingtung": dict(county="屏東縣", parser="dts", interval=5, phase=1, zero_alert_hours=6,
                     url="https://ptloc119.ptfire.gov.tw/DTS/caselist/html"),
    "penghu": dict(county="澎湖縣", parser="dts", interval=5, phase=1, zero_alert_hours=None,
                   url="http://210.241.42.144:8080/DTS/caselist/html"),
    "nantou": dict(county="南投縣", parser="nantou", interval=5, phase=2, zero_alert_hours=12,
                   url="https://www.ntfd.gov.tw/index.php?act=caselist"),
    "yilan": dict(county="宜蘭縣", parser="dts", interval=5, phase=2, zero_alert_hours=None,
                  url="https://nfa119.e-land.gov.tw/DTS/caselist/html"),
    "taoyuan": dict(county="桃園市", parser="taoyuan", interval=5, phase=4, zero_alert_hours=12,
                    url="https://www.tyfd.gov.tw/cht/index.php?act=caselist"),
    # 警廣即時路況（data.gov.tw 15221）：滾動最後 1,000 筆，約 10–20 分鐘換一版
    "pbs": dict(county=None, parser="pbs", interval=5, phase=4, zero_alert_hours=2,
                url="https://rtr.pbs.gov.tw/NMP103_PbsWS/resources/roadData/opendata"),
}

RAW_EXT = {"taipei": "json", "new_taipei": "json", "pbs": "json"}


# ── 共用小工具 ─────────────────────────────────────────────────────────
def parse_local_dt(s: str | None) -> datetime | None:
    """來源的本地時間字串 → +08:00 aware datetime；解析不了回 None。"""
    s = (s or "").strip()
    for f in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(s, f).replace(tzinfo=TZ)
        except ValueError:
            pass
    return None


def split_units(s: str | None) -> list[str]:
    return [u.strip() for u in re.split(r"[,，、]", s or "") if u.strip()]


def to_float(v) -> float | None:
    """字串／數字 → float；0、NaN、無限大、空值一律 None（臺北 0.0＝缺值）。"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f == 0.0:
        return None
    return f


def rec(**k) -> dict:
    base = dict(native_id=None, category_raw="", status_raw="", received_at=None, location_text="",
                lon=None, lat=None, precision="none", units=[], hospital=None, dest_lon=None, dest_lat=None)
    base.update(k)
    return base


# ── TWD97 TM2（EPSG:3826，GRS80，中央經線 121°E，k0=0.9999，東偏 250 km）→ WGS84 ──
# TWD97 與 WGS84 差距在公分級，直接當 WGS84 用（同 pyproj 3826→4326 的預設轉換）。
_A = 6378137.0
_F = 1 / 298.257222101
_K0 = 0.9999
_LON0 = math.radians(121.0)
_FE = 250000.0


def twd97_to_wgs84(x: float, y: float) -> tuple[float, float]:
    """TM2 反算（Snyder 1987 式 8-18～8-25），台灣範圍內誤差 < 1e-6 度。"""
    e2 = _F * (2 - _F)
    ep2 = e2 / (1 - e2)
    m = y / _K0
    mu = m / (_A * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
            + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
            + (151 * e1 ** 3 / 96) * math.sin(6 * mu)
            + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
    c1 = ep2 * math.cos(phi1) ** 2
    t1 = math.tan(phi1) ** 2
    n1 = _A / math.sqrt(1 - e2 * math.sin(phi1) ** 2)
    r1 = _A * (1 - e2) / (1 - e2 * math.sin(phi1) ** 2) ** 1.5
    d = (x - _FE) / (n1 * _K0)
    lat = phi1 - (n1 * math.tan(phi1) / r1) * (
        d ** 2 / 2 - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
        + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720)
    lon = _LON0 + (d - (1 + 2 * t1 + c1) * d ** 3 / 6
                   + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120) / math.cos(phi1)
    return round(math.degrees(lon), 6), round(math.degrees(lat), 6)


# ── 隱私剔除（落地前）─────────────────────────────────────────────────
_NTPC_PROP_KEEP = ("featureId", "type", "fireType", "title", "endPointInfo", "lng", "lat", "layerName", "dataSource")


def sanitize_taipei(content: bytes) -> bytes:
    """fuzzyAble 不是 N（站方要隱藏）的列：剔除 csX、csY、csPlace。解析不了就丟 ParseError。"""
    try:
        d = json.loads(content)
    except ValueError as e:
        raise ParseError(f"臺北回應不是 JSON：{e}") from None
    if not isinstance(d, dict) or not isinstance(d.get("rows"), list):
        raise ParseError("臺北回應沒有 rows 陣列")
    for r in d["rows"]:
        if isinstance(r, dict) and r.get("fuzzyAble") != "N":
            for k in ("csX", "csY", "csPlace"):
                r.pop(k, None)
    return json.dumps(d, ensure_ascii=False).encode("utf-8")


def sanitize_new_taipei(content: bytes) -> bytes:
    """新北：用白名單重建（JSON 裡包 GeoJSON 字串）。只留醫院名稱與座標、救護車代號；
    軌跡（path）、救護車目前位置（startPoint）、剩餘距離（distance）一律不留。"""
    try:
        d = json.loads(content)
        g = json.loads(d["data"]) if isinstance(d.get("data"), str) else d.get("data")
    except (ValueError, TypeError, KeyError, AttributeError) as e:
        raise ParseError(f"新北回應結構不符：{e}") from None
    if not isinstance(g, dict) or not isinstance(g.get("features"), list):
        raise ParseError("新北回應沒有 features 陣列")
    feats = []
    for f in g["features"]:
        p = (f or {}).get("properties") or {}
        props = {k: p.get(k) for k in _NTPC_PROP_KEEP if k in p}
        cl = p.get("caseList") or []
        props["caseList"] = [{"startPointInfo": c.get("startPointInfo")} for c in cl
                             if isinstance(c, dict) and c.get("startPointInfo")]
        geom = f.get("geometry") if isinstance(f.get("geometry"), dict) and f["geometry"].get("type") == "Point" else None
        feats.append({"type": "Feature", "properties": props, "geometry": geom})
    out = {"status": d.get("status"), "message": d.get("message"),
           "data": json.dumps({"type": "FeatureCollection", "features": feats}, ensure_ascii=False)}
    return json.dumps(out, ensure_ascii=False).encode("utf-8")


def sanitize(source: str, content: bytes) -> bytes:
    parser = SOURCES[source]["parser"]
    if parser == "taipei":
        return sanitize_taipei(content)
    if parser == "new_taipei":
        return sanitize_new_taipei(content)
    return content


# ── 解析：bytes → [rec] ───────────────────────────────────────────────
def parse_taipei(b: bytes) -> list[dict]:
    try:
        d = json.loads(b)
    except ValueError as e:
        raise ParseError(f"臺北回應不是 JSON：{e}") from None
    if not isinstance(d, dict) or not isinstance(d.get("rows"), list):
        raise ParseError("臺北回應沒有 rows 陣列")
    out = []
    for r in d["rows"]:
        lon = lat = None
        prec = "none"
        # fuzzyAble=N（捷運出口、學校等公共場所）：csX/csY 是站方公開的精確點。
        # fuzzyAble=Y：只用 fuzzyX/Y（模糊點）；即使夾帶 csX/csY 也不採用（站方本意是隱藏，sanitize 已剔除）。
        # 沒有座標時 fuzzyX/fuzzyY 為 0.0 → to_float 回 None（缺值，不是座標）。
        cx, cy = to_float(r.get("csX")), to_float(r.get("csY"))
        fx, fy = to_float(r.get("fuzzyX")), to_float(r.get("fuzzyY"))
        if r.get("fuzzyAble") == "N" and cx and cy:
            lon, lat = twd97_to_wgs84(cx, cy)
            prec = "exact"
        elif r.get("fuzzyAble") == "Y" and fx and fy:
            lon, lat = twd97_to_wgs84(fx, fy)
            prec = "fuzzy"
        cat = (r.get("csKindName") or "") + (f"/{r['csCodeName']}" if r.get("csCodeName") else "")
        out.append(rec(category_raw=cat, status_raw=r.get("caseStatus") or "",
                       received_at=parse_local_dt(r.get("inTime")),
                       location_text=r.get("csPlaceFuzzy") or "", lon=lon, lat=lat, precision=prec))
    return out


def parse_new_taipei(b: bytes) -> list[dict]:
    """新北只有「送醫途中救護車」：事發地沒有座標，只存醫院（dest）與救護車代號（units）。"""
    try:
        d = json.loads(b)
        g = json.loads(d["data"]) if isinstance(d.get("data"), str) else d.get("data")
    except (ValueError, TypeError, KeyError, AttributeError) as e:
        raise ParseError(f"新北回應結構不符：{e}") from None
    if not isinstance(g, dict) or not isinstance(g.get("features"), list):
        raise ParseError("新北回應沒有 features 陣列")
    out = []
    for f in g["features"]:
        p = (f or {}).get("properties") or {}
        t = p.get("type") or ""
        units = [c.get("startPointInfo") for c in (p.get("caseList") or [])
                 if isinstance(c, dict) and c.get("startPointInfo")]
        amb = t.startswith("Ambulance")
        # 非救護車型別（實測未出現）語意未確認：不猜，當無位置、地點文字放 endPointInfo
        out.append(rec(native_id=p.get("featureId"),
                       category_raw=f"{t}|{p.get('fireType') or ''}|{p.get('title') or ''}",
                       status_raw=t, location_text="" if amb else (p.get("endPointInfo") or ""),
                       hospital=(p.get("endPointInfo") or None) if amb else None, units=units,
                       dest_lon=to_float(p.get("lng")) if amb else None,
                       dest_lat=to_float(p.get("lat")) if amb else None))
    return out


def _soup(b: bytes):
    from bs4 import BeautifulSoup  # HTML 來源才需要
    return BeautifulSoup(b, "html.parser")


def _table(b: bytes) -> list[dict]:
    """DTS／南投的 <table>：找到含「受理時間／受理案件時間」的表頭才算結構正確。"""
    s = _soup(b)
    hdr = None
    out = []
    for tr in s.find_all("tr"):
        cells = [re.sub(r"\s+", "", c.get_text(strip=True)) if c.name == "th" else c.get_text(strip=True)
                 for c in tr.find_all(["th", "td"])]
        if any(h in cells for h in ("受理時間", "受理案件時間")):
            hdr = [re.sub(r"\s+", "", c) for c in cells]
            continue
        if hdr and len(cells) == len(hdr):
            out.append(dict(zip(hdr, cells)))
    if hdr is None:
        raise ParseError("找不到含「受理時間」的表頭")
    return out


def parse_dts(b: bytes) -> list[dict]:
    out = []
    for r in _table(b):
        t = parse_local_dt(r.get("受理時間"))
        if not t:
            continue
        nid = r.get("編號")  # 只有臺南有；每日重號，所以和受理日期一起當 key（30 輪實測穩定）
        out.append(rec(native_id=f"{t:%Y%m%d}-{nid}" if nid else None,
                       category_raw=(r.get("案類") or "") + (f"/{r['案別']}" if r.get("案別") else ""),
                       status_raw=r.get("執行狀況") or "", received_at=t,
                       location_text=r.get("發生地點") or "", units=split_units(r.get("派遣分隊"))))
    return out


def parse_nantou(b: bytes) -> list[dict]:
    out = []
    for r in _table(b):
        t = parse_local_dt(r.get("受理案件時間"))
        if not t:
            continue
        out.append(rec(category_raw=(r.get("案類") or "") + (f"/{r['案別']}" if r.get("案別") else ""),
                       status_raw=r.get("執行狀況") or "", received_at=t,
                       location_text=r.get("發生地點") or "", units=split_units(r.get("派遣分隊"))))
    return out


def parse_taoyuan(b: bytes) -> list[dict]:
    s = _soup(b)
    rows = s.select("div.tr")
    if not any(tr.select_one("div.th") for tr in rows):
        raise ParseError("桃園找不到 div.tr 表頭")
    out = []
    for tr in rows:
        d = tr.find("date")
        if not d:
            continue
        tds = tr.select("div.td")

        def txt(i, title):
            return tds[i].get_text(" ", strip=True).removeprefix(title).strip() if len(tds) > i else ""
        kind = txt(2, "案別")
        out.append(rec(category_raw=txt(1, "案類") + (f"/{kind}" if kind else ""),
                       status_raw=txt(5, ""), received_at=parse_local_dt(d.get_text(strip=True)),
                       location_text=txt(3, "發生地點"), units=split_units(txt(4, "派遣分隊"))))
    return out


def parse_taichung(b: bytes) -> list[dict]:
    s = _soup(b)
    box = s.select_one("div.timely")
    if box is None or box.select_one("li.timely_head") is None:
        raise ParseError("臺中找不到 div.timely 即時案件區")
    out = []
    for li in box.select("ul li:not(.timely_head)"):
        sp = [x.get_text(strip=True) for x in li.find_all("span", recursive=False)]
        if len(sp) < 4:
            continue
        t = parse_local_dt(sp[0])
        if t:
            out.append(rec(category_raw=sp[1], status_raw=sp[3], received_at=t, location_text=sp[2]))
    return out


def parse_changhua(b: bytes) -> list[dict]:
    s = _soup(b)
    items = s.select("li.list_date")
    if not items and s.select_one("ul.rwd-table") is None:
        raise ParseError("彰化找不到 ul.rwd-table 案件清單")
    out = []
    for li in items:
        d = {}
        for sp in li.find_all("span", attrs={"data-th": True}):
            sr = sp.find("span", class_="sr-only")
            if sr:
                sr.extract()
            d[sp["data-th"].rstrip("：")] = sp.get_text(" ", strip=True)
        t = parse_local_dt(d.get("受理時間"))
        if t:
            out.append(rec(category_raw=(d.get("案類") or "") + (f"/{d['案別']}" if d.get("案別") else ""),
                           status_raw=d.get("執行狀況") or "", received_at=t,
                           location_text=d.get("發生地點") or "", units=split_units(d.get("派遣分隊"))))
    return out


def parse_pbs(b: bytes) -> list[dict]:
    """警廣：一列一 UID。座標 x1/y1 為 WGS84 字串；0 或空＝缺值。"""
    try:
        d = json.loads(b)
    except ValueError as e:
        raise ParseError(f"警廣回應不是 JSON：{e}") from None
    if not isinstance(d, dict) or not isinstance(d.get("result"), list):
        raise ParseError("警廣回應沒有 result 陣列")
    out = []
    for x in d["result"]:
        uid = (x.get("UID") or "").strip()
        if not uid:
            continue
        ht = (x.get("happentime") or "").split(".")[0]
        rt = x.get("roadtype") or ""
        mod = parse_local_dt((x.get("modDttm") or "").split(".")[0])
        out.append(dict(
            uid=uid, roadtype=rt or None, category="traffic_incident" if rt == "事故" else "other",
            comment=x.get("comment") or None, area=x.get("areaNm") or None, direction=x.get("direction") or None,
            happened_at=parse_local_dt(f"{x.get('happendate') or ''} {ht}"),
            lon=to_float(x.get("x1")), lat=to_float(x.get("y1")),
            status=pbs_status(x.get("comment")), mod_dttm=mod))
    return out


PARSERS = dict(taipei=parse_taipei, new_taipei=parse_new_taipei, dts=parse_dts, nantou=parse_nantou,
               taoyuan=parse_taoyuan, taichung=parse_taichung, changhua=parse_changhua, pbs=parse_pbs)


def parse(source: str, content: bytes) -> list[dict]:
    return PARSERS[SOURCES[source]["parser"]](content)


# ── 分類 ─────────────────────────────────────────────────────────────
_STATUS_MAP = [
    ("received", ("受理", "派遣中", "待派")),
    ("dispatched", ("出動", "出勤", "已派遣", "派遣")),
    ("arrived", ("到達現場", "已到達", "抵達", "到達")),
    ("transporting", ("送醫", "送往醫院", "到達醫院", "送院")),
    ("returned", ("返隊", "離院", "離開醫院", "已完成", "結案", "完成", "處理完")),
]


def map_category(raw: str | None) -> str:
    r = raw or ""
    if r.startswith("Ambulance") or "救護" in r:
        return "ems"
    if "火" in r:
        return "fire"
    if any(w in r for w in ("搶救", "救助", "搜救", "山域", "水域", "救援")):
        return "rescue"
    return "other"


def map_status(s: str | None, category: str | None = None) -> str:
    s = s or ""
    if s == "AmbulanceBack":  # 實測：路徑尾端＝醫院，救護車逐輪逼近 → 送醫途中
        return "transporting"
    if "離開現場" in s:  # 救護：離開現場多半送醫；其他案類：離開＝收隊
        return "transporting" if category == "ems" else "returned"
    if "到院" in s or "到達醫院" in s or "送醫" in s or "送往" in s:
        return "transporting"
    for k, kws in _STATUS_MAP:
        if any(w in s for w in kws):
            return k
    return "other"


def pbs_status(comment: str | None) -> str:
    if re.search(r"排除|已清除|恢復", comment or ""):
        return "cleared"
    if "處理中" in (comment or ""):
        return "ongoing"
    return "reported"


def incident_key(source: str, r: dict) -> str:
    """原生 id 加來源前綴（新北 ntpc:、臺南 tainan:）；其餘 sha1(source|received_at|location|category_raw)[:16]。
    不含狀態與分隊（會變）。"""
    nid = r.get("native_id")
    if nid:
        prefix = {"new_taipei": "ntpc"}.get(source, source)
        return f"{prefix}:{nid}"
    ra = r["received_at"].isoformat() if r.get("received_at") else None
    return hashlib.sha1(f"{source}|{ra}|{r.get('location_text') or ''}|{r.get('category_raw') or ''}"
                        .encode("utf-8")).hexdigest()[:16]
