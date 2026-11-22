"""台灣即時快照三件（B8，2026-11-21）的純解析邏輯：只用標準函式庫，主 repo collector 與 VM 鏡像共用同一份。

- dengue：疾管署近兩週本土登革熱聚集區（od.cdc.gov.tw，22 縣市檔＋_all；data.gov.tw 179291）
- nav：航港局航船布告（aton.motcmpb.gov.tw；data.gov.tw 179037）
- marine_alerts：氣象署大潮 O-B0069-001＋長浪 O-B0070-001，座標依 O-B0076-001 名稱對照

規格正本與實測數字：taipei-gis-analytics docs/topic-research/tw_layer_ext_20261120/b8/layer-plan.md；
同邏輯的研究版 pipeline：analytics pipelines/{poi/dengue_cluster_snapshot,marine/motcmpb_nav_notices,marine/cwa_tide_swell_alerts}/。
VM 版（external/tw_live_snapshots_vm/）部署時把本檔 scp 過去，不要另抄一份。

結果一律回 (主要 FeatureCollection, status dict)；不寫檔、不連網。缺值留 None，不補 0、不補座標。
"""
from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

TPE = timezone(timedelta(hours=8))

# ─── dengue ──────────────────────────────────────────────────────────────────
DENGUE_BASE = "https://od.cdc.gov.tw/eic/DengueCluster/"
DENGUE_CITIES = ["Changhua", "Chiayi", "ChiayiC", "Hsinchu", "HsinchuC", "Hualien", "Kaohsiung", "Keelung", "Kinmen",
                 "Lienchiang", "Miaoli", "Nantou", "NewTaipei", "Penghu", "Pingtung", "Taichung", "Tainan", "Taipei",
                 "Taitung", "Taoyuan", "Yilan", "Yunlin"]
TW_BBOX = (118.0, 21.5, 123.0, 26.5)


def _ring_ok(geom: dict) -> bool:
    if (geom or {}).get("type") not in {"Polygon", "MultiPolygon"}:
        return False
    polys = [geom["coordinates"]] if geom["type"] == "Polygon" else geom["coordinates"]
    w, s, e, n = TW_BBOX
    for poly in polys:
        for ring in poly:
            if len(ring) < 4 or ring[0] != ring[-1]:
                return False
            if any(not (w <= x <= e and s <= y <= n) for x, y in ring):
                return False
    return True


def dengue_normalise(files: dict[str, dict], snapshot_date: str, retrieved_at: str) -> tuple[dict, dict]:
    """files：{'Tainan': FeatureCollection, …, 'all': FeatureCollection}。空集合照實回 0 筆。"""
    for name, fc in files.items():
        if (fc or {}).get("type") != "FeatureCollection" or not isinstance(fc.get("features"), list):
            raise ValueError(f"DengueCluster_{name}: not a FeatureCollection")
    feats: dict[str, dict] = {}
    issues = Counter()
    for name in DENGUE_CITIES:
        for f in (files.get(name) or {"features": []})["features"]:
            p = f.get("properties") or {}
            code = str(p.get("CODE1") or "").strip()
            if not code or not _ring_ok(f.get("geometry")):
                issues["bad_geometry"] += 1
                continue
            try:
                count = int(str(p.get("COUNT")).strip())
            except ValueError:
                count = None
            if count is not None and count < 2:
                issues["count_lt_2"] += 1
            feats[code] = {"type": "Feature", "geometry": f["geometry"], "properties": {
                "code1": code, "county": p.get("COUNTY"), "town": p.get("TOWN"), "village": p.get("VILLAGE"),
                "case_count": count, "center_lon": p.get("lon"), "center_lat": p.get("lat"),
                "source_file": f"DengueCluster_{name}.json", "snapshot_date": snapshot_date, "retrieved_at": retrieved_at}}
    all_codes = {str((f.get("properties") or {}).get("CODE1") or "").strip() for f in files.get("all", {"features": []})["features"]}
    fc = {"type": "FeatureCollection", "name": "dengue_cluster_latest",
          "features": sorted(feats.values(), key=lambda f: f["properties"]["code1"])}
    status = {"snapshot_date": snapshot_date, "retrieved_at": retrieved_at, "feature_count": len(feats),
              "snapshot_empty": not feats, "issues": dict(issues),
              "per_file_features": {n: len((files.get(n) or {"features": []})["features"]) for n in DENGUE_CITIES},
              "all_matches_city_union": all_codes == set(feats) if "all" in files else None}
    return fc, status


# ─── nav notices ─────────────────────────────────────────────────────────────
NAV_URL = "https://aton.motcmpb.gov.tw/navmark/get_notice_opendata/"
_NAME = re.compile(r"[\u3400-\u9fff\uf900-\ufaff]{1,3}(?:先生|小姐|女士)|(?<=聯絡人[：:])\s*[\u3400-\u9fff\uf900-\ufaff]{2,3}(?=[\s\d(（#]|$)")
_PHONE = re.compile(r"(?:\(0\d{1,2}\)\s?|0\d{1,2}-?)\d{3,4}-?\d{3,4}(?:\s?(?:轉|#|分機)\s?\d{1,5})?|09\d{2}-?\d{3}-?\d{3}")
_NM = 1852.0
_EARTH_R = 6371008.8


def _ts(s) -> str | None:
    s = (s or "").strip()
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=TPE).isoformat()
        except ValueError:
            continue
    return None


def _num(v):
    try:
        x = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _dest(lon: float, lat: float, az_deg: float, dist_m: float) -> list[float]:
    """球面大圓推算（半徑數 nm 內與 WGS84 橢球差 <0.5%）。"""
    d, az = dist_m / _EARTH_R, math.radians(az_deg)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(az))
    l2 = l1 + math.atan2(math.sin(az) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return [round(math.degrees(l2), 6), round(math.degrees(p2), 6)]


def _derive_polygon(center, radius_m: float, b0, b1) -> dict:
    lon, lat = center
    if b0 is None or b1 is None:
        ring = [_dest(lon, lat, i * 5.0, radius_m) for i in range(72)]
        ring.append(ring[0])
    else:
        span = (b1 - b0) % 360 or 360.0
        n = max(int(span // 5), 1)
        ring = [[lon, lat]] + [_dest(lon, lat, b0 + span * i / n, radius_m) for i in range(n + 1)] + [[lon, lat]]
    return {"type": "Polygon", "coordinates": [ring]}


def _coords(g: dict) -> list:
    t, c = g["type"], g["coordinates"]
    return [c] if t == "Point" else c if t == "LineString" else [pt for ring in c for pt in ring] if t == "Polygon" else []


def nav_normalise(src: dict, now: datetime) -> tuple[dict, dict, dict]:
    """回 (latest＝有效期間內, archive＝合併後全部, status)。"""
    rows = (src or {}).get("features")
    if (src or {}).get("type") != "FeatureCollection" or not rows:
        raise ValueError("nav notices: not a non-empty FeatureCollection")
    nowi = now.astimezone(TPE).replace(microsecond=0).isoformat()
    groups: dict[tuple, list] = defaultdict(list)
    unlocated: dict[str, str] = {}
    for f in rows:
        p = f["properties"]
        if not f.get("geometry"):
            unlocated.setdefault(p["code"], p.get("noticekind"))
            continue
        groups[(p["code"], p["shape_name"], json.dumps(f["geometry"], sort_keys=True))].append(f)
    feats, masked, name_masked = [], 0, 0
    for (code, shape_name, _), fs in groups.items():
        p, geom = fs[0]["properties"], fs[0]["geometry"]
        # 來源 shape_start_times／shape_end_times 恆對調（2026-11-21 實測 721／721）→ end 欄是起、start 欄是迄
        windows = sorted({(_ts(x["properties"].get("shape_end_times")), _ts(x["properties"].get("shape_start_times")))
                          for x in fs if x["properties"].get("shape_start_times") or x["properties"].get("shape_end_times")},
                         key=lambda w: (w[0] or "", w[1] or ""))
        info = p.get("info") or None
        if info:
            info, n = _PHONE.subn("（電話略）", info)
            info, k = _NAME.subn("（姓名略）", info)  # 承辦／聯絡人姓名不公開
            masked += n > 0
            name_masked += k > 0
        src_shape = p.get("shape") or geom["type"]
        radius, unit = _num(p.get("radius")), (p.get("unit") or "").strip() or None
        derived = False
        center = geom["coordinates"] if geom["type"] == "Point" else None
        if src_shape in ("Circle", "Sector") and center and radius:
            r_m = radius * _NM if unit == "nm" else radius if unit == "m" else None
            if r_m:
                b = (_num(p.get("start")), _num(p.get("end"))) if src_shape == "Sector" else (None, None)
                geom, derived = _derive_polygon(center, r_m, *b), True
        start, end = _ts(p.get("startdate")), _ts(p.get("enddate"))
        state = "not_started" if start and start > nowi else "expired" if end and end < nowi else "active"
        feats.append({"type": "Feature", "geometry": geom, "properties": {
            "feature_id": f"{code}:{shape_name}", "notice_code": code, "shape_name": shape_name, "title": p.get("name"),
            "notice_kind": p.get("noticekind"), "dept": p.get("dept"), "lifetime": p.get("lifetime"),
            "start_at": start, "end_at": end, "state": state, "issued_at": _ts(p.get("ldate")),
            "source_shape": src_shape, "geometry_derived": derived,
            "center_lon": center[0] if derived else None, "center_lat": center[1] if derived else None,
            "radius": radius, "radius_unit": unit,
            "bearing_start": _num(p.get("start")) if src_shape == "Sector" else None,
            "bearing_end": _num(p.get("end")) if src_shape == "Sector" else None,
            "windows": json.dumps([{"from": a, "to": b} for a, b in windows], ensure_ascii=False) if windows else None,
            "window_count": len(windows), "gov_doc": p.get("gov_doc") or None, "info": info,
            "web_url": p.get("web_url") or None, "pdf_url": p.get("pdf_url") or None, "other_file": p.get("other_file") or None,
            "source_rows": len(fs), "snapshot_at": nowi}})
    feats.sort(key=lambda f: (f["properties"]["notice_code"], f["properties"]["shape_name"]))
    suspect = {f["properties"]["feature_id"] for f in feats
               if any(not (100 <= x <= 135 and 0 <= y <= 35) for x, y in _coords(f["geometry"]))}
    active = [f for f in feats if f["properties"]["state"] == "active" and f["properties"]["feature_id"] not in suspect]
    status = {"snapshot_at": nowi, "source_rows": len(rows), "merged_features": len(feats), "active_features": len(active),
              "active_notices": len({f["properties"]["notice_code"] for f in active}),
              "state": dict(Counter(f["properties"]["state"] for f in feats)),
              "active_by_kind": dict(Counter(f["properties"]["notice_kind"] for f in active)),
              "unlocated_notices": len(unlocated), "suspect_coords_excluded": sorted(suspect),
              "info_phone_masked_features": masked, "info_name_masked_features": name_masked,
              "max_issued_at": max((f["properties"]["issued_at"] or "") for f in feats) or None}
    return ({"type": "FeatureCollection", "name": "motcmpb_nav_notices_latest", "features": active},
            {"type": "FeatureCollection", "name": "motcmpb_nav_notices_all", "features": feats}, status)


# ─── marine alerts ───────────────────────────────────────────────────────────
CWA_S3 = "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Observation/"
MARINE_IDS = {"spring": "O-B0069-001", "swell": "O-B0070-001", "stations": "O-B0076-001"}
# 警戒字串：官方定義取自 O-B0069／O-B0070 檔內 Metadata 的 warningStatus 說明（2026-11-22 讀取）
#   大潮：潮位注意（潮高 ≥ springCriteria 潮位注意值）、潮位警戒（≥ surgeCriteria 潮位警戒值）、低於警戒
#   長浪：長浪警戒（週期 ≥ 8 秒且浪高 ≥ 1.5 m）、巨浪警戒（浪高 > 6 m）、低於警戒
# 實際觀察到、但說明沒列的字串：「暴潮警戒」（2026-11-22 雲林，最大潮高 2.77 m ≥ 潮位警戒值 2.35 m，
#   即說明裡的「潮位警戒」條件）→ 同等級 2。其他沒看過的字串一律記 unknown_status、等級留空，不猜。
LEVELS = {"spring": {"低於警戒": 0, "潮位注意": 1, "潮位警戒": 2, "暴潮警戒": 2},
          "swell": {"低於警戒": 0, "長浪警戒": 1, "巨浪警戒": 2}}
KIND_ZH = {"spring": "大潮監測", "swell": "長浪監測"}
# 大潮監測點名是地區名、不是站名。下列對照以「該點最大潮高＝某潮位站同時段 O-B0075 逐時潮高最大值」比對，
# 兩個獨立 8 小時視窗（2026-11-21 09–17 時、2026-11-22 04–12 時）在 85 站中都只對到同一站才採用：
#   雲林→萡子寮（麥寮 1.26／—，不符）、嘉義→東石（塭港 0.84，不符）、臺東→富岡（綠島 0.55、小港 0.79，不符）、
#   宜蘭→烏石、恆春→後壁湖、苗栗→外埔、桃園→竹圍。官方沒有公開「監測點↔潮位站」清單。
SPRING_SITE_STATION = {"雲林": "C4J01", "嘉義": "C4L02", "臺東": "1586", "宜蘭": "C4U02", "恆春": "C4Q03",
                       "苗栗": "C4E01", "桃園": "C4C01"}
# 不在 O-B0076 的長浪浮標：座標取自 O-B0079-001 站表（2024-03 最後版本；該資料集已停更）
LEGACY_COORDS = {"臺北港浮標": (121.35, 25.1658, "TPBU01")}


def _hnum(v):
    return None if v in (None, "", "None", "-99", "-999") else _num(v)


def _stations(meta: dict) -> list[dict]:
    out = []
    for loc in meta["Resources"]["Resource"]["Data"]["SeaSurfaceObs"]["Location"]:
        s = loc["Station"]
        out.append({"id": s["StationID"], "name": s["StationName"], "attr": s.get("StationAttribute"),
                    "county": (s.get("County") or {}).get("CountyName"), "town": (s.get("Town") or {}).get("TownName"),
                    "lon": _num(s.get("StationLongitude")), "lat": _num(s.get("StationLatitude")),
                    "status": (loc.get("StationObsStatus") or {}).get("StationStatus")})
    return out


def _match_spring(name, st):
    tide = [s for s in st if s["attr"] == "潮位站"]
    exact = [s for s in tide if s["name"] == f"{name}潮位站"]
    if len(exact) == 1:
        return exact[0], "exact_name"
    sub = [s for s in tide if s["name"].removesuffix("潮位站") and s["name"].removesuffix("潮位站") in name]
    if len(sub) == 1:
        return sub[0], "name_substring"
    hit = [s for s in tide if s["id"] == SPRING_SITE_STATION.get(name)]
    if len(hit) == 1:
        return hit[0], "max_tide_crosscheck"
    return None, "no_candidate"


def _match_swell(name, st):
    variants = {name.replace("浮標", "資料浮標"), f"{name}式波浪站", name}
    hit = [s for s in st if s["name"] in variants and s["attr"] in ("浮標站", "浮球站")]
    if len(hit) == 1:
        return hit[0], "exact_name_variant"
    if name in LEGACY_COORDS:
        lon, lat, sid = LEGACY_COORDS[name]
        return {"id": sid, "name": name, "lon": lon, "lat": lat, "status": None}, "O-B0079_2024_station_coords"
    return None, "no_candidate"


def marine_alerts_build(spring: dict, swell: dict, stations: dict, retrieved_at: str) -> tuple[dict, dict]:
    """輸入為三份 cwaopendata JSON 的 'cwaopendata' 物件。"""
    st = _stations(stations)
    docs = {"spring": spring, "swell": swell}
    sites = {k: d["Resources"]["Resource"]["Data"]["SeaSurfaceObs"] for k, d in docs.items()}
    feats, unlocated, unknown = [], [], []
    for kind in ("spring", "swell"):
        for x in sites[kind]:
            loc = x["Location"]
            name = loc["StationName"]
            t = loc["StationObsTimes"]["StationObsTime"]
            we = t.get("WeatherElements") or {}
            status = we.get("WarningStatus")
            level = LEVELS[kind].get(status)
            if level is None:
                unknown.append(f"{kind}:{name}:{status}")
            if kind == "spring":
                s, method = _match_spring(name, st)
                crit = loc.get("StationWarningCriteria") or {}
                extra = {"max_height_m": _hnum(we.get("MaximumTideHeight")),
                         "spring_criteria_m": _hnum((crit.get("SpringCriteria") or {}).get("TideHeight")),
                         "surge_criteria_m": _hnum((crit.get("SurgeCriteria") or {}).get("TideHeight"))}
            else:
                s, method = _match_swell(name, st)
                extra = {"max_height_m": _hnum(we.get("MaximumWaveHeight")), "spring_criteria_m": None, "surge_criteria_m": None}
            props = {"site_id": f"{kind}:{name}", "kind": kind, "kind_zh": KIND_ZH[kind], "site_name": name,
                     "warning_status": status, "alert_level": level, **extra,
                     "obs_start": t.get("StartTime"), "obs_end": t.get("EndTime"),
                     "station_id": s["id"] if s else None,
                     "station_uid": f'cwa:{s["id"]}' if s and method != "O-B0079_2024_station_coords" else None,
                     "station_name": s["name"] if s else None, "station_status": s.get("status") if s else None,
                     "match_method": method, "data_sent": docs[kind].get("Sent"), "source_dataset": MARINE_IDS[kind],
                     "retrieved_at": retrieved_at}
            if s and s.get("lon") is not None and s.get("lat") is not None:
                feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
                              "properties": props})
            else:
                unlocated.append(props["site_id"])
    status = {"feature_count": len(feats), "unlocated": unlocated, "unknown_status": unknown,
              "alert_sites": [f["properties"]["site_id"] for f in feats if (f["properties"]["alert_level"] or 0) > 0],
              "data_sent": {k: d.get("Sent") for k, d in docs.items()}}
    return {"type": "FeatureCollection", "name": "cwa_tide_swell_alerts_latest", "features": feats}, status


def content_hash(fc: dict, volatile=("retrieved_at", "snapshot_at")) -> str:
    """去掉每輪都會變的欄位後的內容雜湊，用來判斷「沒變就不重寫 R2」。"""
    import hashlib
    feats = [{"g": f["geometry"], "p": {k: v for k, v in f["properties"].items() if k not in volatile}} for f in fc["features"]]
    return hashlib.sha256(json.dumps(feats, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
