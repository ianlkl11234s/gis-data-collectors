"""日本氣象廳 季節觀測收集器（生物季節／山頂初冠雪／梅雨速報），每日數次、只記變動。

資料來源（免認證 HTML／XML／CSV；授權 PDL1.0，前端需標「出典：気象庁」並註明加工）：
  生物季節 當年觀測表（每日 08:35 更新；HTML 無時間戳 → 用 HTTP Last-Modified 當 source_time）
    https://www.data.jma.go.jp/sakura/data/phn_012.html  いちょう黄葉 → 現象碼 013
    https://www.data.jma.go.jp/sakura/data/phn_013.html  いちょう落葉 → 014
    https://www.data.jma.go.jp/sakura/data/phn_014.html  かえで紅葉   → 015
    https://www.data.jma.go.jp/sakura/data/phn_015.html  かえで落葉   → 016
      ⚠ 頁面檔名編號 ≠ 累年 CSV／XML 現象碼（差 1），以 PHN_PAGES 對照為準
      兩年並列（例 2025、2026 欄）：每欄各成一列；「-」＝未觀測（不寫列）、「///」＝無差值（NULL）
    https://www.data.jma.go.jp/sakura/data/sakura_kaika.html   さくら開花 → 004（12–6 月 1 日 3 次更新）
    https://www.data.jma.go.jp/sakura/data/sakura_mankai.html  さくら満開 → 005
      有平年日欄；「観測しているさくらの種類」空白＝そめいよしの（alt_species NULL）
  生物季節 XML 電文 VGSK55（防災情報 XML other_l.xml 長期 feed，約保留 7 天）
    每則一站一現象；Kind/Code 與累年 CSV 現象碼相同（11＝すすき 011）。全現象都收（不只上面 6 種）。
  山頂初冠雪（44 山）https://www.data.jma.go.jp/stats/data/mdrr/fsn_cap/index.php（每時 25 分更新）
    2027 寒候年（2026-08～2027-07）起改用解析積雪深判定 → judgement_method='analysis'；
    2026 寒候年以前為目視 → 'visual'（records/*.html，由 scripts/backfill_jma_phenology.py 回填）。
  梅雨速報 https://www.data.jma.go.jp/cpd/baiu/sokuhou_baiu.html（季節外仍是最近一季，照實存並標年份）

寫入（gis-platform migration 439，全部 ON CONFLICT DO NOTHING、永久保留）：
  live.jma_phenology_observations  UNIQUE NULLS NOT DISTINCT（source, 站, 現象, 季年, 觀測日, 兩個差值, 代替種, info_type）
  live.jma_snowcap                 UNIQUE (mountain_code, season_year, judgement_method, first_snowcap_date)
  live.jma_snowcap_status          PK (mountain_code, source_time)；collector 只在狀態改變時送
  live.jma_rainy_season            UNIQUE NULLS NOT DISTINCT (region, year, event, period_text, class_text)

只送新項目：record_key 與 DB 唯一鍵一致，seen 狀態只在 DB 寫入成功後前進（require_db_write，照 jma_quake）。
原始頁面：只有產出新列的來源才把原文放進 raw_payload（本地 JSON → ArchiveTask 每日歸檔 S3）。
缺值一律 None，不補 0。
"""

from __future__ import annotations

import csv
import io
import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Optional
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import JmaFetchError, new_jma_session

JST = timezone(timedelta(hours=9))

DATA_BASE = "https://www.data.jma.go.jp"
SAKURA_BASE = f"{DATA_BASE}/sakura/data"
URL_SNOWCAP = f"{DATA_BASE}/stats/data/mdrr/fsn_cap/index.php"
URL_SNOWCAP_RECORD = f"{DATA_BASE}/stats/data/mdrr/fsn_cap/records/{{code}}.html"
URL_BAIU = f"{DATA_BASE}/cpd/baiu/sokuhou_baiu.html"
URL_XML_FEED = f"{DATA_BASE}/developer/xml/feed/other_l.xml"
URL_RUINENCHI = f"{SAKURA_BASE}/ruinenchi/{{code}}.csv"
URL_AMEDAS_TABLE = "https://www.jma.go.jp/bosai/amedas/const/amedastable.json"

# 當年觀測表頁面 → 現象碼（累年 CSV／XML Kind/Code 體系）
PHN_PAGES = {
    "phn_012": "013",
    "phn_013": "014",
    "phn_014": "015",
    "phn_015": "016",
}
SAKURA_PAGES = {
    "sakura_kaika": "004",
    "sakura_mankai": "005",
}

# 現象碼 → (名稱, 季節型)。spring：12 月前觀測記在隔年欄；autumn：1 月後觀測記在前一年欄
PHENOMENA = {
    "001": ("うめの開花", "spring"),
    "004": ("さくらの開花", "spring"),
    "005": ("さくらの満開", "spring"),
    "009": ("あじさいの開花", "calendar"),
    "011": ("すすきの開花", "calendar"),
    "013": ("いちょうの黄葉", "autumn"),
    "014": ("いちょうの落葉", "autumn"),
    "015": ("かえでの紅葉", "autumn"),
    "016": ("かえでの落葉", "autumn"),
}
RUINENCHI_CODES = ("004", "005", "013", "014", "015", "016")
ALT_RM_CODES = {6, 7}  # 6 代替種目・構内、7 代替種目・付近（8／9 為正規種目）

# 站名與 amedastable kjName 不同者
STATION_ALIASES = {"南大東島": "南大東"}

CONST_REFRESH = timedelta(hours=24)
XML_SEEN_KEEP = 2000

SOURCES = ("phenology_html", "phenology_xml", "snowcap", "rainy")


# ───────────────────────── 共用小工具 ─────────────────────────

def _clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(value)).strip()


def _none_if_blank(value: Any) -> Optional[str]:
    s = _clean(value)
    return s or None


def season_calendar_year(season_year: int, month: int, kind: str) -> int:
    """季年欄 + 月 → 實際日曆年。累年表規則：冬春種目在 1/1 前觀測記在隔年欄；秋種目在 12/31 後記在前一年欄。"""
    if kind == "spring" and month >= 10:
        return season_year - 1
    if kind == "autumn" and month <= 6:
        return season_year + 1
    return season_year


def season_year_of(obs: date, kind: str) -> int:
    """實際觀測日 → 所屬季年欄（season_calendar_year 的反函數）。"""
    if kind == "spring" and obs.month >= 10:
        return obs.year + 1
    if kind == "autumn" and obs.month <= 6:
        return obs.year - 1
    return obs.year


def phenomenon_kind(code: str) -> str:
    return PHENOMENA.get(code, ("", "calendar"))[1]


def decode_mmdd(value: Any, season_year: int, kind: str) -> Optional[date]:
    """累年 CSV 的「月×100＋日」→ date；0／空／非法 → None（缺測，不當 0）。"""
    try:
        v = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    month, day = divmod(v, 100)
    try:
        return date(season_calendar_year(season_year, month, kind), month, day)
    except ValueError:
        return None


_MD_RE = re.compile(r"(\d{1,2})\s*月\s*(\d{1,2})\s*日")


def parse_month_day(text: Any) -> Optional[tuple[int, int]]:
    m = _MD_RE.search(_clean(text))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def resolve_md(text: Any, season_year: int, kind: str) -> Optional[date]:
    md = parse_month_day(text)
    if not md:
        return None
    try:
        return date(season_calendar_year(season_year, md[0], kind), md[0], md[1])
    except ValueError:
        return None


def parse_signed_int(text: Any) -> Optional[int]:
    """'+9' / '-1' / '0' → int；'///'、'-'、空白 → None。"""
    s = _clean(text).replace("＋", "+").replace("－", "-").replace("−", "-")
    if not re.fullmatch(r"[+-]?\d+", s):
        return None
    return int(s)


def http_time(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return None


# ───────────────────────── 站點索引 ─────────────────────────

def build_station_index(ruinenchi_csv_text: str, amedastable: Optional[dict]) -> dict[str, dict]:
    """站名 → {station_no(WMO 5 碼), lat, lon}。站號取累年 CSV「番号」（國際地點番號下三碼），座標取 amedastable。

    amedastable 同名多站時取 type 'A'（気象官署）；南大東島在站表叫「南大東」。
    """
    coords: dict[str, tuple[Optional[float], Optional[float]]] = {}
    if isinstance(amedastable, dict):
        best: dict[str, tuple[int, dict]] = {}
        for v in amedastable.values():
            if not isinstance(v, dict) or not v.get("kjName"):
                continue
            rank = 0 if v.get("type") == "A" else 1
            name = v["kjName"]
            if name not in best or rank < best[name][0]:
                best[name] = (rank, v)
        for name, (_, v) in best.items():
            coords[name] = (_deg(v.get("lat")), _deg(v.get("lon")))

    out: dict[str, dict] = {}
    for row in csv.reader(io.StringIO(ruinenchi_csv_text or "")):
        if len(row) < 2 or not row[0].strip().isdigit():
            continue
        name = _clean(row[1]).replace(" ", "")
        if not name:
            continue
        lat, lon = coords.get(STATION_ALIASES.get(name, name), (None, None))
        out[name] = {"station_no": f"47{int(row[0]):03d}", "lat": lat, "lon": lon}
    return out


def _deg(value: Any) -> Optional[float]:
    if isinstance(value, (list, tuple)) and len(value) == 2:
        try:
            return round(float(value[0]) + float(value[1]) / 60.0, 5)
        except (TypeError, ValueError):
            return None
    return None


def _station_fields(stations: dict[str, dict], name: str) -> dict:
    st = stations.get(name.replace(" ", "")) or {}
    return {"station_no": st.get("station_no"), "lat": st.get("lat"), "lon": st.get("lon")}


def station_name_by_no(stations: dict[str, dict]) -> dict[str, str]:
    return {v["station_no"]: k for k, v in stations.items() if v.get("station_no")}


# ───────────────────────── 生物季節：當年 HTML ─────────────────────────

def _phen_row(*, stations, name, code, season_year, obs_date, diff_normal, diff_last, normal_date,
              normal_mmdd=None, alt_species=None, rm_code=None, site_status=None, remark=None,
              info_type=None, source, source_ref, source_time, collected_at) -> dict:
    return {
        "_type": "phenology",
        **_station_fields(stations, name),
        "station_name": name,
        "phenomenon_code": code,
        "phenomenon_name": PHENOMENA.get(code, (None,))[0],
        "season_year": season_year,
        "obs_date": obs_date.isoformat() if obs_date else None,
        "diff_normal_days": diff_normal,
        "diff_last_year_days": diff_last,
        "normal_date": normal_date.isoformat() if normal_date else None,
        "normal_mmdd": normal_mmdd,
        "alt_species": alt_species,
        "rm_code": rm_code,
        "site_status": site_status,
        "remark": remark,
        "info_type": info_type,
        "source": source,
        "source_ref": source_ref,
        "source_time": source_time,
        "collected_at": collected_at,
    }


def parse_phn_page(html: str, code: str, stations: dict[str, dict], *, source_ref: str,
                   source_time: Optional[str], collected_at: str) -> list[dict]:
    """いちょう／かえで頁：地點名 | (年A 観測日 平年差 昨年差) | (年B …) | 代替種目。

    平年日欄不存在 → normal_date 由 obs_date − 平年差推得（定義上精確）；平年差缺則 NULL。
    """
    kind = phenomenon_kind(code)
    soup = BeautifulSoup(html, "html.parser")
    years: list[int] = []
    for th in soup.find_all("th"):
        m = re.fullmatch(r"(\d{4})年", _clean(th.get_text()))
        if m:
            years.append(int(m.group(1)))
    if len(years) < 1:
        raise ValueError(f"{source_ref}: 找不到年份表頭")
    rows: list[dict] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 1 + 3 * len(years):
            continue
        cells = [_clean(td.get_text()) for td in tds]
        name = cells[0].replace(" ", "")
        if not name:
            continue
        alt = _none_if_blank(cells[1 + 3 * len(years)]) if len(cells) > 1 + 3 * len(years) else None
        for i, year in enumerate(years):
            obs = resolve_md(cells[1 + 3 * i], year, kind)
            if obs is None:
                continue  # 「-」＝尚未觀測或欠測，不寫列
            diff_n = parse_signed_int(cells[2 + 3 * i])
            diff_l = parse_signed_int(cells[3 + 3 * i])
            normal = obs - timedelta(days=diff_n) if diff_n is not None else None
            rows.append(_phen_row(
                stations=stations, name=name, code=code, season_year=year, obs_date=obs,
                diff_normal=diff_n, diff_last=diff_l, normal_date=normal, alt_species=alt,
                source="current_html", source_ref=source_ref, source_time=source_time,
                collected_at=collected_at))
    return rows


def parse_sakura_page(html: str, code: str, stations: dict[str, dict], *, source_ref: str,
                      source_time: Optional[str], collected_at: str) -> list[dict]:
    """さくら頁：地點名(th) | 観測日 | 平年差 | 平年日 | 昨年差 | 昨年日 | 種類（空白＝そめいよしの）。"""
    kind = phenomenon_kind(code)
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    m = re.search(r"(\d{4})年", _clean(h1.get_text()) if h1 else "")
    if not m:
        raise ValueError(f"{source_ref}: h1 找不到年份")
    year = int(m.group(1))
    rows: list[dict] = []
    for tr in soup.find_all("tr"):
        th = tr.find("th", attrs={"scope": "row"})
        tds = tr.find_all("td")
        if th is None or len(tds) < 6:
            continue
        name = _clean(th.get_text()).replace(" ", "")
        cells = [_clean(td.get_text()) for td in tds]
        obs = resolve_md(cells[0], year, kind)
        if obs is None:
            continue
        rows.append(_phen_row(
            stations=stations, name=name, code=code, season_year=year, obs_date=obs,
            diff_normal=parse_signed_int(cells[1]), diff_last=parse_signed_int(cells[3]),
            normal_date=resolve_md(cells[2], year, kind),
            alt_species=_none_if_blank(cells[5]) if len(cells) > 5 else None,
            source="current_html", source_ref=source_ref, source_time=source_time,
            collected_at=collected_at))
    return rows


# ───────────────────────── 生物季節：XML VGSK55 ─────────────────────────

ATOM = "{http://www.w3.org/2005/Atom}"


def parse_feed_vgsk55(feed_xml: str) -> list[str]:
    """Atom feed → VGSK55 電文 URL（舊→新）。"""
    root = ET.fromstring(feed_xml.encode("utf-8") if isinstance(feed_xml, str) else feed_xml)
    urls = []
    for entry in root.findall(f"{ATOM}entry"):
        link = entry.find(f"{ATOM}link")
        href = link.get("href") if link is not None else None
        if href and "_VGSK55_" in href:
            urls.append(href)
    return sorted(set(urls))


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_all(node, name: str):
    return [e for e in node.iter() if _local(e.tag) == name]


def _find_text(node, name: str) -> Optional[str]:
    for e in node.iter():
        if _local(e.tag) == name:
            return _none_if_blank(e.text)
    return None


def parse_vgsk55(xml_text: str, stations: dict[str, dict], *, source_ref: str,
                 collected_at: str) -> list[dict]:
    root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)
    head = next((e for e in root if _local(e.tag) == "Head"), root)
    body = next((e for e in root if _local(e.tag) == "Body"), root)
    report_time = _find_text(head, "ReportDateTime")
    info_type = _find_text(head, "InfoType")
    dev_normal = parse_signed_int(_find_text(body, "DeviationFromNormal"))
    dev_last = parse_signed_int(_find_text(body, "DeviationFromLastYear"))
    by_no = station_name_by_no(stations)
    rows: list[dict] = []
    for info in _find_all(body, "MeteorologicalInfo"):
        dt_text = _find_text(info, "DateTime")
        try:
            obs = datetime.fromisoformat(dt_text).date() if dt_text else None
        except ValueError:
            obs = None
        if obs is None:
            continue
        for item in _find_all(info, "Item"):
            kind_node = next((e for e in item if _local(e.tag) == "Kind"), None)
            st_node = next((e for e in item if _local(e.tag) == "Station"), None)
            if kind_node is None or st_node is None:
                continue
            raw_code = _find_text(kind_node, "Code")
            if not raw_code or not raw_code.isdigit():
                continue
            code = f"{int(raw_code):03d}"
            st_no = _find_text(st_node, "Code")
            st_xml_name = _find_text(st_node, "Name")
            name = by_no.get(st_no or "") or st_xml_name or st_no
            if not name:
                continue
            condition = _find_text(kind_node, "Condition")
            class_name = _find_text(kind_node, "ClassName")
            alt = class_name if condition and "代替" in condition else None
            row = _phen_row(
                stations=stations, name=name, code=code,
                season_year=season_year_of(obs, phenomenon_kind(code)), obs_date=obs,
                diff_normal=dev_normal, diff_last=dev_last,
                normal_date=obs - timedelta(days=dev_normal) if dev_normal is not None else None,
                alt_species=alt, site_status=_find_text(st_node, "Status"),
                remark="; ".join(x for x in (
                    f"Condition={condition}" if condition else None,
                    f"ClassName={class_name}" if class_name else None,
                    f"Station={st_xml_name}" if st_xml_name else None,
                    f"ReportDateTime={report_time}" if report_time else None,
                ) if x) or None,
                info_type=info_type, source="xml", source_ref=source_ref,
                source_time=report_time, collected_at=collected_at)
            if row["phenomenon_name"] is None:
                row["phenomenon_name"] = _find_text(kind_node, "Name")
            if row["station_no"] is None and st_no:
                row["station_no"] = st_no
            rows.append(row)
    return rows


# ───────────────────────── 生物季節：累年 CSV（回填用）─────────────────────────

def parse_ruinenchi_csv(text: str, code: str, stations: dict[str, dict], *, years: Iterable[int],
                        source_ref: str, source_time: Optional[str], collected_at: str) -> list[dict]:
    """累年 CSV（Shift_JIS 已解碼）→ 指定年份的觀測列；同列帶平年値（normal_mmdd／normal_date）。

    值 0 或 rm 0＝缺測 → 不寫列；rm 6/7 代替種目、8/9 正規種目（7/9 為「付近」觀測）。
    """
    kind = phenomenon_kind(code)
    rows_in = list(csv.reader(io.StringIO(text)))
    header = next((r for r in rows_in if r and r[0].strip() == "番号"), None)
    if header is None:
        raise ValueError(f"{source_ref}: 找不到表頭")
    col = {h.strip(): i for i, h in enumerate(header) if h.strip() not in ("rm", "")}
    normal_idx = col.get("平年値")
    out: list[dict] = []
    for r in rows_in:
        if not r or not r[0].strip().isdigit():
            continue
        name = _clean(r[1]).replace(" ", "")
        normal_mmdd = None
        if normal_idx is not None and normal_idx < len(r):
            try:
                normal_mmdd = int(r[normal_idx]) or None
            except ValueError:
                normal_mmdd = None
        for year in years:
            i = col.get(str(year))
            if i is None or i + 1 >= len(r):
                continue
            obs = decode_mmdd(r[i], year, kind)
            try:
                rm = int(r[i + 1])
            except ValueError:
                rm = None
            if obs is None or rm == 0:
                continue
            normal = decode_mmdd(normal_mmdd, year, kind) if normal_mmdd else None
            out.append(_phen_row(
                stations=stations, name=name, code=code, season_year=year, obs_date=obs,
                diff_normal=None, diff_last=None, normal_date=normal, normal_mmdd=normal_mmdd,
                rm_code=rm, alt_species=("代替種目" if rm in ALT_RM_CODES else None),
                site_status={6: "構内", 7: "付近", 8: "構内", 9: "付近"}.get(rm),
                source="ruinenchi_csv", source_ref=source_ref, source_time=source_time,
                collected_at=collected_at))
    return out


# ───────────────────────── 山頂初冠雪 ─────────────────────────

def _mountain_key(text: str) -> str:
    """'旭岳（アサヒダケ）' / '旭岳(アサヒダケ)' → '旭岳'。"""
    return re.split(r"[（(]", _clean(text), maxsplit=1)[0].strip()


def judgement_method(season_year: int) -> str:
    return "analysis" if season_year >= 2027 else "visual"


def parse_snowcap_page(html: str, *, collected_at: str) -> tuple[list[dict], list[dict], dict]:
    """→ (first_rows, status_rows, mountains)。mountains: 山名 → {code, observatory, kana}（過去紀錄表）。"""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ")
    m = re.search(r"(\d{4})寒候年：", text)
    if not m:
        raise ValueError("fsn_cap: 找不到本季寒候年")
    season_year = int(m.group(1))
    t = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日\s*(\d{1,2})時\s*時点", text)
    source_time = (datetime(int(t.group(1)), int(t.group(2)), int(t.group(3)), int(t.group(4)),
                            tzinfo=JST).isoformat() if t else None)

    mountains: dict[str, dict] = {}
    for a in soup.find_all("a", href=re.compile(r"records/(\d+)\.html")):
        code = re.search(r"records/(\d+)\.html", a["href"]).group(1)
        tr = a.find_parent("tr")
        tds = tr.find_all("td") if tr else []
        obs = _clean(tds[1].get_text()) if len(tds) > 1 else None
        kana = re.search(r"[（(](.*)[)）]\s*$", _clean(a.get_text()))
        mountains[_mountain_key(a.get_text())] = {
            "code": code, "observatory": obs or None, "kana": kana.group(1) if kana else None,
        }

    first_rows: list[dict] = []
    status_rows: list[dict] = []
    h2 = soup.find(id="latest_snw")
    table = h2.find_next("table") if h2 else None
    for tr in (table.find_all("tr") if table else []):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        full = _clean(tds[0].get_text())
        key = _mountain_key(full)
        mt = mountains.get(key) or {}
        code = mt.get("code") or key  # 對不到代碼時以山名當鍵（不臆造代碼）
        base = {"mountain_code": code, "mountain_name": key, "observatory": mt.get("observatory"),
                "season_year": season_year, "collected_at": collected_at}
        dm = re.search(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", _clean(tds[1].get_text()))
        if dm:
            first_rows.append({
                "_type": "snowcap", **base,
                "first_snowcap_date": date(int(dm.group(1)), int(dm.group(2)), int(dm.group(3))).isoformat(),
                "judgement_method": judgement_method(season_year),
                "source": "current_html", "source_time": source_time,
            })
        cls = tds[2].get("class")
        status_rows.append({
            "_type": "snow_status", **base,
            "source_time": source_time,
            "status_text": _none_if_blank(tds[2].get_text()),
            "status_class": " ".join(cls) if isinstance(cls, list) else _none_if_blank(cls),
            "judgement_method": judgement_method(season_year),
        })
    return first_rows, status_rows, mountains


def parse_snowcap_record(html: str, *, mountain_code: str, mountain_name: str,
                         observatory: Optional[str], years: Iterable[int], collected_at: str) -> list[dict]:
    """records/{code}.html（目視紀錄，2026 寒候年止）→ 指定寒候年的初冠雪列。"""
    want = set(years)
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tr in soup.find_all("tr"):
        tds = [_clean(td.get_text()) for td in tr.find_all("td")]
        if len(tds) < 2 or not tds[0].isdigit() or int(tds[0]) not in want:
            continue
        dm = re.fullmatch(r"(\d{4})/(\d{1,2})/(\d{1,2})", tds[1])
        if not dm:
            continue  # 「--」等＝無紀錄
        out.append({
            "_type": "snowcap", "mountain_code": mountain_code, "mountain_name": mountain_name,
            "observatory": observatory, "season_year": int(tds[0]),
            "first_snowcap_date": date(int(dm.group(1)), int(dm.group(2)), int(dm.group(3))).isoformat(),
            "judgement_method": "visual", "source": "records_html", "source_time": None,
            "collected_at": collected_at,
        })
    return out


# ───────────────────────── 梅雨速報 ─────────────────────────

_REIWA_RE = re.compile(r"令和\s*(\d+|元)\s*年")


def reiwa_to_year(text: str) -> Optional[int]:
    m = _REIWA_RE.search(text or "")
    if not m:
        return None
    n = 1 if m.group(1) == "元" else int(m.group(1))
    return 2018 + n


def parse_diff_text(text: Any) -> Optional[int]:
    """'6日早い' → -6；'2日遅い' → +2；'同じ' → 0；其他（'－' 等）→ None。"""
    s = _clean(text)
    if s == "同じ":
        return 0
    m = re.fullmatch(r"(\d+)日(早い|遅い)", s)
    if not m:
        return None
    n = int(m.group(1))
    return -n if m.group(2) == "早い" else n


def parse_baiu_page(html: str, *, collected_at: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    year = reiwa_to_year(_clean(h1.get_text()) if h1 else "")
    if year is None:
        raise ValueError("baiu: h1 找不到令和年")
    upd = soup.find(class_="update")
    upd_m = re.search(r"令和\s*(\d+|元)\s*年\s*(\d{1,2})月\s*(\d{1,2})日", _clean(upd.get_text()) if upd else "")
    page_updated = None
    if upd_m:
        y = 2018 + (1 if upd_m.group(1) == "元" else int(upd_m.group(1)))
        page_updated = date(y, int(upd_m.group(2)), int(upd_m.group(3))).isoformat()
    rows: list[dict] = []
    for table, event in (("onset", "onset"), ("over", "end")):
        tbl = soup.find("table", class_=table)
        if tbl is None:
            continue
        tbody = tbl.find("tbody") or tbl
        for tr in tbody.find_all("tr"):
            th = tr.find("th")
            tds = [_clean(td.get_text()) for td in tr.find_all("td")]
            if th is None or len(tds) < 7:
                continue
            period, cls, dn, dl, normal, last_period, last_cls = tds[:7]
            md = parse_month_day(period)
            nmd = parse_month_day(normal)
            rows.append({
                "_type": "rainy",
                "region": _clean(th.get_text()),
                "year": year,
                "event": event,
                "period_text": period or None,
                "period_date": date(year, *md).isoformat() if md else None,
                "class_text": cls or None,
                "diff_normal_text": dn or None,
                "diff_normal_days": parse_diff_text(dn),
                "diff_last_year_text": dl or None,
                "diff_last_year_days": parse_diff_text(dl),
                "normal_text": normal or None,
                "normal_date": date(year, *nmd).isoformat() if nmd else None,
                "last_year_period_text": last_period or None,
                "last_year_class_text": last_cls or None,
                "status": "速報",
                "page_updated": page_updated,
                "source_ref": URL_BAIU,
                "collected_at": collected_at,
            })
    return rows


# ───────────────────────── 唯一鍵（與 migration 439 一致）─────────────────────────

def record_key(r: dict) -> str:
    t = r.get("_type")
    if t == "phenology":
        parts = ("source", "station_name", "phenomenon_code", "season_year", "obs_date",
                 "diff_normal_days", "diff_last_year_days", "alt_species", "info_type")
    elif t == "snowcap":
        parts = ("mountain_code", "season_year", "judgement_method", "first_snowcap_date")
    elif t == "snow_status":
        parts = ("mountain_code", "source_time")
    elif t == "rainy":
        parts = ("region", "year", "event", "period_text", "class_text")
    else:
        parts = ()
    return f"{t}|" + "|".join("" if r.get(p) is None else str(r.get(p)) for p in parts)


def source_of(r: dict) -> str:
    t = r.get("_type")
    if t == "phenology":
        return "phenology_xml" if r.get("source") == "xml" else "phenology_html"
    if t in ("snowcap", "snow_status"):
        return "snowcap"
    return "rainy"


# ───────────────────────── Collector ─────────────────────────

class JmaSeasonalCollector(BaseCollector):
    """生物季節（HTML＋XML VGSK55）／初冠雪／梅雨速報；只送新項目，全部 DO NOTHING。"""

    name = "jma_seasonal"
    interval_minutes = config.JAPAN_JMA_SEASONAL_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-seasonal")
        self._stations: dict[str, dict] = {}
        self._stations_loaded_at: Optional[datetime] = None
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "jma_seasonal_state.json"
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None

    def require_db_write(self) -> bool:
        return True  # state 只在 DB 寫入成功後前進

    # ---- state ----
    def _load_state(self) -> dict:
        if self._state is None:
            try:
                raw = json.loads(self.state_path.read_text())
            except (OSError, ValueError):
                raw = {}
            self._state = {
                "seen": set(raw.get("seen", [])),
                "xml_seen": list(raw.get("xml_seen", [])),
                "snow_status": dict(raw.get("snow_status", {})),
            }
        return self._state

    def _save_state(self, state: dict) -> None:
        self._state = state
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({
                "seen": sorted(state["seen"]),
                "xml_seen": state["xml_seen"][-XML_SEEN_KEEP:],
                "snow_status": state["snow_status"],
            }, ensure_ascii=False))
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[{self.name}] ⚠ state 檔寫入失敗（僅影響重啟後重送一次，DB DO NOTHING 擋重複）: {e}")

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_state(self._pending_state)
        self._pending_state = None
        return stats

    # ---- fetch ----
    def _get(self, url: str, *, encoding: Optional[str] = None) -> tuple[str, Optional[str]]:
        """回傳 (text, Last-Modified ISO)。"""
        last: Exception | None = None
        for attempt in range(2):
            if attempt:
                time.sleep(2)  # 有限 backoff（只有第二次前）
            try:
                resp = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
                resp.raise_for_status()
                if encoding:
                    text = resp.content.decode(encoding)
                else:
                    resp.encoding = resp.encoding if resp.encoding and resp.encoding.lower() != "iso-8859-1" else "utf-8"
                    text = resp.text
                return text, http_time(resp.headers.get("Last-Modified"))
            except Exception as e:  # noqa: BLE001
                last = e
        raise JmaFetchError(f"{url}: {last}")

    def _station_index(self) -> dict[str, dict]:
        now = datetime.now(timezone.utc)
        if (not self._stations or self._stations_loaded_at is None
                or now - self._stations_loaded_at > CONST_REFRESH):
            csv_text, _ = self._get(URL_RUINENCHI.format(code="004"), encoding="shift_jis")
            try:
                table = self._session.get(URL_AMEDAS_TABLE, timeout=config.REQUEST_TIMEOUT).json()
            except Exception as e:  # noqa: BLE001
                print(f"[{self.name}] ⚠ amedastable 抓取失敗（座標本輪 NULL）: {e}")
                table = None
            self._stations = build_station_index(csv_text, table)
            self._stations_loaded_at = now
        return self._stations

    # ---- collect ----
    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        failures: dict[str, str] = {}
        records: list[dict] = []
        raw: dict[str, list] = {s: [] for s in SOURCES}
        source_times: dict[str, Optional[str]] = {}
        state = self._load_state()

        try:
            stations = self._station_index()
        except JmaFetchError as e:
            stations = self._stations or {}
            print(f"[{self.name}] ⚠ 站點索引抓取失敗（station_no／座標本輪 NULL）: {e}")

        # 1) 當年 HTML
        page_failures: dict[str, str] = {}
        for page, code in {**PHN_PAGES, **SAKURA_PAGES}.items():
            url = f"{SAKURA_BASE}/{page}.html"
            try:
                html, lm = self._get(url)
                parser = parse_sakura_page if page.startswith("sakura") else parse_phn_page
                rows = parser(html, code, stations, source_ref=url, source_time=lm,
                              collected_at=collected_at)
            except (JmaFetchError, ValueError) as e:
                page_failures[page] = str(e)  # 單頁失敗不中斷後面頁面
                continue
            source_times[page] = lm
            records.extend(rows)
            raw["phenology_html"].append({"url": url, "last_modified": lm, "rows": len(rows), "html": html})
        if page_failures:
            failures["phenology_html"] = str(page_failures)

        # 2) XML VGSK55（只抓沒看過的電文）
        new_xml_urls: list[str] = []
        try:
            feed, lm = self._get(URL_XML_FEED)
            source_times["xml_feed"] = lm
            known = set(state["xml_seen"])
            xml_failures: dict[str, str] = {}
            for url in parse_feed_vgsk55(feed):
                if url in known:
                    continue
                try:
                    xml_text, _ = self._get(url)
                    rows = parse_vgsk55(xml_text, stations, source_ref=url, collected_at=collected_at)
                except (JmaFetchError, ET.ParseError) as e:
                    xml_failures[url] = str(e)  # 失敗 URL 不進 xml_seen，下輪重試，不擋後續電文
                    continue
                records.extend(rows)
                raw["phenology_xml"].append({"url": url, "xml": xml_text})
                new_xml_urls.append(url)
            if xml_failures:
                failures["phenology_xml"] = str(xml_failures)
        except (JmaFetchError, ET.ParseError) as e:
            failures["phenology_xml"] = str(e)

        # 3) 初冠雪
        snow_status_next = dict(state["snow_status"])
        try:
            html, _ = self._get(URL_SNOWCAP)
            first_rows, status_rows, _m = parse_snowcap_page(html, collected_at=collected_at)
            records.extend(first_rows)
            source_times["snowcap"] = status_rows[0]["source_time"] if status_rows else None
            for r in status_rows:
                sig = f"{r['season_year']}|{r['status_text']}|{r['status_class']}"
                if state["snow_status"].get(r["mountain_code"]) == sig:
                    continue
                if not r.get("source_time"):
                    continue  # 無時間戳不送列，也不推進狀態，待下輪補
                records.append(r)
                snow_status_next[r["mountain_code"]] = sig
            raw["snowcap"].append({"url": URL_SNOWCAP, "html": html})
        except (JmaFetchError, ValueError) as e:
            failures["snowcap"] = str(e)

        # 4) 梅雨速報
        try:
            html, lm = self._get(URL_BAIU)
            rows = parse_baiu_page(html, collected_at=collected_at)
            for r in rows:
                r["source_time"] = lm
            records.extend(rows)
            source_times["rainy"] = rows[0]["page_updated"] if rows else lm
            raw["rainy"].append({"url": URL_BAIU, "last_modified": lm, "html": html})
        except (JmaFetchError, ValueError) as e:
            failures["rainy"] = str(e)

        if len(failures) == len(SOURCES):
            raise RuntimeError(f"jma_seasonal: 全部來源失敗 {failures}")

        # 只送新項目
        fresh = [r for r in records if record_key(r) not in state["seen"]]
        counts = {s: sum(1 for r in records if source_of(r) == s) for s in SOURCES}
        new_counts = {s: sum(1 for r in fresh if source_of(r) == s) for s in SOURCES}
        stale = self._stale_sources(source_times)

        next_state = {
            "seen": state["seen"] | {record_key(r) for r in records},
            "xml_seen": state["xml_seen"] + new_xml_urls,
            "snow_status": snow_status_next,
        }
        self._pending_state = next_state

        out: dict[str, Any] = {
            "counts": counts, "new_counts": new_counts, "source_times": source_times,
            "stale_sources": stale, "source_failures": failures, "collected_at": collected_at,
        }
        if failures:
            out["_collector_error"] = f"jma_seasonal: 部分來源失敗 {failures}"
        if not fresh:
            print(f"[{self.name}] no change（{counts}）")
            out["no_change"] = True
            return out
        # 原文只留有新列的來源（冷儲存：本地 JSON → ArchiveTask → S3）
        out["raw_payload"] = {s: raw[s] for s in SOURCES if new_counts.get(s)}
        out["data"] = fresh
        return out

    @staticmethod
    def _stale_sources(source_times: dict[str, Optional[str]], now: Optional[datetime] = None) -> dict:
        """用來源時間戳判斷新鮮度：初冠雪每時更新（>6h 視為過期）；生物季節 HTML 每日更新（>3 天）。
        只回報、不擋寫入（季節外頁面本來就不動）。"""
        now = now or datetime.now(timezone.utc)
        limits = {"snowcap": timedelta(hours=6)}
        limits.update({p: timedelta(days=3) for p in PHN_PAGES})
        out = {}
        for k, limit in limits.items():
            ts = source_times.get(k)
            if not ts:
                continue
            try:
                age = now - datetime.fromisoformat(ts)
            except ValueError:
                continue
            if age > limit:
                out[k] = round(age.total_seconds() / 3600, 1)
        return out


if __name__ == "__main__":
    c = JmaSeasonalCollector.__new__(JmaSeasonalCollector)
    c.name = "jma_seasonal"
    c._session = new_jma_session("jma-seasonal-test")
    c._stations, c._stations_loaded_at = {}, None
    c.state_path = Path("/nonexistent/jma_seasonal_state.json")
    c._state, c._pending_state = None, None
    out = c.collect()
    print({k: v for k, v in out.items() if k not in ("data", "raw_payload")})
