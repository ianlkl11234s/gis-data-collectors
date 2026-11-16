# 來源：taipei-gis-analytics pipelines/realtime/road_gazetteer/match.py
#   commit 738de02069483d128ca078a60dd2fe3cad49ec41（2026-11-16，RULES_VERSION=1）
#   原檔 sha256 9cb246c859f90f68efbab17abdc5f938109a7378fe16143165d8f28227984acd
# 原封不動複製（只加這段標頭）；要改規則請改上游再重新複製，並更新這裡的 commit／sha256。
# 對應的 sqlite（road_gazetteer_<YYYYMMDD>.sqlite）不進 git，部署時另外 scp（見 README）。
"""路名對照表查詢模組（給收集器用）。

只依賴標準庫（sqlite3、re、hashlib），VM 上不需額外套件。
正規化函式同時被 build_gazetteer.py 匯入，OSM 名稱與查詢文字走同一套規則。

用法：
    from match import Gazetteer
    gz = Gazetteer("road_gazetteer_20261116.sqlite")
    r = gz.match("彰化縣", "埔鹽鄉彰水路二段148巷")
    # r = {"precision": "road", "type": "road", "match_level": "full", "road_key": ..., ...}

回傳一律是 dict；precision 只有三種：
    'road'      對到路段（type='road' 或 'intersection'），lon/lat 有值
    'district'  只解析到鄉鎮，lon/lat 為 None，只給 town_code
    'none'      連鄉鎮都解析不出，lon/lat、town_code 皆 None
絕不回傳猜測座標。
"""
from __future__ import annotations

import hashlib
import re
import sqlite3

RULES_VERSION = "1"

# ── 正規化 ────────────────────────────────────────────────────────────
_FW = {ord(c): ord(c) - 0xFEE0 for c in "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－［］（）／"}
_FW[ord("　")] = ord(" ")
_FW[ord("【")] = ord("[")
_FW[ord("】")] = ord("]")
_FW[ord("、")] = ord("、")

_CN_DIGIT = {"零": 0, "〇": 0, "一": 1, "二": 2, "兩": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
_CN_CHARS = "零〇一二三四五六七八九十百兩"
_DIGIT_CN = "零一二三四五六七八九"


def cn_to_int(s: str) -> int | None:
    """國字數字（≤999）轉整數；不是國字數字回 None。"""
    if not s or any(c not in _CN_CHARS for c in s):
        return None
    total, cur = 0, 0
    for c in s:
        if c in _CN_DIGIT:
            cur = _CN_DIGIT[c]
        elif c == "十":
            total += (cur or 1) * 10
            cur = 0
        elif c == "百":
            total += (cur or 1) * 100
            cur = 0
    return total + cur


def int_to_cn(n: int) -> str:
    """段號用：1→一、10→十、12→十二、20→二十。"""
    if n < 10:
        return _DIGIT_CN[n]
    if n < 20:
        return "十" + (_DIGIT_CN[n % 10] if n % 10 else "")
    return _DIGIT_CN[n // 10] + "十" + (_DIGIT_CN[n % 10] if n % 10 else "")


_SEG_DIGIT_RE = re.compile(r"(?<!\d)(\d{1,2})段")
_LANE_CN_RE = re.compile(r"([零〇一二三四五六七八九十百兩]+)([巷弄])")


def norm_basic(s: str | None) -> str:
    """臺/台統一、全半形、去空白。"""
    s = (s or "").translate(_FW)
    s = s.replace("台", "臺")
    s = re.sub(r"\s+", "", s)
    return s


def norm_name(s: str | None) -> str:
    """路名正規化：段號一律國字（2段→二段），巷弄號一律阿拉伯數字（七巷→7巷）。"""
    s = norm_basic(s)
    s = _SEG_DIGIT_RE.sub(lambda m: int_to_cn(int(m.group(1))) + "段", s)
    s = _LANE_CN_RE.sub(lambda m: f"{cn_to_int(m.group(1))}{m.group(2)}", s)
    return s


# 門牌號以下（號、之、樓、鄰）全部去掉
_HOUSE_RE = re.compile(r"[\d一二三四五六七八九十百之\-~]*\d*號.*$|\d+[之\-]\d+.*$|\d+樓.*$|\d+鄰.*$")
_SUFFIX_CHARS = ("路", "街", "道", "段", "巷", "弄")
_SEG_RE = re.compile(r"[一二三四五六七八九十]+段")


def strip_house(s: str) -> str:
    return _HOUSE_RE.sub("", s)


_LANE_SPLIT_RE = re.compile(r"^(.*?(?:路|街|道|段))(\d+巷(?:\d+弄)?|\d+弄)$")
_SEG_TAIL_RE = re.compile(r"^(.+?(?:路|街|道))[一二三四五六七八九十]+段$")


def strip_lane(name_full: str) -> str:
    """從字尾去巷弄（保留段）：彰水路二段148巷 → 彰水路二段；中正路8巷3弄 → 中正路。
    只有「路／街／道／段＋數字巷弄」才算某路的巷弄；東崎7巷、大岩巷、中正路和平巷是自己的名字，不剝。"""
    m = _LANE_SPLIT_RE.match(name_full)
    return m.group(1) if m else name_full


def has_lane(name_full: str) -> bool:
    return _LANE_SPLIT_RE.match(name_full) is not None


def name_base(name_full: str) -> str:
    """從字尾去巷弄、再去段號：彰水路二段148巷 → 彰水路；公道五路二段 → 公道五路；鐵路街 → 鐵路街。"""
    s = strip_lane(name_full)
    m = _SEG_TAIL_RE.match(s)
    return m.group(1) if m else s


def road_key(county: str, town: str, name_full: str) -> str:
    """穩定 key：sha1("縣市|鄉鎮|name_full") 前 12 碼（皆為正規化後字串）。"""
    return hashlib.sha1(f"{county}|{town}|{name_full}".encode("utf-8")).hexdigest()[:12]


def road_prefixes(text: str) -> list[str]:
    """列出以路／街／道／段／巷／弄結尾的前綴，由長到短。"""
    out = []
    for i in range(len(text), 1, -1):
        if text[i - 1] in _SUFFIX_CHARS:
            out.append(text[:i])
    return out


_VILLAGE_RE = re.compile(r"^[^路街巷弄段\d]{1,4}?[村里](.+)$")
_INTER_RE = re.compile(r"(?<=[路街道段巷弄])(?:交叉口)?(?:和|與|及|&|、)|、")


def split_intersection(text: str):
    """「A路和B路」「A路與B街」「仁一、劉銘傳路」→ (A, B)；不是交叉口回 None。
    和／與／及 必須緊接在路名字尾後（避免切開「和平路」「貴和街」）；「、」一律視為分隔。"""
    m = _INTER_RE.search(text)
    if not m:
        return None
    a, b = text[: m.start()], text[m.end():]
    b = re.sub(r"(?:交叉)?口.*$", "", b)
    if len(a) < 1 or len(b) < 2 or not road_prefixes(b):
        return None
    return a, b
_PAREN_RE = re.compile(r"\([^)]*\)?")


# ── 查詢 ─────────────────────────────────────────────────────────────
class Gazetteer:
    """唯讀查詢。鄉鎮表（368 列）載入記憶體，道路一律走 sqlite 索引查詢。"""

    INTERSECTION_MAX_M = 30  # 與建置時相同；只收建置時就相距 ≤30 m 的路對

    def __init__(self, path: str):
        self.db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
        self.db.execute("PRAGMA cache_size=-2000")  # 約 2 MB
        self.towns: dict[str, list[tuple[str, str]]] = {}  # county -> [(town, town_code)]，長名在前
        for county, town, code in self.db.execute("SELECT county, town, town_code FROM towns"):
            self.towns.setdefault(county, []).append((town, code))
        for v in self.towns.values():
            v.sort(key=lambda x: -len(x[0]))

    def close(self):
        self.db.close()

    # 鄉鎮解析 ---------------------------------------------------------
    def _strip_county(self, county: str, t: str) -> str:
        if county and t.startswith(county):
            return t[len(county):].lstrip("-")
        return t.lstrip("-")

    def _resolve_town(self, county: str, t: str):
        """回 (town, town_code, rest) 或 None。"""
        for town, code in self.towns.get(county, []):
            if t.startswith(town):
                return town, code, t[len(town):].lstrip("-")
        # 截斷的鄉鎮名（例：「阿里」）：整段文字是唯一一個鄉鎮名的前綴才接受
        if len(t) >= 2:
            hits = [(tn, c) for tn, c in self.towns.get(county, []) if tn.startswith(t)]
            if len(hits) == 1:
                return hits[0][0], hits[0][1], ""
        return None

    # 路名查表 ---------------------------------------------------------
    def _lookup(self, county, town, name):
        return self.db.execute(
            "SELECT id, road_key, name_full, mid_lon, mid_lat, length_m, alt_count, kind "
            "FROM roads WHERE county=? AND town=? AND name_full=?",
            (county, town, name),
        ).fetchone()

    def _find_road(self, county, town, text):
        """回 (row, match_level) 或 None。依序：完整 → 去巷弄 → 去段號；先不剝村里，查不到再剝。"""
        texts = [text]
        m = _VILLAGE_RE.match(text)
        if m:
            texts.append(m.group(1))
        for t in texts:
            prefixes = road_prefixes(t)
            if not prefixes:
                continue
            longest = prefixes[0]
            tried = set()
            # 完整（最長前綴）→ 較短前綴（去巷弄）→ 去段號
            cands = [(p, None) for p in prefixes]
            base = name_base(longest)
            cands.append((base, "base"))
            for p, lvl in cands:
                if p in tried or len(p) < 2:
                    continue
                tried.add(p)
                row = self._lookup(county, town, p)
                if row:
                    if p == longest:
                        lvl = "full"
                    elif _SEG_RE.search(longest) and not _SEG_RE.search(p):
                        lvl = "base"  # 去段號
                    else:
                        lvl = "no_lane"  # 去巷弄（保留段）
                    return row, lvl
        return None

    def _intersection(self, county, town, row_a, row_b):
        ids_a = self._member_ids(county, town, row_a)
        ids_b = self._member_ids(county, town, row_b)
        best = None
        qa = ",".join("?" * len(ids_a))
        qb = ",".join("?" * len(ids_b))
        sql = (f"SELECT lon, lat, dist_m FROM intersections WHERE "
               f"(a IN ({qa}) AND b IN ({qb})) OR (a IN ({qb}) AND b IN ({qa})) ORDER BY dist_m LIMIT 1")
        best = self.db.execute(sql, (*ids_a, *ids_b, *ids_b, *ids_a)).fetchone()
        return best

    def _member_ids(self, county, town, row):
        # 彙總列（去段號的整條路）：自己＋它底下的各段都查
        if row[7] == "base_agg":
            ids = [r[0] for r in self.db.execute(
                "SELECT id FROM roads WHERE county=? AND town=? AND name_base=? AND kind='road' AND has_lane=0",
                (county, town, row[2]))]
            return [row[0]] + ids
        return [row[0]]

    # 主介面 -----------------------------------------------------------
    def match(self, county: str | None, location_text: str | None) -> dict:
        county = norm_basic(county)
        raw = location_text or ""
        out = {"precision": "none", "type": None, "match_level": None, "county": county or None,
               "town": None, "town_code": None, "road_key": None, "name": None,
               "lon": None, "lat": None, "length_m": None, "alt_count": None, "note": None}
        t = norm_name(raw)
        t = _PAREN_RE.sub("", t)
        # 「地標[路名」：取括號後；括號後若又帶縣市鄉鎮就再剝，沒帶就沿用前面的鄉鎮
        before, after = t, None
        if "[" in t:
            before, after = t.split("[", 1)
            after = after.replace("]", "")
        elif "/" in t:
            before, after = t.split("/", 1)
        before = self._strip_county(county, before)
        tw = self._resolve_town(county, before)
        rest = tw[2] if tw else None
        if after is not None:
            a2 = self._strip_county(county, after)
            tw2 = self._resolve_town(county, a2)
            if tw2:
                tw, rest = tw2, tw2[2]
            elif tw:
                rest = a2
            out["note"] = "bracket" if "[" in t else "slash"
        if not tw:
            out["note"] = "town_unresolved"
            return out
        town, code = tw[0], tw[1]
        out.update(precision="district", town=town, town_code=code)
        rest = strip_house(rest)
        if not rest:
            return out

        # 交叉口「A路和B路」
        sp = split_intersection(rest)
        if sp:
            a_txt, b_txt = sp
            if not road_prefixes(a_txt):
                # 「仁一、劉銘傳路」：前段省略字尾，借後段的字尾補齊
                for suf in ("大道", "路", "街"):
                    if b_txt.endswith(suf) or re.search(suf + r"[一二三四五六七八九十]*段?$", b_txt):
                        a_txt = a_txt + suf
                        break
            ra = self._find_road(county, town, a_txt)
            rb = self._find_road(county, town, b_txt)
            if ra and rb:
                hit = self._intersection(county, town, ra[0], rb[0])
                if hit:
                    row, lvl = ra
                    out.update(precision="road", type="intersection", match_level=lvl, road_key=row[1],
                               name=row[2], lon=hit[0], lat=hit[1], length_m=row[5], alt_count=row[6],
                               note="intersection")
                    return out
            # 兩路不相接或其中一條對不到：回第一條（對不到再試第二條）的中點，不硬取最近點
            for r, nt in ((ra, "intersection_fallback_first"), (rb, "intersection_fallback_second")):
                if r:
                    row, lvl = r
                    out.update(precision="road", type="road", match_level=lvl, road_key=row[1], name=row[2],
                               lon=row[3], lat=row[4], length_m=row[5], alt_count=row[6], note=nt)
                    return out
            return out

        r = self._find_road(county, town, rest)
        if r:
            row, lvl = r
            out.update(precision="road", type="road", match_level=lvl, road_key=row[1], name=row[2],
                       lon=row[3], lat=row[4], length_m=row[5], alt_count=row[6])
        return out
