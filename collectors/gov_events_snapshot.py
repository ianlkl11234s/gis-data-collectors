"""政府裁罰/稽查「不收就流失」紅燈快照收集器（共用引擎 + 6 個薄子類）

背景與端點實測全文：
  taipei-gis-analytics/docs/topic-research/gov_events/endpoints.md
欄位語意 SSOT（統一事件 schema v1）：
  taipei-gis-analytics/docs/topic-research/gov_events/schema.md

為什麼是「快照 + diff」而不是一般 pipeline
--------------------------------------------------
這 6 支上游只呈現「現在有效的清單」或「近 N 年滾動窗口」，歷史正在持續消失：
  tpc_fire:145800  北市消防重大不合格   名單型，實測舊筆被新月份整批取代
  pcc:5988         拒絕往來廠商（現行） 名單型，Expire 一到就從檔案消失（實測最小 Expire = 隔日）
  twse_mops:22817  證期局上市處分       僅當年度，跨年歸零
  twse_mops:22818  證期局上櫃處分       僅當年度，跨年歸零
  fda:6133         邊境查驗不合格       滾動窗口 ~3.6 年，2023 前已流失
  moi_opd:7069     全國消防重大不合格   狀態名單（複查仍未改善者），改善即消失
  moe_sai:31768    留遊學契約查核       單年快照，改版即整批取代

上游沒有「除名事件」這種資料，只有「今天名單裡沒有它了」。
→ `last_seen_at` 只能靠每次快照的 diff 在 DB 端推出來，不做快照就永遠推不出來。

架構
----
`BaseCollector.run()` 用 `self.name` 分派 storage 與 supabase 寫入，一個 instance 服務不了 6 源，
所以做成「共用快照引擎 + 薄子類」：子類只宣告 `spec`（SnapshotSpec），抓取/正規化/守門全在引擎。

⚠️ 本 collector 只負責「忠實取得並正規化單次快照」，明確不做：
  - 不 geocode（`geom` / `addr_normalized` 留給 taipei-gis-analytics 落地 pipeline）
  - 不推 `first_seen_at` / `last_seen_at`（跨快照 DB 端 diff 才算得出來，單次 collect() 不可能知道）
  - 不 NLP 抽罰鍰金額（22817/22818 金額只在自由文字裡）→ `amount_twd` 一律 None，schema §1.4 明訂 NULL ≠ 0
  - 不判「有沒有事」（31768 有 724 筆是「查了沒事」）→ 忠實收全量，分類留給落地層

實測踩雷（都已寫進 spec，改動前先讀 endpoints.md）
  1. curl 全過但 requests 炸：data.taipei / web.pcc.gov.tw / opdadm.moi.gov.tw 的憑證缺 SKI，
     studyabroadinfo.moe.gov.tw 沒送中介憑證 → 這 4 支 `verify=False` 是硬需求，改 header 沒用。
  2. 慢端點靜默截斷：5988 / 6133 / 31768 為 2.4-2.8 MB，6133 實測單次可達 134.6 s
     → timeout 走 spec 自帶欄位（300 s），不吃 config.REQUEST_TIMEOUT。
  3. 22817/22818 的 Content-Type 不帶 charset，requests 會猜 ISO-8859-1
     → 引擎一律把 **bytes** 交給 parser，編碼由 parser 自決，絕不用 r.text。
  4. 序號欄（項次/編號/序號/_id）與 `出表日期` 每次都變 → 一律排除在 source_row_key 外。

寫入：
  - 本地 storage（BaseCollector.run 自動）
  - Supabase：**尚未註冊 transformer**（表結構待 gov_events 分流表拍板）。
    已確認 `SupabaseWriter._transform()` 對未註冊名字是 `return []`，`write()` 隨即 return
    → 即使誤開 *_ENABLED 也只寫本地檔，不會寫 DB、不會炸。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# 多支政府主機憑證鏈不完整（缺 SKI / 沒送中介憑證），verify=False 後關閉警告噪音
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER_AGENT = "GIS-DataCollectors/1.0 (gov-events-snapshot)"


# ============================================================
# 共用小工具
# ============================================================

def _s(v: Any) -> Optional[str]:
    """任意值 → 去頭尾空白的字串；空字串與 None 都回 None"""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _roc_compact(v: Any) -> Optional[date]:
    """民國緊縮日期 → date

    `1150717` → 2026-07-17；`1150813 00:00` → 2026-08-13（時分丟棄）
    容忍 2-3 位民國年（`991231`）。
    """
    s = _s(v)
    if not s:
        return None
    m = re.match(r"^(\d{2,3})(\d{2})(\d{2})(?:\s|$)", s)
    if not m:
        return None
    try:
        return date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _ce_slash(v: Any) -> Optional[date]:
    """西元斜線日期 → date：`2023/01/03` → 2023-01-03（也吃 `-` 分隔）"""
    s = _s(v)
    if not s:
        return None
    m = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})", s)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


_UBN_RE = re.compile(r"^\d{8}$")


def _ubn(v: Any) -> Optional[str]:
    """統編正規化 → 8 碼字串，不合格回 None

    5988 的 Corporation_Number 同檔混三種型別（endpoints.md §2）：
      - JSON number `52614922`   → 前導零已被吃掉，需 zfill(8)
      - JSON string `"04795023"` → 前導零有保留
      - 遮罩身分證 `"K1229*****"` → **不是統編**，回 None
    """
    s = _s(v)
    if not s:
        return None
    if s.isdigit():
        s = s.zfill(8)
        return s if _UBN_RE.match(s) else None
    return None


_SOLE_PROP_KW = ("事務所", "工作室", "工程行", "商行", "企業社", "商號", "診所")


def _target_type(name: Optional[str], ubn: Optional[str], raw_id: Any = None) -> str:
    """粗判受處分人型別（schema.md §1.3）

    保守策略：只有「明確看得出是自然人」才回 natural_person，其餘公司關鍵字回 company，
    其他一律 unknown —— 寧可 unknown 也不要把自然人誤標成 company 而被畫上地圖。

    ⚠️ 5988 實測 51 筆 `Corporation_Number` 是遮罩身分證（`E1009*****`），其中不少是
    「劉秉宏建築師事務所」這種**掛個人身分證的獨資行號** → 歸 `sole_proprietor` 而非
    `natural_person`（後者依 schema §5 整列不出點，誤標會平白丟掉可用資料）。
    """
    n = name or ""
    if any(k in n for k in ("公司", "有限", "股份", "銀行", "大廈", "社區")):
        return "company"
    rid = _s(raw_id) or ""
    masked_personal_id = "*" in rid or bool(re.match(r"^[A-Z][12]\d", rid))
    if any(k in n for k in _SOLE_PROP_KW):
        return "sole_proprietor"
    if masked_personal_id:
        return "natural_person"
    if ubn:
        return "company"
    return "unknown"


def _masked(v: Any) -> bool:
    """遮罩值（`新北市鶯歌區*****`）不可 geocode，也不該當地址存"""
    s = _s(v)
    return bool(s and "*" in s)


# ============================================================
# SnapshotSpec
# ============================================================

@dataclass(frozen=True)
class SnapshotSpec:
    """單一快照來源的完整宣告

    parser 收 **bytes**（不是 text）—— 編碼三態（big5 / utf-8-sig / utf-8）由各 parser 自決，
    引擎不猜 encoding（22817 的 Content-Type 不帶 charset，requests 會猜錯成 ISO-8859-1）。
    parser 回傳 list[dict]，每個 dict 是**原始欄名的原始列**；正規化交給 normalizer。
    """

    source_id: str                      # `{source_platform}:{nid}`，schema.md §1.1 複合鍵前綴
    source_platform: str
    dataset_nid: str                    # data.gov.tw nid（溯源用）
    url: str
    fmt: str                            # json | csv （informational；實際解碼在 parser）
    list_type: str                      # state_list | rolling_window | annual
    parser: Callable[[bytes], list[dict]]
    normalizer: Callable[["GovEventSnapshotCollector", dict], dict]
    key_fields: tuple[str, ...]         # 組 source_row_key 的原始欄名（排除易變欄）
    min_rows: int                       # 全有或全無守門下限，低於此值 raise
    timeout: int = 60                   # 慢端點自報，不吃 config.REQUEST_TIMEOUT
    verify: bool = True                 # 缺 SKI / 斷鏈的主機必須 False
    headers: dict = field(default_factory=dict)
    notes: str = ""


# ============================================================
# 共用引擎
# ============================================================

class GovEventSnapshotCollector(BaseCollector):
    """政府裁罰/稽查快照共用引擎——子類只覆寫 `name` / `interval_minutes` / `spec`"""

    spec: SnapshotSpec = None  # 子類必填

    COLLECT_TIMEOUT = 400  # 6133 實測單次可達 134.6 s，留足觀察上限

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        if self.spec.headers:
            self._session.headers.update(self.spec.headers)
        self._session.verify = self.spec.verify

    # ---------- 抓取 ----------

    def _fetch(self) -> bytes:
        resp = self._session.get(self.spec.url, timeout=self.spec.timeout)
        resp.raise_for_status()
        return resp.content

    # ---------- row key ----------

    def _row_key(self, row: dict) -> str:
        """自然鍵 → sha1[:16]

        key_fields 刻意排除每次都變的欄位（出表日期 / 項次 / 編號 / 序號 / _id / Appeal_Result），
        否則跨快照 uid 不穩，名單型 diff 的前提整個崩掉。
        """
        basis = "|".join(_s(row.get(f)) or "" for f in self.spec.key_fields)
        return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _row_hash(row: dict) -> str:
        """整列 hash（不含引擎後製欄位）——供重複列排序與 provenance 用"""
        blob = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:16]

    def _assign_keys(self, raw_rows: list[dict]) -> list[tuple[str, dict]]:
        """配 source_row_key，完全重複列用群內序位後綴 `#2`、`#3` 消解

        群內先按整列 hash 排序再編號：全同列排序是 no-op，有差異的列排序具決定性
        → 不依賴上游輸出順序穩定（6133 有 38 組、5988 有 4 組真重複列）。
        """
        counts = Counter(self._row_key(r) for r in raw_rows)
        buckets: dict[str, list[dict]] = {}
        for r in raw_rows:
            buckets.setdefault(self._row_key(r), []).append(r)

        out: list[tuple[str, dict]] = []
        for key, rows in buckets.items():
            if counts[key] == 1:
                out.append((key, rows[0]))
                continue
            for i, r in enumerate(sorted(rows, key=self._row_hash), start=1):
                out.append((key if i == 1 else f"{key}#{i}", r))
        return out

    # ---------- 正規化骨架 ----------

    def _base_record(self, snapshot_date: date, row_key: str, raw: dict,
                     fetched_at: datetime, upstream_stamp: Optional[str]) -> dict:
        """schema.md 的識別/溯源骨架，各源 normalizer 再往上疊語意欄位"""
        spec = self.spec
        return {
            "event_uid":       f"{spec.source_id}:{row_key}",
            "source_id":       spec.source_id,
            "source_platform": spec.source_platform,
            "source_row_key":  row_key,
            "snapshot_date":   snapshot_date.isoformat(),
            "list_type":       spec.list_type,
            "_provenance": {
                "dataset_nid":     spec.dataset_nid,
                "source_url":      spec.url,
                "fetched_at":      fetched_at.isoformat(),
                "upstream_stamp":  upstream_stamp,   # 上游自報的刷新戳（5988 Renewal_Date 等）
                "key_fields":      list(spec.key_fields),
                "row_hash":        self._row_hash(raw),
                "raw":             raw,              # 原欄名原值；parser 有 bug 時唯一救援路徑
            },
        }

    # ---------- 主流程 ----------

    def collect(self) -> dict:
        spec = self.spec
        now = datetime.now(tz=TAIPEI_TZ)
        snapshot_date = now.date()

        payload = self._fetch()
        raw_rows = spec.parser(payload)

        # 全有或全無：殘缺快照在下游會被讀成「大量除名」，比沒收更糟
        if len(raw_rows) < spec.min_rows:
            raise ValueError(
                f"[{self.name}] 快照疑似不完整：解析 {len(raw_rows)} 列 < 下限 {spec.min_rows} "
                f"（bytes={len(payload)}）——拒絕回傳部分資料"
            )

        upstream_stamp = getattr(spec.parser, "last_upstream_stamp", None)

        records = []
        for row_key, raw in self._assign_keys(raw_rows):
            rec = self._base_record(snapshot_date, row_key, raw, now, upstream_stamp)
            rec.update(spec.normalizer(self, raw))
            records.append(rec)

        return {
            "data":            records,
            "row_count":       len(records),
            "source_id":       spec.source_id,
            "list_type":       spec.list_type,
            "snapshot_date":   snapshot_date.isoformat(),
            "upstream_stamp":  upstream_stamp,
            "collected_at":    now.isoformat(),
        }


# ============================================================
# parser：bytes → list[dict]（原欄名原值）
# ============================================================

def _parse_csv(payload: bytes, encoding: str) -> list[dict]:
    text = payload.decode(encoding)
    rows = list(csv.DictReader(io.StringIO(text)))
    # 政府 CSV 常有行尾多餘逗號 → DictReader 產生空欄名的偽欄（7069 實測）
    return [{k: v for k, v in r.items() if _s(k)} for r in rows]


def parse_csv_utf8_bom(payload: bytes) -> list[dict]:
    """7069 / 22817 / 22818：UTF-8 with BOM"""
    return _parse_csv(payload, "utf-8-sig")


def parse_csv_big5(payload: bytes) -> list[dict]:
    """145800 CSV 備援路徑：BIG5（主路徑走 data.taipei JSON）"""
    return _parse_csv(payload, "cp950")


def parse_json_array(payload: bytes) -> list[dict]:
    """6133 / 31768：裸 JSON 陣列"""
    data = json.loads(payload.decode("utf-8-sig"))
    if not isinstance(data, list):
        raise ValueError(f"預期 JSON array，實得 {type(data).__name__}")
    return data


def parse_pcc_rvlm(payload: bytes) -> list[dict]:
    """5988：三層包裝 {"Rvlmd_List": {"title", "Renewal_Date", "Rvlmd": [...]}}"""
    data = json.loads(payload.decode("utf-8-sig"))
    wrapper = data.get("Rvlmd_List") or {}
    rows = wrapper.get("Rvlmd")
    if not isinstance(rows, list):
        raise ValueError("5988 回應缺 Rvlmd_List.Rvlmd 陣列")
    # 上游自報的名單刷新時刻（`20260814 00:09`），比我們的 snapshot_date 精確
    parse_pcc_rvlm.last_upstream_stamp = _s(wrapper.get("Renewal_Date"))
    return rows


parse_pcc_rvlm.last_upstream_stamp = None


def parse_data_taipei(payload: bytes) -> list[dict]:
    """145800 主路徑：data.taipei v1 API

    名單型必須驗 count == len(results)，且 count > limit 直接 raise
    —— 殘缺快照 = 假除名（endpoints.md §8-1）。
    """
    data = json.loads(payload.decode("utf-8-sig"))
    result = data.get("result") or {}
    rows = result.get("results")
    if not isinstance(rows, list):
        raise ValueError("145800 回應缺 result.results 陣列")
    count = result.get("count")
    limit = result.get("limit")
    if isinstance(count, int):
        if isinstance(limit, int) and count > limit:
            raise ValueError(f"145800 需要分頁：count={count} > limit={limit}，本引擎未實作分頁")
        if count != len(rows):
            raise ValueError(f"145800 count={count} 與實得 {len(rows)} 列不符")
    stamps = [
        (r.get("_importdate") or {}).get("date")
        for r in rows if isinstance(r.get("_importdate"), dict)
    ]
    parse_data_taipei.last_upstream_stamp = max((s for s in stamps if s), default=None)
    return rows


parse_data_taipei.last_upstream_stamp = None


# ============================================================
# normalizer：原始列 → schema.md 語意欄位
# ============================================================

def norm_tpc_fire_145800(self: GovEventSnapshotCollector, r: dict) -> dict:
    d = _roc_compact(r.get("檢查日期"))
    name = _s(r.get("場所名稱"))
    return {
        "agency_name":    "臺北市政府消防局",
        "agency_code":    "TPE_FIRE",
        "agency_level":   "local",
        "agency_county":  "A",                       # 臺北市
        "target_raw":     name,
        "target_name":    name,
        "target_person":  None,
        "target_type":    _target_type(name, None),
        "target_ubn":     None,
        "target_ubn_source": None,
        "law_name":       None,
        "law_article":    None,
        "violation_desc": _s(r.get("違規項目")),
        "sanction_type":  "rectify_order",
        "amount_twd":     None,                      # 來源無此欄 → NULL ≠ 0
        "sanction_detail": _s(r.get("場所用途")),
        "date_sanction":  _iso(d),
        "date_announce":  None,
        "date_violation": None,
        "date_primary":   "date_sanction",
        "record_status":  "active",
        "valid_from":     None,
        "valid_to":       None,
        "addr_raw":       _s(r.get("場所地址")),
        "loc_semantics":  "target_addr",
        "county_code":    "A",
    }


def norm_pcc_5988(self: GovEventSnapshotCollector, r: dict) -> dict:
    raw_id = r.get("Corporation_Number")
    ubn = _ubn(raw_id)
    name = _s(r.get("Corporation_Name"))
    addr = r.get("Corporation_Address")
    ttype = _target_type(name, ubn, raw_id)
    # 自然人列的地址是遮罩的（`新北市鶯歌區*****`）→ 不存 addr_raw，避免下游誤 geocode
    return {
        "agency_name":    _s(r.get("Announce_Agency_Name")),
        "agency_code":    _s(r.get("Announce_Agency_No")),
        # ⚠️ 本源是唯一「公告機關橫跨中央與地方」的來源，且無法從 Announce_Agency_No 推：
        #    實測 prefix `3.` 底下同時有「國防部軍備局」與「高雄市政府勞工局」→ 規則不成立。
        #    誠實留 None，由落地層以機關名稱對照機關代碼表判定，不在此處猜。
        "agency_level":   None,
        "agency_county":  None,
        "target_raw":     name,
        "target_name":    name if ttype != "natural_person" else None,
        "target_person":  _s(r.get("Supplier_Principal")),
        "target_type":    ttype,
        "target_ubn":     ubn,
        "target_ubn_source": "direct" if ubn else None,
        "law_name":       _s(r.get("Suitable_Law")) or "政府採購法",
        "law_article":    _s(r.get("GPA101_Caluse")),
        "violation_desc": _s(r.get("Crime_Info")) or _s(r.get("GPA101_Caluse")),
        "sanction_type":  "blacklist",
        "amount_twd":     None,
        "sanction_detail": _s(r.get("Effective_Duration")),
        "date_sanction":  None,
        "date_announce":  _iso(_roc_compact(r.get("Announce_Date"))),
        "date_violation": None,
        "date_primary":   "date_announce",
        "record_status":  "active",    # 檔案只含現行有效者；期滿即消失
        "valid_from":     _iso(_roc_compact(r.get("Effective_Date"))),
        "valid_to":       _iso(_roc_compact(r.get("Expire_Date"))),
        "addr_raw":       None if _masked(addr) else _s(addr),
        "loc_semantics":  "target_addr",
        "county_code":    None,
        # 本源特有：機關地址與標案，落地層可另建關聯
        "extra": {
            "case_no":                _s(r.get("Case_no")),
            "case_name":              _s(r.get("Case_Name")),
            "agency_addr_raw":        _s(r.get("Announce_Agency_Address")),
            "origional_announce_date": _iso(_roc_compact(r.get("Origional_Announce_Date"))),
            "remark":                 _s(r.get("Remark")),
            "judgment_doc_no":        _s(r.get("Judgment_Doc_No")),
            "corporation_country":    _s(r.get("Corporation_Country")),
            "addr_is_masked":         _masked(addr),
        },
    }


def _norm_twse_sfb(self: GovEventSnapshotCollector, r: dict, board: str) -> dict:
    name = _s(r.get("公司名稱"))
    return {
        "agency_name":    "金融監督管理委員會證券期貨局",
        "agency_code":    "FSC_SFB",
        "agency_level":   "central",
        "agency_county":  None,
        "target_raw":     name,
        "target_name":    name,
        "target_person":  None,
        "target_type":    "company",
        "target_ubn":     None,
        "target_ubn_source": None,
        "law_name":       _s(r.get("違反法規")),
        "law_article":    None,
        "violation_desc": _s(r.get("違規事由")),
        "sanction_type":  "fine",
        "amount_twd":     None,   # 金額只在「裁處情形」自由文字裡，v1 不 NLP 抽 → NULL ≠ 0
        "sanction_detail": _s(r.get("裁處情形")),
        "date_sanction":  _iso(_roc_compact(r.get("發函日期"))),
        "date_announce":  None,
        "date_violation": None,
        "date_primary":   "date_sanction",
        "record_status":  "active",
        "valid_from":     None,
        "valid_to":       None,
        "addr_raw":       None,
        # 無任何地址欄 → 只能畫面量圖，不得畫點（schema.md §4 硬規則）
        "loc_semantics":  "agency_only",
        "county_code":    None,
        "extra": {
            "board":         board,             # listed 上市 / otc 上櫃
            "stock_code":    _s(r.get("股票代號")),
            # 出表日期＝每日重打的匯出戳，只留檔不入 key
            "export_date":   _iso(_roc_compact(r.get("出表日期"))),
        },
    }


def norm_twse_22817(self, r): return _norm_twse_sfb(self, r, "listed")
def norm_twse_22818(self, r): return _norm_twse_sfb(self, r, "otc")


def norm_fda_6133(self: GovEventSnapshotCollector, r: dict) -> dict:
    name = _s(r.get("進口商名稱"))
    detail = " / ".join(x for x in (
        _s(r.get("不合格原因暨檢出量詳細說明")), _s(r.get("處置情形"))) if x)
    return {
        "agency_name":    "衛生福利部食品藥物管理署",
        "agency_code":    "MOHW_FDA",
        "agency_level":   "central",
        "agency_county":  None,
        "target_raw":     name,
        "target_name":    name,
        "target_person":  None,
        "target_type":    _target_type(name, None),
        "target_ubn":     None,
        "target_ubn_source": None,
        "law_name":       "食品安全衛生管理法",
        "law_article":    _s(r.get("法規限量標準")),
        "violation_desc": _s(r.get("原因")),
        "sanction_type":  "inspection_fail",
        "amount_twd":     None,
        "sanction_detail": detail or None,
        "date_sanction":  None,
        "date_announce":  _iso(_ce_slash(r.get("發布日期"))),
        "date_violation": None,
        "date_primary":   "date_announce",
        # 滾動窗口型：檔案內的列不代表「現在有效」，是歷史列
        "record_status":  "historical",
        "valid_from":     None,
        "valid_to":       None,
        "addr_raw":       _s(r.get("進口商地址")),
        "loc_semantics":  "target_addr",
        "county_code":    None,
        "extra": {
            "product":            _s(r.get("主旨")),
            "origin":             _s(r.get("產地")),
            "ccc_code":           _s(r.get("貨品分類號列")),
            "manufacturer":       _s(r.get("製造廠或出口商名稱")),
            "manufacturer_code":  _s(r.get("製造商代碼")),
            "brand":              _s(r.get("牌名")),
            "weight":             _s(r.get("重量")),
            "test_method":        _s(r.get("檢驗方法")),
            "declare_date":       _iso(_ce_slash(r.get("報驗受理日期"))),
            "attachment_url":     _s(r.get("附圖")),
        },
    }


def norm_moi_7069(self: GovEventSnapshotCollector, r: dict) -> dict:
    name = _s(r.get("不合格場所名稱"))
    return {
        "agency_name":    _s(r.get("填報單位")) or "內政部消防署",
        "agency_code":    "MOI_NFA",
        "agency_level":   "central",
        "agency_county":  None,   # 填報單位恆為「消防署」，縣市要從「地點」前綴解析（落地層做）
        "target_raw":     name,
        "target_name":    name,
        "target_person":  None,
        "target_type":    _target_type(name, None),
        "target_ubn":     None,
        "target_ubn_source": None,
        "law_name":       "消防法",
        "law_article":    None,
        "violation_desc": _s(r.get("檢查結果")),
        "sanction_type":  "rectify_order",
        "amount_twd":     None,
        "sanction_detail": None,
        "date_sanction":  _iso(_roc_compact(r.get("檢查日期"))),
        "date_announce":  None,
        "date_violation": None,
        "date_primary":   "date_sanction",
        "record_status":  "active",   # 狀態名單：複查仍未改善者，改善即消失
        "valid_from":     None,
        "valid_to":       None,
        "addr_raw":       _s(r.get("地點")),
        "loc_semantics":  "target_addr",
        "county_code":    None,
    }


def norm_moe_31768(self: GovEventSnapshotCollector, r: dict) -> dict:
    name = _s(r.get("公司名稱"))
    ubn = _ubn(r.get("統一編號"))
    roc_year = _s(r.get("年度"))
    files = r.get("資料JSON檔案") or []
    statuses = [_s(f.get("符合狀況")) for f in files if isinstance(f, dict)]
    return {
        "agency_name":    "教育部國際及兩岸教育司",
        "agency_code":    "MOE_SAI",
        "agency_level":   "central",
        "agency_county":  None,
        "target_raw":     name,
        "target_name":    name,
        "target_person":  None,
        "target_type":    _target_type(name, ubn),
        "target_ubn":     ubn,
        "target_ubn_source": "direct" if ubn else None,
        "law_name":       "留學業務機構及遊學業務機構定型化契約應記載及不得記載事項",
        "law_article":    None,
        "violation_desc": _s(r.get("審查備註")),
        "sanction_type":  "inspection_fail",
        "amount_twd":     None,
        "sanction_detail": _s(r.get("審核方式")),
        "date_sanction":  None,
        # 來源只有「年度」沒有日期 → 不捏造月日，日期留 NULL，年度另存 extra
        "date_announce":  None,
        "date_violation": None,
        "date_primary":   "date_announce",
        "record_status":  "active",
        "valid_from":     None,
        "valid_to":       None,
        "addr_raw":       _s(r.get("地址")),
        "loc_semantics":  "target_addr",
        "county_code":    None,
        "extra": {
            "roc_year":        roc_year,
            "ce_year":         (int(roc_year) + 1911) if roc_year and roc_year.isdigit() else None,
            "ad_name":         _s(r.get("廣告名稱")),
            "postal_code":     _s(r.get("郵遞區號")),
            "in_association":  _s(r.get("是否加入公會")),
            "check_category":  _s(r.get("查核類別")),
            "check_category_code": _s(r.get("查核類別編號")),
            # ⚠️ 929 筆中 724 筆是「查了沒事」，靠這裡的符合狀況區分，不可全當裁罰事件
            "check_items":     files,
            "compliance_flags": statuses,
        },
    }


# ============================================================
# 6 個薄子類（每個只宣告 spec）
# ============================================================

class GovEventsTpcFire145800Collector(GovEventSnapshotCollector):
    """臺北市消防安全檢查重大不合格場所（名單型，週更）"""

    name = "gov_events_tpc_fire_145800"
    interval_minutes = config.GOV_EVENTS_TPC_FIRE_145800_INTERVAL
    spec = SnapshotSpec(
        source_id="tpc_fire:145800",
        source_platform="tpc_fire",
        dataset_nid="145800",
        url=("https://data.taipei/api/v1/dataset/"
             "d417e957-7205-451e-bf45-a31db78f6ec8?scope=resourceAquire&limit=1000&offset=0"),
        fmt="json",
        list_type="state_list",
        parser=parse_data_taipei,
        normalizer=norm_tpc_fire_145800,
        key_fields=("場所名稱", "場所地址", "檢查日期", "違規項目"),
        min_rows=1,
        timeout=60,
        verify=False,   # data.taipei 憑證缺 SKI
        notes="CSV 備援：https://data.taipei/api/dataset/871874fc-c3b8-4752-aa56-76101395589d/"
              "resource/d417e957-7205-451e-bf45-a31db78f6ec8/download（BIG5，走 parse_csv_big5）",
    )


class GovEventsPcc5988Collector(GovEventSnapshotCollector):
    """工程會 拒絕往來廠商（現行名單，每日）"""

    name = "gov_events_pcc_5988"
    interval_minutes = config.GOV_EVENTS_PCC_5988_INTERVAL
    spec = SnapshotSpec(
        source_id="pcc:5988",
        source_platform="pcc",
        dataset_nid="5988",
        url="https://web.pcc.gov.tw/vms/rvlm/rvlmPublicSearch/queryRVFile/json",
        fmt="json",
        list_type="state_list",
        parser=parse_pcc_rvlm,
        normalizer=norm_pcc_5988,
        # 排除 Appeal_Result：自由文字且上游會事後補寫，入 key 會造成假除名
        key_fields=("Corporation_Number", "Announce_Agency_No", "Case_no",
                    "Origional_Announce_Date", "Effective_Date", "Expire_Date",
                    "GPA101_Caluse", "Corporation_Name", "Case_Name"),
        min_rows=1000,   # 2026-08-14 實測 1,815
        timeout=300,     # 實測 30-47 s
        verify=False,    # web.pcc.gov.tw 憑證缺 SKI
    )


class GovEventsTwse22817Collector(GovEventSnapshotCollector):
    """證期局 上市公司裁罰（僅當年度，每週）"""

    name = "gov_events_twse_22817"
    interval_minutes = config.GOV_EVENTS_TWSE_22817_INTERVAL
    spec = SnapshotSpec(
        source_id="twse_mops:22817",
        source_platform="twse_mops",
        dataset_nid="22817",
        url="https://mopsfin.twse.com.tw/opendata/t187ap22_L.csv",
        fmt="csv",
        list_type="annual",
        parser=parse_csv_utf8_bom,
        normalizer=norm_twse_22817,
        key_fields=("發函日期", "股票代號", "違規事由", "裁處情形"),  # 排除每日變動的 出表日期
        min_rows=1,
        timeout=60,
        verify=True,
    )


class GovEventsTwse22818Collector(GovEventSnapshotCollector):
    """證期局 上櫃公司裁罰（僅當年度，每週）"""

    name = "gov_events_twse_22818"
    interval_minutes = config.GOV_EVENTS_TWSE_22818_INTERVAL
    spec = SnapshotSpec(
        source_id="twse_mops:22818",
        source_platform="twse_mops",
        dataset_nid="22818",
        url="https://mopsfin.twse.com.tw/opendata/t187ap22_O.csv",
        fmt="csv",
        list_type="annual",
        parser=parse_csv_utf8_bom,
        normalizer=norm_twse_22818,
        key_fields=("發函日期", "股票代號", "違規事由", "裁處情形"),
        min_rows=1,
        timeout=60,
        verify=True,
    )


class GovEventsFda6133Collector(GovEventSnapshotCollector):
    """食藥署 邊境查驗不合格（滾動窗口 ~3.6 年，每 7 日）"""

    name = "gov_events_fda_6133"
    interval_minutes = config.GOV_EVENTS_FDA_6133_INTERVAL
    spec = SnapshotSpec(
        source_id="fda:6133",
        source_platform="fda",
        dataset_nid="6133",
        url="https://data.fda.gov.tw/data/opendata/export/52/json",
        fmt="json",
        list_type="rolling_window",
        parser=parse_json_array,
        normalizer=norm_fda_6133,
        # 附圖 id（.NET ticks）實測 2,550/2,550 唯一，是本源唯一可靠自然鍵；
        # 身份欄位組合就算疊到 9 欄仍有 38 組重複列（同批貨拆多列）。
        key_fields=("附圖", "發布日期", "進口商名稱", "主旨"),
        min_rows=1500,   # 2026-08-14 實測 2,550
        timeout=300,     # 實測 17.7-134.6 s，抖動極大
        verify=True,
    )


class GovEventsMoi7069Collector(GovEventSnapshotCollector):
    """消防署 全國重大不合格場所（狀態名單，每月）"""

    name = "gov_events_moi_7069"
    interval_minutes = config.GOV_EVENTS_MOI_7069_INTERVAL
    spec = SnapshotSpec(
        source_id="moi_opd:7069",
        source_platform="moi_opd",
        dataset_nid="7069",
        url=("https://opdadm.moi.gov.tw/api/v1/no-auth/resource/api/dataset/"
             "C955F9D4-0984-4F5B-8D89-880B3A9BBB46/resource/"
             "C0CC7E90-3F6A-4C14-9842-40289333692B/download"),
        fmt="csv",
        list_type="state_list",
        parser=parse_csv_utf8_bom,
        normalizer=norm_moi_7069,
        # ⚠️ 表頭第一欄實際是「編號」不是 metadata 宣告的「序號」，且流水號不可入 key
        key_fields=("不合格場所名稱", "地點", "檢查日期", "檢查結果"),
        min_rows=100,    # 2026-08-14 實測 168
        timeout=60,
        verify=False,    # opdadm.moi.gov.tw 憑證缺 SKI
    )


class GovEventsMoe31768Collector(GovEventSnapshotCollector):
    """教育部 留遊學業者契約查核（單年快照，每 30 日）"""

    name = "gov_events_moe_31768"
    interval_minutes = config.GOV_EVENTS_MOE_31768_INTERVAL
    spec = SnapshotSpec(
        source_id="moe_sai:31768",
        source_platform="moe_sai",
        dataset_nid="31768",
        # 尾巴 `&P1=&P2`（P2 無等號）是 data.gov.tw metadata 原文，實測可用，不要「修正」
        url="https://studyabroadinfo.moe.gov.tw/web/API/Handler.ashx?SO=LCR&P1=&P2",
        fmt="json",
        list_type="annual",
        parser=parse_json_array,
        normalizer=norm_moe_31768,
        key_fields=("統一編號", "年度", "查核類別編號"),   # 實測 929/929 唯一
        min_rows=500,    # 2026-08-14 實測 929
        timeout=300,     # 實測 15.7-30.6 s
        verify=False,    # studyabroadinfo.moe.gov.tw 沒送中介憑證
    )
