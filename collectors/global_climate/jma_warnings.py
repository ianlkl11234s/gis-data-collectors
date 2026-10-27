"""日本氣象廳 警報・注意報（R8 新防災氣象情報）收集器 — 只記變動（gis-platform migration 438）

資料來源：JMA bosai（免認證、非公式 JSON）
  https://www.jma.go.jp/bosai/warning/data/r8/map_time.json → {"latestControlDatetime": "...Z"}
  https://www.jma.go.jp/bosai/warning/data/r8/map.json      → [ {controlDatetime, reportDatetime,
        publishingOffice, dataTypeCode, warning: {class10Items: [...], class20Items: [...]}}, ... ]
  https://www.jma.go.jp/bosai/common/const/area.json         → 區域名（offices / class10s / class20s）
  🚫 禁用舊 warning/data/warning/（2026-05-28 凍結，HTTP 仍回 200）

map.json 是「全國現況快照」：每個氣象台 × dataTypeCode 一份最新公報（最舊可回到數月前的解除公報）。
  - control_datetime 一律用 map_time.json 的 latestControlDatetime（快照戳）；entry 的 reportDatetime → report_datetime。
  - 有效集合：排除「発表警報・注意報はなし」與「解除」；同一快照內同 (area_code, kind_code) 保留 reportDatetime 最新者。

只記變動（2026-10-11 第二批，取代每輪全量快照約 860–1,000 列）：
  每輪比對「上一輪有效集合 (area_code, kind_code) → status」與本輪：
    新出現 → start；消失 → end（status 取快照中該區該種別的解除原文，沒有就填「解除」）；status 改變 → status_change。
  寫入 live.jma_warning_events  UNIQUE (control_datetime, area_code, kind_code, change_type) DO NOTHING；永久保留。
  public.jma_warnings_current 由 events 重建（每個 (area, kind) 最新事件、排除 end），欄位與舊 view 相同。
  哨兵列不再需要；live.jma_warnings 停寫（舊表留著讓 7 天資料自然清空，另案刪表）。

狀態：LOCAL_DATA_DIR/state/jma_warnings_state.json = {last_control_datetime, active: {"area|kind": {...}}}
  只在 DB 寫入成功後前進（run()）。state 遺失／舊格式（無 active）時，從 DB 的 public.jma_warnings_current
  （唯讀查詢，用 writer 既有連線池）重建上一輪集合；重建失敗就整輪失敗，絕不把全部當 start 重灌。
  DB 寫入失敗時本 collector 不進 writer buffer（storage/supabase_writer.py NO_BUFFER_COLLECTORS），
  否則下一輪重新差分（新 control_datetime）＋ buffer 補寫舊一輪 → 同一變動寫兩次。
冷資料：每輪（control 有變）把整份有效快照放在 result['raw_payload']（只進本地 JSON → 每日 tar.gz 歸檔 S3，
  不進 DB、不進通知 stats），保留細節可回溯。
變更偵測：latestControlDatetime 沒變 → 不抓 map.json，回傳不含 'data' 的 dict（不算失敗、不寫空檔）。
新鮮度：latestControlDatetime 落後 > 6 小時 → print stale 警告。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.global_climate.jma_common import (
    JmaFetchError, age_minutes, fetch, new_jma_session, parse_dt,
)

URL_MAP_TIME = "warning/data/r8/map_time.json"
URL_MAP = "warning/data/r8/map.json"
URL_AREA = "common/const/area.json"

STALE_HOURS = 6
AREA_REFRESH = timedelta(hours=24)

# 不算「有效」的 status（原文）。出現在快照裡時只用來填 end 事件的 status 原文。
EXCLUDED_STATUSES = frozenset({"発表警報・注意報はなし", "解除"})
END_STATUS_FALLBACK = "解除"

CHANGE_START = "start"
CHANGE_STATUS = "status_change"
CHANGE_END = "end"
CHANGE_TYPES = (CHANGE_START, CHANGE_STATUS, CHANGE_END)

# state / 事件列沿用的描述欄（事件列＝這些＋control_datetime/area_code/kind_code/status/change_type/collected_at）
INFO_FIELDS = ("office_code", "office_name", "area_name", "area_level", "kind_name", "report_datetime")

# DB 重建上一輪有效集合（state 遺失時）：438 前後 public.jma_warnings_current 欄位相同，同一句可跨切換使用
REBUILD_SQL = (
    "SELECT area_code, kind_code, status, office_code, office_name, area_name, area_level, "
    "kind_name, report_datetime FROM public.jma_warnings_current"
)

# class10Items / class20Items → area.json 的 key 與 area_level 值
AREA_CLASSES = {
    "class10Items": ("class10s", "class10"),
    "class15Items": ("class15s", "class15"),
    "class20Items": ("class20s", "class20"),
}

# 警報・注意報種別碼 → 名稱（R8 新防災氣象情報體系）。
# 取自 https://www.jma.go.jp/bosai/warning/ 頁面 inline JS 的對照表（變數 h：code → elem/level，
# 名稱由 e[elem][level] 拼接），2026-10-09 擷取。⚠ 同頁另有洪水專用表 w（20/21/22… 與本表撞號），
# 只用於指定河川洪水予報，不適用 class10Items/class20Items，不可合併。
KIND_NAMES = {
    "33": "レベル５大雨特別警報", "43": "レベル４大雨危険警報", "03": "レベル３大雨警報", "10": "レベル２大雨注意報",
    "39": "レベル５土砂災害特別警報", "49": "レベル４土砂災害危険警報", "09": "レベル３土砂災害警報", "29": "レベル２土砂災害注意報",
    "38": "レベル５高潮特別警報", "48": "レベル４高潮危険警報", "08": "レベル３高潮警報", "19": "レベル２高潮注意報",
    "35": "暴風特別警報", "05": "暴風警報", "15": "強風注意報",
    "32": "暴風雪特別警報", "02": "暴風雪警報", "13": "風雪注意報",
    "36": "大雪特別警報", "06": "大雪警報", "12": "大雪注意報",
    "37": "波浪特別警報", "07": "波浪警報", "16": "波浪注意報",
    "14": "雷注意報", "17": "融雪注意報", "20": "濃霧注意報", "21": "乾燥注意報", "22": "なだれ注意報",
    "23": "低温注意報", "24": "霜注意報", "25": "着氷注意報", "26": "着雪注意報",
}


def parse_warning_map(entries: list, area: dict, *, control_datetime: str,
                      collected_at: str, released: Optional[dict] = None) -> tuple[list[dict], set[str]]:
    """r8/map.json → 有效警報列。回傳 (rows, unknown_kind_codes)。

    released（可選，dict）：同一快照中「解除／なし」的 (area_code, kind_code) → {status, report_datetime}
    （取 reportDatetime 最新者），只用來填 end 事件的 status 原文，不影響有效集合。
    """
    area = area or {}
    best: dict[tuple[str, str], dict] = {}
    unknown: set[str] = set()
    rel_sort: dict[tuple[str, str], str] = {}
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        warning = entry.get("warning") or {}
        report_dt = parse_dt(entry.get("reportDatetime"))
        report_iso = report_dt.isoformat() if report_dt else None
        # office_code：entry 是單一氣象台公報，取第一個 class10 區域的 parent
        office_code = None
        for item in warning.get("class10Items") or []:
            parent = ((area.get("class10s") or {}).get(str(item.get("areaCode"))) or {}).get("parent")
            if parent:
                office_code = parent
                break
        for cls_key, items in warning.items():
            if cls_key not in AREA_CLASSES or not isinstance(items, list):
                continue
            area_key, area_level = AREA_CLASSES[cls_key]
            for item in items:
                area_code = str(item.get("areaCode") or "")
                if not area_code:
                    continue
                for kind in item.get("kinds") or []:
                    code = kind.get("code")
                    status = kind.get("status")
                    if not code:
                        continue
                    if status in EXCLUDED_STATUSES:
                        if released is not None:
                            rkey = (area_code, str(code))
                            rsort = report_dt.astimezone(timezone.utc).isoformat() if report_dt else ""
                            if rkey not in rel_sort or rsort > rel_sort[rkey]:
                                rel_sort[rkey] = rsort
                                released[rkey] = {"status": status, "report_datetime": report_iso}
                        # 解除／なし 也參與「同鍵最新公報」比較：較新的解除要蓋掉較舊的有效列
                        xkey = (area_code, str(code))
                        xsort = report_dt.astimezone(timezone.utc).isoformat() if report_dt else ""
                        prev_x = best.get(xkey)
                        if prev_x is None or xsort > prev_x["_report_sort"]:
                            best[xkey] = {"_report_sort": xsort, "_excluded": True}
                        continue
                    code = str(code)
                    if code not in KIND_NAMES:
                        unknown.add(code)
                    row = {
                        "control_datetime": control_datetime,
                        "office_code": office_code,
                        "office_name": entry.get("publishingOffice"),
                        "area_code": area_code,
                        "area_name": ((area.get(area_key) or {}).get(area_code) or {}).get("name"),
                        "area_level": area_level,
                        "kind_code": code,
                        "kind_name": KIND_NAMES.get(code),
                        "status": status,
                        "report_datetime": report_iso,
                        "collected_at": collected_at,
                    }
                    row["_report_sort"] = report_dt.astimezone(timezone.utc).isoformat() if report_dt else ""
                    key = (area_code, code)
                    prev = best.get(key)
                    if prev is None or row["_report_sort"] > prev["_report_sort"]:
                        best[key] = row
    rows = []
    for r in best.values():
        if r.get("_excluded"):
            continue
        r.pop("_report_sort", None)
        rows.append(r)
    rows.sort(key=lambda r: (r["area_code"], r["kind_code"]))
    return rows, unknown


def state_key(area_code: str, kind_code: str) -> str:
    return f"{area_code}|{kind_code}"


def active_from_rows(rows: list[dict]) -> dict[str, dict]:
    """有效列 → state 用的有效集合 {"area|kind": {status, office_code, …, report_datetime}}。"""
    return {state_key(r["area_code"], r["kind_code"]): {"status": r.get("status"),
                                                       **{f: r.get(f) for f in INFO_FIELDS}}
            for r in rows}


def diff_active(prev: dict[str, dict], rows: list[dict], released: dict, *,
                control_datetime: str, collected_at: str) -> list[dict]:
    """上一輪有效集合 prev 與本輪有效列 rows 比對 → 事件列（start／status_change／end）。

    end 列的描述欄沿用上一輪（區域、種別、氣象台），status 取本快照該區該種別的解除原文
    （released），沒有就填「解除」；report_datetime 取解除公報的時間，沒有則 NULL。
    """
    cur = {state_key(r["area_code"], r["kind_code"]): r for r in rows}
    events: list[dict] = []
    for key, r in cur.items():
        before = prev.get(key)
        if before is None:
            change = CHANGE_START
        elif before.get("status") != r.get("status"):
            change = CHANGE_STATUS
        else:
            continue
        events.append({**{k: r.get(k) for k in ("area_code", "kind_code", "status", *INFO_FIELDS)},
                       "control_datetime": control_datetime, "collected_at": collected_at,
                       "change_type": change})
    for key, before in prev.items():
        if key in cur:
            continue
        area_code, kind_code = key.split("|", 1)
        rel = released.get((area_code, kind_code)) or {}
        events.append({
            **{f: before.get(f) for f in INFO_FIELDS},
            "area_code": area_code, "kind_code": kind_code,
            "status": rel.get("status") or END_STATUS_FALLBACK,
            "report_datetime": rel.get("report_datetime"),
            "control_datetime": control_datetime, "collected_at": collected_at,
            "change_type": CHANGE_END,
        })
    events.sort(key=lambda e: (e["area_code"], e["kind_code"], e["change_type"]))
    return events


class JmaWarningsCollector(BaseCollector):
    """R8 警報・注意報：map_time 有變才抓，與上一輪有效集合差分，只寫變動事件。"""

    name = "jma_warnings"
    interval_minutes = config.JAPAN_JMA_WARNINGS_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-warnings")
        self._area: dict = {}
        self._area_loaded_at: Optional[datetime] = None
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "jma_warnings_state.json"
        self._state: Optional[dict] = None
        self._pending_state: Optional[dict] = None

    def require_db_write(self) -> bool:
        # 差分依賴 state；寫入沒成功就不能前進 state，所以 DB 寫入是必要條件
        return True

    def run(self) -> dict:
        self._pending_state = None
        stats = super().run()
        if self._pending_state is not None and not stats.get("error"):
            self._save_state(self._pending_state)
        self._pending_state = None
        return stats

    # ---- 跨重啟狀態 ----
    def _load_state(self) -> Optional[dict]:
        """{last_control_datetime, active}；檔案不存在、壞掉或舊格式（無 active）→ None（＝state 遺失）。"""
        if self._state is None:
            try:
                st = json.loads(self.state_path.read_text())
                if isinstance(st, dict) and isinstance(st.get("active"), dict):
                    self._state = st
            except (OSError, ValueError):
                self._state = None
        return self._state

    def _save_state(self, st: dict) -> None:
        self._state = st
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(st, ensure_ascii=False))
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[{self.name}] ⚠ state 檔寫入失敗（重啟後會從 DB 重建）: {e}")

    def rebuild_active_from_db(self) -> dict[str, dict]:
        """state 遺失時從 DB 的 public.jma_warnings_current 重建上一輪有效集合（唯讀）。失敗丟例外。"""
        writer = getattr(self, "supabase_writer", None)
        if writer is None:
            raise RuntimeError("jma_warnings: state 遺失且無 Supabase writer，無法重建有效集合（不可全部當 start）")
        with writer.with_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(REBUILD_SQL)
                cols = [d[0] for d in cur.description]
                fetched = [dict(zip(cols, row)) for row in cur.fetchall()]
        rows = []
        for r in fetched:
            rd = r.get("report_datetime")
            rows.append({**r, "area_code": str(r["area_code"]), "kind_code": str(r["kind_code"]),
                         "report_datetime": rd.isoformat() if hasattr(rd, "isoformat") else rd})
        return active_from_rows(rows)

    def _area_const(self) -> dict:
        now = datetime.now(timezone.utc)
        if not self._area or self._area_loaded_at is None or now - self._area_loaded_at > AREA_REFRESH:
            try:
                self._area = fetch(self._session, URL_AREA) or {}
                self._area_loaded_at = now
            except JmaFetchError as e:
                if not self._area:
                    print(f"[{self.name}] ⚠ area.json 抓取失敗，area_name/office_code 本輪為 NULL: {e}")
                else:
                    print(f"[{self.name}] ⚠ area.json 更新失敗，沿用舊表: {e}")
        return self._area

    def collect(self) -> dict:
        collected_at = datetime.now(TAIPEI_TZ).isoformat()
        mt = fetch(self._session, URL_MAP_TIME) or {}
        ctrl_dt = parse_dt(mt.get("latestControlDatetime"))
        if ctrl_dt is None:
            raise RuntimeError(f"jma_warnings: map_time.json 無 latestControlDatetime: {str(mt)[:120]}")
        control = ctrl_dt.isoformat()

        lag = age_minutes(ctrl_dt)
        stale = lag is not None and lag > STALE_HOURS * 60
        if stale:
            print(f"[{self.name}] ⚠ stale: latestControlDatetime={control} 已 {lag / 60:.1f} 小時未更新（>{STALE_HOURS}h）")

        st = self._load_state()
        if st is not None and control == st.get("last_control_datetime"):
            print(f"[{self.name}] no change（control_datetime={control}）")
            return {
                "no_change": True,
                "control_datetime": control,
                "stale": stale,
                "collected_at": collected_at,
            }

        rebuilt = st is None
        if rebuilt:
            prev = self.rebuild_active_from_db()  # 失敗直接丟例外：整輪失敗、下輪再試
            print(f"[{self.name}] state 遺失 → 從 public.jma_warnings_current 重建上一輪有效集合 {len(prev)} 筆")
        else:
            prev = st["active"]

        entries = fetch(self._session, URL_MAP)
        if not isinstance(entries, list):
            raise RuntimeError(f"jma_warnings: map.json 非 list（{type(entries).__name__}）")
        released: dict = {}
        rows, unknown = parse_warning_map(entries, self._area_const(), control_datetime=control,
                                          collected_at=collected_at, released=released)
        if unknown:
            print(f"[{self.name}] ⚠ 未知 kind_code（kind_name=NULL，需補 KIND_NAMES）: {sorted(unknown)}")
        events = diff_active(prev, rows, released, control_datetime=control, collected_at=collected_at)
        counts = {c: sum(1 for e in events if e["change_type"] == c) for c in CHANGE_TYPES}

        # state 只在 DB 寫入成功後才前進（見 run()），避免寫入失敗時整輪被當成「已處理」
        self._pending_state = {"last_control_datetime": control, "active": active_from_rows(rows)}
        return {
            "data": events,
            "event_counts": counts,
            "active_count": len(rows),
            "prev_active_count": len(prev),
            "state_rebuilt": rebuilt,
            "entry_count": len(entries),
            "control_datetime": control,
            "unknown_kind_codes": sorted(unknown),
            "stale": stale,
            "collected_at": collected_at,
            # 整份有效快照只進本地 JSON（→ 每日 tar.gz 歸檔 S3），不進 DB／通知 stats
            "raw_payload": {"active_rows": rows},
        }


if __name__ == "__main__":
    import tempfile
    c = JmaWarningsCollector.__new__(JmaWarningsCollector)
    c.name = "jma_warnings"
    c._session = new_jma_session("jma-warnings-test")
    c._area, c._area_loaded_at, c._state, c._pending_state = {}, None, None, None
    c.state_path = Path(tempfile.mkdtemp()) / "state.json"
    c._state = {"last_control_datetime": None, "active": {}}
    out = c.collect()
    print({k: v for k, v in out.items() if k not in ("data", "raw_payload")})
