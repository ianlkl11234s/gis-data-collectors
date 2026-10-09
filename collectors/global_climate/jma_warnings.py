"""日本氣象廳 警報・注意報（R8 新防災氣象情報）收集器

資料來源：JMA bosai（免認證、非公式 JSON）
  https://www.jma.go.jp/bosai/warning/data/r8/map_time.json → {"latestControlDatetime": "...Z"}
  https://www.jma.go.jp/bosai/warning/data/r8/map.json      → [ {controlDatetime, reportDatetime,
        publishingOffice, dataTypeCode, warning: {class10Items: [...], class20Items: [...]}}, ... ]
  https://www.jma.go.jp/bosai/common/const/area.json         → 區域名（offices / class10s / class20s）
  🚫 禁用舊 warning/data/warning/（2026-05-28 凍結，HTTP 仍回 200）

map.json 是「全國現況快照」：每個氣象台 × dataTypeCode 一份最新公報（最舊可回到數月前的解除公報）。
因此：
  - control_datetime 一律用 map_time.json 的 latestControlDatetime（快照戳），
    不用各 entry 自己的 controlDatetime；否則 public.jma_warnings_current
    （control_datetime = max）只剩最後更新的那個氣象台。entry 的 reportDatetime → report_datetime。
  - 只寫有效（ACTIVE）列：排除「発表警報・注意報はなし」與「解除」（解除公報會在快照裡掛數月，
    寫進去會淹沒現況 view 且 30 天量爆增；實測 2026-10-09 一份快照 3,639 列帶 code，其中 3,019 列是解除）。
  - 整份快照沒有任何有效警報 → 寫一列哨兵 area_code/kind_code='__none__'、status='none'，
    讓 view 仍能判定「最新一輪」（view 端排除哨兵列）。
  - 同一快照內同 (area_code, kind_code) 若出現在多個 entry，保留 reportDatetime 最新者。

變更偵測：latestControlDatetime 沒變 → 不抓 map.json，回傳不含 'data' 的 dict（不算失敗、不寫空檔）。
  跨 process 重啟：最後處理過的 control_datetime 存在 LOCAL_DATA_DIR/state/jma_warnings_state.json
  （Zeabur Volume /data）；就算 state 遺失，重抓同一快照也只會被 UNIQUE DO NOTHING 吃掉。
新鮮度：latestControlDatetime 落後 > 6 小時 → print stale 警告。

寫入：live.jma_warnings  UNIQUE (control_datetime, area_code, kind_code) DO NOTHING；保留 30 天
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

# 不寫入的 status（原文）。要改成連「解除」也存，把 '解除' 拿掉即可。
EXCLUDED_STATUSES = frozenset({"発表警報・注意報はなし", "解除"})

SENTINEL_CODE = "__none__"
SENTINEL_STATUS = "none"

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
                      collected_at: str) -> tuple[list[dict], set[str]]:
    """r8/map.json → 有效警報列。回傳 (rows, unknown_kind_codes)。

    rows 為空時由呼叫端決定是否補哨兵（見 sentinel_row）。
    """
    area = area or {}
    best: dict[tuple[str, str], dict] = {}
    unknown: set[str] = set()
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
                    if not code or status in EXCLUDED_STATUSES:
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
        r.pop("_report_sort", None)
        rows.append(r)
    rows.sort(key=lambda r: (r["area_code"], r["kind_code"]))
    return rows, unknown


def sentinel_row(control_datetime: str, collected_at: str) -> dict:
    """全國無有效警報時的哨兵列（view 排除，但用來判定最新一輪）。"""
    return {
        "control_datetime": control_datetime,
        "office_code": None, "office_name": None,
        "area_code": SENTINEL_CODE, "area_name": None, "area_level": None,
        "kind_code": SENTINEL_CODE, "kind_name": None,
        "status": SENTINEL_STATUS, "report_datetime": None,
        "collected_at": collected_at,
    }


class JmaWarningsCollector(BaseCollector):
    """R8 警報・注意報全國快照（map_time 有變才抓）。"""

    name = "jma_warnings"
    interval_minutes = config.JAPAN_JMA_WARNINGS_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = new_jma_session("jma-warnings")
        self._area: dict = {}
        self._area_loaded_at: Optional[datetime] = None
        self.state_path = Path(config.LOCAL_DATA_DIR) / "state" / "jma_warnings_state.json"
        self._last_control: Optional[str] = None
        self._pending_control: Optional[str] = None

    def require_db_write(self) -> bool:
        # 「沒變就跳過」依賴 state；寫入沒成功就不能前進 state，所以 DB 寫入是必要條件
        return True

    def run(self) -> dict:
        self._pending_control = None
        stats = super().run()
        if self._pending_control and not stats.get("error"):
            self._save_last_control(self._pending_control)
        self._pending_control = None
        return stats

    # ---- 跨重啟狀態 ----
    def _load_last_control(self) -> Optional[str]:
        if self._last_control is None:
            try:
                self._last_control = json.loads(self.state_path.read_text()).get("last_control_datetime")
            except (OSError, ValueError, AttributeError):
                self._last_control = None
        return self._last_control

    def _save_last_control(self, value: str) -> None:
        self._last_control = value
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"last_control_datetime": value}))
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[{self.name}] ⚠ state 檔寫入失敗（僅影響重啟後多抓一次）: {e}")

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

        if control == self._load_last_control():
            print(f"[{self.name}] no change（control_datetime={control}）")
            return {
                "no_change": True,
                "control_datetime": control,
                "stale": stale,
                "collected_at": collected_at,
            }

        entries = fetch(self._session, URL_MAP)
        if not isinstance(entries, list):
            raise RuntimeError(f"jma_warnings: map.json 非 list（{type(entries).__name__}）")
        rows, unknown = parse_warning_map(entries, self._area_const(),
                                          control_datetime=control, collected_at=collected_at)
        if unknown:
            print(f"[{self.name}] ⚠ 未知 kind_code（kind_name=NULL，需補 KIND_NAMES）: {sorted(unknown)}")
        active = len(rows)
        if not rows:
            rows = [sentinel_row(control, collected_at)]

        # state 只在 DB 寫入成功後才前進（見 run()），避免寫入失敗時整輪被當成「已處理」
        self._pending_control = control
        return {
            "data": rows,
            "active_count": active,
            "entry_count": len(entries),
            "control_datetime": control,
            "unknown_kind_codes": sorted(unknown),
            "stale": stale,
            "collected_at": collected_at,
        }


if __name__ == "__main__":
    import tempfile
    c = JmaWarningsCollector.__new__(JmaWarningsCollector)
    c.name = "jma_warnings"
    c._session = new_jma_session("jma-warnings-test")
    c._area, c._area_loaded_at, c._last_control = {}, None, None
    c.state_path = Path(tempfile.mkdtemp()) / "state.json"
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
    print(out.get("data", [None])[0])
