"""
放流水 水量水質自動監測連線傳輸（CWMS）即時收集器

資料來源：環境部 data.moenv.gov.tw API v2（需 MOENV_API_KEY）
  22 縣市各一 dataset：datagov 35105–35125 → wqx_p_47..wqx_p_67；嘉義市 147031 → wqx_p_126
  每個 dataset 是滾動視窗（約最近 1–2 小時的 5 分鐘紀錄值）→ 60 分 cron 抓全窗、DB 去重。

上游陷阱（2026-10-02 實測）：
  - m_date 民國 'YYYMMDD'；m_time 'HH:MM' 或 'HHMM'
  - wgs84x/wgs84y 對調（x 為緯度），且約 45% 設施空白 → 依數值範圍判定，不硬寫對調
  - 超標以 status='超限值' 為準；std1/std2 語意混雜只存原值
  - 同 (cno, dp_no, desp, 時間) 偶有重複列 → 批內去重
  - 部分縣市 dataset 停更（臺中 wqx_p_54 停在 2024-12）→ 照寫，由 observed_at/RPC 判 stale
  - 金門/連江等列數本來就少；成功判定看全國總列數，不看單一縣市

寫入：
  - live.water_effluent_readings  UNIQUE(reading_key, observed_at) DO NOTHING
  - live.water_effluent_current   PK=reading_key UPSERT
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.moenv_common import fetch_pages, new_session, MoenvFetchError

# dataset code → 縣市
EFFLUENT_DATASETS: dict[str, str] = {
    "wqx_p_47": "基隆市", "wqx_p_48": "臺北市", "wqx_p_49": "新北市", "wqx_p_50": "桃園市",
    "wqx_p_51": "新竹縣", "wqx_p_52": "新竹市", "wqx_p_53": "苗栗縣", "wqx_p_54": "臺中市",
    "wqx_p_55": "彰化縣", "wqx_p_56": "南投縣", "wqx_p_57": "雲林縣", "wqx_p_58": "嘉義縣",
    "wqx_p_59": "臺南市", "wqx_p_60": "高雄市", "wqx_p_61": "屏東縣", "wqx_p_62": "宜蘭縣",
    "wqx_p_63": "花蓮縣", "wqx_p_64": "臺東縣", "wqx_p_65": "澎湖縣", "wqx_p_66": "金門縣",
    "wqx_p_67": "連江縣", "wqx_p_126": "嘉義市",
}
STALE_THRESHOLD = timedelta(hours=3)
MAX_PAGES_PER_DATASET = 5   # 實測最大 2,400 列（彰化）


def _num(v) -> Optional[float]:
    s = str(v or "").strip()
    if s in ("", "-", "--", "NA", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_roc_datetime(m_date: str | None, m_time: str | None) -> Optional[datetime]:
    d = (m_date or "").strip()
    t = (m_time or "").strip().replace(":", "")
    if len(d) != 7 or not d.isdigit() or len(t) not in (3, 4) or not t.isdigit():
        return None
    t = t.zfill(4)
    try:
        return datetime(int(d[:3]) + 1911, int(d[3:5]), int(d[5:7]),
                        int(t[:2]), int(t[2:]), tzinfo=TAIPEI_TZ)
    except ValueError:
        return None


def resolve_lonlat(x, y) -> tuple[Optional[float], Optional[float]]:
    """上游 wgs84x/wgs84y 有對調情形 → 依範圍判定經緯度；判不出就回 None（不合成）。"""
    a, b = _num(x), _num(y)
    if a is None or b is None:
        return None, None
    def is_lat(v): return 20.0 <= v <= 27.0
    def is_lon(v): return 116.0 <= v <= 123.5
    if is_lon(a) and is_lat(b):
        return a, b
    if is_lat(a) and is_lon(b):
        return b, a
    return None, None


def parse_effluent_records(records: list[dict], code: str, county: str,
                           collected_at: datetime) -> list[dict]:
    out: list[dict] = []
    for r in records:
        cno = (r.get("cno") or "").strip()
        obs = parse_roc_datetime(r.get("m_date"), r.get("m_time"))
        if not cno or obs is None:
            continue
        outlet = (r.get("dp_no") or "").strip()
        item = (r.get("desp") or "").strip()
        status = (r.get("status") or "").strip() or None
        lon, lat = resolve_lonlat(r.get("wgs84x"), r.get("wgs84y"))
        out.append({
            "reading_key":    f"{cno}|{outlet}|{item}",
            "cno":            cno,
            "facility_name":  (r.get("abbr") or "").strip() or None,
            "county":         county,
            "outlet_no":      outlet or None,
            "item_name":      item or None,
            "value":          _num(r.get("m_val")),
            "unit":           (r.get("unit") or "").strip() or None,
            "status":         status,
            "is_exceed":      status == "超限值" if status else None,
            "std1":           _num(r.get("std1")),
            "std2":           _num(r.get("std2")),
            "std_note":       (r.get("std_s") or "").strip() or None,
            "observed_at":    obs.isoformat(),
            "lon":            lon,
            "lat":            lat,
            "source_dataset": code,
            "collected_at":   collected_at.isoformat(),
        })
    return out


def dedup_sort(rows: list[dict]) -> list[dict]:
    """批內 (reading_key, observed_at) 去重，依 observed_at 升冪（current upsert 保留最新）。"""
    seen: dict[tuple, dict] = {}
    for r in rows:
        seen[(r["reading_key"], r["observed_at"])] = r
    return sorted(seen.values(), key=lambda r: r["observed_at"])


class WaterEffluentMonitoringCollector(BaseCollector):
    """放流水連線自動監測（22 縣市，60 分鐘 cron）"""

    name = "water_effluent_monitoring"
    interval_minutes = config.WATER_EFFLUENT_MONITORING_INTERVAL
    COLLECT_TIMEOUT = 600

    def __init__(self):
        super().__init__()
        self._session = new_session("water-effluent")

    def require_db_write(self) -> bool:
        return True

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        rows: list[dict] = []
        per_county: dict[str, int] = {}
        failures: dict[str, str] = {}
        for code, county in EFFLUENT_DATASETS.items():
            try:
                recs = fetch_pages(self._session, code, config.MOENV_API_KEY,
                                   max_pages=MAX_PAGES_PER_DATASET,
                                   timeout=config.REQUEST_TIMEOUT * 2)
            except MoenvFetchError as e:
                failures[code] = str(e)
                continue
            parsed = parse_effluent_records(recs, code, county, now)
            per_county[county] = len(parsed)
            rows.extend(parsed)

        rows = dedup_sort(rows)
        if not rows:
            raise RuntimeError(
                f"water effluent: 0 rows from {len(EFFLUENT_DATASETS)} datasets; failures={failures}")

        latest_by_county: dict[str, str] = {}
        for r in rows:
            c = r["county"]
            if r["observed_at"] > latest_by_county.get(c, ""):
                latest_by_county[c] = r["observed_at"]
        stale_counties = sorted(
            c for c, ts in latest_by_county.items()
            if now - datetime.fromisoformat(ts) > STALE_THRESHOLD
        )
        result = {
            "data":             rows,
            "row_count":        len(rows),
            "facility_count":   len({r["cno"] for r in rows}),
            "exceed_count":     sum(1 for r in rows if r["is_exceed"]),
            "no_coord_facilities": len({r["cno"] for r in rows} - {r["cno"] for r in rows if r["lon"] is not None}),
            "per_county":       per_county,
            "stale_counties":   stale_counties,
            "dataset_failures": failures,
            "collected_at":     now.isoformat(),
        }
        # 部分 dataset 失敗不擋寫入，但超過一半失敗視為異常
        if len(failures) > len(EFFLUENT_DATASETS) // 2:
            result["_collector_error"] = f"water effluent: {len(failures)} datasets failed"
        return result


if __name__ == "__main__":
    c = WaterEffluentMonitoringCollector.__new__(WaterEffluentMonitoringCollector)
    c._session = new_session("water-effluent-test")
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
    print("sample:", out["data"][-1])
