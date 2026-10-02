"""
固定污染源 CEMS 連續自動監測收集器（煙道 1 小時值＋廢氣燃燒塔 1 小時值）

資料來源：環境部 data.moenv.gov.tw API v2（需 MOENV_API_KEY）
  - stack_1h：datagov 31970 → aqx_p_187（約 100 家、~1,680 列/時）
  - flare_1h：datagov 151827 → aqx_p_493（約 40 家、~820 列/時）
  不收 aqx_p_186（15 分，與 1h 重複）、aqx_p_188（前日全測項，實測樣本停在 2025-11）。

特性：
  - 上游預設排序 m_time desc；每輪回看 LOOKBACK（6h）即停止翻頁
  - 上游延遲約 4–5 小時 → stale 門檻 6 小時
  - 無座標，只有 cno（管制編號）；座標由 RPC 對 spatial.pollution_regulated_facilities 補
    （2026-10-02 對點 117/128 = 91.4%）
  - 逾限：code2desc 含「數值逾限」（不含「未逾限」）；std '' / -1.00 = 無標準 → NULL

寫入：
  - live.cems_stack_readings  UNIQUE(reading_key, observed_at) DO NOTHING
  - live.cems_stack_current   PK=reading_key UPSERT
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import config
from collectors.base import BaseCollector, TAIPEI_TZ
from collectors.moenv_common import fetch_pages, new_session, MoenvFetchError

CEMS_SOURCES: dict[str, dict] = {
    "stack_1h": {"code": "aqx_p_187", "point_field": "polno", "max_pages": 15},
    "flare_1h": {"code": "aqx_p_493", "point_field": "flareno", "max_pages": 8},
}
LOOKBACK = timedelta(hours=6)
STALE_THRESHOLD = timedelta(hours=6)


def _num(v) -> Optional[float]:
    s = str(v or "").strip()
    if s in ("", "-", "--", "NA", "N/A"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_m_time(s: str | None) -> Optional[datetime]:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=TAIPEI_TZ)
        except ValueError:
            continue
    return None


def is_exceed(code2_desc: str | None) -> Optional[bool]:
    d = (code2_desc or "").strip()
    if not d:
        return None
    return ("數值逾限" in d) and ("未逾限" not in d)


def std_value(v) -> Optional[float]:
    n = _num(v)
    if n is None or n < 0:   # -1.00 = 無適用標準
        return None
    return n


def parse_cems_records(records: list[dict], source: str, point_field: str,
                       cutoff: datetime, collected_at: datetime) -> list[dict]:
    out: list[dict] = []
    for r in records:
        cno = (r.get("cno") or "").strip()
        obs = parse_m_time(r.get("m_time"))
        if not cno or obs is None or obs < cutoff:
            continue
        point = (r.get(point_field) or "").strip()
        item_code = (r.get("item") or "").strip()
        desc = (r.get("code2desc") or "").strip() or None
        out.append({
            "reading_key":   f"{source}|{cno}|{point}|{item_code}",
            "source":        source,
            "cno":           cno,
            "facility_name": (r.get("abbr") or "").strip() or None,
            "county":        (r.get("epb_county") or "").strip() or None,
            "point_no":      point or None,
            "item_code":     item_code or None,
            "item_name":     (r.get("itemdesc") or "").strip() or None,
            "value":         _num(r.get("m_value")),
            "unit":          (r.get("unit") or "").strip() or None,
            "std_value":     std_value(r.get("std")),
            "std_law":       ((r.get("std_s") or "").strip() or None) if (r.get("std_s") or "").strip() != "無" else None,
            "code2":         (r.get("code2") or "").strip() or None,
            "code2_desc":    desc,
            "is_exceed":     is_exceed(desc),
            "observed_at":   obs.isoformat(),
            "collected_at":  collected_at.isoformat(),
        })
    return out


class CemsStackMonitoringCollector(BaseCollector):
    """CEMS 煙道＋燃燒塔 1 小時值（60 分鐘 cron）"""

    name = "cems_stack_monitoring"
    interval_minutes = config.CEMS_STACK_MONITORING_INTERVAL
    COLLECT_TIMEOUT = 600

    def __init__(self):
        super().__init__()
        self._session = new_session("cems-stack")

    def require_db_write(self) -> bool:
        return True

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        rows: list[dict] = []
        per_source: dict[str, int] = {}
        latest: dict[str, Optional[str]] = {}
        failures: dict[str, str] = {}

        for source, spec in CEMS_SOURCES.items():
            # 以本 dataset 最新 m_time 為錨點回看，避免上游延遲時整窗落空
            anchor: dict[str, Optional[datetime]] = {"t": None}

            def _stop(page: list[dict], _a=anchor) -> bool:
                times = [t for t in (parse_m_time(x.get("m_time")) for x in page) if t]
                if not times:
                    return True
                if _a["t"] is None:
                    _a["t"] = max(times)
                return min(times) < _a["t"] - LOOKBACK

            try:
                recs = fetch_pages(self._session, spec["code"], config.MOENV_API_KEY,
                                   max_pages=spec["max_pages"],
                                   timeout=config.REQUEST_TIMEOUT * 3,
                                   stop_when=_stop)
            except MoenvFetchError as e:
                failures[source] = str(e)
                continue
            times = [t for t in (parse_m_time(x.get("m_time")) for x in recs) if t]
            if not times:
                per_source[source] = 0
                continue
            newest = max(times)
            parsed = parse_cems_records(recs, source, spec["point_field"],
                                        newest - LOOKBACK, now)
            per_source[source] = len(parsed)
            latest[source] = newest.isoformat()
            rows.extend(parsed)

        # 批內去重＋升冪
        dedup: dict[tuple, dict] = {(r["reading_key"], r["observed_at"]): r for r in rows}
        rows = sorted(dedup.values(), key=lambda r: r["observed_at"])
        if not rows:
            raise RuntimeError(f"cems: 0 rows; per_source={per_source}; failures={failures}")

        stale_sources = sorted(
            s for s, ts in latest.items()
            if ts and now - datetime.fromisoformat(ts) > STALE_THRESHOLD
        )
        result = {
            "data":           rows,
            "row_count":      len(rows),
            "facility_count": len({r["cno"] for r in rows}),
            "exceed_count":   sum(1 for r in rows if r["is_exceed"]),
            "per_source":     per_source,
            "latest_m_time":  latest,
            "stale_sources":  stale_sources,
            "source_failures": failures,
            "collected_at":   now.isoformat(),
        }
        if failures or any(per_source.get(s, 0) == 0 for s in CEMS_SOURCES):
            result["_collector_error"] = f"cems: source empty/failed per_source={per_source} failures={failures}"
        return result


if __name__ == "__main__":
    c = CemsStackMonitoringCollector.__new__(CemsStackMonitoringCollector)
    c._session = new_session("cems-stack-test")
    out = c.collect()
    print({k: v for k, v in out.items() if k != "data"})
    print("sample:", out["data"][-1])
