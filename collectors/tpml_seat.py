"""
臺北市立圖書館座位即時狀態收集器

資料來源：北市圖座位管理系統（非正式 open API，data.taipei 標示「即時(10分鐘/1分鐘)」）
  端點：GET https://seat.tpml.edu.tw/sm/service/getAllArea
  認證：無（免金鑰、免 header）
  ⚠ TWCA 憑證缺 Subject Key Identifier：curl 可過但 Python TLS 驗證失敗
    （2026-07-16 實測 SSLCertVerificationError: Missing Subject Key Identifier），
    同 NHI / wic.gov.taipei 前例，需 verify=False。
  ⚠ F5 WAF 會擋 `Accept: application/json` header（2026-07-16 實測：帶此 header
    一律回 200 + HTML「Request Rejected」，與 UA 無關；不帶則正常回 JSON）。
    禁止在本 collector 加 Accept header。
  回應：JSON array（2026-07 實測 29 區 / 6 分館 / 1,167 席），每筆 6 欄：
    areaId(int) / branchName / floorName / areaName / freeCount(int) / totalCount(int)
  無 timestamp、無座標 → observed_at = 收集當下（同 er_hospital sysdate fallback 慣例）

業務規則：
  - 單輪 snapshot 全部區域 freeCount == 0 → 該輪所有 record is_closed = true
    （閉館偵測：全區同時 0 幾乎不可能是真滿座；前端應顯示「休館中」而非「0 空位」）
  - 不 hardcode 分館清單，分館增減自動跟 API 內容走

寫入：
  - live.tpml_seat_status   (時序，UNIQUE(area_id, observed_at)，ON CONFLICT DO NOTHING)
  - live.tpml_seat_current  (最新狀態，UPSERT by area_id)

Standalone usage（dry-run，不寫 DB）：
  cd data-collectors
  python3 -m collectors.tpml_seat --dry-run
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from typing import Optional

import requests
import urllib3

import config
from collectors.base import BaseCollector, TAIPEI_TZ

# TWCA 憑證缺 SKI，verify=False 後關閉警告噪音（同 er_hospital / wic 慣例）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ENDPOINT = "https://seat.tpml.edu.tw/sm/service/getAllArea"


def _int(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


class TpmlSeatCollector(BaseCollector):
    """北市圖座位即時狀態收集器（每 10 分鐘，對齊 data.taipei 官方標示頻率）"""

    name = "tpml_seat"
    interval_minutes = config.TPML_SEAT_INTERVAL
    COLLECT_TIMEOUT: int = 30

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.verify = False  # TWCA 憑證缺 SKI
        self._session.headers.update({
            # 不可加 Accept: application/json — F5 WAF 會擋（見檔頭 docstring）
            "User-Agent": "GIS-DataCollectors/1.0 (tpml_seat)",
        })

    def _fetch(self) -> list[dict]:
        resp = self._session.get(ENDPOINT, timeout=20)
        resp.raise_for_status()
        j = resp.json()
        if not isinstance(j, list):
            raise ValueError(f"預期 JSON array，收到 {type(j).__name__}")
        return j

    def _normalize(self, r: dict, observed_at: datetime,
                   collected_at: datetime) -> Optional[dict]:
        """單筆區域 → record；areaId 缺失/非數字（upsert key）→ 跳過該筆。
        is_closed 先給 False，由 collect() 依「全區 0」規則回填。"""
        if not isinstance(r, dict):
            return None
        area_id = _int(r.get("areaId"))
        if area_id is None:
            return None
        return {
            "area_id":      area_id,
            "branch_name":  r.get("branchName"),
            "floor_name":   r.get("floorName"),
            "area_name":    r.get("areaName"),
            "free_count":   _int(r.get("freeCount")),
            "total_count":  _int(r.get("totalCount")),
            "is_closed":    False,
            "observed_at":  observed_at.isoformat(),
            "collected_at": collected_at.isoformat(),
        }

    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        try:
            raw = self._fetch()
        except Exception as e:
            print(f"[{self.name}] 擷取失敗：{e}")
            return {
                "data": [],
                "area_count": 0,
                "branch_count": 0,
                "error": str(e)[:200],
                "collected_at": now.isoformat(),
            }

        records = [n for n in (self._normalize(r, now, now) for r in raw) if n]

        # 閉館偵測：該輪全部區域 freeCount == 0 → is_closed = true
        # （free_count 為 None 的異常筆不算 0，保守判定為未閉館）
        is_closed = bool(records) and all(r["free_count"] == 0 for r in records)
        if is_closed:
            for r in records:
                r["is_closed"] = True

        branches = {r["branch_name"] for r in records if r["branch_name"]}
        free_total = sum(r["free_count"] for r in records if r["free_count"] is not None)
        return {
            "data":         records,
            "area_count":   len(records),
            "branch_count": len(branches),
            "free_total":   free_total,
            "is_closed":    is_closed,
            "collected_at": now.isoformat(),
        }


def _dry_run() -> int:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    print("=" * 60)
    print("TpmlSeat Collector — DRY RUN（不寫 DB）")
    print("=" * 60)
    coll = TpmlSeatCollector()
    coll.supabase_writer = None

    t0 = time.time()
    print(f"\n[1/2] collect() 單輪（走正式路徑，不寫 DB）…")
    result = coll.collect()
    if result.get("error"):
        print(f"      ❌ {result['error']}")
        return 1
    print(f"      ✅ areas={result['area_count']}  branches={result['branch_count']}"
          f"  is_closed={result['is_closed']}  free_total={result['free_total']}")

    print(f"\n[2/2] Sample first record:")
    if result["data"]:
        import json
        print(json.dumps(result["data"][0], ensure_ascii=False, indent=2))

    print(f"\n[done] 耗時 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        sys.exit(_dry_run())
    print("TpmlSeat Collector module. Use --dry-run to test fetch+parse without DB write.")
    sys.exit(0)
