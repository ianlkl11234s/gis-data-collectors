"""
食品價格每日收集器（菜 / 魚 / 肉 / 蛋批發拍賣成交價）

資料來源：農業部 data.moa.gov.tw（全部免金鑰、OGDL-Taiwan-1.0、每日 T+1 更新）

🔴 四個來源的分頁與日期參數規則**完全不同**，不可共用 helper：
  蔬果  /api/v1/AgriProductsTransType/   Start_time/End_time（民國點分）；上限 1,000 筆，
                                         Page 參數無效（Next=true 是假訊號）→ 走市場 × 類別迴圈
  漁產  FromM/AquaticTransData.aspx      StartDate/EndDate（民國七碼）；上限 9,999 筆
  毛豬  FromM/AnimalTransData.aspx       日期參數一律無效 → 取 $skip=0 首頁後自行篩日期
  家禽  FromM/PoultryTrans*.aspx         整檔即全歷史（2010 起）→ 下載後篩日期

  誤用其他來源的日期參數**不會報錯**，會靜默回最近資料 —— 見
  taipei-gis-analytics/docs/api-platforms/moa/gotchas.md P5

寫入：
  - live.food_price_daily  UNIQUE(source, trade_date, category, item_name, market_name) DO NOTHING

歷史回補與指數建構在 taipei-gis-analytics/pipelines/food_prices/wholesale_prices/，
本 collector 只負責每日增量。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

import requests

import config
from collectors.base import BaseCollector, TAIPEI_TZ

BASE = "https://data.moa.gov.tw"

# 蔬果：有效篩選維度只有 MarketName + TcType（Market_No 無效）
PRODUCE_MARKETS = [
    "三重區", "南投市", "台中市", "台中市場", "台北一", "台北二", "台北市場",
    "台南市場", "台東市", "嘉義市", "宜蘭市", "屏東市", "彰化市場", "東勢鎮",
    "板橋區", "桃農", "永靖鄉", "溪湖鎮", "花蓮市", "西螺鎮", "豐原區",
    "高雄市", "高雄市場", "鳳山區",
]
# N04 蔬菜 / N05 水果；N06 花卉為觀賞園藝非民生食品，不收
TC_TYPE = {"N04": "vegetable", "N05": "fruit"}

POULTRY_FEEDS = [
    ("PoultryTransBoiledChickenData.aspx", "056"),
    ("PoultryTransGooseDuckData.aspx", "058"),
]
# 家禽為寬表，欄名即品項；「農曆」欄不是價格
POULTRY_ITEMS = {
    "白肉雞(2.0Kg以上)": ("chicken", "元/台斤"),
    "白肉雞(1.75-1.95Kg)": ("chicken", "元/台斤"),
    "白肉雞(門市價高屏)": ("chicken", "元/台斤"),
    "雞蛋(產地價)": ("egg", "元/台斤"),
    "雞蛋(大運輸價)": ("egg", "元/台斤"),
    "肉鵝(白羅曼)": ("goose", "元/台斤"),
    "正番鴨(公)": ("duck", "元/台斤"),
    "土番鴨(75天)": ("duck", "元/台斤"),
    "鴨蛋(新蛋)(台南)": ("egg", "元/台斤"),
}
# 毛豬為 37 欄寬表，只取兩個代表性 item
HOG_ITEMS = {
    "毛豬-全體": ("成交頭數-平均價格", "成交頭數-總數"),
    "毛豬-規格豬": ("規格豬-平均價格", "規格豬-頭數"),
}

# 數值欄位會混入的 sentinel —— 一律視為無觀測，**絕不可當 0**
NULL_TOKENS = {"", "休市", "None", "null", "-", "－"}


def _num(v) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in NULL_TOKENS:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _roc_dot(d: date) -> str:
    """民國點分 115.08.03（蔬果 api/v1 用）"""
    return f"{d.year - 1911:03d}.{d.month:02d}.{d.day:02d}"


def _roc_compact(d: date) -> str:
    """民國七碼 1150803（漁產 aspx 用）"""
    return f"{d.year - 1911:03d}{d.month:02d}{d.day:02d}"


def _parse_roc(v) -> Optional[date]:
    """民國點分或七碼 → date（去掉非數字後取 7 碼）"""
    if not v:
        return None
    s = "".join(ch for ch in str(v) if ch.isdigit())
    if len(s) != 7:
        return None
    try:
        return date(int(s[:3]) + 1911, int(s[3:5]), int(s[5:7]))
    except ValueError:
        return None


def _parse_slash(v) -> Optional[date]:
    """西元斜線 2026/07/30 → date（家禽用）"""
    try:
        return datetime.strptime(str(v).strip(), "%Y/%m/%d").date()
    except (ValueError, TypeError):
        return None


class FoodPricesCollector(BaseCollector):
    """農業部四類批發價每日增量（T+1 更新，每日跑一次即可）"""

    name = "food_prices"
    interval_minutes = config.FOOD_PRICES_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (food-prices)",
            "Accept": "application/json",
        })

    # ---------------------------------------------------------------- helpers
    def _get(self, path: str, params: dict) -> list:
        """回 list；非 JSON（平台偶爾回 HTML 錯誤頁）視為空"""
        try:
            r = self._session.get(f"{BASE}{path}", params=params,
                                  timeout=config.REQUEST_TIMEOUT)
            r.raise_for_status()
            if "json" not in r.headers.get("content-type", "").lower():
                return []
            d = r.json()
        except Exception as e:
            self.logger.warning(f"[food_prices] {path} 取得失敗: {type(e).__name__}: {e}")
            return []
        if isinstance(d, dict):
            return d.get("Data") or []
        return d if isinstance(d, list) else []

    # ---------------------------------------------------------------- sources
    def _produce(self, lo: date, hi: date) -> list:
        rows = []
        for market in PRODUCE_MARKETS:
            for tc, cat in TC_TYPE.items():
                for r in self._get("/api/v1/AgriProductsTransType/", {
                    "Start_time": _roc_dot(lo), "End_time": _roc_dot(hi),
                    "MarketName": market, "TcType": tc,
                }):
                    d = _parse_roc(r.get("TransDate"))
                    p = _num(r.get("Avg_Price"))
                    if not d or not p or p <= 0:
                        continue
                    rows.append({
                        "trade_date": d, "category": cat,
                        "item_code": r.get("CropCode"), "item_name": r.get("CropName"),
                        "market_name": r.get("MarketName"), "price_avg": p,
                        "price_high": _num(r.get("Upper_Price")),
                        "price_mid": _num(r.get("Middle_Price")),
                        "price_low": _num(r.get("Lower_Price")),
                        "quantity": _num(r.get("Trans_Quantity")),
                        "unit": "元/公斤", "source": "moa:037",
                    })
        return rows

    def _aquatic(self, lo: date, hi: date) -> list:
        rows = []
        for r in self._get("/Service/OpenData/FromM/AquaticTransData.aspx", {
            "IsTransData": 1, "UnitId": "039",
            "StartDate": _roc_compact(lo), "EndDate": _roc_compact(hi),
        }):
            d = _parse_roc(r.get("交易日期"))
            p = _num(r.get("平均價"))
            if not d or not p or p <= 0:
                continue
            rows.append({
                "trade_date": d, "category": "aquatic",
                "item_code": str(r.get("品種代碼")) if r.get("品種代碼") is not None else None,
                "item_name": r.get("魚貨名稱"), "market_name": r.get("市場名稱"),
                "price_avg": p, "price_high": _num(r.get("上價")),
                "price_mid": _num(r.get("中價")), "price_low": _num(r.get("下價")),
                "quantity": _num(r.get("交易量")),
                "unit": "元/公斤", "source": "moa:039",
            })
        return rows

    def _hog(self, lo: date, hi: date) -> list:
        """日期參數無效 → 取首頁（9,999 筆，排序新→舊）後自行篩日期"""
        rows = []
        for r in self._get("/Service/OpenData/FromM/AnimalTransData.aspx",
                           {"IsTransData": 1, "UnitId": "026"}):
            d = _parse_roc(r.get("交易日期"))
            if not d or not (lo <= d <= hi):
                continue
            for item, (pcol, qcol) in HOG_ITEMS.items():
                p = _num(r.get(pcol))
                if not p or p <= 0:
                    continue
                rows.append({
                    "trade_date": d, "category": "hog", "item_code": None,
                    "item_name": item, "market_name": r.get("市場名稱"),
                    "price_avg": p, "price_high": None, "price_mid": None,
                    "price_low": None, "quantity": _num(r.get(qcol)),
                    "unit": "元/公斤", "source": "moa:026",
                })
        return rows

    def _poultry(self, lo: date, hi: date) -> list:
        """整檔即全歷史 → 下載後篩日期；寬表 melt 成長格式"""
        rows = []
        for endpoint, unit in POULTRY_FEEDS:
            for r in self._get(f"/Service/OpenData/FromM/{endpoint}",
                               {"IsTransData": 1, "UnitId": unit}):
                d = _parse_slash(r.get("日期"))
                if not d or not (lo <= d <= hi):
                    continue
                for col, (cat, unit_txt) in POULTRY_ITEMS.items():
                    if col not in r:
                        continue
                    p = _num(r.get(col))
                    if not p or p <= 0:
                        continue
                    rows.append({
                        "trade_date": d, "category": cat, "item_code": None,
                        "item_name": col, "market_name": "全國",
                        "price_avg": p, "price_high": None, "price_mid": None,
                        "price_low": None, "quantity": None,
                        "unit": unit_txt, "source": f"moa:{unit}",
                    })
        return rows

    # ---------------------------------------------------------------- collect
    def collect(self) -> dict:
        now = datetime.now(tz=TAIPEI_TZ)
        hi = now.date()
        lo = hi - timedelta(days=config.FOOD_PRICES_LOOKBACK_DAYS)

        rows, per_source, failed = [], {}, []
        for key, fn in (("produce", self._produce), ("aquatic", self._aquatic),
                        ("hog", self._hog), ("poultry", self._poultry)):
            try:
                got = fn(lo, hi)
            except Exception as e:
                self.logger.error(f"[food_prices] {key} 收集失敗: {type(e).__name__}: {e}")
                failed.append(key)
                got = []
            per_source[key] = len(got)
            rows.extend(got)

        # 同鍵去重（上游偶有重複列），保留先出現者
        seen, deduped = set(), []
        for r in rows:
            k = (r["source"], r["trade_date"], r["category"], r["item_name"], r["market_name"])
            if k in seen:
                continue
            seen.add(k)
            r["collected_at"] = now
            deduped.append(r)

        return {
            "data": deduped,
            "row_count": len(deduped),
            "per_source": per_source,
            "failed_sources": failed,
            "window": [lo.isoformat(), hi.isoformat()],
            "collected_at": now.isoformat(),
        }
