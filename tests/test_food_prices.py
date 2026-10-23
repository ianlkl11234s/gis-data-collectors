"""food_prices 錯誤路徑：取得或單一來源失敗時要記 log 並繼續，不能自己炸掉。

2026-10-23：錯誤路徑呼叫不存在的 self.logger，AttributeError 蓋掉上游真正的錯誤，
collector 停了約 1.5 天（collect() 失敗心跳上線後才看見）。
"""

from unittest.mock import MagicMock

import requests

from collectors.food_prices import FoodPricesCollector


def _collector():
    collector = FoodPricesCollector.__new__(FoodPricesCollector)
    collector._session = MagicMock()
    return collector


def test_get_failure_logs_and_returns_empty(caplog):
    collector = _collector()
    collector._session.get.side_effect = requests.ConnectionError("reset")

    assert collector._get("/api/v1/AgriProductsTransType/", {}) == []
    assert "取得失敗" in caplog.text


def test_one_source_failing_keeps_the_others(caplog):
    collector = _collector()
    row = {"source": "hog", "trade_date": "2026-10-22", "category": "c", "item_name": "i", "market_name": "m"}
    collector._produce = MagicMock(side_effect=RuntimeError("boom"))
    collector._aquatic = MagicMock(return_value=[])
    collector._hog = MagicMock(return_value=[row])
    collector._poultry = MagicMock(return_value=[])

    result = collector.collect()

    assert result["failed_sources"] == ["produce"]
    assert result["row_count"] == 1
    assert "produce 收集失敗" in caplog.text
