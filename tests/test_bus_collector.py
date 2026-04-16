"""
BusCollector 的平行抓取行為測試

驗證：
1. 22 縣市透過 ThreadPoolExecutor 內部平行呼叫（不是序列化）
2. 單一城市錯誤不影響其他城市
3. Timeout 錯誤被正確歸類
4. collect() 的回傳格式維持不變（下游 transformer 相容）
"""

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

# 在 import bus 前先把 config.BUS_CITIES 設為測試值
# 避免 BusCollector(interval_minutes = config.BUS_INTERVAL) 的 class attribute 有問題
import config


class TestBusCollectorFetchCity:
    """_fetch_city 不應拋出 exception，錯誤需以 error 欄位回傳"""

    def _make_collector(self):
        """建構 BusCollector 但繞過真實 HTTP session 與 TDX auth"""
        from collectors.bus import BusCollector

        collector = BusCollector.__new__(BusCollector)
        # 繞過 BaseCollector __init__（避免 storage / supabase 連線）
        collector.storage = MagicMock()
        collector.supabase_writer = None
        collector.last_run = None
        collector.last_success_at = None
        collector.run_count = 0
        collector.error_count = 0
        collector.consecutive_errors = 0
        # 注入假的 session 與 auth
        collector._session = MagicMock()
        collector.auth = MagicMock()
        collector.auth.get_auth_header.return_value = {'Authorization': 'fake'}
        return collector

    def test_fetch_city_success(self):
        """正常回應應回傳 raw + active"""
        collector = self._make_collector()

        fake_response = MagicMock()
        fake_response.json.return_value = [
            {'PlateNumb': 'A1', 'BusPosition': {'PositionLat': 25.0, 'PositionLon': 121.5}},
            {'PlateNumb': 'A2', 'BusPosition': {'PositionLat': None, 'PositionLon': None}},
            {'PlateNumb': 'A3', 'BusPosition': {}},
        ]
        fake_response.raise_for_status.return_value = None
        collector._session.get.return_value = fake_response

        result = collector._fetch_city('Taipei')

        assert result['city'] == 'Taipei'
        assert result['error'] is None
        assert len(result['raw']) == 3
        assert len(result['active']) == 1  # 只有 A1 有 GPS
        assert result['active'][0]['PlateNumb'] == 'A1'

    def test_fetch_city_http_error(self):
        """HTTP 錯誤應回傳 error 欄位而非拋 exception"""
        collector = self._make_collector()

        fake_response = MagicMock()
        fake_response.status_code = 500
        http_err = requests.exceptions.HTTPError(response=fake_response)
        fake_response.raise_for_status.side_effect = http_err
        collector._session.get.return_value = fake_response

        result = collector._fetch_city('BadCity')

        assert result['error'] is not None
        assert 'HTTP 500' in result['error']
        assert result['raw'] == []
        assert result['active'] == []

    def test_fetch_city_timeout(self):
        """Timeout 應被捕捉為 'Timeout' error"""
        collector = self._make_collector()
        collector._session.get.side_effect = requests.exceptions.Timeout("timed out")

        result = collector._fetch_city('SlowCity')

        assert result['error'] == 'Timeout'
        assert result['raw'] == []

    def test_fetch_city_unexpected_error(self):
        """任何其他 exception 都應被吃掉"""
        collector = self._make_collector()
        collector._session.get.side_effect = RuntimeError("weird")

        result = collector._fetch_city('WeirdCity')

        assert result['error'] == 'weird'


class TestBusCollectorCollect:
    """collect() 的平行抓取與錯誤隔離"""

    def _make_collector_with_fake_fetch(self, fetch_results_by_city):
        """建立 collector，讓 _fetch_city 回傳預先設定好的結果"""
        from collectors.bus import BusCollector

        collector = BusCollector.__new__(BusCollector)
        collector.storage = MagicMock()
        collector.supabase_writer = None
        collector.last_run = None
        collector.last_success_at = None
        collector.run_count = 0
        collector.error_count = 0
        collector.consecutive_errors = 0
        collector._session = MagicMock()
        collector.auth = MagicMock()

        # 覆寫 _fetch_city 為可控的假實作
        def fake_fetch(city):
            time.sleep(0.1)  # 模擬 HTTP 延遲
            return fetch_results_by_city.get(
                city,
                {'city': city, 'raw': [], 'active': [], 'error': 'not mocked'}
            )
        collector._fetch_city = fake_fetch

        return collector

    def test_collect_runs_cities_in_parallel(self, monkeypatch):
        """5 個城市每個 sleep 0.1s，平行應 <0.3s（序列會是 0.5s）"""
        cities = ['Taipei', 'NewTaipei', 'Taoyuan', 'Taichung', 'Tainan']
        fake_results = {
            city: {
                'city': city,
                'raw': [{'PlateNumb': f'{city}-1'}],
                'active': [{'PlateNumb': f'{city}-1', 'BusPosition': {'PositionLat': 25, 'PositionLon': 121}}],
                'error': None,
            }
            for city in cities
        }

        monkeypatch.setattr(config, 'BUS_CITIES', cities)
        monkeypatch.setattr(config, 'BUS_FETCH_WORKERS', 5)

        collector = self._make_collector_with_fake_fetch(fake_results)

        start = time.monotonic()
        result = collector.collect()
        elapsed = time.monotonic() - start

        assert elapsed < 0.35, f"應平行但實際耗時 {elapsed:.2f}s（疑似序列化）"
        assert result['total_active'] == 5
        assert len(result['data']) == 5

    def test_collect_isolates_per_city_error(self, monkeypatch):
        """單一城市失敗不影響其他城市"""
        cities = ['GoodCity', 'BadCity', 'AnotherGoodCity']
        fake_results = {
            'GoodCity': {
                'city': 'GoodCity',
                'raw': [{}],
                'active': [{'PlateNumb': 'G1', 'BusPosition': {'PositionLat': 25, 'PositionLon': 121}}],
                'error': None,
            },
            'BadCity': {
                'city': 'BadCity',
                'raw': [],
                'active': [],
                'error': 'HTTP 500',
            },
            'AnotherGoodCity': {
                'city': 'AnotherGoodCity',
                'raw': [{}],
                'active': [{'PlateNumb': 'AG1', 'BusPosition': {'PositionLat': 25, 'PositionLon': 121}}],
                'error': None,
            },
        }

        monkeypatch.setattr(config, 'BUS_CITIES', cities)
        monkeypatch.setattr(config, 'BUS_FETCH_WORKERS', 3)

        collector = self._make_collector_with_fake_fetch(fake_results)
        result = collector.collect()

        # 2 台好城市各有 1 台車
        assert result['total_active'] == 2
        assert result['by_city']['BadCity'] == {'error': 'HTTP 500'}
        assert result['by_city']['GoodCity']['active'] == 1
        assert result['by_city']['AnotherGoodCity']['active'] == 1

    def test_collect_injects_city_and_fetch_time(self, monkeypatch):
        """每台車應被注入 _city 和 _fetch_time 欄位（下游 transformer 相容）"""
        monkeypatch.setattr(config, 'BUS_CITIES', ['Taipei'])
        monkeypatch.setattr(config, 'BUS_FETCH_WORKERS', 1)

        collector = self._make_collector_with_fake_fetch({
            'Taipei': {
                'city': 'Taipei',
                'raw': [{}],
                'active': [{'PlateNumb': 'T1', 'BusPosition': {'PositionLat': 25, 'PositionLon': 121}}],
                'error': None,
            }
        })

        result = collector.collect()

        assert len(result['data']) == 1
        bus = result['data'][0]
        assert bus['_city'] == 'Taipei'
        assert '_fetch_time' in bus
        assert bus['PlateNumb'] == 'T1'


class TestBusCollectorConfig:
    """驗證 config 預設值涵蓋全台 22 縣市"""

    def test_default_bus_cities_covers_all_22(self):
        """預設 BUS_CITIES 應包含全台 22 縣市"""
        expected = {
            # 6 直轄市
            'Taipei', 'NewTaipei', 'Taoyuan', 'Taichung', 'Tainan', 'Kaohsiung',
            # 3 省轄市
            'Keelung', 'Hsinchu', 'Chiayi',
            # 10 縣
            'HsinchuCounty', 'MiaoliCounty', 'ChanghuaCounty', 'NantouCounty',
            'YunlinCounty', 'ChiayiCounty', 'PingtungCounty', 'YilanCounty',
            'HualienCounty', 'TaitungCounty',
            # 3 離島
            'PenghuCounty', 'KinmenCounty', 'LienchiangCounty',
        }
        # 直接用 default 常數比對（避免受環境變數影響）
        default_cities = set(config.BUS_CITIES_DEFAULT.split(','))
        assert default_cities == expected
        assert len(default_cities) == 22

    def test_default_interval_is_2_minutes(self):
        """為了 TDX quota 管理，預設 BUS_INTERVAL 應為 2 分鐘"""
        # 用環境變數會覆蓋，所以這裡檢查 default fallback
        # 間接測試：hardcoded 的 '2'
        import os
        if 'BUS_INTERVAL' not in os.environ:
            assert config.BUS_INTERVAL == 2

    def test_bus_fetch_workers_configurable(self):
        """BUS_FETCH_WORKERS 應可從環境變數設定，預設 5"""
        assert hasattr(config, 'BUS_FETCH_WORKERS')
        assert isinstance(config.BUS_FETCH_WORKERS, int)
        assert config.BUS_FETCH_WORKERS >= 1
