"""
公車即時位置收集器

從 TDX API 取得公車即時定頻資料 (RealTimeByFrequency / A1)。
以 GPS 回報為準（不依賴 DutyStatus/BusStatus，各縣市業者填寫習慣不一致）。

預設涵蓋全台 22 縣市（6 直轄市 + 3 省轄市 + 10 縣 + 3 離島）。
城市 API 呼叫採 ThreadPoolExecutor 內部平行化，避免串行累積耗時。
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession
from .base import BaseCollector

logger = logging.getLogger(__name__)


class BusCollector(BaseCollector):
    """公車即時位置收集器"""

    name = "bus"
    interval_minutes = config.BUS_INTERVAL
    # 22 城市平行抓取，配合 BUS_FETCH_WORKERS=5 預估 ~10-15 秒可完成
    # 保留 2 分鐘 timeout 以應對 TDX 偶發慢回應
    COLLECT_TIMEOUT = 120

    def __init__(self):
        super().__init__()
        # TDXSession 會自動通過全域 TDX rate limiter（4 req/sec 預設）
        # 所以即使 BUS_FETCH_WORKERS=5 平行呼叫 _fetch_city，實際送出仍被節流
        self._session = TDXSession()
        self.auth = TDXAuth(session=self._session)

    def _fetch_city(self, city: str) -> dict:
        """取得單一城市公車即時位置。

        回傳 {'city': str, 'raw': list, 'active': list, 'error': Optional[str]}
        保證不拋 exception，錯誤以 error 欄位回傳。
        """
        url = f"{config.TDX_API_BASE}/v2/Bus/RealTimeByFrequency/City/{city}"
        try:
            headers = self.auth.get_auth_header()
            response = self._session.get(
                url,
                headers=headers,
                params={'$format': 'JSON'},
                timeout=config.REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            raw = response.json()
            active = self._has_gps(raw)
            return {'city': city, 'raw': raw, 'active': active, 'error': None}
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 'N/A'
            return {'city': city, 'raw': [], 'active': [], 'error': f'HTTP {status}'}
        except requests.exceptions.Timeout:
            return {'city': city, 'raw': [], 'active': [], 'error': 'Timeout'}
        except Exception as e:
            return {'city': city, 'raw': [], 'active': [], 'error': str(e)}

    @staticmethod
    def _has_gps(buses: list) -> list:
        """保留有經緯度的車輛。

        不再依賴 DutyStatus / BusStatus — 北中南業者填寫習慣不一致
        （桃中高把 0 當執勤中，很多 BusStatus 為 null），實際以 GPS 回報為準。
        """
        result = []
        for b in buses:
            pos = b.get('BusPosition') or {}
            if isinstance(pos, dict) and pos.get('PositionLat') and pos.get('PositionLon'):
                result.append(b)
        return result

    def collect(self) -> dict:
        """收集全台縣市公車即時位置（內部平行抓取）"""
        fetch_time = datetime.now()
        fetch_iso = fetch_time.isoformat()
        all_buses = []
        city_stats = {}

        cities = config.BUS_CITIES
        workers = max(1, min(config.BUS_FETCH_WORKERS, len(cities)))

        print(f"   平行抓取 {len(cities)} 城市 (workers={workers})")

        # 內部 ThreadPoolExecutor 抓取所有城市
        # 注意：這是 scheduler 內部的 pool，與外層 CollectorScheduler 是兩層
        # 因 HTTP I/O 為主，max_workers=5 的額外負擔很小
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='bus-fetch') as pool:
            future_map = {pool.submit(self._fetch_city, city): city for city in cities}

            for future in as_completed(future_map):
                city = future_map[future]
                try:
                    # result 本身不應該拋 exception（_fetch_city 已包過），但留個保險
                    result = future.result(timeout=config.REQUEST_TIMEOUT + 10)
                except Exception as e:
                    logger.error(f"[bus] {city} future 異常: {e}")
                    city_stats[city] = {'error': f'future exception: {e}'}
                    continue

                if result['error']:
                    city_stats[city] = {'error': result['error']}
                    print(f"   {city}: ✗ {result['error']}")
                    continue

                active = result['active']
                raw_count = len(result['raw'])
                active_count = len(active)

                # 標記城市與抓取時間（保留原欄位名稱與下游 transform 相容）
                for bus in active:
                    bus['_city'] = city
                    bus['_fetch_time'] = fetch_iso

                city_stats[city] = {'raw': raw_count, 'active': active_count}
                all_buses.extend(active)
                print(f"   {city}: {active_count}/{raw_count} 台有 GPS")

        total = len(all_buses)
        succeeded = sum(1 for s in city_stats.values() if 'error' not in s)
        failed = len(city_stats) - succeeded
        print(f"   ✓ 合計: {total} 台公車（{succeeded}/{len(cities)} 城市成功，{failed} 失敗）")

        return {
            'fetch_time': fetch_iso,
            'total_active': total,
            'by_city': city_stats,
            'data': all_buses,
        }
