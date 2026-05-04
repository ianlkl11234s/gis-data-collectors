"""
垃圾車即時 GPS 收集器（高雄 / 新北 / 台南）

三家政府開放資料 GPS 端點實打驗證後（2026-05-04 14:10 平日午班）：
    - 高雄  openapi.kcg.gov.tw      JSON wrapper, x/y 經緯度, ISO8601 time
    - 新北  data.ntpc.gov.tw        CSV (UTF-8 BOM), longitude/latitude, YYYY/MM/DD HH:MM:SS
    - 台南  soa.tainan.gov.tw       JSON wrapper（與高雄同框架）, x/y, YYYY-MM-DD HH:MM:SS

詳細實打結果與欄位對照見：
    taipei-gis-analytics/docs/topic-research/waste-management/api-validation.md

Quiet hours：垃圾車收運時段為早班 06-12 / 午班 12-18 / 晚班 18-24，凌晨幾乎零信號。
預設 01:00-06:00 跳過此 tick（節省 ~25% 流量；空回傳 INSERT 0 筆本身成本極低，
但跳過可少打三次 HTTP）。透過 WASTE_POSITIONS_QUIET_HOURS 調整或關閉。
"""

import csv
import io
from datetime import datetime
from typing import Optional

import requests

import config
from .base import BaseCollector, TAIPEI_TZ


CITY_NAMES = {
    'Kaohsiung': '高雄市',
    'NewTaipei': '新北市',
    'Tainan': '臺南市',
}

ENDPOINTS = {
    'Kaohsiung': 'https://openapi.kcg.gov.tw/Api/Service/Get/aaf4ce4b-4ca8-43de-bfaf-6dc97e89cac0',
    'NewTaipei': 'https://data.ntpc.gov.tw/api/datasets/28ab4122-60e1-4065-98e5-abccb69aaca6/csv/file',
    'Tainan':    'https://soa.tainan.gov.tw/Api/Service/Get/2c8a70d5-06f2-4353-9e92-c40d33bcd969',
}

# 用於從 location 字串推測 status：含這些關鍵字 → parked，否則 → collecting
PARKED_KEYWORDS = ('停車場', '區隊', '清潔隊', '車隊')

# 三家時間格式（含 fallback 順序）
TIME_FORMATS = (
    '%Y-%m-%dT%H:%M:%S',     # 高雄 ISO8601 純秒
    '%Y-%m-%dT%H:%M:%S.%f',  # 高雄 ISO8601 含 ms
    '%Y-%m-%d %H:%M:%S',     # 台南
    '%Y/%m/%d %H:%M:%S',     # 新北
)


def _parse_quiet_hours(spec: Optional[str]) -> Optional[tuple[int, int]]:
    """parse 'HH-HH' 為 (start, end)（前閉後開，可跨午夜）。空/none/off 為關閉。"""
    if not spec or spec.lower() in ('none', 'off'):
        return None
    try:
        s, e = spec.split('-')
        return (int(s), int(e))
    except (ValueError, AttributeError):
        return None


def _is_in_quiet_hours(hour: int, qh: Optional[tuple[int, int]]) -> bool:
    if qh is None:
        return False
    s, e = qh
    if s == e:
        return False
    if s < e:
        return s <= hour < e
    return hour >= s or hour < e


def _classify_status(location: str) -> str:
    if not location:
        return 'unknown'
    return 'parked' if any(kw in location for kw in PARKED_KEYWORDS) else 'collecting'


def _parse_observed_at(raw: Optional[str], fallback: datetime) -> str:
    """三家時間格式 → ISO8601 +08:00。失敗回退 fallback (collector 抓取時間)"""
    if not raw:
        return fallback.isoformat()
    raw = raw.strip()
    for fmt in TIME_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=TAIPEI_TZ).isoformat()
        except ValueError:
            continue
    return fallback.isoformat()


def _safe_float(v) -> Optional[float]:
    if v is None or v == '':
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


class WastePositionsCollector(BaseCollector):
    """垃圾車即時 GPS 收集器（高雄 / 新北 / 台南）"""

    name = "waste_positions"
    interval_minutes = config.WASTE_POSITIONS_INTERVAL

    def __init__(self, cities: Optional[list] = None):
        super().__init__()
        self.cities = cities or config.WASTE_POSITIONS_CITIES
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; TaipeiGISBot/1.0; +https://github.com/)',
            'Accept': '*/*',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        })

    # ------------------------------------------------------------
    # quiet hours
    # ------------------------------------------------------------

    def _check_quiet_hours(self, now: datetime) -> Optional[str]:
        spec = getattr(config, 'WASTE_POSITIONS_QUIET_HOURS', '01-06')
        qh = _parse_quiet_hours(spec)
        if qh and _is_in_quiet_hours(now.hour, qh):
            return f'quiet_hours ({qh[0]:02d}-{qh[1]:02d})'
        return None

    # ------------------------------------------------------------
    # 三家 fetcher
    # ------------------------------------------------------------

    def _fetch_kaohsiung(self, fetch_time: datetime) -> list[dict]:
        url = ENDPOINTS['Kaohsiung']
        r = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        body = r.json()
        if not body.get('success'):
            raise RuntimeError(f"Kaohsiung API rejected: {body.get('message')}")
        return self._normalize_soa(body.get('data') or [], '高雄市', url, fetch_time)

    def _fetch_tainan(self, fetch_time: datetime) -> list[dict]:
        url = ENDPOINTS['Tainan']
        r = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        body = r.json()
        if not body.get('success'):
            raise RuntimeError(f"Tainan API rejected: {body.get('message')}")
        return self._normalize_soa(body.get('data') or [], '臺南市', url, fetch_time)

    def _normalize_soa(self, rows: list, city: str, url: str, fetch_time: datetime) -> list[dict]:
        """高雄 / 台南共用 SOA 平台 schema (x/y/linid/car/time/location)"""
        out = []
        for row in rows:
            lng = _safe_float(row.get('x'))
            lat = _safe_float(row.get('y'))
            if lat is None or lng is None:
                continue
            location = row.get('location') or ''
            out.append({
                'city': city,
                'vehicle_no': (row.get('car') or '').strip(),
                'route_id': (row.get('linid') or '').strip() or None,
                'lat': lat,
                'lng': lng,
                'location': location,
                'observed_at': _parse_observed_at(row.get('time'), fetch_time),
                'status': _classify_status(location),
                'source_url': url,
            })
        return out

    def _fetch_new_taipei(self, fetch_time: datetime) -> list[dict]:
        url = ENDPOINTS['NewTaipei']
        r = self._session.get(url, timeout=config.REQUEST_TIMEOUT)
        r.raise_for_status()
        # CSV 有 UTF-8 BOM
        text = r.content.decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(text))
        out = []
        for row in reader:
            lng = _safe_float(row.get('longitude'))
            lat = _safe_float(row.get('latitude'))
            if lat is None or lng is None:
                continue
            location = row.get('location') or ''
            # 注意：NTPC 的 'cityname' 欄實為「行政區」（永和區/三重區...）
            out.append({
                'city': '新北市',
                'vehicle_no': (row.get('car') or '').strip(),
                'route_id': (row.get('lineid') or '').strip() or None,
                'lat': lat,
                'lng': lng,
                'location': location,
                'observed_at': _parse_observed_at(row.get('time'), fetch_time),
                'status': _classify_status(location),
                'district': (row.get('cityname') or '').strip() or None,
                'source_url': url,
            })
        return out

    FETCHERS = {
        'Kaohsiung': '_fetch_kaohsiung',
        'NewTaipei': '_fetch_new_taipei',
        'Tainan':    '_fetch_tainan',
    }

    # ------------------------------------------------------------
    # collect
    # ------------------------------------------------------------

    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)

        skip_reason = self._check_quiet_hours(fetch_time)
        if skip_reason:
            print(f"   ⏸  {skip_reason}：跳過本次抓取")
            return {
                'fetch_time': fetch_time.isoformat(),
                'total': 0,
                'skipped': skip_reason,
                'by_city': {},
                'data': [],
            }

        all_records: list[dict] = []
        city_stats: dict[str, dict] = {}

        for city in self.cities:
            city_name = CITY_NAMES.get(city, city)
            method_name = self.FETCHERS.get(city)
            if not method_name:
                print(f"   ✗ {city_name}: 未支援的城市代碼")
                city_stats[city] = {'name': city_name, 'error': 'unsupported city'}
                continue

            try:
                records = getattr(self, method_name)(fetch_time)
                all_records.extend(records)
                collecting = sum(1 for r in records if r.get('status') == 'collecting')
                parked = sum(1 for r in records if r.get('status') == 'parked')
                city_stats[city] = {
                    'name': city_name,
                    'count': len(records),
                    'collecting': collecting,
                    'parked': parked,
                }
                print(f"   ✓ {city_name}: {len(records)} 筆 (出勤 {collecting} / 待命 {parked})")
            except requests.exceptions.HTTPError as e:
                err = f"HTTP {e.response.status_code}"
                city_stats[city] = {'name': city_name, 'error': err}
                print(f"   ✗ {city_name}: {err}")
            except Exception as e:
                err = str(e)[:200]
                city_stats[city] = {'name': city_name, 'error': err}
                print(f"   ✗ {city_name}: {err}")

        total = len(all_records)
        active = sum(1 for s in city_stats.values() if 'error' not in s)
        print(f"\n   📊 總計: {total} 筆，{active}/{len(self.cities)} 城市成功")

        return {
            'fetch_time': fetch_time.isoformat(),
            'total': total,
            'by_city': city_stats,
            'data': all_records,
        }
