"""
衛星軌道追蹤收集器

每 2 小時從 CelesTrak 分組拉取 3LE + GP JSON，
用 sgp4 計算即時位置（經緯度、高度、速度），
自動辨識星座與軌道類型後寫入。

資料來源：
- CelesTrak 3LE + GP JSON（免註冊）: https://celestrak.org/NORAD/elements/
- SGP4 軌道傳播：sgp4 Python 套件

TLE 每 2 小時更新，更頻繁拉取沒有意義。
前端如需即時動畫，應用 JS 版 satellite.js 在瀏覽器計算。
"""

import math
import time as _time
from datetime import datetime, timezone

import requests

import config
from collectors.base import BaseCollector

# CelesTrak endpoint
CELESTRAK_GP_URL = "https://celestrak.org/NORAD/elements/gp.php"

# CelesTrak 對 GROUP=active / starlink 有速率限制（403），改用分組拉取
# 涵蓋主要衛星類型，可依需求增減
CELESTRAK_GROUPS = [
    # 大型星座
    'starlink', 'qianfan',
    # 導航（glonass-operational 已失效，改用 glo-ops）
    'gps-ops', 'galileo', 'beidou', 'glo-ops',
    # 大型通訊星座
    'oneweb', 'iridium-NEXT', 'orbcomm', 'globalstar',
    # 氣象與地球觀測
    'weather', 'noaa', 'goes', 'resource', 'planet', 'spire',
    # 同步軌道通訊
    'geo', 'ses',
    # 太空站與科學
    'stations', 'science',
    # 搜救、環境監測、中繼
    'sarsat', 'argos', 'tdrss',
    # 特殊軌道與軍事
    'molniya', 'military', 'radar', 'analyst',
    # 小型衛星與教育
    'cubesat', 'education', 'dmc',
    # 業餘、社群追蹤、肉眼可見（含 FORMOSAT 7 / COSMIC-2）
    'amateur', 'satnogs', 'visual',
    # 太空碎片（三大碎片事件）
    'fengyun-1c-debris', 'cosmos-2251-debris', 'iridium-33-debris',
]

# 軌道類型分類閾值（km）
ORBIT_THRESHOLDS = {
    'LEO': 2000,
    'GEO_LOW': 35586,   # GEO ± 200 km
    'GEO_HIGH': 35986,
}

# 主要星座辨識（依衛星名稱）
CONSTELLATION_PATTERNS = {
    'STARLINK': 'Starlink',
    'ONEWEB': 'OneWeb',
    'IRIDIUM': 'Iridium',
    'GLOBALSTAR': 'Globalstar',
    'ORBCOMM': 'Orbcomm',
    'PLANET': 'Planet',
    'SPIRE': 'Spire',
    'GPS': 'GPS',
    'GALILEO': 'Galileo',
    'GLONASS': 'GLONASS',
    'BEIDOU': 'BeiDou',
    'COSMOS': 'COSMOS',
    'NAVSTAR': 'GPS',
    'QIANFAN': 'Qianfan',
    'FORMOSAT': 'FORMOSAT',
    'COSPAS': 'COSPAS-SARSAT',
    'TDRS': 'TDRS',
    'MOLNIYA': 'Molniya',
    'DMC': 'DMC',
}


def _classify_orbit(period_min: float, eccentricity: float) -> str:
    """根據軌道週期和離心率分類軌道類型"""
    if eccentricity > 0.25:
        return 'HEO'

    # T = 2π√(a³/μ), μ = 398600.4418 km³/s²
    mu = 398600.4418
    t_sec = period_min * 60
    a_km = (mu * (t_sec / (2 * math.pi)) ** 2) ** (1 / 3)
    alt_km = a_km - 6371

    if alt_km < ORBIT_THRESHOLDS['LEO']:
        return 'LEO'
    elif ORBIT_THRESHOLDS['GEO_LOW'] <= alt_km <= ORBIT_THRESHOLDS['GEO_HIGH']:
        return 'GEO'
    elif alt_km < 35786:
        return 'MEO'
    else:
        return 'GEO'


def _identify_constellation(name: str) -> str:
    """從衛星名稱辨識星座歸屬"""
    name_upper = (name or '').upper()
    for prefix, constellation in CONSTELLATION_PATTERNS.items():
        if prefix in name_upper:
            return constellation
    return ''


def _sgp4_propagate(sat, ts: datetime) -> dict | None:
    """用 sgp4 計算衛星在指定時刻的位置"""
    from sgp4.api import jday

    jd, fr = jday(ts.year, ts.month, ts.day,
                  ts.hour, ts.minute, ts.second + ts.microsecond / 1e6)
    e, r, v = sat.sgp4(jd, fr)

    if e != 0:
        return None

    x, y, z = r  # km, ECI
    vx, vy, vz = v  # km/s

    # GMST（格林威治恆星時）
    d = jd - 2451545.0 + fr
    gmst_rad = math.radians(math.fmod(280.46061837 + 360.98564736629 * d, 360.0))

    # ECI → ECEF → LLA
    x_ecef = x * math.cos(gmst_rad) + y * math.sin(gmst_rad)
    y_ecef = -x * math.sin(gmst_rad) + y * math.cos(gmst_rad)

    lng = math.degrees(math.atan2(y_ecef, x_ecef))
    lat = math.degrees(math.atan2(z, math.sqrt(x_ecef ** 2 + y_ecef ** 2)))
    alt_km = math.sqrt(x ** 2 + y ** 2 + z ** 2) - 6371.0
    velocity = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2)

    return {
        'lat': round(lat, 4),
        'lng': round(lng, 4),
        'altitude_km': round(alt_km, 1),
        'velocity_kms': round(velocity, 2),
    }


def _parse_3le(text: str) -> list[tuple[str, str, str]]:
    """解析 3LE 格式文字，回傳 [(name, line1, line2), ...]"""
    lines = [l for l in text.strip().split('\r\n') if l.strip()]
    results = []
    i = 0
    while i < len(lines) - 2:
        # 3LE: name / line1 / line2
        if lines[i + 1].startswith('1 ') and lines[i + 2].startswith('2 '):
            results.append((lines[i].strip(), lines[i + 1].strip(), lines[i + 2].strip()))
            i += 3
        else:
            i += 1
    return results


class SatelliteCollector(BaseCollector):
    """CelesTrak 衛星軌道追蹤收集器"""

    name = "satellite"
    interval_minutes = config.SATELLITE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'GIS-DataCollectors/1.0 (satellite-tracker)',
        })

    def _fetch_group_3le(self, group: str) -> list[tuple[str, str, str]]:
        """從 CelesTrak 拉取單一群組的 3LE 格式"""
        resp = self._session.get(
            CELESTRAK_GP_URL,
            params={'GROUP': group, 'FORMAT': '3le'},
            timeout=config.REQUEST_TIMEOUT * 2,
        )
        if resp.status_code in (404, 403):
            print(f"[satellite]   ⚠️ {group}: HTTP {resp.status_code}，跳過")
            return []
        resp.raise_for_status()

        result = _parse_3le(resp.text)
        # 大群組（>500 顆）拉完後多等一下，避免 CelesTrak rate limit
        delay = 2.0 if len(result) > 500 else config.REQUEST_INTERVAL
        _time.sleep(delay)
        return result

    def _fetch_all_groups(self) -> list[tuple[str, str, str]]:
        """分組拉取 3LE 並以 NORAD ID 去重"""
        seen = set()
        all_sats = []

        for group in CELESTRAK_GROUPS:
            try:
                entries = self._fetch_group_3le(group)
                new_count = 0
                for name, l1, l2 in entries:
                    # NORAD ID 在 line1 的 col 2-7
                    try:
                        norad_id = int(l1[2:7].strip())
                    except ValueError:
                        continue
                    if norad_id not in seen:
                        seen.add(norad_id)
                        all_sats.append((name, l1, l2))
                        new_count += 1
                if entries:
                    print(f"[{self.name}]   {group}: {len(entries)} 顆（新增 {new_count}）")
            except Exception as e:
                print(f"[{self.name}]   {group}: 拉取失敗 ({e})")

        return all_sats

    def collect(self) -> dict:
        from sgp4.api import Satrec

        fetch_time = datetime.now(timezone.utc)

        # 1. 分組拉取 3LE 並去重
        print(f"[{self.name}] 開始分組拉取 {len(CELESTRAK_GROUPS)} 個群組...")
        tle_list = self._fetch_all_groups()
        total_fetched = len(tle_list)
        print(f"[{self.name}] 去重後共 {total_fetched} 顆衛星")

        # 2. 逐顆用 SGP4 計算即時位置
        satellites = []
        error_count = 0

        for name, line1, line2 in tle_list:
            try:
                sat = Satrec.twoline2rv(line1, line2)
                pos = _sgp4_propagate(sat, fetch_time)
                if not pos:
                    error_count += 1
                    continue

                # 從 TLE 提取軌道參數
                norad_id = sat.satnum
                inclination = math.degrees(sat.inclo)
                eccentricity = sat.ecco
                mean_motion = sat.no_kozai * 1440.0 / (2 * math.pi)  # rad/min → rev/day
                period_min = 1440.0 / mean_motion if mean_motion > 0 else 0

                orbit_type = _classify_orbit(period_min, eccentricity)
                constellation = _identify_constellation(name)

                # intl designator 在 line1 col 9-17
                intl_des = line1[9:17].strip()

                # epoch 在 line1 col 18-32
                epoch_str = line1[18:32].strip()

                satellites.append({
                    'norad_id': norad_id,
                    'name': name,
                    'intl_designator': intl_des,
                    'constellation': constellation,
                    'orbit_type': orbit_type,
                    'lat': pos['lat'],
                    'lng': pos['lng'],
                    'altitude_km': pos['altitude_km'],
                    'velocity_kms': pos['velocity_kms'],
                    'inclination': round(inclination, 2),
                    'eccentricity': round(eccentricity, 6),
                    'period_min': round(period_min, 2),
                    'tle_line1': line1,
                    'tle_line2': line2,
                    'tle_epoch': epoch_str,
                })

            except Exception:
                error_count += 1
                continue

        # 統計
        orbit_stats = {}
        constellation_stats = {}
        for s in satellites:
            ot = s['orbit_type']
            orbit_stats[ot] = orbit_stats.get(ot, 0) + 1
            cs = s['constellation'] or 'Other'
            constellation_stats[cs] = constellation_stats.get(cs, 0) + 1

        print(f"[{self.name}] 成功計算 {len(satellites)} 顆位置"
              f"（{error_count} 個計算失敗）")
        print(f"[{self.name}] 軌道分佈: {orbit_stats}")

        top_constellations = sorted(constellation_stats.items(),
                                    key=lambda x: -x[1])[:5]
        print(f"[{self.name}] Top 星座: {dict(top_constellations)}")

        return {
            'fetch_time': fetch_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'total_fetched': total_fetched,
            'satellite_count': len(satellites),
            'sgp4_errors': error_count,
            'orbit_stats': orbit_stats,
            'top_constellations': dict(top_constellations),
            'data': satellites,
        }
