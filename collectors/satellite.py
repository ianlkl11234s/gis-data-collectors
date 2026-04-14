"""
衛星軌道追蹤收集器（Space-Track 版）

從 Space-Track.org GP class 一次拉取完整 satellite catalog（含已失效衛星），
用 sgp4 計算活躍衛星的即時位置（經緯度、高度、速度），
自動辨識星座與軌道類型後寫入。

資料來源：
- Space-Track.org GP class（需免費帳號）: https://www.space-track.org/
- SGP4 軌道傳播：sgp4 Python 套件

寫入策略：
- satellite_current / satellite_positions：只寫「活躍衛星」（保持顯示乾淨）
- satellite_tle：寫「全部」（含已失效），作為完整 TLE 知識庫

改用 Space-Track 原因：Zeabur 出口 IP 被 CelesTrak 封鎖（2026-04 起）。
Space-Track 是 CelesTrak 的上游資料源，資料更完整、API 穩定。

TLE 每 8-24 小時更新，每 2 小時拉一次足夠。
"""

import math
from datetime import datetime, timezone

import requests

import config
from collectors.base import BaseCollector

SPACETRACK_LOGIN_URL = "https://www.space-track.org/ajaxauth/login"
# GP class = 每顆衛星最新一筆 TLE（含已 decay 的，附 DECAY_DATE 欄位）
SPACETRACK_GP_URL = (
    "https://www.space-track.org/basicspacedata/query/class/gp"
    "/orderby/NORAD_CAT_ID/format/json"
)

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


def _parse_decay_date(raw: str | None) -> str | None:
    """Space-Track DECAY_DATE 格式為 'YYYY-MM-DD HH:MM:SS' 或 None，只保留日期部分"""
    if not raw:
        return None
    return raw.split(' ')[0] if ' ' in raw else raw[:10]


class SatelliteCollector(BaseCollector):
    """Space-Track 衛星軌道追蹤收集器"""

    name = "satellite"
    interval_minutes = config.SATELLITE_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'GIS-DataCollectors/1.0 (satellite-tracker)',
        })

    def _login(self):
        if not config.SPACETRACK_USERNAME or not config.SPACETRACK_PASSWORD:
            raise RuntimeError("SPACETRACK_USERNAME / SPACETRACK_PASSWORD 未設定")

        resp = self._session.post(
            SPACETRACK_LOGIN_URL,
            data={
                'identity': config.SPACETRACK_USERNAME,
                'password': config.SPACETRACK_PASSWORD,
            },
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()

        if 'chocolatechip' not in self._session.cookies:
            raise RuntimeError(f"Space-Track 登入失敗（無 session cookie，帳密可能錯誤）")

        print(f"[{self.name}] Space-Track 登入成功")

    def _fetch_all_gp(self) -> list[dict]:
        """一次拉取完整 GP class（~30,000 筆，含已 decay 衛星）"""
        resp = self._session.get(SPACETRACK_GP_URL, timeout=config.REQUEST_TIMEOUT * 4)
        resp.raise_for_status()
        return resp.json()

    def collect(self) -> dict:
        from sgp4.api import Satrec

        fetch_time = datetime.now(timezone.utc)

        # 1. 登入 + 拉取全部 GP
        self._login()
        print(f"[{self.name}] 從 Space-Track 拉取完整 GP class...")
        gp_list = self._fetch_all_gp()
        total_fetched = len(gp_list)
        print(f"[{self.name}] 收到 {total_fetched} 筆 GP 記錄")

        # 2. 解析 + SGP4 計算
        all_satellites = []        # 全部（寫 satellite_tle）
        active_satellites = []     # 活躍（寫 satellite_current / positions）
        sgp4_errors = 0
        parse_errors = 0

        for gp in gp_list:
            try:
                line1 = gp.get('TLE_LINE1') or ''
                line2 = gp.get('TLE_LINE2') or ''
                name = (gp.get('OBJECT_NAME') or '').strip()

                if not line1 or not line2:
                    parse_errors += 1
                    continue

                decay_date = _parse_decay_date(gp.get('DECAY_DATE'))
                is_decayed = bool(decay_date)
                object_type = (gp.get('OBJECT_TYPE') or '').strip().upper()

                sat = Satrec.twoline2rv(line1, line2)
                norad_id = sat.satnum
                inclination = math.degrees(sat.inclo)
                eccentricity = sat.ecco
                mean_motion = sat.no_kozai * 1440.0 / (2 * math.pi)  # rad/min → rev/day
                period_min = 1440.0 / mean_motion if mean_motion > 0 else 0

                orbit_type = _classify_orbit(period_min, eccentricity)
                constellation = _identify_constellation(name)
                intl_des = (gp.get('OBJECT_ID') or line1[9:17]).strip()
                epoch_str = line1[18:32].strip()

                base = {
                    'norad_id': norad_id,
                    'name': name,
                    'intl_designator': intl_des,
                    'constellation': constellation,
                    'orbit_type': orbit_type,
                    'inclination': round(inclination, 2),
                    'eccentricity': round(eccentricity, 6),
                    'period_min': round(period_min, 2),
                    'tle_line1': line1,
                    'tle_line2': line2,
                    'tle_epoch': epoch_str,
                    'decay_date': decay_date,
                    'is_decayed': is_decayed,
                    'object_type': object_type,
                }

                # 只有「活躍 + PAYLOAD（衛星本體）」才算位置 + 寫 current/positions
                # ROCKET BODY / DEBRIS / UNKNOWN / TBA 只寫入 satellite_tle 作為完整目錄
                if not is_decayed and object_type == 'PAYLOAD':
                    pos = _sgp4_propagate(sat, fetch_time)
                    if pos:
                        active_satellites.append({**base, **pos})
                    else:
                        sgp4_errors += 1

                all_satellites.append(base)

            except Exception:
                parse_errors += 1
                continue

        # 統計
        orbit_stats = {}
        constellation_stats = {}
        for s in active_satellites:
            ot = s['orbit_type']
            orbit_stats[ot] = orbit_stats.get(ot, 0) + 1
            cs = s['constellation'] or 'Other'
            constellation_stats[cs] = constellation_stats.get(cs, 0) + 1

        decayed_count = sum(1 for s in all_satellites if s['is_decayed'])
        type_stats = {}
        for s in all_satellites:
            t = s.get('object_type') or 'UNKNOWN'
            type_stats[t] = type_stats.get(t, 0) + 1

        print(f"[{self.name}] 活躍 PAYLOAD {len(active_satellites)} 顆（寫 current/positions）"
              f" / 失效 {decayed_count} 顆 / SGP4 錯誤 {sgp4_errors} / 解析失敗 {parse_errors}")
        print(f"[{self.name}] 物件分類: {type_stats}")
        print(f"[{self.name}] 軌道分佈: {orbit_stats}")

        top_constellations = sorted(constellation_stats.items(),
                                    key=lambda x: -x[1])[:5]
        print(f"[{self.name}] Top 星座: {dict(top_constellations)}")

        return {
            'fetch_time': fetch_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'total_fetched': total_fetched,
            'satellite_count': len(active_satellites),
            'decayed_count': decayed_count,
            'sgp4_errors': sgp4_errors,
            'parse_errors': parse_errors,
            'orbit_stats': orbit_stats,
            'top_constellations': dict(top_constellations),
            'data': active_satellites,       # 寫 current / positions 用（只含活躍）
            'data_all': all_satellites,      # 寫 satellite_tle 用（含失效）
        }
