"""
國道即時車流 + 壅塞收集器

從 TDX API 取得國道 VD 即時車流與路段壅塞等級。
資料約每 60 秒更新一次，預設每 10 分鐘收集。

API 端點:
  - /v2/Road/Traffic/Live/Freeway       路段壅塞等級 + 旅行速率
  - /v2/Road/Traffic/Live/VD/Freeway    VD 即時車流（速率/佔有率/車流量）
"""

from collections import Counter
from datetime import datetime

import requests

import config
from utils.auth import TDXAuth
from .base import BaseCollector

# 車種代碼（國道無機車）
VEHICLE_TYPES = {
    'S': '小型車',
    'L': '大型車',
    'T': '聯結車',
}

# 壅塞等級
CONGESTION_LABELS = {
    1: '順暢',
    2: '車多',
    3: '略壅塞',
    4: '壅塞',
    5: '嚴重壅塞',
}

ROAD_NAMES = {
    '0001': '國道1號', '0002': '國道2號',
    '0003': '國道3號', '0003A': '國道3甲',
    '0004': '國道4號', '0005': '國道5號',
    '0006': '國道6號', '0008': '國道8號', '0010': '國道10號',
}


class FreewayVDCollector(BaseCollector):
    """國道即時車流 + 壅塞收集器"""

    name = "freeway_vd"
    interval_minutes = config.FREEWAY_VD_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self.auth = TDXAuth(session=self._session)

    def _fetch_live_traffic(self) -> tuple:
        """取得路段即時壅塞"""
        url = f"{config.TDX_API_BASE}/v2/Road/Traffic/Live/Freeway"
        headers = self.auth.get_auth_header()

        resp = self._session.get(
            url, headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()

        lives = data.get('LiveTraffics', [])
        collect_time = lives[0].get('DataCollectTime', '') if lives else ''

        result = []
        for lv in lives:
            result.append({
                'SectionID': lv.get('SectionID', ''),
                'TravelTime': lv.get('TravelTime', 0),
                'TravelSpeed': lv.get('TravelSpeed', 0),
                'CongestionLevel': lv.get('CongestionLevel', 0),
                'DataCollectTime': lv.get('DataCollectTime', ''),
            })

        return result, collect_time

    def _fetch_live_vd(self) -> tuple:
        """取得 VD 即時車流"""
        url = f"{config.TDX_API_BASE}/v2/Road/Traffic/Live/VD/Freeway"
        headers = self.auth.get_auth_header()

        resp = self._session.get(
            url, headers=headers,
            params={'$format': 'JSON'},
            timeout=config.REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()

        vd_lives = data.get('VDLives', [])
        collect_time = vd_lives[0].get('DataCollectTime', '') if vd_lives else ''
        return vd_lives, collect_time

    def _parse_vd_live(self, vd_live: dict) -> dict:
        """解析單一 VD 即時資料"""
        total_volume = {'S': 0, 'L': 0, 'T': 0}
        speeds = []
        occupancies = []
        lane_count = 0

        for link_flow in vd_live.get('LinkFlows', []):
            for lane in link_flow.get('Lanes', []):
                lane_count += 1
                speed = lane.get('Speed', 0)
                occupancy = lane.get('Occupancy', 0)
                if speed > 0:
                    speeds.append(speed)
                if occupancy >= 0:
                    occupancies.append(occupancy)
                for vehicle in lane.get('Vehicles', []):
                    v_type = vehicle.get('VehicleType', '')
                    volume = vehicle.get('Volume', 0)
                    if volume > 0 and v_type in total_volume:
                        total_volume[v_type] += volume

        avg_speed = sum(speeds) / len(speeds) if speeds else 0
        avg_occupancy = sum(occupancies) / len(occupancies) if occupancies else 0

        return {
            'VDID': vd_live.get('VDID', ''),
            'DataCollectTime': vd_live.get('DataCollectTime', ''),
            'Status': vd_live.get('Status', -1),
            'LaneCount': lane_count,
            'AvgSpeed': round(avg_speed, 1),
            'AvgOccupancy': round(avg_occupancy, 1),
            'TotalVolume': sum(total_volume.values()),
            'VolumeSmallCar': total_volume['S'],
            'VolumeLargeCar': total_volume['L'],
            'VolumeTrailer': total_volume['T'],
        }

    def collect(self) -> dict:
        """收集國道即時車流 + 壅塞資料"""
        fetch_time = datetime.now()

        # 取得路段壅塞
        section_data, section_time = self._fetch_live_traffic()
        print(f"   ✓ 路段壅塞: {len(section_data)} 段")

        # 壅塞分布
        levels = Counter(s['CongestionLevel'] for s in section_data)
        for level in sorted(levels.keys()):
            if level == 0:
                continue
            label = CONGESTION_LABELS.get(level, f'Level {level}')
            print(f"     {label}: {levels[level]} 段")

        # 取得 VD 車流
        vd_lives_raw, vd_time = self._fetch_live_vd()
        vd_data = [self._parse_vd_live(vd) for vd in vd_lives_raw]
        print(f"   ✓ VD 車流: {len(vd_data)} 個")

        # 統計
        total_volume = sum(d['TotalVolume'] for d in vd_data)
        valid_speeds = [d['AvgSpeed'] for d in vd_data if d['AvgSpeed'] > 0]
        avg_speed = sum(valid_speeds) / len(valid_speeds) if valid_speeds else 0

        print(f"\n   📊 車流 {total_volume:,} 輛/5min | 均速 {avg_speed:.1f} km/h")

        return {
            'fetch_time': fetch_time.isoformat(),
            'section_collect_time': section_time,
            'vd_collect_time': vd_time,
            'section_count': len(section_data),
            'vd_count': len(vd_data),
            'total_volume': total_volume,
            'avg_speed': round(avg_speed, 1),
            'congestion_distribution': {
                CONGESTION_LABELS.get(k, f'Level {k}'): v
                for k, v in sorted(levels.items()) if k > 0
            },
            'data': {
                'sections': section_data,
                'vd': vd_data,
            }
        }
