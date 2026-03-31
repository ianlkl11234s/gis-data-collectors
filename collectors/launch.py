"""
太空發射資料收集器 — Launch Library 2 (TheSpaceDevs)

收集策略：
1. 歷史回溯：啟動時檢查 DB 最舊紀錄，從最近往前抓到 5 年前自動停止
2. 每日同步：upcoming launches UPSERT（狀態自動更新 TBD→Go→Success）
3. 發射台：首次同步全部 233 個，之後每月更新一次
4. 太空事件：每日抓取 upcoming events

Rate Limit（免費 15 次/小時）：
- 歷史回溯：每次 100 筆，間隔 4-5 分鐘
- 日常同步：每天 2-3 次 API call

資料來源：
- Launch Library 2 (免費，無需 API key): https://ll.thespacedevs.com/2.2.0/
"""

import time as _time
from datetime import datetime, timezone, timedelta

import requests

import config
from collectors.base import BaseCollector

# LL2 API
LL2_BASE = "https://ll.thespacedevs.com/2.2.0"

# 歷史回溯停止點：5 年前
BACKFILL_CUTOFF_YEARS = 5

# Rate limit 安全間隔（秒）— 付費 token 才縮短
RATE_LIMIT_PAID_INTERVAL = 2


def _parse_launch(raw: dict) -> dict:
    """將 LL2 API 回傳的 launch 物件轉為扁平化 dict"""
    pad = raw.get('pad') or {}
    location = pad.get('location') or {}
    mission = raw.get('mission') or {}
    orbit = mission.get('orbit') or {}
    rocket = raw.get('rocket') or {}
    rocket_config = rocket.get('configuration') or {}
    status = raw.get('status') or {}
    provider = raw.get('launch_service_provider') or {}
    program = raw.get('program') or []

    return {
        'id': raw.get('id', ''),
        'name': raw.get('name', ''),
        'slug': raw.get('slug', ''),
        'net': raw.get('net'),
        'window_start': raw.get('window_start'),
        'window_end': raw.get('window_end'),
        'status': status.get('abbrev', ''),
        'status_name': status.get('name', ''),
        'rocket_name': rocket_config.get('name', ''),
        'rocket_family': rocket_config.get('family', ''),
        'rocket_full_name': rocket_config.get('full_name', ''),
        'mission_name': mission.get('name', ''),
        'mission_type': mission.get('type', ''),
        'mission_description': mission.get('description', ''),
        'orbit_name': orbit.get('name', ''),
        'orbit_abbrev': orbit.get('abbrev', ''),
        'agency_name': provider.get('name', ''),
        'agency_type': provider.get('type', ''),
        'pad_id': str(pad.get('id', '')) if pad.get('id') else None,
        'pad_name': pad.get('name', ''),
        'pad_latitude': _safe_float(pad.get('latitude')),
        'pad_longitude': _safe_float(pad.get('longitude')),
        'location_name': location.get('name', ''),
        'country_code': location.get('country_code', ''),
        'probability': raw.get('probability'),
        'weather_concerns': raw.get('weather_concerns', ''),
        'webcast_live': raw.get('webcast_live', False),
        'image_url': raw.get('image', ''),
        'infographic_url': raw.get('infographic', ''),
        'program_names': ', '.join(p.get('name', '') for p in program) if program else '',
        'last_updated': raw.get('last_updated'),
    }


def _parse_pad(raw: dict) -> dict:
    """將 LL2 API 回傳的 pad 物件轉為扁平化 dict"""
    location = raw.get('location') or {}
    return {
        'id': str(raw.get('id', '')),
        'name': raw.get('name', ''),
        'latitude': _safe_float(raw.get('latitude')),
        'longitude': _safe_float(raw.get('longitude')),
        'location_name': location.get('name', ''),
        'country_code': location.get('country_code', ''),
        'total_launch_count': raw.get('total_launch_count', 0),
        'orbital_launch_attempt_count': raw.get('orbital_launch_attempt_count', 0),
        'map_url': raw.get('map_url', ''),
    }


def _parse_event(raw: dict) -> dict:
    """將 LL2 API 回傳的 event 物件轉為扁平化 dict"""
    event_type = raw.get('type') or {}
    program = raw.get('program') or []
    launches = raw.get('launches') or []

    return {
        'id': str(raw.get('id', '')),
        'name': raw.get('name', ''),
        'description': raw.get('description', ''),
        'type_name': event_type.get('name', ''),
        'date': raw.get('date'),
        'location': raw.get('location', ''),
        'news_url': raw.get('news_url', ''),
        'video_url': raw.get('video_url', ''),
        'image_url': raw.get('feature_image', ''),
        'program_names': ', '.join(p.get('name', '') for p in program) if program else '',
        'launch_ids': ', '.join(l.get('id', '') for l in launches) if launches else '',
        'last_updated': raw.get('last_updated'),
    }


def _safe_float(val) -> float | None:
    """安全轉換為 float"""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class LaunchCollector(BaseCollector):
    """Launch Library 2 太空發射資料收集器"""

    name = "launch"
    interval_minutes = config.LAUNCH_INTERVAL

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'GIS-DataCollectors/1.0 (launch-tracker)',
        })
        # 如果有付費 API token
        if config.LAUNCH_API_TOKEN:
            self._session.headers['Authorization'] = f'Token {config.LAUNCH_API_TOKEN}'

        self._backfill_complete = False
        self._backfill_offset = 0
        self._pads_synced = False
        # 狀態機：每次 collect() 只做一個任務，輪流執行
        # upcoming → backfill → pads → events → upcoming → backfill → ...
        self._next_task = 'upcoming'

    def _api_get(self, endpoint: str, params: dict = None) -> dict:
        """呼叫 LL2 API（不 sleep，由排程間隔控制頻率）"""
        url = f"{LL2_BASE}{endpoint}"
        resp = self._session.get(url, params=params, timeout=config.REQUEST_TIMEOUT * 2)
        resp.raise_for_status()
        # 付費 token 才加短間隔（用於 pads 多頁抓取）
        if config.LAUNCH_API_TOKEN:
            _time.sleep(RATE_LIMIT_PAID_INTERVAL)
        return resp.json()

    def _fetch_upcoming_launches(self) -> list[dict]:
        """抓取未來預計發射的任務"""
        print(f"[{self.name}] 抓取 upcoming launches...")
        result = self._api_get('/launch/upcoming/', {
            'limit': 100,
            'format': 'json',
        })
        launches = [_parse_launch(r) for r in result.get('results', [])]
        print(f"[{self.name}]   upcoming: {len(launches)} 筆")
        return launches

    def _fetch_previous_launches(self, offset: int = 0) -> tuple[list[dict], int]:
        """抓取歷史發射紀錄（從最近往前）"""
        result = self._api_get('/launch/previous/', {
            'limit': 100,
            'offset': offset,
            'ordering': '-net',  # 最近的先
            'format': 'json',
        })
        total = result.get('count', 0)
        launches = [_parse_launch(r) for r in result.get('results', [])]
        return launches, total

    def _fetch_pads(self) -> list[dict]:
        """抓取所有發射台"""
        print(f"[{self.name}] 抓取所有發射台...")
        all_pads = []
        offset = 0
        while True:
            result = self._api_get('/pad/', {
                'limit': 100,
                'offset': offset,
                'format': 'json',
            })
            pads = [_parse_pad(r) for r in result.get('results', [])]
            all_pads.extend(pads)
            if not result.get('next'):
                break
            offset += 100
        print(f"[{self.name}]   發射台: {len(all_pads)} 個")
        return all_pads

    def _fetch_upcoming_events(self) -> list[dict]:
        """抓取即將到來的太空事件"""
        print(f"[{self.name}] 抓取 upcoming events...")
        result = self._api_get('/event/upcoming/', {
            'limit': 50,
            'format': 'json',
        })
        events = [_parse_event(r) for r in result.get('results', [])]
        print(f"[{self.name}]   events: {len(events)} 筆")
        return events

    def _backfill_history(self) -> list[dict]:
        """歷史回溯：從最近往前抓，到 5 年前自動停止

        每次 collect() 只抓一批（100 筆），下次 collect() 繼續。
        避免一次抓太多觸發 rate limit。
        """
        if self._backfill_complete:
            return []

        # 計算截止日期
        cutoff = datetime.now(timezone.utc) - timedelta(days=BACKFILL_CUTOFF_YEARS * 365)

        offset = self._backfill_offset

        print(f"[{self.name}] 歷史回溯: offset={offset}, 截止 {cutoff.strftime('%Y-%m-%d')}")
        launches, total = self._fetch_previous_launches(offset)

        if not launches:
            print(f"[{self.name}] 歷史回溯完成（無更多資料）")
            self._backfill_complete = True
            return []

        # 檢查最舊的一筆是否超過截止日期
        oldest = launches[-1]
        oldest_net = oldest.get('net', '')
        if oldest_net:
            try:
                oldest_date = datetime.fromisoformat(oldest_net.replace('Z', '+00:00'))
                if oldest_date < cutoff:
                    # 過濾掉超過截止日期的
                    launches = [l for l in launches if l.get('net') and
                                datetime.fromisoformat(l['net'].replace('Z', '+00:00')) >= cutoff]
                    print(f"[{self.name}] 已達 {BACKFILL_CUTOFF_YEARS} 年前，回溯完成")
                    self._backfill_complete = True
            except (ValueError, TypeError):
                pass

        print(f"[{self.name}]   歷史: {len(launches)} 筆 (offset {offset}/{total})")

        # 更新 offset
        self._backfill_offset = offset + 100

        return launches

    def collect(self) -> dict:
        """每次只做一個 API call，不 sleep，不阻塞其他收集器。

        狀態機輪轉：upcoming → backfill → pads → events → upcoming → ...
        由排程間隔（LAUNCH_INTERVAL，建議 5 分鐘）控制頻率。
        """
        fetch_time = datetime.now(timezone.utc)
        all_launches = []
        all_pads = []
        all_events = []
        task = self._next_task

        if task == 'upcoming':
            all_launches = self._fetch_upcoming_launches()
            # 下一步：回溯歷史（如果還沒完成）或 events
            self._next_task = 'backfill' if not self._backfill_complete else 'events'

        elif task == 'backfill':
            all_launches = self._backfill_history()
            # 下一步：pads（如果還沒同步）或 events
            if not self._pads_synced:
                self._next_task = 'pads'
            else:
                self._next_task = 'events'

        elif task == 'pads':
            # pads 有多頁，付費 token 一次抓完；免費一次只抓一頁
            if config.LAUNCH_API_TOKEN:
                all_pads = self._fetch_pads()
                self._pads_synced = True
            else:
                all_pads = self._fetch_pads_page()
            self._next_task = 'events'

        elif task == 'events':
            all_events = self._fetch_upcoming_events()
            # 回到 upcoming，完成一輪
            self._next_task = 'upcoming'

        # 統計
        print(f"[{self.name}] task={task}: "
              f"{len(all_launches)} launches, {len(all_pads)} pads, {len(all_events)} events"
              f"{' | backfill: ' + ('完成' if self._backfill_complete else f'offset={self._backfill_offset}') if task == 'backfill' else ''}")

        return {
            'fetch_time': fetch_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'task': task,
            'launch_count': len(all_launches),
            'pad_count': len(all_pads),
            'event_count': len(all_events),
            'backfill_complete': self._backfill_complete,
            'data': {
                'launches': all_launches,
                'pads': all_pads,
                'events': all_events,
            },
        }

    def _fetch_pads_page(self) -> list[dict]:
        """免費模式：每次只抓一頁 pads（100 個），多次呼叫完成全部"""
        offset = getattr(self, '_pads_offset', 0)
        print(f"[{self.name}] 抓取發射台 (offset={offset})...")
        result = self._api_get('/pad/', {
            'limit': 100,
            'offset': offset,
            'format': 'json',
        })
        pads = [_parse_pad(r) for r in result.get('results', [])]
        if not result.get('next'):
            self._pads_synced = True
            self._pads_offset = 0
            print(f"[{self.name}]   發射台同步完成（本頁 {len(pads)} 個）")
        else:
            self._pads_offset = offset + 100
            print(f"[{self.name}]   發射台: {len(pads)} 個 (offset {offset}, 還有下一頁)")
        return pads
