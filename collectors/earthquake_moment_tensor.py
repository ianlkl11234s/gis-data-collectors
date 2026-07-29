"""
震源機制解收集器（中研院 AutoBATS）

資料源：中研院地球所 BATS 寬頻地震網 AutoBATS
    https://tecdc.earth.sinica.edu.tw/FM/AutoBATS/php/getEventData.php
        ?date=YYYY-MM-DD&time=HH:MM:SS&type={1|0}

⚠️ 已驗證的坑（2026-07-29 實測）：
  1. **沒有清單端點**：latest.php / cmtquery.php 都 404，只能一次查一個事件。
     → 本 collector 先用 CWA E-A0015-001 + E-A0016-001 取近期事件清單，再逐一查。
  2. **時間是 UTC**：CWA OriginTime 2026-07-27T10:14:49+08:00 的地震，
     要查 date=2026-07-27&time=02:14:49。
  3. **秒數必須完全吻合**：±1 秒就查無 → 仍保留 ±1/±2 fallback 兜上游取整。
  4. **查無不是 404**：一律 HTTP 200，回一份 147 bytes 的空殼 JSON
     （只有 ISO/Mrr…/ball，全部 0，ball 路徑長 './data/Quick//.210.../.png'）
     → 判空條件是「缺 date 或 lat」，不能靠 status code。
  5. type=1 落在 ./data/Quick/（自動快解，發震後數分鐘就有）；
     type=0 落在 ./data/Final/（人工修訂解，實測近期事件全部尚未產出）。
     本檔以 solution_type 'R'=Quick(type=1) / 'A'=Final(type=0) 入庫。
  6. 只有規模較大的事件會有解：實測 M5.6 / M4.8 / M4.0 顯著地震有 Quick 解，
     小區域 M4.4 / M3.7 / M3.0 查無 → 「查無」是常態，不是錯誤。

寫入 live.earthquake_moment_tensor，UNIQUE(origin_time_utc, solution_type)，
UPSERT DO UPDATE（Final 解晚到要能覆蓋既有列）。

節流：對這台學術站台每輪最多 MAX_EVENTS 個事件、每次請求間隔 REQUEST_GAP 秒，
且對「查無」的 (事件, 解型) 做指數退避（記憶體內，重啟即重來，fail-open）。
"""

import json
import time
from datetime import datetime, timedelta, timezone

import requests

import config
from .base import BaseCollector, TAIPEI_TZ
from .earthquake_common import as_list, make_event_id, safe_float, safe_int

AUTOBATS_URL = "https://tecdc.earth.sinica.edu.tw/FM/AutoBATS/php/getEventData.php"
AUTOBATS_BASE = "https://tecdc.earth.sinica.edu.tw/FM/AutoBATS/"

# type 參數 → 入庫的 solution_type
SOLUTION_TYPES = (
    (1, 'R'),   # ./data/Quick/  自動快解
    (0, 'A'),   # ./data/Final/  人工修訂解
)

# 上游若把秒數取整，往前後各試 2 秒
SECOND_OFFSETS = (0, 1, -1, 2, -2)

# 矩張量與主軸分量：整包塞進 tensor JSONB
TENSOR_KEYS = (
    'm11', 'm22', 'm33', 'm12', 'm13', 'm23',
    'Mrr', 'Mtt', 'Mff', 'Mrt', 'Mrf', 'Mtf',
    'exponent', 'Paz', 'Ppl', 'Baz', 'Bpl', 'Taz', 'Tpl',
    'CMTType', 'msec', 'opt',
)


class EarthquakeMomentTensorCollector(BaseCollector):
    """AutoBATS 震源機制解（斷層面解 + 矩張量）收集器"""

    name = "earthquake_moment_tensor"
    interval_minutes = config.EARTHQUAKE_MOMENT_TENSOR_INTERVAL

    COLLECT_TIMEOUT = 600

    # 只回頭查最近幾天的事件（更早的若沒解，等下次年度回填人工處理）
    RECENT_DAYS = 30
    # 每輪最多處理幾個事件（節流上限）
    MAX_EVENTS = 20
    # 每次 HTTP 請求之間的間隔（秒）
    REQUEST_GAP = 0.4
    # 查無時的退避：第 n 次失敗後隔 min(interval * 2^(n-1), MAX_BACKOFF_MIN) 分鐘再試
    MAX_BACKOFF_MIN = 720

    def __init__(self):
        super().__init__()
        self.api_key = config.CWA_API_KEY
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "GIS-DataCollectors/1.0 (earthquake-moment-tensor)",
            "Accept": "application/json",
            "Referer": AUTOBATS_BASE,
        })
        # (origin_time_utc_iso, solution_type) → {'count': n, 'next_try': monotonic}
        self._miss: dict = {}
        # 本進程已取得解的 key；用於「命中的秒數與 CWA OriginTime 差 ±1~2 秒」時，
        # DB 的 origin_time_utc 對不回 CWA 時間，光靠 _existing_solutions 會永遠重查
        self._solved: set = set()

        if not self.api_key:
            raise ValueError("CWA_API_KEY 未設定")

    # ------------------------------------------------------------------
    # 近期事件清單（來自 CWA）
    # ------------------------------------------------------------------
    def _fetch_cwa_events(self) -> list:
        """回 [{'event_id', 'origin_local': datetime, 'origin_utc': datetime, 'magnitude'}]"""
        events = {}
        cutoff = datetime.now(TAIPEI_TZ) - timedelta(days=self.RECENT_DAYS)

        for source_type, endpoint_id in (('significant', 'E-A0015-001'),
                                         ('local', 'E-A0016-001')):
            try:
                resp = self._session.get(
                    f"{config.CWA_API_BASE}/v1/rest/datastore/{endpoint_id}",
                    params={'Authorization': self.api_key, 'format': 'JSON', 'limit': 30},
                    timeout=config.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                payload = resp.json()
                if payload.get('success') != 'true':
                    raise ValueError(f"API 回傳失敗: {payload.get('success')}")
                records = as_list(payload.get('records', {}).get('Earthquake', []))
            except Exception as e:
                print(f"   [{source_type}] CWA 事件清單取得失敗: {e}")
                continue

            for eq in records:
                info = eq.get('EarthquakeInfo', {}) or {}
                origin_time = info.get('OriginTime', '')
                try:
                    origin_local = datetime.fromisoformat(origin_time)
                except ValueError:
                    continue
                if origin_local.tzinfo is None:
                    origin_local = origin_local.replace(tzinfo=TAIPEI_TZ)
                if origin_local < cutoff:
                    continue

                origin_utc = origin_local.astimezone(timezone.utc)
                key = origin_utc.isoformat()
                if key in events:
                    continue
                events[key] = {
                    'event_id': make_event_id(eq.get('EarthquakeNo'), origin_time, source_type),
                    'origin_local': origin_local,
                    'origin_utc': origin_utc,
                    'magnitude': safe_float(
                        (info.get('EarthquakeMagnitude', {}) or {}).get('MagnitudeValue')
                    ),
                }

        ordered = sorted(events.values(), key=lambda e: e['origin_utc'], reverse=True)
        return ordered[:self.MAX_EVENTS]

    # ------------------------------------------------------------------
    # 既有解 / 退避
    # ------------------------------------------------------------------
    def _existing_solutions(self, origin_utcs: list) -> set:
        """DB 已有的 (origin_time_utc iso, solution_type)"""
        if not origin_utcs or not self.supabase_writer:
            return set()
        try:
            with self.supabase_writer.with_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT origin_time_utc, solution_type "
                        "FROM live.earthquake_moment_tensor "
                        "WHERE origin_time_utc = ANY(%s::timestamptz[])",
                        (origin_utcs,),
                    )
                    return {
                        (row[0].astimezone(timezone.utc).isoformat(), row[1])
                        for row in cur.fetchall()
                    }
        except Exception as e:
            print(f"   [moment_tensor] 既有解查詢失敗（改為照常查詢上游）: {e}")
            return set()

    def _backoff_blocked(self, key) -> bool:
        state = self._miss.get(key)
        return bool(state and time.monotonic() < state['next_try'])

    def _record_miss(self, key) -> None:
        state = self._miss.get(key, {'count': 0, 'next_try': 0.0})
        state['count'] += 1
        wait_min = min(
            self.interval_minutes * (2 ** (state['count'] - 1)),
            self.MAX_BACKOFF_MIN,
        )
        state['next_try'] = time.monotonic() + wait_min * 60
        self._miss[key] = state

    # ------------------------------------------------------------------
    # AutoBATS 查詢
    # ------------------------------------------------------------------
    @staticmethod
    def _is_empty(data) -> bool:
        """查無解的空殼：只有張量欄位全 0、沒有 date / lat"""
        if not isinstance(data, dict):
            return True
        return not data.get('date') or not data.get('lat')

    def _query_autobats(self, origin_utc: datetime, type_param: int):
        """回 (payload dict, 實際命中的 UTC datetime) 或 (None, None)"""
        for offset in SECOND_OFFSETS:
            probe = origin_utc + timedelta(seconds=offset)
            try:
                resp = self._session.get(
                    AUTOBATS_URL,
                    params={
                        'date': probe.strftime('%Y-%m-%d'),
                        'time': probe.strftime('%H:%M:%S'),
                        'type': type_param,
                    },
                    timeout=config.REQUEST_TIMEOUT,
                )
                time.sleep(self.REQUEST_GAP)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as e:
                # 含 JSON decode 失敗（上游偶爾回錯誤字串）
                print(f"   [moment_tensor] {probe.isoformat()} type={type_param} 查詢異常: {e}")
                continue

            if not self._is_empty(data):
                return data, probe
        return None, None

    def _build_record(self, data: dict, event: dict, probe_utc: datetime,
                      solution_type: str) -> dict:
        lat = safe_float(data.get('lat'))
        lon = safe_float(data.get('lon'))
        tensor = {k: data.get(k) for k in TENSOR_KEYS if k in data}
        ball = data.get('ball') or ''
        beachball_url = (
            AUTOBATS_BASE + ball.lstrip('./') if ball.startswith('.') else (ball or None)
        )

        return {
            'origin_time_utc': probe_utc.isoformat(),
            'origin_time_local': event['origin_local'].isoformat(),
            'event_id': event['event_id'],
            'lat': lat,
            'lon': lon,
            'ml': safe_float(data.get('ML')),
            'mw': safe_float(data.get('Mw')),
            'm0': safe_float(data.get('M0')),
            'strike1': safe_float(data.get('strike1')),
            'dip1': safe_float(data.get('dip1')),
            'rake1': safe_float(data.get('slip1')),
            'strike2': safe_float(data.get('strike2')),
            'dip2': safe_float(data.get('dip2')),
            'rake2': safe_float(data.get('slip2')),
            'centroid_depth': safe_float(data.get('Centroid_depth')),
            'cwb_depth': safe_float(data.get('CWB_depth')),
            'clvd_pct': safe_float(data.get('CLVD')),
            'iso_pct': safe_float(data.get('ISO')),
            'misfit': safe_float(data.get('misfit')),
            'gap': safe_float(data.get('gap')),
            'nsta': safe_int(data.get('nsta')),
            'quality': data.get('QC') or None,
            'tensor': json.dumps(tensor, ensure_ascii=False),
            'solution_type': solution_type,
            'beachball_url': beachball_url,
            'raw': json.dumps(data, ensure_ascii=False),
        }

    # ------------------------------------------------------------------
    def collect(self) -> dict:
        fetch_time = datetime.now(TAIPEI_TZ)

        events = self._fetch_cwa_events()
        if not events:
            print("   近期無事件可查")
            return {
                'fetch_time': fetch_time.isoformat(),
                'candidate_events': 0,
                'solution_count': 0,
                'data': [],
            }

        existing = self._existing_solutions([e['origin_utc'].isoformat() for e in events])

        records = []
        queried = 0
        for event in events:
            origin_iso = event['origin_utc'].isoformat()
            for type_param, solution_type in SOLUTION_TYPES:
                key = (origin_iso, solution_type)
                if key in existing or key in self._solved:
                    continue          # 已有解，不再查
                if self._backoff_blocked(key):
                    continue          # 近期查過沒有，退避中
                queried += 1
                data, probe = self._query_autobats(event['origin_utc'], type_param)
                if data is None:
                    self._record_miss(key)
                    continue
                self._miss.pop(key, None)
                self._solved.add(key)
                records.append(self._build_record(data, event, probe, solution_type))

        print(f"   候選事件 {len(events)} 個 / 上游查詢 {queried} 次 / 取得解 {len(records)} 筆")

        return {
            'fetch_time': fetch_time.isoformat(),
            'candidate_events': len(events),
            'queried': queried,
            'solution_count': len(records),
            'data': records,
        }
