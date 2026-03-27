"""
Mini Taipei 每日時刻表發布任務

讀取 rail_timetable 收集器存放的 TDX 原始資料，
轉換為 Mini Taipei 前端格式，上傳到 S3。

S3 路徑結構:
  {S3_PREFIX}/tra/daily/{YYYY-MM-DD}.json
  {S3_PREFIX}/tra/index.json
  {S3_PREFIX}/thsr/daily/{YYYY-MM-DD}.json
  {S3_PREFIX}/thsr/index.json
  {S3_PREFIX}/tra/coverage/{YYYY-MM-DD}.json

需要的額外資料:
  {S3_PREFIX}/tra/od_station_progress.json  (手動上傳一次)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import config
from storage.local import LocalStorage
from storage.s3 import S3Storage

logger = logging.getLogger(__name__)

# ============================================================
# TRA 站 ID 對照表: TDX 新 ID -> 軌道舊 ID
# ============================================================
STATION_ID_MAPPING = {
    '3250': '3240',  # 潭子
    '3260': '3243',  # 頭家厝
    '3270': '3245',  # 松竹
    '3280': '3247',  # 太原
    '3290': '3249',  # 精武
    '3340': '3330',  # 新烏日 -> 烏日
    '3350': '3330',  # 成功
}

# ============================================================
# 車種代碼對照表
# ============================================================
TDX_TRAIN_TYPE_MAPPING = {
    '普悠瑪(普悠瑪)': 'PP',
    '太魯閣(太魯閣)': 'TZ',
    '自強(3000)(EMU3000 型電車)': 'TC',
    '自強(推拉式自強號且無自行車車廂)': 'TC-PP',
    '自強(推拉式自強號且有自行車車廂)': 'TC-PP',
    '自強(DMU3100 型柴聯)': 'TC-DMU',
    '自強(商務專開列車)': 'TC',
    '莒光(有身障座位)': 'CG',
    '莒光(無身障座位)': 'CG',
    '區間快': 'CK',
    '區間': 'LC',
    '區間(專開列車)': 'LC',
    '普快(專開列車)': 'LC',
}

# 高鐵車站線序 (南港->左營)
THSR_SOUTHBOUND_STATIONS = [
    "0990", "1000", "1010", "1020", "1030",
    "1035", "1040", "1043", "1047", "1050", "1060", "1070"
]
THSR_NORTHBOUND_STATIONS = list(reversed(THSR_SOUTHBOUND_STATIONS))


def normalize_station_id(station_id: str) -> str:
    return STATION_ID_MAPPING.get(station_id, station_id)


def get_train_type_code(tdx_train_type: str) -> str:
    return TDX_TRAIN_TYPE_MAPPING.get(tdx_train_type, 'OTHER')


def time_to_seconds(time_str: str) -> int:
    try:
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2]) if len(parts) > 2 else 0
        return hours * 3600 + minutes * 60 + seconds
    except Exception:
        return 0


# ============================================================
# TRA 轉換邏輯
# ============================================================

def build_track_index(od_progress: Dict) -> Dict[Tuple[str, str], List[str]]:
    index = defaultdict(list)
    for track_id, stations in od_progress.items():
        if len(stations) < 2:
            continue
        sorted_stations = sorted(stations.items(), key=lambda x: x[1])
        origin = sorted_stations[0][0]
        destination = sorted_stations[-1][0]
        index[(origin, destination)].append(track_id)
    return dict(index)


def find_matching_track(
    origin_id: str, dest_id: str, stop_ids: List[str],
    track_index: Dict, od_progress: Dict
) -> Optional[str]:
    origin_n = normalize_station_id(origin_id)
    dest_n = normalize_station_id(dest_id)
    stops_n = [normalize_station_id(s) for s in stop_ids]
    stops_set = set(stops_n)

    def score_track(track_id: str) -> Tuple[float, float, int, str]:
        track_stations = od_progress[track_id]
        track_set = set(track_stations.keys())
        coverage = len(stops_set & track_set)
        coverage_ratio = coverage / len(stops_n) if stops_n else 0

        prev_prog = -1.0
        forward = backward = 0
        for sid in stops_n:
            if sid in track_stations:
                prog = track_stations[sid]
                if prev_prog >= 0:
                    if prog > prev_prog:
                        forward += 1
                    elif prog < prev_prog:
                        backward += 1
                prev_prog = prog
        total_pairs = forward + backward
        backward_ratio = backward / total_pairs if total_pairs > 0 else 0

        if track_id.startswith('OD-'):
            type_priority = 0
        elif track_id.startswith('SP-'):
            type_priority = 1
        elif track_id.startswith('BB-'):
            type_priority = 2
        else:
            type_priority = 3

        return (backward_ratio, -coverage_ratio, type_priority, track_id)

    # 精確匹配
    key = (origin_n, dest_n)
    if key in track_index:
        return min(track_index[key], key=score_track)

    # 模糊匹配
    candidates = []
    for track_id, stations in od_progress.items():
        if origin_n in stations and dest_n in stations:
            if stations[origin_n] < stations[dest_n]:
                candidates.append(track_id)

    if candidates:
        return min(candidates, key=score_track)
    return None


def convert_tra_train(train: Dict, track_index: Dict, od_progress: Dict) -> Optional[Dict]:
    train_info = train.get('TrainInfo', train.get('DailyTrainInfo', {}))
    stops = train.get('StopTimes', [])

    if not stops or len(stops) < 2:
        return None

    train_no = train_info.get('TrainNo', '')
    train_type_name = train_info.get('TrainTypeName', {}).get('Zh_tw', '區間')
    train_type_code = get_train_type_code(train_type_name)

    origin_id = stops[0]['StationID']
    dest_id = stops[-1]['StationID']
    origin_name = stops[0].get('StationName', {}).get('Zh_tw', '')
    dest_name = stops[-1].get('StationName', {}).get('Zh_tw', '')
    stop_ids = [s['StationID'] for s in stops]

    track_id = find_matching_track(origin_id, dest_id, stop_ids, track_index, od_progress)
    if not track_id:
        return None

    departure_time = stops[0].get('DepartureTime', '00:00')
    if len(departure_time.split(':')) == 2:
        departure_time += ':00'
    first_departure = time_to_seconds(departure_time)

    track_station_set = set(od_progress.get(track_id, {}).keys())

    converted_stations = []
    for i, stop in enumerate(stops):
        normalized_sid = normalize_station_id(stop['StationID'])

        arr_time = stop.get('ArrivalTime', departure_time)
        dep_time = stop.get('DepartureTime', arr_time)
        if len(arr_time.split(':')) == 2:
            arr_time += ':00'
        if len(dep_time.split(':')) == 2:
            dep_time += ':00'

        arrival_sec = time_to_seconds(arr_time) - first_departure
        departure_sec = time_to_seconds(dep_time) - first_departure

        if arrival_sec < 0:
            arrival_sec += 86400
        if departure_sec < 0:
            departure_sec += 86400
        if i == 0:
            arrival_sec = departure_sec = 0

        if normalized_sid not in track_station_set:
            continue

        converted_stations.append({
            'station_id': normalized_sid,
            'arrival': arrival_sec,
            'departure': departure_sec,
        })

    # 去重
    seen_ids = set()
    deduped = []
    for st in converted_stations:
        if st['station_id'] not in seen_ids:
            seen_ids.add(st['station_id'])
            deduped.append(st)
    converted_stations = deduped

    if len(converted_stations) < 2:
        return None

    return {
        'train_id': f"{train_type_code}-{train_no}",
        'train_no': train_no,
        'train_type': train_type_name,
        'train_type_code': train_type_code,
        'departure_time': departure_time,
        'od_track_id': track_id,
        'origin_station': origin_name,
        'destination_station': dest_name,
        'total_travel_time': converted_stations[-1]['arrival'],
        'stations': converted_stations,
    }


def convert_tra_timetable(raw_data: list, date: str, track_index: Dict, od_progress: Dict) -> dict:
    """將 TDX TRA 原始資料轉換為 Mini Taipei 格式，含覆蓋率分析"""
    converted = []
    failed = []
    uncovered = []

    for train in raw_data:
        result = convert_tra_train(train, track_index, od_progress)
        if result:
            converted.append(result)
        else:
            train_info = train.get('TrainInfo', train.get('DailyTrainInfo', {}))
            stops = train.get('StopTimes', [])
            origin = stops[0].get('StationName', {}).get('Zh_tw', '') if stops else ''
            dest = stops[-1].get('StationName', {}).get('Zh_tw', '') if stops else ''
            origin_id = stops[0]['StationID'] if stops else ''
            dest_id = stops[-1]['StationID'] if stops else ''
            train_no = train_info.get('TrainNo', '')

            failed_info = {
                'train_no': train_no,
                'origin': origin,
                'destination': dest,
                'origin_id': origin_id,
                'destination_id': dest_id,
            }
            failed.append(failed_info)

            # 嘗試分析為何匹配失敗
            origin_n = normalize_station_id(origin_id)
            dest_n = normalize_station_id(dest_id)
            has_origin = any(origin_n in s for s in od_progress.values())
            has_dest = any(dest_n in s for s in od_progress.values())
            uncovered.append({
                **failed_info,
                'origin_in_tracks': has_origin,
                'destination_in_tracks': has_dest,
                'fallback_reason': 'no_matching_od_track',
            })

    output = {
        'metadata': {
            'title': f'TRA 每日時刻表 {date}',
            'date': date,
            'total_trains': len(converted),
            'failed': len(failed),
            'generated_at': datetime.now().isoformat(),
            'source': 'TDX DailyTrainTimetable v3',
        },
        'schedules': converted,
    }

    coverage = {
        'date': date,
        'total_trains': len(converted) + len(failed),
        'covered': len(converted),
        'uncovered': len(failed),
        'coverage_ratio': len(converted) / (len(converted) + len(failed)) if (converted or failed) else 0,
        'uncovered_trains': uncovered[:50],  # 最多 50 筆
    }

    # 車種統計
    type_counts = defaultdict(int)
    for t in converted:
        type_counts[t['train_type_code']] += 1
    coverage['by_type'] = dict(type_counts)

    return output, coverage


# ============================================================
# THSR 轉換邏輯
# ============================================================

def convert_thsr_timetable(raw_data: list, date: str) -> dict:
    departures_0 = []  # 南下
    departures_1 = []  # 北上
    skipped = []

    for train in raw_data:
        info = train.get('DailyTrainInfo', {})
        stop_times = train.get('StopTimes', [])
        direction = info.get('Direction')
        train_no = info.get('TrainNo', '')

        if not stop_times:
            skipped.append({'train_no': train_no, 'reason': 'no_stops'})
            continue

        first_stop = stop_times[0]
        departure_time = first_stop['DepartureTime']
        if len(departure_time) == 5:
            departure_time += ":00"
        base_seconds = time_to_seconds(first_stop['DepartureTime'])

        stations = []
        for stop in stop_times:
            arrival_sec = time_to_seconds(stop['ArrivalTime']) - base_seconds
            departure_sec = time_to_seconds(stop['DepartureTime']) - base_seconds
            if arrival_sec < 0:
                arrival_sec += 86400
            if departure_sec < 0:
                departure_sec += 86400
            stations.append({
                "station_id": stop['StationID'],
                "arrival": arrival_sec,
                "departure": departure_sec,
            })

        total_travel_time = stations[-1]['arrival'] if stations else 0

        departure = {
            "departure_time": departure_time,
            "train_id": f"THSR-{train_no}",
            "stations": stations,
            "total_travel_time": total_travel_time,
        }

        if direction == 0:
            departures_0.append(departure)
        elif direction == 1:
            departures_1.append(departure)
        else:
            skipped.append({'train_no': train_no, 'reason': f'unknown_direction_{direction}'})

    departures_0.sort(key=lambda d: d['departure_time'])
    departures_1.sort(key=lambda d: d['departure_time'])

    return {
        "THSR-1-0": {
            "track_id": "THSR-1-0",
            "route_id": "THSR",
            "name": "台灣高鐵 南下",
            "origin": "南港",
            "destination": "左營",
            "stations": THSR_SOUTHBOUND_STATIONS,
            "travel_time_minutes": 105,
            "dwell_time_seconds": 120,
            "is_weekday": True,
            "departure_count": len(departures_0),
            "departures": departures_0,
        },
        "THSR-1-1": {
            "track_id": "THSR-1-1",
            "route_id": "THSR",
            "name": "台灣高鐵 北上",
            "origin": "左營",
            "destination": "南港",
            "stations": THSR_NORTHBOUND_STATIONS,
            "travel_time_minutes": 105,
            "dwell_time_seconds": 120,
            "is_weekday": True,
            "departure_count": len(departures_1),
            "departures": departures_1,
        },
        "_metadata": {
            "date": date,
            "total_trains": len(departures_0) + len(departures_1),
            "southbound": len(departures_0),
            "northbound": len(departures_1),
            "skipped": len(skipped),
            "generated_at": datetime.now().isoformat(),
            "source": "TDX DailyTimetable",
        }
    }


# ============================================================
# 主任務
# ============================================================

class MiniTaipeiPublishTask:
    """Mini Taipei 每日時刻表發布任務"""

    def __init__(self):
        self.local_storage = LocalStorage()
        self.s3_storage = S3Storage()
        self.s3_prefix = getattr(config, 'MINI_TAIPEI_S3_PREFIX', 'mini-taipei')
        self._od_progress = None
        self._track_index = None

    def _load_od_progress(self) -> Dict:
        """載入 OD station progress (從 S3 cache 或本地)"""
        if self._od_progress is not None:
            return self._od_progress

        # 嘗試從 S3 載入
        s3_key = f"{self.s3_prefix}/tra/od_station_progress.json"
        try:
            data = self.s3_storage.get_json(s3_key)
            if data:
                self._od_progress = data
                self._track_index = build_track_index(data)
                logger.info(f"從 S3 載入 od_station_progress: {len(data)} 條軌道")
                return data
        except Exception as e:
            logger.warning(f"從 S3 載入 od_station_progress 失敗: {e}")

        # 嘗試從本地 cache 載入
        cache_path = config.LOCAL_DATA_DIR / 'mini_taipei_cache' / 'od_station_progress.json'
        if cache_path.exists():
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self._od_progress = data
            self._track_index = build_track_index(data)
            logger.info(f"從本地 cache 載入 od_station_progress: {len(data)} 條軌道")
            return data

        raise RuntimeError(
            f"找不到 od_station_progress.json。"
            f"請上傳到 S3: {s3_key}，"
            f"或放置到: {cache_path}"
        )

    def _upload_json_to_s3(self, s3_key: str, data: dict) -> bool:
        """上傳 JSON 到 S3 (public-read)"""
        try:
            body = json.dumps(data, ensure_ascii=False)
            self.s3_storage.s3.put_object(
                Bucket=self.s3_storage.bucket,
                Key=s3_key,
                Body=body.encode('utf-8'),
                ContentType='application/json',
            )
            return True
        except Exception as e:
            logger.error(f"上傳失敗 {s3_key}: {e}")
            return False

    def _update_index(self, system: str, new_date: str):
        """更新 S3 上的 index.json"""
        s3_key = f"{self.s3_prefix}/{system}/index.json"

        # 讀取現有 index
        existing = self.s3_storage.get_json(s3_key) or {'dates': []}
        dates = set(existing.get('dates', []))
        dates.add(new_date)

        # 只保留最近 90 天
        sorted_dates = sorted(dates, reverse=True)[:90]

        index_data = {
            'dates': sorted_dates,
            'latest': sorted_dates[0] if sorted_dates else None,
            'updated_at': datetime.now().isoformat(),
        }

        self._upload_json_to_s3(s3_key, index_data)
        logger.info(f"更新 {system} index.json: {len(sorted_dates)} 個日期")

    def _get_raw_timetable(self, date: str = None) -> dict:
        """取得 rail_timetable 的原始 TDX 資料"""
        # 嘗試讀取 latest.json
        data = self.local_storage.get_latest('rail_timetable')
        if data:
            stored_date = data.get('date', '')
            if date is None or stored_date == date:
                return data

        # 如果指定日期不在 latest，嘗試讀取特定日期目錄
        if date:
            try:
                parsed = datetime.strptime(date, '%Y-%m-%d')
                date_path = config.LOCAL_DATA_DIR / 'rail_timetable' / parsed.strftime('%Y/%m/%d')
                if date_path.exists():
                    # 取最新的檔案
                    json_files = sorted(date_path.glob('rail_timetable_*.json'), reverse=True)
                    if json_files:
                        with open(json_files[0], 'r', encoding='utf-8') as f:
                            return json.load(f)
            except Exception as e:
                logger.warning(f"讀取 {date} 原始資料失敗: {e}")

        return data  # fallback 到 latest

    def run(self, date: str = None) -> dict:
        """執行發布任務

        Args:
            date: 指定日期 (YYYY-MM-DD)，None 表示使用最新資料

        Returns:
            dict: 發布結果
        """
        start_time = datetime.now()
        results = {
            'date': None,
            'tra': {'status': 'skipped'},
            'thsr': {'status': 'skipped'},
            'coverage': None,
        }

        try:
            # 1. 取得原始資料
            raw_data = self._get_raw_timetable(date)
            if not raw_data:
                logger.error("找不到 rail_timetable 原始資料")
                results['error'] = 'no_raw_data'
                return results

            actual_date = raw_data.get('date', datetime.now().strftime('%Y-%m-%d'))
            results['date'] = actual_date
            print(f"   📅 發布 Mini Taipei 時刻表: {actual_date}")

            # 2. 轉換 TRA
            tra_raw = raw_data.get('data', {}).get('tra', {}).get('data', [])
            if tra_raw:
                try:
                    od_progress = self._load_od_progress()
                    track_index = self._track_index

                    tra_output, tra_coverage = convert_tra_timetable(
                        tra_raw, actual_date, track_index, od_progress
                    )

                    # 上傳轉換後的時刻表
                    tra_s3_key = f"{self.s3_prefix}/tra/daily/{actual_date}.json"
                    if self._upload_json_to_s3(tra_s3_key, tra_output):
                        self._update_index('tra', actual_date)
                        results['tra'] = {
                            'status': 'success',
                            'trains': tra_output['metadata']['total_trains'],
                            'failed': tra_output['metadata']['failed'],
                        }
                        print(f"   ✓ TRA: {tra_output['metadata']['total_trains']} 班 "
                              f"(失敗 {tra_output['metadata']['failed']})")

                    # 上傳覆蓋率報告
                    coverage_key = f"{self.s3_prefix}/tra/coverage/{actual_date}.json"
                    self._upload_json_to_s3(coverage_key, tra_coverage)
                    results['coverage'] = tra_coverage

                    # 覆蓋率低於 95% 時警告
                    if tra_coverage['coverage_ratio'] < 0.95:
                        logger.warning(
                            f"TRA 覆蓋率偏低: {tra_coverage['coverage_ratio']:.1%} "
                            f"({tra_coverage['uncovered']} 班未覆蓋)"
                        )

                except Exception as e:
                    logger.error(f"TRA 轉換失敗: {e}", exc_info=True)
                    results['tra'] = {'status': 'error', 'error': str(e)}
            else:
                print("   ⚠️  TRA 原始資料為空")

            # 3. 轉換 THSR
            thsr_raw = raw_data.get('data', {}).get('thsr', {}).get('data', [])
            if thsr_raw:
                try:
                    thsr_output = convert_thsr_timetable(thsr_raw, actual_date)
                    thsr_s3_key = f"{self.s3_prefix}/thsr/daily/{actual_date}.json"
                    if self._upload_json_to_s3(thsr_s3_key, thsr_output):
                        self._update_index('thsr', actual_date)
                        meta = thsr_output['_metadata']
                        results['thsr'] = {
                            'status': 'success',
                            'trains': meta['total_trains'],
                            'southbound': meta['southbound'],
                            'northbound': meta['northbound'],
                        }
                        print(f"   ✓ THSR: {meta['total_trains']} 班 "
                              f"(南下 {meta['southbound']}, 北上 {meta['northbound']})")
                except Exception as e:
                    logger.error(f"THSR 轉換失敗: {e}", exc_info=True)
                    results['thsr'] = {'status': 'error', 'error': str(e)}
            else:
                print("   ⚠️  THSR 原始資料為空")

            duration = (datetime.now() - start_time).total_seconds()
            results['duration_seconds'] = duration
            print(f"   ✓ 完成 ({duration:.1f}s)")

        except Exception as e:
            logger.error(f"Mini Taipei 發布任務失敗: {e}", exc_info=True)
            results['error'] = str(e)

        return results
