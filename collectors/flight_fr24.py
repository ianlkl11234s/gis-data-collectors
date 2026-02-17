"""
FlightRadar24 台灣機場航班軌跡收集器

使用非官方 FlightRadarAPI 套件，每 5 分鐘掃描台灣機場的已降落/已起飛航班，
抓取完整飛行軌跡（trail）。

運作邏輯：
1. 掃描 arrivals：篩選 "Landed" 航班，立即抓 trail（抵達台灣的航班）
2. 掃描 departures：記錄已出發航班到 pending 待追蹤清單
3. 每輪輪詢檢查 pending 航班，透過 clickhandler API 確認是否已降落
4. 已降落的 pending 航班抓取完整 trail，移入 collected
5. 累積整天資料，每次 collect 輸出當日所有已收集航班

資料來源：FlightRadar24（非官方，僅供教育用途）
"""

import json
import random
import time as time_module
from datetime import datetime
from pathlib import Path

import requests

import config
from .base import BaseCollector


class FlightFR24Collector(BaseCollector):
    """FlightRadar24 台灣機場航班軌跡收集器"""

    name = "flight_fr24"
    interval_minutes = config.FLIGHT_FR24_INTERVAL

    # FlightRadar24 API 端點
    FR24_AIRPORT_URL = "https://api.flightradar24.com/common/v1/airport.json"
    FR24_DETAIL_URL = "https://data-live.flightradar24.com/clickhandler/"

    # 台灣民航機場 ICAO → (名稱, IATA)
    AIRPORTS = {
        "RCTP": ("桃園國際", "TPE"),
        "RCSS": ("松山", "TSA"),
        "RCKH": ("高雄小港", "KHH"),
        "RCMQ": ("台中", "RMQ"),
        "RCNN": ("台南", "TNN"),
        "RCYU": ("花蓮", "HUN"),
        "RCBS": ("金門", "KNH"),
        "RCFN": ("台東", "TTT"),
        "RCQC": ("澎湖馬公", "MZG"),
        "RCFG": ("馬祖南竿", "LZN"),
        "RCMT": ("馬祖北竿", "MFK"),
        "RCLY": ("蘭嶼", "KYD"),
        "RCKU": ("嘉義", "CYI"),
        "RCKW": ("恆春", "HCN"),
        "RCGI": ("綠島", "GNI"),
        "RCCM": ("七美", "CMJ"),
        "RCWA": ("望安", "WOT"),
    }

    # User-Agent 輪替池，模擬不同瀏覽器降低被封鎖風險
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # pending 航班最長追蹤時間（秒），超過即放棄
    PENDING_TIMEOUT = 24 * 3600  # 24 小時

    # 持久化檔案路徑
    PERSIST_DIR = config.LOCAL_DATA_DIR / "flight_fr24"
    PENDING_FILE = PERSIST_DIR / "_pending.json"
    COLLECTED_IDS_FILE = PERSIST_DIR / "_collected_ids.json"

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            'Accept': 'application/json',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        })
        # 當日已收集的 flight ID → 航班資料（記憶體快取，跨次 collect 保留）
        self._collected: dict[str, dict] = {}
        # 待追蹤的出發航班 flight ID → 航班基本資料 + 記錄時間
        self._pending: dict[str, dict] = {}
        self._today: str = ""
        # 從磁碟載入持久化資料
        self._load_persistent_data()

    def _rotate_ua(self):
        """隨機切換 User-Agent"""
        self._session.headers['User-Agent'] = random.choice(self.USER_AGENTS)

    def _sleep_jitter(self, base: float):
        """帶隨機抖動的等待，避免固定節奏被偵測"""
        jitter = base * random.uniform(0.5, 1.5)
        time_module.sleep(jitter)

    # === 持久化方法 ===

    def _load_persistent_data(self):
        """啟動時從磁碟載入 pending 和 collected IDs"""
        self.PERSIST_DIR.mkdir(parents=True, exist_ok=True)

        # 載入 pending
        if self.PENDING_FILE.exists():
            try:
                data = json.loads(self.PENDING_FILE.read_text(encoding="utf-8"))
                self._pending = data if isinstance(data, dict) else {}
                print(f"   💾 從磁碟載入 {len(self._pending)} 筆 pending 航班")
            except Exception as e:
                print(f"   ⚠ 載入 pending 失敗: {e}")
                self._pending = {}

        # 載入 collected IDs（僅 ID，用於去重）
        if self.COLLECTED_IDS_FILE.exists():
            try:
                data = json.loads(self.COLLECTED_IDS_FILE.read_text(encoding="utf-8"))
                today = datetime.now().strftime("%Y-%m-%d")
                # 只載入當天的 collected IDs
                if data.get("date") == today:
                    for fid in data.get("ids", []):
                        if fid not in self._collected:
                            self._collected[fid] = {}  # 佔位，僅做去重
                    self._today = today
                    print(f"   💾 從磁碟載入 {len(data.get('ids', []))} 筆 collected IDs")
            except Exception as e:
                print(f"   ⚠ 載入 collected IDs 失敗: {e}")

    def _save_pending(self):
        """將 pending 寫入磁碟"""
        try:
            self.PERSIST_DIR.mkdir(parents=True, exist_ok=True)
            self.PENDING_FILE.write_text(
                json.dumps(self._pending, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"   ⚠ 儲存 pending 失敗: {e}")

    def _save_collected_ids(self):
        """將當日 collected flight IDs 寫入磁碟（僅 ID，不含完整資料）"""
        try:
            self.PERSIST_DIR.mkdir(parents=True, exist_ok=True)
            self.COLLECTED_IDS_FILE.write_text(
                json.dumps({
                    "date": self._today,
                    "ids": list(self._collected.keys()),
                }, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"   ⚠ 儲存 collected IDs 失敗: {e}")

    def _reset_if_new_day(self):
        """跨日時清空當日 collected（pending 保留，由 PENDING_TIMEOUT 控制過期）"""
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._today:
            self._collected = {}
            # pending 不清除！跨日仍需追蹤未降落航班
            self._today = today

    def _get_airport_flights(self, icao: str) -> tuple[list[dict], list[dict]]:
        """
        從機場頁面取得航班清單。

        回傳 (landed_flights, departure_flights):
        - landed_flights: arrivals 中 Landed 的航班（可直接抓 trail）
        - departure_flights: departures 中已出發/排定的航班（放入 pending 追蹤）
        """
        landed = []
        departures = []
        airport_name, airport_iata = self.AIRPORTS.get(icao, ("", ""))

        for direction in ["arrivals", "departures"]:
            try:
                self._rotate_ua()
                from FlightRadar24 import FlightRadar24API
                fr_api = FlightRadar24API()
                details = fr_api.get_airport_details(
                    icao, page=1, flight_limit=100
                )
                plugin = details.get("airport", {}).get("pluginData", {})
                schedule_data = plugin.get("schedule", {}).get(direction, {})
                flights = schedule_data.get("data", [])

                for f in flights:
                    flight = f.get("flight", {})
                    ident = flight.get("identification", {})
                    status_text = flight.get("status", {}).get("text", "")
                    fr24_id = ident.get("id")

                    if not fr24_id:
                        continue

                    # 提取機場資訊
                    airport_info = flight.get("airport", {})
                    origin = airport_info.get("origin", {})
                    dest = airport_info.get("destination", {})

                    time_info = flight.get("time", {})
                    real_time = time_info.get("real", {})
                    sched_time = time_info.get("scheduled", {})

                    aircraft = flight.get("aircraft", {})

                    origin_icao = (origin.get("code", {}).get("icao") or "") if origin else ""
                    origin_iata = (origin.get("code", {}).get("iata") or "") if origin else ""
                    dest_icao = (dest.get("code", {}).get("icao") or "") if dest else ""
                    dest_iata = (dest.get("code", {}).get("iata") or "") if dest else ""

                    # arrivals 方向：FR24 省略 destination（就是本機場），補上
                    if direction == "arrivals" and not dest_icao:
                        dest_icao = icao
                        dest_iata = airport_iata

                    # departures 方向：FR24 省略 origin（就是本機場），補上
                    if direction == "departures" and not origin_icao:
                        origin_icao = icao
                        origin_iata = airport_iata

                    flight_info = {
                        "fr24_id": fr24_id,
                        "callsign": ident.get("number", {}).get("default", ""),
                        "status": status_text,
                        "origin_icao": origin_icao,
                        "origin_iata": origin_iata,
                        "dest_icao": dest_icao,
                        "dest_iata": dest_iata,
                        "dep_time": real_time.get("departure") or sched_time.get("departure"),
                        "arr_time": real_time.get("arrival") or sched_time.get("arrival"),
                        "aircraft_type": aircraft.get("model", {}).get("code", "") if aircraft else "",
                        "registration": aircraft.get("registration", "") if aircraft else "",
                    }

                    if direction == "arrivals":
                        if "Landed" in status_text:
                            landed.append(flight_info)
                    else:
                        # departures：記錄已出發或即將出發的航班
                        if "Departed" in status_text or "Estimated" in status_text:
                            departures.append(flight_info)

            except Exception as e:
                print(f"   ⚠ {icao} {direction} 錯誤: {e}")

            self._sleep_jitter(1.5)

        return landed, departures

    def _get_flight_detail(self, fr24_id: str) -> dict | None:
        """透過 clickhandler API 取得航班詳情（狀態 + trail）"""
        try:
            self._rotate_ua()
            resp = self._session.get(
                self.FR24_DETAIL_URL,
                params={"flight": fr24_id},
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"   ⚠ detail {fr24_id} 錯誤: {e}")
            return None

    def _extract_trail(self, data: dict) -> list:
        """從 clickhandler 回應中提取軌跡"""
        trail = data.get("trail", [])
        # FR24 trail 為倒序（最新→最舊），反轉為時間正序
        trail = list(reversed(trail))

        path = []
        for point in trail:
            lat = point.get("lat")
            lng = point.get("lng")
            if lat is not None and lng is not None:
                alt_ft = point.get("alt", 0)
                path.append([
                    lat,
                    lng,
                    round(alt_ft * 0.3048) if alt_ft else 0,  # ft → m
                    point.get("ts"),
                ])
        return path

    def _check_pending_flights(self) -> int:
        """檢查 pending 中的航班是否已降落，已降落則抓 trail 移入 collected"""
        if not self._pending:
            return 0

        now = time_module.time()
        completed = 0
        expired = []

        print(f"   追蹤中航班: {len(self._pending)} 筆")

        for fid, info in list(self._pending.items()):
            # 超時放棄
            if now - info["_tracked_since"] > self.PENDING_TIMEOUT:
                expired.append(fid)
                continue

            # 已經被 arrivals 收集過（其他機場的 arrivals 可能先抓到）
            if fid in self._collected:
                expired.append(fid)
                continue

            detail = self._get_flight_detail(fid)
            if not detail:
                self._sleep_jitter(2)
                continue

            # 檢查狀態
            status_text = detail.get("status", {}).get("text", "")

            if "Landed" in status_text:
                trail = self._extract_trail(detail)

                # 嘗試從 detail 補充目的地資訊
                dest_icao = info.get("dest_icao", "")
                dest_iata = info.get("dest_iata", "")
                if not dest_icao:
                    airport = detail.get("airport", {}).get("destination", {})
                    if airport:
                        dest_icao = airport.get("code", {}).get("icao", "")
                        dest_iata = airport.get("code", {}).get("iata", "")

                self._collected[fid] = {
                    "fr24_id": fid,
                    "callsign": info["callsign"],
                    "registration": info["registration"],
                    "aircraft_type": info["aircraft_type"],
                    "origin_icao": info["origin_icao"],
                    "origin_iata": info["origin_iata"],
                    "dest_icao": dest_icao,
                    "dest_iata": dest_iata,
                    "dep_time": info["dep_time"],
                    "arr_time": info["arr_time"],
                    "status": status_text,
                    "trail_points": len(trail),
                    "path": trail,
                }
                expired.append(fid)
                completed += 1
                print(f"   ✈ {info['callsign']} ({info['origin_iata']}→{dest_iata or '?'}) "
                      f"已降落，{len(trail)} 軌跡點")
            else:
                print(f"   ⏳ {info['callsign']} ({info['origin_iata']}→{info.get('dest_iata','?')}) "
                      f"仍在飛行 [{status_text}]")

            self._sleep_jitter(2)

        # 清理已完成或過期的
        for fid in expired:
            self._pending.pop(fid, None)

        if expired:
            self._save_pending()  # 有變動就存檔
            timed_out = len(expired) - completed
            if timed_out > 0:
                print(f"   ♻ 移除 {timed_out} 筆過期追蹤")

        return completed

    def collect(self) -> dict:
        """收集台灣機場已完成航班的軌跡"""
        fetch_time = datetime.now()
        self._reset_if_new_day()

        airports = config.FLIGHT_FR24_AIRPORTS
        new_flights = 0
        new_pending = 0
        skipped = 0

        # === 階段一：掃描各機場 ===
        for icao, (name, iata) in self.AIRPORTS.items():
            if icao not in airports:
                continue

            print(f"   掃描 {name}({icao})...")
            landed, departures = self._get_airport_flights(icao)

            # 處理已降落航班（arrivals）
            for f in landed:
                fid = f["fr24_id"]

                if fid in self._collected or fid in self._pending:
                    skipped += 1
                    continue

                self._rotate_ua()
                print(f"   → {f['callsign']} ({f['origin_iata']}→{f['dest_iata']}) "
                      f"[{f['status']}]")

                detail = self._get_flight_detail(fid)
                trail = self._extract_trail(detail) if detail else []

                self._collected[fid] = {
                    "fr24_id": fid,
                    "callsign": f["callsign"],
                    "registration": f["registration"],
                    "aircraft_type": f["aircraft_type"],
                    "origin_icao": f["origin_icao"],
                    "origin_iata": f["origin_iata"],
                    "dest_icao": f["dest_icao"],
                    "dest_iata": f["dest_iata"],
                    "dep_time": f["dep_time"],
                    "arr_time": f["arr_time"],
                    "status": f["status"],
                    "trail_points": len(trail),
                    "path": trail,
                }
                new_flights += 1
                self._sleep_jitter(config.FLIGHT_FR24_TRAIL_DELAY)

            # 處理出發航班（departures）→ 加入 pending 追蹤
            for f in departures:
                fid = f["fr24_id"]
                if fid in self._collected or fid in self._pending:
                    continue

                self._pending[fid] = {
                    **f,
                    "_tracked_since": time_module.time(),
                }
                new_pending += 1
                print(f"   📡 追蹤 {f['callsign']} ({f['origin_iata']}→{f['dest_iata'] or '?'}) "
                      f"[{f['status']}]")

        # 新增 pending 後存檔
        if new_pending > 0:
            self._save_pending()

        # === 階段二：檢查 pending 航班是否已降落 ===
        completed_pending = self._check_pending_flights()

        # === 持久化 ===
        self._save_collected_ids()

        # === 整理輸出 ===
        all_flights = list(self._collected.values())
        with_trail = sum(1 for f in all_flights if f.get("path"))

        print(f"   ✓ 新增 {new_flights} 筆(抵達), "
              f"新追蹤 {new_pending} 筆(出發), "
              f"追蹤完成 {completed_pending} 筆, "
              f"跳過 {skipped} 筆")
        print(f"   累計 {len(all_flights)} 筆 ({with_trail} 筆有軌跡), "
              f"追蹤中 {len(self._pending)} 筆")

        return {
            "fetch_time": fetch_time.isoformat(),
            "date": self._today,
            "flight_count": len(all_flights),
            "with_trail": with_trail,
            "new_this_round": new_flights,
            "pending_completed": completed_pending,
            "pending_tracking": len(self._pending),
            "skipped_this_round": skipped,
            "airports_scanned": [icao for icao in self.AIRPORTS if icao in airports],
            "data": all_flights,
        }
