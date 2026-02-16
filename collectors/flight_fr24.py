"""
FlightRadar24 台灣機場航班軌跡收集器

使用非官方 FlightRadarAPI 套件，每 5 分鐘掃描台灣機場的已降落/已起飛航班，
抓取完整飛行軌跡（trail）。

運作邏輯：
1. 呼叫 get_airport_details 取得各機場的 arrivals/departures
2. 篩選狀態為 "Landed" 且有 flight ID 的航班
3. 比對快取，跳過已抓過的 flight ID
4. 對新航班呼叫 clickhandler API 取得完整 trail
5. 累積整天資料，每次 collect 輸出當日所有已收集航班

資料來源：FlightRadar24（非官方，僅供教育用途）
"""

import time as time_module
from datetime import datetime

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

    # 台灣民航機場 ICAO 代碼（17 個，對應 taipei-gis-analytics 機場資料）
    AIRPORTS = {
        # 大型國際機場
        "RCTP": "桃園國際",
        "RCSS": "松山",
        "RCKH": "高雄小港",
        "RCMQ": "台中",
        "RCNN": "台南",
        "RCYU": "花蓮",
        # 中型機場
        "RCBS": "金門",
        "RCFN": "台東",
        "RCQC": "澎湖馬公",
        "RCFG": "馬祖南竿",
        "RCMT": "馬祖北竿",
        "RCLY": "蘭嶼",
        "RCKU": "嘉義",
        "RCKW": "恆春",
        # 小型機場
        "RCGI": "綠島",
        "RCCM": "七美",
        "RCWA": "望安",
    }

    def __init__(self):
        super().__init__()
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        })
        # 當日已收集的 flight ID → 航班資料（記憶體快取，跨次 collect 保留）
        self._collected: dict[str, dict] = {}
        self._today: str = ""

    def _reset_if_new_day(self):
        """跨日時清空快取"""
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._today:
            self._collected = {}
            self._today = today

    def _get_airport_flights(self, icao: str) -> list[dict]:
        """從機場頁面取得已降落/已起飛的航班清單"""
        results = []

        for direction in ["arrivals", "departures"]:
            try:
                # 使用 FlightRadar24API 套件的底層 API
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

                    # 只取已降落且有 ID 的（確保 trail 完整）
                    if not fr24_id:
                        continue
                    if "Landed" not in status_text:
                        continue

                    # 提取機場資訊
                    airport_info = flight.get("airport", {})
                    origin = airport_info.get("origin", {})
                    dest = airport_info.get("destination", {})

                    time_info = flight.get("time", {})
                    real_time = time_info.get("real", {})
                    sched_time = time_info.get("scheduled", {})

                    aircraft = flight.get("aircraft", {})

                    results.append({
                        "fr24_id": fr24_id,
                        "callsign": ident.get("number", {}).get("default", ""),
                        "status": status_text,
                        "origin_icao": (origin.get("code", {}).get("icao") or "") if origin else "",
                        "origin_iata": (origin.get("code", {}).get("iata") or "") if origin else "",
                        "dest_icao": (dest.get("code", {}).get("icao") or "") if dest else "",
                        "dest_iata": (dest.get("code", {}).get("iata") or "") if dest else "",
                        "dep_time": real_time.get("departure") or sched_time.get("departure"),
                        "arr_time": real_time.get("arrival") or sched_time.get("arrival"),
                        "aircraft_type": aircraft.get("model", {}).get("code", "") if aircraft else "",
                        "registration": aircraft.get("registration", "") if aircraft else "",
                    })

            except Exception as e:
                print(f"   ⚠ {icao} {direction} 錯誤: {e}")

            time_module.sleep(1)  # 避免請求過快

        return results

    def _get_flight_trail(self, fr24_id: str) -> list:
        """取得航班完整軌跡"""
        try:
            resp = self._session.get(
                self.FR24_DETAIL_URL,
                params={"flight": fr24_id},
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

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

        except Exception as e:
            print(f"   ⚠ trail {fr24_id} 錯誤: {e}")
            return []

    def collect(self) -> dict:
        """收集台灣機場已完成航班的軌跡"""
        fetch_time = datetime.now()
        self._reset_if_new_day()

        airports = config.FLIGHT_FR24_AIRPORTS
        new_flights = 0
        skipped = 0

        for icao, name in self.AIRPORTS.items():
            if icao not in airports:
                continue

            print(f"   掃描 {name}({icao})...")
            flights = self._get_airport_flights(icao)

            for f in flights:
                fid = f["fr24_id"]

                # 已收集過 → 跳過
                if fid in self._collected:
                    skipped += 1
                    continue

                # 抓 trail
                print(f"   → {f['callsign']} ({f['origin_iata']}→{f['dest_iata']}) "
                      f"[{f['status']}]")
                trail = self._get_flight_trail(fid)

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

                time_module.sleep(config.FLIGHT_FR24_TRAIL_DELAY)

        # 整理輸出
        all_flights = list(self._collected.values())
        with_trail = sum(1 for f in all_flights if f["path"])

        print(f"   ✓ 新增 {new_flights} 筆, 跳過 {skipped} 筆, "
              f"累計 {len(all_flights)} 筆 ({with_trail} 筆有軌跡)")

        return {
            "fetch_time": fetch_time.isoformat(),
            "date": self._today,
            "flight_count": len(all_flights),
            "with_trail": with_trail,
            "new_this_round": new_flights,
            "skipped_this_round": skipped,
            "airports_scanned": [icao for icao in self.AIRPORTS if icao in airports],
            "data": all_flights,
        }
