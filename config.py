"""
共用設定模組

從環境變數讀取所有設定，提供預設值。
"""

import os
from pathlib import Path

# 載入 .env 檔案
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv 未安裝時略過

# ============================================================
# 環境偵測
# ============================================================

IS_PRODUCTION = os.getenv('ZEABUR') or os.getenv('PRODUCTION')
IS_DEBUG = os.getenv('DEBUG', '').lower() in ('true', '1', 'yes')

# 實例名稱（用於多實例部署時辨識來源）
INSTANCE_NAME = os.getenv('INSTANCE_NAME', '')

# ============================================================
# TDX API 設定
# ============================================================

TDX_APP_ID = os.getenv('TDX_APP_ID')
TDX_APP_KEY = os.getenv('TDX_APP_KEY')
TDX_AUTH_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
TDX_API_BASE = "https://tdx.transportdata.tw/api/basic"

# TDX 全域 rate limit (req/sec/金鑰)
# TDX 免費/專業方案多為 5 req/sec/金鑰，預設 4 留 1 req/sec buffer
# 所有 TDX collector 的 HTTP 請求（含 token refresh）都會共用此節流器
# 詳見 docs/TDX_RATE_LIMITING.md
TDX_RATE_LIMIT = float(os.getenv('TDX_RATE_LIMIT', '4'))

# ============================================================
# CWA 氣象局 API 設定
# ============================================================

CWA_API_KEY = os.getenv('CWA_API_KEY')
CWA_API_BASE = "https://opendata.cwa.gov.tw/api"
CWA_FILE_API_BASE = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi"

# ============================================================
# 環境部 MOENV API 設定 (空氣品質)
# ============================================================
# 公開 key 已發布於 data.gov.tw 各資料集頁面，專屬申請：https://data.moenv.gov.tw/
MOENV_API_KEY = os.getenv('MOENV_API_KEY')

# ============================================================
# 水利署 IoT 水資源物聯網 OAuth2（都市淹水感知器 USWG）
# ============================================================
# 註冊：https://iot.wra.gov.tw/SignUp.jsp
# Swagger: https://iot.wra.gov.tw/swagger/v1/swagger.json
IOW_CLIENT_ID     = os.getenv('IOW_CLIENT_ID')
IOW_CLIENT_SECRET = os.getenv('IOW_CLIENT_SECRET')

# ============================================================
# 儲存設定
# ============================================================

# S3 設定
S3_BUCKET = os.getenv('S3_BUCKET')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY') or os.getenv('AWS_ACCESS_KEY_ID')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY')
S3_REGION = os.getenv('S3_REGION', 'ap-southeast-2')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')  # 用於 MinIO 等相容服務

# 歸檔設定
ARCHIVE_ENABLED = os.getenv('ARCHIVE_ENABLED', 'true').lower() in ('true', '1', 'yes')
ARCHIVE_RETENTION_DAYS = int(os.getenv('ARCHIVE_RETENTION_DAYS', '7'))  # 本地保留天數（預設全域）
ARCHIVE_TIME = os.getenv('ARCHIVE_TIME', '03:00')  # 每日歸檔時間 (HH:MM)

# 單一 collector 的本地保留天數覆寫（留空則套用 ARCHIVE_RETENTION_DAYS）
# 環境變數格式：{NAME}_ARCHIVE_RETENTION_DAYS，例如 IOT_WRA_ARCHIVE_RETENTION_DAYS=3
# 目錄名稱（collector_name）→ retention 天數
COLLECTOR_RETENTION_OVERRIDES = {
    name: int(os.environ[f'{name.upper()}_ARCHIVE_RETENTION_DAYS'])
    for name in ('iot_wra', 'bus', 'bus_intercity', 'youbike', 'train',
                 'ship_ais', 'flight_fr24', 'flight_fr24_zone', 'freeway_vd',
                 'satellite', 'cwa_satellite', 'temperature', 'weather',
                 'air_quality', 'air_quality_microsensors', 'air_quality_imagery',
                 'foursquare_poi', 'ncdr_alerts', 'rain_gauge_realtime',
                 'river_water_level', 'groundwater_level', 'water_reservoir',
                 'water_reservoir_daily_ops', 'news_events')
    if os.getenv(f'{name.upper()}_ARCHIVE_RETENTION_DAYS')
}


def get_retention_days(collector_name: str) -> int:
    """回傳特定 collector 的本地保留天數，fallback 到全域設定。"""
    return COLLECTOR_RETENTION_OVERRIDES.get(collector_name, ARCHIVE_RETENTION_DAYS)

# 本地儲存路徑
# Zeabur Volume 掛載在 /data，優先使用環境變數 DATA_DIR
if os.getenv('DATA_DIR'):
    LOCAL_DATA_DIR = Path(os.getenv('DATA_DIR'))
elif IS_PRODUCTION:
    LOCAL_DATA_DIR = Path('/data')  # Zeabur Volume 掛載點
else:
    LOCAL_DATA_DIR = Path(__file__).parent / 'data'

# ============================================================
# Supabase 設定
# ============================================================

SUPABASE_ENABLED = os.getenv('SUPABASE_ENABLED', 'false').lower() in ('true', '1', 'yes')
SUPABASE_DB_URL = os.getenv('SUPABASE_DB_URL')  # Supavisor Transaction mode (port 6543)
SUPABASE_BUFFER_INTERVAL = int(os.getenv('SUPABASE_BUFFER_INTERVAL', '5'))  # buffer 重試間隔（分鐘）

# ============================================================
# API 設定
# ============================================================

API_KEY = os.getenv('API_KEY')  # 用於 HTTP API 認證
API_PORT = int(os.getenv('API_PORT', '8080'))

# ============================================================
# 通知設定
# ============================================================

WEBHOOK_URL = os.getenv('WEBHOOK_URL')
LINE_TOKEN = os.getenv('LINE_TOKEN')
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')

# Telegram Bot
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# 每日報告
DAILY_REPORT_ENABLED = os.getenv('DAILY_REPORT_ENABLED', 'true').lower() in ('true', '1', 'yes')
DAILY_REPORT_TIME = os.getenv('DAILY_REPORT_TIME', '08:00')  # 每日報告時間 (HH:MM)

# 連續錯誤告警門檻
CONSECUTIVE_ERROR_THRESHOLD = int(os.getenv('CONSECUTIVE_ERROR_THRESHOLD', '3'))

# 磁碟空間告警門檻（MB）
DISK_ALERT_THRESHOLD_MB = int(os.getenv('DISK_ALERT_THRESHOLD_MB', '35000'))  # 預設 35GB

# S3 費用估算（USD/GB/月，ap-southeast-2 2026 定價）
# S3_PRICE_PER_GB 保留為舊介面（=Standard 價），供 fallback
S3_PRICE_PER_GB = float(os.getenv('S3_PRICE_PER_GB', '0.025'))
# 按 storage class 分級估算；未列出的 class 會 fallback 到 STANDARD 價
# 對應 bucket lifecycle：0-30d STANDARD → 30-90d STANDARD_IA → 90+ GLACIER_IR
S3_PRICE_BY_STORAGE_CLASS = {
    'STANDARD': float(os.getenv('S3_PRICE_STANDARD', '0.025')),
    'STANDARD_IA': float(os.getenv('S3_PRICE_STANDARD_IA', '0.0138')),
    'ONEZONE_IA': float(os.getenv('S3_PRICE_ONEZONE_IA', '0.011')),
    'INTELLIGENT_TIERING': float(os.getenv('S3_PRICE_INTELLIGENT_TIERING', '0.025')),
    'GLACIER_IR': float(os.getenv('S3_PRICE_GLACIER_IR', '0.005')),
    'GLACIER': float(os.getenv('S3_PRICE_GLACIER', '0.0045')),
    'DEEP_ARCHIVE': float(os.getenv('S3_PRICE_DEEP_ARCHIVE', '0.002')),
    'REDUCED_REDUNDANCY': float(os.getenv('S3_PRICE_REDUCED_REDUNDANCY', '0.024')),
}

# ============================================================
# 收集器設定
# ============================================================
# 每個 collector 的 {PREFIX}_ENABLED / {PREFIX}_INTERVAL 改由 _COLLECTOR_TOGGLES
# 迴圈生成，降低重複樣板。新增 collector 時：
#   1. 在 _COLLECTOR_TOGGLES 加一筆 (prefix, default_enabled, default_interval)
#   2. 對照的 class / required_env 在 collectors/registry.py 定義
# 特殊變數（*_CITIES、*_AIRPORTS、bbox、憑證等）仍保留個別宣告。

def _env_bool(key: str, default: bool) -> bool:
    return os.getenv(key, 'true' if default else 'false').lower() in ('true', '1', 'yes')


# (prefix, enabled_default, interval_default_minutes)
_COLLECTOR_TOGGLES = (
    ('YOUBIKE',                      True,  10),  # 2026-06 擴張至全台 12 城（實測有 YouBike/Moovo 站點），interval 15→10
    ('WEATHER',                      True,  60),
    ('VD',                           False, 5),
    ('FREEWAY_VD',                   True,  10),
    ('ROAD_CONGESTION',              False, 5),    # 省道全國 + 市區 5 縣市實測堪用
    ('TEMPERATURE',                  True,  60),
    ('PARKING',                      False, 15),  # OnStreet 路邊（既有，221 補 Supabase 寫入）
    ('PARKING_OFFSTREET',            False, 10),  # OffStreet 路外場館 3 變體（City/SA/Tourism）
    ('BUS',                          True,  2),    # 22 城擴充後預設 2 分鐘
    ('BUS_INTERCITY',                False, 2),
    ('TOURIST_SHUTTLE',              False, 2),  # 台灣好行 A1 全國單一端點
    ('TRA_TRAIN',                    True,  2),
    ('TRA_STATIC',                   True,  1440),
    ('RAIL_TIMETABLE',               True,  1440),
    ('SHIP_TDX',                     False, 2),
    ('SHIP_AIS',                     False, 10),  # ⚠️ Taiwan IP required — 跑在 HiCloud VM，Zeabur 端強制關閉，見 docs/EXTERNAL_COLLECTORS.md
    ('FLIGHT_FR24',                  False, 5),
    ('FLIGHT_FR24_ZONE',             False, 5),
    ('FLIGHT_OPENSKY',               False, 5),
    ('EARTHQUAKE',                   True,  1440),
    ('SATELLITE',                    False, 120),  # TLE 每 8-24h 更新，2 小時足夠
    ('LAUNCH',                       False, 15),  # LL2 免費 tier ~15 calls/hr，15min 安全（4 calls/hr）
    ('CWA_SATELLITE',                True,  10),
    ('NCDR_ALERTS',                  True,  15),
    ('FOURSQUARE_POI',               False, 43200),  # 每 30 天
    ('AIR_QUALITY_IMAGERY',          False, 60),
    ('AIR_QUALITY',                  False, 60),
    ('AIR_QUALITY_MICROSENSORS',     False, 5),
    ('WATER_RESERVOIR',              False, 60),
    ('RIVER_WATER_LEVEL',            False, 10),
    ('RAIN_GAUGE_REALTIME',          False, 10),
    ('GROUNDWATER_LEVEL',            False, 60),  # 原始每 10 分鐘更新，但資料量大
    ('WATER_RESERVOIR_DAILY_OPS',    False, 1440),  # 官方 09:30 前更新
    ('WRA_DROUGHT_ALERT',            False, 1440),  # 水情燈號 daily（上游不定期，hash 比對去重）
    ('IOT_WRA',                      False, 60),   # 水利署 IoT 7 類站點整合收集（河川/地下水/閘門/沖刷/流量/堤防/揚塵）
    ('USWG',                         False, 10),   # 都市淹水感知器（OAuth2，1999 站全國淹水深度即時，rain-impact 用）
    ('PRECIPITATION_RASTER',         False, 60),   # 水利署累積雨量柵格圖 PNG（共用 IOW_CLIENT_*，每小時 4 張 ch=1/3/6/24）
    ('WASTE_POSITIONS',              False, 2),    # ⚠️ 跑在 HiCloud VM（Zeabur 端強制關閉，見 docs/EXTERNAL_COLLECTORS.md）— 對齊 NTPC 官方 2 分頻率
    ('WASTE_MATCH',                  False, 5),    # 垃圾車 OSRM map-matching，輸出 matched daily pre-aggregate
    ('ROAD_EVENT_LIVE',              False, 5),    # TDX RoadEvent LiveEvent (freeway+highway+city)
    ('ROAD_EVENT_PLANNED',           False, 720),  # TDX RoadEvent Event/City（預告型，12 hr）
    ('ER_HOSPITAL_REALTIME',         False, 15),   # 健保署重度級急診即時量能（來源每 15 分更新，無金鑰，無歷史）
    ('POWER_TAIPOWER',               False, 10),   # 台電即時電力供需（系統供需+各機組+區域用電，來源每 10 分更新，無金鑰）
    ('LIGHTNING_EVENTS',             False, 1),    # 台電落雷即時 (nid 61139，上游 1min snapshot 整檔覆寫→必 1min cron 才不漏；event_id+dedup_hash 去重；S3 archive 3 天/Supabase raw 7 天 + analytics.lightning_daily_summary 永久)
    ('NUCLEAR_RADIATION',            False, 15),   # 核設施環境輻射劑量 (nid 42326，51 站，UTF-8 BOM，>30min stale；S3 archive 14 天/Supabase measurements 30 天 + analytics.nuclear_radiation_daily 永久)
    ('WIC_SEWER',                    False, 10),   # 北市雨水下水道水位 (233 站，wic.gov.taipei，無金鑰)
    ('WIC_EVACUATE',                 False, 10),   # 北市疏散門狀態 (35 站，wic.gov.taipei，無金鑰)
    ('WIC_PUMB',                     False, 10),   # 北市抽水站運轉 (97 站，heopublic.gov.taipei，無金鑰)
    ('NEWS_EVENTS',                  False, 10),   # 新聞事件 RSS + Gemini 地點抽取 + GIS 相關性評估（v2 prompt）
    ('SATELLITE_PASSES_DAILY',       False, 1440), # 中國軍偵衛星通過台灣每日彙總（補昨+前天），需 SATELLITE collector 累積 TLE 歷史
    ('TWSE_MARKET_INDEX',            False, 1),    # TWSE 加權指數 ticker（盤中 5s 更新，1 分 polling 已遠快於前端需要）
    ('PLA_ACTIVITY_DAILY',           False, 30),   # 共機 @MoNDefense 每日通報（每 30 分鐘抓推特看當天有沒有更新）
    ('CDC_PUBLIC_HEALTH_WEEKLY',     False, 360),  # ⚠️ Taiwan IP required — Zeabur 必設 false（od.cdc.gov.tw 連線 timeout）；實際走 external/cdc_public_health_weekly_vm/
    ('YT_LIVE_VIDEO_RESOLVER',       False, 5),    # YouTube 14 家新聞台當前直播 videoId 解析（cron 5min，video_id 約 1-7 天換一次）
)

for _prefix, _en_default, _intv_default in _COLLECTOR_TOGGLES:
    globals()[f'{_prefix}_ENABLED'] = _env_bool(f'{_prefix}_ENABLED', _en_default)
    globals()[f'{_prefix}_INTERVAL'] = int(os.getenv(f'{_prefix}_INTERVAL', str(_intv_default)))

# ------------------------------------------------------------
# 各 collector 的「額外設定」（city list、API 金鑰、參數）
# ------------------------------------------------------------

# YouBike — 2026-06 實測 12 縣市有 YouBike/Moovo 站點（共 ~9,100 站）
# 其他 10 縣市（Keelung/Changhua/Yunlin/Pingtung/NantouCounty/YilanCounty/HualienCounty/PenghuCounty/KinmenCounty/LienchiangCounty）TDX 回 0 站
YOUBIKE_CITIES = os.getenv(
    'YOUBIKE_CITIES',
    'Taipei,NewTaipei,Taoyuan,Taichung,Tainan,Kaohsiung,Hsinchu,HsinchuCounty,Chiayi,ChiayiCounty,MiaoliCounty,TaitungCounty'
).split(',')

# Weather
WEATHER_STATIONS = os.getenv('WEATHER_STATIONS', '').split(',') if os.getenv('WEATHER_STATIONS') else []

# VD 車輛偵測器（縣市道路）
VD_CITIES = os.getenv('VD_CITIES', 'Taipei,NewTaipei').split(',')

# Road Event 縣市清單（Phase 1 EDA 驗證有資料的 10 縣市）
ROAD_EVENT_CITIES = os.getenv(
    'ROAD_EVENT_CITIES',
    'Taipei,NewTaipei,Taoyuan,Taichung,Tainan,Kaohsiung,Keelung,ChiayiCounty,YilanCounty,KinmenCounty'
).split(',')

# 溫度網格資料集編號（CWA）
TEMPERATURE_DATASET = 'O-A0038-003'  # 小時溫度觀測分析格點資料

# 路邊停車
PARKING_CITIES = os.getenv('PARKING_CITIES', 'Taipei,NewTaipei,Taichung').split(',')

# 省道+市區路況 — 市區實測 2026-06-19 僅 5 縣市有及時資料（Taoyuan/Taichung/Tainan/Keelung/YilanCounty）
# 其他縣市多回 0 段或 TravelTime=-99；北市 6/16 三天前停滯。用戶後續要全收只需設 env 覆寫。
ROAD_CONGESTION_CITIES = os.getenv(
    'ROAD_CONGESTION_CITIES',
    'Taoyuan,Taichung,Tainan,Keelung,YilanCounty'
).split(',')

# 路外停車場（OffStreet）— 預設 19 縣市（2026-06-19 實測 Changhua/Yunlin/Pingtung 端點回 HTTP 400 移出）
PARKING_OFFSTREET_CITIES = os.getenv(
    'PARKING_OFFSTREET_CITIES',
    'Taipei,NewTaipei,Taoyuan,Taichung,Tainan,Kaohsiung,Keelung,Hsinchu,HsinchuCounty,'
    'MiaoliCounty,Chiayi,ChiayiCounty,NantouCounty,YilanCounty,'
    'HualienCounty,TaitungCounty,PenghuCounty,KinmenCounty,LienchiangCounty'
).split(',')

# 公車即時位置（TDX Bus RealTimeByFrequency）
# 預設涵蓋全台 22 縣市（6 直轄市 + 3 省轄市 + 10 縣 + 3 離島縣）
# 調整配額：22 城 × 2 分鐘 = 15,840 req/日，超過 TDX 免費 10k 日配額
#   - 免費 key：調高 BUS_INTERVAL 至 3-5 分鐘，或縮減 BUS_CITIES
#   - 付費 key（1M/日）：預設即可
BUS_CITIES_DEFAULT = (
    # 直轄市 (6)
    'Taipei,NewTaipei,Taoyuan,Taichung,Tainan,Kaohsiung,'
    # 省轄市 (3)
    'Keelung,Hsinchu,Chiayi,'
    # 縣 (10)
    'HsinchuCounty,MiaoliCounty,ChanghuaCounty,NantouCounty,YunlinCounty,'
    'ChiayiCounty,PingtungCounty,YilanCounty,HualienCounty,TaitungCounty,'
    # 離島 (3)
    'PenghuCounty,KinmenCounty,LienchiangCounty'
)
BUS_CITIES = os.getenv('BUS_CITIES', BUS_CITIES_DEFAULT).split(',')
# 單一 collector 內部並行抓取的城市數上限（避免超出 TDX rate limit）
BUS_FETCH_WORKERS = int(os.getenv('BUS_FETCH_WORKERS', '5'))

# FlightRadar24 航班軌跡
FLIGHT_FR24_AIRPORTS = os.getenv('FLIGHT_FR24_AIRPORTS', 'RCTP,RCSS,RCKH,RCMQ,RCNN,RCYU,RCBS,RCFN,RCQC,RCFG,RCMT,RCLY,RCKU,RCKW,RCGI,RCCM,RCWA').split(',')
FLIGHT_FR24_TRAIL_DELAY = float(os.getenv('FLIGHT_FR24_TRAIL_DELAY', '3'))  # trail 請求間隔秒數

# FR24 Zone 空域快照 bbox（台灣周邊）
FLIGHT_FR24_ZONE_LAMIN = float(os.getenv('FLIGHT_FR24_ZONE_LAMIN', '20.8'))
FLIGHT_FR24_ZONE_LAMAX = float(os.getenv('FLIGHT_FR24_ZONE_LAMAX', '27.5'))
FLIGHT_FR24_ZONE_LOMIN = float(os.getenv('FLIGHT_FR24_ZONE_LOMIN', '116.2'))
FLIGHT_FR24_ZONE_LOMAX = float(os.getenv('FLIGHT_FR24_ZONE_LOMAX', '124.5'))

# OpenSky 空域快照
FLIGHT_OPENSKY_CLIENT_ID = os.getenv('FLIGHT_OPENSKY_CLIENT_ID', '')      # OAuth2（新帳號）
FLIGHT_OPENSKY_CLIENT_SECRET = os.getenv('FLIGHT_OPENSKY_CLIENT_SECRET', '')
FLIGHT_OPENSKY_USERNAME = os.getenv('FLIGHT_OPENSKY_USERNAME', '')        # Basic Auth（舊帳號）
FLIGHT_OPENSKY_PASSWORD = os.getenv('FLIGHT_OPENSKY_PASSWORD', '')

# 衛星軌道追蹤 — Space-Track 憑證
# 改用 Space-Track 原因：Zeabur 出口 IP 被 CelesTrak 封鎖（2026-04 起），切換到源頭資料供應者
# 註冊：https://www.space-track.org/
SPACETRACK_USERNAME = os.getenv('SPACETRACK_USERNAME', '')
SPACETRACK_PASSWORD = os.getenv('SPACETRACK_PASSWORD', '')

# 太空發射 (Launch Library 2)
LAUNCH_API_TOKEN = os.getenv('LAUNCH_API_TOKEN', '')  # Patreon 付費 token（可選）

# CWA 衛星雲圖 + 雷達回波 PNG
CWA_SATELLITE_DATASETS = (
    os.getenv('CWA_SATELLITE_DATASETS', '').split(',')
    if os.getenv('CWA_SATELLITE_DATASETS') else []
)  # 空 list = 使用 collector 內的 DEFAULT_DATASETS

# Foursquare OS Places POI
FOURSQUARE_POI_RELEASE_DT = os.getenv('FOURSQUARE_POI_RELEASE_DT', '')  # 指定 release 日期，如 2026-03-18
HF_TOKEN = os.getenv('HF_TOKEN', '')  # HuggingFace access token

# 空氣品質 - airtw 全台色階圖 PNG
AIR_QUALITY_IMAGERY_PRODUCTS = (
    os.getenv('AIR_QUALITY_IMAGERY_PRODUCTS', '').split(',')
    if os.getenv('AIR_QUALITY_IMAGERY_PRODUCTS') else []
)  # 空 list = 使用 DEFAULT_PRODUCTS (AQI/PM25/PM10/O3/NO2)

# 空氣品質 - LASS AirBox 微型感測器
AIR_QUALITY_MICROSENSORS_PM25_OUTLIER = float(os.getenv('AIR_QUALITY_MICROSENSORS_PM25_OUTLIER', '500'))  # μg/m³ 超過此值視為異常

# 垃圾車 GPS — 城市清單與 quiet hours
# 已驗證可打的 3 個城市：Kaohsiung / NewTaipei / Tainan（台北無公開 GPS API）
WASTE_POSITIONS_CITIES = os.getenv('WASTE_POSITIONS_CITIES', 'Kaohsiung,NewTaipei,Tainan').split(',')
# 凌晨幾乎零信號 → 預設 01-06 跳過此 tick；可設 'none' / 'off' / '' 關閉
# 格式 'HH-HH'（前閉後開，可跨午夜，例 '22-06'）
WASTE_POSITIONS_QUIET_HOURS = os.getenv('WASTE_POSITIONS_QUIET_HOURS', '01-06')

# 垃圾車 OSRM map-matching
OSRM_URL = os.getenv('OSRM_URL', 'http://localhost:5000').rstrip('/')
# 若走 osrm-proxy 跨 project 對外 endpoint，需帶 Bearer token；同 project 內網直連可空
OSRM_TOKEN = os.getenv('OSRM_TOKEN', '').strip()
WASTE_MATCH_CITIES = os.getenv('WASTE_MATCH_CITIES', '高雄市').split(',')
WASTE_MATCH_TARGET_DAYS = int(os.getenv('WASTE_MATCH_TARGET_DAYS', '2'))  # today + yesterday
WASTE_MATCH_MAX_TRIPS = int(os.getenv('WASTE_MATCH_MAX_TRIPS', '80'))
WASTE_MATCH_MAX_POINTS = int(os.getenv('WASTE_MATCH_MAX_POINTS', '100'))  # OSRM default max matching size
WASTE_MATCH_RADIUS_M = int(os.getenv('WASTE_MATCH_RADIUS_M', '50'))
WASTE_MATCH_MIN_CONFIDENCE = float(os.getenv('WASTE_MATCH_MIN_CONFIDENCE', '0.35'))

# 新聞事件 LLM 地點抽取（Gemini）
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL = os.getenv('GEMINI_MODEL', 'gemini-3.1-flash-lite-preview')

# Mini Taipei 每日時刻表發布
MINI_TAIPEI_PUBLISH_ENABLED = os.getenv('MINI_TAIPEI_PUBLISH_ENABLED', 'true').lower() in ('true', '1', 'yes')
MINI_TAIPEI_PUBLISH_TIME = os.getenv('MINI_TAIPEI_PUBLISH_TIME', '07:00')  # 每日發布時間
MINI_TAIPEI_S3_PREFIX = os.getenv('MINI_TAIPEI_S3_PREFIX', 'mini-taipei')  # S3 路徑前綴

# ============================================================
# 全域設定
# ============================================================

# 預設請求逾時（秒）
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))

# 請求間隔（秒）
REQUEST_INTERVAL = float(os.getenv('REQUEST_INTERVAL', '0.2'))

# 日誌等級
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


def validate_config():
    """驗證必要的設定"""
    errors = []

    if not TDX_APP_ID or not TDX_APP_KEY:
        errors.append("TDX_APP_ID 和 TDX_APP_KEY 未設定")

    if errors:
        print("⚠️  設定錯誤:")
        for error in errors:
            print(f"   - {error}")
        return False

    return True


def print_config():
    """顯示目前設定（隱藏敏感資訊）"""
    print("=" * 50)
    print("📋 設定")
    print("=" * 50)
    print(f"   環境: {'Production' if IS_PRODUCTION else 'Development'}")
    print(f"   TDX: {'✓' if TDX_APP_ID else '✗'}")
    print(f"   CWA: {'✓' if CWA_API_KEY else '✗'}")
    print(f"   S3:  {'✓ ' + S3_BUCKET if S3_BUCKET else '✗ (使用本地儲存)'}")
    print(f"   Supabase: {'✓' if SUPABASE_ENABLED and SUPABASE_DB_URL else '✗ (未啟用)'}")
    print(f"   API: {'✓ Port ' + str(API_PORT) if API_KEY else '✗ (未設定 API_KEY)'}")
    print(f"   通知: {'✓' if WEBHOOK_URL or LINE_TOKEN or TELEGRAM_BOT_TOKEN else '✗'}")
    if TELEGRAM_BOT_TOKEN:
        print(f"   Telegram: ✓ (日報 {DAILY_REPORT_TIME})")
    print(f"   資料目錄: {LOCAL_DATA_DIR}")
    print("=" * 50)
