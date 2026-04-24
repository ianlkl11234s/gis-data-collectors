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
ARCHIVE_RETENTION_DAYS = int(os.getenv('ARCHIVE_RETENTION_DAYS', '7'))  # 本地保留天數
ARCHIVE_TIME = os.getenv('ARCHIVE_TIME', '03:00')  # 每日歸檔時間 (HH:MM)

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
    ('YOUBIKE',                      True,  15),
    ('WEATHER',                      True,  60),
    ('VD',                           False, 5),
    ('FREEWAY_VD',                   True,  10),
    ('TEMPERATURE',                  True,  60),
    ('PARKING',                      False, 15),
    ('BUS',                          True,  2),    # 22 城擴充後預設 2 分鐘
    ('BUS_INTERCITY',                False, 2),
    ('TRA_TRAIN',                    True,  2),
    ('TRA_STATIC',                   True,  1440),
    ('RAIL_TIMETABLE',               True,  1440),
    ('SHIP_TDX',                     False, 2),
    ('SHIP_AIS',                     True,  10),
    ('FLIGHT_FR24',                  False, 5),
    ('FLIGHT_FR24_ZONE',             False, 5),
    ('FLIGHT_OPENSKY',               False, 5),
    ('EARTHQUAKE',                   True,  1440),
    ('SATELLITE',                    False, 120),  # TLE 每 8-24h 更新，2 小時足夠
    ('LAUNCH',                       False, 5),
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
    ('IOT_WRA',                      False, 60),   # 水利署 IoT 7 類站點整合收集（河川/地下水/閘門/沖刷/流量/堤防/揚塵）
)

for _prefix, _en_default, _intv_default in _COLLECTOR_TOGGLES:
    globals()[f'{_prefix}_ENABLED'] = _env_bool(f'{_prefix}_ENABLED', _en_default)
    globals()[f'{_prefix}_INTERVAL'] = int(os.getenv(f'{_prefix}_INTERVAL', str(_intv_default)))

# ------------------------------------------------------------
# 各 collector 的「額外設定」（city list、API 金鑰、參數）
# ------------------------------------------------------------

# YouBike
YOUBIKE_CITIES = os.getenv('YOUBIKE_CITIES', 'Taipei,NewTaipei,Taoyuan').split(',')

# Weather
WEATHER_STATIONS = os.getenv('WEATHER_STATIONS', '').split(',') if os.getenv('WEATHER_STATIONS') else []

# VD 車輛偵測器（縣市道路）
VD_CITIES = os.getenv('VD_CITIES', 'Taipei,NewTaipei').split(',')

# 溫度網格資料集編號（CWA）
TEMPERATURE_DATASET = 'O-A0038-003'  # 小時溫度觀測分析格點資料

# 路邊停車
PARKING_CITIES = os.getenv('PARKING_CITIES', 'Taipei,NewTaipei,Taichung').split(',')

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
