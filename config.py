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

# ============================================================
# CWA 氣象局 API 設定
# ============================================================

CWA_API_KEY = os.getenv('CWA_API_KEY')
CWA_API_BASE = "https://opendata.cwa.gov.tw/api"
CWA_FILE_API_BASE = "https://opendata.cwa.gov.tw/fileapi/v1/opendataapi"

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

# S3 費用估算（USD/GB/月，預設 S3 Standard ap-southeast-2）
S3_PRICE_PER_GB = float(os.getenv('S3_PRICE_PER_GB', '0.025'))

# ============================================================
# 收集器設定
# ============================================================

# YouBike
YOUBIKE_ENABLED = os.getenv('YOUBIKE_ENABLED', 'true').lower() in ('true', '1', 'yes')
YOUBIKE_CITIES = os.getenv('YOUBIKE_CITIES', 'Taipei,NewTaipei,Taoyuan').split(',')
YOUBIKE_INTERVAL = int(os.getenv('YOUBIKE_INTERVAL', '15'))

# 氣象
WEATHER_ENABLED = os.getenv('WEATHER_ENABLED', 'true').lower() in ('true', '1', 'yes')
WEATHER_INTERVAL = int(os.getenv('WEATHER_INTERVAL', '60'))
WEATHER_STATIONS = os.getenv('WEATHER_STATIONS', '').split(',') if os.getenv('WEATHER_STATIONS') else []

# VD 車輛偵測器（縣市道路）
VD_ENABLED = os.getenv('VD_ENABLED', 'false').lower() in ('true', '1', 'yes')  # 預設停用
VD_CITIES = os.getenv('VD_CITIES', 'Taipei,NewTaipei').split(',')
VD_INTERVAL = int(os.getenv('VD_INTERVAL', '5'))

# 國道即時車流 + 壅塞 (TDX Freeway VD/Live)
FREEWAY_VD_ENABLED = os.getenv('FREEWAY_VD_ENABLED', 'true').lower() in ('true', '1', 'yes')
FREEWAY_VD_INTERVAL = int(os.getenv('FREEWAY_VD_INTERVAL', '10'))  # 每 10 分鐘

# 溫度網格 (CWA O-A0038-003)
TEMPERATURE_ENABLED = os.getenv('TEMPERATURE_ENABLED', 'true').lower() in ('true', '1', 'yes')
TEMPERATURE_INTERVAL = int(os.getenv('TEMPERATURE_INTERVAL', '60'))
TEMPERATURE_DATASET = 'O-A0038-003'  # 小時溫度觀測分析格點資料

# 路邊停車 (TDX Parking API)
PARKING_ENABLED = os.getenv('PARKING_ENABLED', 'false').lower() in ('true', '1', 'yes')  # 預設停用
PARKING_CITIES = os.getenv('PARKING_CITIES', 'Taipei,NewTaipei,Taichung').split(',')
PARKING_INTERVAL = int(os.getenv('PARKING_INTERVAL', '15'))

# 公車即時位置 (TDX Bus RealTimeByFrequency)
BUS_ENABLED = os.getenv('BUS_ENABLED', 'true').lower() in ('true', '1', 'yes')
BUS_CITIES = os.getenv('BUS_CITIES', 'Taipei,NewTaipei,Taoyuan').split(',')
BUS_INTERVAL = int(os.getenv('BUS_INTERVAL', '1'))  # 每 1 分鐘

# 台鐵 (TDX TRA API)
TRA_TRAIN_ENABLED = os.getenv('TRA_TRAIN_ENABLED', 'true').lower() in ('true', '1', 'yes')
TRA_TRAIN_INTERVAL = int(os.getenv('TRA_TRAIN_INTERVAL', '2'))  # 即時列車位置，每 2 分鐘
TRA_STATIC_ENABLED = os.getenv('TRA_STATIC_ENABLED', 'true').lower() in ('true', '1', 'yes')
TRA_STATIC_INTERVAL = int(os.getenv('TRA_STATIC_INTERVAL', '1440'))  # 靜態資料，每日一次

# 台鐵 + 高鐵每日時刻表歸檔（DailyTimetable，含停駛/加班車）
RAIL_TIMETABLE_ENABLED = os.getenv('RAIL_TIMETABLE_ENABLED', 'true').lower() in ('true', '1', 'yes')
RAIL_TIMETABLE_INTERVAL = int(os.getenv('RAIL_TIMETABLE_INTERVAL', '1440'))  # 每日一次

# 航運 (Ship)
SHIP_TDX_ENABLED = os.getenv('SHIP_TDX_ENABLED', 'false').lower() in ('true', '1', 'yes')  # TDX 國內航線
SHIP_TDX_INTERVAL = int(os.getenv('SHIP_TDX_INTERVAL', '2'))  # 每 2 分鐘
SHIP_AIS_ENABLED = os.getenv('SHIP_AIS_ENABLED', 'true').lower() in ('true', '1', 'yes')  # 航港局 AIS
SHIP_AIS_INTERVAL = int(os.getenv('SHIP_AIS_INTERVAL', '10'))  # 每 10 分鐘

# FlightRadar24 航班軌跡
FLIGHT_FR24_ENABLED = os.getenv('FLIGHT_FR24_ENABLED', 'false').lower() in ('true', '1', 'yes')
FLIGHT_FR24_INTERVAL = int(os.getenv('FLIGHT_FR24_INTERVAL', '5'))  # 每 5 分鐘
FLIGHT_FR24_AIRPORTS = os.getenv('FLIGHT_FR24_AIRPORTS', 'RCTP,RCSS,RCKH,RCMQ,RCNN,RCYU,RCBS,RCFN,RCQC,RCFG,RCMT,RCLY,RCKU,RCKW,RCGI,RCCM,RCWA').split(',')
FLIGHT_FR24_TRAIL_DELAY = float(os.getenv('FLIGHT_FR24_TRAIL_DELAY', '3'))  # trail 請求間隔秒數

# FR24 Zone 空域快照（公開 feed，無需 API key）
FLIGHT_FR24_ZONE_ENABLED = os.getenv('FLIGHT_FR24_ZONE_ENABLED', 'false').lower() in ('true', '1', 'yes')
FLIGHT_FR24_ZONE_INTERVAL = int(os.getenv('FLIGHT_FR24_ZONE_INTERVAL', '5'))
FLIGHT_FR24_ZONE_LAMIN = float(os.getenv('FLIGHT_FR24_ZONE_LAMIN', '20.8'))
FLIGHT_FR24_ZONE_LAMAX = float(os.getenv('FLIGHT_FR24_ZONE_LAMAX', '27.5'))
FLIGHT_FR24_ZONE_LOMIN = float(os.getenv('FLIGHT_FR24_ZONE_LOMIN', '116.2'))
FLIGHT_FR24_ZONE_LOMAX = float(os.getenv('FLIGHT_FR24_ZONE_LOMAX', '124.5'))

# OpenSky 空域快照
FLIGHT_OPENSKY_ENABLED = os.getenv('FLIGHT_OPENSKY_ENABLED', 'false').lower() in ('true', '1', 'yes')
FLIGHT_OPENSKY_INTERVAL = int(os.getenv('FLIGHT_OPENSKY_INTERVAL', '5'))  # 每 5 分鐘
FLIGHT_OPENSKY_CLIENT_ID = os.getenv('FLIGHT_OPENSKY_CLIENT_ID', '')      # OAuth2（新帳號）
FLIGHT_OPENSKY_CLIENT_SECRET = os.getenv('FLIGHT_OPENSKY_CLIENT_SECRET', '')
FLIGHT_OPENSKY_USERNAME = os.getenv('FLIGHT_OPENSKY_USERNAME', '')        # Basic Auth（舊帳號）
FLIGHT_OPENSKY_PASSWORD = os.getenv('FLIGHT_OPENSKY_PASSWORD', '')

# 地震報告 (CWA Earthquake API)
EARTHQUAKE_ENABLED = os.getenv('EARTHQUAKE_ENABLED', 'true').lower() in ('true', '1', 'yes')
EARTHQUAKE_INTERVAL = int(os.getenv('EARTHQUAKE_INTERVAL', '1440'))  # 每日一次 (1440 分鐘)

# 衛星軌道追蹤 (CelesTrak GP + SGP4，免註冊)
SATELLITE_ENABLED = os.getenv('SATELLITE_ENABLED', 'false').lower() in ('true', '1', 'yes')
SATELLITE_INTERVAL = int(os.getenv('SATELLITE_INTERVAL', '120'))  # 每 2 小時（配合 CelesTrak 更新頻率）

# 太空發射 (Launch Library 2，免費 15 req/hr，付費可提升)
LAUNCH_ENABLED = os.getenv('LAUNCH_ENABLED', 'false').lower() in ('true', '1', 'yes')
LAUNCH_INTERVAL = int(os.getenv('LAUNCH_INTERVAL', '5'))  # 每 5 分鐘一次（每次只做 1 個 API call，不阻塞其他收集器）
LAUNCH_API_TOKEN = os.getenv('LAUNCH_API_TOKEN', '')  # Patreon 付費 token（可選）

# CWA 衛星雲圖 + 雷達回波 PNG (O-C0042-004 / O-A0058-005，需 CWA API Key)
CWA_SATELLITE_ENABLED = os.getenv('CWA_SATELLITE_ENABLED', 'true').lower() in ('true', '1', 'yes')
CWA_SATELLITE_INTERVAL = int(os.getenv('CWA_SATELLITE_INTERVAL', '10'))  # 每 10 分鐘
CWA_SATELLITE_DATASETS = (
    os.getenv('CWA_SATELLITE_DATASETS', '').split(',')
    if os.getenv('CWA_SATELLITE_DATASETS') else []
)  # 空 list = 使用 collector 內的 DEFAULT_DATASETS

# NCDR 災害示警 (CAP feed，無需 API key)
NCDR_ALERTS_ENABLED = os.getenv('NCDR_ALERTS_ENABLED', 'true').lower() in ('true', '1', 'yes')
NCDR_ALERTS_INTERVAL = int(os.getenv('NCDR_ALERTS_INTERVAL', '15'))  # 每 15 分鐘

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
