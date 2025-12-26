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

# ============================================================
# 收集器設定
# ============================================================

# YouBike
YOUBIKE_CITIES = os.getenv('YOUBIKE_CITIES', 'Taipei,NewTaipei,Taoyuan').split(',')
YOUBIKE_INTERVAL = int(os.getenv('YOUBIKE_INTERVAL', '15'))

# 氣象
WEATHER_INTERVAL = int(os.getenv('WEATHER_INTERVAL', '60'))
WEATHER_STATIONS = os.getenv('WEATHER_STATIONS', '').split(',') if os.getenv('WEATHER_STATIONS') else []

# VD 車輛偵測器
VD_CITIES = os.getenv('VD_CITIES', 'Taipei,NewTaipei').split(',')
VD_INTERVAL = int(os.getenv('VD_INTERVAL', '5'))

# 溫度網格 (CWA O-A0038-003)
TEMPERATURE_INTERVAL = int(os.getenv('TEMPERATURE_INTERVAL', '60'))
TEMPERATURE_DATASET = 'O-A0038-003'  # 小時溫度觀測分析格點資料

# 路邊停車 (TDX Parking API)
PARKING_CITIES = os.getenv('PARKING_CITIES', 'Taipei,NewTaipei,Taichung').split(',')
PARKING_INTERVAL = int(os.getenv('PARKING_INTERVAL', '15'))

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
    print(f"   API: {'✓ Port ' + str(API_PORT) if API_KEY else '✗ (未設定 API_KEY)'}")
    print(f"   通知: {'✓' if WEBHOOK_URL or LINE_TOKEN else '✗'}")
    print(f"   資料目錄: {LOCAL_DATA_DIR}")
    print("=" * 50)
