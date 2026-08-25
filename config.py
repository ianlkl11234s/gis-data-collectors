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

# === 全球氣候 plan-misty-fog（2026-06-28）===
# CMEMS (Copernicus Marine) — 帳號 username + password (非 email)
COPERNICUSMARINE_SERVICE_USERNAME = os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')
COPERNICUSMARINE_SERVICE_PASSWORD = os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')
# CAMS (Copernicus Atmosphere Monitoring) — ADS API key
CAMS_API_KEY = os.getenv('CAMS_API_KEY')

# ============================================================
# YouTube Data API v3（yt_live_video_resolver）
# ============================================================
# GCP 專案需啟用 YouTube Data API v3，建 API key 並限制到該 service。
# 配額：search.list 獨立桶 100 calls/day/專案；其餘端點共用 10,000 units/day，PT 午夜重置。
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# ============================================================
# 儲存設定
# ============================================================

# S3 設定
S3_BUCKET = os.getenv('S3_BUCKET')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY') or os.getenv('AWS_ACCESS_KEY_ID')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY')
S3_REGION = os.getenv('S3_REGION', 'ap-southeast-2')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')  # 用於 MinIO 等相容服務

# Cloudflare R2 設定（影像 CDN 雙寫，AR-11 read-path-cdn）
# 與上方 AWS S3（歸檔）分離，避免影響既有歸檔路徑；R2 走 S3 相容 API（boto3 + endpoint）。
# 4 個變數任一未設 → R2 功能停用（安全 rollout / best-effort，不因 CDN 壞掉丟資料）。
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_BUCKET = os.getenv('R2_BUCKET', 'mini-tw-pulse')

# === Supabase → S3 backup system (tasks/backup_supabase.py) ===
BACKUP_ENABLED = os.getenv('BACKUP_ENABLED', 'false').lower() == 'true'
BACKUP_DRY_RUN = os.getenv('BACKUP_DRY_RUN', 'false').lower() == 'true'
BACKUP_MANIFEST_PATH = Path(__file__).parent / 'config' / 'backup_manifest.yaml'
BACKUP_STATIC_STORAGE_CLASS = os.getenv('BACKUP_STATIC_STORAGE_CLASS', 'GLACIER_IR')
BACKUP_REALTIME_STORAGE_CLASS = os.getenv('BACKUP_REALTIME_STORAGE_CLASS', 'STANDARD')
BACKUP_STATEMENT_TIMEOUT_MS = int(os.getenv('BACKUP_STATEMENT_TIMEOUT_MS', '300000'))

# === 每日軌跡凍結匯出 (scripts/export_daily_trails.py) ===
# Supabase 的 live.*_trails_daily 是滾動視窗（bus / bus_intercity 3 天、ship 7 天、
# flight 7~9 天），不每天凍結成 S3 靜態檔就永久遺失。
# ⚠️ 預設 false 且只能在「單一實例」開：這個 job 打共用 DB、寫共用 manifest
#    （get → merge → put 非原子），兩個實例同時跑會互相覆蓋掉對方寫的 dates。
TRAILS_EXPORT_ENABLED = os.getenv('TRAILS_EXPORT_ENABLED', 'false').lower() in ('true', '1', 'yes')
# 02:00 為容器本地時間，Dockerfile 已設 TZ=Asia/Taipei。
# 依據：summary 表 refreshed_at 顯示每日資料 01:00~01:20 才定版，早於此會匯到半成品。
TRAILS_EXPORT_TIME = os.getenv('TRAILS_EXPORT_TIME', '02:00')

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
# Buffer 目錄檔數上限（比照 external/vm_common/vm_buffer.py 的 MAX_BUFFER_FILES 模式）：
# DB 長時間不可用時，寫新 buffer 檔前超量先刪最舊並記 warning，防 /data volume 塞爆。
# 3 天 age 丟棄邏輯（BUFFER_MAX_AGE_DAYS）另行保留，兩者獨立。
SUPABASE_BUFFER_MAX_FILES = int(os.getenv('SUPABASE_BUFFER_MAX_FILES', '500'))
# 寫入韌性：避免 DB hang 時整條寫入鏈卡死（單連線 + 單 RLock）
SUPABASE_CONNECT_TIMEOUT = int(os.getenv('SUPABASE_CONNECT_TIMEOUT', '10'))        # 建線 timeout（秒）
SUPABASE_STATEMENT_TIMEOUT_MS = int(os.getenv('SUPABASE_STATEMENT_TIMEOUT_MS', '30000'))  # 單筆語句 timeout（毫秒）
# 連線池：取代舊「一條 conn + RLock」設計，避免單條連線 wedge 連鎖卡住所有 collector
SUPABASE_POOL_MIN = int(os.getenv('SUPABASE_POOL_MIN', '2'))                       # pool 最小連線數
# 預設 15；主站（Zeabur data-collectors-gomn）env 覆寫為 30。2026-07-05：db.py borrow()
# 加 pool pre-ping（PR #33）修掉 connection already closed 後，殘留錯誤轉為「borrow timeout
# — 所有連線都 busy」（尖峰 15 條不夠 + pre-ping 每次多一次 SELECT 1 往返）。Postgres
# max_connections=90、實際僅用 ~24，餘量足 → 拉到 30。若之後仍 borrow timeout，改走
# idle-threshold pre-ping（熱連線跳過 SELECT 1），別再無腦加大。
SUPABASE_POOL_MAX = int(os.getenv('SUPABASE_POOL_MAX', '15'))                      # pool 最大連線數（並發上限）
SUPABASE_BORROW_TIMEOUT_SEC = float(os.getenv('SUPABASE_BORROW_TIMEOUT_SEC', '5')) # 借連線 timeout（秒）— 借不到就 buffer
# Watchdog：主迴圈 heartbeat 超過此秒數沒更新 → 判定卡死
HEALTH_MAX_LOOP_SILENCE = int(os.getenv('HEALTH_MAX_LOOP_SILENCE', '120'))
# 進程內 watchdog：心跳過期時 os._exit(1) 自我了斷 → 容器崩潰 → 平台重啟
# （Zeabur 不會因 runtime unhealthy 重啟容器，但會重啟崩潰的進程，故走自殺式）
HEALTH_WATCHDOG_ENABLED = os.getenv('HEALTH_WATCHDOG_ENABLED', 'true').lower() in ('true', '1', 'yes')

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
    ('PARKING_OFFSTREET',            False, 15),  # OffStreet 路外場館 3 變體（City/SA/Tourism）
    ('PARKING_REF',                  False, 43200),  # 停車靜態座標 ref（月更），直接 SQL 寫 spatial.parking_segments_ref / parking_lots_ref
    ('BUS',                          True,  2),    # 22 城擴充後預設 2 分鐘
    ('BUS_INTERCITY',                False, 2),
    ('TOURIST_SHUTTLE',              False, 2),  # 台灣好行 A1 全國單一端點
    ('TRA_TRAIN',                    True,  2),
    ('TRA_STATIC',                   True,  1440),
    ('RAIL_TIMETABLE',               True,  1440),
    ('SHIP_TDX',                     False, 2),
    ('SHIP_AIS',                     False, 10),  # ⚠️ Taiwan IP required — 跑在 HiCloud VM，Zeabur 端強制關閉，見 docs/EXTERNAL_COLLECTORS.md
    ('AISSTREAM',                    False, 1),   # 常駐 WebSocket；由 main.py 另行啟動，不走 interval scheduler
    ('GFW_VESSEL_PRESENCE',          False, 1440), # GFW 4Wings daily; token + license gate required
    ('FLIGHT_FR24',                  False, 5),
    ('FLIGHT_FR24_ZONE',             False, 5),
    ('FLIGHT_OPENSKY',               False, 5),
    ('EARTHQUAKE',                   True,  15),   # 有感地震報告 + 逐站觀測 + 海嘯資訊（事件驅動，15 分鐘才追得上）
    ('EARTHQUAKE_CATALOG',           False, 1440), # CWA E-A0073-001 本年度正式地震目錄（含無感，半年更新一批）；每日比對有新才寫
    ('EARTHQUAKE_TOWN_INTENSITY',    False, 15),   # CWA E-A0015-005 全台 368 鄉鎮震度（S3 免金鑰，只留最新一次 → 靠本表存歷史）
    ('EARTHQUAKE_SHAKEMAP_GRID',     False, 15),   # NCDR EQ1 全台 2.5km 網格 4,377 格（免金鑰，只留最新一次；event_time 守門避免重寫）
    ('EARTHQUAKE_MOMENT_TENSOR',     False, 30),   # 中研院 AutoBATS 震源機制解（無清單端點，靠 CWA 事件清單逐一查；UTC 秒級對時）
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
    # ⚠️ 上游自 2026-07-10 起靜默斷供（端點 200 但永遠只有標題行）→ interval 1→30 省請求。
    #    **恢復時必須立刻調回 1**：端點是 1min 整檔覆寫，每批只存在 60 秒，30 分鐘會漏 29/30。
    #    collector 偵測到恢復會發 Telegram 提醒（_maybe_notify_recovery）。替代源見 LIGHTNING_CWA。
    ('LIGHTNING_EVENTS',             False, 30),   # 台電落雷即時 (nid 61139；event_id+dedup_hash 去重；S3 archive 3 天/Supabase raw 7 天 + analytics.lightning_daily_summary 永久)
    ('LIGHTNING_CWA',                False, 5),    # 氣象署落雷 O-A0039-001（KMZ，滾動 1hr 視窗每 5 分更新）；寫同表 source='cwa'。台電自 2026-07-10 起端點活著但永遠空檔 → 本源為替代兼交叉驗證
    ('NUCLEAR_RADIATION',            False, 15),   # 核設施環境輻射劑量 (nid 42326，51 站，UTF-8 BOM，>30min stale；S3 archive 14 天/Supabase measurements 30 天 + analytics.nuclear_radiation_daily 永久)
    ('WIC_SEWER',                    False, 10),   # 北市雨水下水道水位 (233 站，wic.gov.taipei，無金鑰)
    ('WIC_EVACUATE',                 False, 10),   # 北市疏散門狀態 (35 站，wic.gov.taipei，無金鑰)
    ('WIC_PUMB',                     False, 10),   # 北市抽水站運轉 (97 站，heopublic.gov.taipei，無金鑰)
    ('NEWS_EVENTS',                  False, 10),   # 新聞事件 RSS + Gemini 地點抽取 + GIS 相關性評估（v2 prompt）
    ('SATELLITE_PASSES_DAILY',       False, 1440), # 中國軍偵衛星通過台灣每日彙總（補昨+前天），需 SATELLITE collector 累積 TLE 歷史
    ('TWSE_MARKET_INDEX',            False, 1),    # TWSE 加權指數 ticker（盤中 5s 更新，1 分 polling 已遠快於前端需要）
    ('PLA_ACTIVITY_DAILY',           False, 30),   # 共機 @MoNDefense 每日通報（每 30 分鐘抓推特看當天有沒有更新）
    ('PLA_TRACKS_VECTORIZE',         False, 1440), # 共機航跡示意圖向量化（CV，非 API）；補齊 pla_activity_daily 已抓到 track_chart_url 但尚未向量化的日子，1 row/day 到 spatial.pla_tracks_runs
    ('CDC_PUBLIC_HEALTH_WEEKLY',     False, 360),  # ⚠️ Taiwan IP required — Zeabur 必設 false（od.cdc.gov.tw 連線 timeout）；實際走 external/cdc_public_health_weekly_vm/
    ('YT_LIVE_VIDEO_RESOLVER',       False, 5),    # YouTube 14 家新聞台當前直播 videoId 解析（cron 5min，video_id 約 1-7 天換一次）
    ('CORRECTIONAL_DAILY_SNAPSHOT',  False, 1440), # 矯正機關每日收容動態（prisonmuseum.moj.gov.tw/jqw_pub/today.xml，全國總計 1 row/day，無金鑰）
    ('ANIMAL_ADOPTION',              False, 1440), # 農業部待認領養動物完整快照；成功全量才更新 current/daily，保留 S3 raw archive
    # 農業部動物福利月報：兩個官方資料集各自獨立 job，均為完整歷史月報快照。
    ('ANIMAL_SHELTER_OUTCOMES',      False, 43200), # datagov:41236 收容成果（約月更）
    ('ANIMAL_SHELTER_PRESSURE',      False, 43200), # datagov:73396 收容壓力/滯留（約月更）
    ('ANIMAL_VETERINARY_CLINICS',    False, 259200), # datagov:8705 獸醫師/佐開業執照（180日）
    ('ANIMAL_LICENSED_PET_BUSINESSES', False, 525600), # datagov:97070 合法特定寵物業（365日）
    ('ANIMAL_PROTECTION_OFFICES',     False, 129600), # datagov:134283 動物保護機關（90日）
    ('IMMIGRATION_APIS_AIRPORT',     False, 60),   # 移民署機場入出境 6 端點 demographic snapshot（無時間戳，每細格 paxCnt，無金鑰）
    ('NPA_TRAFFIC_ACCIDENT_A1',      False, 720),  # 警政署即時 A1 交通事故（24h 死亡，累積年度，每日 1-2 次抓 dedup by hash）
    ('TPML_SEAT',                    False, 10),   # 北市圖座位即時 (seat.tpml.edu.tw，6 分館 29 區，無金鑰；來源無 timestamp → observed_at=收集時刻；閉館全 0 → is_closed)
    ('FOOD_PRICES',                  False, 1440), # 農業部四類批發價（蔬果/漁產/毛豬/家禽，無金鑰）；T+1 更新故每日 1 次即可，約 52 次請求/日、~2,000 row/日
    # === 全球氣候（plan-misty-fog 2026-06-28）===
    ('GLOBAL_CLIMATE_USGS_EARTHQUAKE', False, 60),  # USGS hourly feed M≥任意（無認證、GeoJSON），全球地震寫 live.earthquakes_global；台灣周邊 1y M≥4.0 約 172 筆
    ('GLOBAL_CLIMATE_JMA_TYPHOON',     False, 180), # JMA RSMC Tokyo 颱風（無認證、JSON）；targetTc.json 空就 idle；展開為 typhoon_positions row source='jma'
    ('GLOBAL_CLIMATE_JTWC',            False, 360), # JTWC 颱風（無認證、RSS+ATCF 文字）；同 typhoon_positions source='jtwc'
    ('GLOBAL_CLIMATE_CMEMS',           False, 1440), # CMEMS 海洋模式（CMEMS account 必設 USERNAME/_PASSWORD）；binary 走 S3 + Supabase digest
    ('GLOBAL_CLIMATE_CAMS',            False, 1440), # CAMS 大氣（CAMS_API_KEY 必設 + dataset licence accept）；排隊 5-30 min
    ('GLOBAL_CLIMATE_NOAA_GFS',        False, 1440), # NOAA GFS 全球風場（無認證 AWS Open Data）；HTTP Range pull
    ('GLOBAL_CLIMATE_BAKE',            False, 360),  # 烤圖：GFS/CMEMS/CAMS 最新實況場 → deploy-assets/climate/*_latest.{png,json}（前端粒子/raster）；每 6h 重烤取最新
)

for _prefix, _en_default, _intv_default in _COLLECTOR_TOGGLES:
    globals()[f'{_prefix}_ENABLED'] = _env_bool(f'{_prefix}_ENABLED', _en_default)
    globals()[f'{_prefix}_INTERVAL'] = int(os.getenv(f'{_prefix}_INTERVAL', str(_intv_default)))

# AISStream 常駐 WebSocket worker（獨立於既有 ship_ais / SupabaseWriter pipeline）
# 預設關閉；啟用前需先完成 gis-platform migration、S3 權限與 Zeabur smoke test。
AISSTREAM_ENABLED = _env_bool('AISSTREAM_ENABLED', False)
AISSTREAM_API_KEY = os.getenv('AISSTREAM_API_KEY', '')
AISSTREAM_WS_URL = os.getenv('AISSTREAM_WS_URL', 'wss://stream.aisstream.io/v0/stream')
AISSTREAM_CAMPAIGN_DAYS = int(os.getenv('AISSTREAM_CAMPAIGN_DAYS', '14'))
AISSTREAM_RECONNECT_MAX_SECONDS = float(os.getenv('AISSTREAM_RECONNECT_MAX_SECONDS', '60'))
AISSTREAM_QUEUE_MAXSIZE = int(os.getenv('AISSTREAM_QUEUE_MAXSIZE', '10000'))
AISSTREAM_SPOOL_ROTATE_MINUTES = int(os.getenv('AISSTREAM_SPOOL_ROTATE_MINUTES', '60'))
AISSTREAM_SPOOL_MAX_MB = int(os.getenv('AISSTREAM_SPOOL_MAX_MB', '10240'))
AISSTREAM_DB_BATCH_SIZE = int(os.getenv('AISSTREAM_DB_BATCH_SIZE', '100'))
AISSTREAM_DB_FLUSH_SECONDS = float(os.getenv('AISSTREAM_DB_FLUSH_SECONDS', '5'))
AISSTREAM_S3_STORAGE_CLASS = os.getenv('AISSTREAM_S3_STORAGE_CLASS', 'GLACIER_IR')
AISSTREAM_S3_PREFIX = os.getenv('AISSTREAM_S3_PREFIX', 'aisstream/raw/v1')
AISSTREAM_HEALTH_INTERVAL_SECONDS = int(os.getenv('AISSTREAM_HEALTH_INTERVAL_SECONDS', '60'))

# Global Fishing Watch backend token。Legacy DAILY DB presence collector 維持 disabled；
# production 由下方 unified hourly publisher 的獨立 enable + redistribution 雙閘門控制。
GFW_ACCESS_TOKEN = os.getenv('GFW_ACCESS_TOKEN', '')
GFW_RAW_ARCHIVE_ENABLED = _env_bool('GFW_RAW_ARCHIVE_ENABLED', False)  # license gate; not implemented until approved
GFW_REPORT_URL = os.getenv('GFW_REPORT_URL', 'https://gateway.api.globalfishingwatch.org/v3/4wings/report')
GFW_DATA_LAG_DAYS = int(os.getenv('GFW_DATA_LAG_DAYS', '5'))
if not 4 <= GFW_DATA_LAG_DAYS <= 30:
    raise ValueError('GFW_DATA_LAG_DAYS 必須介於 4 與 30 天（dataset 約落後 96 小時）')

# GFW hourly unified release：AIS grid + approximate tracks 共用一次 normalized
# rolling fetch；SAR unmatched 是語意獨立的 sequential report。這是可重新散佈的
# CDN product，因此 enable 與 redistribution approval 必須同時明確開啟。
GFW_HOURLY_PUBLISH_ENABLED = _env_bool('GFW_HOURLY_PUBLISH_ENABLED', False)
GFW_HOURLY_REDISTRIBUTION_APPROVED = _env_bool('GFW_HOURLY_REDISTRIBUTION_APPROVED', False)
GFW_HOURLY_PUBLISH_TIME = os.getenv('GFW_HOURLY_PUBLISH_TIME', '06:30')
GFW_HOURLY_BBOX = os.getenv(
    'GFW_HOURLY_BBOX', '122.43400,23.22953,132.85274,34.35812'
)
GFW_HOURLY_ROLLING_DAYS = int(os.getenv('GFW_HOURLY_ROLLING_DAYS', '7'))
GFW_HOURLY_TILE_SIZE_DEGREES = float(os.getenv('GFW_HOURLY_TILE_SIZE_DEGREES', '3'))
GFW_HOURLY_EXPECTED_TILE_COUNT = int(os.getenv('GFW_HOURLY_EXPECTED_TILE_COUNT', '16'))
GFW_HOURLY_MAX_TRACK_FEATURES = int(os.getenv('GFW_HOURLY_MAX_TRACK_FEATURES', '5000'))
GFW_HOURLY_MAX_TRACK_POINTS = int(os.getenv('GFW_HOURLY_MAX_TRACK_POINTS', '150000'))
GFW_HOURLY_TRACK_GAP_HOURS = float(os.getenv('GFW_HOURLY_TRACK_GAP_HOURS', '2'))
GFW_HOURLY_MAX_SPEED_KNOTS = float(os.getenv('GFW_HOURLY_MAX_SPEED_KNOTS', '80'))
GFW_HOURLY_RELEASES_TO_KEEP = int(os.getenv('GFW_HOURLY_RELEASES_TO_KEEP', '2'))
GFW_HOURLY_FAILED_SPOOL_RETENTION_DAYS = int(
    os.getenv('GFW_HOURLY_FAILED_SPOOL_RETENTION_DAYS', '7')
)
GFW_HOURLY_S3_PREFIX = os.getenv(
    'GFW_HOURLY_S3_PREFIX', 'deploy-assets/global-maritime/gfw-hourly'
).strip('/')
# Must map exactly to the public Cloudflare origin path ending in
# /global-maritime/gfw-hourly; no public default is safe to guess.
GFW_HOURLY_PUBLIC_URL_PREFIX = os.getenv('GFW_HOURLY_PUBLIC_URL_PREFIX', '').rstrip('/')
GFW_HOURLY_SPOOL_DIR = LOCAL_DATA_DIR / 'gfw_hourly_publish_spool'

# AISStream BoundingBoxes 格式為 [[[lat_min, lon_min], [lat_max, lon_max]], ...]。
# 五個區域刻意保留獨立標籤，日後可依區域比較涵蓋率；collector 仍只開一條 WebSocket。
AISSTREAM_BBOXES = os.getenv(
    'AISSTREAM_BBOXES',
    '[{"name":"taiwan_north_east","box":[[23.3,120.5],[26.5,123.5]]},'
    '{"name":"yonaguni_ishigaki","box":[[23.5,122.0],[25.5,125.5]]},'
    '{"name":"miyako_okinawa","box":[[24.4,124.0],[27.6,129.0]]},'
    '{"name":"amami","box":[[27.0,127.0],[30.2,131.0]]},'
    '{"name":"kyushu_southwest","box":[[30.0,128.5],[34.5,132.8]]}]'
)

# ------------------------------------------------------------
# 各 collector 的「額外設定」（city list、API 金鑰、參數）
# ------------------------------------------------------------

# 食品價格（農業部四類批發價）
# 每次抓最近 N 天而非只抓昨天——上游偶有補登；DB 端 UNIQUE DO NOTHING 吸收重複。
# 加大此值會等比放大請求數（蔬果走市場 × 類別迴圈），7 天約 52 次請求。
FOOD_PRICES_LOOKBACK_DAYS = int(os.getenv('FOOD_PRICES_LOOKBACK_DAYS', '7'))

# 動物認領養：首輪（2026-08-19）為 8,190 rows、root JSON array，不能把 API 異常
# 或部分回應當作真實 0。啟用前可依上游長期基準調整，但不得設為 0。
ANIMAL_ADOPTION_MIN_ROWS = int(os.getenv('ANIMAL_ADOPTION_MIN_ROWS', '1000'))
ANIMAL_ADOPTION_HTTP_RETRIES = int(os.getenv('ANIMAL_ADOPTION_HTTP_RETRIES', '3'))

# 動物收容成果／壓力月報：不能把上游空殼或部分回應當成「全國 0」。
ANIMAL_SHELTER_OUTCOMES_MIN_ROWS = int(os.getenv('ANIMAL_SHELTER_OUTCOMES_MIN_ROWS', '3000'))
ANIMAL_SHELTER_PRESSURE_MIN_ROWS = int(os.getenv('ANIMAL_SHELTER_PRESSURE_MIN_ROWS', '1800'))
ANIMAL_SHELTER_HTTP_RETRIES = int(os.getenv('ANIMAL_SHELTER_HTTP_RETRIES', '3'))
ANIMAL_VETERINARY_CLINICS_MIN_ROWS = int(os.getenv('ANIMAL_VETERINARY_CLINICS_MIN_ROWS', '1800'))
ANIMAL_LICENSED_PET_BUSINESSES_MIN_ROWS = int(os.getenv('ANIMAL_LICENSED_PET_BUSINESSES_MIN_ROWS', '5000'))
ANIMAL_PROTECTION_OFFICES_MIN_ROWS = int(os.getenv('ANIMAL_PROTECTION_OFFICES_MIN_ROWS', '22'))
ANIMAL_WELFARE_POINTS_HTTP_RETRIES = int(os.getenv('ANIMAL_WELFARE_POINTS_HTTP_RETRIES', '3'))

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

# 停車靜態座標 ref（半靜態月更）— 路邊三城（台北有 POLYGON geom）
PARKING_REF_ONSTREET_CITIES = os.getenv(
    'PARKING_REF_ONSTREET_CITIES', 'Taipei,NewTaipei,Taichung'
).split(',')
# 場外高覆蓋城市（PK1 驗證命中率 99%+：台中/高雄/台南/花蓮/金門/桃園/宜蘭；台北 10% 也收）
# 另固定抓國道服務區(NFB) + 觀光景點(TBROC) 兩個全國端點
PARKING_REF_OFFSTREET_CITIES = os.getenv(
    'PARKING_REF_OFFSTREET_CITIES',
    'Taichung,Kaohsiung,Tainan,HualienCounty,KinmenCounty,Taoyuan,YilanCounty,Taipei'
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

    if GFW_HOURLY_PUBLISH_ENABLED:
        if not GFW_HOURLY_REDISTRIBUTION_APPROVED:
            errors.append("GFW hourly publish 需明確設 GFW_HOURLY_REDISTRIBUTION_APPROVED=true")
        if GFW_VESSEL_PRESENCE_ENABLED:
            errors.append(
                "GFW_HOURLY_PUBLISH_ENABLED 與舊 GFW_VESSEL_PRESENCE_ENABLED 不得同時開啟"
            )
        required = {
            'GFW_ACCESS_TOKEN': GFW_ACCESS_TOKEN,
            'SUPABASE_DB_URL': SUPABASE_DB_URL,
            'S3_BUCKET': S3_BUCKET,
            'GFW_HOURLY_PUBLIC_URL_PREFIX': GFW_HOURLY_PUBLIC_URL_PREFIX,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            errors.append(f"GFW hourly publish 缺少: {', '.join(missing)}")

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
