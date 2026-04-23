# Data Collectors

定期自動化資料收集服務，部署於 Zeabur 24hr 運作。支援 S3 歸檔與資料生命週期管理。

## 收集器總覽

共 30 個收集器，每個都可獨立啟停。

| 收集器 | `_ENABLED` 環境變數 | 預設 | 頻率 | 來源 | 說明 |
|--------|---------------------|------|------|------|------|
| YouBike | `YOUBIKE_ENABLED` | `true` | 15 min | TDX | 即時車位 |
| Weather | `WEATHER_ENABLED` | `true` | 60 min | CWA | 氣象觀測站 |
| VD | `VD_ENABLED` | **false** | 5 min | TDX | 縣市道路車輛偵測器 |
| Freeway VD | `FREEWAY_VD_ENABLED` | `true` | 10 min | TDX | 國道即時車流+壅塞 |
| Temperature | `TEMPERATURE_ENABLED` | `true` | 60 min | CWA | 溫度網格 |
| Parking | `PARKING_ENABLED` | **false** | 15 min | TDX | 路邊停車即時可用性 |
| Bus | `BUS_ENABLED` | `true` | 2 min | TDX | 市區公車即時位置（22 縣市） |
| Bus InterCity | `BUS_INTERCITY_ENABLED` | **false** | 2 min | TDX | 公路客運 / 國道客運（跨縣市） |
| TRA Train | `TRA_TRAIN_ENABLED` | `true` | 2 min | TDX | 台鐵即時列車位置 |
| TRA Static | `TRA_STATIC_ENABLED` | `true` | 1440 min | TDX | 台鐵靜態資料（每日） |
| Rail Timetable | `RAIL_TIMETABLE_ENABLED` | `true` | 1440 min | TDX | 台鐵+高鐵每日時刻表歸檔 |
| Ship TDX | `SHIP_TDX_ENABLED` | **false** | 2 min | TDX | 國內航線船位 |
| Ship AIS | `SHIP_AIS_ENABLED` | `true` | 10 min | 航港局 | AIS 船位追蹤 |
| Flight FR24 | `FLIGHT_FR24_ENABLED` | **false** | 5 min | FR24 | 台灣機場航班完整軌跡 |
| FR24 Zone | `FLIGHT_FR24_ZONE_ENABLED` | **false** | 5 min | FR24 | 空域快照（最多飛機+起降機場） |
| OpenSky | `FLIGHT_OPENSKY_ENABLED` | **false** | 5 min | OpenSky | 空域快照（精確高度+垂直速率） |
| Earthquake | `EARTHQUAKE_ENABLED` | `true` | 1440 min | CWA | 有感地震 + 完整地震目錄（每日） |
| Satellite | `SATELLITE_ENABLED` | **false** | 120 min | CelesTrak | 全球衛星軌道追蹤（SGP4） |
| Launch | `LAUNCH_ENABLED` | **false** | 5 min | Launch Library 2 | 全球太空發射任務 |
| CWA Satellite | `CWA_SATELLITE_ENABLED` | `true` | 10 min | CWA | 衛星雲圖 + 雷達回波 PNG |
| **NCDR Alerts** | `NCDR_ALERTS_ENABLED` | `true` | 15 min | NCDR | **災害示警 CAP（颱風/豪雨/強風/枯旱/水庫等）** |
| Foursquare POI | `FOURSQUARE_POI_ENABLED` | **false** | 43200 min | HuggingFace | Foursquare OS Places POI 全量快照（每月） |
| Air Quality Imagery | `AIR_QUALITY_IMAGERY_ENABLED` | **false** | 60 min | airtw | 全台空品色階圖 PNG（AQI/PM2.5/PM10/O3/NO2） |
| Air Quality | `AIR_QUALITY_ENABLED` | **false** | 60 min | MOENV | 環境部 77 站即時 AQI 觀測 |
| Air Quality MicroSensors | `AIR_QUALITY_MICROSENSORS_ENABLED` | **false** | 5 min | LASS | AirBox 微型感測器網路 |
| Water Reservoir | `WATER_RESERVOIR_ENABLED` | **false** | 60 min | WRA 水利署 | 全台水庫水情 |
| River Water Level | `RIVER_WATER_LEVEL_ENABLED` | **false** | 10 min | WRA | 河川水位站即時觀測 |
| Rain Gauge Realtime | `RAIN_GAUGE_REALTIME_ENABLED` | **false** | 10 min | CWA | 即時雨量站（需 `CWA_API_KEY`） |
| Groundwater Level | `GROUNDWATER_LEVEL_ENABLED` | **false** | 60 min | WRA | 地下水水位站 |
| Water Reservoir Daily Ops | `WATER_RESERVOIR_DAILY_OPS_ENABLED` | **false** | 1440 min | WRA | 水庫每日營運狀況（每日 09:30 更新） |

> 預設 **false** 的收集器需手動啟用（設定環境變數為 `true`）。

## 執行架構

### CollectorScheduler（統一調度，Phase 1 升級）

所有 collector 皆透過 `CollectorScheduler` 以 ThreadPoolExecutor 平行執行：

- **每個 collector 獨立 thread**：互不阻塞（慢打的 `flight_fr24` 不會拖慢 `bus`、`ncdr_alerts`）
- **Skip-if-running 保護**：同一 collector 仍在跑時，下個 tick 觸發會跳過，避免疊加
- **`schedule` 套件僅負責觸發**：實際執行統一交給 scheduler pool
- **Pool 大小**：`max(10, collector 數量 + 2)`

舊的「主排程 + 背景 thread `BACKGROUND_COLLECTORS`」雙軌模型已淘汰，現在全部走 scheduler。

### Supabase 旁路寫入

每次 collector 完成後，除了寫入本地檔案，還會旁路寫一份到 Supabase（PostGIS）：

- **不影響本地儲存流程**：DB 寫入失敗時 collector 仍視為成功
- **失敗自動 buffer**：寫入失敗的資料會暫存到 `data/buffer/*.json`
- **定期重試**：每 `SUPABASE_BUFFER_INTERVAL`（預設 5 分鐘）排程 flush buffer
- **連續錯誤告警**：DB 連續失敗 3 次後 Telegram 通知
- **Thread-safe**：`SupabaseWriter` 內部用 `RLock` 保護，背景 thread 與主 thread 共用安全

### 飛機三源互補

三個飛機收集器各有優勢，都有 `icao24` 欄位可直接 merge：

| 收集器 | 角色 | 飛機數/輪 | 優勢 | 缺失 |
|--------|------|-----------|------|------|
| Flight FR24 | 台灣起降航班 | ~20-50 | 完整 trail 軌跡 | 只追蹤台灣機場起降 |
| FR24 Zone | 空域快照 | ~120 | 最多飛機、有 origin/dest | 無軌跡 |
| OpenSky | 空域快照 | ~65 | 精確高度、垂直速率 | 無 origin/destination |

## 專案結構

```
data-collectors/
├── README.md
├── docs/                   # 詳細文件
│   ├── API.md             # API 詳細文件
│   ├── ARCHITECTURE.md    # 架構與歸檔流程
│   └── S3_SETUP.md        # S3 設定指南
├── requirements.txt
├── Dockerfile
├── zeabur.json
├── .env.example            # 環境變數範本
│
├── main.py                 # 主程式入口（統一排程器）
├── config.py               # 共用設定
│
├── collectors/             # 各資料收集器
│   ├── __init__.py
│   ├── base.py            # 收集器基底類別
│   ├── youbike.py         # YouBike 即時車位
│   ├── weather.py         # 氣象觀測站資料（CWA）
│   ├── vd.py              # VD 車輛偵測器
│   ├── freeway_vd.py      # 國道即時車流+壅塞（TDX）
│   ├── temperature.py     # 溫度網格資料（CWA）
│   ├── parking.py         # 路邊停車即時可用性
│   ├── bus.py             # 市區公車即時位置（TDX，六都）
│   ├── bus_intercity.py   # 公路客運/國道客運即時位置（TDX InterCity）
│   ├── tra_train.py       # 台鐵即時列車位置（TDX）
│   ├── tra_static.py      # 台鐵靜態資料（TDX）
│   ├── rail_timetable.py  # 台鐵+高鐵每日時刻表歸檔（TDX）
│   ├── ship_tdx.py        # TDX 國內航線船位
│   ├── ship_ais.py        # 航港局 AIS 船位
│   ├── flight_fr24.py     # FlightRadar24 航班軌跡
│   ├── flight_fr24_zone.py # FR24 Zone 空域快照
│   ├── flight_opensky.py  # OpenSky 空域快照
│   ├── earthquake.py      # CWA 地震報告 + 完整目錄
│   ├── satellite.py       # 全球衛星軌道追蹤（CelesTrak + SGP4）
│   ├── launch.py          # 太空發射任務（Launch Library 2）
│   ├── cwa_satellite.py   # CWA 衛星雲圖 + 雷達回波 PNG
│   ├── ncdr_alerts.py     # NCDR 災害示警 CAP feed
│   ├── foursquare_poi.py  # Foursquare OS Places POI（HuggingFace parquet）
│   ├── air_quality_imagery.py      # airtw 全台空品色階圖 PNG
│   ├── air_quality.py              # 環境部 77 站即時 AQI
│   ├── air_quality_microsensors.py # LASS AirBox 微型感測器
│   ├── water_reservoir.py          # 水利署水庫水情
│   ├── river_water_level.py        # 水利署河川水位
│   ├── rain_gauge_realtime.py      # CWA 即時雨量站
│   ├── groundwater_level.py        # 水利署地下水水位
│   └── water_reservoir_daily_ops.py # 水利署水庫每日營運狀況
│
├── storage/                # 儲存後端
│   ├── __init__.py
│   ├── local.py           # 本地檔案儲存
│   ├── s3.py              # AWS S3 儲存與歸檔
│   └── supabase_writer.py # Supabase 旁路寫入（thread-safe + buffer）
│
├── tasks/                  # 排程任務
│   ├── __init__.py
│   └── archive.py         # S3 歸檔任務
│
├── utils/                  # 共用工具
│   ├── __init__.py
│   ├── auth.py            # API 認證（TDX、CWA）
│   └── notify.py          # 通知（Webhook、LINE）
│
├── api/                    # HTTP API（下載資料）
│   ├── __init__.py
│   └── server.py          # Flask API Server
│
└── data/                   # 本地資料（開發用）
    └── .gitkeep
```

## 快速開始

### 本地開發

```bash
# 安裝依賴
pip install -r requirements.txt

# 設定環境變數
cp .env.example .env
# 編輯 .env 填入 API 金鑰

# 執行
python main.py
```

### 部署到 Zeabur

1. 推送到 GitHub
2. 在 Zeabur 建立專案，連結 repo
3. 設定環境變數（見下方）
4. 部署

## 環境變數

### 必填

| 變數 | 說明 |
|------|------|
| `TDX_APP_ID` | TDX API Client ID |
| `TDX_APP_KEY` | TDX API Client Secret |
| `CWA_API_KEY` | 氣象局 API Key（Weather、Temperature 需要） |

### 選填

| 變數 | 預設 | 說明 |
|------|------|------|
| `API_KEY` | | HTTP API 認證金鑰（設定才會啟動 API Server） |
| `API_PORT` | `8080` | HTTP API 端口 |
| `S3_BUCKET` | | S3 儲存桶（啟用歸檔必填） |
| `S3_ACCESS_KEY` | | AWS Access Key |
| `S3_SECRET_KEY` | | AWS Secret Key |
| `S3_REGION` | `ap-southeast-2` | S3 區域 |
| `S3_ENDPOINT` | | S3 相容 endpoint（MinIO 等） |
| `WEBHOOK_URL` | | 通知 Webhook（Discord、Slack） |
| `LINE_TOKEN` | | LINE Notify Token |
| `TELEGRAM_BOT_TOKEN` | | Telegram Bot Token（每日報告 + 連續錯誤告警） |
| `TELEGRAM_CHAT_ID` | | Telegram Chat ID |
| `SUPABASE_ENABLED` | `false` | 啟用 Supabase 旁路寫入 |
| `SUPABASE_DB_URL` | | Supavisor Transaction mode (port 6543) |
| `SUPABASE_BUFFER_INTERVAL` | `5` | Buffer flush 間隔（分鐘） |

### 歸檔設定

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `ARCHIVE_ENABLED` | `true` | 是否啟用 S3 歸檔 |
| `ARCHIVE_RETENTION_DAYS` | `7` | 本地資料保留天數 |
| `ARCHIVE_TIME` | `03:00` | 每日歸檔執行時間 |

### 收集器開關與設定

每個收集器都有 `_ENABLED` 開關和 `_INTERVAL` 頻率設定。

#### 陸運

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `YOUBIKE_ENABLED` | `true` | YouBike 即時車位 |
| `YOUBIKE_CITIES` | `Taipei,NewTaipei,Taoyuan` | 收集城市 |
| `YOUBIKE_INTERVAL` | `15` | 間隔（分鐘） |
| `VD_ENABLED` | `false` | 縣市道路 VD 車輛偵測器 |
| `VD_CITIES` | `Taipei,NewTaipei` | 收集城市 |
| `VD_INTERVAL` | `5` | 間隔（分鐘） |
| `FREEWAY_VD_ENABLED` | `true` | 國道即時車流+壅塞 |
| `FREEWAY_VD_INTERVAL` | `10` | 間隔（分鐘） |
| `PARKING_ENABLED` | `false` | 路邊停車即時可用性 |
| `PARKING_CITIES` | `Taipei,NewTaipei,Taichung` | 收集城市 |
| `PARKING_INTERVAL` | `15` | 間隔（分鐘） |
| `BUS_ENABLED` | `true` | 市區公車即時位置 |
| `BUS_CITIES` | 全台 22 縣市 | 收集城市（留空用預設 22 縣市清單） |
| `BUS_INTERVAL` | `2` | 間隔（分鐘，22 城擴充後預設值） |
| `BUS_FETCH_WORKERS` | `5` | 並行抓城市的 worker 數 |
| `BUS_INTERCITY_ENABLED` | `false` | 公路客運 / 國道客運即時位置 |
| `BUS_INTERCITY_INTERVAL` | `2` | 間隔（分鐘，全台單一 endpoint，不需指定城市） |

#### 鐵路

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `TRA_TRAIN_ENABLED` | `true` | 台鐵即時列車位置 |
| `TRA_TRAIN_INTERVAL` | `2` | 間隔（分鐘） |
| `TRA_STATIC_ENABLED` | `true` | 台鐵靜態資料（車站、路線等） |
| `TRA_STATIC_INTERVAL` | `1440` | 間隔（分鐘，1440=每日） |
| `RAIL_TIMETABLE_ENABLED` | `true` | 台鐵+高鐵每日時刻表歸檔 |
| `RAIL_TIMETABLE_INTERVAL` | `1440` | 間隔（分鐘，1440=每日） |

#### 航運

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `SHIP_TDX_ENABLED` | `false` | TDX 國內航線船位 |
| `SHIP_TDX_INTERVAL` | `2` | 間隔（分鐘） |
| `SHIP_AIS_ENABLED` | `true` | 航港局 AIS 船位 |
| `SHIP_AIS_INTERVAL` | `10` | 間隔（分鐘） |

#### 航空

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `FLIGHT_FR24_ENABLED` | `false` | FR24 台灣機場航班軌跡 |
| `FLIGHT_FR24_INTERVAL` | `5` | 間隔（分鐘） |
| `FLIGHT_FR24_AIRPORTS` | 17 個台灣機場 ICAO | 掃描機場清單 |
| `FLIGHT_FR24_TRAIL_DELAY` | `3` | trail 請求間隔（秒） |
| `FLIGHT_FR24_ZONE_ENABLED` | `false` | FR24 Zone 空域快照 |
| `FLIGHT_FR24_ZONE_INTERVAL` | `5` | 間隔（分鐘） |
| `FLIGHT_FR24_ZONE_LAMIN` | `21` | bbox 南界緯度 |
| `FLIGHT_FR24_ZONE_LAMAX` | `27` | bbox 北界緯度 |
| `FLIGHT_FR24_ZONE_LOMIN` | `117` | bbox 西界經度 |
| `FLIGHT_FR24_ZONE_LOMAX` | `123` | bbox 東界經度 |
| `FLIGHT_OPENSKY_ENABLED` | `false` | OpenSky 空域快照 |
| `FLIGHT_OPENSKY_INTERVAL` | `5` | 間隔（分鐘） |
| `FLIGHT_OPENSKY_CLIENT_ID` | | OAuth2 Client ID |
| `FLIGHT_OPENSKY_CLIENT_SECRET` | | OAuth2 Client Secret |
| `FLIGHT_OPENSKY_USERNAME` | | Basic Auth 帳號（二擇一） |
| `FLIGHT_OPENSKY_PASSWORD` | | Basic Auth 密碼 |

#### 衛星與太空

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `SATELLITE_ENABLED` | `false` | 衛星軌道追蹤（CelesTrak + SGP4） |
| `SATELLITE_INTERVAL` | `120` | 間隔（分鐘，配合 CelesTrak 2hr 更新） |
| `LAUNCH_ENABLED` | `false` | 太空發射任務（Launch Library 2） |
| `LAUNCH_INTERVAL` | `5` | 間隔（分鐘，每次只 1 個 API call） |
| `LAUNCH_API_TOKEN` | | Patreon 付費 token（可選） |

#### 氣象

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `WEATHER_ENABLED` | `true` | 氣象觀測站 |
| `WEATHER_INTERVAL` | `60` | 間隔（分鐘） |
| `TEMPERATURE_ENABLED` | `true` | 溫度網格 |
| `TEMPERATURE_INTERVAL` | `60` | 間隔（分鐘） |
| `CWA_SATELLITE_ENABLED` | `true` | 衛星雲圖 + 雷達回波 PNG |
| `CWA_SATELLITE_INTERVAL` | `10` | 間隔（分鐘） |
| `CWA_SATELLITE_DATASETS` | | 自訂 dataset id 清單（逗號分隔），空值用內建預設 |

#### 災害與安全

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `EARTHQUAKE_ENABLED` | `true` | CWA 有感地震 + 完整地震目錄 |
| `EARTHQUAKE_INTERVAL` | `1440` | 間隔（分鐘，每日一次） |
| `NCDR_ALERTS_ENABLED` | `true` | NCDR 災害示警 CAP feed（無需 API key） |
| `NCDR_ALERTS_INTERVAL` | `15` | 間隔（分鐘） |

#### 空氣品質

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `AIR_QUALITY_IMAGERY_ENABLED` | `false` | airtw 全台色階圖 PNG |
| `AIR_QUALITY_IMAGERY_INTERVAL` | `60` | 間隔（分鐘） |
| `AIR_QUALITY_IMAGERY_PRODUCTS` | `AQI,PM25,PM10,O3,NO2` | 色階圖產品清單 |
| `AIR_QUALITY_ENABLED` | `false` | 環境部 77 站即時 AQI（需 `MOENV_API_KEY`） |
| `AIR_QUALITY_INTERVAL` | `60` | 間隔（分鐘） |
| `AIR_QUALITY_MICROSENSORS_ENABLED` | `false` | LASS AirBox 微型感測器 |
| `AIR_QUALITY_MICROSENSORS_INTERVAL` | `5` | 間隔（分鐘） |
| `MOENV_API_KEY` | | 環境部 API Key（Air Quality 需要） |

#### 水文

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `WATER_RESERVOIR_ENABLED` | `false` | 水利署水庫水情 |
| `WATER_RESERVOIR_INTERVAL` | `60` | 間隔（分鐘，對應 WRA 每小時更新） |
| `RIVER_WATER_LEVEL_ENABLED` | `false` | 水利署河川水位 |
| `RIVER_WATER_LEVEL_INTERVAL` | `10` | 間隔（分鐘） |
| `RAIN_GAUGE_REALTIME_ENABLED` | `false` | CWA 即時雨量站（需 `CWA_API_KEY`） |
| `RAIN_GAUGE_REALTIME_INTERVAL` | `10` | 間隔（分鐘） |
| `GROUNDWATER_LEVEL_ENABLED` | `false` | 水利署地下水水位 |
| `GROUNDWATER_LEVEL_INTERVAL` | `60` | 間隔（分鐘） |
| `WATER_RESERVOIR_DAILY_OPS_ENABLED` | `false` | 水庫每日營運狀況（進流量、放流量、蓄水量等） |
| `WATER_RESERVOIR_DAILY_OPS_INTERVAL` | `1440` | 間隔（分鐘，每日一次，官方 09:30 前更新） |

#### POI

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `FOURSQUARE_POI_ENABLED` | `false` | Foursquare OS Places POI（HuggingFace parquet，耗時，一次全量） |
| `FOURSQUARE_POI_INTERVAL` | `43200` | 間隔（分鐘，43200 = 每 30 天） |
| `HF_TOKEN` | | HuggingFace API Token |

## 收集器說明

### YouBike 即時車位
- **來源**: TDX API `/v2/Bike/Availability/{City}`
- **範圍**: 臺北市、新北市、桃園市
- **資料量**: ~3,800 站/次

### 氣象觀測站資料
- **來源**: CWA API `O-A0001-001`
- **資料類型**: 即時觀測（溫度、雨量、風速、氣壓等）
- **資料量**: ~700 測站

### VD 車輛偵測器
- **來源**: TDX API `/v2/Road/Traffic/VD/{City}`
- **範圍**: 臺北市、新北市
- **資料類型**: 車流量、車速

### 國道即時車流+壅塞
- **來源**: TDX API `/v2/Road/Traffic/VD/Freeway` + `/v1/Road/Traffic/Live/Freeway`
- **資料類型**: 國道各偵測器車流量、車速、壅塞等級

### 溫度網格資料
- **來源**: CWA File API `O-A0038-003`
- **資料類型**: 小時溫度觀測分析格點資料
- **解析度**: 0.03 度（約 3.3 公里）
- **資料量**: ~50,000 格點

### 路邊停車即時可用性
- **來源**: TDX API `/v1/Parking/OnStreet/ParkingSegmentAvailability/{City}`
- **範圍**: 臺北市、新北市、臺中市
- **資料量**: ~4,600 路段

### 市區公車即時位置
- **來源**: TDX API `/v2/Bus/RealTimeByFrequency/City/{City}`
- **範圍**: 依 `BUS_CITIES` 設定（預設六都）
- **資料類型**: 公車即時 GPS 座標、速度、方位角、路線、方向
- **過濾條件**: 以「有 BusPosition 經緯度」為準，不依賴 `DutyStatus` / `BusStatus`（各縣市業者填寫習慣不一致，桃中高將 `DutyStatus=0` 當執勤中，與 TDX 官方定義相反）
- **Supabase 表**: `realtime.bus_positions`（分區歷史，保留 3 天）+ `realtime.bus_current`（依 `plate_numb` UPSERT）
- **RPC**: `public.get_bus_current(cities text[])` / `public.get_bus_trails(date, cities text[])`

### 公路客運 / 國道客運即時位置
- **來源**: TDX API `/v2/Bus/RealTimeByFrequency/InterCity`（全台單一 endpoint，不需指定城市）
- **範圍**: 跨縣市公路客運、國道客運（統聯、國光、和欣等 40+ 家業者）
- **資料類型**: 同市區公車，欄位結構一致
- **`city` 欄位語意**: 存業者代號（`OperatorID`），而非城市
- **過濾條件**: 同市區公車，以 GPS 位置為準
- **Supabase 表**: `realtime.bus_intercity_positions`（分區歷史）+ `realtime.bus_intercity_current`
- **RPC**: `public.get_bus_intercity_current(sub_authorities text[])`
- **資料量**: ~1,700 台執勤中（全台即時）

### 台鐵即時列車位置
- **來源**: TDX API
- **資料類型**: 即時列車位置與班次

### 台鐵靜態資料
- **來源**: TDX API
- **資料類型**: 車站、路線、站序、車種、當日時刻表

### 台鐵+高鐵每日時刻表歸檔
- **來源**: TDX API `/v3/Rail/TRA/DailyTrainTimetable/Today` + `/v2/Rail/THSR/DailyTimetable/Today`
- **資料類型**: 每日時刻表（含停駛/加班車標記）
- **資料量**: 台鐵 ~950 班 + 高鐵 ~160 班
- **說明**: TDX DailyTimetable 每天更新，歷史僅保留約 90 天，需每日歸檔留存

### 航港局 AIS 船位
- **來源**: 航港局 AIS 開放資料
- **資料類型**: 台灣周邊船舶即時位置

### TDX 國內航線船位
- **來源**: TDX API
- **資料類型**: 國內航線船舶位置

### FlightRadar24 航班軌跡
- **來源**: FlightRadar24（非官方 API）
- **範圍**: 台灣 17 個民航機場
- **資料類型**: 完整飛行軌跡（含經緯度、高度、時間戳記）
- **運作機制**:
  - arrivals：掃描已降落航班，立即抓取完整 trail
  - departures：記錄至 pending 追蹤清單，確認降落後抓取 trail
- **防封鎖**: User-Agent 輪替、隨機抖動等待間隔

### FR24 Zone 空域快照
- **來源**: FlightRadar24 公開 feed endpoint（無需 API key）
- **範圍**: 台灣空域 bbox（21-27N, 117-123E）
- **資料類型**: 即時飛機位置、origin/destination IATA
- **資料量**: ~120 架/次
- **防封鎖**: 獨立 User-Agent 池（與 Flight FR24 不同）

### OpenSky 空域快照
- **來源**: OpenSky Network API
- **範圍**: 台灣空域 bbox
- **資料類型**: state vectors（位置、高度、垂直速率、squawk 等）
- **認證**: OAuth2 > Basic Auth > 匿名（匿名 400 credits/天）

### 衛星軌道追蹤
- **來源**: CelesTrak GP 3LE（免註冊）
- **範圍**: 全球活躍衛星（~2,000 顆，不含 Starlink）
- **資料類型**: SGP4 計算的即時位置（經緯度、高度、速度）+ TLE 軌道參數
- **軌道分佈**: LEO ~1,200 / GEO ~600 / MEO ~140 / HEO ~12
- **Supabase 表**: `satellite_positions`（分區歷史）、`satellite_current`（最新狀態）、`satellite_tle`（前端計算用）
- **前端應用**: 載入 `satellite_tle` 表的 TLE 參數，用 JS 版 `satellite.js` 在瀏覽器即時計算位置與軌道預測線
- **擴充**: 如需 Starlink ~10,000 顆，需註冊 Space-Track.org 帳號

### 太空發射任務
- **來源**: Launch Library 2 (`thespacedevs.com`)
- **資料類型**: 即將/歷史發射任務（火箭、發射台、機構、軌道、影像）
- **Supabase 表**: `launches` / `launch_pads` / `launch_events`
- **歷史回溯**: 啟動時自動分批 backfill，每次抓 100 筆（避免 rate limit）

### 地震報告
- **來源**: CWA `E-A0015-001` (顯著有感) + `E-A0016-001` (小區域有感) + `E-A0073-001` (完整目錄含無感)
- **資料類型**: 震央、深度、規模、各測站震度、報告影像
- **Supabase 表**: `earthquake_events`（依 `event_id` UPSERT）

### CWA 衛星雲圖 + 雷達回波
- **來源**: CWA File API（衛星 `O-C0042-*`、雷達 `O-A0058-*` 等）
- **資料類型**: PNG 圖檔 + metadata
- **預設 datasets**: 由 collector 內 `DEFAULT_DATASETS` 控制，可用 `CWA_SATELLITE_DATASETS` 覆寫

### NCDR 災害示警 (CAP)
- **來源**: NCDR 災害示警公開資料平台 `https://alerts.ncdr.nat.gov.tw/JSONAtomFeeds.ashx`（無需 API key）
- **資料類型**: 颱風、地震、豪大雨、強風、低溫、土石流、淹水、水庫放流、枯旱等示警
- **格式**: CAP (Common Alerting Protocol) v1.2 → 解析 polygon → MULTIPOLYGON (WGS84)
- **時效欄位**: `sent / effective / onset / expires`
- **Supabase 表**: `realtime.disaster_alerts`（PK = `identifier`，UPSERT 模式，過期不刪除以累積歷史）
- **便利視圖**: `realtime.disaster_alerts_active` 過濾出生效中示警
- **特性**: feed 只列「目前生效」示警，過期會從 feed 消失 → 收集器持續累積即建立完整歷史庫

## 資料儲存與歸檔

### 本地優先 + tar.gz 歸檔

收集器永遠寫入本地（LocalStorage），S3 歸檔由 ArchiveTask 以 tar.gz 批次上傳。
PUT 請求從 ~5,000/天 降至 ~10/天（99.8% 降幅）。

| 層級 | 儲存位置 | 資料範圍 | 用途 |
|------|----------|----------|------|
| 熱資料 | Zeabur Volume | 最近 7 天 | 即時存取（個別 JSON） |
| 冷資料 | AWS S3 | 全部歷史 | 永久歸檔（tar.gz） |

### 儲存結構
```
data/                           # 本地 (最近 7 天，個別 JSON)
├── youbike/ weather/ vd/ freeway_vd/ temperature/ parking/
├── bus/ bus_intercity/ tra_train/ tra_static/ rail_timetable/
├── ship_tdx/ ship_ais/
├── flight_fr24/ flight_fr24_zone/ flight_opensky/
├── satellite/ launch/ cwa_satellite/ earthquake/ ncdr_alerts/
├── foursquare_poi/
├── air_quality_imagery/ air_quality/ air_quality_microsensors/
└── water_reservoir/ river_water_level/ rain_gauge_realtime/ groundwater_level/ water_reservoir_daily_ops/

s3://bucket/                   # S3 (永久歸檔，tar.gz)
└── {collector}/
    └── archives/
        └── YYYY-MM-DD.tar.gz  # 每天 1 個 PUT
```

### S3 Lifecycle（冷儲存分層）

生產 bucket `migu-gis-data-collector` 已套用 lifecycle rule `tiered-cold-storage`，**資料不刪除、只分層**：

| 物件年齡 | Storage Class | 相對成本 |
|---------|---------------|---------|
| 0–30 天 | STANDARD | 100% |
| 30–90 天 | STANDARD_IA | ~60% |
| 90+ 天 | GLACIER_IR | ~25% |

- **毫秒存取**：Glacier Instant Retrieval 不需 restore，API / presigned URL 無需改動
- **不完整 multipart 清理**：超過 7 天自動清除殘片（僅影響失敗的上傳）
- **驗證**：AWS Console → S3 → `migu-gis-data-collector` → Management 分頁

套用方式（若需複製到其他 bucket）：

```python
import boto3
s3 = boto3.client('s3', region_name='ap-southeast-2')
s3.put_bucket_lifecycle_configuration(
    Bucket='<bucket-name>',
    LifecycleConfiguration={
        'Rules': [{
            'ID': 'tiered-cold-storage',
            'Status': 'Enabled',
            'Filter': {'Prefix': ''},
            'Transitions': [
                {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                {'Days': 90, 'StorageClass': 'GLACIER_IR'},
            ],
            'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7},
        }]
    },
)
```

### 歸檔流程

每日 03:00 自動執行：
1. 遍歷各收集器的 YYYY/MM/DD 日期目錄（跳過今天）
2. 壓成 `collector/archives/YYYY-MM-DD.tar.gz`
3. 上傳 1 個 PUT 到 S3（每個收集器每天 1 個）
4. 刪除超過保留天數且 S3 已有歸檔的日期目錄
5. 清理空目錄

## Supabase RPC 預聚合（docs/sql/）

為避開 Supabase Supavisor pooler 強制的 2 分鐘 statement_timeout，以及讓前端讀取穩定落在百毫秒級，所有高頻時序 RPC 都採「普通 table + per-day refresh function + pg_cron + 薄 SELECT RPC」pattern：

| RPC | SQL 檔 | Cron | Before → After |
|---|---|---|---|
| `get_ship_trails` | `matview_ship_trails.sql` | `*/10` | timeout → 123ms |
| `get_flight_trails` | `matview_flight_trails.sql` | `*/10` | timeout → 126ms |
| `get_freeway_congestion_day` | `matview_freeway_congestion.sql` | `*/10` | 60s → 302ms |
| `get_youbike_h3_snapshots` | `matview_youbike_h3.sql` | `*/10` | 6.4s → 82ms |
| `get_temperature_frames` | `matview_temperature_frames.sql` | `*/30` | 551ms → 107ms |
| `get_temperature_dates` | `matview_temperature_dates.sql` | `*/15` | 1.9s → 72ms |
| `get_temperature_grid_info` | `reference_temperature_grid.sql`（靜態） | — | 1.08s → 269ms |
| `get_disaster_alerts_day` | `matview_disaster_alerts.sql` | `*/10` | **13.2s → 110ms** |
| `get_cwa_imagery_frames_batch` | `cwa_imagery_rpcs.sql` | — | `Failed to fetch` → 57MB/1.7s |

每個 refresh function 內含 `pg_advisory_xact_lock` 避免 cron 與手動呼叫 race condition，cleanup function 每日 18:00 UTC 清超過 7 天的舊 row。檔名叫 `matview_*` 是歷史包袱，實際是普通 table。

套用方式：`psql "$SUPABASE_DB_URL" -f docs/sql/matview_xxx.sql`，之後 `SELECT public.refresh_xxx(d::date) FROM generate_series(current_date - 6, current_date, '1 day') d;` backfill。

詳細架構說明請參閱 [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)

首次設定 S3 請參閱 [docs/S3_SETUP.md](./docs/S3_SETUP.md)

## HTTP API

設定 `API_KEY` 環境變數後，會自動啟動 HTTP API Server。

詳細文件請參閱 [docs/API.md](./docs/API.md)

```bash
# 健康檢查（無需認證）
curl https://your-app.zeabur.app/health

# 列出所有收集器
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/collectors

# 取得最新資料
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/latest

# 列出可用日期（包含 S3 歷史資料）
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/dates
```

## 開發新收集器

1. 在 `collectors/` 建立新模組
2. 繼承 `BaseCollector` 類別
3. 實作 `collect()` 方法
4. 在 `config.py` 新增 `_ENABLED` 和 `_INTERVAL` 設定
5. 在 `collectors/__init__.py` 註冊
6. 在 `main.py` 初始化並加入排程

```python
from collectors.base import BaseCollector
import config

class MyCollector(BaseCollector):
    name = "my_collector"
    interval_minutes = config.MY_COLLECTOR_INTERVAL

    def collect(self) -> dict:
        data = ...
        return {"count": len(data), "data": data}
```

## 授權

MIT License
