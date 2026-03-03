# Data Collectors

定期自動化資料收集服務，部署於 Zeabur 24hr 運作。支援 S3 歸檔與資料生命週期管理。

## 收集器總覽

共 16 個收集器，每個都可獨立啟停。

| 收集器 | `_ENABLED` 環境變數 | 預設 | 頻率 | 來源 | 說明 |
|--------|---------------------|------|------|------|------|
| YouBike | `YOUBIKE_ENABLED` | `true` | 15 min | TDX | 即時車位 |
| Weather | `WEATHER_ENABLED` | `true` | 60 min | CWA | 氣象觀測站 |
| VD | `VD_ENABLED` | **false** | 5 min | TDX | 縣市道路車輛偵測器 |
| Freeway VD | `FREEWAY_VD_ENABLED` | `true` | 10 min | TDX | 國道即時車流+壅塞 |
| Temperature | `TEMPERATURE_ENABLED` | `true` | 60 min | CWA | 溫度網格 |
| Parking | `PARKING_ENABLED` | **false** | 15 min | TDX | 路邊停車即時可用性 |
| Bus | `BUS_ENABLED` | `true` | 1 min | TDX | 公車即時位置 |
| TRA Train | `TRA_TRAIN_ENABLED` | `true` | 2 min | TDX | 台鐵即時列車位置 |
| TRA Static | `TRA_STATIC_ENABLED` | `true` | 1440 min | TDX | 台鐵靜態資料（每日） |
| Rail Timetable | `RAIL_TIMETABLE_ENABLED` | `true` | 1440 min | TDX | 台鐵+高鐵每日時刻表歸檔 |
| Ship TDX | `SHIP_TDX_ENABLED` | **false** | 2 min | TDX | 國內航線船位 |
| Ship AIS | `SHIP_AIS_ENABLED` | `true` | 10 min | 航港局 | AIS 船位追蹤 |
| Flight FR24 | `FLIGHT_FR24_ENABLED` | **false** | 5 min | FR24 | 台灣機場航班完整軌跡 |
| FR24 Zone | `FLIGHT_FR24_ZONE_ENABLED` | **false** | 5 min | FR24 | 空域快照（最多飛機+起降機場） |
| OpenSky | `FLIGHT_OPENSKY_ENABLED` | **false** | 5 min | OpenSky | 空域快照（精確高度+垂直速率） |

> 預設 **false** 的收集器需手動啟用（設定環境變數為 `true`）。

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
│   ├── bus.py             # 公車即時位置（TDX）
│   ├── tra_train.py       # 台鐵即時列車位置（TDX）
│   ├── tra_static.py      # 台鐵靜態資料（TDX）
│   ├── rail_timetable.py  # 台鐵+高鐵每日時刻表歸檔（TDX）
│   ├── ship_tdx.py        # TDX 國內航線船位
│   ├── ship_ais.py        # 航港局 AIS 船位
│   ├── flight_fr24.py     # FlightRadar24 航班軌跡
│   ├── flight_fr24_zone.py # FR24 Zone 空域快照
│   └── flight_opensky.py  # OpenSky 空域快照
│
├── storage/                # 儲存後端
│   ├── __init__.py
│   ├── local.py           # 本地檔案儲存
│   └── s3.py              # AWS S3 儲存與歸檔
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
| `BUS_ENABLED` | `true` | 公車即時位置 |
| `BUS_CITIES` | `Taipei,NewTaipei,Taoyuan` | 收集城市 |
| `BUS_INTERVAL` | `1` | 間隔（分鐘） |

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

#### 氣象

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `WEATHER_ENABLED` | `true` | 氣象觀測站 |
| `WEATHER_INTERVAL` | `60` | 間隔（分鐘） |
| `TEMPERATURE_ENABLED` | `true` | 溫度網格 |
| `TEMPERATURE_INTERVAL` | `60` | 間隔（分鐘） |

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

### 公車即時位置
- **來源**: TDX API `/v2/Bus/RealTimeByFrequency/{City}`
- **範圍**: 依 `BUS_CITIES` 設定
- **資料類型**: 公車即時 GPS 座標、速度、方位角

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
├── youbike/
├── weather/
├── vd/
├── freeway_vd/
├── temperature/
├── parking/
├── bus/
├── tra_train/
├── tra_static/
├── rail_timetable/
├── ship_tdx/
├── ship_ais/
├── flight_fr24/
├── flight_fr24_zone/
└── flight_opensky/

s3://bucket/                   # S3 (永久歸檔，tar.gz)
└── {collector}/
    └── archives/
        └── YYYY-MM-DD.tar.gz  # 每天 1 個 PUT
```

### 歸檔流程

每日 03:00 自動執行：
1. 遍歷各收集器的 YYYY/MM/DD 日期目錄（跳過今天）
2. 壓成 `collector/archives/YYYY-MM-DD.tar.gz`
3. 上傳 1 個 PUT 到 S3（每個收集器每天 1 個）
4. 刪除超過保留天數且 S3 已有歸檔的日期目錄
5. 清理空目錄

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
