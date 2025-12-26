# Data Collectors

定期自動化資料收集服務，部署於 Zeabur 24hr 運作。支援 S3 歸檔與資料生命週期管理。

## 專案結構

```
data-collectors/
├── README.md
├── docs/                   # 詳細文件
│   ├── API.md             # API 詳細文件
│   └── ARCHITECTURE.md    # 架構與歸檔流程
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
│   ├── temperature.py     # 溫度網格資料（CWA）
│   └── parking.py         # 路邊停車即時可用性
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

| 變數 | 必填 | 說明 |
|------|------|------|
| `TDX_APP_ID` | ✅ | TDX API Client ID |
| `TDX_APP_KEY` | ✅ | TDX API Client Secret |
| `CWA_API_KEY` | ✅ | 氣象局 API Key |
| `API_KEY` | | HTTP API 認證金鑰（建議設定） |
| `API_PORT` | | HTTP API 端口（預設 8080） |
| `S3_BUCKET` | | S3 儲存桶（啟用歸檔必填） |
| `S3_ACCESS_KEY` | | AWS Access Key |
| `S3_SECRET_KEY` | | AWS Secret Key |
| `S3_REGION` | | S3 區域（預設 ap-southeast-2） |
| `WEBHOOK_URL` | | 通知 Webhook |
| `LINE_TOKEN` | | LINE Notify Token |

### 歸檔設定

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `ARCHIVE_ENABLED` | `true` | 是否啟用 S3 歸檔 |
| `ARCHIVE_RETENTION_DAYS` | `7` | 本地資料保留天數 |
| `ARCHIVE_TIME` | `03:00` | 每日歸檔執行時間 |

### 收集器專屬設定

| 變數 | 預設值 | 說明 |
|------|--------|------|
| `YOUBIKE_CITIES` | `Taipei,NewTaipei,Taoyuan` | YouBike 收集城市 |
| `YOUBIKE_INTERVAL` | `15` | YouBike 收集間隔（分鐘） |
| `WEATHER_INTERVAL` | `60` | 氣象站收集間隔（分鐘） |
| `VD_CITIES` | `Taipei,NewTaipei` | VD 收集城市 |
| `VD_INTERVAL` | `5` | VD 收集間隔（分鐘） |
| `TEMPERATURE_INTERVAL` | `60` | 溫度網格收集間隔（分鐘） |
| `PARKING_CITIES` | `Taipei,NewTaipei,Taichung` | 路邊停車收集城市 |
| `PARKING_INTERVAL` | `15` | 路邊停車收集間隔（分鐘） |

## 收集器說明

### YouBike 即時車位
- **頻率**: 每 15 分鐘
- **來源**: TDX API `/v2/Bike/Availability/{City}`
- **範圍**: 臺北市、新北市、桃園市
- **資料量**: ~3,800 站/次

### 氣象觀測站資料
- **頻率**: 每 60 分鐘
- **來源**: CWA API `O-A0001-001`
- **資料類型**: 即時觀測（溫度、雨量、風速、氣壓等）
- **資料量**: ~700 測站

### VD 車輛偵測器
- **頻率**: 每 5 分鐘
- **來源**: TDX API `/v2/Road/Traffic/VD/{City}`
- **範圍**: 臺北市、新北市
- **資料類型**: 車流量、車速

### 溫度網格資料 🆕
- **頻率**: 每 60 分鐘
- **來源**: CWA File API `O-A0038-003`
- **資料類型**: 小時溫度觀測分析格點資料
- **解析度**: 0.03 度（約 3.3 公里）
- **覆蓋範圍**: 全台灣
- **資料量**: ~50,000 格點

### 路邊停車即時可用性 🆕
- **頻率**: 每 15 分鐘
- **來源**: TDX API `/v1/Parking/OnStreet/ParkingSegmentAvailability/{City}`
- **範圍**: 臺北市、新北市、臺中市
- **注意**: 高雄市不在 TDX 支援範圍
- **資料量**: ~4,600 路段

## 每日 API 呼叫統計

| 收集器 | 頻率 | 每日次數 | 來源 |
|--------|------|---------|------|
| YouBike | 15 min | 96 × 3 城市 = 288 | TDX |
| Weather | 60 min | 24 | CWA |
| VD | 5 min | 288 × 2 城市 = 576 | TDX |
| Temperature | 60 min | 24 | CWA |
| Parking | 15 min | 96 × 3 城市 = 288 | TDX |

## 資料儲存與歸檔

### 雙層儲存架構

採用熱/冷資料分離策略，有效降低儲存成本：

| 層級 | 儲存位置 | 資料範圍 | 用途 |
|------|----------|----------|------|
| 熱資料 | Zeabur Volume | 最近 7 天 | 快速存取 |
| 冷資料 | AWS S3 | 全部歷史 | 永久歸檔 |

### 成本比較 (50GB 資料/月)

- **純 Zeabur**: ~$7.50/月
- **Zeabur + S3**: ~$1.79/月 (節省 76%)

### 儲存結構
```
data/                       # 本地 (最近 7 天)
├── youbike/
│   ├── latest.json        # 最新資料快取
│   └── 2025/12/26/
│       └── youbike_0900.json
├── weather/
├── vd/
├── temperature/
└── parking/

s3://bucket/               # S3 (永久歸檔)
├── youbike/
│   └── 2025/12/20/       # 歷史資料
│       └── youbike_0900.json
├── weather/
└── ...
```

### 歸檔流程

每日 03:00 自動執行：
1. 同步所有本地資料到 S3（跳過已存在）
2. 刪除超過 7 天的本地檔案
3. 清理空目錄

詳細架構說明請參閱 [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)

## HTTP API

設定 `API_KEY` 環境變數後，會自動啟動 HTTP API Server。

**特色功能**：
- 自動從本地或 S3 讀取資料（透明切換）
- 支援列出所有可用日期
- 歸檔狀態查詢

詳細文件請參閱 [docs/API.md](./docs/API.md)

### 快速範例

```bash
# 健康檢查（無需認證）
curl https://your-app.zeabur.app/health

# 列出所有收集器
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/collectors

# 取得最新路邊停車資料
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/latest

# 列出可用日期（包含 S3 歷史資料）
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/dates

# 取得歷史資料（自動從 S3 讀取）
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/2025-12-01

# 查看歸檔狀態
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/archive/status
```

## 資料格式

### 溫度網格 (temperature)

```json
{
  "fetch_time": "2025-12-26T09:00:00",
  "observation_time": "2025-12-26T09:00:00+08:00",
  "geo_info": {
    "bottom_left_lon": 118.0,
    "bottom_left_lat": 21.0,
    "top_right_lon": 123.0,
    "top_right_lat": 26.0,
    "resolution_deg": 0.03,
    "resolution_km": 3.3
  },
  "grid_size": { "rows": 167, "cols": 167 },
  "valid_points": 48392,
  "min_temp": 5.2,
  "max_temp": 28.4,
  "avg_temp": 18.6,
  "data": [[18.2, 18.3, ...], ...]
}
```

### 路邊停車 (parking)

```json
{
  "fetch_time": "2025-12-26T09:00:00",
  "total_segments": 4627,
  "total_spaces": 133509,
  "total_available": 45231,
  "overall_occupancy": 0.661,
  "by_city": {
    "Taipei": {
      "name": "臺北市",
      "segments": 2365,
      "total_spaces": 46864,
      "available_spaces": 15234,
      "full_segments": 128,
      "avg_occupancy": 0.675
    }
  },
  "data": [
    {
      "segment_id": "1002053",
      "segment_name": "中山北路1段53巷",
      "total_spaces": 8,
      "available_spaces": 4,
      "occupancy": 0.5,
      "full_status": 0,
      "_city": "Taipei"
    }
  ]
}
```

## 監控

- 每次執行會輸出統計日誌
- 可設定 Webhook 接收執行結果
- 支援 LINE Notify 異常通知

## 開發新收集器

1. 在 `collectors/` 建立新模組
2. 繼承 `BaseCollector` 類別
3. 實作 `collect()` 方法
4. 在 `collectors/__init__.py` 註冊
5. 在 `main.py` 初始化並加入排程

```python
from collectors.base import BaseCollector

class MyCollector(BaseCollector):
    name = "my_collector"
    interval_minutes = 30

    def collect(self) -> dict:
        # 實作資料收集邏輯
        data = self.fetch_api(...)
        return {"count": len(data), "data": data}
```

## 授權

MIT License
