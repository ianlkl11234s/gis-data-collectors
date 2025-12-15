# Data Collectors

定期自動化資料收集服務，部署於 Zeabur 24hr 運作。

## 專案結構

```
data-collectors/
├── README.md
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
│   ├── weather.py         # 氣象資料（CWA）
│   └── parking.py         # 停車場資料（未來）
│
├── storage/                # 儲存後端
│   ├── __init__.py
│   ├── local.py           # 本地檔案儲存
│   ├── s3.py              # AWS S3 儲存
│   └── gcs.py             # Google Cloud Storage（未來）
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
| `API_KEY` | | HTTP API 認證金鑰（建議設定） |
| `API_PORT` | | HTTP API 端口（預設 8080） |
| `CWA_API_KEY` | | 氣象局 API Key |
| `S3_BUCKET` | | S3 儲存桶 |
| `S3_ACCESS_KEY` | | AWS Access Key |
| `S3_SECRET_KEY` | | AWS Secret Key |
| `WEBHOOK_URL` | | 通知 Webhook |
| `LINE_TOKEN` | | LINE Notify Token |

## 收集器說明

### YouBike 即時車位
- **頻率**: 每 15 分鐘
- **來源**: TDX API `/v2/Bike/Availability/{City}`
- **範圍**: 臺北市、新北市、桃園市
- **資料量**: ~3,800 站/次

### 氣象資料（規劃中）
- **頻率**: 每小時
- **來源**: CWA 開放資料平台
- **資料類型**:
  - 即時觀測（溫度、雨量、風速）
  - 未來 36hr 預報
  - 雷達回波圖

## 資料儲存

### 本地模式
資料儲存在 `data/` 目錄，適合開發測試。

### S3 模式（推薦）
設定 `S3_BUCKET` 後，資料自動上傳到 S3：
```
s3://your-bucket/
├── youbike/
│   ├── 2024/12/09/
│   │   ├── availability_0000.json
│   │   ├── availability_0015.json
│   │   └── ...
│   └── latest.json
├── weather/
│   └── ...
└── logs/
    └── ...
```

## HTTP API（下載資料）

設定 `API_KEY` 環境變數後，會自動啟動 HTTP API Server，可透過 API 下載收集的資料。

### 認證方式

使用 API Key 認證，支援兩種方式：

```bash
# 方式 1: Header（推薦）
curl -H "X-API-Key: your_api_key" https://your-app.zeabur.app/api/...

# 方式 2: Query Parameter
curl "https://your-app.zeabur.app/api/...?api_key=your_api_key"
```

### 產生 API Key

```bash
# 使用 OpenSSL 產生隨機金鑰
openssl rand -hex 32
```

### API 端點

| 端點 | 認證 | 說明 |
|------|------|------|
| `GET /` | 不需要 | 服務資訊 |
| `GET /health` | 不需要 | 健康檢查 |
| `GET /api/collectors` | 需要 | 列出所有收集器 |
| `GET /api/data/<collector>` | 需要 | 列出某收集器的所有檔案 |
| `GET /api/data/<collector>/latest` | 需要 | 取得最新資料 |
| `GET /api/data/<collector>/<date>` | 需要 | 取得特定日期資料（YYYY-MM-DD） |
| `GET /api/download/<collector>/<filename>` | 需要 | 下載特定檔案 |

### 使用範例

```bash
# 列出所有收集器
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/collectors

# 取得 YouBike 最新資料
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/youbike/latest

# 列出 YouBike 所有檔案
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/youbike

# 取得特定日期的資料
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/youbike/2024-12-15

# 下載特定檔案
curl -H "X-API-Key: your_key" -O https://your-app.zeabur.app/api/download/youbike/availability_2024-12-15_1430.json
```

### 回應格式

所有 API 回應皆為 JSON 格式：

```json
// GET /api/data/youbike/latest
{
  "filename": "availability_2024-12-15_1430.json",
  "data": {
    "collected_at": "2024-12-15T14:30:00+08:00",
    "cities": ["Taipei", "NewTaipei", "Taoyuan"],
    "total_stations": 3837,
    "stations": [...]
  }
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
4. 在 `main.py` 註冊排程

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
