# Data Collectors API 文件

本文件說明 Data Collectors HTTP API 的使用方式。

## 基本資訊

| 項目 | 說明 |
|------|------|
| Base URL | `https://your-app.zeabur.app` |
| 認證方式 | API Key |
| 回應格式 | JSON |
| 編碼 | UTF-8 |
| 版本 | 1.1.0 |

## 認證

API 使用 API Key 認證，支援兩種方式：

### 方式 1: Header（推薦）

```bash
curl -H "X-API-Key: your_api_key" https://your-app.zeabur.app/api/collectors
```

### 方式 2: Query Parameter

```bash
curl "https://your-app.zeabur.app/api/collectors?api_key=your_api_key"
```

### 產生 API Key

```bash
# 使用 OpenSSL 產生隨機金鑰
openssl rand -hex 32
```

### 錯誤回應

| HTTP 狀態碼 | 說明 |
|------------|------|
| 401 | 未提供 API Key |
| 403 | API Key 無效 |
| 503 | API 未設定（伺服器未設定 API_KEY） |

---

## API 端點

### 公開端點（無需認證）

#### `GET /`
服務資訊

**回應範例：**
```json
{
  "service": "Data Collectors API",
  "version": "1.1.0",
  "status": "running",
  "storage": {
    "local": "/data",
    "s3": "migu-gis-data-collector",
    "s3_region": "ap-southeast-2"
  },
  "endpoints": {
    "/health": "Health check (no auth)",
    "/api/collectors": "List available collectors",
    "/api/data/<collector>": "List data files for collector",
    "/api/data/<collector>/latest": "Get latest data",
    "/api/data/<collector>/<date>": "Get data by date (YYYY-MM-DD)",
    "/api/data/<collector>/dates": "List available dates (from S3)",
    "/api/download/<collector>/<path>": "Download file",
    "/api/archive/status": "Archive status and statistics"
  },
  "auth": "Use X-API-Key header or api_key query parameter",
  "note": "Data automatically retrieved from S3 if not available locally"
}
```

#### `GET /health`
健康檢查

**回應範例：**
```json
{
  "status": "healthy",
  "timestamp": "2025-12-26T09:00:00.123456",
  "data_dir": "/data",
  "data_dir_exists": true
}
```

---

### 需認證端點

#### `GET /api/collectors`
列出所有可用的收集器

**回應範例：**
```json
{
  "collectors": [
    {
      "name": "youbike",
      "file_count": 96,
      "has_latest": true
    },
    {
      "name": "weather",
      "file_count": 24,
      "has_latest": true
    },
    {
      "name": "vd",
      "file_count": 288,
      "has_latest": true
    },
    {
      "name": "temperature",
      "file_count": 24,
      "has_latest": true
    },
    {
      "name": "parking",
      "file_count": 96,
      "has_latest": true
    }
  ],
  "data_dir": "/data"
}
```

---

#### `GET /api/data/<collector>`
列出指定收集器的所有資料檔案

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱（youbike, weather, vd, temperature, parking） |

**回應範例：**
```json
{
  "collector": "parking",
  "files": [
    {
      "filename": "parking_0900.json",
      "path": "2025/12/26/parking_0900.json",
      "size": 1234567,
      "modified": "2025-12-26T09:00:05.123456"
    }
  ],
  "total": 96
}
```

---

#### `GET /api/data/<collector>/latest`
取得指定收集器的最新資料

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱 |

**Query 參數：**
| 參數 | 說明 |
|------|------|
| format | 設為 `file` 時下載原始檔案 |

---

#### `GET /api/data/<collector>/<date>`
取得特定日期的資料檔案列表（自動從本地或 S3 讀取）

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱 |
| date | 日期，格式 YYYY-MM-DD |

**回應範例：**
```json
{
  "collector": "parking",
  "date": "2025-12-20",
  "files": [
    {
      "filename": "parking_0900.json",
      "path": "2025/12/20/parking_0900.json",
      "size": 1234567,
      "modified": "2025-12-20T09:00:05.123456",
      "source": "s3"
    }
  ],
  "total": 96,
  "source": "s3"
}
```

**注意**：`source` 欄位表示資料來源：
- `local`: 從本地 Zeabur 儲存讀取
- `s3`: 從 AWS S3 歸檔讀取

---

#### `GET /api/data/<collector>/dates`
列出收集器有資料的所有日期（合併本地與 S3）

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱 |

**回應範例：**
```json
{
  "collector": "parking",
  "dates": [
    "2025-12-26",
    "2025-12-25",
    "2025-12-24",
    "2025-12-23",
    "2025-12-22"
  ],
  "total": 5
}
```

---

#### `GET /api/download/<collector>/<path>`
下載指定檔案（自動從本地或 S3 讀取）

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱 |
| path | 檔案路徑（如 `2025/12/26/parking_0900.json`） |

**回應標頭：**
| 標頭 | 說明 |
|------|------|
| X-Data-Source | 資料來源（`local` 或 `s3`） |

**範例：**
```bash
curl -H "X-API-Key: your_key" \
  -O https://your-app.zeabur.app/api/download/parking/2025/12/26/parking_0900.json
```

---

#### `GET /api/archive/status`
取得歸檔狀態與統計資訊

**回應範例：**
```json
{
  "enabled": true,
  "s3_configured": true,
  "s3_bucket": "migu-gis-data-collector",
  "retention_days": 7,
  "archive_time": "03:00",
  "local_data_dir": "/data",
  "collectors": [
    {
      "name": "parking",
      "local_files": 96,
      "local_size_mb": 125.5,
      "s3_files": 672,
      "s3_size_mb": 875.2
    },
    {
      "name": "youbike",
      "local_files": 96,
      "local_size_mb": 45.3,
      "s3_files": 672,
      "s3_size_mb": 315.8
    }
  ]
}
```

---

## 資料來源優先順序

API 會自動處理資料來源：

1. **優先本地**：最近 7 天的資料存放在 Zeabur 本地儲存，讀取速度較快
2. **回退 S3**：超過 7 天的歷史資料自動從 S3 讀取
3. **透明切換**：使用者無需關心資料實際位置，API 自動處理

---

## 使用範例

### Python

```python
import requests

API_KEY = "your_api_key"
BASE_URL = "https://your-app.zeabur.app"

headers = {"X-API-Key": API_KEY}

# 取得最新停車資料
response = requests.get(
    f"{BASE_URL}/api/data/parking/latest",
    headers=headers
)
data = response.json()

# 取得歷史資料（自動從 S3 讀取）
response = requests.get(
    f"{BASE_URL}/api/data/parking/2025-12-01",
    headers=headers
)
history = response.json()
print(f"資料來源: {history['source']}")  # 可能是 's3'

# 列出所有可用日期
response = requests.get(
    f"{BASE_URL}/api/data/parking/dates",
    headers=headers
)
dates = response.json()
print(f"共有 {dates['total']} 天的資料")
```

### cURL

```bash
# 列出所有收集器
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/collectors

# 取得歸檔狀態
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/archive/status

# 列出可用日期
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/dates

# 取得歷史資料（自動從 S3 讀取）
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/2025-12-01
```

---

## 錯誤處理

所有錯誤回應都包含 `error` 和 `message` 欄位：

```json
{
  "error": "Not found",
  "message": "File not found: 2025/12/01/parking_0900.json"
}
```

常見錯誤：

| HTTP 狀態碼 | error | 說明 |
|------------|-------|------|
| 400 | Invalid date | 日期格式錯誤 |
| 400 | Invalid path | 檔案路徑無效 |
| 401 | Unauthorized | 未提供 API Key |
| 403 | Forbidden | API Key 無效 |
| 404 | Not found | 收集器或檔案不存在 |
| 404 | No data | 該日期無資料 |
| 500 | Failed to get archive status | 無法取得歸檔狀態 |
| 503 | API not configured | 伺服器未設定 API_KEY |

---

## 限制與注意事項

1. **資料大小**: 溫度網格和停車資料較大，建議使用串流下載
2. **更新頻率**:
   - VD 車流: 每 5 分鐘更新
   - 溫度網格：每小時更新
   - 路邊停車：每 15 分鐘更新
   - YouBike: 每 15 分鐘更新
   - 氣象觀測: 每小時更新
3. **歷史資料**: 本地保留 7 天，S3 永久保存
4. **並行請求**: 建議控制並行請求數量
5. **時區**: 所有時間皆為台灣時間 (UTC+8)
6. **S3 讀取延遲**: 從 S3 讀取可能比本地慢 100-500ms
