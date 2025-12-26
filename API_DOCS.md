# Data Collectors API 文件

本文件說明 Data Collectors HTTP API 的使用方式。

## 基本資訊

| 項目 | 說明 |
|------|------|
| Base URL | `https://your-app.zeabur.app` |
| 認證方式 | API Key |
| 回應格式 | JSON |
| 編碼 | UTF-8 |

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
  "version": "1.0.0",
  "status": "running",
  "endpoints": {
    "/health": "Health check (no auth)",
    "/api/collectors": "List available collectors",
    "/api/data/<collector>": "List data files for collector",
    "/api/data/<collector>/latest": "Get latest data",
    "/api/data/<collector>/<date>": "Get data by date (YYYY-MM-DD)",
    "/api/download/<collector>/<path>": "Download file"
  },
  "auth": "Use X-API-Key header or api_key query parameter"
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
| collector | 收集器名稱（youbike, weather, temperature, parking, vd） |

**回應範例：**
```json
{
  "collector": "parking",
  "files": [
    {
      "filename": "parking_2025-12-26T09-00-00.json",
      "path": "2025/12/26/parking_2025-12-26T09-00-00.json",
      "size": 1234567,
      "modified": "2025-12-26T09:00:05.123456"
    },
    {
      "filename": "parking_2025-12-26T08-45-00.json",
      "path": "2025/12/26/parking_2025-12-26T08-45-00.json",
      "size": 1234123,
      "modified": "2025-12-26T08:45:05.123456"
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

**回應範例：**
```json
{
  "filename": "parking_2025-12-26T09-00-00.json",
  "modified": "2025-12-26T09:00:05.123456",
  "data": {
    "fetch_time": "2025-12-26T09:00:00",
    "total_segments": 4627,
    "total_spaces": 133509,
    "total_available": 45231,
    "by_city": { ... },
    "data": [ ... ]
  }
}
```

---

#### `GET /api/data/<collector>/<date>`
取得指定日期的資料檔案列表

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱 |
| date | 日期，格式 YYYY-MM-DD |

**回應範例：**
```json
{
  "collector": "parking",
  "date": "2025-12-26",
  "files": [
    {
      "filename": "parking_2025-12-26T09-00-00.json",
      "path": "2025/12/26/parking_2025-12-26T09-00-00.json",
      "size": 1234567,
      "modified": "2025-12-26T09:00:05.123456"
    }
  ],
  "total": 24
}
```

---

#### `GET /api/download/<collector>/<path>`
下載指定檔案

**路徑參數：**
| 參數 | 說明 |
|------|------|
| collector | 收集器名稱 |
| path | 檔案路徑（如 `2025/12/26/parking_2025-12-26T09-00-00.json`） |

**回應：**
直接下載 JSON 檔案

**範例：**
```bash
curl -H "X-API-Key: your_key" \
  -O https://your-app.zeabur.app/api/download/parking/2025/12/26/parking_2025-12-26T09-00-00.json
```

---

## 資料格式說明

### 溫度網格 (temperature)

全台灣溫度網格資料，每小時更新。

**欄位說明：**

| 欄位 | 型別 | 說明 |
|------|------|------|
| fetch_time | string | 資料擷取時間 (ISO 8601) |
| observation_time | string | 觀測時間 (ISO 8601) |
| geo_info | object | 地理範圍資訊 |
| geo_info.bottom_left_lon | number | 左下角經度 |
| geo_info.bottom_left_lat | number | 左下角緯度 |
| geo_info.top_right_lon | number | 右上角經度 |
| geo_info.top_right_lat | number | 右上角緯度 |
| geo_info.resolution_deg | number | 解析度（度） |
| geo_info.resolution_km | number | 解析度（公里） |
| grid_size.rows | integer | 網格列數 |
| grid_size.cols | integer | 網格欄數 |
| valid_points | integer | 有效格點數 |
| min_temp | number | 最低溫度 (°C) |
| max_temp | number | 最高溫度 (°C) |
| avg_temp | number | 平均溫度 (°C) |
| std_temp | number | 溫度標準差 |
| data | array | 二維溫度陣列，null 表示無資料 |

**資料範例：**
```json
{
  "fetch_time": "2025-12-26T09:00:00.123456",
  "observation_time": "2025-12-26T09:00:00+08:00",
  "geo_info": {
    "bottom_left_lon": 118.0,
    "bottom_left_lat": 21.0,
    "top_right_lon": 123.0,
    "top_right_lat": 26.0,
    "resolution_deg": 0.03,
    "resolution_km": 3.3
  },
  "grid_size": {
    "rows": 167,
    "cols": 167
  },
  "valid_points": 48392,
  "min_temp": 5.2,
  "max_temp": 28.4,
  "avg_temp": 18.6,
  "std_temp": 4.23,
  "data": [
    [null, null, 18.2, 18.3, 18.5, ...],
    [null, 17.8, 18.0, 18.2, 18.4, ...],
    ...
  ]
}
```

**座標計算方式：**
```python
# 給定格點 (row, col)，計算經緯度
lon = bottom_left_lon + col * resolution_deg
lat = bottom_left_lat + row * resolution_deg
```

---

### 路邊停車 (parking)

台北市、新北市、台中市路邊停車即時可用性，每 15 分鐘更新。

**頂層欄位：**

| 欄位 | 型別 | 說明 |
|------|------|------|
| fetch_time | string | 資料擷取時間 (ISO 8601) |
| total_segments | integer | 總路段數 |
| total_spaces | integer | 總車位數 |
| total_available | integer | 總空位數 |
| total_full_segments | integer | 滿載路段數 |
| overall_occupancy | number | 整體使用率 (0-1) |
| by_city | object | 各城市統計 |
| data | array | 各路段詳細資料 |

**by_city 欄位：**

| 欄位 | 型別 | 說明 |
|------|------|------|
| name | string | 城市中文名稱 |
| segments | integer | 路段數 |
| total_spaces | integer | 總車位數 |
| available_spaces | integer | 可用車位數 |
| full_segments | integer | 滿載路段數 |
| tight_segments | integer | 緊張路段數（< 10% 空位） |
| avg_occupancy | number | 平均使用率 |
| update_time | string | 來源更新時間 |

**data 陣列欄位：**

| 欄位 | 型別 | 說明 |
|------|------|------|
| segment_id | string | 路段代碼 |
| segment_name | string | 路段名稱 |
| total_spaces | integer | 總車位數 |
| available_spaces | integer | 可用車位數（-1 表示無資料） |
| occupancy | number | 使用率 (0-1)，null 表示無資料 |
| full_status | integer | 滿載狀態：0=有空位, 1=已滿, -1=無資料 |
| service_status | integer | 服務狀態 |
| charge_status | integer | 收費狀態 |
| space_types | array | 車位類型明細 |
| data_collect_time | string | 資料收集時間 |
| _city | string | 城市代碼 |
| _city_name | string | 城市中文名稱 |
| _fetch_time | string | 擷取時間 |

**資料範例：**
```json
{
  "fetch_time": "2025-12-26T09:00:00.123456",
  "total_segments": 4627,
  "total_spaces": 133509,
  "total_available": 45231,
  "total_full_segments": 312,
  "overall_occupancy": 0.661,
  "by_city": {
    "Taipei": {
      "name": "臺北市",
      "segments": 2365,
      "total_spaces": 46864,
      "available_spaces": 15234,
      "full_segments": 128,
      "tight_segments": 245,
      "avg_occupancy": 0.675,
      "update_time": "2025-12-26T08:55:00+08:00"
    },
    "NewTaipei": {
      "name": "新北市",
      "segments": 1939,
      "total_spaces": 76843,
      "available_spaces": 26543,
      "full_segments": 156,
      "tight_segments": 312,
      "avg_occupancy": 0.654,
      "update_time": "2025-12-26T08:55:00+08:00"
    },
    "Taichung": {
      "name": "臺中市",
      "segments": 323,
      "total_spaces": 9802,
      "available_spaces": 3454,
      "full_segments": 28,
      "tight_segments": 45,
      "avg_occupancy": 0.647,
      "update_time": "2025-12-26T08:55:00+08:00"
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
      "service_status": 1,
      "charge_status": 1,
      "space_types": [
        {
          "type": 1,
          "total": 8,
          "available": 4,
          "occupancy": 0.5
        }
      ],
      "data_collect_time": "2025-12-26T08:55:00+08:00",
      "_city": "Taipei",
      "_city_name": "臺北市",
      "_fetch_time": "2025-12-26T09:00:00.123456"
    }
  ]
}
```

---

### YouBike (youbike)

公共自行車即時車位資料。

**主要欄位：**

| 欄位 | 說明 |
|------|------|
| fetch_time | 資料擷取時間 |
| total_stations | 總站點數 |
| total_bikes | 可借車輛數 |
| total_spaces | 可還空位數 |
| by_city | 各城市統計 |
| data | 各站點詳細資料 |

---

### 氣象觀測 (weather)

氣象站即時觀測資料。

**主要欄位：**

| 欄位 | 說明 |
|------|------|
| fetch_time | 資料擷取時間 |
| total_stations | 總測站數 |
| avg_temperature | 平均溫度 |
| avg_humidity | 平均濕度 |
| temp_range | 溫度範圍 |
| by_county | 各縣市統計 |
| data | 各測站詳細資料 |

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

# 取得統計
print(f"總空位: {data['data']['total_available']}")
print(f"使用率: {data['data']['overall_occupancy'] * 100:.1f}%")

# 找出滿載路段
for segment in data['data']['data']:
    if segment['full_status'] == 1:
        print(f"滿載: {segment['segment_name']}")
```

### JavaScript

```javascript
const API_KEY = 'your_api_key';
const BASE_URL = 'https://your-app.zeabur.app';

// 取得最新溫度資料
fetch(`${BASE_URL}/api/data/temperature/latest`, {
  headers: { 'X-API-Key': API_KEY }
})
  .then(res => res.json())
  .then(data => {
    console.log(`平均溫度: ${data.data.avg_temp}°C`);
    console.log(`溫度範圍: ${data.data.min_temp}°C ~ ${data.data.max_temp}°C`);
  });
```

### cURL

```bash
# 列出所有收集器
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/collectors

# 取得最新溫度網格
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/temperature/latest

# 取得最新停車資料
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/latest

# 取得特定日期的停車資料
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/data/parking/2025-12-26

# 下載特定檔案
curl -H "X-API-Key: your_key" -O \
  https://your-app.zeabur.app/api/download/parking/2025/12/26/parking_2025-12-26T09-00-00.json
```

---

## 錯誤處理

所有錯誤回應都包含 `error` 和 `message` 欄位：

```json
{
  "error": "Not found",
  "message": "Collector \"invalid\" not found"
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
| 503 | API not configured | 伺服器未設定 API_KEY |

---

## 限制與注意事項

1. **資料大小**: 溫度網格和停車資料較大，建議使用串流下載
2. **更新頻率**:
   - 溫度網格：每小時更新
   - 路邊停車：每 15 分鐘更新
3. **歷史資料**: 保留期限依儲存設定而定
4. **並行請求**: 建議控制並行請求數量
5. **時區**: 所有時間皆為台灣時間 (UTC+8)
