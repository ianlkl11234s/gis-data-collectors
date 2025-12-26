# S3 歸檔設定指南

本文件說明如何設定 AWS S3 作為 Data Collectors 的歸檔儲存。

## 目錄

1. [建立 S3 Bucket](#1-建立-s3-bucket)
2. [建立 IAM 使用者](#2-建立-iam-使用者)
3. [設定環境變數](#3-設定環境變數)
4. [測試連線](#4-測試連線)
5. [S3 Lifecycle 規則](#5-s3-lifecycle-規則進階)
6. [常見問題排解](#6-常見問題排解)

---

## 1. 建立 S3 Bucket

### 步驟

1. 登入 [AWS Console](https://console.aws.amazon.com/)
2. 搜尋並進入 **S3** 服務
3. 點擊 **Create bucket**
4. 設定：
   - **Bucket name**: `your-bucket-name` (全球唯一)
   - **AWS Region**: `Asia Pacific (Sydney) ap-southeast-2` (建議)
   - **Object Ownership**: ACLs disabled (推薦)
   - **Block Public Access**: 全部勾選 (保持私有)
   - **Bucket Versioning**: 可選擇啟用 (防止誤刪)
5. 點擊 **Create bucket**

### 建議區域

| 區域 | 代碼 | 延遲 | 成本 |
|------|------|------|------|
| 雪梨 | ap-southeast-2 | 中 | 低 |
| 東京 | ap-northeast-1 | 低 | 中 |
| 新加坡 | ap-southeast-1 | 低 | 中 |

---

## 2. 建立 IAM 使用者

### 步驟

1. 進入 [IAM Console](https://console.aws.amazon.com/iam/)
2. 左側選單點擊 **Users**
3. 點擊 **Create user**
4. 設定：
   - **User name**: `data-collectors-s3` (或自訂名稱)
   - 勾選 **Provide user access to the AWS Management Console**: 不需要
5. 點擊 **Next**
6. 選擇 **Attach policies directly**
7. 點擊 **Create policy** (新分頁)

### 建立自訂 Policy

在 JSON 編輯器貼上：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DataCollectorsS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:HeadObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_BUCKET_NAME",
        "arn:aws:s3:::YOUR_BUCKET_NAME/*"
      ]
    }
  ]
}
```

> ⚠️ 將 `YOUR_BUCKET_NAME` 替換為你的 bucket 名稱

Policy 設定：
- **Policy name**: `DataCollectorsS3Policy`
- 點擊 **Create policy**

### 回到使用者建立

1. 重新整理 policy 列表
2. 搜尋並勾選 `DataCollectorsS3Policy`
3. 點擊 **Next** → **Create user**

### 建立 Access Key

1. 點擊剛建立的使用者名稱
2. 選擇 **Security credentials** 分頁
3. 在 **Access keys** 區塊，點擊 **Create access key**
4. 選擇 **Application running outside AWS**
5. 點擊 **Next** → **Create access key**
6. **立即複製並保存**：
   - Access key ID
   - Secret access key

> ⚠️ Secret access key 只會顯示一次，請妥善保存！

---

## 3. 設定環境變數

### 本地開發 (.env)

```bash
# S3 設定
S3_BUCKET=your-bucket-name
S3_ACCESS_KEY=AKIA...your-access-key
S3_SECRET_KEY=your-secret-key
S3_REGION=ap-southeast-2

# 歸檔設定 (可選)
ARCHIVE_ENABLED=true
ARCHIVE_RETENTION_DAYS=7
ARCHIVE_TIME=03:00
```

### Zeabur 部署

在 Zeabur 專案設定中加入環境變數：

| 變數名稱 | 值 |
|----------|-----|
| `S3_BUCKET` | your-bucket-name |
| `S3_ACCESS_KEY` | AKIA...your-access-key |
| `S3_SECRET_KEY` | your-secret-key |
| `S3_REGION` | ap-southeast-2 |

---

## 4. 測試連線

### 方法 1: Python 腳本

在專案目錄執行：

```bash
python -c "
from storage.s3 import S3Storage
s3 = S3Storage()
print('✓ S3 連線成功')
print(f'  Bucket: {s3.bucket_name}')
print(f'  Region: {s3.region}')
"
```

### 方法 2: 完整測試

```python
import json
from datetime import datetime
from storage.s3 import S3Storage

s3 = S3Storage()

# 測試上傳
test_data = {"test": True, "time": datetime.now().isoformat()}
test_key = "_test/connection_test.json"

s3.client.put_object(
    Bucket=s3.bucket_name,
    Key=test_key,
    Body=json.dumps(test_data),
    ContentType='application/json'
)
print("✓ 上傳測試成功")

# 測試下載
response = s3.client.get_object(Bucket=s3.bucket_name, Key=test_key)
content = json.loads(response['Body'].read().decode('utf-8'))
print(f"✓ 下載測試成功: {content}")

# 清理
s3.client.delete_object(Bucket=s3.bucket_name, Key=test_key)
print("✓ 刪除測試成功")

print("\n🎉 S3 連線測試全部通過！")
```

### 方法 3: API 端點

部署後透過 API 確認：

```bash
curl -H "X-API-Key: your_key" https://your-app.zeabur.app/api/archive/status
```

---

## 5. S3 Lifecycle 規則 (進階)

設定 Lifecycle 規則可進一步降低儲存成本。

### 建議規則

| 天數 | 儲存類別 | 成本 (相對) |
|------|----------|-------------|
| 0-30 天 | S3 Standard | 100% |
| 30-90 天 | S3 Standard-IA | 60% |
| 90+ 天 | S3 Glacier Instant | 20% |

### 設定步驟

1. 進入 S3 Bucket
2. 選擇 **Management** 分頁
3. 點擊 **Create lifecycle rule**
4. 設定：
   - **Rule name**: `archive-lifecycle`
   - **Rule scope**: Apply to all objects
5. 加入 Lifecycle rule actions：
   - **Transition current versions**:
     - 30 天 → Standard-IA
     - 90 天 → Glacier Instant Retrieval
6. 點擊 **Create rule**

### 成本估算 (100GB 資料)

| 方案 | 月費 |
|------|------|
| 全部 S3 Standard | ~$2.30 |
| 含 Lifecycle 規則 | ~$1.20 |

---

## 6. 常見問題排解

### 連線失敗

**症狀**: `NoCredentialsError` 或 `InvalidAccessKeyId`

**解決方案**:
1. 確認環境變數已正確設定
2. 確認 Access Key 沒有多餘空格
3. 確認 IAM 使用者有正確權限

```bash
# 檢查環境變數
python -c "import config; print(f'S3_BUCKET: {config.S3_BUCKET}')"
```

### 權限不足

**症狀**: `AccessDenied` 錯誤

**解決方案**:
1. 確認 IAM Policy 的 Resource ARN 正確
2. 確認 Policy 已附加到使用者
3. 重新產生 Access Key

### Bucket 不存在

**症狀**: `NoSuchBucket` 錯誤

**解決方案**:
1. 確認 bucket 名稱拼寫正確
2. 確認 bucket 所在區域與 `S3_REGION` 一致

### 歸檔任務未執行

**症狀**: 每日 03:00 歸檔未執行

**解決方案**:
1. 確認 `ARCHIVE_ENABLED=true`
2. 確認 `S3_BUCKET` 已設定
3. 檢查應用程式日誌

```bash
# 手動執行歸檔測試
python -c "
from tasks.archive import ArchiveTask
task = ArchiveTask()
task.run()
"
```

---

## 安全建議

1. **定期輪換 Access Key**: 每 90 天更換一次
2. **使用專用 IAM 使用者**: 不要使用 root 帳號
3. **最小權限原則**: 只授予必要權限
4. **啟用 S3 版本控制**: 防止誤刪資料
5. **監控 CloudWatch**: 設定異常存取警報

---

## 相關文件

- [ARCHITECTURE.md](./ARCHITECTURE.md) - 系統架構說明
- [API.md](./API.md) - API 端點文件
- [README.md](../README.md) - 專案總覽
