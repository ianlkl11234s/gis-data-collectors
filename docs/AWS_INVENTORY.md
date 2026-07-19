# AWS 資源清冊

**Last updated**: 2026-05-11
**範圍**：data-collectors 專案使用到的所有 AWS 資源
**用途**：避免忘記 AWS 上有哪些資料、各自的儲存等級、是否仍在使用

> ⚠️ **動到 AWS 後請更新此文件**（轉 storage class、刪除 prefix、新增 bucket、改 lifecycle policy 等）

---

## 1. AWS 服務概覽

| 服務 | 用途 | 配置位置 |
|---|---|---|
| **S3** | Collector 歷史資料歸檔（tar.gz）+ 早期 raw JSON | `.env` 的 `S3_BUCKET` / `S3_ACCESS_KEY` |
| **Redshift** | 資料倉儲（dbt pipeline，與 data-collectors 解耦） | `.env` 的 `REDSHIFT_*` |
| **IAM** | S3 access key 對應的 IAM user | （AWS Console 管理） |

> Redshift 由 dbt pipeline 使用，與 data-collectors 的即時收集主路徑無關（主路徑寫 Supabase）。

---

## 2. S3 Bucket 配置

| 項目 | 值 |
|---|---|
| Bucket | `migu-gis-data-collector` |
| Region | `ap-southeast-2` (Sydney) |
| 總容量 | **91.32 GB** / 80,657 objects（2026-05-11 snapshot） |

### Lifecycle Policy（套用整個 bucket，Prefix=""）

```
Rule ID: tiered-cold-storage
  30 天後  → STANDARD_IA
  90 天後  → GLACIER_IR (Glacier Instant Retrieval)
  Multipart upload >7 天未完成 → 中止
```

`flight_fr24/` **不適用此規則**（已手動全部轉 DEEP_ARCHIVE，見下方）。

---

## 3. 各 Prefix 用途與狀態

### 🟢 活躍 collector（最新 ≤ 7 天，每天 archive）

| Prefix | 對應 collector | 容量 | Objects | 備註 |
|---|---|---|---|---|
| `ship_ais/` | ship_ais (AIS 船舶) | 13.94 GB | 3,566 | 大宗 |
| `youbike/` | youbike (3 城) | 12.60 GB | 7,235 | 大宗 |
| `bus/` | bus (六都公車) | 6.23 GB | 72 | 全 archives |
| `satellite/` | satellite | 2.72 GB | 44 | 全 archives |
| `tra_train/` | tra_train | 2.37 GB | 43,736 | 物件多但小 |
| `bus_intercity/` | bus_intercity | 1.30 GB | 27 | 全 archives |
| `weather/` | weather (CWA) | 1.21 GB | 1,884 | |
| `cwa_satellite/` | cwa_satellite (PNG) | 0.92 GB | 34 | |
| `tra_static/` | tra_static | 0.80 GB | 160 | |
| `iot_wra/` | iot_wra | 0.53 GB | 17 | |
| `freeway_vd/` | freeway_vd | 0.48 GB | 69 | |
| `temperature/` | temperature (CWA grid) | 0.15 GB | 1,635 | |
| `rain_gauge_realtime/` | rain_gauge_realtime | 0.14 GB | 22 | |
| `air_quality_microsensors/` | air_quality_microsensors (LASS) | 0.13 GB | 27 | |
| `launch/` | launch (火箭發射) | 0.10 GB | 41 | |
| `flight_fr24_zone/` | flight_fr24_zone (FR24 空域) | 0.08 GB | 69 | |
| `air_quality_imagery/` | air_quality_imagery (PNG) | 0.07 GB | 27 | |
| `rail_timetable/` | rail_timetable | 0.04 GB | 69 | |
| `flight_opensky/` | flight_opensky | 0.04 GB | 69 | |
| `ncdr_alerts/` | ncdr_alerts | 0.02 GB | 34 | |
| `road_event_live/` | road_event_live | 0.02 GB | 2 | |
| `waste_positions/` | waste_positions (垃圾車) | 0.02 GB | 7 | |
| `groundwater_level/` | groundwater_level | 0.01 GB | 21 | |
| `river_water_level/` | river_water_level | 0.01 GB | 22 | |
| `earthquake/` | earthquake | 0.003 GB | 66 | |
| `water_reservoir/` | water_reservoir | 0.003 GB | 23 | |
| `air_quality/` | air_quality (MOENV 77 站) | 0.003 GB | 27 | |
| `road_event_planned/` | road_event_planned | 0.001 GB | 1 | |
| `water_reservoir_daily_ops/` | water_reservoir_daily_ops | <0.001 GB | 20 | |

### 🟡 已停用 collector（殭屍 prefix — 留歷史，不會再寫入）

| Prefix | 對應 collector | 容量 | Objects | 最後寫入 | 狀態 |
|---|---|---|---|---|---|
| `flight_fr24/` | flight_fr24 | **32.38 GB** | 2,541 | 2026-05-04 | **IP 被封鎖，已停**；全部 DEEP_ARCHIVE |
| `parking/` | parking | 10.43 GB | 3,781 | 2026-02-03 (97d) | 已停（registry 還在但 toggle off） |
| `vd/` | vd | 1.60 GB | 5,592 | 2026-01-14 (117d) | 已停 |
| `ship_tdx/` | ship_tdx | 0.05 GB | 8,483 | 2026-02-15 (85d) | 已停 |

### 🔵 非 collector 用途 prefix（應用端 / 部署）

| Prefix | 用途 | 容量 |
|---|---|---|
| `flight-arc/` | 應用端使用 | 2.11 GB |
| `deploy-assets/` | 部署資產（含 `climate/*_latest.{png,json}` 單幀 + `climate/frames/` 多幀時間軸 PNG＋manifest.json，由 global_climate_bake 每 6h 產出、pulse container sync）| 0.42 GB |
| `pulse-db/` | 應用端 | 0.29 GB |
| `mini-taipei/` | 應用端（mini taipei publish task） | 0.05 GB |
| `rail-data/` | 應用端 rail 相關 | 0.05 GB |
| `_external_vm_health/` | **監控**：HiCloud VM + 本機 Mac 每日推 health snapshot JSON。被 `tasks/daily_report.py` 撈來顯示「VM 健康」段。**勿手動改 / 勿加 lifecycle** | < 0.01 GB |

---

## 4. 特殊狀態 — flight_fr24 Deep Archive

### 為何在 Deep Archive
- `flight_fr24` collector 因 **IP 被 FR24 封鎖**而停用（最後寫入 2026-05-04）
- 32.38 GB 歷史資料想保留以備日後解封後分析使用
- 已從 STANDARD/STANDARD_IA 手動轉 **DEEP_ARCHIVE**（2026-05-11 操作）

### 月費對照
| Tier | 月費 |
|---|---|
| 原 STANDARD_IA | ~$0.40 USD/月 |
| 現 DEEP_ARCHIVE | **~$0.032 USD/月** |

### 注意事項
- **180 天 minimum 計費**：2026-05-11 起算，11/07 之前刪除或轉走會被收剩餘天數費用
- 取出需要 5-12 小時（Bulk 模式）
- 物件本身 metadata 仍可隨時查（list、headobject 都是即時的，只有讀內容才需 restore）

### 救資料指令
```bash
# 1. 發起 restore 請求（單檔）
aws s3api restore-object \
  --bucket migu-gis-data-collector \
  --key 'flight_fr24/archives/2026-05-04.tar.gz' \
  --restore-request '{"Days":7,"GlacierJobParameters":{"Tier":"Bulk"}}'

# 2. 等 5-12 小時，檢查 restore 狀態
aws s3api head-object \
  --bucket migu-gis-data-collector \
  --key 'flight_fr24/archives/2026-05-04.tar.gz' \
  --query 'Restore'
# 出現 ongoing-request="false" 表示可下載

# 3. 下載
aws s3 cp 's3://migu-gis-data-collector/flight_fr24/archives/2026-05-04.tar.gz' .
```

**整批救（2,541 物件）成本**：Bulk $0.0025/GB × 32.38 GB ≈ **$0.08 USD**

---

## 5. 常用操作 cheatsheet

### 看整個 bucket 用量
```bash
python3 -c "
import boto3, os
from dotenv import load_dotenv; load_dotenv('.env')
s3 = boto3.client('s3', region_name=os.environ['S3_REGION'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY'])
total_n=0; total_b=0
for p in s3.get_paginator('list_objects_v2').paginate(Bucket=os.environ['S3_BUCKET']):
    for o in p.get('Contents',[]):
        total_n+=1; total_b+=o['Size']
print(f'{total_n} objs / {total_b/1024/1024/1024:.2f} GB')
"
```

### 看某個 prefix 用量
```bash
aws s3 ls s3://migu-gis-data-collector/flight_fr24/ --recursive --summarize | tail
```

### 看 lifecycle policy
```bash
aws s3api get-bucket-lifecycle-configuration --bucket migu-gis-data-collector
```

### 把某個 prefix 全部轉到 Deep Archive
```python
# 完整腳本見 docs/AWS_INVENTORY.md §4 的 flight_fr24 移轉案例
import boto3, os
from concurrent.futures import ThreadPoolExecutor
s3 = boto3.client('s3', ...)
for obj in s3.get_paginator('list_objects_v2').paginate(Bucket='...', Prefix='xxx/'):
    for o in obj.get('Contents',[]):
        s3.copy_object(
            Bucket='...', Key=o['Key'],
            CopySource={'Bucket': '...', 'Key': o['Key']},
            StorageClass='DEEP_ARCHIVE',
            MetadataDirective='COPY')
```

### 刪除整個 prefix（不可逆，需確認！）
```bash
# 殭屍 prefix 真的不要時才執行
aws s3 rm s3://migu-gis-data-collector/parking/ --recursive
```

---

## 6. 何時要更新此文件

**動到 AWS 的任何下列操作後，請更新此文件對應段落**：

| 操作 | 要更新的段落 |
|---|---|
| 啟用 / 停用某個 collector（toggle on/off） | §3（移動到對應分類） |
| 手動轉某個 prefix 的 storage class | §3 + §4 案例補一節 |
| 改 bucket lifecycle policy | §2 |
| 新增 / 刪除 prefix | §3 |
| 刪除殭屍 prefix | §3 移除該行 |
| 更換 bucket / region | §2 |

**最低限度**：每季掃一次（執行 §5 第一條腳本），確認用量沒突然暴增。

---

## 7. 相關文件

- [S3_SETUP.md](S3_SETUP.md) — Bucket/IAM 初始設定
- [ARCHITECTURE.md](ARCHITECTURE.md) — 整體架構
- [`tasks/archive.py`](../tasks/archive.py) — daily archive 流程實作
- [`storage/s3.py`](../storage/s3.py) — S3Storage class
