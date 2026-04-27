# 2026-04-24：air_quality_observations / micro_sensor_readings 分區管理失靈，連 buggy trigger 一起爆

## 症狀

兩波告警先後爆出來：

**第一波（2026-04-24）**：
```
DB 寫入連續失敗：air_quality_microsensors
no partition of relation "micro_sensor_readings" found for row
DETAIL: Partition key of the failing row contains (collected_at) = (2026-04-24 14:09 UTC)
```

**第二波（2026-04-25，第一波修完當天）**：
```
DB 寫入連續失敗：air_quality_microsensors
cannot CREATE TABLE .. PARTITION OF "micro_sensor_readings_20260425"
because it is being used by active queries in this session
```

## 診斷結論

實際是兩個獨立的坑連環爆：

### 坑 1：新分區表沒掛進 cron tables 陣列

`gis-platform/migrations/038_air_quality.sql` 新增兩張 `PARTITION BY RANGE (collected_at)` 表
（`air_quality_observations` / `micro_sensor_readings`），但：

- **038 只預建當天 + 7 天分區**
- **沒更新 022/034 的 `manage_all_partitions()` cron tables 陣列**

7 天後 cron 永遠補不到這兩張表，分區用完 → INSERT 全噴 `no partition found`。

### 坑 2：auto_create_partition() trigger 設計從根本不可行

修第一波時把 022 的「BEFORE INSERT trigger 當分區用完防線」一起補上，結果發現：

1. **PostgreSQL 對 partitioned table 的 BEFORE INSERT trigger 會自動傳到所有子分區**
2. **trigger 在子分區觸發時 `TG_TABLE_NAME` 是子分區名**（不是 parent）
3. 022 原寫 `parent_name := TG_TABLE_NAME` → 算出的 tbl_name 變雙重後綴
   `micro_sensor_readings_20260425_20260425`
4. 試圖把已被 INSERT 鎖住的子分區當成 partitioned parent → noisy lock 衝突報錯

**更深的設計問題**：partition routing 發生在 BEFORE INSERT trigger **之前**。沒有匹配
分區時 PG 直接拒絕 INSERT，trigger 根本沒機會跑。所以 022 想用 trigger 當「分區用完防線」**從一開始就不可能 work**。實測：

```
INSERT future-date row → 報 "no partition found"，trigger 沒觸發
```

trigger 唯一還能做的「保險」是時間邊界（cron 還沒補新一天，但 INSERT 已經過午夜）那個窄窗口。
不太重要，但至少不能讓它本身成為錯誤源。

## 解法

### Migration 059_fix_air_quality_partition_management.sql

- `manage_all_partitions()` tables 陣列加入兩張新表
- inline 重建 `auto_create_partition()`（DB 實際不存在，022 那段 Step 4 應該從沒跑過）
- 兩張表掛 BEFORE INSERT trigger
- 立即 `SELECT manage_all_partitions()` 補當前缺失分區（建了 16 個）

### Migration 060_fix_auto_partition_trigger.sql

- `auto_create_partition()` 改用 `TG_ARGV[0]` 接 parent name、`TG_ARGV[1]` 接時間欄位
- `EXECUTE format('SELECT ($1).%I', time_col) INTO ts USING NEW` 動態讀時間欄位
- 重建兩張表的 trigger 帶上 arguments

## 給後人的教訓

### 1. 新增 partitioned 表必須同時改 `manage_all_partitions()`

跨 repo checklist 缺這條，已經踩過第二次了（022 → 034 → 038 → 059）。

**新增 partitioned 表時必做**：
- migration 內 `CREATE OR REPLACE FUNCTION realtime.manage_all_partitions()` 重新覆蓋
  完整 tables 陣列（複製最新版本，加上新表名）
- 立即 `SELECT realtime.manage_all_partitions();` 補建分區
- 不要只靠 `DO $$ ... $$;` 預建幾天就算完

### 2. PG partitioned table 的 BEFORE INSERT trigger 兩個要記的行為

- **trigger 自動繼承到所有子分區**，無法關
- **trigger 在子分區觸發時 `TG_TABLE_NAME` 是子分區名**
- 通用 trigger function 想拿 parent name → **必須用 `TG_ARGV[0]`**，不要靠 `TG_TABLE_NAME`

### 3. partition routing 比 BEFORE INSERT trigger 還早

「分區用完用 trigger 自動建」這個構想無效。**真正的防線只有 cron 預建**。trigger 最多只是
極端時間邊界的兜底，不能當主要機制看待。

## 跨 repo 影響

| Repo | 動作 |
|---|---|
| gis-platform | 新增 059, 060 migration（已 push） |
| data-collectors | buffer 自動補回，無 code 改動 |
| taipei-gis-analytics | 無影響 |
