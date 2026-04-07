# 2026-04-07 — Supabase 寫入 +8h 時區偏移 bug

## TL;DR

`collectors/base.py` 用 `datetime.now()` 產生 naive datetime 寫入 Supabase，
PostgreSQL UTC session 把它當 UTC 解讀，**所有 `collected_at` 偏移 +8 小時**。

從專案上線以來所有 ship/flight 資料都是錯的，前端 timeline 完全對不上實際時間。

## 症狀（前端 mini-taiwan-pulse 觀察到的）

1. 初次發現：4/6 timeline 從 00:00 開始播放，**完全看不到任何船舶/航班**，但 console 顯示載入了 10K+ ships
2. 切換到 4/5 → 還是 0 ships
3. 看到 4/7 的 timeline 在 13:33 就有資料，但**真實時間只有 10:51**（時間在「未來」）
4. 4/6 flight 資料只有 21:00-23:59，前面 00:00-21:00 完全沒有

## 真相：兩個獨立問題疊加

### 問題 A: data-collectors 在 Zeabur 當機 ~61 小時
- 4/4 ~08:00 起 collector 停止寫入 Supabase
- 4/6 ~13:00 才恢復（但因為 +8h bug 顯示為 21:00）
- 期間 4/4、4/5、4/6 大部分時段沒有資料
- **但 S3 冷備份依然在寫入**（因為本地 → S3 流程不依賴 Supabase）

### 問題 B: `datetime.now()` naive timestamp（核心 bug）
- `collectors/base.py` line 61: `timestamp = datetime.now()`
- 容器設定 `TZ=Asia/Taipei`，所以回傳 naive **台灣時間**
- 透過 psycopg2 寫入 Supabase Pooler（session timezone = **UTC**）
- PostgreSQL 把 `"2026-04-07T13:00:00"` 當成 UTC 13:00 存入
- **實際是台灣 13:00 (= UTC 05:00)**，差 8 小時

## 為什麼前端「以為」沒問題那麼久？

因為**整個系統都偏移**：
- 寫入：Taiwan time → 存成 UTC
- 讀取：用 RPC `get_ship_trails(target_date)` 查詢，邊界用 Taiwan timezone 計算
- 偏移後的資料剛好還能對應到偏移後的查詢
- 但前端 timeline 用 `dayStartUnix(today)` 算出**真實 UTC**，跟偏移資料對不上

簡單說：偏移資料只在「跟偏移查詢配對」時才能用，跟絕對時間或前端真實 UTC 對照就會錯。

## 如何發現

加 debug log 印出 `currentTime` (frontend) vs `data range` (backend)：

```
[Ship DEBUG] currentTime=1775404973 (2026-04-05T16:02:53.237Z)  ← Taiwan 4/6 00:02 (real)
  data range: 1775480409 ~ 1775494556 (4/6 13:00 ~ 16:55 UTC)   ← 偏移後的資料
```

差 ~21 小時 → 顯然有問題。

接著查 DB：
```sql
SELECT now() AT TIME ZONE 'Asia/Taipei' AS now_tw,
       (SELECT MAX(collected_at) AT TIME ZONE 'Asia/Taipei' FROM realtime.ship_positions) AS max_tw;
-- now_tw      | 2026-04-07 10:57
-- max_tw      | 2026-04-07 18:52   ← +8h 在「未來」
```

一目了然。

## 修正

### 1. 程式碼修正（`collectors/base.py`）

```python
from datetime import datetime, timezone, timedelta

# 台灣時區（無 DST）
TAIPEI_TZ = timezone(timedelta(hours=8))

# 在 run() 中
timestamp = datetime.now(TAIPEI_TZ)  # ← timezone-aware
# isoformat() 會產生 "2026-04-07T13:00:00+08:00"，PostgreSQL 正確轉 UTC
```

`collectors/ship_ais.py` 的內部 `fetch_time` 也同步修正（雖然 `collected_at` 不是用這個，但為了一致性）。

### 2. 資料修復策略

**選擇了「全部從 S3 重做」**（非 in-place UPDATE），原因：
- UPDATE 跨分區邏輯複雜（`collected_at` 是分區鍵，要搬移分區）
- S3 上有完整 30 天備份
- 一次清乾淨，不用擔心新舊混雜

執行步驟：
```sql
TRUNCATE realtime.ship_positions;
TRUNCATE realtime.flight_positions;
TRUNCATE realtime.ship_current;
```

```bash
python3 scripts/backfill_ship_flight.py 2026-03-09 ... 2026-04-06
```

注意：必須**先部署修正版到 Zeabur**再 TRUNCATE，否則 buggy collector 會繼續寫入髒資料。

### 3. 回補腳本陷阱

`scripts/backfill_ship_flight.py` 處理 S3 archive JSON 時，發現兩個收集器格式不一致：

| 收集器 | 欄位名 | 格式 | 處理 |
|--------|--------|------|------|
| `ship_ais` | `_fetch_time` | 台灣時間 naive | 加 `+08:00` |
| `flight_opensky` | `fetch_time` | UTC naive (用 `strftime` 去掉時區) | 加 `+00:00` |

**證據**：flight 檔名 `flight_opensky_2358.json` 對應 `fetch_time = "...T15:58:38"`。
若 `fetch_time` 是台灣時間應為 `15:58`（檔名 `1558`），實為 UTC 15:58 = 台灣 23:58 ✓。

第一次回補時誤把 flight 也加 `+08:00`，導致變成 -8h 偏移。修正後再跑一次。

## 教訓 & 原則

### 1. **寫入 PostgreSQL 的 datetime 必須 timezone-aware**
即使容器設定 `TZ=Asia/Taipei`，`datetime.now()` 還是 naive。Supabase Pooler session timezone 是 UTC，會把 naive 字串當 UTC 解讀。

**驗證方法**：
```sql
SELECT (SELECT MAX(collected_at) AT TIME ZONE 'Asia/Taipei' FROM <table>),
       now() AT TIME ZONE 'Asia/Taipei';
-- 兩個值的差距應該等於「最後寫入到現在」的真實間隔
-- 如果差 ±8 小時 → 有時區 bug
```

### 2. **容器 TZ 環境變數是雙面刃**
`TZ=Asia/Taipei` 讓 log 顯示台灣時間方便看，但也讓 `datetime.now()` 變成 naive 台灣時間，容易出 bug。
更安全的做法是**容器內部用 UTC**，只在顯示層轉時區。

### 3. **冷備份的價值**
S3 archive 救了這次。即使 Supabase 寫入失敗 / 格式錯了，冷備份還有「真理之源」可以重做。
**任何「線上 DB」都應該有獨立的冷備份**，不然出事就只能吞下去。

### 4. **不同 collector 的內部欄位格式不要混用假設**
ship 用台灣時間 naive、flight 用 UTC naive，兩個都是「能跑」但格式不同。
這種「歷史包袱」在第一次接觸時會踩坑。寫 backfill 腳本前**先看實際資料樣本**。

### 5. **debug 時把 frontend 與 backend 的時間戳並列輸出**
最快定位時區 bug 的方法。直接用 epoch 數字比對，避免時區字串造成的混淆。

### 6. **在分區表上 UPDATE 分區鍵會出事**
這次選擇 TRUNCATE + 重灌，避開了這個坑。如果要 UPDATE collected_at，要記得：
- PostgreSQL 會搬移 row 到新分區
- 如果新分區不存在會 fail
- 大表 UPDATE 很慢，且鎖定範圍大

## 影響範圍 & 後續注意

- ✅ **已修復**：程式碼 + Supabase 30 天歷史資料 + Live collector
- ⚠️ **新增分區**：手動建立 `ship_positions_20260308` ~ `ship_positions_20260326` 等（cron 不會回頭建歷史分區）
- ⚠️ **下次回補類似資料**：要先確認所有需要的分區都存在
- ⚠️ **其他 collectors（youbike, temperature, parking）也有同樣 `datetime.now()` 寫法**：但他們的 `collected_at` 是由 `base.py` 的 `timestamp` 提供，所以已被 base.py 的修正涵蓋。**內部 JSON 的 `fetch_time` 還是 naive**，未來如果要從 S3 backfill 這幾類資料時要注意。

## 相關 commit

- `6e2e2d0` — `fix: 修正 Supabase 寫入 +8h 時區偏移 bug`

## 相關檔案

- `collectors/base.py` — 核心修正點
- `collectors/ship_ais.py` — 一致性修正
- `scripts/backfill_ship_flight.py` — S3 回補腳本
- `storage/supabase_writer.py` — `_transform_*` 用 `ts.isoformat()`
