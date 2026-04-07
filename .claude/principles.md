# 專案運作原則

## 部署
- **平台**: Zeabur，從 `main` branch 自動部署
- **方式**: `git push origin main` → Zeabur webhook → 重建容器並重啟
- **時區**: 容器環境變數 `TZ=Asia/Taipei`（注意：這會讓 `datetime.now()` 回傳 naive 台灣時間，**有陷阱**，見 pitfalls）

## 資料儲存

### 三層結構
1. **本地 (Zeabur Volume `/data`)**: 收集器即時寫入 JSON
2. **S3 (`migu-gis-data-collector` bucket)**: 每日 03:00 (Taipei) 將前一天的資料打包成 `<collector>/archives/YYYY-MM-DD.tar.gz` 上傳，本地保留 7 天
3. **Supabase (`gis-platform`)**: 旁路寫入 `realtime.*_positions` 等分區表

### Supabase 連線
- **DB URL**: 在 `gis-platform/.env` 的 `DATABASE_POOL_URL`（Supavisor Transaction mode, port 6543）
- **Session timezone**: UTC（重要！詳見 timezone pitfall）
- **分區管理**: `realtime.manage_all_partitions()` 每日 00:05 (Taipei) 預建未來 7 天 + 清理 30 天前
- **自動建分區 trigger**: `realtime.auto_create_partition()` 在 INSERT 找不到分區時自動建立

### S3 為冷備份的真理之源
- 即使 Supabase 寫入失敗，本地 → S3 流程仍會繼續
- **任何 Supabase 資料異常都可從 S3 回補**
- 回補腳本：`scripts/backfill_ship_flight.py`

## 時間戳處理（核心原則）

### 必須使用 timezone-aware datetime
**所有要寫入 Supabase 的 `datetime` 都必須帶時區資訊**，避免 PostgreSQL 將 naive datetime 誤判為 UTC。

```python
from datetime import datetime, timezone, timedelta
TAIPEI_TZ = timezone(timedelta(hours=8))

# ✅ 正確
ts = datetime.now(TAIPEI_TZ)
ts.isoformat()  # "2026-04-07T13:00:00+08:00"

# ❌ 錯誤（即使容器 TZ=Asia/Taipei）
ts = datetime.now()
ts.isoformat()  # "2026-04-07T13:00:00"  ← 沒有時區，會被當 UTC
```

### S3 archive JSON 中的時間欄位格式不一致（歷史包袱）
| 收集器 | 欄位 | 格式 | 備註 |
|--------|------|------|------|
| `ship_ais` | `_fetch_time` / `fetch_time` | 台灣時間 naive | `datetime.now()` |
| `flight_opensky` | `fetch_time` | UTC naive | `datetime.now(timezone.utc)` 但用 strftime 去掉時區 |
| `flight_fr24_zone` | `fetch_time` | UTC naive | 同上 |

回補時要分別處理（見 `scripts/backfill_ship_flight.py`）。

## 資料收集慣例

### Collector 規範
- 繼承 `collectors/base.py` 的 `BaseCollector`
- 實作 `collect() -> dict` 回傳含 `data` 鍵的字典
- `base.py` 的 `run()` 會自動：
  - 計時並產生 `timestamp` (timezone-aware Taipei)
  - 呼叫 `collect()`
  - 存本地 JSON
  - 旁路寫入 Supabase（透過 `supabase_writer`）
  - 失敗時暫存 buffer，定期重試

### Supabase 寫入失敗處理
- 連續 3 次失敗 → Telegram 告警
- 失敗的批次寫入 `data/buffer/<collector>_<timestamp>.json`
- 每 5 分鐘自動 flush buffer

## Git 慣例

- Commit message 用繁體中文 + conventional commits prefix (`fix:` / `feat:` / `refactor:` 等)
- 重大修正請在訊息中說明 **問題 → 修正 → 影響範圍**
- Push 到 `main` 即觸發 Zeabur 部署，**注意衝擊範圍**

## 開發環境

- **Python**: 3.11+
- **指令**: 統一使用 `python3` / `pip3`
- **本地測試 collector**: `python3 -m collectors.<name>` 或寫單檔測試腳本

## 與 Claude 協作

- **CLAUDE.md** 在專案根目錄，記錄技術棧、表結構、常用指令等「always-on」上下文
- **`.claude/`** 在專案根目錄，記錄協作經驗、踩坑歷史、原則
- 重大決策或複雜 debug 完成後，請更新 `.claude/pitfalls/` 留下教訓
