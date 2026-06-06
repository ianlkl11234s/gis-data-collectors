# 監控系統

> 單一 Telegram 日報，蓋三層健康（collector / Supabase / S3）+ 跨層一致 + VM 心跳 + 異常去重 + 成本趨勢 + 今日 Action。
> 目標：**每天早上一則訊息看完整體狀態，不用自己解讀。**

## 北極星

每天 `config.DAILY_REPORT_TIME` 的時刻，Telegram 收到一則 ~12 段日報，看完就知道整套 GIS 基礎設施昨天活得好不好、今天該優先做什麼。**所有層面都涵蓋、沒有盲點、不會洗版。**

---

## 架構

```
┌── Zeabur (data-collectors) ──────────────────────────────────┐
│                                                              │
│  tasks/daily_report.py                                       │
│    ├─ _section_collector_status         （in-memory 心跳）   │
│    ├─ _section_supabase_realtime  ──┐                        │
│    ├─ _section_s3_archives        ──┤   tasks/monitoring.py  │
│    ├─ _section_cross_layer        ──┤   (helper, yaml loader,│
│    ├─ _section_external_vm_health ──┤    RPC client, S3 list,│
│    ├─ _section_anomaly_trend      ──┤    anomaly state)      │
│    ├─ _section_file_stats           │                        │
│    ├─ _section_s3_stats             │                        │
│    ├─ _section_archive              │                        │
│    ├─ _section_system_info          │                        │
│    ├─ _section_cost_trend         ──┤                        │
│    └─ _section_today_action       ──┘                        │
│                                                              │
│  config/cross_layer_map.yaml  ← collector ↔ SB ↔ S3 對應    │
│  config/realtime_tables.yaml  ← 50 張 Supabase 表的清冊     │
│  data/anomaly_state.json      ← D1/D3/D7 去重持久化         │
│                                                              │
└──────────┬───────────┬───────────────────────┬───────────────┘
           ↓           ↓                       ↓
  ┌────────────┐  ┌─────────────┐  ┌─────────────────────┐
  │  Supabase  │  │  AWS S3     │  │  Telegram (送達)    │
  │  realtime  │  │  bucket     │  │                     │
  │  .health_  │  │  ・archives │  │                     │
  │  snapshot()│  │  ・_external│  │                     │
  └────────────┘  │  _vm_health │  └─────────────────────┘
                  └──────┬──────┘
                         │ pushes JSON daily
            ┌────────────┴────────────┐
            ↓                         ↓
  ┌──────────────────┐    ┌──────────────────┐
  │  HiCloud VM      │    │  本機 Mac        │
  │  /opt/external-  │    │  scripts/        │
  │   health/        │    │  local_audit_    │
  │  health_report.py│    │  push.py         │
  │  cron 07:00      │    │  launchd 10:00   │
  └──────────────────┘    └──────────────────┘
```

---

## 日報的 12 個 section

| # | Section | 解的盲點 | 來源 |
|---|---|---|---|
| 1 | 收集狀態 | collector 物件 `last_run` / `consecutive_errors` | in-memory |
| 2 | Supabase realtime 寫入 | **50 表新鮮度從沒監控過** | RPC `realtime.health_snapshot()` |
| 3 | S3 archives 心跳 | flight_fr24 silent fail 32 天類問題 | 掃 S3 `*/archives/*.tar.gz` |
| 4 | 跨層一致性 | **「SB 動但 ARC 卡」這種斷層** | A×B×C 三向交叉 |
| 5 | HiCloud VM 健康 | VM 死了 / IP 又被擋 | S3 `_external_vm_health/<host>/` |
| 6 | 異常 7 天趨勢 | Telegram 每天洗版同一則 | `data/anomaly_state.json` D1/D3/D7 去重 |
| 7 | 檔案統計 | 本機 data/ 容量 | LocalStorage |
| 8 | S3 統計 | bucket-level 用量 / lifecycle 分層 | S3 `get_bucket_stats()` |
| 9 | 歸檔結果 | 昨日 ArchiveTask 結果 | conditional |
| 10 | 系統資訊 | Zeabur 容器資源 | psutil |
| 11 | 成本趨勢 | 月增 > 20% 早期警告 | CloudWatch `BucketSizeBytes` |
| 12 | 今日 Action | **不用自己解讀** | 規則式從前面 sections 推導 1-3 件事 |

---

## yaml 真相來源（重要：新增 collector 必改）

### `config/cross_layer_map.yaml`

每個 collector 對應的 Supabase 表 + S3 prefix + 部署位置。**Daily report 整套跨層檢查的真相**。

```yaml
ship_ais:
  enabled: true
  deployment: hicloud_vm           # zeabur | hicloud_vm | disabled
  expected_interval_min: 10
  supabase_tables: [realtime.ship_positions, realtime.ship_current]
  s3_prefixes:
    - {prefix: ship_ais/archives/, expected_daily: true}
  critical: true                   # 壞了會 page 你
  notes: 自由備註
```

| 欄位 | 用途 |
|---|---|
| `enabled` | 本 collector「應該」要在跑（不論 Zeabur / VM） |
| `deployment` | `zeabur` / `hicloud_vm` / `disabled` — 影響跨層判斷邏輯 |
| `expected_interval_min` | 跨層 A 段（in-memory heartbeat）的容忍門檻 |
| `supabase_tables` | 跨層 B 段（24h 寫入計數）要查的表 |
| `s3_prefixes` | 跨層 C 段（每日 tar.gz）要掃的路徑；`expected_daily: false` 表示事件驅動，不檢查 |
| `critical` | 異常時放紅旗 + 今日 Action 優先 |

### `config/realtime_tables.yaml`

50 張 Supabase 表的清冊。**RPC `realtime.health_snapshot()` 一次撈這份清單**。

```yaml
- {schema: realtime, table: ship_positions,  time_column: collected_at, owner_collector: ship_ais, expected_interval_min: 10, critical: true}
```

| 欄位 | 用途 |
|---|---|
| `schema` + `table` | 真實 Supabase 表名（含 schema） |
| `time_column` | 算 `MAX()` 的時間欄（多數 `collected_at`、少數 `observed_at` / `occurred_at` / `published_date`） |
| `owner_collector` | 對應 `cross_layer_map.yaml` 的 key |
| `expected_interval_min` | 用於判斷 OK / STALE / DEAD |
| `critical` | STALE 或 DEAD 時放紅旗 |

### 維護規則

新增 / 改 collector 時**必須同步**這兩個 yaml — 否則 daily_report 會漏報。已寫進 [`.claude/CLAUDE.md`](../.claude/CLAUDE.md) SOP 步驟 7 + 8。

---

## RPC `realtime.health_snapshot()`

| 項目 | 值 |
|---|---|
| Migration | [`gis-platform/migrations/149_realtime_health_snapshot.sql`](https://github.com/ianlkl11234s/gis-platform/blob/main/migrations/149_realtime_health_snapshot.sql) |
| 簽名 | `realtime.health_snapshot(tables jsonb) → TABLE(schema, table, max_time, count_24h, error_msg)` |
| 輸入 | `[{schema, table, time_column}, ...]`（從 `realtime_tables.yaml` 組） |
| 為什麼用 RPC | 50 條獨立 query → 1 條 RPC，**快 100x + 時間點一致** |
| 容錯 | 個別表查詢失敗回 `error_msg`，不中斷其他表 |
| Security | `SECURITY DEFINER`，避免 caller 缺權限就漏報 |

```sql
-- 測試用法
SELECT * FROM realtime.health_snapshot('[
  {"schema":"realtime","table":"ship_positions","time_column":"collected_at"}
]'::jsonb);
```

---

## 異常 rolling 7 天（D1/D3/D7 去重）

問題：**Telegram 每天洗版同一則**「flight_fr24 archive 落後 33 天」。

解法：`data/anomaly_state.json` 持久化每個異常的 `first_seen`，三個訊號分開處理：

| 分類 | 規則 | 提報邏輯 |
|---|---|---|
| **🆕 新發生** | 上次沒看到、這次出現 | 一定報 |
| **✅ 已修復** | 上次有、這次沒了 | 一定報 |
| **⏳ 持續中** | 兩次都有 | 只在 first_seen + 1/3/7 天提報，其他天靜音 |

**規則式**：`monitoring.should_notify_persistent(anomaly_id, state)` 判斷 `(now - first_seen).days ∈ {1, 3, 7}`。

異常 id 設計：
```
sb:realtime.ship_positions:dead         ← Supabase 表新鮮度
sb:realtime.foo:err                     ← RPC 錯誤
s3:flight_fr24:stale                    ← S3 daily archive 落後
s3:weather:missing                      ← S3 從未歸檔
```

---

## HiCloud VM health snapshot

完整流程見 [`external/_shared/README.md`](../external/_shared/README.md)。摘要：

- **VM cron 07:00** → `health_report.py`：
  - tail collector log 統計 24h runs/success/last_count
  - 系統指標 (uptime / load / disk%)
  - 對來源 API ping 一次（**提早發現 IP 又被擋**）
  - 推 `s3://<BUCKET>/_external_vm_health/<host>/YYYY-MM-DD.json`
- **Zeabur daily_report** 撈 S3、`age > 26h` → 標 VM 失聯 🔴

---

## 本機 Mac audit（compliance / coverage）

完整流程見 [`scripts/local_audit_push.py`](../scripts/local_audit_push.py)。摘要：

- **macOS launchd 每天 10:00** → 跑 `local_audit_push.py`
  - 收三個 GIS repo 的 git status（dirty / ahead）
  - 跑 `taipei-gis-analytics/.claude/skills/data-catalog-audit/audit.py --format json` 拿 compliance / coverage / grade / fatal-warn 統計
  - 推 `s3://<BUCKET>/_external_vm_health/local-<hostname>/YYYY-MM-DD.json`
- **與 VM snapshot 共用** `_section_external_vm_health` 顯示通道，hostname 開頭 `local-` 自動辨識

⚠️ 唯一不在雲上跑的環節 — **Mac 沒開（過了 10:00）= 當天無新本機 snapshot**。雲端其他層不受影響。

---

## 「今日 Action」推薦規則

`_section_today_action` 依優先序掃前面所有 section 的偵測結果，吐 1-3 件最該做的：

1. SB DEAD 且 `critical=true` 的表 — 「**修 `realtime.xxx`（DEAD 超過 N 分）**」
2. S3 archives 落後且 `critical=true` 的 collector — 「**檢查 `xxx` archive task**」
3. VM 失聯 — 「**檢查 HiCloud VM 是否還活著**」
4. 異常持續 ≥ 7 天但未修 — 「**清舊異常**」

最多 3 件，照優先序取。**全綠時印「✅ 沒有需要立即處理的事，安心睡覺」。**

---

## Telegram 範例（實機輸出）

```
📊 資料收集日報 — 2026-06-06

📦 Supabase realtime (50 表 / 47 OK)
  🔴 DEAD（>12x interval）
    realtime.reservoir_daily_ops⚠️ 落後 480 分

☁️ S3 archives (5 OK / 1 異常)
  🔴 落後
    flight_fr24 ⚠️ 最新 2026-05-04（落後 32 天）

🔁 跨層一致性 (24 OK / 1 斷層)
  🔴 flight_fr24 [ABc] SB / collector 動但 S3 archive 沒上（archive task silent fail）

🖥️ HiCloud VM 健康
  🟢 chttl-b1b1b645a3a60c79 (210.61.15.74) up 0.2d / load 0.1 / disk 22%
    ✓ ship_ais 20/20  最後 15:40 6416  snapshot 55MB
    ✓ waste_positions 23/69  最後 15:40 276  snapshot 2MB
    ✓ outbound 4/4 OK
  🟢 local-zhengminhongdeMacBook
    🔴 taipei-gis-analytics audit grade=fail compliance=82.3% coverage=188.1% fatal=105 warn=149
    ✓ data-collectors main
    🟡 gis-platform main dirty=10

📈 異常 7 天趨勢 (新 2 / 持續 0 / 已修復 0)
  🆕 新發生
    s3:flight_fr24:stale
    sb:realtime.reservoir_daily_ops:dead

💰 成本趨勢 (StandardStorage)
  🟢 目前 91.32 GB | 週增 +0.5% | 月增 +2.3%

🎯 今日 Action
  1. 修 realtime.reservoir_daily_ops (DEAD 超過 480 分，critical)
  2. 檢查 flight_fr24 archive task（落後 32 天，最新 2026-05-04）
```

---

## 災難復原 / 重新部署

### Zeabur 端 daily_report 設定

```bash
# 環境變數
DAILY_REPORT_ENABLED=true
DAILY_REPORT_TIME=10:30          # 建議晚於 VM 07:00 + Mac 10:00
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
SUPABASE_DB_URL=...              # 必要：RPC 撈表
S3_BUCKET=migu-gis-data-collector
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
S3_REGION=ap-southeast-2
```

### Migration 套用

```bash
psql "$SUPABASE_DB_URL" -f ../gis-platform/migrations/149_realtime_health_snapshot.sql
```

### HiCloud VM 端（如果整台重來）

依序執行 [`external/ship_ais_vm/README.md`](../external/ship_ais_vm/README.md) → [`external/waste_positions_vm/README.md`](../external/waste_positions_vm/README.md) → [`external/_shared/README.md`](../external/_shared/README.md) 的 Deploy 章節即可。

### 本機 Mac launchd

```bash
mkdir -p ~/.config
# 編輯 ~/.config/.gis-audit-env 填 S3 key（chmod 600）
GIS_REPO_PATH=/path/to/data-collectors
sed -e "s|\${USER}|$USER|g" -e "s|\${GIS_REPO_PATH}|$GIS_REPO_PATH|g" \
    $GIS_REPO_PATH/scripts/com.gis.local_audit.plist \
    > ~/Library/LaunchAgents/com.gis.local_audit.plist
launchctl load ~/Library/LaunchAgents/com.gis.local_audit.plist
```

---

## 故障排除

### `_section_supabase_realtime` 印「RPC 撈不到資料」
- 確認 migration 149 已 apply：`psql "$DB_URL" -c "\df realtime.health_snapshot"`
- 確認 `realtime_tables.yaml` 不是空的

### `_section_external_vm_health` 印「尚未收到任何 VM snapshot」
- VM 上 `python3 /opt/external-health/health_report.py` 手動跑一次看看是否 OK
- 看 `/var/log/external-health/health.log`
- S3 上 `_external_vm_health/<host>/YYYY-MM-DD.json` 是否真的有

### 異常 trend 一直在抱怨同一筆
- 看 `data/anomaly_state.json` 的 `first_seen` 對不對
- 若想強制重置：刪除該檔，下次跑會全部當「新發生」

### 新加 collector 後 daily_report 漏報
- 多半是 `cross_layer_map.yaml` 沒同步加
- 補上後等隔天 daily_report 即可

---

## 相關文件

- [`.claude/CLAUDE.md`](../.claude/CLAUDE.md) — 新增 collector SOP
- [`docs/EXTERNAL_COLLECTORS.md`](EXTERNAL_COLLECTORS.md) — HiCloud VM 上的外部 collector pattern
- [`docs/AWS_INVENTORY.md`](AWS_INVENTORY.md) — S3 bucket / lifecycle 設定
- [`external/_shared/README.md`](../external/_shared/README.md) — VM health_report 詳細部署
- [`scripts/local_audit_push.py`](../scripts/local_audit_push.py) — 本機 audit 推 S3 邏輯
