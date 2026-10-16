# 執行架構

一句話：一個 Python 進程依 registry 建立所有 collector，由排程器在執行緒池裡輪流跑；每輪結果先存本地 JSON，再經連線池旁路寫進 Supabase，寫不進去就進 buffer 稍後補寫。少數被雲端 IP 封鎖的來源改在 HiCloud VM 上以單檔 cron 執行。

> 本文描述程式行為，數值以程式為準（文中常數都標了出處檔案）。保留期與冷層見 [data-lifecycle.md](data-lifecycle.md)，監控與營運見 [operations.md](operations.md)，collector 清單見 [collectors.md](collectors.md)。

## 架構圖

```
                         ┌──────────────── Zeabur 主容器（main.py，一個進程）────────────────┐
                         │                                                                   │
 collectors/registry.py ─┼─► 初始化（ENABLED？required_env？）                                │
 config.py 開關／間隔    │        │                                                          │
                         │        ▼                                                          │
                         │  schedule 庫（何時觸發）──► CollectorScheduler（怎麼執行）          │
                         │   every N 分＋jitter       ThreadPoolExecutor、skip-if-running     │
                         │   或 fixed_minutes                 │                              │
                         │                                    ▼                              │
                         │                     BaseCollector.run()                           │
                         │                      collect() → dict{'data': …}                  │
                         │                       │                     │                     │
                         │          storage.save()（本地 JSON）   SupabaseWriter.write()     │
                         │                       │                     │ TABLE_MAP 轉換      │
                         │                       │                     │ 亂碼 allowlist      │
                         │                       │                     │ pool.borrow(5s)     │
                         │                       │                     │ _txn: SET LOCAL     │
                         │                       │                     ├─ 成功 → heartbeat   │
                         │                       │                     └─ 失敗 → buffer/     │
                         │                       │                          sb_flush 每 5 分 │
                         │  每日 task：archive、backup_supabase、daily_report、trails_export、 │
                         │            mini_taipei_publish、gfw_hourly_publish（子行程）       │
                         │  長駐 worker：AISStream、RIPE RIS Live（獨立執行緒，不進排程器）     │
                         │  Flask API（/health 等）＋ 主迴圈 heartbeat ＋ watchdog            │
                         └───────────────┬────────────────────────────┬────────────────────────┘
                                         │                            │
                               S3 每日 tar.gz                   Supabase（live.*／reference.*／spatial.*）
                                         ▲                            ▲
                         ┌───────────────┴────────────────────────────┴───────┐
                         │ HiCloud VM（<VM_HOST>）：external/<name>_vm/ 單檔     │
                         │ cron 執行、vm_buffer 本地暫存、每日 health snapshot  │
                         └──────────────────────────────────────────────────────┘
```

## 1. Registry：誰會被啟動

- `collectors/registry.py` 的 `COLLECTOR_REGISTRY` 是唯一清單。每筆 `CollectorEntry(cls, display_name, config_prefix, required_env=(), persistent=False)`。
- `main.py::_init_collector_from_entry` 依序檢查：
  1. `persistent=True` → 交給長駐 worker，不建排程用的 collector。
  2. `{PREFIX}_ENABLED` 是 false → 印「已停用」略過。開關與預設間隔集中在 `config.py` 的 `_COLLECTOR_TOGGLES`。
  3. `required_env` 任一在 `config` 模組上取不到 → 印 ⚠ 略過（**不報錯**）。所以變數要在 `config.py` 用 `os.getenv` 宣告，只寫在 registry 會被 silent skip（[principles D-01](principles.md)）。
  4. 建構失敗 → 印 ✗ 略過。

## 2. 排程：何時跑、怎麼跑

| 機制 | 位置 | 行為 |
|---|---|---|
| 觸發 | `main.py::_schedule_collector` | 預設 `schedule.every(interval).minutes`，首次觸發加 0～min(間隔, 5 分) 的隨機偏移；collector 設了 `fixed_minutes` 就改成每小時固定分鐘（例：海象避開 DB cron 的 :14／:15／:40） |
| 首輪 | `main.py::run_collectors` | 啟動時每個 collector 立即提交一次，彼此間隔 1–3 秒，避免連線尖峰 |
| 執行 | `scheduler.py::CollectorScheduler` | `ThreadPoolExecutor`，`max_workers = max(10, collector 數 + 8)`；同一 collector 上一輪沒跑完就 skip 這一輪並記 warning |
| 逾時 | `BaseCollector.COLLECT_TIMEOUT`（預設 300 秒） | **只記 warning，不中斷**。Python 執行緒無法強制停止，真正的保護是各 HTTP 呼叫自己的 timeout |
| 每日 task | `main.py::_as_task` | archive、backup、daily_report 等包成假 collector 丟進同一個排程器，避免堵主迴圈觸發 watchdog |

## 3. BaseCollector.run()：一輪做什麼

`collectors/base.py`：

1. 產生帶時區的台北時間 `timestamp`（[principles W-01](principles.md)）。
2. `collect()` 回傳 dict；有 `data` 鍵才存檔與寫 DB。
3. `should_persist_local()` 為真時寫本地 JSON（`storage/local.py`）。
4. 有 writer 時呼叫 `supabase_writer.write()`：
   - 一般 collector：寫入例外只印警告，**這一輪仍算成功**。
   - `require_db_write()` 為真的 collector：寫入失敗或回傳 False 就讓這一輪失敗。
   - `MojibakeWriteRejected`（亂碼防護整批拒寫）一律讓這一輪失敗。
5. 結果帶 `_collector_error` → 視為失敗（完整快照型 collector 用它把失敗寫成 ledger，[principles W-09](principles.md)）。
6. 成功：更新 in-memory 的 `last_run`／`last_success_at`，`consecutive_errors` 歸零；之前已達告警門檻（`CONSECUTIVE_ERROR_THRESHOLD`，預設 3）會發恢復通知。失敗：`consecutive_errors` 加一並通知。

第 4 點和 writer 的「零筆不寫心跳」合起來，就是 [operations.md](operations.md)「三個盲點」裡的兩個。

## 4. SupabaseWriter：寫入層

`storage/supabase_writer.py`、`storage/db.py`、`storage/supabase_tables.py`。

### 4.1 TABLE_MAP：collector 對到哪張表

`storage/supabase_tables.py` 的 `TABLE_MAP` 以 collector 名稱為鍵，設定：

| 欄位 | 意思 |
|---|---|
| `history`／`columns` | 歷史表（append-only 或分區）與 INSERT 欄位順序 |
| `current`／`current_key`／`current_columns` | 最新狀態表與 UPSERT 鍵 |
| `upsert_key`／`upsert_strategy` | history 的衝突鍵；`do_nothing` 時用**不指定目標**的 `ON CONFLICT DO NOTHING`（[principles W-04](principles.md)） |
| `keep_on_null` | DO UPDATE 時新值為 NULL 就保留舊值 |
| `current_prune_by` | 寫完 current 後刪掉該欄小於本批時間的舊列（清幽靈資料） |
| `history_dedup_cols` | 只有這些欄位變了才寫 history；每個台北日第一輪全量寫一次當日快照 |
| `is_reference`／`is_multi_table` | 走自訂寫入流程 |

沒有 `TABLE_MAP` 條目的 collector 只存本地與 S3，不寫 DB。每個 collector 的轉換函式是 `_transform_<name>`，都是純函式，不碰連線。

### 4.2 連線池

| 項目 | 值（以程式與部署 env 為準） | 出處 |
|---|---|---|
| 池 | `psycopg2` `ThreadedConnectionPool` 外包一層 `SupabaseConnectionPool` | `storage/db.py` |
| 大小 | `SUPABASE_POOL_MIN` 預設 2、`SUPABASE_POOL_MAX` 預設 15；`config.py` 註解記載主站 env 覆寫為 30（實際值待確認，以 Zeabur 設定為準） | `config.py` |
| 借用逾時 | `SUPABASE_BORROW_TIMEOUT_SEC` 預設 5 秒；借不到 raise `PoolBorrowTimeout`，呼叫端進 buffer | `storage/db.py` |
| 斷路器 | 建線連續失敗時指數退避，冷卻 15 秒起、上限 300 秒；冷卻中 raise `PoolBreakerOpen` | `storage/db.py` |
| 死連線 | 借出前 pre-ping，失敗就關掉換一條；with 區塊出錯時歸還並關閉該連線 | `storage/db.py` |

為什麼是池：2026-06-26 舊版「一條連線＋一把 RLock」在連線卡住（TCP 沒斷、server 不回）時，所有 collector 排隊 3 小時。改成池後，一條壞連線只影響當下借它的那個 collector（[principles L-10](principles.md)）。

需要長交易的工作（`satellite_passes_daily`、`waste_match`）用 `storage/db.py::connect_supabase()` 開自己的連線，不佔共用池。

### 4.3 SET LOCAL：timeout 怎麼生效

`SUPABASE_DB_URL` 連的是 Supavisor transaction pooler（6543）。實測（2026-07-02）pooler 會丟掉連線 startup 的 `statement_timeout`，session 級 `SET` 也會因換 backend 而失效或殘留到別人的連線。所以 `_txn()` 在每個交易的第一句執行 `set_config('statement_timeout', …, true)`（等同 `SET LOCAL`），交易結束自動失效、下個交易重下。預設 `SUPABASE_STATEMENT_TIMEOUT_MS` 30 秒。任何 session 級設定（含 `psycopg2 set_session()`）都不能用（[principles L-09](principles.md)）。

### 4.4 Buffer：寫不進去時

| 項目 | 行為 | 出處 |
|---|---|---|
| 何時寫 | 借不到連線、斷路器開啟、寫入例外 → 把這輪 result 存成 `data/buffer/*.json` | `write()` |
| 不寫 buffer 的 collector | `NO_BUFFER_COLLECTORS`：`jma_warnings`（只記變動，buffer 重放會讓同一變動寫兩筆）、`env_jp_wbgt`（DO UPDATE，晚到的舊批會把上游修正蓋回）；`gfw_vessel_presence` 受授權閘門限制 | `_should_buffer()` |
| 補寫 | `sb_flush` 每 `SUPABASE_BUFFER_INTERVAL` 分（預設 5）跑一次，借一條連線依序補寫 | `flush_buffer()` |
| 過期 | 超過 `BUFFER_MAX_AGE_DAYS`（3 天）直接丟棄，因為對應分區可能已被清掉 | `flush_buffer()` |
| 放棄本輪 | 連續 `BUFFER_FAIL_THRESHOLD`（5）筆失敗就判定 DB 還沒好，留到下輪 | `flush_buffer()` |
| 上限 | `SUPABASE_BUFFER_MAX_FILES` 預設 500，超過先刪最舊 | `config.py` |
| 壞檔 | 被亂碼防護拒絕的 buffer 檔移到 `buffer/quarantine/`，不擋後面的檔 | `flush_buffer()` |

告警：同一 collector 連續 3 次寫入失敗才發 Telegram，恢復時再發一則；借不到連線屬「DB 暫時不可用」，不發 bug 類通知。

### 4.5 Heartbeat

- 成功寫入後，在同一條連線上開**獨立交易**呼叫 `report_collector_heartbeat(name, success, records, error)`，寫進 `metadata.collector_status`。心跳失敗只回滾心跳本身，例外吞掉。
- 寫入失敗時用短逾時（1 秒）另借一條連線回報失敗心跳，借不到就放棄。
- **零筆時 `write()` 在心跳之前就 return**，`last_success_at` 不會更新（見 [operations.md](operations.md)「三個盲點」）。
- 2026-10-16 起（gis-platform `migrations/465_heartbeat_clear_last_error_on_success.sql`）成功心跳會清掉舊的 `last_error`；之前成功時會保留上一次的錯誤。

### 4.6 亂碼防護 allowlist

- `MOJIBAKE_GUARD_COLLECTORS` 目前只有 `ncdr_alerts`。名單內 collector 的文字欄位若含西里爾字母（U+0400–U+04FF），該列剔除、其餘照寫；**整批都壞才** raise `MojibakeWriteRejected`，且不進 buffer。log 只列欄位路徑，不印原文。
- 為什麼只能 allowlist：global_events（國際新聞標題）、aisstream 船名、usgs 地名、衛星名稱都可能合法含西里爾字母，一體適用會讓它們停擺（[principles W-06](principles.md)）。
- 根因修在來源端：NCDR 用 `r.content` 解析，不讓 requests 猜編碼（[principles W-07](principles.md)）。
- 背景：[調查 2026-10-05 GD-2](../investigations/2026-10-05-gd2-mojibake-storage-guard.md)。

## 5. 長駐 worker

不是輪詢型的來源，不進排程器，各自一條 daemon 執行緒：

| worker | 檔案 | 要點 |
|---|---|---|
| AISStream | `workers/aisstream.py` | WebSocket；原始事件先寫本機 durable spool，每小時 gzip NDJSON 上 S3，大小與 sha256 驗證通過才刪本地；與 `ship_ais`、SupabaseWriter 完全分離 |
| RIPE RIS Live | `workers/ripe_ris_live.py` | 有界訂閱清單；`replicas=1`、行程鎖、DB 與私有 S3 是啟用硬閘 |

兩者都在主執行緒先 `prepare()`，硬閘失敗就讓整個進程啟動失敗，避免「容器看起來健康、worker 其實死了」。

## 6. VM 分流

### 何時放 VM

ConnectTimeout（不是 403／404）、本機（台灣 ISP）連得到、Zeabur 容器內 TCP 443 逾時 → 來源封鎖雲端 IP，改跑 HiCloud VM。判斷步驟、目前清單與部署 SOP 的正本是 [EXTERNAL_COLLECTORS.md](../EXTERNAL_COLLECTORS.md)，這裡不重抄。

固定做法：

- 主 repo 仍保留完整 collector 實作與 `TABLE_MAP`／轉換邏輯（schema 只有一份真相），Zeabur 端 `{PREFIX}_ENABLED=false`，避免雙跑。
- VM 版是 `external/<name>_vm/` 的單檔腳本，由 cron 執行，寫同一個 Supabase、同一個 S3 bucket。
- 和主 repo 共用寫入邏輯時要逐位元組相同（例：`external/isohe_port_marine_vm/marine_write.py` 與 `storage/marine_write.py`，由測試強制）。
- VM 是手動部署，**git push 不會生效**（[principles D-04](principles.md)；操作步驟見 [operations.md](operations.md)「VM 操作」）。

### VM 的 buffer 與重送

`external/vm_common/vm_buffer.py` 是主容器 buffer 的精簡版，和 collector 單檔一起部署：

| 項目 | 行為 |
|---|---|
| 存檔 | DB 寫入失敗時，該輪資料存成一輪一檔 JSON |
| 重送 | 每輪開頭先 `flush_pending()` 補寫積壓檔，成功才刪 |
| 失敗 | 單檔補寫失敗就中止本輪 flush（DB 大概率還沒恢復）；JSON 壞檔直接刪 |
| 過期 | `BUFFER_MAX_AGE_DAYS` 預設 3 天（同主 repo）；週跑型可調大 |
| 上限 | 目錄超過 `MAX_BUFFER_FILES` 先刪最舊 |
| 連線 | `connect_with_retry()` 預設重試 2 次、間隔 5 秒 |

VM 沒有主容器的 Telegram 通知與 in-memory 心跳；每日由 `external/_shared/health_report.py` 推 health snapshot 到 S3，主容器日報讀取（見 [operations.md](operations.md)）。部分 VM collector 的寫入繞過 `report_collector_heartbeat`，所以 `metadata.collector_status` 上看起來會是舊的（2026-10-05 調查：ship_ais、waste_positions、cdc_public_health_weekly）。

## 7. 內建 API

`api/server.py`（Flask，背景執行緒；沒設 `API_KEY` 就不啟動）。除 `/health` 外都要 `X-API-Key` header 或 `api_key` 參數。

| 路徑 | 用途 |
|---|---|
| `/health` | 存活檢查，不需認證 |
| `/api/collectors` | collector 清單與狀態 |
| `/api/data/<collector>`、`/latest`、`/<date>`、`/dates` | 讀本地或 S3 的原始 JSON |
| `/api/download/<collector>/<path>` | 下載檔案 |
| `/api/fr24/*` | FR24 資料 manifest 與檔案 |
| `/api/mini-taipei/<system>/*` | Mini Taipei 時刻表發布檔 |
| `/api/archive/status` | 歸檔狀態 |

舊的 `docs/API.md` 停在 2025-12，缺上表後三組端點；以 `api/server.py` 為準。

## 8. 存活保護：heartbeat 與 watchdog

2026-06-24 曾發生主迴圈卡死、進程靜默 14 小時，`/health` 卻一直回 healthy。現在三層：

| 層 | 位置 | 行為 |
|---|---|---|
| 主迴圈心跳 | `main.py` 每秒 `health.heartbeat()` | 主迴圈只負責觸發排程，工作都在執行緒池，不會堵住它 |
| 進程內 watchdog | `health.py::start_watchdog` | 心跳超過 `HEALTH_MAX_LOOP_SILENCE`（預設 120 秒）就發 Telegram 並 `os._exit(1)`，讓平台重啟崩潰的進程（Zeabur 不會因 unhealthy 重啟，但會重啟崩潰的進程） |
| `/health`＋Docker HEALTHCHECK | `api/server.py`、`healthcheck.py`、`Dockerfile` | 心跳過期回 503；DB 狀態只當資訊、**不**因 DB 斷線回 503（重啟救不了外部 DB，還可能重啟迴圈）；探針不做任何 fallback，否則等於關掉 watchdog |

`HEALTH_WATCHDOG_ENABLED` 預設開啟。`WATCHDOG_SELFTEST=true` 會在啟動 90 秒後故意停止心跳，只用於驗證平台會重啟，測完要移除。

## 相關文件

- [principles.md](principles.md)：上文引用的原則
- [data-lifecycle.md](data-lifecycle.md)：本地 → S3 → R2、保留期去哪查
- [operations.md](operations.md)：監控、健康判讀、部署操作
- [EXTERNAL_COLLECTORS.md](../EXTERNAL_COLLECTORS.md)：VM 分流正本
- [new-collector.md](new-collector.md)：新增 collector 的步驟
