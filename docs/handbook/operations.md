# 監控與營運

一句話：每天一份 Telegram 日報看整體，DB 端另有一個「整條管線停寫」的死人開關；但心跳不等於健康，判斷一個 collector 好不好要分開看「有沒有跑、有沒有解析成功、有沒有新資料」三件事。動正式環境時逐項套用、每項後做健康檢查，出事時先切斷最大的來源。

本文件合併了舊的 `docs/MONITORING.md` 與 `docs/MONITORING_GREEN_FIRST.md`（兩檔現在只剩指標）。

## 1. 監控全景

| 層 | 什麼時候 | 在哪 | 看什麼 |
|---|---|---|---|
| collector 即時告警 | 每輪 | `collectors/base.py`、`utils/notify.py` | 連續失敗達 `CONSECUTIVE_ERROR_THRESHOLD`（預設 3）發通知，恢復時再發一則 |
| DB 寫入告警 | 每次寫入 | `storage/supabase_writer.py` | 同一 collector DB 寫入連續失敗 3 次發通知；借不到連線不洗版 |
| 進程存活 | 每秒 | `health.py`、`/health` | 主迴圈心跳超過 120 秒 → watchdog 重啟進程（見 [architecture.md](architecture.md) §8） |
| 每日日報 | `DAILY_REPORT_TIME`（預設 08:00，容器時間＝台北） | `tasks/daily_report.py`、`tasks/monitoring.py` | 三層健康（collector／Supabase／S3）＋跨層一致＋VM＋異常趨勢＋成本＋今日 Action，見 §2 |
| 日報附帶檢查 | 同上 | `_check_silence()`、`_check_disk_usage()` | collector 超過 2 倍間隔沒跑；本地檔案總量與共用檔案系統壓力（80% 警告、90% 嚴重） |
| DB 死人開關 | pg_cron 每 15 分 | gis-platform `migrations/255_collector_freshness_alert.sql` `metadata.check_collector_freshness(30)` | **所有** enabled collector 的 `MAX(last_success_at)` 超過 30 分鐘 → 用 pg_net 發 Telegram。用途是「整個 collector 進程死掉」，抓不到單一 collector 停擺（盲點 1） |
| VM 健康 | VM cron 每日 | `external/_shared/health_report.py` | 推 `s3://<BUCKET>/_external_vm_health/<host>/YYYY-MM-DD.json`，日報讀取 |
| 本機稽核 | macOS launchd 每日 | `scripts/local_audit_push.py` | 三個 GIS repo 的 git 狀態與 analytics 資料目錄稽核，推同一個 S3 前綴（`local-<hostname>`）；Mac 沒開機當天就沒有 |
| 外部巡檢（Hermes） | 30 分鐘／4 小時 | 不在本 repo | 唯讀巡檢 Supabase 新鮮度、S3 metadata 與 VM health，發 Discord。**現在是否仍在執行待確認**（查法見 §2.5） |

## 2. 每日日報

### 2.1 區塊

`tasks/daily_report.py::_build_report` 的順序（以程式為準）：

| 區塊 | 解決的盲點 | 來源 |
|---|---|---|
| 昨日健診 | 一眼看出昨天是正常／有問題／嚴重 | 彙整下列結果 |
| 收集狀態 | collector 的 `last_run`、`consecutive_errors` | in-memory |
| Supabase realtime 寫入 | 各表新鮮度 | RPC `public.health_snapshot()` |
| GFW 發布健康 | GFW 每日發布 | 發布 metadata |
| S3 archives 心跳 | archive task 靜默失敗（曾有 flight_fr24 落後 32 天沒人發現） | 掃 `*/archives/*.tar.gz` |
| Supabase 備份健康 | 備份三機器人 | 備份狀態表 |
| Retention 覆蓋 | 沒登記保留期的表 | `metadata.check_retention_coverage()`（有盲區，見 [principles R-10](principles.md)） |
| 跨層一致性 | 「DB 有在寫但 S3 歸檔卡住」這類斷層 | collector × DB × S3 三向交叉 |
| HiCloud VM 健康 | VM 掛掉或 IP 又被擋 | S3 `_external_vm_health/` |
| 異常 7 天趨勢 | 同一則告警每天洗版 | `data/anomaly_state.json` |
| 檔案統計、S3 統計、成本趨勢 | 本地容量、bucket 用量、月增 > 20% 預警 | 本地、S3、CloudWatch |
| 今日 Action | 不用自己解讀 | 規則式推出最多 3 件事 |
| 歸檔結果、系統資訊 | 昨日 ArchiveTask、容器資源 | 有設定才顯示 |

另外會發一則 3–5 行的精簡速報（`_build_speed_digest`）。每個區塊各自 try/except，一個失敗不會讓整份日報失敗。

### 2.2 清冊 yaml（新增或改 collector 必改）

| 檔 | 用途 |
|---|---|
| `config/cross_layer_map.yaml` | 每個 collector 的 `enabled`、`deployment`（`zeabur`／`hicloud_vm`／`disabled`）、`expected_interval_min`、`supabase_tables`、`s3_prefixes`（`expected_daily: false` 表示事件驅動不檢查）、`critical`、`notes`。跨層檢查的真相來源 |
| `config/realtime_tables.yaml` | 要查新鮮度的表清單：`schema`、`table`、`time_column`、`owner_collector`、`expected_interval_min`、`critical`。張數以檔案為準（舊文件寫的「50 張」早已過時） |

漏改時日報會默默漏報；CI 的 `tests/test_cross_layer_sync.py` 會擋一部分。`time_column` 必須有以它開頭的索引，否則健康查詢會全表掃描（[principles L-08](principles.md)）。

### 2.3 新鮮度與異常去重

- `tasks/monitoring.py::classify_freshness`：資料年齡超過 `expected_interval_min` 的 3 倍為 **STALE**，12 倍為 **DEAD**，沒有資料為 **NEVER**。事件驅動或季節性資料（地震、季外的 WBGT）長期沒新列屬預期，門檻放寬寫在清冊。
- RPC `public.health_snapshot(tables jsonb)`（gis-platform `migrations/306_health_snapshot_public.sql`）一次查完清單上所有表，回傳每表 `max_time`、`count_24h`、`error_msg`；個別表失敗不影響其他表。用兩段式查詢（近 2 天分區＋必要時全表），早期版本曾單次 29 分鐘，修好後要防止退化（[pitfall 2026-07-23](../../.claude/pitfalls/2026-07-23-disk-io-cron-spiral.md)、[2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md)）。重新驗證成本用 `scripts/diagnose_health_snapshot.py --statement-timeout-ms 15000`，只記錄時間與錯誤類型。
- 異常去重：每個異常記 `first_seen`。新發生、已修復一定報；持續中的只在第 1、3、7 天報，其他天靜音。異常 id 例：`sb:live.ship_positions:dead`、`s3:flight_fr24:stale`。想重置就刪 `data/anomaly_state.json`。

### 2.4 今日 Action 規則

依序取最多 3 件：critical 表 DEAD → critical collector 的 S3 歸檔落後 → VM 失聯 → 持續 ≥ 7 天未修的異常。全綠時印「沒有需要立即處理的事」。

### 2.5 狀態契約（綠燈優先）

所有巡檢共用的判定語意（2026-07-22 定）：

| 狀態 | 意思 |
|---|---|
| green | 所有能驗證的服務都健康。事件驅動、hash 去重、明確停用的表屬預期狀況，不算錯 |
| watch | 一次性的監控或 S3 失敗，或歸檔還沒到台北 04:00 完成期限 |
| critical | 確認的新鮮度或可用性事故；暫時性的 Supabase／S3 失敗連續 3 次才升級 |
| unknown | 監控無法判定狀態；**不可以說成 green** |

通知只在狀態轉換時發（新事故、升級、恢復）。巡檢一律唯讀，不得重啟、重新部署、寫資料或改設定。外部巡檢（Hermes）是否仍在運作**待確認**。2026-10-16 查到的事實：Zeabur openAB 專案仍有 `habermas-hermes` 服務；同一容器負責的 air-ticket-radar 寫入（`air_tickets.fare_offers`）停在 2026-09-08；DB 端無法佐證巡檢是否還在——`pg_stat_statements` 已滿（5,000 筆上限、持續淘汰），低頻呼叫會被擠掉，查無 `gis_monitor_hermes` 的紀錄不代表沒呼叫。查法：Zeabur 主控台看該服務的執行狀態與最近 log，或看 Discord 巡檢頻道最後一則訊息的時間。

## 3. 健康判讀：三件事與三個盲點

### 3.1 三件事分開看

| 要回答的問題 | 看什麼 | 不能只看 |
|---|---|---|
| 有沒有跑？ | collector log、`metadata.collector_status.last_run_at`、日報「收集狀態」 | 清冊或 config 的預設值（曾有 19 個正在寫的 collector 被自動標成停用） |
| 有沒有解析成功？ | run ledger／receipt 表、`consecutive_errors`、`_collector_error` | `last_success_at`（見盲點 2） |
| 有沒有新資料？ | 目標表 `max(time_column)` 與資料內的來源時間；共用表要分來源 | 心跳；HTTP 200；`updated_at`（UPSERT 不一定更新它） |

「0 筆」常是合法結果（沒雷雨、沒共機、沒地震）。判斷上游是否斷供，可靠度由高到低：ledger 表 → 第二資料源交叉驗證 → 第三方訊號反證（例：同期有雷雨特報卻 0 筆落雷）。S3 歸檔檔案大小連續好幾天完全一樣，表示內容一樣，是上游停供的免費時間軸。正本：本 repo `.claude/pitfalls/2026-08-07-silent-upstream-outage.md`。

### 3.2 三個盲點

| # | 盲點 | 程式位置 | 後果 |
|---|---|---|---|
| 1 | DB 死人開關只看所有 enabled collector 的 `MAX(last_success_at)`，只要有一個還在寫，其他停擺都不告警 | gis-platform `migrations/255_collector_freshness_alert.sql` | 乾旱燈號自 2026-05-15 停寫，約 143 天無告警 |
| 2 | `write()` 失敗只回傳 False 或例外被印成警告，非 `require_db_write` 的 collector 這一輪仍算成功 | `collectors/base.py::run()` | in-memory 狀態與通知都顯示成功 |
| 3 | 零筆（或去重後沒有新列）時 `write()` 在心跳之前就 return，`last_success_at` 不更新，舊的 current snapshot 也不會清 | `storage/supabase_writer.py::write()` 的 `if not records: return True` | 上游回空時看起來像 collector 掛了；已撤銷的舊資料（例：限水區域）永久殘留 |

另一個已修正的盲點：`collect()` 本身 raise 時 writer 不會被呼叫，原本不寫任何心跳，狀態表看不到「每輪都失敗」。2026-10-23 起 `BaseCollector.run()` 會自己回報失敗心跳（H-05，[pitfall 2026-10-21](../../.claude/pitfalls/2026-10-21-news-llm-all-or-nothing.md)）。

調查證據：[2026-10-05-collector-health](../investigations/2026-10-05-collector-health.md)（DIO-4、DIO-6）。決策背景：.gis-agent-system `decisions/0018-data-freshness-auto-refresh.md`。

### 3.3 `last_error` 怎麼讀

- 先看 `consecutive_errors` 和 `last_success_at`／`last_run_at` 的時間，再看 `last_error`。`consecutive_errors = 0` 而且最近有成功，`last_error` 就是歷史訊息，不是現況。
- 2026-10-16 起（gis-platform `migrations/465_heartbeat_clear_last_error_on_success.sql`）成功心跳會清掉 `last_error`；成功但帶訊息（例：部分端點失敗）會保留該訊息。在這之前，成功時會一直保留上一次的錯誤——例：immigration 的 `last_error` 停在 2026-09-28 VM 磁碟滿，但它每小時都成功；台電落雷顯示 `borrow timeout 5.0s`，曾誤導往連線池查。
- 已停用或不再執行的 collector 會保留最後一筆錯誤。

### 3.4 已知監控缺口（2026-10-15 盤點）

- 共用表時，一個來源壞掉會被其他來源的新鮮度蓋過去：`lightning_events`（台電、CWA、JMA）、`internet_health_*`（Cloudflare、IODA、RIPE）。
- `tra_static`、`satellite_passes_daily`、`global_climate_bake` 啟用中但沒有表級監控（`tra_static` 沒有 TABLE_MAP 條目、`satellite_passes_daily` 用獨立連線寫入、`global_climate_bake` 只輸出到 S3）。
- VM 上的 `ship_ais`、`waste_positions`、`cdc_public_health_weekly` 寫入繞過 generic writer，`collector_status` 會是舊的；`isohe_port_marine`（VM）在 `collector_status` 沒有列；`aisstream` 沒有同名 status。
- 動物福利快照類（`animal_adoption`、`animal_shelter_*`、`animal_veterinary_clinics` 等）抓取失敗時只在自己的 run ledger 記 `failed`：寫入 failed ledger 本身算一次成功的 DB 寫入，`collector_status` 照樣回報成功（`BaseCollector.run()` 的 `writer_reported` 讓 `_collector_error` 不再補報失敗心跳）。2026-10-23 00:56 起 `live.animal_adoption_snapshot_runs` 已記 `failed`，`collector_status` 卻顯示正常。程式未修；判斷這幾支健康要看 ledger 的 `status`，不能只看 `collector_status`。
- `config/realtime_tables.yaml` 曾把 `live.aisstream_archive_manifests` 的 `time_column` 寫成表上沒有的 `created_at`（DIO-5），2026-10-16 已改成 `uploaded_at`。
- `aisstream` 清冊曾標 `enabled: false`／`deployment: disabled`，但 worker 一直在跑；2026-10-16 依 `live.aisstream_position_observations.received_at`、`aisstream_ingest_health.updated_at`（數分鐘內）校正為 `enabled: true`／`zeabur`。S3 檢查讀 `aisstream/raw/v1/date=YYYY-MM-DD/`，`tasks/monitoring.py` 會從 key 解析日期。

## 4. 量測口徑（DB 負載）

量 Disk IO 前先知道工具看不到什麼（[principles M-01～M-07](principles.md)）：

| 工具 | 看得到 | 看不到 |
|---|---|---|
| `pg_stat_io`（依 `backend_type`） | 實際讀寫量，含 autovacuum | 是哪個語句、哪張表 |
| `pg_statio_user_tables` | 表級讀取（含 autovacuum 造成的） | 語句 |
| `pg_stat_statements` | 每個 top-level 語句的讀取與暫存檔 | autovacuum、背景程序、沒完成的語句 |

- 2026-10-12 實測 autovacuum 佔讀取 58%，用 `pg_stat_statements` 完全看不到。
- 統計在 DB 重啟時歸零，窗口起點用 `pg_postmaster_start_time()`；`pg_stat_database.stats_reset` 是 NULL，不能拿來算速率；`track_functions = none`，沒有函式層級呼叫數。
- 因此「零呼叫」「`idx_scan = 0`」只代表重啟後那幾天。刪索引或 RPC 前要有三個證據：函式定義、各 repo 程式碼（gis-platform 的 RPC 也被其他前端 repo 呼叫）、EXPLAIN（[principles L-05](principles.md)）。
- 不同口徑的數字不能比較；事故中量到的數字不能排優先順序；分項推估不能相加。

查詢範本（唯讀）在 gis-platform `docs/research/2026-10-12-disk-io-round2.md` §2，例如依 backend_type 的讀寫：

```sql
select backend_type, sum(reads*op_bytes)/1073741824.0 read_gb, sum(writes*op_bytes)/1073741824.0 write_gb
from pg_stat_io group by 1 order by 2 desc nulls last;
```

- 膨脹自檢用「每列 bytes」與 HOT 比例（`n_tup_hot_upd / n_tup_upd`），純 catalog 查詢、零 IO。`n_live_tup` 是估計值，下結論前要驗證。
- 收到 Disk IO 告警第一時間先 dump 統計快照，平台可能隨時重啟。
- 正式 DB 查詢一律唯讀、有 LIMIT 或有界聚合；重查詢避開台灣 10:00–20:00。

## 5. 部署與日常操作

### 5.1 Zeabur（主容器）

- push 到 `main` 會觸發 Zeabur 從 GitHub 重建並部署，注意衝擊範圍。
- **env 改完要 restart**；程式改了要 **redeploy**，restart 拿不到新程式。`git commit --allow-empty` 不會觸發 build。
- `*_ENABLED=true` 卻沒資料：先看啟動 log 的 ⚠ 行（`required_env` 缺漏會 silent skip），再查 image 有沒有裝相依套件（缺套件時 lazy import 靜默失敗）。
- 判斷部署生效：看只有新版才會產生的內容特徵，或進容器確認新程式已載入；不要看 `updated_at`，`deployment list` 的 RUNNING 標籤也會滯後。
- **Zeabur CLI 會印出祕密值**：`zeabur variable env`、`variable create`（寫入後回顯全部變數）都用表格印出值，遮蔽規則抓不到。寫入一律把輸出導向 `/dev/null`，查詢時只列 key 名稱。跨 service 的變數參照（`${…}`）用 dashboard 設，CLI 的參數解析會弄壞它。
- **權限不足或不想碰 CLI 時，從 DB 行為確認部署**：例如 2026-10-15 #146／#147 合併後，以 `iot_wra_stations`、`uswg_stations` 的 `n_tup_upd` 在 11 分鐘內 0 增加確認新版「沒變不寫」已生效（gis-platform `docs/research/2026-10-16-disk-io-round3.md` §4）。
- 新 collector 上線後觀察 24 小時錯誤率；ConnectTimeout 型的錯誤可能是 IP 被封，見 [EXTERNAL_COLLECTORS.md](../EXTERNAL_COLLECTORS.md)。

### 5.2 VM 操作

VM（`<VM_HOST>`）上的 collector 是手動部署，git push 不會生效。部署步驟：

1. **先 diff VM 現行檔**和要部署的版本，確認差異只有這次的改動（VM 上可能有沒進 git 的修改）。
2. **備份現行檔**（例：`<檔名>.bak_YYYYMMDD`）。
3. 上傳新版後 **`python3 -m py_compile`** 確認語法。
4. **看 log** 等下一輪 cron，確認出現新版才有的訊息（例：waste #148 的「重複點由 ON CONFLICT DO NOTHING 略過」）。
5. 要配合 DB 變更時注意順序：寫入端冪等先上，DB 唯一索引後建（[principles I-02](principles.md)）；可挑 collector 的 quiet hours 動 DB。

VM 的建置、cron 與 health report 部署細節見 [EXTERNAL_COLLECTORS.md](../EXTERNAL_COLLECTORS.md) 與 `external/*/README.md`。VM 健康排障：在 VM 上手動跑一次 `health_report.py`，看 health log，確認 S3 上當天的 snapshot 存在。

#### SSH 只允許金鑰登入（2026-10-16 起）

- 設定：新增 `/etc/ssh/sshd_config.d/00-hardening.conf`，內容為 `PasswordAuthentication no`、`KbdInteractiveAuthentication no`、`PermitRootLogin prohibit-password`。原本的主設定檔備份在 `/etc/ssh/sshd_config.bak.20261016`。檔名用 `00-` 開頭，是因為 sshd 對同一選項採用第一個讀到的值，要排在其他 drop-in 前面。
- 原因：`auth.log` 約 5 天內有約 8.9 萬次密碼爆破嘗試；同期兩週內所有成功登入都是 publickey，關掉密碼登入不影響現有用法。
- ⚠️ **VM 只登記了一把金鑰**。換電腦或重灌之前，要先用舊電腦把新電腦的公鑰加進 `/root/.ssh/authorized_keys` 並測試登入成功；否則只能從 HiCloud 主控台（網頁 console）登入救援。

**改遠端 sshd 的安全步驟**（改錯會把自己鎖在外面，每次都照做）：

1. 先開一條連線並保持不關（建議用 SSH ControlMaster 留一條 master 連線），全程不要登出。
2. 改設定前先排一個自動還原：`systemd-run --on-active=10min --unit=sshd-revert <還原指令>`（還原指令＝刪掉新的 drop-in、把備份複製回去、`systemctl reload ssh`），萬一後面斷線，10 分鐘後會自動回到舊設定。
3. 寫入設定後先 `sshd -t` 檢查語法，再 `systemctl reload ssh`（服務名依發行版可能是 `sshd`；reload 不會中斷既有連線）。
4. **另開一條新連線**測試：金鑰可以登入、密碼登入會被拒絕（例：`ssh -o PubkeyAuthentication=no -o PreferredAuthentications=password …` 應該失敗）。
5. 新連線測試都通過，才取消還原計時器（`systemctl stop sshd-revert.timer`）；沒通過就什麼都不做，等它自動還原。

### 5.3 祕密處理

- 不讀 `.env` 的值再過濾；只查 key 名稱與是否存在。需要用到時 `set -a; source .env; set +a` 載入，過程不印出值。
- 祕密不進 log、例外訊息、fixture、commit、artifact；API key 放 URL params 時，例外訊息要改寫（`raise_for_status()` 的訊息含完整 URL）。
- token 一律從環境變數讀，沒設定就直接報錯；不寫死在程式（OSRM token 曾寫死在公開 repo，已更換）。
- 排查認證問題時本地重放請求，只印狀態碼與 error body（[pitfall 2026-08-30](../../.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md)）。
- 本 repo 是公開的：文件不寫金鑰值、連線字串、VM 位址與登入帳號、個人 email；可以寫環境變數名稱。

### 5.4 故障排除速查

| 症狀 | 先看 |
|---|---|
| 日報「RPC 撈不到資料」 | `public.health_snapshot` 是否存在（mig 306；舊的 `live.health_snapshot` 已棄用）；`realtime_tables.yaml` 是否為空 |
| 日報「尚未收到任何 VM snapshot」 | VM 上手動跑 `health_report.py`；S3 `_external_vm_health/<host>/` 當天檔案 |
| 異常趨勢一直報同一筆 | `data/anomaly_state.json` 的 `first_seen`；要重置就刪檔 |
| 新 collector 日報漏報 | 多半是 `cross_layer_map.yaml`／`realtime_tables.yaml` 沒加 |
| 大量 `canceling statement due to statement timeout` | 這是壅塞不是斷線：查 `pg_stat_activity` 與 `cron.job_run_details`，不要自動重啟（[principles M-07](principles.md)） |
| 隨機出現 `read-only transaction` 等「時好時壞」錯誤 | 連線池被 session 設定污染（[pitfall 2026-07-24](../../.claude/pitfalls/2026-07-24-supavisor-session-set-poisoning.md)） |
| 多支 collector 同時 401／429 | 共用金鑰問題，見 [pitfall 2026-08-30](../../.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md) |

### 5.5 重新部署日報（災難復原）

- 主容器需要的環境變數（只列名稱）：`DAILY_REPORT_ENABLED`、`DAILY_REPORT_TIME`、`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`、`SUPABASE_DB_URL`、`S3_BUCKET`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`、`S3_REGION`。`DAILY_REPORT_TIME` 建議晚於 VM 與本機稽核推送的時間。
- DB 端需要 gis-platform `migrations/306_health_snapshot_public.sql`（`149_realtime_health_snapshot.sql` 是舊版，不必再套）。
- VM 整台重建：依序照 `external/ship_ais_vm/README.md`、`external/waste_positions_vm/README.md`、`external/_shared/README.md` 的部署章節。
- 本機稽核：以 `scripts/com.gis.local_audit.plist` 範本建立 launchd 設定，金鑰放本機設定檔並 `chmod 600`。

## 6. 事故時第一步：切斷最大的來源

DB 是收資料、重算、供網站讀的共用資源，一個來源吃光 IO 會讓其他全部變慢，重算重疊、清理延後，形成正回饋（[principles L-12](principles.md)）。

1. **先止血**：找出最大的來源並切斷，再查細節。2026-10-12 海象寫入卡死時，經使用者同意後 `pg_terminate_backend` 終止該 collector 的連線、把 `{PREFIX}_ENABLED` 設 false 並重啟；原始資料仍在本地 JSON，照常歸檔 S3，不會遺失（[調查](../investigations/2026-10-12-marine-write-stall.md)）。
2. **保留證據**：在統計歸零前 dump `pg_stat_io`、`pg_stat_statements`、`pg_stat_activity`、`cron.job_run_details`。
3. **區分壅塞與斷線**：QueryCanceled 是壅塞的症狀；buffer 會接住寫不進去的資料，不要為了「讓它動起來」重啟。
4. **唯讀調查**：明列禁止的操作；子代理在收到停止指令前多跑的查詢也會污染證據。
5. 事後寫長文到 `.claude/pitfalls/`，在 [pitfalls.md](pitfalls.md) 加一行。

終止連線、停用 collector、改設定都是正式環境操作，要使用者同意。

## 7. DB 變更：逐項套用與健康檢查

正式 DB 的 migration、刪索引、VACUUM FULL、cron 調整，照 Disk IO 第二、三輪的做法（gis-platform `docs/research/2026-10-12-disk-io-round2.md` §5、`2026-10-16-disk-io-round3.md` §4）：

1. **先唯讀查證、排名**：所有候選（含 collector 程式修正）一起查證，產出「哪些措施有效」的排名，再挑要做的；推估寫明是估計值、不可相加。
2. **使用者拍板**後才套用；PR 先合併、CI 全綠。
3. **時間**：避開台灣 10:00–20:00；VM collector 可挑 quiet hours。
4. **順序**：collector 端（例：冪等寫入、沒變不寫）先部署並確認生效，DB 端（唯一索引、刪索引）後動。
5. **逐項套用**：一次一支 migration，`psql -1 -X -v ON_ERROR_STOP=1`，需要鎖的設 `lock_timeout`；大索引用 `CONCURRENTLY`。每項套完立即驗證（函式定義、`indisvalid`、HOT 比例、表大小）。
6. **改 cron 前做 canary**：新函式先手動跑，與 oracle 逐列比對 0 缺 0 多，再切 job。
7. **健康檢查**：套用期間與收尾看 `cron.job_run_details` 全部 succeeded、collector 異常清單只有套用前就存在的項目。
8. **留文件**：在 gis-platform `docs/research/` 寫報告（背景、量測口徑、做了什麼、套用紀錄、待追蹤），collector 相關事故在本 repo pitfall 補一段並連結。
9. **約一週後同口徑複量**；DB 若重啟要換窗口。

## 相關文件

- [principles.md](principles.md) H、M、D 類
- [architecture.md](architecture.md)：心跳、buffer、watchdog 的程式行為
- [pitfalls.md](pitfalls.md)：事故索引
- [EXTERNAL_COLLECTORS.md](../EXTERNAL_COLLECTORS.md)：VM 正本
- [AWS_INVENTORY.md](../AWS_INVENTORY.md)：S3 現況
- `external/_shared/README.md`：VM health report 部署
