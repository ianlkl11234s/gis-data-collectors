# 資料生命週期

一句話：資料先熱後冷——Supabase 只放網站和分析要用的近期資料，本地 JSON 每天打包上 S3 當永久冷備份，網站要大量讀的檔案另外發布到 R2／CDN；保留期一律以 `metadata.retention_policies` 和 migration 為準，文件不抄天數。

## 1. 四層總覽

| 層 | 放在哪 | 放什麼 | 誰寫 | 保留期去哪查 |
|---|---|---|---|---|
| 熱 | Supabase `live.*` 的 current 表 | 每個物件的最新狀態（一車一列、一站一列） | SupabaseWriter UPSERT | 多數不過期，靠 `current_prune_by` 或熱冷分離清掉久未出現的物件（見 §5） |
| 熱 | Supabase `live.*` 的 history／分區表 | 近期時序（軌跡點、觀測值） | SupabaseWriter INSERT | `metadata.retention_policies`；分區表由 `cleanup-expired-partitions` 清 |
| 熱 | Supabase trails／daily 預聚合表 | 網站回放用的每日軌跡與統計 | gis-platform pg_cron refresh | 各 cleanup 函式的天數參數（對應 migration） |
| 熱 | Supabase `reference.*`／`spatial.*` 名冊 | 站點、路段、船舶最後出現時間等 | collector 或 cron | 通常不過期 |
| 溫 | 容器 Volume `/data/<collector>/YYYY/MM/DD/*.json` | 每輪原始結果 | `BaseCollector.run()` | `ARCHIVE_RETENTION_DAYS`（預設 7）與 `{NAME}_ARCHIVE_RETENTION_DAYS` 覆寫，見 `config.py` |
| 冷 | S3 `<collector>/archives/YYYY-MM-DD.tar.gz` | 每日一個 tar | `tasks/archive.py` | **不過期**；儲存等級轉換見 [AWS_INVENTORY.md](../AWS_INVENTORY.md) §2 |
| 冷 | S3 `supabase-snapshots/` | DB 表的備份 | `tasks/backup_supabase.py` | [AWS_INVENTORY.md](../AWS_INVENTORY.md) |
| 發布 | Cloudflare R2（CDN） | 影像幀、發布用 JSON | 各 collector 雙寫、publish task | R2 bucket lifecycle（在 Cloudflare 設定）＋ manifest RPC 的回看窗 |

S3 是冷層也是**最後的副本**：Supabase 寫入失敗時本地 → S3 流程照常，所以 DB 資料出錯都能從 S3 回補（2026-04-07 時區事故就是這樣補回 30 天，[pitfall](../../.claude/pitfalls/2026-04-07-timezone-bug.md)）。回補前注意各 collector 的 archive JSON 時間欄格式不一致（有的 naive 台灣時間、有的 naive UTC），見 `scripts/backfill_ship_flight.py`。

## 2. 保留期去哪查

**不在任何文件抄天數。** 天數會變，文件抄了就會互相矛盾（盤點時發現 iot_wra、jma_amedas、R2 影像都有文件與 migration 不一致）。

| 想知道 | 查這裡 |
|---|---|
| 某張 DB 表留幾天 | `SELECT * FROM metadata.retention_policies WHERE …`（唯讀，帶 LIMIT）；或 gis-platform `migrations/` 裡登記該表的那支 |
| 註冊表本身怎麼運作 | gis-platform `migrations/282_retention_registry_and_coverage.sql` 檔頭 |
| 補登的表、天數怎麼決定 | gis-platform `migrations/385_live_retention_unregistered_tables.sql`；mini-taiwan-pulse `docs/proposal/supabase-retention-2026-09-01/README.md` |
| 刻意永久保留的表 | `retention_days IS NULL` 的列；gis-platform `migrations/309…`、`352…`、`412_lifecycle_retention_safety.sql` |
| 本地 JSON 留幾天 | `config.py` `ARCHIVE_RETENTION_DAYS`、`COLLECTOR_RETENTION_OVERRIDES` |
| S3 何時轉 IA／GIR／Deep Archive | [AWS_INVENTORY.md](../AWS_INVENTORY.md) §2（S3 lifecycle 現況的正本） |
| 氣象網格各層（T0–T3）留多久 | .gis-agent-system `decisions/0021-weather-data-collection-and-tiered-retention.md` |
| R2 發布幀留多久 | R2 lifecycle 在 Cloudflare 設定；網站實際會拿到哪些幀由 gis-platform `migrations/462_weather_imagery_manifest_r2_retention.sql` 的 manifest 回看窗決定。兩者的關係見 §7 |

新增時序表的規則（[principles R-01、R-02、R-03](principles.md)）：

1. 在 `metadata.retention_policies` **INSERT 一列**，不要改 `cleanup_expired_partitions()` 或任何清理函式的內容。舊做法是函式內的硬編碼清單，220–222 各自覆寫同一函式時把別人的表踢掉，4 張表寫入失敗；282 起改成註冊表。
2. 天數下限＝前端或 RPC 實際回看的窗口，再加 margin。
3. 決定永久保留就明寫 `retention_days = NULL`，和「忘了登記」區分開。
4. 上線後不要只看 `metadata.check_retention_coverage()` 回報 0 缺口（它有盲區，[principles R-10](principles.md)），自己確認註冊表裡有這張表。

## 3. 過期怎麼清：DROP PARTITION 還是 DELETE

| 方式 | 適用 | 機制 | 代價 |
|---|---|---|---|
| DROP PARTITION | 依時間分區的表 | `cleanup_expired_partitions()` 讀註冊表，掃**所有** ≤ cutoff 的分區一次刪掉（282 前只刪「剛好等於今天減天數」那一天，漏跑就永遠掃不回） | 幾乎零成本，空間直接還給 OS |
| 批次 DELETE | 非分區表 | `live.cleanup_rows_batched`：每批 5 萬列、批間 COMMIT，可加額外守衛條件（例：`image_key IS NOT NULL`） | 產生 dead tuple 與 autovacuum 負擔；**磁碟不會縮**，要 VACUUM FULL（[principles R-09](principles.md)） |

設計新表時，量大且以時間過期的資料優先做成分區表。分區命名用台北日、邊界用 UTC，回補前先建前一個 UTC 日的分區（[principles W-03](principles.md)）。清理 cron 的時間要避開 S3 歸檔與其他清理；實際排程見各 migration。

## 4. 衍生表：全量 refresh 還是增量

網站讀的 trails／daily 表是 pg_cron 從 history 算出來的。原本每次 DELETE 當天全部再重算，資料越到晚上越大，單次執行逼近排程間隔（ship 最慢 1,410 秒、間隔 30 分鐘）。

增量骨架（[principles L-06](principles.md)）：

- **水位表**：記上次處理到的 `collected_at`，只接新資料。
- **晚到批偵測**：發現比水位更早的資料就退回全量。
- **夜間全量基準**：昨天的資料每天全量重算一次，當作校正（也符合「昨天的資料一天算一次」，[principles L-07](principles.md)）。
- **上線前 oracle 比對**：在 `BEGIN … ROLLBACK` 裡用全量函式重算，與增量結果 `EXCEPT ALL` 雙向逐列比對，0 缺 0 多才切換。

| 表 | migration（gis-platform） | 結果 |
|---|---|---|
| road_congestion_daily | `402_road_congestion_daily_incremental.sql` | 第一支增量化（2026-09-04 事故後續） |
| ship_trails_daily | `442_ship_trails_daily_incremental.sql` | 平均 243 秒 → 3.1 秒 |
| bus_trails_daily | `443_bus_trails_daily_incremental.sql` | 平均 122 秒 → 2.5 秒 |
| satellite_maneuvers | `457_satellite_maneuvers_incremental.sql` | MV 改增量表＋每日全量校正；排程改每小時（理由見 [principles S-01](principles.md)） |
| bus_intercity_trails_daily | `459_bus_intercity_trails_daily_incremental.sql` | 增量與全量逐列 md5 相同後切換 |

過程與數字：gis-platform `docs/research/2026-10-12-disk-io-round2.md` §4–§5、`docs/research/2026-10-16-disk-io-round3.md` §4。

## 5. 熱冷分離：current 表不要永遠長大

current 表一個物件一列，物件只增不減時表會一直變大，每次 UPSERT 的代價也跟著變大。

- `ship_current` 曾有 22.7 萬艘，其中 79% 超過 7 天沒出現。gis-platform `migrations/460_ship_last_seen_registry.sql` 建名冊 `live.ship_last_seen`（記每艘船最後出現時間，永久），再每天清掉 current 裡 7 天未出現的船。刪除不可逆，但船再出現時 collector 會自然寫回。名冊本身每日整表備份到 S3（`config/backup_manifest.yaml` 的 `cadence: full_table`，見 §8）。
- `TABLE_MAP` 的 `current_prune_by` 是另一種做法：寫完 current 後刪掉本批沒出現的舊列（例：satellite 清幽靈列）。只適合「每輪都是完整快照」的來源。
- 新 collector 若寫 current 表，設計時就決定「多久沒出現算離開」與名冊放哪（見 [new-collector.md](new-collector.md)）。

## 6. 溫層 → 冷層：本地 JSON 與 S3 歸檔

`tasks/archive.py`（`ArchiveTask`，每日 `ARCHIVE_TIME`，預設 03:00 容器時間；`Dockerfile` 設 `TZ=Asia/Taipei`，所以是台北 03:00。2026-10-16 查 S3 `youbike`／`iot_wra`／`jma_amedas` 的 `archives/2026-10-15.tar.gz` LastModified 都是 19:02–19:12 UTC＝台北 03:02–03:12，確認正式環境沒有覆寫。gis-platform mig 282 檔頭寫的「03:00 UTC」是錯的）：

1. 逐 collector 掃 `YYYY/MM/DD` 目錄，跳過今天。
2. 每天的 JSON 壓成一個 `<collector>/archives/YYYY-MM-DD.tar.gz`（決定性 tar，串流寫到磁碟不佔記憶體），一次 PUT。每日一個 tar 是刻意的：物件數比容量貴（[principles R-07](principles.md)）。
3. 上傳後**全量讀回驗證**才寫 receipt，有 receipt 才刪本地（下節）。
4. 超過保留天數的日期目錄才刪。

S3 規則（正本 [AWS_INVENTORY.md](../AWS_INVENTORY.md)）：不設 expiration；每新增一個 collector 要補一條 `archive-deep-270d:{name}` 規則，而 `put-bucket-lifecycle-configuration` 是整份覆寫（[principles R-06](principles.md)）。

### 歸檔驗證（2026-09-18 起的長期結論）

原始紀錄：[lifecycle-hardening-20260918.md](../lifecycle-hardening-20260918.md)。

- 本地日期目錄只有在**遠端 tar 全量讀回**、逐檔比對檔名、SHA-256 與位元組數都相符，並以原子方式寫好本地 receipt（`.archive-receipts/<collector>/<date>.json`，記錄成員清單與遠端 identity）之後，才可以刪。讀回時鎖定 HEAD 回傳的 VersionId 或 ETag。
- 之後每次執行只比對「本地成員清單＋遠端 HEAD identity」和 receipt，不重壓、不重下載。
- 遠端缺檔：先壓到暫存檔、上傳、讀回驗證，再發 receipt。遠端內容不符或讀不到：保留本地，**絕不覆寫**遠端。
- 壓縮需要「來源大小＋1 MiB」的可用空間；清理不需要壓縮空間，所以磁碟滿時已驗證的目錄照樣能清。
- 目錄裡有不認得的檔、symlink 或子目錄就整個保留。今天的目錄與各 collector 既有的本地保留窗不變。
- receipt 是驗證證據，不是歸檔本身。雲端部署與實際的還原讀回是另外的驗收關卡。
- 日報加了共用檔案系統壓力檢查：80% 警告、90% 嚴重（另有既有的本地檔總量門檻 `DISK_ALERT_THRESHOLD_MB`）；查不到容量時明確報錯，不當成健康。

長駐 worker 有自己的冷層：AISStream 的原始事件每小時 gzip 上 S3，大小與 sha256 驗證通過才刪本地 spool（`workers/aisstream.py`）。

## 7. 發布層：R2／CDN

網站要大量讀、而且每個人讀的都一樣的資料，做成共用快照放 R2，不要每個訪客都打 DB。

- **影像雙寫**：`cwa_satellite`、`air_quality_imagery` 等影像 collector 除了寫 DB，另寫一份到 R2（`storage/r2.py`，key 由 `(dataset, observed_at)` 決定、永不變，所以設 immutable 長快取）。R2 憑證沒設時跳過上傳、`image_key` 為 NULL，DB 照寫。
- **影像清理守衛**：DB 影像表的清理條件一律加 `image_key IS NOT NULL`——沒上 R2 的幀永不刪，寧可留著也不丟（[principles R-04](principles.md)；gis-platform `migrations/282…`、`388_aqi_imagery_cleanup_cron.sql`）。
- **氣象網格發布幀**：`jma_raster` 等依 ADR-0021 分 T0–T3 四層，發布幀放 R2。R2 bucket `mini-tw-pulse` 的 lifecycle 規則 `jma-imagery-expire-*`（前綴 `imagery/jma/`；另有較長的 `imagery-expire-*` 涵蓋整個 `imagery/`，短的先生效）刪除物件；`get_weather_imagery_manifest` 只回傳 `published_at` 在回看窗內的幀（gis-platform mig 462），**回看窗刻意比 R2 規則少一天**當安全邊際，避免前端拿到已被 R2 刪掉的 key；更舊的幀從 S3 的 T2 重新發布（重發會更新 `published_at`，R2 物件也重新起算）。兩邊天數不抄在這裡：R2 用 `get_bucket_lifecycle_configuration`（唯讀）查，RPC 看 mig 462 的 `interval`。改任何一邊都要同時改另一邊，維持「RPC 窗 < R2 天數」。
- **凍結匯出**：DB 的 trails 是滾動視窗，`trails_export`（`scripts/export_daily_trails.py`，每日 `TRAILS_EXPORT_TIME` 預設 02:00 台北）把昨天凍結成 S3 靜態檔。資料在 D+1 01:00–01:20 才定版，太早會匯出半成品；漏跑不會自動補，超出 DB 保留窗就救不回（[principles R-08](principles.md)）。預設關閉，**只能開單一實例**（共用 manifest 是 get→merge→put，非原子）。

## 8. 備份：Supabase → S3

`tasks/backup_supabase.py`，設定在 `config/backup_manifest.yaml`，`BACKUP_ENABLED` 預設關閉。排程台北 `03:30`：`schedule` 套件用容器時間（`Dockerfile` `TZ=Asia/Taipei`）。2026-10-16 查 `metadata.backup_audit_log` 當天各機器人在台北 03:30–03:38 執行、`backup_state.last_backup_at` 近 7 天全落在台北 03 時；`main.py` 舊註解寫的 UTC 已更正。

| 機器人 | 做什麼 |
|---|---|
| A 靜態快照 | 逐表算指紋（列數＋最大 `updated_at`），有變才重新 COPY → gzip → S3 |
| B 即時快照 | 傾印昨天的即時表資料；已由 ArchiveTask 歸檔的表跳過（`skip_if_archive_py_covered`）。找不到時間欄的表會被跳過（`skipped_no_time_column`），隔天盤點報 `missing_s3`；這類表若要備份，在 manifest `overrides` 設 `cadence: full_table` 改成每日整表快照（例：`live.ship_last_seen` 名冊，2026-10-16 起） |
| C 盤點 | 比對資料庫、備份狀態與 S3 物件，列出缺漏、孤兒與新表 |

表的完整清冊與備份三層：gis-platform `docs/data-safety-inventory.md`、`docs/data-inventory.md`（手冊不重抄數量）。

## 相關文件

- [principles.md](principles.md) R 類：保留期與生命週期原則
- [architecture.md](architecture.md)：寫入層與 buffer
- [AWS_INVENTORY.md](../AWS_INVENTORY.md)：S3 bucket、lifecycle、prefix 現況
- gis-wiki `inbox/2026-10-15-gis-data-engineering-blind-spots.md`：熱溫冷封存、全量 vs 增量、讀取端共用快照的心智模型
