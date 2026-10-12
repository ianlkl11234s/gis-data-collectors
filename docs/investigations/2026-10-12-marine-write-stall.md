# 2026-10-12 海象 collector 寫入卡死事件

## 經過（台灣時間）
- 約 11:20 起，`cwa_marine_observation` 一筆寫入 `live.marine_observation_readings` 的交易跑了約 2 小時（IO／DataFileRead），之後每 15 分鐘一輪寫 `reference.marine_observation_stations` 的交易都在等它的列鎖，疊了 6 筆，最久約 4 小時，佔滿 Supabase 連線池（session 模式 max clients 15），公車、USWG、jma_raster 等出現寫入逾時。
- 15:40 使用者同意方案 A：`pg_terminate_backend` 終止 7 個 marine 連線；Zeabur `CWA_MARINE_OBSERVATION_ENABLED=false` 並重啟。原始資料在本機 JSON，照常由 ArchiveTask 歸檔 S3。
- 終止後全庫仍普遍 IO DataFileRead（autovacuum、trails_daily refresh 同時進行）。

## 根因（唯讀調查）
1. **每輪重寫 48 小時**：O-B0075 是 rolling 48h，`collectors/cwa_marine_observation.py:53-79` 每 15 分把整包轉長表全部 upsert（`storage/supabase_writer.py:2556-2563`），推估每輪約 2.8 萬列，新資料只約 1/48。`DO UPDATE` 無 WHERE，且 `payload_sha256`（整包 hash，`marine_observation.py:131`）與 `collected_at` 每輪不同，每列必改寫。DB 實測 n_tup_upd 9,106,498 對 n_tup_ins 43,424（約 210:1），n_dead_tup 356,705。
2. **非 HOT 更新寫 6 個索引**：表 443 MB、總 1,486 MB；GIST `idx_marine_observation_readings_geom` 611 MB 且 idx_scan＝0；`station_metric_time` 133 MB、`network_time` 49 MB 也是 0 次使用。
3. **鎖鏈**：同一交易先 upsert stations（每輪 116 站全更新＋trigger）再寫 readings；readings 卡住時持有 stations 列鎖，後續每輪排隊。交易無 statement_timeout／lock_timeout。
4. **isohe_port_marine**（HiCloud VM，每 10 分）也寫同組表，同樣全量 upsert 無 WHERE；station_uid 不同，不被 CWA 列鎖擋，但一起推高 IO。
5. **schema**（gis-platform mig 378）：readings 無分區，PK (station_uid, observed_at, metric_code, depth_key)＋5 索引＋FK，保留 90 天；`analytics.marine_observation_hourly` 374 MB、從未 autovacuum/analyze，每小時 :15 重算近 4 小時、無 WHERE upsert。
6. **排程撞期**：CWA 每 15 分 vs cron :15（marine hourly、temperature）與 :14（ship trails）；trails_daily 先刪再插，autovacuum 266–320 次。
- 未取得：pg_stat_statements 前 10 名（查詢逾時）。

## 建議修法（待使用者拍板）
- Collector：只送「DB 最新時間 − 3 小時」之後的資料，或 `DO UPDATE … WHERE (value_raw, is_missing, is_valid) IS DISTINCT FROM (EXCLUDED…)`；stations 只在站表 hash 改變時寫、獨立短交易；交易加 `SET LOCAL statement_timeout='120s'`、`lock_timeout='10s'`；isohe 同步修。預期 readings 寫入量 −95% 以上。
- DB（migration）：刪 0 次使用的 GIST 索引（−611 MB），確認另兩個 0 次索引是否被 RPC 使用；寫入降下後 `REINDEX CONCURRENTLY`、hourly 表 `VACUUM ANALYZE`；hourly upsert 加 IS DISTINCT FROM。
- 排程：collector 改 :07/:22/:37/:52；`refresh-marine-observation-hourly` 移到 :40。
- 修完觀察一週，若全庫 DataFileRead 仍高，再評估 Supabase compute 等級。

## 修正（2026-10-12，分支 data-collectors `fix/marine-write-path`、gis-platform `fix/marine-indexes`；未 push、未 apply）

### Collector（Zeabur `cwa_marine_observation` 與 VM `isohe_port_marine` 共用）
- 寫入契約集中在 `storage/marine_write.py`，VM 用逐位元相同的副本 `external/isohe_port_marine_vm/marine_write.py`（測試強制一致）。
- readings：`DO UPDATE … WHERE t.collected_at <= EXCLUDED.collected_at AND (value_raw, value_numeric, unit_source, unit_canonical, vertical_datum, is_missing, is_valid, missing_reason, source_status, quality_flags, geom_at_observation) IS DISTINCT FROM (EXCLUDED…)`；payload_sha256、collected_at 不參與比對。current 同樣加 IS DISTINCT FROM 與 collected_at guard。
- 增量：同一交易內先讀 `live.marine_observation_current` 每個 (station, metric, depth) 的最新有效 observed_at，只送「最新 − 3 小時」之後的列（`MARINE_WRITE_LOOKBACK_HOURS`，預設 3）。current 只放有效值，所以 cutoff 只會偏早（多送不會漏送）；不在 current 的 key、無法解析或無時區的時間一律照送。
- stations：站表內容 hash（排除 first/last_seen_at）有變，或距上次寫入滿 6 小時（讓 last_seen_at 前進、Pulse popup 有用）才寫，獨立短交易，先 commit 再寫 readings；`WHERE t.last_seen_at <= EXCLUDED.last_seen_at` 擋舊站表回放。Zeabur 狀態存在 writer 記憶體，VM 存 `$DATA_DIR/state/station_roster.json`。
- 每個交易開頭 `set_config('statement_timeout','120000',true)`、`set_config('lock_timeout','10000',true)`。
- 排程：BaseCollector 新增 `fixed_minutes`；CWA 固定 :07/:22/:37/:52（`CWA_MARINE_OBSERVATION_MINUTES`），不套 jitter。VM cron 改 `8-59/10`（:08/:18/…/:58），外加 `timeout -k 30 540`。
- VM：`SIGALRM` 480 秒（預設處置，阻塞在 libpq 時也會被 kernel 終止；Python handler 不會觸發，這正是 3.6 天卡住的原因）；`$DATA_DIR/state/collect.lock` 非阻塞鎖，前一輪還在就拒跑並回 1。
- vm_buffer replay 檢查：原本會用舊值蓋新值（`flush_pending` 先補舊批，再寫本輪；main repo buffer 同理），current 的 trigger 在 observed_at 相同時只看 received_at=now()，也擋不住。現在 readings／current 都有 collected_at guard，且舊批的列多半落在 cutoff 之前根本不送。
- 測試：`tests/test_marine_observation.py`（SQL 條件、增量過濾、站表 hash、timeouts、固定分鐘）、`tests/test_isohe_port_marine_vm.py`（VM 契約、state 檔、buffer replay、run lock、SIGALRM、cron 分鐘）；全套 729 passed、2 skipped。

### DB（gis-platform migration 441，未 apply）
- DROP `idx_marine_observation_readings_geom`（617 MB）、`idx_marine_observation_readings_network_time`（49 MB）：兩者 idx_scan＝0，grep gis-platform、mini-taiwan-pulse、mini-pulse-gis-mcp、data-collectors 沒有任何查詢用到。
- 保留 `idx_marine_observation_readings_station_metric_time`（134 MB，0 次）：條件正好對應 `get_marine_observation_history`（Pulse popup 歷史），目前由 PK 服務；寫入降下後再評估。
- `analytics.refresh_marine_observation_hourly` upsert 加 IS DISTINCT FROM（函式其餘與 378／live 定義一致）；cron `refresh-marine-observation-hourly` :15 → :40（alter_job 只改 schedule，active 不動）。
- `SET LOCAL lock_timeout='5s'`；不含 VACUUM FULL／REINDEX。
- 預演（BEGIN…以 RAISE 中止，16:42 CST）：索引剩 pkey／station_metric_time／time／valid_time，cron `40 * * * * active=true`；同交易內 refresh 第一次 1,261 列（0.14 秒）、第二次 0 列（0.04 秒）。事後確認 DB 無殘留（6 索引、cron 仍 :15、函式未變）。

### 預期寫入量（以 2026-10-12 DB 實際列數試算）
| | 改前每輪送出 | 改後每輪送出 | 實際寫入（tuple） |
|---|---|---|---|
| CWA（15 分） | 28,080（48h × 84 站） | 約 2,340（−92%） | 約 585 列／小時（只有新的一小時），改前約 11.2 萬列／小時 → −99.5% |
| ISOHE（10 分） | 27,048（72h × 32 站） | 約 1,208（−95.5%） | 約 376 列／小時，改前約 16.2 萬列／小時 → −99.8% |
| stations | 116 站 × 每輪 | 0（hash 不變）；每 6 小時 116 | |
| hourly refresh | 4 小時窗全部重寫 | 只寫有變的 bucket | 預演第二次 0 列 |

### 待確認
- Zeabur `_txn` 原本就會 `SET LOCAL statement_timeout = SUPABASE_STATEMENT_TIMEOUT_MS`（預設 30 秒），但事件中有 2 小時的交易與 4 小時的鎖等待，表示正式環境的 env 可能覆寫成 0 或很大的值；請主 agent 查 Zeabur `SUPABASE_STATEMENT_TIMEOUT_MS`。海象路徑現在固定 120 秒、lock 10 秒，不受它影響。
- 比最新觀測晚 3 小時以上的上游修正不再寫入 DB（raw archive 仍有）。
- 啟動時 scheduler 仍會立即跑一次（可能落在 :14／:15），之後才對齊固定分鐘。
