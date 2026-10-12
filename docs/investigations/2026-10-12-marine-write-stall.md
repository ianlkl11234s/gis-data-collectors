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
