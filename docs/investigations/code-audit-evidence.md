# Collector health code audit evidence

範圍：DIO-4/5/6、WA-2、BL-7 的靜態程式碼與 manifest 檢查。此審查未直接呼叫正式 API 或 DB；下列根因主要為原始碼可證實的行為。既有 `docs/investigations/evidence/owner-join.json` 的同時刻 live owner join 由主調查提供，未在此重跑。

## 可行動問題

### P1 — daily reservoir collector 可在上游未更新時把空/非 list 回應當作成功

- 證據：`collectors/water_reservoir_daily_ops.py:67-71` 將非 `list` JSON 直接轉為 `[]`；`:97-108` 無論 rows 是否為空都回傳含 `data` 的成功結果。`collectors/base.py:90-133` 對任何無 `_collector_error` 的結果更新 `last_success_at`，而 `storage/supabase_writer.py:185-189` 對空 records 直接回傳成功。
- 影響：WRA 回應 schema 改變、認證/錯誤 JSON 但 HTTP 為 2xx，或產生空清單時，collector heartbeats 仍是成功；`live.reservoir_daily_ops` 的 `observed_at` 不前進。每日資料本身不應被補成零值，但目前沒有區分「合法零水庫」與「無法解析」。
- 建議：在 collector 保留 `source_count` 與 parse-status；僅在來源明確宣告空資料為合法時成功。否則回傳 `_collector_error` 或可查的 run receipt。不要為了 heartbeat 捏造 row。
- 狀態界線：提供的 live evidence 顯示 `water_reservoir_daily_ops` 近期已有新 `observed_at`，所以此項是可重現的失真根因，不能宣稱目前 BL-7 仍停滯。

### P1 — drought HTTP 200 的無效/空頁會被記為成功，且資料面告警最慢 90 天才出現

- 證據：`collectors/wra_drought_alert.py:79-113` 允許無 `updatetime`、無 `info-list` 或無可辨識燈號，仍產出 hash；`:191-203` 只有 alerts 才產 records；`:216-222` 仍回傳正常 result。`collectors/base.py:90-133` 因而更新 success。`storage/supabase_writer.py:185-189` 對空 records 視為成功。
- 監控鏈：`config/realtime_tables.yaml:120-123` 只以 `drought_alert_current.fetched_at` 監控，interval=43200 分；`tasks/monitoring.py:201-207` 在 3 倍才 STALE（90 天）、12 倍才 DEAD（360 天）。`tasks/daily_report.py:725-751` 對低頻資料也只容許 `max_time < 3x interval`；D1/D3/D7 僅在異常已被建立後才重送（`tasks/monitoring.py:492-503`、`tasks/daily_report.py:875-943`）。
- 影響：現有 current row 在來源頁被 WAF/error HTML/結構改版後，collector 的 in-memory 成功與跨層 B 皆可能持續為綠，直到舊 `fetched_at` 超過 90 天。這不是把未發布誤成藍燈的建議；應保留「未變」與「不可解析/未驗證」兩種狀態。
- 建議：新增每輪可寫入的 source-run/heartbeat（含 parse validity、alert count、source hash）；將內容新鮮度與輪詢健康分開。無日期或無 alerts 的 HTML 應為顯式 invalid/unknown，不能更新成功心跳。
- 提供的 parser evidence 顯示該頁可解析到 `published_date=2026-05-26`、`alerts=[]` 與新 hash；因此此路徑已實際造成「零 records → writer early return → current 留舊值」。但僅憑 HTML 不能判定這代表真實全藍燈還是 selector/上游版型改變；兩者不可混同。若產品定義全藍燈為合法結果，需另以有審核的 DB mutation 清理/取代 current，不能在本次唯讀審查假設成功。

### P2 — A1 累積來源以新事故列作健康心跳，會把合法無新事故誤報為 collector 停止

- 證據：A1 是年度累積 snapshot，且 dedup 僅寫新 hash（`collectors/npa_traffic_accident_a1.py:7-14`、`:142-153`；`storage/supabase_tables.py:342-354` 的 `ON CONFLICT DO NOTHING`）。realtime manifest 卻用新列的 `collected_at` 作 720 分鐘心跳（`config/realtime_tables.yaml:181`），cross-layer 也以同表為 B 層（`config/cross_layer_map.yaml:836-844`）。
- 門檻：`tasks/monitoring.py:201-207` 於 36 小時 STALE、144 小時 DEAD；cross-layer 對低頻資料在 3×720 分鐘（36 小時）後 B=false（`tasks/daily_report.py:735-751`）。
- 影響：A1 沒有新事故但 collector 正常抓到同一年度快照時，DB 不增加列，約 36 小時後會被診斷為跨層斷層。反之，這個表也不能證明 collector 真正輪詢成功，只能證明曾出現新事故。
- 建議：新增/使用每輪 run receipt（source `資料提供日期`、row_count、parse/error 狀態與 fetched_at）；將它作 polling health，事故表維持事件資料與其真實時間語意。`geom` 目前對越界座標會保留事故但設為 `NULL`（`collectors/npa_traffic_accident_a1.py:112-118`），此審查未見應改成假座標的問題。

### P2 — daily reservoir 的來源發布時間未被排程約束；archive 名稱是擷取日期而非 source observed date

- 證據：來源宣告每日 09:30 前更新（`collectors/water_reservoir_daily_ops.py:4-9`，`config.py:322`），但 `main.py:104-110` 於服務啟動即跑，後續按 interval，而非固定在 09:30 後；`scheduler.py:10-14` 說明為 `every(N).minutes`。本地檔案目錄使用 collector run timestamp（`storage/local.py:39-49`），ArchiveTask 依該目錄日期產生 S3 key（`tasks/archive.py:87-114`、`:185-189`），全域 archive clock 預設 03:00（`config.py:115-118`）。
- 影響：若服務相位落在 09:30 前，該 run 可讀到上一版資料；archive `YYYY-MM-DD` 表示本地擷取日，不能解讀成 `observed_at` 的來源日。資料庫仍保存 `observed_at`（`collectors/water_reservoir_daily_ops.py:77-95`；`gis-platform/migrations/051_reservoir_daily_ops.sql:18-34`），因此不應把 archive key 當資料日期。
- 建議：主 agent 決定是改成 09:30 後固定時間排程，或明確將 archive directory/key 文件化為 capture-date；兩者皆須保留 source `observed_at`，不可用 archive date 覆寫它。

## DIO-4 接線與 manifest 對照

指定三條寫入路徑均完整且表名一致：

| owner | registry | TABLE_MAP / transformer | cross-layer / realtime |
|---|---|---|---|
| `water_reservoir_daily_ops` | `collectors/registry.py:165` | `storage/supabase_tables.py:421-433`; `storage/supabase_writer.py:1414-1416,2213-2217` | `config/cross_layer_map.yaml:504-512`; `config/realtime_tables.yaml:112-114` |
| `wra_drought_alert` | `collectors/registry.py:166` | `storage/supabase_tables.py:434-446`; `storage/supabase_writer.py:1418-1423,2277-2279` | `config/cross_layer_map.yaml:541-552`; `config/realtime_tables.yaml:120-123` |
| `npa_traffic_accident_a1` | `collectors/registry.py:94,202` | `storage/supabase_tables.py:342-355`; `storage/supabase_writer.py:1770-1778,2268-2270` | `config/cross_layer_map.yaml:836-844`; `config/realtime_tables.yaml:181` |

機械 YAML/AST 對照：109 realtime entries、71 unique `owner_collector`；所有 owner 除一項都存在於 `cross_layer_map.yaml` 和 config toggle 集合。例外是 `air_ticket_radar → air_tickets.fare_offers`（`config/realtime_tables.yaml:75`）：沒有同名 registry/config toggle/cross-layer owner。本 repo 對其只有監控 snapshot 旁路（`scripts/gis_collectors_monitor_snapshot.py:172-192`）。現有 `tests/test_cross_layer_sync.py:138-166` 只驗證「enabled cross-layer 表被 realtime 清冊涵蓋」，所以不會驗出反向 owner orphan。

這是跨模組 ownership 決策：若 air-ticket scheduler 不屬本 repo，應以能表達 external owner 的 manifest 契約取代虛構 collector 名稱；若屬本 repo，補 registry/toggle/cross-layer 與 run health。不能從本次靜態碼推斷哪個系統實際負責，交由主 agent 以 runtime/部署證據裁定。

主調查提供的同一時刻 owner freshness join 結果為 **62 FRESH / 5 STALE / 4 MISSING**。下列是 9 個確實未滿足 generic heartbeat contract 的 owner；這不等於 9 個已證實 runtime failure，因為其中部分使用 VM/direct writer，未寫 generic status。

| state | owner | manifest tables | 原始碼解讀 |
|---|---|---|
| STALE | `ship_ais` | `live.ship_positions`, `live.ship_current` | manifest 指定 HiCloud VM、10 分鐘及 archive lag 8 天（`config/cross_layer_map.yaml:119-127`）；VM 用 psycopg 直接 INSERT/UPSERT 兩表（`external/ship_ais_vm/ship_ais_collect.py:142-193`），未經 generic collector writer，因此舊 status 無法判定 VM 是否停機。 |
| STALE | `earthquake_shakemap_grid` | `live.earthquake_shakemap_grid` | 事件型資料；generic heartbeat 已 stale，但需以震災事件與 source/run receipt 判斷是否漏跑，不能從靜態碼判停。 |
| STALE | `wra_drought_alert` | `public.drought_alert_current` | P1 空結果成功/舊 current 路徑，見上方 drought 根因。 |
| STALE | `waste_positions` | `spatial.waste_positions_realtime` | manifest 指定 HiCloud VM（`config/cross_layer_map.yaml:593-601`）；VM collector 自行 INSERT spatial history（`external/waste_positions_vm/waste_positions_collect.py:233-261`），不經 generic status。 |
| STALE | `cdc_public_health_weekly` | `live.public_health_weekly` | manifest 指定 HiCloud、週四 11:00、無 S3 archive（`config/cross_layer_map.yaml:603-610`）；VM direct UPSERT（`external/cdc_public_health_weekly_vm/cdc_public_health_weekly_collect.py:206-250`）繞過 generic heartbeat。 |
| MISSING | `aisstream` | `live.aisstream_ingest_runs`, `live.aisstream_archive_manifests`, `live.aisstream_position_observations`, `live.aisstream_vessel_current`, `live.aisstream_ingest_health` | persistent registry entry 與專用 health 表存在，但 generic status owner 缺失；需確認專用 health 是否為正式契約。 |
| MISSING | `gfw_vessel_presence` | `live.gfw_vessel_presence_runs`, `live.gfw_vessel_presence_archive_manifests`, `live.gfw_vessel_presence_snapshots`, `live.gfw_vessel_presence_current` | manifest 標示 legacy/disabled；generic status 缺失可能是設計意圖，仍需 runtime/deployment 證據。 |
| MISSING | `flight_fr24` | `live.flight_positions` | generic status owner 缺失；未有足夠靜態證據判定 collector 是否部署或停止。 |
| MISSING | `air_ticket_radar` | `air_tickets.fare_offers` | 完整靜態 orphan：無 registry/config toggle/cross-layer owner，見上方機械對照。 |

其中 owner contract 的四個 MISSING 是 `aisstream`、`gfw_vessel_presence`、`flight_fr24`、`air_ticket_radar`：前 3 個均有 registry entry（`collectors/registry.py:134-136`）但不在 generic collector-status；最後一個連 registry/toggle/cross-layer 都沒有。VM/direct owners 也應明確標示 external/persistent，或由 VM 寫入相容 run health；採用何種合約為跨模組決策。

## 檢查與未知

- `pytest -q tests/test_cross_layer_sync.py`：6 passed（僅靜態同步守門）。
- `git diff --check`：通過；工作樹已有其他人未提交變更，未修改它們。
- 未直接進行 DB/API、S3、部署或 Telegram 驗證；未宣稱本 agent 驗證了 production freshness、coverage 或 alert delivery。上表 runtime state 來自同時刻 `owner-join.json`，仍須由主 agent 以 live/deployment 證據裁定。
