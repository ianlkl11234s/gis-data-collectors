# 海、空、太空：船舶 AIS、GFW、航班、衛星、火箭發射

**一句話**：這一類追的是「移動物件」，資料量大、更新快、常有多個重疊來源；最大的坑是存取模式：移動物件的 current 表每輪幾乎每列都改，熱表上的 GiST 索引和「沒變也寫」會直接變成 Disk IO 大戶，而且有些來源（航港局 AIS）只讓台灣 IP 連。

清單與寫入表以 [collectors.md](../collectors.md) 為準。上游 API 細節在 taipei-gis-analytics（private）`docs/api-platforms/` 的 `aisstream/`、`global_fishing_watch/`，這裡只連結。FR24、OpenSky、Space-Track、Launch Library 2 目前在 analytics 沒有對應頁。

## 共通機制

- **熱冷分離**：current 表要清掉久未出現的物件，不然會累積「幽靈」列（2026-10 時 `ship_current` 有 79% 的船超過 7 天沒出現）。船舶由 gis-platform migration 460 處理；衛星用 `TABLE_MAP` 的 `current_prune_by=collected_at` 在同一交易清掉本批沒出現的列。清理規則與天數以 gis-platform 為準，見 [data-lifecycle.md](../data-lifecycle.md)。
- **熱表索引是寫入稅**：HOT update 只在「被索引的欄位沒變」時發生。`ship_current` 的 geom 有索引，船一動 geom 就變，HOT 比例只有約 3.7%。新增移動物件表前先想清楚哪些索引真的有人用，見 [2026-09-04 Disk IO 膨脹](../../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) 與 gis-platform（private）`docs/research/2026-10-12-disk-io-round2.md`、`2026-10-16-disk-io-round3.md`。
- **trails 是衍生資料**：船、公車、航班的每日軌跡由 DB 端預聚合（`*_trails_daily`），2026-10-12 起改增量 refresh（ship 每次約 243 秒降到約 3 秒）。凍結到 S3 的匯出任務 `trails_export` 預設關閉，只能開單一實例。見 [data-lifecycle.md](../data-lifecycle.md)。

## 船舶

### ship_ais
航港局 AIS 船位（mpbais.motcmpb.gov.tw）。

- 頻率／位置：每 10 分鐘，**跑在 HiCloud VM**（只接受台灣 IP，雲端 IP 一律 ConnectTimeout）；Zeabur 端 `SHIP_AIS_ENABLED` 必須維持 false，主 repo 仍保留完整實作。VM 部署見 [EXTERNAL_COLLECTORS.md](../../EXTERNAL_COLLECTORS.md)（主機以 `<VM_HOST>` 表示），手動 scp 部署，git push 不會生效。critical。
- 寫入：`live.ship_positions`（history）、`live.ship_current`。
- 守門：VM 端有本地 buffer（`external/vm_common/vm_buffer.py`），DB 斷線時暫存。
- 已知坑：VM 寫入繞過主程式的 `SupabaseWriter`，2026-10-05 調查時心跳停在 06-06 但表裡有當天的資料，**這支的心跳不可信，要直接看表**（[collector-health 調查](../../investigations/2026-10-05-collector-health.md)）。`ship_current` 是熱表膨脹的主角之一（見共通機制）。
- 上游細節：analytics `api-platforms/` 目前沒有航港局 AIS 的專頁。

### ship_tdx
TDX 國內航線船位。已被 `ship_ais`（覆蓋更廣）取代，清冊標停用，不寫 DB。

### aisstream
AISStream WebSocket（獨立 provider，五個區域 bbox）。

- 頻率／位置：**長駐 worker**（`workers/aisstream.py`；registry 裡的 class 只是 shim，不走 interval scheduler），清冊標停用（repo 預設關）。需 `AISSTREAM_API_KEY`。
- 寫入：`live.aisstream_*` 五張表（ingest runs、archive manifests、position observations、vessel current、ingest health），不寫既有 `live.ship_*`。raw 以 gzip NDJSON＋manifest 永久存 S3 `aisstream/raw/v1/`，上傳並 HEAD 驗證後才清本地 spool，不設 S3 到期刪除。
- 守門：DB run／health ledger 與 S3 冷儲存是**啟動硬條件**，preflight 失敗就讓容器啟動失敗，不會出現「容器活著、worker 靜默死掉」；socket 開著不算健康，要收到訂閱確認或第一筆合法事件；`observed_at` 與 `received_at` 分開；out-of-order 不可覆蓋較新的 current。
- 已知坑：
  - 船名可能合法含西里爾字母，所以不放進亂碼防護名單（見 [cwa-ncdr.md#ncdr_alerts](cwa-ncdr.md#ncdr_alerts)）。
  - 沒有同名 collector 心跳，健康要看專用的 `aisstream_ingest_health`；`config/realtime_tables.yaml` 曾用不存在的 `created_at` 監控 archive manifests（DIO-5，[collector-health 調查](../../investigations/2026-10-05-collector-health.md)）。
  - Disk IO 第三輪評估過「逐筆寫改成批次寫」，結論是省不到 IO，所以沒做（評估細節見 gis-platform（private）`docs/research/2026-10-16-disk-io-round3.md`，**待確認**該檔是否收錄此項）。
- 細節：[`docs/AISSTREAM.md`](../../AISSTREAM.md)；上游：analytics `aisstream/gotchas.md`（去重、observed vs received、watchdog）。

### gfw_vessel_presence
Global Fishing Watch 4Wings 每日 presence（舊版 DB collector）。

- 狀態：清冊標停用，**刻意保持關閉**，與 `gfw_hourly_publish` 互斥（`config.py` 啟動檢查）。需 `GFW_ACCESS_TOKEN`。
- 寫入：migration 371 的四張表 `live.gfw_vessel_presence_{runs,archive_manifests,snapshots,current}`；partial／failed run 只寫 run ledger，不推進 snapshot／current。
- 來源契約：`POST /v3/4wings/report`，dataset `public-global-presence:latest`（實際版本只從回應的 `x-datasets` header 記錄）；資料約落後 96 小時，`GFW_DATA_LAG_DAYS` 預設 5（只接受 4–30）；同帳號同時只允許一個 report，五個 corridor 依序送，不可平行；回應出現非零 `nextOffset` 就 fail closed，不發布截斷資料。
- 資料語意：座標是 GFW 每日格網中心（約 0.01°／0.1°），不是精確船位，也不是暗船偵測；每列帶 `quality_flags=["grid_cell_center"]`；跨 corridor 重複以 `source_event_key` 去重；缺座標標 `suspect`，不製造座標。
- **授權閘門**：`GFW_RAW_ARCHIVE_ENABLED` 預設 false：不寫本地 raw、DB 失敗也不寫 buffer、ArchiveTask 跳過殘留目錄。只有確認帳號與資料集條款允許永久冷儲存後才可由人明確打開。

### gfw_hourly_publish
GFW 每小時軌跡＋格網＋SAR 未配對偵測的每日發布 job（`tasks/gfw_hourly_publish.py`，不在 registry，清冊以排程 task 登記）。

- 排程：Zeabur production，每日 08:30 Asia/Taipei（UTC 換日後），deploy 不會立刻跑。**以子行程執行**，抓取尖峰數 GB 記憶體時不會 OOM 殺掉其他 collector。
- 每次重抓最新完整日往回 7 個 UTC 日（吸收 GFW 延遲修正），不是每天只補一天。
- 抓取：bbox 切成 16 個 3° tile。AIS（`public-global-presence:latest`，HOURLY／HIGH／VESSEL_ID）每 tile 一個 report，投影成 normalized shard 後同時 fan-out 給格網與軌跡，不為兩個圖層重抓；SAR（`public-global-sar-presence:latest`，`matched='false'`）另 16 個 report。共 32 個 logical report，**全部依序，不可平行**。schema drift 一律 fail closed。
- 語意：`dark_vessels` 只能解釋為「SAR 偵測到、但未配對到 AIS」，不是確定關 AIS、暗船或非法；座標是 HIGH 格網中心。沒有偵測的小時仍發布 0 feature 的檔。
- 不存 raw：raw 回應不寫 DB／S3／spool，只留當次 fan-out 用的 normalized shard。
- **發布契約（manifest-last）**：
  - 只有一個對外 root：`deploy-assets/global-maritime/gfw-hourly/manifest.json`，內含 `tracks.days[]`、`grid.hours[]`、`dark_vessels.hours[]`、`assets[]`。
  - 每個 immutable asset 先 PUT（SHA-256 寫進 object metadata），立刻 HEAD 比對大小與 hash；全部通過才上傳 release manifest，root 永遠最後 PUT 再 HEAD。三類產品缺一就不發布。root HEAD 異常時回復舊 root。
  - cache：root `public,max-age=60,s-maxage=60,stale-while-revalidate=300`；release asset `public,max-age=604800,immutable`。
  - 保留 current＋previous 兩版；清理只刪舊 root manifest 明列、且在嚴格日期 release 前綴下的 exact key，不用 list、glob 或前綴刪除，遇到未知 key 就 fail closed。失敗的本地 spool 保留，下次只清超過 7 天的 `status=failed` 已知樹。
  - S3 client、bucket、key prefix、公開 URL 都由 job 注入，模組內不讀 credentials；拒絕空值、絕對或 traversal key、非 HTTPS URL。
- **ledger 閘門**：`live.upsert_gfw_hourly_publish_run(jsonb)` 必須在第一個 GFW 請求前成功寫 `running`，失敗就不發請求；root cutover 後才寫 `succeeded`；之前的例外寫 `failed` 並保留 spool。cutover 後若 `succeeded` 寫不進去，有限重試 3 次，仍失敗則保留 `cutover_succeeded_ledger_pending` spool 與 `reconcile-ledger.json` 對帳，不反向寫 `failed`。前端新鮮度以 CDN root manifest 為準，DB health RPC（`public.get_gfw_hourly_publish_health()`）只供維運；`live.gfw_hourly_publish_runs` 因此不放進 `realtime_tables.yaml`。
- **full-fidelity 不變量**（schema v3）：每個 tile 都要在 fetch ledger、同一個 resolved dataset 版本、沒有非零 `nextOffset`；每小時 `sum(grid vessel_count) == canonical_vessel_hour_count`；發布的 segment 數與點數等於 eligible；不允許 display cap；每個 canonical 點屬於一條 segment 或一個 singleton。資源不夠時可以讓整輪失敗，**不可只發布一部分**。軌跡規則：每船每 UTC 小時一個 canonical 點；間隔或隱含航速超過門檻就斷開新 segment；只有一點的 segment 發布成 singleton，不捏造線；只允許同 segment 相鄰點的線性內插。格網多邊形是以中心 ±0.005° 推定的視覺化範圍，不是 GFW 官方邊界。
- 對外使用：非商業，前端要顯示 `Powered by Global Fishing Watch`、資料集版本與日期範圍、「延遲且為近似位置」的說明；token 不得進前端 bundle。
- 歷程（時點性，**待確認**現況）：2026-08-20 首次發布 v2 release；當時 root edge cache 回 `CF-Cache-Status: DYNAMIC` 尚未驗收。v3 shadow（2026-08-15..21，約 995 MB、3,313 檔）已完成 S3 全量 HEAD 稽核與 migration 377，但 2026-08-26 時 v3 程式尚未部署；P0 轉換問題（`running` 列繼承 v2 預設）由 migration 377 與 collector 防禦修正。安全發布的調查：[2026-09-18 GFW safe publish](../../investigations/2026-09-18-gfw-safe-publish.md)。
- 本機 POC 腳本：`scripts/gfw_hourly_tracks_poc.py`、`gfw_hourly_grid_poc.py`、`gfw_hourly_release.py`；測試 `tests/test_gfw_*`。
- 舊檔 [`docs/GFW_VESSEL_PRESENCE.md`](../../GFW_VESSEL_PRESENCE.md)、[`docs/GFW_HOURLY_FULL_FIDELITY.md`](../../GFW_HOURLY_FULL_FIDELITY.md) 已改為指向本節。官方文件：GFW 4Wings report API、license-rate-limits。上游：analytics `global_fishing_watch/`。

## 航空

### flight_fr24_zone
FR24 公開 feed 的 bbox 快照（最多飛機數、含起訖機場）。每 5 分鐘，Zeabur；寫 `live.flight_positions`（與 `flight_opensky` 共用）。

- 已知坑：心跳的名字是 `flight_fr24_zone`，不是 `flight_fr24`；監控 owner 曾對錯名字（[collector-health 調查](../../investigations/2026-10-05-collector-health.md)）。

### flight_fr24
FR24 非官方套件（`FlightRadarAPI`）抓台灣起降航班完整軌跡。2026-05 起停跑，清冊標停用；寫 `live.flight_positions`、`live.flight_trails`（分區 parent 恆空，所以不監控）。停用後 S3 archives 沒有新檔是預期。

### flight_opensky
OpenSky 台灣空域 state vectors。每 5 分鐘，Zeabur；寫 `live.flight_positions`。認證優先順序：OAuth2 client credentials（約 4,000 credits／日）→ Basic Auth（舊帳號）→ 匿名（約 400 credits／日），見 `collectors/flight_opensky.py` 開頭。

三個航班來源共用 `live.flight_positions`：任一源停掉，表的新鮮度會被其他源撐住。

## 太空

### satellite
Space-Track GP 資料＋SGP4 計算位置。

- 頻率／位置：每 2 小時，Zeabur（TLE 每 8–24 小時更新，2 小時足夠）。
- 寫入：`live.satellite_positions`、`live.satellite_current`、`live.satellite_tle`、`live.satellite_tle_history`。
- 守門：
  - **TLE 沒變不寫（2026-10-15，#146）**：Space-Track 每輪回全部約 7 萬顆，約 95% 和上一輪相同；`satellite_tle` 的 upsert 加 `IS DISTINCT FROM`，history 只送 epoch 有變的列。`updated_at` 的語意因此變成「最後異動」（`storage/supabase_writer.py` `_write_satellite_tle` 說明）。
  - current 用 `current_prune_by=collected_at` 清掉本批沒出現的幽靈列。
- 已知坑：2026-04 CelesTrak 封鎖 Zeabur IP，改用 Space-Track，見 [2026-04-14 CelesTrak 封鎖](../../../.claude/pitfalls/2026-04-14-celestrak-ip-blocked.md)。衛星名稱可能合法含西里爾字母，不可放進亂碼防護名單。

### satellite_passes_daily
中國衛星每日過境台灣彙總（由 `live.satellite_tle_history`＋SGP4 自算，不打外部 API）。

- 頻率／位置：每日，Zeabur；每次補算「昨天＋前天」（前天用來蓋掉昨天 TLE 還沒到齊的結果）。
- 寫入：不走 `SupabaseWriter`，直接 SQL：`live.satellite_passes`、對 `spatial.county_boundaries` 做 `ST_Intersects` 更新縣市、重算 `live.satellite_passes_daily`，再呼叫 `public.refresh_isr_satellite_passes_daily()`。用獨立連線（`storage.db.connect_supabase`）與 300 秒 statement timeout。
- 已知坑：**沒有表級監控**（清冊註明「表級監控待人工設計」），停了只能從心跳或下游頁面發現。

### launch
火箭發射（Launch Library 2，TheSpaceDevs）。每 15 分鐘，Zeabur；寫 `live.launches`、`live.launch_events`、`live.launch_pads`（多表）。免費額度約每小時 15 次，15 分鐘一輪約每小時 4 次；啟動時回溯 5 年。

## 收這類資料要注意

- 只接受台灣 IP 的來源（航港局）放 VM，Zeabur 端保持關閉；VM 的心跳不可信，直接看表。
- 移動物件的 current 表：預設要有「久未出現就清掉」的機制，索引只留真的被查詢用的。
- 「沒變不寫」對 TLE、站表這類大多不變的資料效益最大；先確認 `updated_at` 沒有被別人當心跳。
- 多個來源寫同一張表（航班三源）時，一源停掉看不出來；要分 source 看新鮮度。
- 授權受限的來源（GFW raw、IODA）只存 normalized 結果，raw 是否冷存要看條款，預設關閉。
- 長駐 WebSocket 的健康標準是「有收到合法事件」，不是「socket 開著」。
- 大型發布一律 manifest-last：immutable asset 驗證完才切 root，清理只刪明列的 key。
