# 日本氣象廳（JMA）、環境省與全球氣候模式

**一句話**：這一類是「網格／圖磚／模式場」為主的大型資料，原始量大、上游保留期短（幾小時到幾天），所以採 ADR-0021 的分層留存（原始冷存、解碼網格溫存、彙總熱存、R2 只放發布幀）；最大的坑是儲存成本和「上游凍結但仍回 200」：物件數比容量貴，舊端點停更也不報錯。

清單與寫入表以 [collectors.md](../collectors.md) 為準。設計依據：.gis-agent-system（private）`decisions/0021-weather-data-collection-and-tiered-retention.md`（四型 S／E／G／Y、T0–T3）。上游 API 細節在 taipei-gis-analytics（private）`docs/api-platforms/` 的 `jma/`、`jma_typhoon/`、`jtwc/`、`env_jp_wbgt/`、`cmems/`、`cams/`、`noaa_gfs/`、`usgs_earthquake/`；前端契約在 analytics `docs/topic-research/japan_opendata/jma-frontend-contract.md`，收集計畫在同目錄 `jma-collectors-plan.md`。

## 共通機制

- **分層（T 層）**：T3 冷＝原始檔每產品每日打成 1 個 tar，上傳時直接 `DEEP_ARCHIVE`，永久；T2 溫＝解碼後網格（npz），S3 STANDARD 後由全桶 lifecycle 轉 IA／GLACIER_IR，永久（2026-10-10 使用者決定細節不可只留彙總）；T1 熱＝每日彙總；R2 只放前端用的發布幀，是可由 T2 重算的快取。DB 只放「狀態列」（`live.weather_raster_frames`、`live.weather_raster_daily`），是資料狀態看板的來源。保留天數以 gis-platform `metadata.retention_policies` 與 R2 lifecycle 為準，見 [data-lifecycle.md](../data-lifecycle.md)。
- **物件數比容量貴**：Deep Archive 每千次 PUT 約 US$0.05，圖磚逐張存一年約 2,100 萬物件、約 US$1,000；每日打成 1 個 tar 一年約 US$0.02。**不可每張圖磚一個物件**。
- **上傳要驗證**：`storage.s3.put_verified`（上傳後 HEAD 比對大小與自訂 metadata sha256，已存在且相符就跳過）；驗證成功才刪本機 spool。
- **state 只在 DB 寫入成功後前進**：多數 JMA collector 設 `require_db_write`，DB 失敗時本輪算失敗、state 不動，下輪重送（`jma_quake` 模式）。
- **缺值不是 0**：缺測、範圍外、表外顏色一律 NULL 或 255，表外顏色要計數並 log 告警；彙總的分母用 `valid_frames`。
- **上游凍結但仍回 200**：JMA 舊警報端點自 2026-05-28 起停更但照常回應。每個 collector 要比對資料內的來源時間，不能只看 HTTP 狀態。
- **授權**：JMA bosai 是公共データ利用規約 1.0（PDL1.0），前端要標「出典：気象庁」並註明加工；環境省 WBGT **不適用 PDL1.0**，轉載要明記出典為環境省，對外發布前要確認。
- **容器依賴**：模式場 collector 依賴 xarray、cfgrib（需系統庫 eccodes）、cdsapi、copernicusmarine。2026-07-02 曾因 requirements 與 Dockerfile 缺套件，`*_ENABLED=true` 卻靜默沒資料（lazy import 失敗）。env 設了沒效果，先查 image 有沒有套件。
- **西里爾字母**：USGS 地名可能合法含西里爾字母，不可放進寫入層亂碼防護名單（見 [cwa-ncdr.md#ncdr_alerts](cwa-ncdr.md#ncdr_alerts)）。

## 日本氣象廳（JMA）

### jma_raster
JMA 網格圖磚（G 型）：雷達、解析雨量、キキクル（土砂／浸水／洪水）、解析積雪深、ひまわり B13。

- 頻率／位置：每 60 分鐘，Zeabur。需 `S3_BUCKET`（沒有 S3 只會累積 spool、無法日結），設 `require_db_write`；`COLLECT_TIMEOUT` 3600 秒。
- 寫入：`live.weather_raster_frames`（每幀狀態，PK `(source, product, valid_time)`，`DO NOTHING`）、`live.weather_raster_daily`（每產品每日狀態，upsert）；public view `weather_raster_daily`、`weather_raster_frames_recent`。表在 gis-platform migration 437／438，發布幀的 image 欄在 461。
- 產品（端點前綴 `https://www.jma.go.jp/bosai`）：

  | product | 上游可回溯 | 收集節奏 | 解碼 |
  |---|---|---|---|
  | radar（nowc hrpns） | 24 小時 | 每 5 分全收 | 0–8 級 |
  | rasrf（解析雨量 1 小時） | 12 小時 | 每 30 分；只收解析值（member `none`），不收預報 | 是 |
  | risk_land／risk_inund（キキクル） | 5.5 小時 | 每 30 分（`JAPAN_JMA_RASTER_CADENCE`，只收 :00／:30，必須是原生間隔的倍數）；member 只收 `none` | 危險度等級 |
  | risk_flood | 5.5 小時 | 同上 | 是 pbf 向量磚，**只冷存不解碼不發布** |
  | snow_depth（解析積雪深） | 24 小時 | 每 1 小時；**只收 10–5 月 JST**（`JAPAN_JMA_RASTER_SEASON`，2026-10-15 由 11–4 月放寬） | 是 |
  | himawari_b13 | 約 119 小時（`targetTimes_fd` 只列約 36 小時，偶有整幀 404） | 每 1 小時 1 張 | 不解碼等級，12 磚拼成灰階 |

- 收集：每輪對每個產品補抓「上次成功到現在」所有沒收的 validtime（在上游保留期內），中斷數小時也不漏。範圍是 Web Mercator z6 x52–59、y22–27（日本畫布）。先抓 z6 全幅，只對非空白的 z6 磚往下抓 z8（每幀上限 48＋15×16＝288 請求，依回波量排序只下鑽前 15 磚，其餘 upsample；過期未抓的記 `fine_unfetched`；backlog 速率 `JAPAN_JMA_RASTER_BACKLOG_RPS`）。並發 ≤ 4。
  - 空磚判定：334 bytes 的透明佔位，或 P-mode 全部 index ≤ 1（radar z8 有 228–246 bytes 的有內容磚，不能只看大小）。
  - 粗圖探測 `JAPAN_JMA_RASTER_COARSE_PROBE=off|shadow|on`（預設 shadow）：先抓 z4，`on` 時 z4 全空的區域不抓 z6／z8 並記 `inferred_blank_tiles`。建議 shadow 跑過有キキクル著色與降雪的日子、`sum(probe_misses)=0` 再切 on。
  - radar 的 `targetTimes_N1` 只露 3 小時，但保留 24 小時，所以用 5 分 cadence 算術回推。
- 解碼：表在 `config/decode_tables/jma/{product}.yaml`（版本、palette→level、依據、生效日）；radar 用 z7（z8 2×2 取最大），其他用 z6。uint8，0＝無、255＝缺測；表外顏色計數告警，不可歸 0。
- 分層（JST 切日；日 D 在 D+1 01:00 之後、上游已不可能補抓時才日結）：

  | 層 | 內容 | 位置 |
  |---|---|---|
  | spool | 原始磚、當日累加器（`np.memmap`，跨重啟續算，不吃 RAM） | Zeabur `/data/weather_raw/jma/{product}/{YYYYMMDD}/`，驗證上傳後即刪，上限 2 天 |
  | T3 冷 | 每產品每日 1 個 tar＋manifest（每幀 vt／bt、每檔 sha256、缺幀、抓取層級、解碼表版本）；磚級 sha256 去重，空白磚只記 manifest | S3 `weather-raw/jma/{product}/{YYYY}/{MM}/{YYYYMMDD}.tar`，DEEP_ARCHIVE |
  | T2 溫 | 每小時一檔網格（檔內 zero／same_as_prev 去重） | S3 `weather-grid/jma/{product}/{YYYYMMDD}/{HH}.npz` |
  | T1 熱 | 每日彙總 `count_ge_{k}`、`max_level`、`valid_frames`＋georef | S3 `weather-daily/jma/{product}/{YYYY}/{YYYYMMDD}.npz` |
  | 發布幀 | 每幀一張 8-bit LA PNG（`level_la_v1`，EPSG:3857，z7 產品 2×2 取最大到 z6）；himawari 為 L-mode JPEG（`gray_jpeg_v1`） | R2 `imagery/jma/{product}/{YYYYMMDD}/{HHMMSS}.{png\|jpg}`（UTC valid_time），immutable cache |
  | 分析圖 | 由 T1 累積的季／月至今每日一張（`count_la_v1`）；可重算的累積快照另存 S3 `weather-daily/jma/_analysis/` | R2 `imagery/jma/ana_*/…`（＋同名 `.json` 旁檔），`max-age=3600` |

- 發布幀（`collectors/weather_raster/publish.py`）：radar、rasrf、risk_land、risk_inund、snow_depth 在解碼完成當下就發布（前端即時可見），補抓段之後的 z8 細化不重發（key immutable）。R2 失敗時，幀定案時用 spool 網格重試一次，仍失敗留給 `scripts/backfill_jma_imagery.py`（讀 T2 重發；已存在、在保留窗內且 R2 HEAD 存在才跳過）。image 欄在狀態列 upsert 成功後另開交易 UPDATE，失敗只 log。開關 `JAPAN_JMA_IMAGERY_PUBLISH`；沒設 R2 自動略過。發布失敗絕不影響 T2／T3。決定性：同環境同輸入同 bytes，跨機器的冪等靠「已存在就跳過」。
- ひまわり B13：z5 x26–29、y11–13 共 12 磚拼成 1024×768（經度 112.5–157.5、緯度 21.94–48.92）。值是上游灰階 0–255（越白越冷），JMA 沒公開灰階對亮度溫度的對照，所以 `gray_jpeg_v1` 只是顯示用單調映射、不是輻射定標。**12 磚不齊就不產 T2、不發布**（灰階 0 是合法值，不能用 0 填），原始磚照樣進 T3。R2 選 L-mode JPEG quality 80：上游磚本身就是 IJG quality 80，同一張量化表重編碼幾乎無損（24 幀中 7 幀逐位相同），而且比原磚小約 12%；無損格式大 3.5–3.9 倍卻沒有多任何資訊。T2 存 uint8 npz（每小時約 430 KB），讓 90 天後重發不必 restore Deep Archive。無 T1。回補 `scripts/backfill_jma_himawari_imagery.py`（上游直抓，≤ 1 req/s）。
- 分析圖（`collectors/weather_raster/analysis.py`，演算法版本 `jma-analysis-v1`）：`ana_snow_first`、`ana_snow_days`（積雪季 10/1–5/31，門檻 ≥ 5 cm）、`ana_rain_days`（用 rasrf 不用 radar，≥ 1 mm）、`ana_risk_land_hours`、`ana_risk_inund_hours`（警戒以上時數，土砂與浸水分開兩張，因為日計數推不出聯集）。每張代表「截至 JST 日 D 結束」。覆蓋率不足 `JAPAN_JMA_ANALYSIS_MIN_COVERAGE`（0.5）的像素標未涵蓋（L＝255），**缺資料不得變 0**；有效起日不早於 `JAPAN_JMA_ANALYSIS_EPOCH`（2026-10-10），所以 2026-10 各月與 season2026 的狀態都會是 partial，不是品質故障。遲到的 T1 會讓之後的圖重算覆寫同 key（所以 cache 不是 immutable）。在 `jma_raster.collect()` 尾端執行（獨立 try、時間預算 180 秒、只看近 7 天；更早用 `scripts/backfill_jma_analysis.py`）。pending 列不可放進產品的 `pending_frames`（00:00 JST＝15:00 UTC 會和 snow_depth 整點相撞）。
- 已知坑：
  - 2026-10-11 修過的 bug：「missing 定案」原本每輪從 epoch 掃到視窗起點，而已日結清單只留 10 天，約 11 天後會把舊日期重寫 missing、重建 spool，再日結時以**空 tar 覆蓋 S3 上的 T3**。改為 state `finalized_until` 只往後掃，並以 `_receipts/{product}/{day}.json` 擋已日結日。教訓：冷存的「覆蓋」要有 receipt 擋。
  - 積雪季外的時刻不抓、不產 frames 列、不算 missing；上游只留 24 小時，季節放寬前漏掉的日子補不回來。
  - 每日請求數約 9.5 萬（10 月小雨日，shadow 模式），雷達佔大宗；全乾日切 on 後約 1.4 萬。
- 測試：`tests/test_jma_raster.py`、`test_jma_publish.py`、`test_jma_analysis.py`。
- 契約正本仍是 [`docs/JMA_RASTER_CONTRACT.md`](../../JMA_RASTER_CONTRACT.md)（程式註解依節名引用，保留全文）；[`docs/JMA_RASTER_NOTES.md`](../../JMA_RASTER_NOTES.md) 已改為指向本節，量測過程見其 git 歷史（2026-10-16 前的版本）。

### jma_ocean_env
JMA 海洋・環境網格：海溫（sst_him 0.1°、sst_mgd 0.25° 全球）、海冰（12–5 月）、UV 解析圖磚、黃砂預測圖。

- 頻率／位置：每 60 分鐘，Zeabur（2026-10-12 起清冊標啟用）。需 `S3_BUCKET`，設 `require_db_write`；`COLLECT_TIMEOUT` 1800 秒。
- 寫入：沿用 `live.weather_raster_frames`／`weather_raster_daily`（與 `jma_raster` 共用，`source='jma'`），不需新 migration。T3／T2 照 jma_raster 的分層；T1 只有 UV。
- 已知坑：UV 更新時間檔的數字是 JST，但圖磚路徑是 UTC；黃砂同檔名每日覆寫，要整批抓再用 sha256 判斷有沒有變；黃砂沒有數值刻度，**只冷存不解碼**。SST、海冰各有自己的缺值碼，一律不當 0。
- 細節：[`docs/JMA_OCEAN_ENV.md`](../../JMA_OCEAN_ENV.md)。共用表：jma_raster 和這支一源停掉，表的新鮮度會被另一源撐住。

### jma_amedas
AMeDAS 約 1,286 站 10 分觀測（bosai 非公式 JSON）。

- 頻率／位置：每 10 分鐘，Zeabur。`COLLECT_TIMEOUT` 600 秒。
- 寫入：`live.jma_amedas_current`（全量 upsert）、`live.jma_amedas_observations`（只寫整點）。另外每個 JST 日結一份每站×10 分的 Parquet 上 S3 `weather-obs/jma/amedas/YYYY/MM/YYYYMMDD.parquet`（`put_verified`；缺的時刻向上游補抓，上游可回溯約 7 天；`JAPAN_JMA_AMEDAS_DAILY_PARQUET=false` 可關）。
- 已知坑：舊註解曾寫 observations 保留 90 天，gis-platform migration 436 已收緊；註解已改成指向 `metadata.retention_policies`（2026-10-16），**以 `retention_policies` 為準**。本機 JSON 目錄用台北日期，JST 日 D 跨兩個目錄。

### jma_warnings
JMA 警報・注意報（R8 `map.json`）。

- 頻率／位置：每 5 分鐘，Zeabur；`map_time` 有變才抓。設 `require_db_write`。
- 寫入：`live.jma_warning_events`，**只記變動**（start／status_change／end；UNIQUE 含 change_type，`DO NOTHING`）。舊 `live.jma_warnings` 停寫（migration 438 以舊快照逐輪差分回填）。整份有效快照只進本地 JSON → S3 archives，不進 DB。
- 守門：state（`state/jma_warnings_state.json`）只在 DB 寫入成功後前進；state 遺失時從 `public.jma_warnings_current` 重建，查不到就整輪失敗（絕不把全部當 start）。**寫入失敗不進 writer buffer**（`NO_BUFFER_COLLECTORS`），否則下一輪用新的基準重新差分、buffer 又補寫舊輪，同一變動會出現兩筆。
- 已知坑：舊端點 `warning/data/warning/` 自 2026-05-28 起凍結但仍回 200，禁用。同 status 只換 report_datetime（継続再發）不算事件。

### jma_quake
JMA 地震、津波 list＋火山 warning。每 2 分鐘，Zeabur；設 `require_db_write`；寫 `live.jma_quake_reports`、`live.jma_tsunami_reports`、`live.jma_volcano_warnings`（`DO NOTHING`，只有新情報才推進 MAX）。事件驅動，清冊門檻放寬到 1 日。

### jma_seasonal
JMA 季節觀測：生物季節（櫻、楓、銀杏，HTML＋XML VGSK55）、山頂初冠雪、梅雨速報。每 6 小時，Zeabur；設 `require_db_write`；寫 migration 439 的四張表，只記變動（`DO NOTHING`，永久）。季節性來源長期沒有新列屬預期。

- 已知坑：頁面檔名編號不等於現象碼；初冠雪 2027 寒候年起改用解析積雪深判定，和目視不可直接比較（`judgement_method` 分開）；`-` 是未觀測不寫列，`///` 是無差值存 NULL。
- 細節：[`docs/JMA_SEASONAL.md`](../../JMA_SEASONAL.md)。

### jma_lightning
JMA 雷ナウキャスト LIDEN 5 分幀。

- 頻率／位置：每 5 分鐘，Zeabur。設 `require_db_write`。
- 寫入：`live.lightning_events`，`source='jma'`（與台電、氣象署落雷共用；`source_type` 存上游 type 原值）。上游 `id` 是幀內序號不是雷擊 ID，去重用 `dedup_hash`（毫秒 UTC、經緯度、type），`DO NOTHING`。
- 守門：`targetTimes` 只列約 3 小時，但伺服器實際留約 4–5 天；collector 自產 5 分格點，補抓 48 小時內缺的幀（`JAPAN_JMA_LIGHTNING_BACKFILL_HOURS`），404 三輪後記為上游缺幀。
- 已知坑：type 0 與 1 的差別官方未說明；共用表時一源壞掉看不出來（見 [cwa-ncdr.md#lightning_events](cwa-ncdr.md#lightning_events)）。
- 細節：[`docs/JMA_LIGHTNING_FORECAST_WBGT.md`](../../JMA_LIGHTNING_FORECAST_WBGT.md)。

### jma_forecast
府縣天氣預報（56 office）＋早期注意情報。每 6 小時，Zeabur；`map.json` 的 reportDatetime 有變才抓該 office；寫 `live.jma_forecasts`、`live.jma_forecast_probabilities`（每次發表都保留，`DO NOTHING`）。

- 已知坑：上游空字串一律 NULL；05 時發表也有「今日最低＝今日最高」的佔位，存 NULL；早期注意的 `''` 是「未達中」不是缺值。6 小時輪詢會漏掉兩次定時發表之間的更正版，是否改 60 分**待使用者決定**（見細節文件）。

### env_jp_wbgt
環境省熱中症 WBGT 實況（月檔，841 站，站號＝AMeDAS）與預報。

- 頻率／位置：每 60 分鐘，Zeabur；服務期約 4/22–10/21，季外不發請求（清冊註明季外 DEAD 屬預期）。設 `require_db_write`。
- 寫入：`live.env_jp_wbgt_observations`（`DO UPDATE … WHERE wbgt IS DISTINCT FROM`，官方曾回溯修正實況）、`live.env_jp_wbgt_forecasts`。
- 守門：HTTP 條件請求（304 跳過）＋每列 md5，列變了才送；**寫入失敗不進 buffer**（`NO_BUFFER_COLLECTORS`），避免舊值晚於修正值重放把它蓋回去。空格不寫列，不當 0。
- 授權：不適用 PDL1.0，見共通機制。

## 全球氣候

### global_climate_usgs_earthquake
USGS 全球地震（`all_day.geojson`，免認證）。每小時，Zeabur；寫 `live.earthquakes_global`。地名可能合法含西里爾字母。

### global_climate_jma_typhoon
JMA RSMC Tokyo 颱風（免認證 JSON）。每 3 小時，Zeabur；`targetTc.json` 為空時不寫；寫 `live.typhoon_positions`（`source='jma'`），與 JTWC 共用表。事件驅動，清冊門檻放寬到 1 日。

### global_climate_jtwc
JTWC 颱風（RSS＋ATCF 文字，免認證）。每 6 小時，Zeabur；寫 `live.typhoon_positions`（`source='jtwc'`）。

### global_climate_cmems
Copernicus Marine GLO12 海洋模式 subset。

- 頻率／位置：每日，Zeabur。需 `COPERNICUSMARINE_SERVICE_USERNAME`／`PASSWORD`；`COLLECT_TIMEOUT` 1800 秒。
- 寫入：`live.global_climate_grids`（三個模式源共用；DB 存 digest，binary 走 S3）。
- 已知坑：**subset 一定要帶時間範圍**。2026-07-02 擴大範圍後沒帶 `--start/--end-datetime`，單檔抓整段時間軸 18.7 GB，container 被健康檢查判死、每約 25 分鐘重跑，一下午約 56 GB 上 S3；修法固定帶「今日 00Z＋48 小時」（實測 27 MB）。事故紀錄：mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-02。

### global_climate_cams
Copernicus ADS CAMS 大氣化學。每日，Zeabur；需 `CAMS_API_KEY`，且帳號要先在網站接受資料集授權；請求要排隊 5–30 分鐘，`COLLECT_TIMEOUT` 2400 秒；寫 `live.global_climate_grids`。

### global_climate_noaa_gfs
NOAA GFS 全球風場（AWS Open Data S3，免認證）。每日，Zeabur；用 HTTP Range 只抓需要的變數；寫 `live.global_climate_grids`。需要系統庫 eccodes（cfgrib），見共通機制的容器依賴。

### global_climate_bake
把 GFS／CMEMS／CAMS 最新場烤成前端用的 PNG＋JSON。每 6 小時，Zeabur；產物在 S3 `deploy-assets/climate/*_latest.*`，**不寫 DB**。

- 已知坑：**沒有表級監控**，也沒有 TABLE_MAP。停了只能從心跳或前端圖層過期發現。全桶 lifecycle 規則會波及 `deploy-assets/`（部署用 `aws s3 sync` 會產生取回費），見 [`docs/AWS_INVENTORY.md`](../../AWS_INVENTORY.md)。

## 收這類資料要注意

- 先算物件數與請求數，再算容量：圖磚一律每日打包，不可逐張存 Deep Archive。
- 原始資料一定冷存（T3），解碼表要版本化，衍生物（T2／T1／R2）都要能從下一層重算。
- 上游保留期短的產品，state 要記「收到哪裡」並自動補抓；季外、範圍外、上游缺幀要和「我們漏抓」分開記。
- 冷存的覆蓋寫入要有 receipt 擋，避免空檔蓋掉已日結的資料。
- 「只記變動」的 collector，寫入失敗不可進 buffer 重放（會重複或以舊蓋新），改靠 state 不前進重送。
- 模式場類請求一定帶時間與空間範圍；新套件要同步 requirements 與 Dockerfile。
- 授權不同：JMA 是 PDL1.0，環境省 WBGT 不是；發布前確認出典標示。
- `global_climate_bake` 沒有表級監控；共用表（落雷、weather_raster、typhoon、global_climate_grids）一源停掉看不出來。
