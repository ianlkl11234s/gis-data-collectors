# JMA 網格圖磚（G 型）契約 — collector `jma_raster` ＋ gis-platform mig 437

依據：`.gis-agent-system/decisions/0021-weather-data-collection-and-tiered-retention.md`（G 流程、T0–T3）、
analytics `docs/topic-research/japan_opendata/jma-collectors-plan.md`（分層留存一節）、`docs/api-platforms/jma/{endpoints.yaml,gotchas.md}`。
使用者硬性要求：**嚴格控制儲存成本，但原始資料一定要有冷儲存**；資料循環週期要清楚。

## 產品（第一版）
| product | 端點 | 上游可回溯 | 取樣 | 解碼 |
|---|---|---|---|---|
| radar (nowc hrpns) | B/jmatile/data/nowc/{bt}/none/{vt}/surf/hrpns/{z}/{x}/{y}.png | 24h | 每 5 分全收 | 是，0–8 級 |
| rasrf (解析雨量 1h) | B/jmatile/data/rasrf/{bt}/immed/{vt}/surf/rasrf/{z}/{x}/{y}.png（只收解析值，不收預報） | 12h | 每 30 分 | 是 |
| risk_land / risk_inund / risk_flood (キキクル) | B/jmatile/data/risk/{bt}/{member}/{vt}/surf/{land\|inund\|flood}/… | 5.5h | 每 10 分 | 是（危險度等級） |
| snow_depth (解析積雪深 snowd) | B/jmatile/data/snow/{bt}/none/{vt}/surf/snowd/… | 24h | 每 1 小時 | 是 |
| himawari_b13 | B/himawari/data/satimg/{bt}/fd/{vt}/B13/TBB/{z}/{x}/{y}.jpg | ~119h（targetTimes_fd 只列 ~36h；偶有整幀 404 的洞） | 每 1 小時 1 張 | 不解碼等級；2026-10-16 起 12 磚拼成灰階 uint8 1024×768（T2＋發布幀，無 T1） |
B = https://www.jma.go.jp/bosai。各產品實際有內容的 zoom、member 意義、色票要先實測（radar／rasrf 已知 z5 是 334 byte 佔位，z4/z6/z8 有內容；4-bit 調色盤 idx2–9 → 8 級）。實測結果寫進回報與解碼表。

## 收集
- collector `jma_raster`（prefix `JAPAN_JMA_RASTER`，預設 False，間隔 60 分）。每輪對每個產品補抓「上次成功之後到現在」所有還沒收的 validtime（上游保留期內），所以電腦／服務中斷數小時也不漏。
- 抓取：範圍 Web Mercator z6 x=52..59、y=22..27（＝日本畫布，與範例頁 z5 x26–29,y11–13 相同範圍）。先抓 z6 全幅；**只對非空白（>334 byte 或非全透明）的 z6 磚往下抓 z8 子磚**（radar／rasrf；其他產品依實測原生層級決定）。並發 ≤4、每請求 timeout、UA 沿用 jma_common；每輪記錄請求數。
- 解碼網格：radar 用 z7（4096×3072，z8 2×2 取最大）；其他產品用 z6（2048×1536）或依原生解析度，寫在解碼表。uint8，0＝無、255＝缺測。表外顏色計數並 log 告警，不可歸 0。
- 解碼表：`config/decode_tables/jma/{product}.yaml`（version、palette index/RGB→level、label、依據、生效日）。

## 儲存循環（T 層）
| 層 | 內容 | key | 等級 | 保留 |
|---|---|---|---|---|
| spool（Zeabur /data） | 原始磚、當日累加器 | /data/weather_raw/jma/{product}/{YYYYMMDD}/… | 本機 | 打包上傳驗證成功即刪；上限 2 天 |
| T3 冷 | 原始磚每產品每日 1 個 tar（不壓縮，PNG 已壓縮）＋ manifest.json（每幀 vt/bt、每檔 sha256/bytes、缺幀、抓取層級、解碼表版本） | S3 `weather-raw/jma/{product}/{YYYY}/{MM}/{YYYYMMDD}.tar` | 上傳時 **StorageClass=DEEP_ARCHIVE** | 永久 |
| T2 溫 | 每小時級距網格（該小時所有幀 stack，np.savez_compressed） | S3 `weather-grid/jma/{product}/{YYYYMMDD}/{HH}.npz` | STANDARD（全桶 lifecycle 30 天轉 IA、90 天轉 GLACIER_IR） | **永久**（2026-10-10 使用者決定：細節不可只留彙總；原「90 天 expiration」已作廢） |
| T1 熱 | 每日彙總：count_ge_{k}（uint16，k 依產品，radar 取 1、2、4 級＝有回波、≥1mm/h、≥10mm/h）、max_level（uint8）、valid_frames（標量）＋ georef JSON | S3 `weather-daily/jma/{product}/{YYYY}/{YYYYMMDD}.npz` | STANDARD | 永久 |
- 日期以 JST 切日。日結：JST 日期 D 在 D+1 01:00 之後、且該日上游已不可能補抓時（或幀已齊）打包；上傳後 head 驗 ContentLength＋自訂 metadata sha256，成功才刪 spool。
- 累加器用 np.memmap 放 spool，跨重啟續算，不吃 RAM。單輪峰值記憶體目標 < 300 MB。
- 不可每張圖磚一個物件（Deep Archive 每千物件 US$0.05）。

| 發布幀（R2 快取） | 每幀一張 8-bit LA PNG（EPSG:3857，z7 產品 2×2 取最大降到 z6）；himawari_b13 為 L-mode JPEG（gray_jpeg_v1）；可由 T2 重算 | R2 `imagery/jma/{product}/{YYYYMMDD}/{HHMMSS}.{png\|jpg}`（UTC valid_time） | R2（immutable Cache-Control） | 90 天（2026-10-15 拍板） |

## 發布幀（2026-10-15；契約 analytics `docs/topic-research/japan_opendata/jma-frontend-contract.md`）
- 模組 `collectors/weather_raster/publish.py`：純函式編碼（`level_la_v1`／`sst_la_v1`）、經緯度網格 nearest 轉 3857、`publish()` 上傳 R2。
- 時點：radar／rasrf／risk_land／risk_inund／snow_depth 在 **`fetch_frame` 解碼完成當下**以記憶體內網格發布（不等小時收尾，前端即時可見）。
  補抓段之後的 z8 細化**不重發**（key immutable；PNG 是 z6，z8 細節經 2×2 取最大後影響有限）。抓取當下 R2 失敗 → 幀定案（settle）時以 spool 網格重試一次；仍失敗留給回補腳本。
  risk_flood（pbf）不發布。himawari_b13 見下節「ひまわり B13 發布幀」。
- DB：image 欄（gis-platform mig 461：image_key、image_bytes、image_width/height、lon/lat_min/max、encoding、published_at）跟著 frames 狀態列送 writer，
  writer 在狀態列 upsert 成功後**另開 transaction 以 UPDATE 寫入**（`_write_weather_raster_images`），失敗只 log，不影響 frames／daily 狀態寫入。
- 開關：`JAPAN_JMA_IMAGERY_PUBLISH`（預設 true）；R2_* 未設定自動略過（只 log 一次）。發布失敗絕不影響 T2／T3。
- 回補：`scripts/backfill_jma_imagery.py`（讀 S3 T2 → 同一套編碼 → R2＋UPDATE；`image_key` 已填且 published_at 在 89 天內且 R2 HEAD 存在才跳過，否則重發並更新 published_at；T2 缺幀不動）。
- 決定性：同環境同輸入同 bytes（compress_level 9、無 metadata）；不同機器的 zlib／Pillow 版本可能不同，冪等靠「已存在就跳過」。
- 實測大小與 CPU 見本檔「發布幀量測」。

## 發布幀量測（2026-10-16，`backfill_jma_imagery.py --dry-run`，真 S3 T2 2026-10-10..10-16，本機 M 系列 Mac）
| product | 幀 | PNG 平均 | 最大 | 編碼 s/幀 | 每日約 |
|---|---|---|---|---|---|
| radar（2048×1536） | 1,550 | 21.3 KB | 52.8 KB | 0.32 | 288 幀 ≈ 6.1 MB（雨天 ~11 MB） |
| rasrf | 259 | 22.2 KB | 45.5 KB | 0.24 | 48 幀 ≈ 1.1 MB |
| risk_land／risk_inund | 423／425 | 6.2 KB | 6.3 KB | 0.1–0.2 | 各 48 幀 ≈ 0.3 MB |
| snow_depth | 74 | 6.2 KB | 6.4 KB | 0.03 | 24 幀 ≈ 0.15 MB（10 月幾乎無雪；冬季會變大） |
| uv_anal（768×768） | 60 | 3.3 KB | 3.8 KB | 0.02 | 13 幀 ≈ 0.04 MB |
| sst_him（800×755） | 5 | 100 KB | 103 KB | 0.5 | 0.1 MB |
| sea_ice | 0 | — | — | — | 季外（12–5 月） |
合計約 8 MB／日 → R2 保留 90 天穩態約 0.7–1.2 GB；每年寫入約 3 GB。

## ひまわり B13 發布幀（2026-10-16）
- 決定：**R2 發布 L-mode JPEG quality 80（`gray_jpeg_v1`，Content-Type image/jpeg，約 78 KB／幀）＋ S3 T2 每小時 uint8 npz（約 430 KB／小時，永久）**；T3 原始 tar（DEEP_ARCHIVE）不變；頻率維持每小時（使用者拍板，不加密）。量測與比較表見 JMA_RASTER_NOTES.md「ひまわり B13 編碼量測」。
- 拼圖：z5 x26–29、y11–13 共 12 磚（`canvas(5)`）→ 1024×768，row 0＝北；四角 = `spool.georef(5, 26, 11, 1024, 768)`：經度 112.5–157.5、緯度 21.94–48.92（EPSG:3857 圖，四角以經緯度記）。
- 值：上游灰階原值 0–255（越白越冷）。JMA bosai 前端資產沒有公開 B13 TBB 灰階→亮度溫度（K）的對照（2026-10-16 查 map.html 只見產品路徑 `ir:"B13/TBB"`，無圖例），所以 `gray_jpeg_v1` 定義為**顯示用單調映射、非輻射定標**。
- 12 磚不齊（任一磚非 200 或無法解碼）：幀狀態照舊 partial，**不產 T2 網格、不發布**（灰階 0 是合法值，不可用 0 填）；原始磚照樣進 T3。
- T2：`weather-grid/jma/himawari_b13/{YYYYMMDD JST}/{HH}.npz`，`spool.write_hour_npz` 同格式（每小時 1 幀），meta 由 `jma_raster.mosaic_t2_meta` 產生（collector 與回補共用），`decode_table_version=null`。無 T1。
- 實作：`Product.publish_kind="gray_jpeg"`（`grid_zoom` 維持 None，不走 decode table／累加器）；`fetch_frame` 抓完 12 磚即拼圖、存 spool grids、發布；settle 時若當下 R2 失敗以 spool 網格重試一次。
- 回補：`scripts/backfill_jma_himawari_imagery.py`（上游直抓，≤1 req/s，冪等規則同 backfill_jma_imagery；T2 已存在不寫）。T3 在 DEEP_ARCHIVE，不 restore。

## DB（gis-platform mig 437，live schema）
- `live.weather_raster_frames`：source text, product text, valid_time timestamptz, base_time timestamptz, zoom_raw smallint, tiles_requested int, tiles_nonempty int, raw_bytes bigint, nonzero_pixels int, unknown_color_pixels int, status text（ok／partial／missing）, collected_at timestamptz default now()；PK (source, product, valid_time)；DO NOTHING；保留 400 天。
- `live.weather_raster_daily`：source, product, obs_date date, frames_expected int, frames_ok int, raw_key text, raw_bytes bigint, raw_sha256 text, raw_storage_class text, grid_objects int, grid_bytes bigint, summary_key text, decode_table_version text, status text（packed／uploaded／verified／failed）, uploaded_at timestamptz；PK (source, product, obs_date)；upsert 更新；永久。
- public views：`public.weather_raster_daily`、`public.weather_raster_frames_recent`（近 48h）。RLS＋anon SELECT（照 418／435）。
- 這兩張表就是「資料狀態」看板的來源。

## 驗收
- 單元測試：解碼（含表外顏色）、spool→tar→manifest、日結判斷、累加器跨重啟。
- 本機實打：radar 抓 ≥1 小時（12 幀）、其他各產品 ≥1 幀，回報每幀請求數、raw bytes、npz bytes、耗時、峰值記憶體；用這些數字推估每日／每年 T1–T3 容量與 S3 月費。
- 一次真實 S3 上傳到 `weather-raw/_smoke/…`（DEEP_ARCHIVE）並驗證後刪除。
