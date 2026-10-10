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
| himawari_b13 | B/himawari/data/satimg/{bt}/fd/{vt}/B13/TBB/{z}/{x}/{y}.jpg | ~119h | 每 1 小時 1 張 | 否（只留原始） |
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
| T2 溫 | 每小時級距網格（該小時所有幀 stack，np.savez_compressed） | S3 `weather-grid/jma/{product}/{YYYYMMDD}/{HH}.npz` | STANDARD | 90 天（主 agent 加 lifecycle expiration） |
| T1 熱 | 每日彙總：count_ge_{k}（uint16，k 依產品，radar 取 1、2、4 級＝有回波、≥1mm/h、≥10mm/h）、max_level（uint8）、valid_frames（標量）＋ georef JSON | S3 `weather-daily/jma/{product}/{YYYY}/{YYYYMMDD}.npz` | STANDARD | 永久 |
- 日期以 JST 切日。日結：JST 日期 D 在 D+1 01:00 之後、且該日上游已不可能補抓時（或幀已齊）打包；上傳後 head 驗 ContentLength＋自訂 metadata sha256，成功才刪 spool。
- 累加器用 np.memmap 放 spool，跨重啟續算，不吃 RAM。單輪峰值記憶體目標 < 300 MB。
- 不可每張圖磚一個物件（Deep Archive 每千物件 US$0.05）。

## DB（gis-platform mig 437，live schema）
- `live.weather_raster_frames`：source text, product text, valid_time timestamptz, base_time timestamptz, zoom_raw smallint, tiles_requested int, tiles_nonempty int, raw_bytes bigint, nonzero_pixels int, unknown_color_pixels int, status text（ok／partial／missing）, collected_at timestamptz default now()；PK (source, product, valid_time)；DO NOTHING；保留 400 天。
- `live.weather_raster_daily`：source, product, obs_date date, frames_expected int, frames_ok int, raw_key text, raw_bytes bigint, raw_sha256 text, raw_storage_class text, grid_objects int, grid_bytes bigint, summary_key text, decode_table_version text, status text（packed／uploaded／verified／failed）, uploaded_at timestamptz；PK (source, product, obs_date)；upsert 更新；永久。
- public views：`public.weather_raster_daily`、`public.weather_raster_frames_recent`（近 48h）。RLS＋anon SELECT（照 418／435）。
- 這兩張表就是「資料狀態」看板的來源。

## 驗收
- 單元測試：解碼（含表外顏色）、spool→tar→manifest、日結判斷、累加器跨重啟。
- 本機實打：radar 抓 ≥1 小時（12 幀）、其他各產品 ≥1 幀，回報每幀請求數、raw bytes、npz bytes、耗時、峰值記憶體；用這些數字推估每日／每年 T1–T3 容量與 S3 月費。
- 一次真實 S3 上傳到 `weather-raw/_smoke/…`（DEEP_ARCHIVE）並驗證後刪除。
