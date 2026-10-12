# JMA 海洋・環境網格（C8＋附加）— collector `jma_ocean_env`

依據：ADR-0021（G 型、T0–T3）、`docs/JMA_RASTER_CONTRACT.md`、analytics `docs/topic-research/japan_opendata/jma-collectors-plan.md`（C8）。
狀態：2026-10-12 功能完成、單元測試與本機實打通過、真實 S3 smoke 通過；**未 push、未部署、Zeabur 未設 env**。

## 設計選擇

- **另開 collector，不擴充 `jma_raster`**：`jma_raster` 的前提是 bosai `targetTimes`、z6 日本畫布、幀與下鑽；這批四種來源都不同
  （GOOS 每日 ASCII 檔、UV 用 `updatetime_*.js`＋JST 06–18 正時、黃砂是整張預測圖且每日覆寫同檔名）。硬塞進 1,000 行的線上 collector
  風險大於收益。共用的部分照用：`weather_raster/decode.py`（解碼表、表外顏色計數）、`weather_raster/spool.py`
  （DayAccumulator、`write_hour_npz`、`pack_day_tar` 的 sha256 去重、`georef`）、`storage.s3.put_verified`、`jma_raster` 的季節解析。
- **不需新 migration**：寫 `live.weather_raster_frames`／`live.weather_raster_daily`（mig 437／438），`source='jma'`，
  product＝`sst_him`／`sst_mgd`／`sea_ice`／`uv_anal`／`kosa_fcst`。writer 與 `jma_raster` 共用轉換與 upsert（frames 狀態只升不降）。
- 間隔 60 分（UV 上游只留約 36h、每小時 20 分更新；其餘每日一次，每輪只打 1–2 個請求）。預設 `JAPAN_JMA_OCEAN_ENV_ENABLED=false`。

## 產品（2026-10-12 實測）

| product | 來源 | 格式／範圍 | 更新 | 上游保留 | 解碼 |
|---|---|---|---|---|---|
| sst_him | `goos/data/pub/JMA-product/him_sst_pac_D/{yyyy}/him_sst_pac_D{yyyymmdd}.txt` | ASCII 1+600 行×800 值×3 字元；0.1° 格心；0–60N、100E–180 | D 的檔約 D+1 10:00 JST | 2017-02 起全部 | 是 |
| sst_mgd | `…/mgd_sst_glb_D/{yyyy}/mgd_sst_glb_D{yyyymmdd}.txt.gz` | 1+720 行×1440 值；0.25° 全球 | 同上 | 1982 起全部 | 是 |
| sea_ice | `…/man_ice_okh_D/{yyyy}/man_ice_okh_D{yyyymmdd}.txt` | 28 byte 標頭＋601 行×1001 字元；0.05° **格點**；35–65N、115–165E；北→南（以北海道陸地位置驗證） | 每日；只有 12–5 月 | 2003 起 | 是 |
| uv_anal | `env/uvindex/tile/uv/uv_a/{UTC}/uvi_anal/{UTC}/surf/uvi_anal/{z}/{x}/{y}.png` | 4-bit P 圖磚；原生 z5–8（z≤4、z≥9 404） | 每正時 06–18 JST，約 :20 更新 | 約到前一日 06 JST（36–48h） | 是 |
| kosa_fcst | `env/kosa/fcst/img/surf/jp/{YYYYMMDDHHNN}_kosafcst-s_jp_jp.png`；清單 `list_js/s_jpjp_list.js` | 800×580 16-bit RGBA；解析 8 張（前一日 3h）＋預測 24 張（3h→6h） | 每日 1 次（約 06 JST，清單 Last-Modified 21:00 UTC） | 同名檔每日覆寫 | **否** |

- **UV 時刻**：`updatetime_uvi_anal.js` 的 `uayy/uamm/uadd/uahh` 是 **JST**（10-12 19:45 JST 時值為 18；若為 UTC 會是未來時刻），
  頁面 JS 用瀏覽器本地時間建 Date 再取 `getUTC*` 組 URL → **圖磚路徑是 UTC**。collector 不需讀 updatetime：直接以 06–18 JST 正時換成 UTC 試抓，
  第一磚 404＝尚未出爐。
- **UV 範圍**：z5 只有 6 磚在解析範圍內（28/12、27/12、27/13、28/11、28/13、26/13），其餘 404＝範圍外（不是缺測）；網格 z5 x26–28、y11–13（768×768）。
  解析格約 0.25°（z5 約 5 px／格，z6 約 10 px），收 z5 不失值；每日 78 磚。
- **UV 解碼表** `config/decode_tables/jma/uv_anal.yaml`：PLTE idx1–14 ＝ UVI 0…13+（凡例 `uvimg/uvi_colorbox.png` 逐格取色比對），idx0 透明＝範圍外。
  **編碼 level＝UVI＋1**，0＝範圍外（不是 UVI 0），255＝缺測／表外顏色（計數並 log 告警）。本機實打 21 幀表外顏色 0。
- **黃砂時刻是 JST**：檔名 `202610130300` 的圖上燒著「2026年10月13日 03時（予測）」；頁面 JS 也以瀏覽器本地時間比對。
  同檔名每日覆寫（Last-Modified 每天 21:00 UTC 更新），所以每次發布都要整批抓、以 sha256 判斷是否真的變了。
- **黃砂不解碼的原因**：凡例只有「濃↔薄」連續漸層、沒有數值刻度；圖疊在地形陰影底圖（含河川、經緯線、文字）上，16-bit RGBA。
  能做的只有「有無黃砂」的粗遮罩，沒有物理量，誤判風險高 → 只冷存原圖，manifest 記 `decoded=false` 與原因。需要時可從 T3 重算。

## 分層與 key

| 層 | sst_him／sst_mgd／sea_ice | uv_anal | kosa_fcst |
|---|---|---|---|
| T3 冷（DEEP_ARCHIVE，永久） | `weather-raw/jma/{p}/{YYYY}/{MM}/{YYYYMMDD}.tar.gz`（文字檔 gzip；mgd 原本就 .gz → `.tar`）：原檔位元組＋manifest.json（url、sha256、Last-Modified、解析統計） | `…/uv_anal/…/{YYYYMMDD}.tar`：`pack_day_tar`（磚 sha256 去重，blobs.pack＋manifest） | `…/kosa_fcst/…/{YYYYMMDD}.tar`（JST 發布日）：清單 js、issue.json、圖；同日重複圖 pack 內去重，**跨日相同圖只在 issue.json 記 `same_as`（day／issue／member）**，不重存 |
| T2 溫（STANDARD→全桶 IA／GIR，永久） | `weather-grid/jma/{p}/{YYYY}/{YYYYMMDD}.npz`：sst `sst_dc` int16（0.1℃）、ice `ice` uint8；`meta`（georef、缺值碼、raw_key、raw_sha256、統計） | `weather-grid/jma/uv_anal/{YYYY}/{YYYYMMDD}.npz`：13 幀每幀 uint8（`write_hour_npz` 格式，zero／same_as_prev 去重；用 `spool.load_hour_npz` 讀） | 無 |
| T1 熱 | **不做**：日檔本身就是日解析，T1 會是 T2 的複本 | `weather-daily/jma/uv_anal/{YYYY}/{YYYYMMDD}.npz`：`max_level`（日最大 UVI＋1）、`count_ge_{4,7,9,12}`（UVI≥3/6/8/11 的小時數）、`missing_count`、`valid_frames` | 無 |

缺值（皆不當 0）：SST `-32768` 陸地／無法決定（原 999）、`-32767` 海冰（原 888）、`-32766` 無法解析或超出 -3.0…40.0℃；
海冰 `0/2/3/4/5` 原級、`254` 陸地（原 9）、`255` 無解析／雲（原 7）或無法解析；UV 見上。

DB（只放狀態）：frames 每日檔 1 列（valid_time＝該日 00:00 JST）、UV 每幀 1 列、黃砂每次發布 1 列（valid_time＝清單 Last-Modified）；
daily 每產品每日 1 列（raw_key／sha256／grid_bytes／summary_key）。缺檔：日檔產品 404 超過 `MISSING_AFTER_DAYS`（預設 7）天記 frames `missing` 列，**不寫 daily 列**（daily 的 CHECK 沒有 missing；
看板以「frames missing＋無 daily」判讀；jma_raster 失敗日則寫 daily `failed`）；
UV 超出上游保留期（34h）記 missing，該日其他幀照常日結。

## 收集邏輯

- 日檔產品：從 `finalized_until`（首跑＝今天−`INITIAL_LOOKBACK_DAYS`）掃到昨天（JST），季外日子不打、不算缺；每輪每產品最多 `MAX_DAYS_PER_RUN`（4）個 200。
  成功一個就地完成 T3→T2→receipt（`weather_raw/jma/_receipts/{p}/{day}.json`）→刪 spool；上傳失敗不寫 receipt，下輪重試。
  上游有完整歷史：把 `JAPAN_JMA_OCEAN_ENV_INITIAL_LOOKBACK_DAYS` 設大（首跑前）即可逐輪回補（每小時 4 天／產品）。
- UV：保留期 34h 是由兩個探針推得的保守下界（37.75h 前的幀仍 200、49.75h 前 404），確切刪除時刻未驗證；
  服務中斷超過 34h 時，前一日早段的幀可能在上游仍在時就被記 missing。範圍磚 6 個由 10-12 單一時刻判定，collector 不偵測新磚出現。
- UV：保留期內所有 06–18 JST 正時，沒抓過就抓；抓到即解碼、寫幀 tar、網格、累加器；該日所有幀定案即日結（T2＋T1＋T3）。
- 黃砂：每輪抓清單（1 請求），內容 sha256 有變才抓清單上全部圖；JST 發布日 D 在 D+1 01:00 後日結。跨日 sha256 索引留 10 天。
- DB 列先放 state 的 pending，`require_db_write` 成功才清（照 jma_raster）。

## 本機實打（2026-10-12 19:45 JST，真上游、假 S3、不寫 DB）

| product | 實打 | 原始 bytes／日 | T3／日 | T2／日 | T1／日 | 請求／日（估） |
|---|---|---|---|---|---|---|
| sst_him | 10-09～10-11 3 檔 | 1,440,613 | 146 KB（tar.gz） | 144 KB | — | 約 11（昨日檔 00–10 JST 每小時 404 一次＋1） |
| sst_mgd | 10-09～10-11 3 檔 | 332,849（.gz） | 338 KB | 316 KB | — | 約 11 |
| sea_ice | 2026-02-26～03-01 4 檔 | 602,231 | 13–15 KB | 16 KB | — | 季內約 11，季外 0 |
| uv_anal | 21 幀（10-11 8 幀、10-12 13 幀）126 req | 32.6 KB（78 磚；去重後 26 KB／60 blob） | 41 KB | 40 KB | 14 KB | 78＋未出爐探測約 13 ≈ 90 |
| kosa_fcst | 1 次發布 32 張 33 req | 11.46 MB | 11.48 MB | — | — | 24（清單）＋32 ≈ 56 |

- 驗證：him 台灣東側 23.5N 122E 附近 28.5–28.7℃；海冰 2026-02-28 有冰格 29,336、陸地 327,957、無解析 197,640；
  UV 10-12 日最大 UVI 東京 5、札幌 3、那霸 2（當日多雲雨，待晴天再比對氣象廳地點值）。
- **峰值記憶體**：首跑全產品一輪（3+3 日檔、21 UV 幀、32 黃砂圖、2 日結）`/usr/bin/time -l` 最大 RSS **110 MB**（含 import 全部 collector registry），81 秒、167 請求。
  單產品 70–95 MB。目標 <200 MB 達成。
- **容量／月費（1 年後；DA US$0.00099、Standard 0.023、GIR 0.004 每 GB-月）**：
  T3 ≈ him 53 MB＋mgd 123 MB＋ice 2.5 MB＋uv 15 MB＋kosa 4.2 GB ≈ **4.4 GB/年 → DA 約 US$0.0044／月**；
  T2＋T1 ≈ 53＋115＋3＋15＋5 ≈ **0.19 GB/年 → Standard 約 US$0.0044／月，轉 GIR 後約 US$0.0008／月**。
  物件數約 5×365＋UV 2×365 ≈ 2,400／年（DA PUT＋轉換約 US$0.12／年；DA 每物件 40 KB 中繼資料計費可忽略）。
  合計 **每年累積 < US$0.01／月**；黃砂占 T3 的 95%。DB：每日約 17 frames＋5 daily 列。

## 真實 S3 smoke（2026-10-12）

`weather-raw/_smoke/jma_ocean_env_{sst_him_20261011.tar.gz, uv_anal_20261012.tar, kosa_fcst_20261012.tar}` 以 `put_verified` 上傳 DEEP_ARCHIVE，
head 驗 ContentLength、metadata sha256、StorageClass 皆符；再呼叫一次確認冪等（不重傳）；**已刪除**，head 回 None。

## 上線前（主 agent）

1. merge／部署 data-collectors（不需 migration；437／438 已在 DB）。
2. S3 lifecycle 加 `archive-deep-270d:jma_ocean_env`（Filter Prefix `jma_ocean_env/archives/`，270 天 DEEP_ARCHIVE），更新 `docs/AWS_INVENTORY.md`。
   `weather-raw/` 上傳時已直接 DA；`weather-grid/`、`weather-daily/` 沿用全桶規則（不可加到期刪除）。
3. Zeabur 設 `JAPAN_JMA_OCEAN_ENV_ENABLED=true`（單一實例）＋ restart。要回補歷史，首跑前設 `JAPAN_JMA_OCEAN_ENV_INITIAL_LOOKBACK_DAYS`。
4. 跨 repo 清冊（12 步之 9–11）：gis-platform `docs/data-inventory.md`、`docs/data-safety-inventory.md`；analytics `docs/data-sources.md`、
   `docs/api-platforms/jma/{endpoints.yaml,gotchas.md,datasets.yaml}`（UV 時刻 JST 已驗證、黃砂每日覆寫同檔名、UV 範圍外 404）。

## 待決定

- 黃砂只冷存不解碼（原因見上）；若需要「有無黃砂」粗遮罩，可之後從 T3 重算。
- UV 預報（`uv_f/{init}/uvi_f|uvic_f`）未收；要的話加產品即可（每日 2 次發布、每次多個預報時刻）。
- 歷史回補範圍（上游 SST 1982／2017 起、海冰 2003 起全在）：使用者原則是「近 3–5 年」，建議 2021-01-01 起，回補期間 T3＋T2 約 +1.3 GB。
- 黃砂是全年收（每年 4.2 GB／US$0.004 月），若只要春季可設 `JAPAN_JMA_OCEAN_ENV_SEASON=sea_ice=12,1,2,3,4,5,kosa_fcst=2,3,4,5`。
