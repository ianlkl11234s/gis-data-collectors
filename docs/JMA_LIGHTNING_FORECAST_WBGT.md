# JMA 落雷（C4）、府縣預報（C5）、環境省 WBGT（C7）

> 2026-10-12 實作。契約：taipei-gis-analytics `docs/topic-research/japan_opendata/jma-collectors-plan.md`、
> `docs/api-platforms/{jma,env_jp_wbgt}/`。DB：gis-platform migration 440（分支 `feat/jma-c457-tables`，**未 apply**）。
> 三個 collector 預設 disabled；migration apply 後設 Zeabur env `*_ENABLED=true` 並 restart 才生效。
> 狀態更新：2026-10-12 起三個 collector 在 production 啟用（`config/cross_layer_map.yaml` 標 enabled），上文「未 apply」是實作當下的紀錄。摘要見 [handbook/sources/jma-global.md](handbook/sources/jma-global.md)。

| collector | prefix | 預設間隔 | 寫入 | DB 保留 |
|---|---|---|---|---|
| `jma_lightning` | `JAPAN_JMA_LIGHTNING` | 5 分 | 既有 `live.lightning_events`（`source='jma'`） | 7 天（既有 cron `cleanup-lightning-events`） |
| `jma_forecast` | `JAPAN_JMA_FORECAST` | 360 分 | `live.jma_forecasts`、`live.jma_forecast_probabilities` | 90 天／30 天 |
| `env_jp_wbgt` | `JAPAN_ENV_WBGT` | 60 分 | `live.env_jp_wbgt_observations`、`live.env_jp_wbgt_forecasts` | 30 天／30 天 |

共通：state 只在 DB 寫入成功後前進（`require_db_write()`，jma_quake 模式）；只送新項目／變動；缺值存 NULL 或不寫列，
從不補 0；本地 JSON 由 ArchiveTask 每日歸檔到 S3 `{collector}/archives/`（冷資料）。

## C4 jma_lightning（LIDEN）

- 來源：`bosai/jmatile/data/nowc/targetTimes_N3.json` 找最新含 `liden` 的 validtime；
  幀 `nowc/{vt}/none/{vt}/surf/liden/data.geojson`（`Content-Encoding: gzip`；curl 要 `--compressed`）。
- **每幀＝validtime 前 5 分鐘的雷擊**，幀之間不重疊；vt 為 UTC 且 5 分對齊（非對齊 404）。
- **保留期**：targetTimes 只列約 3 小時，但伺服器實測留約 4–5 天（2026-10-07 00Z 仍 200、10-06 00Z 已 404）。
  collector 自產 5 分格點，補抓 `JAPAN_JMA_LIGHTNING_BACKFILL_HOURS`（預設 48h）內未完成的幀；每輪最多
  `JAPAN_JMA_LIGHTNING_MAX_FRAMES`（36）幀，最新 3 幀優先、其餘舊→新。空幀（200＋0 features）也標完成；
  404 重試 3 輪後記為上游缺幀。剛出爐未滿 `JAPAN_JMA_LIGHTNING_SETTLE_MINUTES`（10）分的幀下輪再抓一次。
- **type 語意**：`bosai/nowc/table/nowc.properties__*.xml` 的圖示規則 `type === 4 ? CG : CC`，圖例「対地放電（落雷）／雲放電」。
  實測（10-10～11 兩日 25,400 筆）只見 0（7,542）、1（5,646）、4（12,212）。0 與 1 的差別**官方未說明**（網頁都畫成雲放電）。
  寫入：`strike_type` 沿用表語意 0=雲對地／1=雲中 → 4→0、0／1→1、未知→NULL 並 log；`source_type` 存原值（mig 440 新欄）。
- 去重：上游 `id` 是幀內序號、不是雷擊 ID。`dedup_hash = md5(毫秒 UTC ISO|lon|lat|source_type)`，
  `event_id` 取 hash 前 15 hex（同 lightning_cwa）；UNIQUE (source, dedup_hash)／(source, event_id) DO NOTHING。
- 無電流強度（`intensity_ka` NULL）；座標 3 位小數。
- 已知小風險：targetTimes 失敗時以 now−10 分推估最新幀，若該幀其實還沒產生，404 三輪後會被記成上游缺幀而不再抓。
- `analytics.refresh_lightning_daily_summary` 依 source 分組、ST_Within 台灣縣市 → jma 列會聚成 `county=NULL, source='jma'`。

## C5 jma_forecast

- 偵測：`forecast/data/forecast/map.json`（56 筆，每府縣一代表 class10 區）→ 以 `common/const/area.json` 的 parent
  對到 office；比對 state `fc|{office}|{short|week}`，有變的 office 才抓 `forecast/{office}.json`（全區域、天氣文字、風浪、
  6 小時降水機率、氣溫地點；週間含信頼度與氣溫上下限）。014030 十勝、460040 奄美的 JSON 404，區域併在 014100／460100。
- 早期注意：`probability/data/probability/r8/map.json`，以府縣首區 code＋series 為 state 鍵。
- 一列＝(report_datetime, area_code, target_date, series)；`area_kind` area／amedas；短期 `pops_6h` 為 `{"HH": %}`，週間 `pop`。
- **NULL 規則**：上游空字串一律 NULL（週間第一天 pops／tempsMin 等）。短期 temps 的 timeDefines 是
  [今日09, 今日00, 明日00, 明日09]，00 時槽＝最低、09 時槽＝最高；**05 時發表也有「今日最低＝今日最高」的佔位**
  （2026-10-12 實測 171/171 站），不限 11 時 → 發表日當天最低＝最高時 `temp_min` 存 NULL。
- 早期注意 `levels` 原字串保留：'高'／'中'／''，'' ＝未達「中」（JMA 網頁「－」），**不是缺值**，所以不轉 NULL。
- raw office JSON 與 probability map（有變時）放 `raw_payload` → 只進本地 JSON → S3 冷存（含未入庫的 tempAverage／precipAverage）。
- 間隔：依任務設 360 分。JMA 定時發表 05／11／17 時，另有不定時更正（實測 map.json 出現 06／07／08 時的 reportDatetime）；
  6 小時輪詢會漏掉兩次發表之間被覆蓋的更正版。改 60 分只多 24×110 KB／日的 map.json 下載（office JSON 仍只在有變時抓），
  **建議主 agent／使用者決定是否改 `JAPAN_JMA_FORECAST_INTERVAL=60`**。

## C7 env_jp_wbgt（環境省）

- 實況 `est15WG/dl/wbgt_all_{yyyymm}.csv`：`Date,Time,{841 站}`，當月 1 日 1:00 到當日 24:00（未來時刻是空列）。
  預報 `prev15WG/dl/yohou_all.csv`：第 1 列 `,,YYYYMMDDHH…`（3 小時一格到翌々日 24 時），之後 `站號, 作成時刻, 值×10`（有前導空白）。
- 已驗證：純 ASCII（官方說明）、LF；**站號＝AMeDAS 站號**（實況 841、預報 841，全部在 amedastable.json）；時刻 JST，
  `24:00`／`…24` → 次日 00:00；預報 ÷10。
- 授權：環境省 `tos.php` 把「暑さ指数（WBGT）電子情報提供サービス」列為 **PDL1.0 適用外**；`data_service.php` 要求轉載到其他媒體時
  必須明記出典為環境省。對外發布前請使用者確認。
- 2026 服務期 4/22（4 月第 4 個週三）～10/21；`JAPAN_ENV_WBGT_SEASON_MONTHS`（預設 4–10，JST）以外不發請求；季內 404 不算錯。
- 「東祖谷」10/22 前用「京上」站號 71191 提供（官方說明）。841 站多數是實況推定值，實測只有 47 站。
- 只記變動：HTTP 帶 If-None-Match／If-Modified-Since（304 直接跳過）；state 記每個月檔每時刻列的 md5，列變了才送該列非空格；
  官方曾公告實況誤算出並回溯修正，所以 obs 表 `ON CONFLICT DO UPDATE … WHERE wbgt IS DISTINCT FROM`；
  DB 寫入失敗**不進 buffer**（`NO_BUFFER_COLLECTORS`），避免舊值晚於修正值 replay 把它蓋回去，靠 state 不前進重送。月初兩天也看上月檔。
  首跑每月只送最近 `JAPAN_ENV_WBGT_INITIAL_LOOKBACK_DAYS`（7）天。預報以作成時刻判斷新一期。
- 缺測：空格不寫列（不當 0）；預報陣列內空白為 NULL 元素。

## 實打數字（2026-10-12）

| 項目 | 數字 |
|---|---|
| 落雷 10-10／10-11（576 幀全 200） | 12,949／12,451 筆／日（秋季，非夏季下限） |
| 落雷本地 JSON | 1 幀 55 筆：24.4 KB → gzip 3.1 KB（約 440 B／筆 raw） |
| 預報首輪（112 區塊） | 1,650 預報列＋1,069 早期注意列；本地 JSON 2.89 MB → gzip 56 KB |
| WBGT 首輪 | 實況 141,288 格（7 天）＋預報 841 列；本地 JSON 28.0 MB → gzip 0.78 MB（只首輪） |
| WBGT 每小時 | 實況 841 格：135 KB → gzip 4.5 KB；新一期預報 841 列：936 KB → gzip 22 KB |
| WBGT 預報作成時刻 | 實測 08:25、16:25 JST（每日期數未完整觀察） |

## DB 容量推估（預演量測的每列大小含索引）

| 表 | 列／日 | B／列 | 每日 | 穩態（保留期） |
|---|---|---|---|---|
| lightning_events（jma） | 約 12,700（秋）；夏季可能數倍 | 約 250（估） | 約 3 MB | 7 天約 22 MB（夏季可能 100 MB 級） |
| jma_forecasts | 約 4,100（短期 3 次＋週間 2 次） | 348 | 1.4 MB | 90 天約 130 MB |
| jma_forecast_probabilities | 約 2,900 | 375 | 1.1 MB | 30 天約 33 MB |
| env_jp_wbgt_observations | 20,184（841×24，服務期） | 147 | 3.0 MB | 30 天約 90 MB |
| env_jp_wbgt_forecasts | 841×每日期數（≥2） | 約 450 | ≥0.8 MB | 30 天 ≥25 MB |

## migration 440 預演（BEGIN…ROLLBACK，2026-10-12）

- migration 本體 4 秒；以實際 writer SQL 寫入 55 筆 jma 落雷（source_type 0/1/4 → strike_type 1/1/0）、
  1,479 預報＋1,069 早期注意、841 WBGT 預報＋3,000 實況；同批重送無新增（DO NOTHING／DISTINCT FROM）。
- 4 表 RLS 開、anon／authenticated 只有 SELECT grant，view REVOKE ALL 後只 GRANT SELECT；`SET ROLE anon` 讀 4 個 public view 正常（WBGT view 841 站全有座標）；cron 4 支、retention_policies 4 列。
- `metadata.check_retention_coverage()` 與 apply 前相同（3 筆既有項，無新增）。曾試在 440 補登記 `lightning_events`，
  反而觸發「no active pg_cron cleanup job references this table」（既有 cron 呼叫專用函式），已移除，另案處理。
- ⚠ migration 對 `live.lightning_events` 做 ADD COLUMN（ACCESS EXCLUSIVE，瞬間）；apply 時不要在同一 transaction 做慢操作。

## 上線步驟（主 agent）

1. apply gis-platform migration 440（`psql "$DATABASE_POOL_URL" -v ON_ERROR_STOP=1 -f migrations/440_jma_lightning_forecast_wbgt.sql`），APPLIED.md 登記。
2. merge data-collectors 分支 `feat/jma-lightning-forecast-wbgt`（註冊檔與其他 agent 同時新增條目，合併時保留雙方）。
3. S3 lifecycle：補 `jma_lightning/archives/`、`jma_forecast/archives/`、`env_jp_wbgt/archives/` 三條 `archive-deep-270d:{collector}` 規則，並更新 `docs/AWS_INVENTORY.md`。
4. Zeabur env：`JAPAN_JMA_LIGHTNING_ENABLED`、`JAPAN_JMA_FORECAST_ENABLED`、`JAPAN_ENV_WBGT_ENABLED`＝true，restart。
5. taipei-gis-analytics：`docs/api-platforms/env_jp_wbgt/`（授權、站號、編碼已驗證）、`jma/gotchas.md`（liden type、保留期 4–5 天、
   05 時也有今日最低佔位）、`docs/data-sources.md` 同步。
