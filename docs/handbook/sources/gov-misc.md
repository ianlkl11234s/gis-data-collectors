# 政府雜項與其他：急診、台電、新聞、國際事件、股市、農產價格、共機、公衛、移民、矯正、交通事故、動保、圖書館、垃圾車、POI

**一句話**：這一類來源各自獨立、格式五花八門（HTML、CSV、XML、RSS、LLM 抽取），沒有共用機制可依賴；最大的坑是「低頻、事件型或只在特定時段有資料」的來源停擺沒人發現，以及政府網站擋雲端 IP，必須改跑 HiCloud VM。

清單與寫入表以 [collectors.md](../collectors.md) 為準。上游細節能連的都連到 taipei-gis-analytics（private）`docs/api-platforms/`：`taipower/`、`cdc_tw/`、`immigration/`、`npa/`、`tpml/`、`moa/`、`moj/`、`huggingface/`；健保署、國防部、TWSE、GDELT、YouTube 目前沒有對應頁。

## 共通機制

- **擋雲端 IP 的政府網站跑 HiCloud VM**：ConnectTimeout（不是 403／404）優先懷疑 IP 被擋。高雄、臺南、航港局、移民署、疾管署都擋雲端 ASN。做法：程式在主 repo 保留完整實作（transform 與 schema 的單一真相），production 改由 `external/<name>_vm/` 的 VM 版 cron 跑，Zeabur 端 `*_ENABLED` 維持 false。VM 是**手動 scp 部署**，git push 不會生效；VM 寫入繞過主程式的 writer，**心跳不可信**，健康要直接看表。部署與監控方式見 [EXTERNAL_COLLECTORS.md](../../EXTERNAL_COLLECTORS.md)（主機以 `<VM_HOST>` 表示）。換機房前要從新機實際 curl 目標 API，不能只測連通。
- **完整快照型 collector**：失敗時要回 `_collector_error` 讓本輪算失敗（`collectors/base.py`），不能用空清單假裝成功；動保系列只有完整成功才呼叫 finalize RPC 更新 current。
- **0 筆是合法結果**：沒共機、沒新事故、休市、閉館都會是 0；用 ledger 表或第二來源判斷健康，不要用「這輪有沒有資料」（本 repo `.claude/pitfalls/2026-08-07-silent-upstream-outage.md`，同日查出台電落雷、警政署 A1、共機航跡三支同病）。
- **改解析邏輯等於改資料契約**：回填要和部署一起排，否則線上舊版 collector 會把修好的資料蓋回去（共機 `raw_text` 曾因此反覆被截斷；mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-01/02）。判斷新版有沒有生效，看「只有新版才會產生的內容特徵」，不看 `updated_at`。

## 醫療、公衛、社會

### er_hospital_realtime
健保署重度級急診即時量能。每 15 分鐘，Zeabur；寫 `live.er_hospital_status`、`live.er_hospital_current`（`current_touch_updated_at`：每輪 touch 當心跳）。上游沒有歷史，我們不收就沒有；憑證缺 SKI，`verify=False`。

### cdc_public_health_weekly
疾管署公衛週報 CSV（流感、登革熱、腸病毒）。

- 頻率／位置：**跑在 HiCloud VM**（od.cdc.gov.tw 從 Zeabur 連線逾時），每週四 11:00 一次；Zeabur 端必須 false。repo 預設間隔 360 分只是程式預設，清冊監控門檻 7 日。
- 寫入：`live.public_health_weekly`；upsert 是 `DO UPDATE`。
- 已知坑：唯一約束曾包含恆為 NULL 的 `is_imported` 欄，`NULL <> NULL` 讓 `ON CONFLICT DO NOTHING` 永遠不觸發，174,002 列對 14,730 個唯一鍵，數字被灌水約 13 倍。修正時去重保留 `collected_at` 最新的一筆，因為 CDC **會回修數字**（原註解寫「不會回修」是錯的），並改成 `DO UPDATE`（mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-21/22）。設計唯一鍵時要確認每個欄位都不會是 NULL，見 [new-collector.md](../new-collector.md)。VM 的 buffer 保留比其他 VM collector 長（週資料）。

### correctional_daily_snapshot
法務部矯正署每日收容（`today.xml`，全國總計一天一列）。每日，Zeabur；寫 `live.prison_population_daily`（`upsert_strategy=update`）。曾經只有一列測試資料、從未排程，直到 2026-07-03 才正式啟用：migration 存在不代表有資料，上線要看 DB 實際列數。

### immigration_apis_airport
移民署機場入出境 APIS（6–8 個端點）。

- 頻率／位置：**跑在 HiCloud VM**，每小時 :07；Zeabur 端維持 false（移民署擋國際雲商 IP）。
- 寫入：`live.border_airport_snapshot`。上游沒有時間戳。

### npa_traffic_accident_a1
警政署即時 A1 交通事故（24 小時內死亡）。

- 頻率／位置：每 12 小時，Zeabur。
- 寫入：`live.traffic_accidents_a1`；上游是年度累積 feed，以 hash 去重，沒有新事故的日子不會有新列。
- 已知坑：**一列是一個當事人，不是一起事故**，下游算件數要去重。2026-08-07 查出曾停約 6 週而沒有告警（與台電落雷同一天查出）。

### animal_adoption
農業部待認領養動物完整快照。每日，HiCloud VM（2026-10-23 起；`data.moa.gov.tw` 擋 Zeabur，見 [`external/moa_opendata_vm/`](../../../external/moa_opendata_vm/README.md)）；寫 `live.animal_adoption_snapshot_runs`、`animal_adoption_snapshots`、`animal_adoption_current` 與 `analytics.animal_adoption_daily`。只有完整成功的 run 才呼叫 `live.finalize_animal_adoption_snapshot` 更新 current 與每日彙總；raw JSON 留本地，由 archive 每日上 S3。`COLLECT_TIMEOUT` 120 秒。

### animal_shelter_outcomes
農業部收容成果月報（datagov 41236，整份歷史快照）。每 30 日，VM（同上）；寫 `live.animal_shelter_outcome_runs`、`live.animal_shelter_outcomes`，完整 root array 才 finalize。與 `animal_shelter_pressure` 共用表。

### animal_shelter_pressure
農業部收容壓力／去向月報（datagov 73396）。每 30 日，VM（同上）；與 `animal_shelter_outcomes` 共用表，保留同月同縣市的 revision。

### animal_veterinary_clinics
農業部獸醫開業執照（datagov 8705）。每 180 日，VM（同上）；寫 `live.animal_welfare_point_runs`、`live.animal_welfare_point_snapshots`（三支動保點位共用）。**不寫負責獸醫姓名**。

### animal_licensed_pet_businesses
農業部合法特定寵物業（datagov 97070）。每 365 日，VM（同上）；共用動保點位表。**不寫負責人姓名**（`own_name`、`bos_name`）。

### animal_protection_offices
農業部動物保護機關（datagov 134283，22 縣市）。每 90 日，VM（同上）；共用動保點位表。

動保點位三支的頻率很低（90–365 日），表級新鮮度門檻也跟著拉長；它們共用表，一支停掉要很久才會從表上看出來。

## 能源、市場、價格

### power_taipower
台電即時電力（系統供需、各機組、區域用電三個端點）。每 10 分鐘，Zeabur；寫 `live.power_system_status`、`live.power_generation_unit`、`live.power_region_demand`（多表）。免金鑰；憑證缺 SKI。

### twse_market_index
證交所加權指數（MIS `getStockInfo`）。每 1 分鐘，Zeabur；寫 `live.market_index_tick`（tick 表大，`MAX()` 貴，由 current 代監控）、`live.market_index_current`。需帶 Referer header；**只有盤中有資料**，清冊門檻放寬到 1 日（3 倍＝3 天，蓋過週末），連假仍可能誤報屬預期。

### food_prices
農業部四類批發價（蔬果、漁產、毛豬、家禽）。

- 頻率／位置：每日，HiCloud VM（2026-10-23 起；`data.moa.gov.tw` 擋 Zeabur，見 [`external/moa_opendata_vm/`](../../../external/moa_opendata_vm/README.md)）（上游 T+1 更新）。
- 寫入：`live.food_price_daily`。
- 已知坑：四支 API 的分頁與日期規則互不相容，而且誤用不報錯：蔬果單次上限 1,000 筆、`Page` 參數無效、`Next:true` 是假訊號，要用「市場 × 類別 × 自適應時間窗遞迴切分」；數值欄會混入「休市」字串，要當成無觀測，不是 0；土雞資料集 2014-03 起停更但仍回 200 加 1,253 筆舊資料。**不可共用 helper**。2026-08-03 併入時漏加清冊，CI 紅了四天。詳細：gis-wiki `inbox/2026-08-06-…四套互不相容的API規則.md`、analytics `docs/topic-research/food_prices/`。

## 新聞與情勢

### news_events
新聞事件（RSS 29 個 feed＋LLM 地名抽取與相關性評估）。

- 頻率／位置：每 10 分鐘，Zeabur；`COLLECT_TIMEOUT` 600 秒。
- 寫入：`live.news_events`；simhash 去重。
- 守門：LLM 金鑰依 `NEWS_EVENTS_LLM_PROVIDER` 在執行期驗證（所以 registry 不宣告 `required_env`，避免 OpenRouter 被 Gemini 的檢查擋掉）。
- LLM 標註逐批（15 則）處理：單批失敗（API 錯誤、JSON 壞掉、缺 idx、欄位不合法）只跳過該批，該批不入庫、下一輪重送；全部批次失敗才整輪失敗。log 的「⚠ LLM 標註 N 批失敗」帶原因類別；結果欄 `llm_failed_batches`、`llm_skipped_items`（I-05）。
- 已知坑：2026-10-16 OpenRouter 額度用完停 4 天，加值後又被舊的全有全無邏輯卡 9 小時；10/17–10/20 的新聞已從 RSS 消失，補不回來（[pitfall](../../../.claude/pitfalls/2026-10-21-news-llm-all-or-nothing.md)）。額度不足時 OpenRouter 回 402。

### global_events
GDELT masterfilelist（只取 metadata）＋OpenRouter Qwen 第一階段評估。

- 頻率／位置：每小時，Zeabur；`COLLECT_TIMEOUT` 1800 秒。需 `OPENROUTER_API_KEY`。
- 寫入：`intel` schema 的 `global_event_collector_batches`、`global_event_collector_run_receipts`、`global_event_candidate_observations`（表級監控看後兩張）；pending TTL 48 小時，本地 raw 保留 72 小時；handoff 檔上 S3 `global_events/handoff/`。
- 已知坑：
  - 2026-09-05 前清冊把它標 `enabled: false`，結果 pipeline 停滯 29 小時，日報完全沒掃到。清冊的 enabled 要反映 production 實況。
  - 國際新聞標題可能合法含西里爾字母，不可放進寫入層亂碼防護名單（見 [cwa-ncdr.md#ncdr_alerts](cwa-ncdr.md#ncdr_alerts)）。
  - 固定 publisher、GitHub archive 與 production gate 尚未完成，handoff 維持 `production_publishable=false`（清冊 notes）。
- 調查（時點性）：[地理品質調查](../../investigations/GLOBAL_EVENTS_GEO_QUALITY.md)、[prompt v4 評測](../../investigations/GLOBAL_EVENTS_PROMPT_V4_EVAL.md)。後者引用的 `scripts/eval_global_event_prompt_ab.py` 不在 repo 內。

### yt_live_video_resolver
YouTube 14 家新聞台直播 videoId（YouTube Data API v3）。

- 頻率／位置：每 5 分鐘，Zeabur。需 `YOUTUBE_API_KEY`。
- 寫入：`live.yt_live_history`（由 current 代監控）、`live.yt_live_current`。
- 守門：**sticky 寫入**：API 失敗（網路、5xx、配額）一律保留上次的 video_id，只寫 `last_error`；只有確定下播且搜不到替代、或連續 48 小時無法驗證才清空。`search.list` 是獨立配額桶（每專案每日 100 次），collector 自設 80 次上限。
- 已知坑：2026-07-26 前是 HTML 爬取，被反爬蟲擋到成功率約 0.2%，而且失敗時清空 video_id，結果 24 小時直播一天只有幾分鐘有值。這是「現值表失敗時不可清空有效值」原則的反例，見 [principles.md](../principles.md)。

### pla_activity_daily
國防部「中共解放軍臺海周邊海、空域動態」每日通報（mnd.gov.tw HTML）。

- 頻率／位置：每 30 分鐘檢查當天有沒有發布（約 08–10 點），Zeabur；清冊門檻 1 日。
- 寫入：`live.pla_activity_daily`（PK＝report_date，upsert）。
- 守門：現行句型＋舊句型 fallback regex；未逾越中線時原文沒有「逾越中線」字樣，記為 0 逾越。用官方語彙，不用「擾台」。
- 已知坑：`raw_text` 曾被截在 2,000 字，修好後又被線上舊版覆蓋回去（見共通機制「改解析邏輯等於改資料契約」）。

### pla_tracks_vectorize
共機航跡示意圖向量化（CV＋OCR，不是 API）。

- 頻率／位置：每日，Zeabur；`COLLECT_TIMEOUT` 1200 秒。需要系統 tesseract（Dockerfile）。
- 寫入：`spatial.pla_tracks`、`live.pla_activity_items`、`spatial.pla_tracks_runs`（ledger）；原圖上 S3 `pla/track_charts/`。
- 守門：**監控看 ledger `spatial.pla_tracks_runs`，不看 `pla_tracks`**：「共機 0 架次」那天 `pla_tracks` 沒有任何列是合法結果，拿它當心跳會把「沒共機」誤判成「沒跑」（`tests/test_cross_layer_sync.py` 豁免清單有說明）。
- 已知坑：2026-08-06 前是 analytics 的手動批次腳本，曾斷 5 天沒告警。

## 地方服務

### tpml_seat
臺北市立圖書館座位即時（6 分館 29 區）。每 10 分鐘，Zeabur；寫 `live.tpml_seat_status`、`live.tpml_seat_current`。來源沒有時間戳，`observed_at` 用收集時刻；閉館時全部是 0，記 `is_closed=true` 而不是當成「沒人」；憑證缺 SKI。

### waste_positions
垃圾車 GPS（高雄、新北、臺南）。

- 頻率／位置：**跑在 HiCloud VM**（高雄、臺南擋雲端 IP），每 2 分鐘、01:00–06:00 跳過；Zeabur 端必須 false。critical。
- 寫入：`spatial.waste_positions_realtime`。
- 守門：**冪等寫入（2026-10-15，#148）**：來源每次回傳最近 N 分鐘的 GPS，2 分鐘輪詢會和上一輪的時間窗重疊，同一城市重複抓到同一點（臺南約 2.5 倍），不是跨城市重複。VM 版 INSERT 改成**不指定目標**的 `ON CONFLICT DO NOTHING`，搭配 gis-platform migration 458 的 `(city, vehicle_no, observed_at)` 唯一索引。**部署順序：先部署 collector，再建唯一索引**；反過來的話，舊版含重複點的整批 INSERT 會撞唯一鍵失敗，資料卡在 buffer 一直重送。VM 部署時保留舊檔備份（`.bak_<日期>`），選離峰時段。
- 已知坑：VM 寫入繞過主程式 writer，心跳停在 06 月但表有當天資料（2026-10-05 調查）；VM 部署說明在 `external/waste_positions_vm/README.md`。

### waste_match
垃圾車軌跡 OSRM map-matching（讀 `spatial.waste_positions_realtime`，輸出每日預聚合）。清冊標停用。需 `OSRM_TOKEN`（舊 token 曾寫死在公開 repo，2026-10-05 已更換，現在一律讀環境變數，沒設就報錯）；用獨立連線。

- 已知坑：「寫成功才標記」的批次，NoMatch 或低信心的 trip 不會寫結果，下一輪又被當成未處理，每天剛好 LIMIT 筆永遠重試；gis-platform migration 075 加 `waste_match_attempts` 記錄嘗試（mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-05-09）。

### foursquare_poi
Foursquare 開放 POI（Hugging Face `foursquare/fsq-os-places`，DuckDB 讀遠端 Parquet）。每 30 日，清冊標停用；需 `HF_TOKEN`；寫 `reference.foursquare_poi`（約 42 萬列）。

## 收這類資料要注意

- ConnectTimeout 先懷疑 IP 被擋；擋雲端 IP 的來源放 VM，主 repo 保留完整實作，Zeabur 端保持關閉，VM 用手動部署並另外看表確認健康。
- VM 寫入要冪等；要加唯一索引時先部署 collector、再建索引。
- 唯一鍵不要包含可能是 NULL 的欄位；上游會回修的資料用 `DO UPDATE` 保留最新。
- 只在特定時段有資料（盤中、開館、季節、事件）的來源，要在清冊 notes 寫清楚預期的空窗，並放寬監控門檻。
- 「0 筆是合法結果」的來源要有 ledger 表當心跳（pla_tracks_runs 是範例）。
- 現值表（video_id 這類）失敗時保留舊值，搭配 TTL，不可清空。
- 個資欄位（負責人、獸醫姓名）不寫進 DB。
- 清冊的 enabled 要反映 production 實況，否則日報不會掃它（global_events 29 小時事件）。
