# TDX 交通類：公車、台鐵、國道、路況、停車、YouBike

**一句話**：這一類全部打交通部 TDX，所有 collector 共用同一組金鑰（`TDX_APP_ID`／`TDX_APP_KEY`）和同一個全域節流器；最大的坑是「一支壞全部倒」：金鑰被平台重置時十幾支 collector 同時 401，瞬間併發則同時 429。

清單、頻率、寫入表以自動產生的 [collectors.md](../collectors.md) 為準；本頁只寫「我們怎麼收」與踩過的雷。上游 API 本身的行為（端點、欄位、即時缺口）在 taipei-gis-analytics（private）`docs/api-platforms/tdx/`（`gotchas.md`、`realtime_gaps.md`、`endpoints.yaml`），這裡不重抄。

## 共通機制

### 全域節流：`TDXSession` ＋ `utils/rate_limiter.py`

- **為什麼**：2026-04-16 公車從 6 都擴到 22 縣市，部署後 17 個縣市和 `tra_train` 同時 429。TDX 的限制是每把金鑰約 5 req/s，而我們的問題從來不是總量（全部 TDX collector 平均約 0.2 req/s），是瞬間 burst：collector 內部平行抓、多支 collector 共用出口 IP、整數分鐘觸發時同一秒擠在一起、token refresh 也算請求。只有 process 層級的全域節流能根治。
- **做法**：所有 TDX 請求走 `utils/tdx_session.TDXSession`（`requests.Session` 子類），每次 request 先向 `get_tdx_rate_limiter()` 取號；全 process 共用一個 singleton，預設 `TDX_RATE_LIMIT=4`（req/s，比上限 5 留 1 的餘裕）。要更保守就把環境變數調成 3，重啟即可，不必改程式。
- **測試**：`tests/test_rate_limiter.py`、`tests/test_tdx_session.py`（多執行緒 burst 不超速、跨 session 共用 limiter、singleton）。改節流邏輯前先跑這兩支。

### 新增 TDX collector 的三條硬規則

1. session 一律 `TDXSession()`，不可用裸 `requests.Session()`（會繞過節流）。
2. 每個 HTTP 呼叫都帶 `timeout=config.REQUEST_TIMEOUT`。
3. 內部 `ThreadPoolExecutor` 不要開大：反正都被節流到 4 req/s，`max_workers` 設 2–5 就好，開更大只增加 context switch。

上線前先算配額：`每輪請求數 × (60 ÷ 間隔分鐘) = 每小時請求數`，和既有的公車（22 城、每 2 分鐘，約 660 次／小時，是最大戶）加總看看。完整上線步驟見 [new-collector.md](../new-collector.md)。

### 429 和 401 是兩回事

| | 429 Too Many Requests | 401（出現在 token 端點） |
|---|---|---|
| 位置 | 業務 API（例如 `/TrainLiveBoard`） | `/auth/realms/TDXConnect/protocol/openid-connect/token` |
| 性質 | 流量問題，會自己好 | 認證問題，降頻、等待都不會好 |
| 先查 | 是不是有 collector 沒用 `TDXSession`（`rg "requests.Session()" collectors/`）；`get_tdx_rate_limiter().get_stats()` 的 `avg_wait_ms` 接近 250 ms 代表節流一直在擋，可調低 `TDX_RATE_LIMIT` | token 端點回應 body 的 `error` 欄位（見下表） |

**關鍵陷阱**：429 可能只是 401 的連鎖症狀。token 壞掉時每支 collector 每輪都重打 token 端點，形成 retry storm，其中一部分被節流擋成 429。看到「token 端點 401 不會恢復＋業務端點偶有 429」時，先當認證問題處理，不要往「金鑰配額不夠」查。

Keycloak 回應判讀（`raise_for_status()` 會吞掉 body；2026-08-30 起 `utils/auth.py` 已把 body 截斷 200 字寫進例外訊息，直接看 log）：

| `error` | 意義 | 處置 |
|---|---|---|
| `invalid_client` | `TDX_APP_ID` 不存在或打錯 | 檢查環境變數有沒有誤植 |
| `unauthorized_client`＋`Invalid client secret` | client_id 在，但 secret 對不上，通常是 TDX 平台端重置了 secret | 到 TDX 會員中心取新 secret |
| 訊息含 disabled／suspended | 金鑰或帳號被停用 | 會員中心確認或聯繫 TDX |

修復步驟：取新 secret → 更新本機與 Zeabur 的 `TDX_APP_KEY` → **restart／redeploy**（Zeabur 改 env 不會自動生效）→ 三件事都要綠才算修好：runtime log 首輪成功、對應表在重啟後有新列、告警停止。排障時不要把 secret 印出來；Zeabur CLI 會回顯變數值的注意事項見 [operations.md](../operations.md)。

### 認證層的程式防護（2026-08-30 起）

- token 端點錯誤帶完整 body（`utils/auth.py`）。
- 全域 token 失敗 backoff：所有 `TDXAuth` 共用 class-level backoff（60 s 起、倍增、上限 600 s；429 帶 `Retry-After` 時照伺服器要求），backoff 期間直接 fail fast，止住 retry storm。
- 資料 API 回 401 時自動作廢 token、重取、重新取號後重試一次（`utils/tdx_session.py`），不再抱著被撤銷的 token 空轉到本地過期。
- 告警指數收斂（`utils/notify.py`）：連續錯誤只在第 3、6、12、24… 次發 Telegram，恢復時發一則。
- 測試：`tests/test_auth.py`、`tests/test_notify.py`。事故長文：[2026-08-30 TDX secret 被重置](../../../.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md)（16 支 collector 連續 15.4 小時 401）。

### 上游資料的共通怪癖（細節在 analytics `tdx/gotchas.md`）

- `LastUpdateTime` 常常只是 API 快取刷新時間，不代表內容有變：路況事件實測 71% 的「更新」內容根本沒變。去重要比對實質欄位，不能看這個時間。
- (0,0) 座標要在寫入前濾掉（國道曾有 435 筆）。
- TDX 回的 `speed` 可能是車機記憶值：車停著也回報時速 30 km/h（見下方 bus 夜間暫停的分析）。

## collector

### bus
市區公車即時位置（A1 定頻，22 縣市）。

- 來源：TDX 市區公車 `RealTimeByFrequency`；城市清單 `BUS_CITIES`。
- 頻率／位置：每 2 分鐘，Zeabur。critical。
- 寫入：`live.bus_positions`（history）、`live.bus_current`（current）。
- 守門：**夜間暫停**（下節）；`COLLECT_TIMEOUT` 120 秒。
- 已知坑：TDX 配額最大戶（2026-04 時約佔全部 TDX 呼叫的 92%）；新增城市前先算配額。
- 上游細節：analytics `docs/api-platforms/tdx/`。

#### 夜間暫停（2026-04-24 起）

規則：台北時間 00:00–04:59 只抓 5 都（臺北、新北、臺中、臺南、高雄），其餘 17 縣市暫停；程式在 `collectors/bus.py` 的 `NIGHT_PAUSE_HOURS`、`NIGHT_KEEP_CITIES`，暫停的城市在 stats 記 `skipped: night_pause`。

- **為什麼**：當時 TDX 免費方案 10,000 次／日，bus 一支就用掉約 15,840 次。目標是不動頻率，只停掉「打了也沒意義」的時段。
- **怎麼判斷沒意義**：不能用 `speed > 0`（車停在原地仍回報 30 km/h，是車機記憶值）；改用 4 小時內實際位移：< 300 m 視為停駛（GPS 漂移）、> 2 km 才是真的在跑。過去 14 天凌晨 01–04 時，17 縣市大多是停駛，5 都仍有實際運行。
- **效果**：每日約省 2,550 次（約 16%）。
- 進一步可省但未做（避免一次改太多）：偏鄉縣市日間降到 10 分鐘、澎湖金門全日降頻、臺南 01–03 時也暫停。
- 分析原始記錄：commit `938c55f3`；舊檔 [`docs/BUS_NIGHT_PAUSE.md`](../../BUS_NIGHT_PAUSE.md) 已改為指向本節。

### bus_intercity
公路／國道客運即時位置（全台單一端點）。

- 來源：TDX InterCity A1。
- 頻率／位置：每 2 分鐘，Zeabur。
- 寫入：`live.bus_intercity_positions`、`live.bus_intercity_current`。
- 守門：只留營運中的車（`DutyStatus=1` 且 `BusStatus=0`）。
- 已知坑：無特別紀錄。

### tourist_shuttle
台灣好行即時位置（全國單一端點）。

- 來源：TDX 台灣好行 A1。
- 頻率／位置：每 2 分鐘，Zeabur。
- 寫入：`live.tourist_shuttle_positions`、`live.tourist_shuttle_current`。歷史表不在表級監控內，由 current 代監控（`tests/test_cross_layer_sync.py` 的豁免清單）。

### youbike
公共自行車即時車位（全台 12 縣市）。

- 來源：TDX Bike Availability；城市清單 `YOUBIKE_CITIES`（2026-06 實測有 YouBike／Moovo 站點的 12 縣市）。
- 頻率／位置：每 10 分鐘（2026-06 從 15 分改 10 分），Zeabur。critical。
- 寫入：`live.youbike_snapshots`、`live.youbike_current`。

### tra_train
台鐵列車即時位置（TrainLiveBoard）。

- 頻率／位置：每 2 分鐘，Zeabur。critical。
- 寫入：只有 history `live.train_positions`，沒有 current 表。
- 已知坑：2026-04-16 曾和 bus 一起被 429（全域節流的起因之一）。

### tra_static
台鐵靜態資料（車站、路線、站序、車種）。

- 頻率／位置：每日，Zeabur。
- 寫入：**不寫 DB**（`TABLE_MAP` 沒有條目），只存本地 JSON → 每日 S3 `tra_static/archives/`。
- 已知坑：**沒有表級監控**。它停了日報不會發現，只能看 S3 archive 有沒有每日新檔。

### rail_timetable
台鐵＋高鐵每日時刻表（DailyTimetable）。

- 頻率／位置：每 4 小時（當日班表會日內修訂），Zeabur。
- 寫入：`reference.daily_schedules`（`TABLE_MAP` 的 `is_reference`，走 writer 的 `_write_schedules`）。另由每日 07:00 的 `mini_taipei_publish` 轉成 Mini Taipei 前端格式上 S3。
- 守門：台鐵抓失敗時以 `tra_daily` key 的 fallback 加 degraded 標記寫入，**degraded 不得覆蓋 healthy**（TD-1；`tests/test_supabase_writer.py` 有測）。證據：[`docs/investigations/td1-evidence.md`](../../investigations/td1-evidence.md)。

### freeway_vd
國道 VD 與路段壅塞。

- 頻率／位置：每 10 分鐘，Zeabur。critical。
- 寫入：`live.freeway_sections`、`live.freeway_sections_current`、`live.freeway_vd_traffic`（多表寫入）。
- 已知坑：(0,0) 座標要濾（見共通怪癖）。

### vd
舊版 VD collector，已被 `freeway_vd` 取代，清冊標停用，不寫 DB。保留程式碼只為相容；要重啟前先確認是否真的需要。

### road_congestion
省道＋5 縣市市區路況。

- 頻率／位置：每 5 分鐘，Zeabur。
- 寫入：`live.road_sections_live`（history）、`live.road_sections_current`。
- 守門：`history_dedup_cols`（壅塞等級、速度、旅行時間有變才寫 history；目前只有這支用）。**每個台北日的第一輪會略過去重、全量寫一次**當心跳，避免「長期不變的路段」在 history 消失（`storage/supabase_writer.py` 的 `_history_dedup_heartbeat_date`）。
- 已知坑：`road_sections_live` 很大，`MAX()` 會 timeout，所以表級監控只看 current 表。

### road_event_live
即時路況事件（國道、省道、10 縣市 LiveEvent）。

- 頻率／位置：每 5 分鐘，Zeabur。
- 寫入：`live.road_events`（history）、`live.road_events_current`（與 `road_event_planned` 共用兩張表）。`utils/section_enricher` 補 SectionID。
- 守門：history 只寫「首次出現或實質內容有變」的事件，比對 `description`、`event_step`、`severity`、`blocked_lanes`、`impact_description` 五欄，不看 `LastUpdateTime`（71% 是假刷新，改比實質欄位後 skip 率從約 30% 升到約 95%）。current 每輪全量 upsert，並刪掉 `expire_time` 已過的事件。
- 已知坑：**current 的 upsert 不能加 `IS DISTINCT FROM`（沒變不寫）**。`road_events_current.collected_at` 同時是 3 天清理與新鮮度監控的依據，沒變不寫會讓它停住，清理和監控一起壞掉。這是「沒變不寫」原則的反例，見 [principles.md](../principles.md) 與 gis-platform（private）`docs/research/2026-10-16-disk-io-round3.md`。
- 上游細節：analytics `docs/api-platforms/tdx/gotchas.md`（LastUpdateTime）。

### road_event_planned
預告型路況事件（Event／City）。每 12 小時，Zeabur；與 `road_event_live` 共用 `live.road_events*`，守門與坑同上。因為共用表，planned 這支停掉時，表的新鮮度會被 live 蓋過，不會被發現。

### parking
路邊停車即時（臺北、新北、臺中）。

- 頻率／位置：每 15 分鐘，Zeabur（repo 預設關，production 用環境變數開）。
- 寫入：`live.parking_segments_availability`、`live.parking_segments_current`；歷史表由 current 代監控。

### parking_offstreet
路外停車場即時（縣市、國道服務區、觀光三種）。每 15 分鐘，Zeabur；寫 `live.parking_lots_availability`、`live.parking_lots_current`，歷史表由 current 代監控。

### parking_ref
停車場靜態座標（TDX v1，月更）。清冊標停用。寫入方式特殊：直接用 psycopg2 upsert `spatial.parking_segments_ref`、`spatial.parking_lots_ref`，不走 `SupabaseWriter`，所以不會有 buffer 與心跳。

## 收這類資料要注意

- 所有 TDX 請求都走 `TDXSession`；上線前算配額，尤其是「縣市數 × 頻率」型的 collector。
- 一次 16 支 collector 同時倒下，先懷疑共用金鑰（401），不是各自的程式問題。
- 判斷「有沒有變」不要用 `LastUpdateTime`；比實質欄位。
- 「沒變不寫」要先確認那個時間欄位是不是別人的心跳或清理依據（`road_events_current.collected_at` 就是）。
- 共用表的兩支 collector（`road_event_live`／`road_event_planned`）一支停掉不會出現在表級監控；`tra_static` 完全沒有表級監控。健康判讀見 [operations.md](../operations.md)。
- 政府端可能擋雲端 IP；TDX 目前從 Zeabur 可以連，但遇到 ConnectTimeout 時參考 [EXTERNAL_COLLECTORS.md](../../EXTERNAL_COLLECTORS.md) 的 VM 模式。
