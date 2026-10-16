# 氣象署（CWA）與 NCDR：天氣、雨量、地震、雲圖、海象、落雷、紫外線、災害示警

**一句話**：這一類大多是官方 JSON／XML／檔案端點，資料本身穩定，但「看起來成功、其實沒資料」的情況最多：302 轉址、回 200 卻只有標題列、缺值用 -99、編碼被猜錯變亂碼、事件型資料本來就常常 0 筆。最大的坑是把「0 筆」或「HTTP 200」當成健康。

清單與寫入表以 [collectors.md](../collectors.md) 為準。上游端點與欄位怪癖在 taipei-gis-analytics（private）`docs/api-platforms/cwa/gotchas.md`、`docs/api-platforms/ncdr/gotchas.md`，這裡只連結。

## 共通機制

- **金鑰**：多數 CWA collector 需要 `CWA_API_KEY`，在 `collectors/registry.py` 的 `required_env` 宣告。缺 key 時 collector 在啟動時被跳過（只在啟動 log 印一行），所以 registry 和 `config.py` 兩邊都要宣告，見 [2026-07-26 required_env 靜默跳過](../../../.claude/pitfalls/2026-07-26-required-env-silent-skip.md)。
- **事件型資料**（地震、落雷、示警）：沒有事件就沒有新列。新鮮度門檻在 `config/cross_layer_map.yaml` 刻意放寬（例如 earthquake、台電落雷），「久沒新列」不等於壞掉；反過來「有在跑」也不等於有資料，判讀方法見 [operations.md](../operations.md)。
- **時間一律 tz-aware**：寫 DB 的時間用 `datetime.now(TAIPEI_TZ)`。2026-04-07 `collectors/base.py` 用 naive datetime，所有 collector 的 `collected_at` 都被當 UTC 偏移 +8 小時（不限氣象類），見 [2026-04-07 時區事故](../../../.claude/pitfalls/2026-04-07-timezone-bug.md) 與 [principles.md](../principles.md)。
- **302 與檔案端點**：CWA fileapi／落雷端點會 302，curl 不加 `-L` 會拿到 0 bytes 看起來像沒資料；路徑寫錯不報錯，而是回官網首頁（細節見 analytics `cwa/gotchas.md`）。

## collector

### weather
自動氣象站逐時觀測（O-A0001-001）。

- 頻率／位置：每小時，Zeabur。critical。需 `CWA_API_KEY`。
- 寫入：`live.weather_observations`（history）、`live.weather_current`。

### temperature
溫度網格（O-A0038-003）。每小時，Zeabur；寫 `live.temperature_grids`（只有 history）。需 `CWA_API_KEY`。

### rain_gauge_realtime
自動雨量站（O-A0002-001）。每 10 分鐘，Zeabur；寫 `live.rain_gauge_readings`。需 `CWA_API_KEY`。

### cwa_satellite
衛星雲圖與雷達回波 PNG。

- 頻率／位置：每 10 分鐘，Zeabur。需 `CWA_API_KEY`。
- 寫入：`live.cwa_imagery_frames`；影像另外雙寫 R2 給 CDN（`storage/r2.py`，key 規則見 `collectors/cwa_satellite.py` 的 `imagery_r2_key`）。
- 守門：DB 端清理必須等影像已在 R2（cleanup 加 `image_key IS NOT NULL` 守衛），否則刪掉 DB 的那一刻就永久遺失。保留期和守衛條件以 gis-platform `metadata.retention_policies` 與對應 migration 為準，見 [data-lifecycle.md](../data-lifecycle.md)。
- 已知坑：同一張雷達圖曾同時存在 DB（base64）、R2、S3 三份；決策脈絡見 .gis-agent-system（private）`decisions/0021`。

### earthquake
有感地震報告＋逐站觀測＋海嘯資訊（E-A0015／E-A0016／E-A0014）。

- 頻率／位置：每 15 分鐘（2026-07 從每日改 15 分），Zeabur。critical。需 `CWA_API_KEY`。
- 寫入：`live.earthquake_events`、`live.earthquake_station_obs`、`live.tsunami_alerts`（多表寫入）。
- 已知坑：事件驅動，每日 archive 可能是空的；上游 E-A0015-001 只保留最近幾筆，輪詢太慢會漏（所以改成 15 分）。完整目錄另由 `earthquake_catalog` 收。

### earthquake_catalog
年度正式地震目錄（E-A0073-001，含無感）。清冊標停用。每日比對，有新的才寫 `live.earthquake_events`（`DO NOTHING`）；上游約半年更新一批。與 `earthquake` 共用表。

### earthquake_town_intensity
鄉鎮震度（E-A0015-005，從 S3 取，免金鑰）。清冊標停用。上游只留最新一次地震，用 `origin_time` 守門避免重寫；寫 `live.earthquake_town_intensity`。

### earthquake_shakemap_grid
NCDR EQ1 全台 2.5 km 網格（4,377 格，免金鑰）。清冊標停用。上游只留最新一次，用 `event_time` 守門避免每輪重寫；寫 `live.earthquake_shakemap_grid`。事件驅動，2026-10-05 調查時心跳與資料都停在 08-26，不能只看時間判斷壞掉（[collector-health 調查](../../investigations/2026-10-05-collector-health.md)）。

### earthquake_moment_tensor
中研院 BATS AutoBATS 震源機制解。清冊標停用。

- 沒有清單端點，靠 CWA 事件清單逐一查（時間是 UTC，秒級要吻合），所以也需要 `CWA_API_KEY`。
- 查無資料時回 HTTP 200 的空殼 JSON，不是 404；只有規模較大的地震才有解。`COLLECT_TIMEOUT` 600 秒。

### cwa_marine_observation
海洋浮標與潮位觀測（O-B0076 測站＋O-B0075 rolling 48 小時）。

- 頻率／位置：每 15 分鐘，**固定在 :07／:22／:37／:52**（`CWA_MARINE_OBSERVATION_MINUTES`，不套 jitter，避開 DB cron 的 :14／:15／:40），Zeabur。需 `CWA_API_KEY`，設 `require_db_write`。
- 寫入：`reference.marine_observation_stations`、`live.marine_observation_readings`、`live.marine_observation_current`、`live.marine_observation_quarantine`；與 `isohe_port_marine` 共用前三張。
- 守門（寫入契約集中在 `storage/marine_write.py`）：
  - **增量**：只送「current 裡最新有效時間 − 3 小時」之後的列（`MARINE_WRITE_LOOKBACK_HOURS`），不再每輪重寫 48 小時。
  - **沒變不寫**：readings／current 的 `DO UPDATE` 加 `IS DISTINCT FROM`，`payload_sha256`、`collected_at` 不參與比對；加 `collected_at` guard，較舊的批次（例如 buffer 重放）不能覆蓋較新的。
  - **站表**：內容 hash 有變，或距上次滿 6 小時才寫，用獨立短交易，先 commit 再寫 readings。
  - 每個交易開頭 `statement_timeout` 120 秒、`lock_timeout` 10 秒。
  - 找不到測站的 StationID 進 quarantine 表，不默默丟掉。
- 已知坑：2026-10-12 寫入卡死。每輪重寫 48 小時（約 2.8 萬列，新資料只約 1/48），`DO UPDATE` 無條件，再加上 6 個索引（含沒人用的 GiST），一筆交易跑 2 小時、持有站表列鎖，後面每輪排隊佔滿連線池，連公車等其他 collector 都寫入逾時。修正後每小時實際寫入降約 99.5%。全文：[2026-10-12 海象寫入卡死](../../investigations/2026-10-12-marine-write-stall.md)；DB 端索引調整見 gis-platform（private）migration 441。
- 上游細節：analytics `cwa/gotchas.md`（海象的發布來源不等於感測器擁有者）。

### isohe_port_marine
商港海氣象（ISOHE，8 港 × 波浪、海流、潮位、風）。

- 頻率／位置：**跑在 HiCloud VM**（只有台灣出口 IP 連得到），cron `8-59/10`（每 10 分、錯開整點）並以 `timeout -k 30 540` 包住；Zeabur 端 `ISOHE_PORT_MARINE_ENABLED` 必須維持 false。VM 不寫 `metadata.collector_status`，看健康要看 `live.marine_observation_current` 裡 `station_uid` 以 `isohe:` 開頭的列的 `collected_at`，以及 S3 `isohe_port_marine/archives/`（VM 晚約 2 天上傳，清冊 `archive_lag_days: 3`）。清冊曾標 `enabled: false`，2026-10-16 依上述兩項都新鮮校正為 `true`。VM 部署方式見 [EXTERNAL_COLLECTORS.md](../../EXTERNAL_COLLECTORS.md)，VM 主機以 `<VM_HOST>` 表示。
- 寫入：與 `cwa_marine_observation` 共用 `reference.marine_observation_stations`、`live.marine_observation_readings`、`live.marine_observation_current`（`station_uid` 不同）。
- 守門：VM 版 `external/isohe_port_marine_vm/marine_write.py` 與 `storage/marine_write.py` **必須逐位元組相同**（`tests/test_isohe_port_marine_vm.py` 強制）；VM 另有 `SIGALRM` 480 秒、`collect.lock` 非阻塞鎖（前一輪沒跑完就拒跑）、站表狀態存 `$DATA_DIR/state/station_roster.json`。排除 MZ legacy XML；不同潮位基準面分開存，不混算。
- 已知坑：2026-10-12 同一事件中，VM 上兩個 collect 程序卡了 3.6 天與 1.5 天，因為 Python 的 alarm handler 在阻塞於 libpq 時不會觸發，改用預設處置讓 kernel 終止。VM 是手動 scp 部署，git push 不會生效。

### ncdr_alerts
NCDR 災害示警（CAP 總和 feed）。

- 頻率／位置：每 15 分鐘，Zeabur。critical。
- 寫入：`live.disaster_alerts`。
- 守門：**寫入層亂碼防護的唯一成員**（`storage/supabase_writer.py` 的 `MOJIBAKE_GUARD_COLLECTORS`）：含西里爾字母（U+0400–U+04FF）的列剔除、其餘照寫；整批都壞才 raise `MojibakeWriteRejected`，不進 buffer，buffer 重放遇到被拒的檔移到 `buffer/quarantine/`。
- 已知坑：
  - 亂碼根因是 `_fetch_cap` 曾用 `r.text`：上游 Content-Type 沒寫 charset，requests 用 chardet 猜，把 UTF-8 中文猜成西里爾單位元組編碼（16,815 筆中壞 95 筆，2022 年起零星出現）。修法是改用 `r.content` 交給 `ET.fromstring(bytes)` 依 XML 宣告解碼（`collectors/ncdr_alerts.py` 註解）；既有壞資料回上游 `cap_url` 重抓，不反解猜測。修資料前要先確認寫入端已停止產生壞資料，否則是在和 collector 賽跑。事故紀錄：mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-21/22。
  - 為什麼只用 allowlist 不全面擋：`global_events` 新聞標題、`aisstream` 船名、`global_climate_usgs_earthquake` 地名、`satellite` TLE 名稱都可能合法出現西里爾字母，全面擋會讓它們停擺。設計見 [2026-10-05 GD-2 調查](../../investigations/2026-10-05-gd2-mojibake-storage-guard.md)。
- AlertType 對照與 feed 留存行為：[`docs/NCDR_ALERTTYPE_MAP.md`](../../NCDR_ALERTTYPE_MAP.md)（總和 feed 已涵蓋所有類型，含停電、停水）。上游細節：analytics `ncdr/gotchas.md`。

### lightning_events
台電落雷即時（CSV，data.gov.tw nid 61139）。

- 頻率／位置：Zeabur。repo 預設**每 30 分鐘**，但這是因為上游停供而降頻：**上游恢復時必須立刻調回每 1 分鐘**（端點每分鐘整檔覆寫，每批只存在 60 秒，30 分鐘會漏掉 29/30）。collector 偵測到恢復會發 Telegram 提醒（`_maybe_notify_recovery`）。
- 寫入：`live.lightning_events`（`source` 區分來源），與 `lightning_cwa`、`jma_lightning` 共用。去重靠 `event_id`＋`dedup_hash`，表上有兩個唯一索引，所以 INSERT 用**不指定目標**的 `ON CONFLICT DO NOTHING`（指定單一 key 會讓整批失敗，2026-07-02 曾因此連續寫入失敗）。
- 已知坑：**上游自 2026-07-10 起靜默斷供**：端點 200，但檔案只有標題列。程序成功、沒有例外、沒有告警，28 天後才發現。S3 archive 每天檔案大小完全相同就是停供起點的證據。全文：mini-taiwan-pulse `.claude/pitfalls/2026-08-07-silent-upstream-outage.md`。
- 共用表風險：三個來源寫同一張表，台電這支停了，表的新鮮度仍被 CWA／JMA 的新列撐著，表級監控看不出來。

### lightning_cwa
氣象署落雷（O-A0039-001，KMZ，滾動 1 小時視窗、每 5 分更新）。

- 頻率／位置：每 5 分鐘，Zeabur。需 `CWA_API_KEY`。
- 寫入：`live.lightning_events`，`source='cwa'`。是台電斷供後的替代源，也用來交叉驗證。
- 已知坑：KML 收尾標籤可能是 `</when >`（多一個空格），regex 寫死 `</when>` 會解出 0 筆且不報錯；通則寫 `</tag\s*>`（mini-taiwan-pulse INCIDENTS §2026-08-07）。

### cwa_uv_daily
每日紫外線最大值（O-A0005-001，前一日值）。

- 頻率／位置：每 12 小時，Zeabur。需 `CWA_API_KEY`，設 `require_db_write`。
- 寫入：`live.cwa_uv_daily`。`observed_at` 是觀測日 00:00，落後 24–48 小時屬正常。
- 守門：`UVIndex = -99` 是缺值，寫成 `uv_index NULL`、`uv_raw` 保留原值，不可當 0。

## 收這類資料要注意

- 0 筆常常是合法結果（沒地震、沒落雷）。判斷健康要看「有沒有跑／有沒有解析成功／有沒有新資料」三件事，並盡量有第二來源交叉驗證（落雷三源就是例子）。見 [operations.md](../operations.md)。
- 上游回 200 不代表有資料：落雷 CSV 只剩標題、AutoBATS 回空殼 JSON、fileapi 路徑錯回首頁。解析結果為 0 時要能分辨「真的沒事」和「格式壞了」。
- 缺值碼（-99、品質碼、缺幀）一律存 NULL，原值另存，不可當 0。
- 抓 XML／CSV 時用 bytes（`r.content`）交給 parser，不要讓 requests 猜編碼。
- 多個 unique index 的表去重，用不指定目標的 `ON CONFLICT DO NOTHING`。
- 共用表（落雷三源、海象 CWA＋ISOHE、地震 events）一源壞掉會被其他源的新鮮度蓋過。
- 「沒變也寫」在高頻、整包重送的來源（海象 48 小時視窗）會變成 Disk IO 大戶；增量＋`IS DISTINCT FROM` 是標準解，但要先確認時間欄位不是別人的心跳（見 [principles.md](../principles.md)）。
