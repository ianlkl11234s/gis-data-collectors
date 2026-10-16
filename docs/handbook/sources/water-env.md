# 水利與環境：水庫、河川、地下水、IoT 感測、都市淹水、北市水利、空品、放流水、煙道、輻射

**一句話**：這一類來源機關多（水利署、北市工務局、環境部、台電、核安會）、測站多、多半免金鑰，共通的坑是政府憑證缺 SKI 要 `verify=False`、測站 metadata 每輪重寫造成 Disk IO、以及「低頻或不定期更新」的來源，停了很久都沒人發現（水情燈號 143 天）。

清單與寫入表以 [collectors.md](../collectors.md) 為準。上游 API 細節在 taipei-gis-analytics（private）`docs/api-platforms/` 的 `opendata_wra/`、`iow/`（水利署 IoT）、`wra_gic/`、`wic_taipei/`、`moenv/`、`taipower/`、`nusc/`，這裡只連結。

## 共通機制

- **政府憑證缺 Subject Key Identifier**：部分政府網站憑證缺 SKI 欄位，新版 OpenSSL 會拒絕，這些 collector 用 `verify=False`（`rg -l "verify=False" collectors/` 可列出）。同樣原因曾讓 analytics 的目錄掃描靜默壞死 84 天，SSL 失敗被吞成「0 筆」。
- **測站表沒變不寫（2026-10-15，#147）**：`iot_wra_stations`、`uswg_stations` 等測站 metadata 改用 `storage/supabase_writer.py` 的 `_station_changed_cond`（`IS DISTINCT FROM`），內容沒變就不 UPDATE。代價：`updated_at` 變成「最後異動」而不是「最後同步」，看新鮮度改看 collector 心跳。背景：Disk IO 第三輪，見 [2026-09-04 Disk IO 膨脹](../../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) 的第三輪段落。
- **重疊來源的判斷**：新 collector 和既有來源疑似重疊時，不要相信編號，用座標 100 m 內配對：重複超過 90% 停一邊、低於 30% 兩邊都留。iot_wra 的地下水子端點和 `groundwater_level` 95% 重複，2026-04-26 起停掉 iot 那邊；河川只 16% 重複，兩邊都留（mini-taiwan-pulse `.claude/memory/PRINCIPLES.md`「Collector 重複度檢核」）。
- **API key 不進例外訊息**：環境部 collector 的 key 只放在 query params，例外訊息會改寫遮蔽（`collectors/moenv_common.py`）。

## collector

### water_reservoir
水庫水情（水利署 OpenData，約 40 座）。每小時，Zeabur；寫 `live.reservoir_status`；啟動時同步水庫基本資料；免金鑰。

### water_reservoir_daily_ops
水庫每日營運資料（data.gov.tw 41568）。

- 頻率／位置：每日，Zeabur。官方 09:30 前後更新，但排程是「啟動後每 N 分鐘」，不是固定在 09:30 之後，所以可能延遲一天才抓到。
- 寫入：`live.reservoir_daily_ops`。
- 已知坑：上游未更新時，空的或非 list 的回應會被轉成 `[]` 並算成功（`last_success_at` 照樣前進）。這是心跳盲點的典型例子，見 [code-audit-evidence.md](../../investigations/code-audit-evidence.md) P1；尚未修。

### wra_drought_alert
水情燈號（水利署網頁 HTML scrape，無 API）。

- 頻率／位置：每日，Zeabur。清冊監控門檻設 30 日（上游不定期更新，18 天沒變屬常態）。
- 寫入：`public.drought_alert_current`（upsert）、`public.drought_alert_history`（`ON CONFLICT DO NOTHING`）；history 由 current 代監控。
- 守門：整頁結構化內容（公告日＋燈號）算 canonical hash，與 DB 最新一筆 `source_hash` 相同就整批跳過。
- 已知坑：`drought_alert_current` 自 2026-05-15 後沒有再寫入，到 2026-10-05 約 143 天都沒人發現。原因疊了三層：hash 相同時 collector 提前 return 不寫心跳、DB 告警只看全域最新 `last_success_at`、HTTP 200 的空頁也被算成功。見 [collector-health 調查](../../investigations/2026-10-05-collector-health.md) 與 [operations.md](../operations.md)。

### river_water_level
河川水位（水利署 OpenData nid 25768）。每 10 分鐘，Zeabur；寫 `live.river_water_level`；免金鑰。與 `iot_wra` 的河川站約 16% 重疊，兩邊都留。

### groundwater_level
地下水位（OpenData nid 161082）。每小時，Zeabur（上游每 10 分更新，但量大所以降頻）；寫 `live.groundwater_level_readings`；免金鑰。保留這支而停掉 iot_wra 的地下水子端點，因為它多了電壓（運維健康度）、地址等欄位。

### iot_wra
水利署 IoT 水文感測（河川、累計流量、閘門、沖刷、堤防、揚塵 6 類；地下水已停）。

- 頻率／位置：Zeabur，**每 10 分鐘一輪**：正式 env `IOT_WRA_INTERVAL` 覆寫為 10，`config.py` 的 repo 預設仍是 60（只在 env 未設時生效）。2026-10-16 唯讀查 `live.iot_wra_measurements` 的 `collected_at`，每 10 分一批、每批約 2,900 筆新讀值，所以上游也至少每 10 分更新。清冊監控門檻（`cross_layer_map.yaml` 的 `expected_interval_min`）已同步改為 10。
- 寫入：`live.iot_wra_measurements`（`ON CONFLICT DO NOTHING`）；另 upsert `public.iot_wra_stations`（沒變不寫，#147）。保留期以 `metadata.retention_policies` 為準（gis-platform mig 412 曾調整，mini-taiwan-pulse DATA_SCOPE 的舊天數已過時）。
- 已知坑：Swagger 寫要 Bearer，實際端點免授權。主鍵欄序曾讓每次寫入散落到大量索引頁（每天讀約 5.7 GB），gis-platform 第三輪改成以時間開頭，見 gis-platform（private）`docs/research/2026-10-16-disk-io-round3.md`。

### uswg
都市淹水感知器（水利署 IoT `/uswg/stations`，全國約 2,000 站）。

- 頻率／位置：每 10 分鐘，Zeabur。需 `IOW_CLIENT_ID`、`IOW_CLIENT_SECRET`（OAuth2 client credentials，token 30 分鐘）。
- 寫入：`live.uswg_measurements`（`ON CONFLICT DO NOTHING`）；另 upsert `public.uswg_stations`（沒變不寫，#147）。
- 已知坑：憑證缺 SKI，`verify=False`；官方 schema 欄位拼成 `Longtiude`。保留期以 `metadata.retention_policies` 為準；清理排程已恢復：2026-10-16 查 pg_cron `cleanup-uswg-measurements` 為 active、近 4 天每天 succeeded，超過保留期的列只剩個位數尾巴。

### precipitation_raster
累積雨量柵格 PNG（水利署 IoT，1／3／6／24 小時四種）。每小時，Zeabur；與 `uswg` 共用 OAuth 憑證；寫 `live.precipitation_raster_frames`。上游常延遲 1–2 小時，所以清冊監控門檻放寬到 2 小時。

### wic_sewer
臺北市雨水下水道水位（工務局水利處，233 站）。每 10 分鐘，Zeabur；寫 `live.taipei_sewer_measurements`，另 upsert 站表。URL 帶固定的 `loginId`／`dataKey`，是 data.taipei 上公開的值，不是祕密。

### wic_evacuate
臺北市疏散門狀態（35 站）。每 10 分鐘，Zeabur；寫 `live.taipei_evacuate_status`；同樣帶公開的固定 dataKey。

### wic_pumb
臺北市抽水站運轉（97 站，heopublic.gov.taipei，無金鑰）。每 10 分鐘，Zeabur；寫 `live.taipei_pumb_status`。

### air_quality
環境部空品測站 AQI（AQX_P_432，77 站）。每小時，Zeabur；需 `MOENV_API_KEY`；寫 `live.air_quality_observations`、`live.air_quality_current`。

- 已知坑：2026-04-24 分區表壞掉事件，見 [2026-04-24 空品分區](../../../.claude/pitfalls/2026-04-24-air-quality-partition-broken.md)。

### air_quality_imagery
全台空品色階圖 PNG（airtw）。每小時，Zeabur；寫 `live.aqi_imagery_frames`（PK `(product_type, observed_at)`），影像雙寫 R2。DB 清理要等 `image_key` 已填（同 `cwa_satellite` 的守衛），見 [data-lifecycle.md](../data-lifecycle.md)。

### air_quality_microsensors
LASS AirBox 微型感測器（約 500 點）。每 5 分鐘，Zeabur；寫 `live.micro_sensor_readings`；免金鑰。

### water_effluent_monitoring
放流水連線自動監測（環境部 `wqx_p_47..67`、`wqx_p_126`，22 縣市）。

- 頻率／位置：每小時，Zeabur。需 `MOENV_API_KEY`，設 `require_db_write`。
- 寫入：`live.water_effluent_readings`、`live.water_effluent_current`；歷史表由 current（每輪 touch `updated_at` 當心跳）代監控。
- 已知坑：臺中 `wqx_p_54` 上游 2024-12 起停更，該縣市 stale 屬預期，不是 collector 壞掉。憑證缺 SKI。上游是 1–2 小時滾動視窗。

### cems_stack_monitoring
固定污染源煙道（`aqx_p_187`）與燃燒塔（`aqx_p_493`）1 小時值。

- 頻率／位置：每小時，Zeabur。需 `MOENV_API_KEY`，設 `require_db_write`。
- 寫入：`live.cems_stack_readings`、`live.cems_stack_current`；歷史表由 current 代監控（`collected_at` 無索引，不適合直接監控）。
- 已知坑：上游延遲 4–5 小時，collector 回看 6 小時；`observed_at` 落後屬正常。

### nuclear_radiation
台電核設施環境輻射（data.gov.tw nid 42326，51 站）。

- 頻率／位置：每 15 分鐘，Zeabur。
- 寫入：`live.nuclear_radiation_measurements`、`live.nuclear_radiation_stations`（站表 touch `updated_at` 當心跳，代監控量測表）；另有每日彙總表由 DB 端產生。
- 已知坑：CSV 帶 UTF-8 BOM；憑證缺 SKI；超過 30 分鐘沒更新視為 stale。

### nusc_gamma_radiation
核安會全國環境輻射（data.gov.tw 119233，63 站，上游每 5 分）。

- 頻率／位置：每 15 分鐘，Zeabur。設 `require_db_write`。
- 寫入：`live.nusc_gamma_measurements`、`live.nusc_gamma_stations`（站表代監控）。
- 已知坑：與台電 `nuclear_radiation` 是**不同監測網**，不可用距離把兩邊的站合併。

## 收這類資料要注意

- 不定期更新的來源（水情燈號、每日營運）要有「解析成功」與「有新資料」兩種訊號；hash 沒變就跳過時，也要留下「這輪有跑、內容沒變」的紀錄，否則停擺看起來和沒變一樣。
- 測站 metadata 每輪全量 upsert 是 Disk IO 大戶，預設用 `_station_changed_cond` 沒變不寫；但先確認 `updated_at` 沒有被當成新鮮度依據。
- 上游停更的單一縣市（臺中放流水）要在清冊 notes 註明，避免日報誤報後被當成 collector 壞掉。
- 新來源和既有來源重疊時，用座標配對算重複率再決定停哪一邊。
- 憑證、BOM、欄位 typo 這類解析細節寫在 collector docstring，上游行為寫回 analytics `api-platforms/`。
