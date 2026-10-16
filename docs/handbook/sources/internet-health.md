# 網路健康：Cloudflare Radar、IODA、RIPE Atlas、RIPE RIS Live

**一句話**：這四個來源都是「證據」，不是「台灣網路正常／中斷」的結論；它們寫進同一組表、各自是獨立的失敗域。最大的坑是共用表：一個來源壞掉，會被其他來源的新鮮度蓋過，而且「沒有異常事件」也不能被解讀成「正常」。

清單與寫入表以 [collectors.md](../collectors.md) 為準。上游 API 細節在 taipei-gis-analytics（private）`docs/api-platforms/` 的 `cloudflare_radar/`、`ioda/`、`ripe_atlas/`、`ripe_ris/`，這裡只連結。

## 共通機制

- **共同資料契約**：四支 collector 都寫 `live.internet_health_source_runs`（每輪 ledger）、`live.internet_health_observations`、`live.internet_health_current`；Cloudflare 與 IODA 另寫 `live.internet_health_incidents`。四支都設 `require_db_write`。
- **證據，不是結論**：collector 不判斷全台是否正常。Cloudflare 只有在 netflow 新鮮、且 anomaly 與 outage 兩個端點都成功又同窗無事件時，才產生 provider 層級的 `normal` 心跳；IODA 在 alert schema 驗證前一律 `unknown`；跨來源的綜合判斷（composite detector）刻意尚未實作（`collectors/internet_health.py` 開頭說明）。同一 provider 的多個訊號只算一個 evidence family；RIPE Atlas 與 RIS 同屬 `independence_group=ripe_ncc`，不能單獨湊成「兩個獨立機構」。
- **缺值不是 0**：NULL／逾時維持缺值；RIS 只要有任何 reconnect／ack／pong／idle 缺口，整個 5 分鐘 bucket 標 partial、metrics 為 NULL／unknown，不以「沒有 BGP 更新」推論正常。
- **raw 只內部**：IODA 的 raw 回應明示 All Rights Reserved；RIPE raw 只進 private S3。公開排除由 gis-platform 契約強制（public RPC 對這些資料回 0 列是預期）。
- **金鑰缺失不靜默跳過**：Cloudflare 的 token 不在 `required_env` 宣告，缺 token 時寫一筆 `config_missing` 的 source run，而不是在啟動時被跳過（`collectors/registry.py` 註解）。這是避免 [required_env 靜默跳過](../../../.claude/pitfalls/2026-07-26-required-env-silent-skip.md) 的做法。

## collector

### cloudflare_radar
Cloudflare Radar 國家層級流量、異常與中斷事件。

- 頻率／位置：每 5 分鐘，Zeabur（repo 預設關，production 2026-08-31 起以環境變數啟用）。需 `CLOUDFLARE_RADAR_API_TOKEN`。
- 寫入：共同契約四張表。
- 已知坑：新鮮度門檻仍待多輪校準（清冊 notes）。

### ioda_internet_health
IODA（Georgia Tech）國家層級原始訊號（BGP、ping、network telescope）。

- 頻率／位置：每 5 分鐘，Zeabur（production 以環境變數啟用）。
- 寫入：共同契約四張表。
- 已知坑：raw 屬 All Rights Reserved，只做內部收集與受控 S3 復原；alerts 未驗證，保持關閉。

### ripe_atlas_internet_health
RIPE Atlas 公開 ping 量測（經審核的 roster）。

- 頻率／位置：每 5 分鐘、30 分鐘 lookback，對齊已結束的 5 分鐘 bucket；樣本數較少的結果不覆蓋較多的。寫入共同契約的 runs／observations／current。
- 守門：**roster 硬閘門** `config/ripe_internet_health.yaml`（版本化、不含憑證）：`review_status: approved`、`internal_only: true`、Atlas 至少一個有明確 probe ID 的量測，否則 fail closed。roster 漂移要審核後改版，不自動探索。公開結果讀取不需金鑰；`RIPE_ATLAS_API_KEY` 只用於經審核的私有量測。
- **正式環境已啟用**（2026-08-31 起）：清冊曾標 `enabled: false`／`deployment: disabled`，日報因此不掃它；2026-10-16 依 `metadata.collector_status`（`is_enabled = true`、`last_success_at` 在幾分鐘內）校正為 `enabled: true`／`zeabur`。
- 細節：[`docs/RIPE_INTERNET_HEALTH.md`](../../RIPE_INTERNET_HEALTH.md)。

### ripe_ris_live
RIPE RIS Live BGP 更新（有界 WebSocket 訂閱，15 個 origin ASN）。

- 頻率／位置：**長駐 worker**（`workers/ripe_ris_live.py`；registry class 是 shim），每 5 分鐘一個 bucket。強制 replicas=1、process-local lock，DB 與 private S3 都是啟用硬閘；不支援跨 replica 的分散式 lease。
- 寫入：共同契約的 runs／observations／current；raw JSON 與 manifest 只進 private S3 `ripe_ris_live/raw/v1/`。
- 守門：roster 只允許有界的 prefix（≤ 256）或 origin ASN（≤ 64）清單，設定上不可能訂閱整條 firehose。
- 正式環境已啟用：清冊同樣曾標停用，2026-10-16 依 `metadata.collector_status` 校正為 `enabled: true`／`zeabur`（同 `ripe_atlas_internet_health`）。

## 收這類資料要注意

- 共用表一定要能**分 source** 看新鮮度：四個來源寫同一組表，任何一個停掉，表的 `MAX(time)` 仍會被其他來源推進。表級監控目前看不出單一來源停擺，見 [operations.md](../operations.md)。
- 「沒有事件」不等於「正常」；只有明確定義的條件下才產生 normal 心跳，其他一律 unknown。
- 授權受限的 raw（IODA、RIPE）只存 private，公開路徑以平台契約擋住，不靠前端自律。
- 長駐 WebSocket 要有 replica 數硬閘與 process lock；連線缺口要讓整個 bucket 變 partial，不能用「沒收到更新」補成正常。
- 金鑰缺失要留下可觀察的 ledger 紀錄，不要靠啟動時的 skip log。
