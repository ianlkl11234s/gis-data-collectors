# Global Fishing Watch vessel presence

狀態：**預設停用**。`GFW_ACCESS_TOKEN` 尚未申請前，registry 會清楚跳過，collector 不會打 live API。

## 冻結的來源契約

- 官方 API：`POST https://gateway.api.globalfishingwatch.org/v3/4wings/report`
- dataset：`public-global-presence:latest`
- dataset 約落後 96 小時，collector 預設 `GFW_DATA_LAG_DAYS=5`，取 UTC 現在日期往前 5 天的最後完整日；設定必須介於 4–30 天。
- 每日 `TEMPORAL-RESOLUTION=DAILY`、`GROUP-BY=VESSEL_ID`，按台灣北部、與那國/石垣、宮古/沖繩、奄美、九州西南五個 corridor 依序送 report。API 同時只允許一個 report，故不可平行轟炸。
- request body 按官方 schema 使用 `{"geojson": "<FeatureCollection JSON string>"}`；Bearer token 放在 Authorization header。
- `latest` 實際版本只在 API response 的 `x-datasets` header 出現時記入 `resolved_dataset_version`；未取得 token 前不假定版本。
- response 若出現非零 `nextOffset`，目前會 fail closed 成 partial/failed ledger，不會把截斷資料更新到 current；取得 token 後再以真實 schema 實作並驗收完整分頁。

## DB / dedup / quality

使用 migration 371 的獨立四張表：`gfw_vessel_presence_runs`、`gfw_vessel_presence_archive_manifests`、`gfw_vessel_presence_snapshots`、`gfw_vessel_presence_current`。同一 vessel/date/position 跨 corridor 重複時以 deterministic `source_event_key` 去重；座標缺失會保留為 `suspect`，不製造座標。lat/lon 是 GFW 每日 presence 的 grid-cell center（約 0.01°/0.1°，依 spatial resolution），不是精確 AIS 船位；每筆 accepted row 會帶 `quality_flags=["grid_cell_center"]`。

GFW presence 是 AIS-derived 的每日格網 presence（不是精確船位、完整航跡，也不是暗船偵測）。能否補足 AISStream 要以 token 後的實際 response、coverage 與許可條款驗證。

## S3 license gate

預設 `GFW_RAW_ARCHIVE_ENABLED=false`：BaseCollector 不寫本地 raw-derived JSON、DB 失敗不寫 raw fallback buffer，`ArchiveTask` 也會跳過過去殘留的 `gfw_vessel_presence` 目錄。只有在確認 GFW 帳號/資料集條款允許永久冷儲存後，才可由 operator 明確設為 `true`。DB normalized snapshots 仍交由既有 backup manifest 保存。

Collector 會在寫入 UTC-5 日 snapshot 前呼叫 migration 371 的 service-only partition creator；partial/failed run 只寫 run ledger，不會推進 snapshot/current。

官方文件：

- [4Wings report API](https://globalfishingwatch.org/our-apis/documentation/docs/v3/4wings/report)
- [4Wings datasets](https://globalfishingwatch.org/our-apis/documentation/docs/v3/4wings)
