# Global Fishing Watch vessel presence

狀態（2026-08-25）：舊 `GFW_VESSEL_PRESENCE_ENABLED` 仍刻意保持 `false`；
新 `gfw_hourly_publish` 已在 Zeabur production 每日 08:30 Asia/Taipei 排程（UTC 換日後）。
首次已成功發布 `2026-08-20` unified v2 release 至 S3/Cloudflare public origin。
Root 的 Cache-Control header 已正確，但 Cloudflare 回應仍為 `CF-Cache-Status: DYNAMIC`，
root edge cache 尚未完成驗收。

### v3 shadow production candidate（audit 已完成；code 尚未 deploy）

2026-08-15..2026-08-21 UTC 的 local v3 shadow candidate 已完整建立：16 個 sequential
AIS reports + 16 個 sequential SAR unmatched reports，HTTP 全為 200、retries 為 0。
計有 points 1,426,359、canonical features 226,830、vessels 64,051、segments 168,936、
singletons 57,894、grid cells 1,105,448；SAR unmatched 為 0。bundle 約 995 MB、3,313
files。S3 v3-shadow root `2026-08-21` 已驗 schema 3 / `full_fidelity`、root bytes/hash
一致，並完成 full 3,311/3,311 HEAD audit：missing/head_errors/bytes/sha mismatches 均為 0、
`timed_out=false`。migration 377 已 applied；Supabase run `e00` 為 succeeded/is_current、schema 3 shadow，
asset/counter 皆一致。code 尚未 deploy，canonical v2 root/release 不變。

P0 transition root cause 是 initial `running` ledger row 在 final succeeded metadata 寫入前可能
繼承 canonical v2 default。migration 377 與 collector defense-in-depth 已修正：`running`
從一開始即寫 schema 3 與 v3-shadow root key。每日排程維持 08:30 Asia/Taipei（UTC 換日後，
配合保守 source lag）。

## 冻結的來源契約

- 官方 API：`POST https://gateway.api.globalfishingwatch.org/v3/4wings/report`
- dataset：`public-global-presence:latest`
- dataset 約落後 96 小時，collector 預設 `GFW_DATA_LAG_DAYS=5`，取 UTC 現在日期往前 5 天的最後完整日；設定必須介於 4–30 天。
- 每日 `TEMPORAL-RESOLUTION=DAILY`、`GROUP-BY=VESSEL_ID`，按台灣北部、與那國/石垣、宮古/沖繩、奄美、九州西南五個 corridor 依序送 report。API 同時只允許一個 report，故不可平行轟炸。
- request body 按 live API schema 使用 `{"geojson": <FeatureCollection object>}`；Bearer token 放在 Authorization header。
- `latest` 實際版本只從 API response 的 `x-datasets` header 記入。
- response 若出現非零 `nextOffset`，會 fail closed，不會發布截斷資料。

## DB / dedup / quality

使用 migration 371 的獨立四張表：`gfw_vessel_presence_runs`、`gfw_vessel_presence_archive_manifests`、`gfw_vessel_presence_snapshots`、`gfw_vessel_presence_current`。同一 vessel/date/position 跨 corridor 重複時以 deterministic `source_event_key` 去重；座標缺失會保留為 `suspect`，不製造座標。lat/lon 是 GFW 每日 presence 的 grid-cell center（約 0.01°/0.1°，依 spatial resolution），不是精確 AIS 船位；每筆 accepted row 會帶 `quality_flags=["grid_cell_center"]`。

GFW presence 是 AIS-derived 的每日格網 presence（不是精確船位、完整航跡，也不是暗船偵測）。能否補足 AISStream 要以 token 後的實際 response、coverage 與許可條款驗證。

## S3 license gate

預設 `GFW_RAW_ARCHIVE_ENABLED=false`：BaseCollector 不寫本地 raw-derived JSON、DB 失敗不寫 raw fallback buffer，`ArchiveTask` 也會跳過過去殘留的 `gfw_vessel_presence` 目錄。只有在確認 GFW 帳號/資料集條款允許永久冷儲存後，才可由 operator 明確設為 `true`。DB normalized snapshots 仍交由既有 backup manifest 保存。

Collector 會在寫入 UTC-5 日 snapshot 前呼叫 migration 371 的 service-only partition creator；partial/failed run 只寫 run ledger，不會推進 snapshot/current。

官方文件：

- [4Wings report API](https://globalfishingwatch.org/our-apis/documentation/docs/v3/4wings/report)
- [4Wings datasets](https://globalfishingwatch.org/our-apis/documentation/docs/v3/4wings)

## Hourly tracks 每日分區發布契約（本機 POC）

`scripts/gfw_hourly_tracks_poc.py` 保留原有 `--output <file.geojson>` 單檔模式，
另可用 `--output-dir <release-root>` 生成 manifest-last 的每日分區發布：

```text
<release-root>/
  manifest.json
  releases/<latest-complete-date>/
    manifest.json
    run.json
    days/YYYY-MM-DD.geojson
  run-ledger/<latest-complete-date>.json
```

- source 仍可是 rolling 7 UTC days，但 frontend 一次只讀一個 display day。
- 每日檔只保留該 UTC 日顯示所需的軌跡：最多 3h lookback＋1h lookahead，
  支援 0.5/1/2/3h 拖尾及相鄰小時格網中心間的線性內插。
- day manifest 記錄 `display_date`/overlap；root manifest 記錄
  `latest_complete_date`、date range、`generated_at`、每日 path/sha256/bytes/features
  與 retention contract。
- 先寫 `staging/<latest>` 並驗證 JSON/時間契約/hash，再 rename 成 immutable
  `releases/<latest>`，最後 atomic replace root manifest。只有 cutover 成功後才以
  精確且經驗證的 path 清理舊 release。
- 預設保留 current＋previous 兩版：兩版合計保存 14 個 release-day payload，
  但相鄰 rolling windows 會重疊；若每日發布，通常是 8 個不同 UTC dates、可回退
  1 個 release，不能誤稱為 14 個不同日期。驗證或發布失敗時保留
  staging/spool，且不推進舊 manifest。
- 舊 release 只在 root manifest cutover 成功後清理；刪除前會逐一驗證 release id、
  manifest 所列 day files 與實際目錄內容。遇到任何未知檔案／symlink 會保留整版並回報，
  不使用 broad recursive delete 或 glob。
- run ledger 只存小型 metadata；不保存 raw GFW response。

## Unified hourly production job（Zeabur production）

`tasks/gfw_hourly_publish.py` 是每日固定時間 job，deploy 不會立即跑。每次都重抓
最新完整日往回 7 UTC days，是為了吸收 GFW 延遲修正；不是每天只補一天。

- 當前 bbox 以 3° tiles 分成 16 格。AIS `public-global-presence:latest`每格只送
  1 個 sequential HOURLY/HIGH/VESSEL_ID report，當場投影成 minimum normalized shard，
  同一份 shard 再 fan-out 到 grid 和 tracks；不會為兩個圖層重複打 32 次 AIS reports。
- SAR `public-global-sar-presence:latest` 是獨立資料源，同 bbox 再依序送 16 個
  `filters[0]=matched='false'` reports。因 GFW 同帳號只允許一個 in-flight report，
  AIS 與 SAR 全部 sequential。總 logical reports 為 32，不可平行。
- 2026-08-25 最小 live probe 驗證 SAR resolved dataset 為
  `public-global-sar-presence:v4.0`，wrapper 為 `{resolved_dataset: [{date,detections,lat,lon}]}`；
  無偵測時 live API 可回 `{resolved_dataset: null}`。其他 schema drift 一律 fail closed。
- 同次 probe response headers 顯示這個帳號當時配額為 50,000 requests/day 與
  1,500,000 requests/month。這是 live account header snapshot，不是寫死的永久方案；
  job 會記錄每次 rate-limit telemetry。正常執行 32 logical reports/day，重試與
  last-report recovery 另計實際 HTTP requests。
- `dark_vessels` 只能解釋為 **SAR detection unmatched to AIS**，不是確定關 AIS、
  暗船或非法船。lat/lon 是 GFW HIGH grid-cell center，不是精確 SAR 命中點。
  7 日每小時都有一個 asset；沒有 detection 的小時仍發布 zero-feature FC。
- raw GFW response 不寫 DB/S3/spool；只保留 normalized shards 供當次 fan-out。

### Unified manifest v2

只有一個 reader-visible root：
`deploy-assets/global-maritime/gfw-hourly/manifest.json`。核心 index 為：

```text
tracks.days[]
grid.hours[]
dark_vessels.hours[]
assets[]: tracks_day | grid_hour | sar_unmatched_hour
```

Immutable keys 為 `releases/<release_id>/tracks/days/...`、`grid/hours/...` 與
`dark_vessels/hours/...`。Root 在所有 immutable objects PUT＋HEAD hash/size 驗證後才
最後 cutover；三類產品任一缺失就不發布。
`dark_vessels.hours[].observed_at`、SAR hour GeoJSON metadata 與 feature properties
一律使用 canonical UTC `YYYY-MM-DDTHH:00:00Z`，不發布等價的 `+00:00` 字串。

### S3 / Cloudflare lifecycle

`scripts/gfw_hourly_release.py` 的 `publish_release_to_s3(...)` 接受可注入的
boto3-compatible client，不在 module 內讀 credentials。`bucket`、`key_prefix`
與 Cloudflare `public_url_prefix` 全由 production job 傳入，並拒絕空值、
absolute/traversal key、非 HTTPS URL 與不合法 bucket name。

- release manifest 以統一 `assets[]` 列出
  `path/sha256/bytes/type/features`；frontend indexes 必須指向同 type asset。
  舊 tracks-only manifest 沒有 `assets` 時，publisher 可從 `days` 安全推導。
- production 只有一個
  `deploy-assets/global-maritime/gfw-hourly/manifest.json`；frontend index 分別放在
  `tracks.days[]` 與 `grid.hours[]`，不建立兩個 latest/root manifest。
  Publisher 仍相容本機 tracks-only top-level `days[]` 舊契約。
- 每個 immutable asset 先 `PUT`，將 SHA-256 寫入 object metadata，立即
  `HEAD` 比對 `ContentLength` 與 metadata SHA-256。全部通過後才上傳
  release `run.json`/`manifest.json`。
- root `<key_prefix>/manifest.json` 永遠最後 `PUT`，並再做 HEAD。
  manifest 內的 `origin_mapping` 明訂
  `s3_key_prefix → public_url_prefix`，frontend path 以相對於 key prefix 的路徑
  映射到 Cloudflare public URL。
- cache 契約固定為 root
  `public,max-age=60,s-maxage=60,stale-while-revalidate=300`，release assets
  `public,max-age=604800,s-maxage=604800,immutable`。沒有 Cloudflare exact URL purge 時，
  已退役 URL 可在 edge 最長殘留 7 天，但 root 不再引用。
- root cutover 成功後才處理 retention。舊 root manifest 必須為每版
  列出 `published_releases[].object_keys`；publisher 只逐筆刪除 manifest
  明列、且完全位於 strict-date release prefix 下的 exact keys。不使用
  S3 list、glob 或 broad prefix delete；任一未知 key 在 cutover 前 fail closed。
- asset/release HEAD 驗證失敗時不上傳 root manifest、不刪舊物件，
  也不動 local staging/spool。root HEAD 異常時會回復舊 root；無舊 root
  時則刪除失敗的新 root。

S3 保留 current＋previous 2 releases。Failed local spool 當次必定保留；下次排程只會
精確驗證並清理超過 7 天的 `status=failed` known tree，任何 unknown file/symlink
會保留整個 spool。DB publish metadata 由 migration 375 獨立保留/清理，不會
觸碰 S3 objects。

### Ledger / scheduler gates

`live.upsert_gfw_hourly_publish_run(jsonb)` 必須在任何 GFW network request 前成功寫
`running`；失敗就 fail before network。S3 root cutover 後才寫 `succeeded`；之前的
exception 寫 `failed` 並保留 spool。Frontend freshness SSOT 仍是 CDN root manifest，
DB health RPC 只供運維。
若 root 已 cutover 但最後 `succeeded` ledger 短暫失敗，job 會有限重試 3 次；
仍失敗時不會反向寫 `failed`，而是保留
`cutover_succeeded_ledger_pending` spool 與 `reconcile-ledger.json`供對帳。

當前 truth：migration 375、Zeabur production flags、S3 manifest-last publish 與
Cloudflare public origin 已用於首次 `2026-08-20` release；排程為每日 08:30
Asia/Taipei（UTC 換日後，避免手動 post-08:00 release 被隔日重複）。尚未驗收的是 Cloudflare **root edge cache**（public URL 可讀且
Cache-Control 正確，但實測 `CF-Cache-Status: DYNAMIC`），不得寫成整個
production 或 public origin 尚未部署。
