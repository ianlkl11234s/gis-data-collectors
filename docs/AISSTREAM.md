# AISStream 長駐收集器

## 目的

`workers/aisstream.py` 是獨立於航港局 `ship_ais` 的 AISStream provider pipeline。
它用一條 WebSocket 訂閱五個區域，將 normalized position/static/current 與健康狀態
寫入 `live.aisstream_*`，不寫入既有 `live.ship_*`。

預設 `AISSTREAM_ENABLED=false`。啟用前必須先部署對應的 gis-platform migration，
並以 15–30 分鐘 smoke test 確認資料量、延遲、S3 archive 及 DB 寫入。

## 覆蓋區域

預設由 `config.AISSTREAM_BBOXES` 設定：

- 台灣北部與東部外海
- 與那國、石垣
- 宮古、沖繩
- 奄美
- 九州西南與東海入口

格式為 `[{"name":"...","box":[[lat_min,lon_min],[lat_max,lon_max]]}]`，
可透過環境變數覆寫；collector 會檢查座標範圍並拒絕空設定。

## WebSocket / resilience

- Endpoint 預設 `wss://stream.aisstream.io/v0/stream`。
- 訂閱 `PositionReport`、兩種 Class-B position、`ShipStaticData`、
  `StaticDataReport`。
- 使用 `websockets.sync.client`（`websockets>=16`）實際協商 RFC7692 per-message deflate，並支援 bytes → UTF-8 decode；socket open 不視為 healthy，需收到 subscription confirmation 或第一筆合法 AIS event。
- `PositionReport.Timestamp` 是 AIS UTC second（0–59），完整 `MetaData.time_utc` 優先，缺少時回退 collector received time，絕不當 Unix epoch。
- 連線中斷使用 full jitter exponential backoff（1 秒至 `AISSTREAM_RECONNECT_MAX_SECONDS`）。
- bounded queue 避免 DB 壅塞拖垮 socket；queue 滿時 raw event 仍先留在 local spool。
- local spool 每 `AISSTREAM_SPOOL_ROTATE_MINUTES` 分鐘轉 gzip NDJSON。
- DB 寫入使用既有 `SupabaseWriter` connection pool，不建立長期 Supavisor session。
- Supabase run/health ledger 與 S3 cold archive 是啟動硬條件；`main.py` 會在建立 daemon thread 前同步 preflight 並寫入 run ledger，任一失敗就讓容器啟動失敗，不會出現 Zeabur 存活但 worker 靜默死亡的假健康。
- `AISSTREAM_CAMPAIGN_DAYS=14` 期滿後自然停止；`worker.stop()` 可 graceful stop。

## S3 永久冷儲存

S3 key：

```text
aisstream/raw/v1/date=YYYY-MM-DD/hour=HH/aisstream-*.jsonl.gz
aisstream/raw/v1/date=YYYY-MM-DD/hour=HH/aisstream-*.manifest.json
```

每個 raw object 都會：

1. 以 `AISSTREAM_S3_STORAGE_CLASS`（預設 `GLACIER_IR`）上傳。
2. 以 `HEAD` 驗證 object size 與 `Metadata.sha256`。
3. 建立 manifest：SHA-256、bytes、筆數、首末時間、bbox config hash、schema version；再驗證 manifest HEAD metadata、size 與 JSON 內容。
4. raw object 與 manifest 都驗證成功、且 DB archive-manifest ledger commit 後才刪除本機 gzip spool；DB ledger 失敗會保留 spool 供下次 retry。

此 pipeline 不設定 S3 object expiration，不呼叫 delete，也不依賴
`archive.py` 的本機 7 天清理。bucket 層既有 lifecycle 若要改動，需先查閱
`docs/AWS_INVENTORY.md` 並經明確核准；本 prefix 的保存語意是永久冷 archive。

S3 upload 失敗時保留本機 gzip，worker 下次啟動會先 `retry_pending()`；磁碟容量達
`AISSTREAM_SPOOL_MAX_MB` 的後續 hardening 應告警並暫停 ingest，而不是刪除未驗證 raw。

## DB contract（與 gis-platform migration 對齊）

| 表 | 用途 | 去重／更新 |
|---|---|---|
| `live.aisstream_ingest_runs` | 每次 worker campaign lifecycle | `run_id`；start/end/status ledger |
| `live.aisstream_archive_manifests` | 已驗證 S3 raw object ledger | `object_key`；S3 HEAD + manifest 後寫入 |
| `live.aisstream_position_observations` | 每筆合法 position event | `(provider, raw_event_hash)`；保留 observed/received 時間 |
| `live.aisstream_vessel_current` | 每 MMSI 最新位置＋sticky static 欄位 | `(provider, mmsi)`；position 只接受較新的 observed_at，static 非 NULL 更新 |
| `live.aisstream_ingest_health` | worker heartbeat、數量、重連、錯誤 | `(provider)` upsert |

必要欄位至少包括 `provider`、`mmsi`、`observed_at`、`received_at`、
`message_type`、latitude/longitude、`raw_event_hash`；品質與原始證據由
`quality_flags` 及 S3 manifest 支援。raw payload 不直接寫入 normalized table。

## 驗收

```bash
# 只驗證設定與 bbox，不會連線（API key 不會輸出）
python3 - <<'PY'
from workers.aisstream import _subscription_boxes
boxes, names = _subscription_boxes()
assert len(boxes) == 5
print('bbox contract ok:', ','.join(names))
PY

# 本地 smoke（需要已設定 secret；建議先用短 campaign）
AISSTREAM_ENABLED=true AISSTREAM_CAMPAIGN_DAYS=0 python3 -m workers.aisstream
```

Zeabur 上線後另外檢查：WebSocket connected、訊息／position rate、reconnect、
spool backlog、S3 manifest、DB current row 與 `/health`；Zeabur CLI 由主 agent 統一執行。
