# Global Events 24h / 15m shadow capture

`scripts/global_events_history_capture.py` 是獨立的 GDELT GKG metadata capture。它只呼叫公開 GDELT master index 與 ZIP，重用 `collectors.global_events.parse_gkg_artifact()` 逐檔解壓串流解析；不建立 candidate、不呼叫 JEV/Qwen/OpenRouter、不寫 Supabase/S3，也不變更既有 collector checkpoint。

## 執行

```bash
python3 scripts/global_events_history_capture.py --mode history --hours 24 --output-dir data/global_events_history_capture
```

可重播的指定區間（`end` 不含在區間內）：

```bash
python3 scripts/global_events_history_capture.py --mode history --start 20260918000000 --end 20260919000000 --output-dir data/global_events_history_capture
```

未來每 15 分鐘排程使用：

```bash
python3 scripts/global_events_history_capture.py --mode run-once
```

`run-once` 選取目前 UTC 15 分鐘邊界之前的完整 slot。例如 12:23 UTC 選 12:00 UTC。建議排程在邊界後數分鐘啟動，給 GDELT 發佈 ZIP 的時間。

## 輸出契約

- `slots/<stream>/<slot>.json`：完整 parsed metadata；每筆 record 只有 GKG URL、標題、來源、時間、theme/location/person/organization/tone 與 parser 衍生訊號，沒有 article body。
- `checkpoint.json`：每個 `stream/slot` 的 `succeeded` 或 `failed` 狀態。成功 slot 且檔案仍存在時會 skip；失敗 slot 下次會重抓。
- `runs/run_<UTC>.json`：本輪選取 slot、來源 index manifest、整輪及各 slot 的 wall time、狀態與匯總。

每個 slot 都有 `input_count`（parser 輸出的 record 數）、`unique_count`（同一 slot 的 `url_norm` 去重數）、`late_count`（文章 GKG timestamp 比該 ZIP slot 早超過 15 分鐘的 record 數）、`downloaded_bytes`、`metadata_bytes`、`estimated_model_input_tokens`。token 是 UTF-8 metadata bytes / 4 向上取整的前置估算，並非任何模型或供應商的實測 usage。

ZIP 僅在單一 slot 的驗 MD5 與解析期間存在記憶體，沒有 ZIP 或解壓副本 cache；持久化的是 index manifest、其 SHA-256、provider MD5、實際下載 ZIP SHA-256、metadata、metrics 與 checkpoint。公開 index 抓取失敗、requested slot 不在 index、ZIP HTTP/校驗/解析失敗都會記為 error 並讓 CLI 回傳非零，絕不以空資料成功。`OPENROUTER_API_KEY` 只會以 `openrouter_api_key_present` 布林值紀錄，從不讀取或輸出其值。

## 離線跨 slot consolidation

```bash
python3 scripts/global_events_history_consolidate.py \
  --input-dir data/global_events_history_capture \
  --output-dir data/global_events_history_capture/consolidated
```

它不讀 secret、不連網。每次只解析一個 slot，將 URL aggregate 與 counter 寫入 `output-dir/.consolidate.sqlite3`；最後由 SQLite `ORDER BY url_norm` cursor 逐列輸出，因此不會把 24 小時 records 或 NDJSON 再次整份載入 RAM。暫存 DB 在成功或失敗後都刪除。`events.ndjson` 依 `url_norm` 穩定排序，exact URL 跨 stream/slot 去重；canonical record 選最早 `gkg_slot`，同 slot 再按 stream/title/record id 排序。每列都保留 required GKG metadata、first/last seen slot、source streams、occurrence/variant count、每個 title variant 的 slot/stream/record-id provenance，因此同 URL 的標題更新不會被靜默丟失。

`summary.json` 包含 raw parsed、cross-slot unique、duplicate、domain/stream/slot counts、time coverage、input/output bytes 與 NDJSON SHA-256（寫出時同步計算，未重新讀取輸出）。slot JSON 無法解析、path 與 artifact 不符、或 record 缺少必要 metadata 時，summary 會是 `failed`、CLI 回傳非零，且不發布成功 NDJSON。
