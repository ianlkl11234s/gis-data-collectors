# 2026-11-06：aisstream、ripe_ris_live 的 spool 孤兒 .jsonl 永遠不上傳

## 現象

正式主機的 `aisstream_spool` 累積 80 個 `.jsonl`（最舊 08-25）、`ripe_ris_live_spool` 40 個（最舊 08-31），S3 上沒有對應副本。

## 根因

- 兩支 worker 只有 `rotate()` 會把正在寫的 `.jsonl` 壓成 `.jsonl.gz` 再上傳。
- `retry_pending()` 只找 `*.jsonl.gz`，而且只在 worker 啟動時呼叫一次。
- worker 被 kill 或重啟時，當下開著的 `.jsonl` 沒經過 `rotate()`，變成孤兒：不會被壓縮、也不會被上傳。
- `_upload` 以檔案 mtime 決定 S3 的 `date=`／`hour=` 分區，所以補傳孤兒檔時必須保留原 mtime。

## 修法

- `utils/spool_recovery.py::compress_orphan_jsonl()`：把「不是目前寫入中」的 `*.jsonl` 壓成 `.gz`（先寫 `.gz.tmp` 再 `os.replace`，保留 mtime，空檔直接移除，失敗保留原檔）。
- 兩支 worker 的 `retry_pending()` 先呼叫它，再走既有的上傳／驗證／刪檔流程。
- 每次 rotate 後也呼叫 `retry_pending()`，上傳失敗的檔不必等到下次重啟。
- 測試：`tests/test_spool_recovery.py`。

## 教訓

「落地檔只由 rotate 這一條路徑收尾」代表任何非正常結束都會留下孤兒；重試機制要掃描所有可能的中間狀態（`.jsonl`、`.jsonl.gz`、`.tmp`），不能只掃最後一種。
