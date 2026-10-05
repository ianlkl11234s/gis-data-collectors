# GD-2：storage 層亂碼防護（allowlist 版）

日期：2026-10-05

## 結論

`SupabaseWriter` 對「文字欄位全為臺灣中文」的 collector 檢查 Cyrillic `U+0400–U+04FF`（解碼失敗訊號）。範圍由 `MOJIBAKE_GUARD_COLLECTORS` allowlist 決定，預設不檢查：

- `ncdr_alerts`：NCDR CAP 全為臺灣政府中文，2022–2026 曾間歇寫入亂碼列。

不加入的來源：`global_events`、`aisstream`、`usgs_earthquake`、`satellite`（國際新聞、船名、地名、TLE 名稱合法含西里爾字母）。`wra_drought_alert` 為中文 HTML scrape，但未逐欄確認，暫不加入；CWA 警特報目前無對應 collector。

## 語意

- `reject_mojibake_records()` 回傳過濾後的 records；含 Cyrillic 的列被剔除，其餘列照常寫入。
- logger.error 只列 collector、列 index、欄位路徑，不印原文。
- 只有「至少 1 列且全部列都壞」才 raise `MojibakeWriteRejected`；該例外不進 buffer、不借 DB connection。
- 呼叫點：`write()`、buffer retry、`_write_to_db()`（direct caller / multi-table）。satellite TLE 旁路不在名單內，不檢查。
- 不轉碼、不填補；來源修正或重抓由上游負責。

## 驗收

- 名單外 collector 帶西里爾字母：不檢查、不 raise。
- 名單內 10 列含 1 列亂碼：寫入 9 列並記錄 error（log 不含原文）。
- 名單內整批亂碼：raise，mock pool 未借出 connection，buffer 為空。
- `_write_to_db` direct call 整批亂碼：未建立 cursor 即 raise。
- Mutation：把 `CYRILLIC_RE` 改成 `r'$^'`，4 個 guard 測試轉紅（26 passed / 4 failed，DID NOT RAISE）；還原後轉綠。
- 全套 `python3 -m pytest -q`（`/private/tmp/gd2-contract-venv`，補 ijson / OpenCC）：**591 passed、2 skipped**。不使用 live Supabase。
