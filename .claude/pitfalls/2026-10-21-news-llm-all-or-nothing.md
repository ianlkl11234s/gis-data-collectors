# 2026-10-21：news_events 額度用完停 4 天、加值後又被「一批失敗整輪放棄」卡 9 小時，狀態表一直顯示 0 錯誤

## 時間軸（台灣時間）

- **2026-10-16 17:48** — 最後一筆新聞寫入。OpenRouter 帳號額度用完（`402 Insufficient credits`），之後每輪的 LLM 標註都失敗，整輪放棄。
- **10-16 → 10-21** — `metadata.collector_status` 一直是 `consecutive_errors = 0`、`last_success_at` 停在 10-16，沒有任何 DB 端告警；Pulse 監看面板顯示「可供地圖使用的定位新聞只到 2026-10-16」。
- **10-21 16:26** — 使用者加值。OpenRouter 恢復，但收集器每輪仍在寫入前放棄。
- **10-21 18:14** — 某一輪剛好所有批次都成功，一次寫進積壓，之後恢復正常。
- **結果** — 10/17–10/20 的新聞在恢復前已從 RSS 消失，各天只剩 13–117 則，補不回來。

## 根因

1. **全有全無**：`_annotate_items` 只要一批（15 則）的 LLM 標註失敗，就 raise，整輪不寫。平常每輪只有一兩批，偶爾失敗下一輪就補上；停了 5 天後積壓 412 則、28 批，本機完整重跑一次有 5 批失敗（2 批模型回傳 JSON 壞掉、2 批 429、1 批 400），幾乎每輪都會碰到至少一批失敗，所以加值後還卡了 9 小時。每輪都重送全部積壓，成功批次的費用也白花。
2. **錯誤原因被吞**：`raise NewsAnnotationError(...) from None` 只留下「batch failed (15 records)」，看不出是額度、限流還是格式問題；這次得在本機逐批重跑才查到。
3. **狀態表不知道失敗**：`collector_status` 只由 `supabase_writer.write()` 回報心跳。`collect()` 本身 raise 時根本不會呼叫 writer，所以表上永遠是「0 次連續錯誤」。

## 修正（PR fix/news-partial-batches）

- `_annotate_items`：單批失敗只跳過該批（該批不標 `_annotated`、不入庫，下一輪去重時仍是新項目會重送）；全部批次都失敗才中止整輪。批內仍是原子的：一則欄位不合法，整批跳過。
- 失敗原因只記例外類別與 HTTP 狀態碼（例：`HTTPError 429`、`JSONDecodeError`），不記例外內文（D-05）。
- `BaseCollector.run()`：`collect()` 失敗且 writer 沒被呼叫時，自己回報失敗心跳（`report_collector_heartbeat(name, false, 0, error)`）。這對所有 collector 生效。

## 教訓

- 對外部服務逐批處理時，單批失敗只跳過該批，不要讓一批拖垮整輪；全部失敗才算整輪失敗（I-05）。
- `collect()` 失敗也要寫心跳，否則狀態表看不到「有在跑但每輪都失敗」（H-05）。
- 失敗訊息要帶得出原因的類別，否則診斷只能重跑。
