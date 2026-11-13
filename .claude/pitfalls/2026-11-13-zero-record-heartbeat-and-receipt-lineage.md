# 2026-11-13：0 筆成功不寫心跳；global_events receipt 缺 hash 卡 buffer

兩件事同一天查到，互相獨立。

## A. 0 筆成功被當成壞掉

### 現象

- `lightning_cwa`（氣象署落雷）：`metadata.collector_status` 顯示 `status=error`、`consecutive_errors=15`、`last_success_at` 停在 11-06。實際每 5 分鐘都在跑、log 每輪「✓ 已儲存」、上游 HTTP 200，只是 11 月沒有落雷。
- `ncdr_alerts`（災害示警）：feed 內 0 筆生效中示警、每輪正常結束；長版日報把 `live.disaster_alerts` 標「🔴 DEAD 落後 715 分」，還排進「今日 Action」第一項。

### 根因

1. `storage/supabase_writer.py::write()` 在 `if not records: return True` 直接返回，不呼叫 `report_collector_heartbeat`。`BaseCollector.run()` 呼叫 writer 前已設 `writer_reported=True`，所以這輪也不會補報。偶發的真失敗（連線被重置等）由失敗心跳累加 `consecutive_errors`，之後的 0 筆成功永遠清不掉。2026-10-05 健康調查就列過這個盲點（operations.md §3.2 #3），當時沒修。
2. `tasks/daily_report.py` 的長版日報（寫 S3 那份）四個區塊（健診摘要、Supabase realtime、異常趨勢、今日 Action）直接用表的最新時間判 DEAD，沒有套 `monitoring.stale_check_skip_reason`；只有精簡版的 `_stale_tables` 有套。`live.disaster_alerts` 本身也沒標 `event_driven`。

### 修法

- `write()` 的 0 筆分支回報成功心跳（records=0）。不回報的兩種：沒有 transformer 的 collector（自己直寫 DB、自報心跳，例 `satellite_passes_daily`），以及 result 帶 `_collector_error`（`run()` 稍後會算失敗）。不呼叫 `alerting.mark_write`，即時告警的「新資料」仍只算 ≥1 筆。
- `ncdr_alerts`：feed 有示警、但 0 筆解析成功（CAP 全部下載失敗、XML 壞、亂碼全擋）時 raise，不讓它變成「0 筆成功」。
- `realtime_tables.yaml`：`live.disaster_alerts` 加 `event_driven: true`；收集器停擺改由 `staleness_watch` 看最後成功執行。
- 長版日報四個區塊與跨層一致性 [B] 都改成和精簡版同口徑（事件驅動、季節外、停用的表不判停更）。

## B. global_events receipt 違反 accepted_lineage，buffer 每 5 分鐘重試失敗

### 現象

pulse-host log 從 03:05 起每 5 分一次：`Buffer 重試失敗：global_events_20261113_030518.json: new row for relation "global_event_collector_run_receipts" violates check constraint "global_event_collector_runs_accepted_lineage"`。條件要求非 failed 的 receipt 帶齊 6 個 lineage 欄位。

### 根因

`collectors/global_events.py::_assess_in_chunks()` 每個 cache miss 的 chunk 先把 `self._raw_response_sha256` 設成 None 再呼叫模型；成功的 chunk 在迴圈內才把彙總 hash 指派回去。如果**最後一個**送出的 chunk 失敗（`requests.ConnectionError`／`Timeout` 歸類 provider、截斷回應歸類 content，都只隔離該 chunk、整輪仍是 accepted），彙總值就停在 None（或被截斷回應的 hash 蓋掉）。迴圈後的保底只在「一個成功的 chunk 都沒有」時才補值。這輪是 accepted partial（72 筆候選、65 筆評估），receipt 的 `raw_response_sha256` 是 NULL，第一次寫入就違反 CHECK，進 buffer 後同一份內容每輪重送都一樣失敗。

佐證：正式庫該 run_id 沒有 receipt；同一個 batch（內容定址）已由下一小時的 run 以 accepted、0 個失敗 chunk 寫入。前一小時另有一個 failed receipt，錯誤是 `Response ended prematurely`，表示當時 provider 不穩。測試以「第三個 chunk 失敗」重現：provider 斷線時是 None，截斷回應時是錯的 hash。

### 修法

彙總 hash 改在迴圈結束後才由 `response_hashes` 計算；沒有任何成功回應時沿用原本的保底（空 Stage1 artifact 的 hash）。

### 那個 buffer 檔

payload 裡已經寫死 NULL，修程式救不了它。`flush_buffer` 對超過 `BUFFER_MAX_AGE_DAYS` 的檔會自動丟棄，所以不會無限重試。同一 batch 已由後一輪寫入，人工移除不會丟資料。

## 教訓

- 「0 筆」要分成兩種：上游真的沒有事件（成功），以及上游有項目卻一筆都沒解析出來（失敗）。前者要寫成功心跳，後者要 raise。
- 跨批次的彙總值（hash、計數）在所有批次結束後一次算；不要在迴圈裡邊做邊指派，否則最後一批的失敗路徑會把它留在中間狀態。
- 資料本身違反 constraint 的 buffer 檔重試永遠不會成功，只是白白重試到過期；這類錯誤和「DB 暫時不可用」要分開處理（見 PR 的待決事項）。
