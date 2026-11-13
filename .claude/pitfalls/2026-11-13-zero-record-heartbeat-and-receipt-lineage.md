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

payload 裡已經寫死 NULL，修程式救不了它。當時 `flush_buffer` 只會在超過 `BUFFER_MAX_AGE_DAYS`（3 天）後丟棄，期間每 5 分鐘重試失敗一次。同一 batch 已由後一輪寫入，人工移除不會丟資料。

### 後續：資料不合法的 buffer 檔改為隔離（同日）

`flush_buffer` 補寫失敗時先用 `classify_invalid_data_error()` 分類，只有資料庫明確回報「資料本身不合法」才隔離：

- 依 `pgcode`：23514（CHECK）、23502（NOT NULL）、22P02、22007、22003、42804。拿不到 `pgcode` 才比對英文訊息。
- 23514 另要求帶 constraint 名或訊息寫明 check constraint，且訊息不含 partition，因為分區表「找不到分區」也是 23514，屬暫時性問題。
- 逾時、連線中斷、鎖等待、唯一鍵、外鍵、權限、writer 自己丟的 `ValueError` 等一律照舊重試（寧可漏判）。

判定不合法的檔移到 `buffer/quarantine/`，log 一行（collector、檔名、類別、SQLSTATE、constraint／欄位名，不印整列資料），不算進「連續失敗」。`buffer/*.json` 不遞迴，所以隔離檔不會被重試、也不會被 3 天過期邏輯丟掉。隔離檔從移入時起保留 `BUFFER_QUARANTINE_MAX_AGE_DAYS`（30）天後刪除；亂碼防護的隔離檔共用同一資料夾，也一起適用。日報（精簡版「現在還壞的」）在有隔離檔時列一行 🟡 各 collector 檔數。

要重試某個隔離檔：修好資料或 DB 後移回 `buffer/`；但 payload 時間超過 3 天會被當過期直接丟棄。

HiCloud VM 的 `external/vm_common/vm_buffer.py` 是另一套補寫，當時沒改；同日補上（COL-GE-2）：

- VM 單檔部署、不帶 `storage/`，所以 `vm_buffer.py` 自備一份 `classify_invalid_data_error()`（同一組 SQLSTATE 與 23514 分區排除），不 import psycopg2、改看例外有沒有 `pgcode`。兩份要同步，`tests/test_vm_buffer.py` 有對照測試逐案比對結果。
- `flush_pending()` 遇到判定不合法的檔：rollback、移到 `<buffer_dir>/quarantine/`、log 一行（檔名、類別、SQLSTATE、constraint／欄位名），繼續補後面的檔；其餘錯誤照舊「rollback＋中止本輪」。隔離檔不計入回傳的（成功數, 丟棄數），`has_pending()` 也看不到它們。
- 隔離檔保留 `QUARANTINE_MAX_AGE_DAYS`（30）天，在 `flush_pending()` 開頭清理；collector 若因 `has_pending()` 為假而沒呼叫 flush，那輪就不清（最多晚幾輪）。
- 要部署才生效：把新版 `vm_buffer.py` 覆蓋到 ship_ais、waste_positions、cdc_public_health_weekly、isohe_port_marine 四個 app dir。

### 後續：0 筆心跳的尾巴（COL-ZERO-1，同日）

1. **吞例外後回空結果**：下列收集器在「本輪 0 筆且有來源拋例外」時帶 `_collector_error`，不再被記成 0 筆成功；正常抓到 0 筆、夜間暫停、已入庫略過等照舊成功。
   - collect() 直接吞：`wic_sewer`、`wic_evacuate`、`wic_pumb`、`tpml_seat`、`uswg`、`pla_activity_daily`（列表抓取失敗）。
   - 多來源迴圈全空且有來源失敗：`youbike`、`bus`、`parking`、`parking_offstreet`、`road_congestion`、`waste_positions`、`iot_wra`、`immigration_apis_airport`、`precipitation_raster`、`earthquake`（有感報告與海嘯都空）、`er_hospital_realtime`、`cwa_satellite`。
   - helper 吞例外回 None：`global_climate_jtwc`（RSS 抓不到才算失敗；RSS 正常但沒有颱風仍是成功）。
   - 沒改（helper 把「抓失敗」和「上游沒資料」混成同一個回傳值，要動 helper 才分得開）：`food_prices._get`、`earthquake_moment_tensor._fetch_cwa_events`、`air_quality_imagery._fetch_png`、`global_climate_cmems`、`global_climate_cams`、`global_climate_noaa_gfs`、`flight_fr24`（資料是當日累積清單）。
   - 帶 `_collector_error` 且 0 筆時，writer 不報成功，`run()` 因 writer 已被呼叫也不補報失敗：DB 的 `collector_status` 這輪不動（`last_success_at` 停住），記憶體內的失敗計數與日報照常。
2. **沒有 transformer 的 collector**（`tra_static`、`parking_ref`、`waste_match`、`global_climate_bake`；`ship_tdx`、`vd` 已停用）：不只 0 筆，任何筆數都不經 writer 報成功心跳；collect() 拋例外時 `run()` 卻會報失敗，所以一次失敗後 DB 上會一直是 error。日報與 `staleness_watch` 用記憶體狀態，不受影響。沒有改：通用修法需要排除自報的 `satellite_passes_daily`，records 也只能填 0；建議要 DB 狀態的那幾支比照 `satellite_passes_daily` 自報（`parking_ref`、`waste_match` 有真實筆數）。

## 教訓

- 「0 筆」要分成兩種：上游真的沒有事件（成功），以及上游有項目卻一筆都沒解析出來（失敗）。前者要寫成功心跳，後者要 raise。
- 跨批次的彙總值（hash、計數）在所有批次結束後一次算；不要在迴圈裡邊做邊指派，否則最後一批的失敗路徑會把它留在中間狀態。
- 資料本身違反 constraint 的 buffer 檔重試永遠不會成功，只是白白重試到過期；這類錯誤和「DB 暫時不可用」要分開處理，現已改為依 SQLSTATE 隔離（見上方「後續」）。
