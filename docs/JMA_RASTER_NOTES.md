# HANDOFF：jma_raster（2026-10-10 更新）

## 2026-10-10 進度
- 主 agent 拍板：rasrf/キキクル只收 none、flood 只冷存 pbf、himawari z5、雷達每幀 ≤288（48＋前 15 磚 z8）、暫定色票表外顏色計數告警。
- 使用者追加：T3 磚級去重（blank 只記 manifest）、T2 每幀網格永久（每小時一檔、檔內 zero/same_as_prev 去重；lifecycle 90 天轉 GLACIER_IR）、
  z8 補抓段 backlog（JAPAN_JMA_RASTER_BACKLOG_RPS 預設 0.5，依回波量排序，過期未抓記 fine_unfetched）、T1 只是衍生物。
- 已完成：collector `collectors/global_climate/jma_raster.py`、`collectors/weather_raster/{decode,spool}.py`、s3.py 加 upload_path/head/delete、
  12 步註冊、tests/test_jma_raster.py（20 個，全套 650 綠）。gis-platform mig 437 已寫並 BEGIN…ROLLBACK 預演通過（未 apply）、兩份清冊已更新。
- S3 smoke：`weather-raw/_smoke/jma_raster_smoke_20261010T062206.tar` 已上傳 DEEP_ARCHIVE、head 驗證（大小、sha256、StorageClass）、**已刪除**；bucket 無 versioning，無殘留。
- 本機實打腳本：scratchpad/jr_scripts/live_run.py、measure_store.py（輸出 scratchpad/jr_live/）。
- 本機實打（2026-10-10 15:20 JST，小雨日）：radar 13 幀每幀 256 req（13/48 z6 有回波，全下鑽，backlog 0）、約 33–43 s/幀；
  T3 去重 13 幀 1.76MB→blob 0.98MB、tar 1.14MB；T2 deflate 515KB/13 幀（lzma 329KB 但峰值 RSS +90MB）；T1 333KB/日。
  峰值 RSS：抓取 85MB、收尾（T2 deflate＋累加器 chunk memmap＋T3）126MB（macOS ru_maxrss）。
  backlog 實打：max_fine_tiles=5 時 0.5 rps 64 秒補 2 磚（32 req）。
- 狀態：功能完成，待主 agent 驗收（apply 437、S3 lifecycle weather-grid/ 90 天轉 GLACIER_IR、Zeabur env）。


契約：scratchpad/jma_raster_contract.md；ADR-0021；analytics docs/api-platforms/jma/gotchas.md。
探測腳本：scratchpad/jr_scripts/（probe_zoom.py、scan.py）；JMA 前端設定 XML：scratchpad/jr_probe/*.properties.xml。
（註：上述 scratchpad 已於 2026-10-11 電腦休眠後被清空，live_run.py 等腳本不在了。）

## 第二批調整（2026-10-11）

使用者拍板五項；gis-platform migration 438（分支 feat/jma-warning-events，已 BEGIN…ROLLBACK 預演，**未 apply**）。

### 1. キキクル每 30 分一幀
- `JAPAN_JMA_RASTER_CADENCE`（預設 `risk_land=30,risk_inund=30,risk_flood=30`），格點從 2000-01-01 UTC 起算 → 只收 :00／:30。
  必須是原生間隔（PRODUCTS.cadence_min）的倍數，否則忽略。上游留 5.5h、每小時一輪，最多 11 幀可補。
- 變更間隔當日已抓的非格點幀：close_day 一併打包進 T3（manifest `off_cadence_frames`），不隨 spool 刪掉；frames_ok 只計格點幀。

### 2. 積雪只收 11–4 月（JST）
- `JAPAN_JMA_RASTER_SEASON`（預設 `snow_depth=11,12,1,2,3,4`）。季外 validtime 在 expected_vts 裡就不存在：不抓、不產 frames 列、不算 missing。
- 保留期內完全沒有季內時刻時不打 targetTimes；但仍設定視窗，讓 4/30 這種最後一天照常在 5/1 01:00 JST 日結。

### 3. 粗圖探測（coarse probe）
- `JAPAN_JMA_RASTER_COARSE_PROBE=off|shadow|on`，預設 shadow。z4 畫布＝x13..14、y5..6（2×2）；**修正 canvas(z<6)**：舊版 floor 只算到 y5，z4 少一排。
  z4 (13,5)/(14,5) 各對應 8 個畫布內 z6，(13,6)/(14,6) 各 16 個。
- 實測（2026-10-11 06:00–06:55 JST）：radar 6 幀、rasrf 2 幀 z4 有內容，4 象限與 z6 一致，miss 0；risk_land／risk_inund／snow_depth z4 回 200，
  但當天全國無著色（只見 334B 佔位），**z4 會不會畫出小範圍著色未驗證**；risk_flood z4 pbf 200／0B（與 z6 同）。himawari 原生只到 z5，不適用。
  SSOT：`Product.probe_zoom`（decode table 的 `coarse_probe_zoom` 只是紀錄）。
- shadow：多 4 請求／幀，照舊全抓 z6；`probe_misses`＝z4 全空但子 z6 非空的磚數（log ⚠）。on：z4 全空的區不抓 z6／z8，
  網格填 0（與 334B 佔位同語意），frame／manifest 記 `inferred_blank_tiles`（推定為空），`tiles_inferred_blank` 進 DB；z4 第一磚 404＝上游無此幀。
  探測磚也進 T3 tar（z4/…）。DB：frames 加 probe_mode／probe_misses／tiles_inferred_blank（mig 438）。
- 建議：shadow 跑過一段有キキクル著色／降雪的日子、`sum(probe_misses)=0` 再切 on。

### 每日請求數估計（不含 backlog 補抓、targetTimes）
| 產品 | 第一批 | 第二批 shadow | 第二批 on（估） |
|---|---|---|---|
| radar（288 幀；小雨日每幀 256） | 73,728 | 74,880 | 小雨日約 72,600；全乾日 1,152 |
| rasrf（48 幀） | 約 12,300 | 約 12,500 | 約 12,100 |
| risk_land＋inund＋flood | 144×48×3＝20,736 | 48×52×3＝7,488 | 無著色日 48×4×3＝576 |
| snow_depth | 24×48＝1,152 | 季外 0（季內 24×52＝1,248） | 季外 0 |
| himawari_b13 | 288 | 288 | 288 |
| 合計（10 月小雨日） | 約 108,000 | 約 95,000（−12%） | 約 86,000；全乾日約 14,000 |

### 4. 警報只記變動（collector jma_warnings）
- 每輪（map_time 有變）比對 state 的有效集合：新出現 start、消失 end（status 取同快照解除原文，否則「解除」，描述欄沿用上一輪）、status 改變 status_change。
  寫 `live.jma_warning_events`（UNIQUE 含 change_type，DO NOTHING，永久）；停寫 live.jma_warnings、不再有哨兵列。
- state `state/jma_warnings_state.json`＝{last_control_datetime, active}；只在 DB 寫入成功後前進。state 遺失或舊格式 → 用 writer 連線池唯讀查
  `public.jma_warnings_current` 重建；查不到就整輪失敗（絕不全部當 start）。438 前後該 view 欄位相同，同一句跨切換可用。
- **jma_warnings 寫入失敗不進 writer buffer**（`SupabaseWriter.NO_BUFFER_COLLECTORS`）：否則下一輪用新 control_datetime 重新差分＋buffer 補寫舊輪 → 同一變動兩筆。
- 整份有效快照放 `result['raw_payload']`：只進本地 JSON → 每日 archives（S3），不進 DB／通知。
- mig 438 由 live.jma_warnings 全部快照逐輪差分回填；預演：舊 view 694＝新 view 694、集合差 0、欄位差 0；130 輪→5,708 事件（start 2,869／status_change 664／end 2,175），
  平均每輪 44 列（舊做法每輪 694–1,002 列）。
- 已知語意差異：view 的 control_datetime／collected_at 變成「該列最後一次變動」；同 status 只換 report_datetime（継続再發）不算事件 → view 中 87 列 report_datetime 比舊 view 舊
  （回填期間共 622 次這種更新，約事件量 11%）。要保留可加第四種 change_type，待使用者決定。

### 5. AMeDAS 每日 Parquet（collector jma_amedas）
- 每輪結尾 `close_daily`：JST 日 D 在 D+1 01:00 後、無 receipt 就日結（每輪最多 1 天；只結 `_first_day` 之後的日子，不回補上線前）。
  讀本機 `jma_amedas/YYYY/MM/DD/*.json`（**台北日期目錄**：JST 日 D＝台北 D-1 與 D 兩個目錄）的 current 列，以 observed_at 過濾、(station, observed_at) 去重；
  缺的 10 分鐘時刻向上游 map/{stamp}.json 補抓（實測上游可回溯約 7 天），所以本機 2 天保留／台北 03:00 刪檔的 3 小時窗口錯過也不會漏。
- 逐小時寫 row group（zstd），上傳 `weather-obs/jma/amedas/YYYY/MM/YYYYMMDD.parquet` STANDARD，`storage.s3.put_verified`（head 大小＋sha256，已存在相符就跳過；raster 也改用同一個）。
  receipt `state/jma_amedas_parquet/{day}.json`；失敗 → `_collector_error`（告警）、下輪重試；`JAPAN_JMA_AMEDAS_DAILY_PARQUET=false` 可關。
- 實測 2026-10-11（全部從上游補抓）：185,184 列、1,286 站、144/144 時刻、**2.31 MB**、212 秒（144 次上游請求；正常走本機檔不需要）、峰值 RSS 149 MB（macOS，含直譯器與 pyarrow）。
- pyarrow 早已在 requirements（>=14，export_daily_trails 用），image 不增加 → 選 (a) Parquet。

### 順手修的既有 bug（jma_raster）
- missing 定案原本每輪從 epoch 掃到 window_start，closed_days 又只留 10 天 → 上線約 11 天後會把已日結的舊日期重寫 missing、重建 spool，
  再日結時以空 tar **覆蓋 S3 上的 T3**。改為 state `finalized_until` 只往後掃，並以 `_receipts/{product}/{day}.json` 擋已日結日。

### 上線順序（主 agent）
1. apply mig 438（回填＋view 切換）。2. merge／部署 data-collectors（新 jma_warnings 首輪從 view 重建 state）。3. restart。
   反過來（先部署）會讓 jma_warnings 每輪寫入失敗（表不存在）但 state 不前進、不重複，apply 後自動恢復。
4. env 皆有預設，不必設；要關 Parquet 設 `JAPAN_JMA_AMEDAS_DAILY_PARQUET=false`；確認 shadow 無 miss 後再設 `JAPAN_JMA_RASTER_COARSE_PROBE=on`。
5. S3 `weather-obs/` 沿用全桶 lifecycle（IA／GIR），不可加到期刪除。


## 已完成
- 兩個 worktree：data-collectors `.worktrees/jma-raster`（feat/jma-raster）、gis-platform `.worktrees/jma-raster`（feat/jma-raster-tables，尚無改動；437 是下一號，已掃遠端分支與 worktree 確認最大 436）。
- 解碼表 `config/decode_tables/jma/{radar,rasrf,risk_land,risk_inund,snow_depth}.yaml`。
- `collectors/weather_raster/decode.py`（P-mode 依 PLTE 建 LUT、表外顏色計數→255、pool_max、upsample）。
- **S3 尚未做任何上傳，沒有 `_smoke` 物件要清。**

## 實測（2026-10-09）
| 產品 | 端點／member | 原生 zoom | 色票 | 備註 |
|---|---|---|---|---|
| radar hrpns | nowc/{bt}/none/{vt} | 偶數層 4/6/8/10（maxNativeZoom 10；奇數層 334B 佔位） | PLTE 已驗：idx0/1 tRNS 透明，idx2–9 = 242,242,255／160,210,255／33,140,255／0,65,255／**250**,245,0／255,153,0／255,40,0／180,0,104 → <1…≥80 mm/h | targetTimes_N1 只露 3h，保留 24h → 要用 5 分 cadence 算術回推 |
| rasrf | rasrf/{vt}/**none**/{vt}（契約寫 immed 要改） | 同上 | PLTE 與雷達相同；legend kaikotan rasrf 1h 雨量同刻度 | immed 只在最新 basetime 有（初報，像素數不同）；30 分解析值為 member=none 且 elements 含 rasrf、bt==vt |
| risk land/inund | risk/{vt}/{member}/{vt} | maxNativeZoom 11、zoomUse even | legend SVG（未以 PLTE 驗）：注意 242,231,0／警戒 255,40,0／危険 170,0,170／災害切迫 12,0,12；白=留意 | member 隨時間老化：最新 3 幀 immed0/1/2（none 404），之後變 none。建議只收 none |
| risk flood | **pbf 向量磚**（type=pbf） | ? | 不適用 | 不是 raster；建議 v1 只留 raw 或拿掉 |
| snow snowd | snow/{vt}/none/{vt} | maxNativeZoom 10 | legend SVG（未驗）：7 色 160,210,255…180,0,104，刻度 5/20/50/100/150/200 cm | 今日全日本 334B 無積雪；收 z6 即可 |
| himawari B13 | himawari/data/satimg/{vt}/fd/{vt}/B13/TBB | **只有 z3–z5**（z6+ 404） | jpg 不解碼 | z5 12 磚 x26–29 y11–13，Tokyo 磚約 6.6KB |
- 空磚判定：334B 透明 RGBA，或 P-mode 全 index≤1（radar z8 有 228–246B 的有內容磚，不能只看 bytes）。
- z6→z8 每磚 16 子磚：全滿 48+768 req/幀，超過 ADR 預算 300 → 依回波像素排序只下鑽前 15 磚，其餘 z6 upsample。
- 記憶體尚未量測（用 resource.getrusage，venv 無 psutil）。

## 設計定案（advisor 已審）
- 每產品 state：保留期內 validtime 視窗 map；oldest-first、每輪幀數上限；首次跑設 epoch（now-1h），之前不算缺幀；超出保留期才定案 missing。
- spool `/data/weather_raw/jma/{product}/{YYYYMMDD}/`：frames/{vt}.tar（決定性 tar）＋{vt}.json receipt、grids/{vt}.npz、acc/ memmap（count_ge_k uint16、max uint8、missing_count、applied.json；崩潰時從 grids 重建）。
- T2：整點收尾，zip 內串流寫 stack.npy（不全進 RAM）。T1：從 memmap 串流寫 npz＋georef。
- T3：決定性 day tar＋manifest.json → sha256 → head（已存在且 sha 相符跳過）→ upload_file DEEP_ARCHIVE（Metadata sha256/bytes，TransferConfig 低並發）→ head 驗 ContentLength/Metadata/StorageClass → 本機 receipt → 刪 spool → daily row。s3.py 需加 upload_path＋head 方法（大檔不可整包進記憶體）。
- DB：TABLE_MAP is_multi_table，_type frame/daily；frames 建議 ON CONFLICT DO UPDATE 狀態只升不降（契約建議）；require_db_write＋覆寫 run（照 jma_quake）。should_persist_local 保留預設（row 小，進 jma_raster/archives/）。

## 下一步
1. `collectors/weather_raster/spool.py`（frame store、accumulator、hour stack、day pack）＋ `collectors/global_climate/jma_raster.py`。
2. s3.py 加方法；12 步註冊（registry、config toggle JAPAN_JMA_RASTER False/60、TABLE_MAP、transformer＋_write_multi_table、cross_layer_map、realtime_tables、backup_manifest、zeabur.json、retention override、archive INTERNAL_DATA_DIRS 加 weather_raw 可選）。
3. tests/test_jma_raster.py（測試內用 Pillow 生成 PNG；mock S3）；全套 pytest。
4. gis-platform 437（照 435：兩表、RLS loop、兩 public view、retention_policies＋frames 400d cron）；BEGIN…ROLLBACK 預演；data-inventory.md、data-safety-inventory.md。
5. 本機實打（radar 12 幀＋其他各 1 幀），量 RSS、bytes；S3 `weather-raw/_smoke/` 小檔 DA 上傳驗證後刪；推估容量月費；docs/AWS_INVENTORY.md。
