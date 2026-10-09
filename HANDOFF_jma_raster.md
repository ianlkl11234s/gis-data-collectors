# HANDOFF：jma_raster（2026-10-09 WIP）

契約：scratchpad/jma_raster_contract.md；ADR-0021；analytics docs/api-platforms/jma/gotchas.md。
探測腳本：scratchpad/jr_scripts/（probe_zoom.py、scan.py）；JMA 前端設定 XML：scratchpad/jr_probe/*.properties.xml。

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
