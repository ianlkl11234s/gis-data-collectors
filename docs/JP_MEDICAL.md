# Japan medical collector orchestration

三個 `BaseCollector` adapter 只執行 analytics 的既有 pipeline，回傳 scheduler 可記錄的小型 run 摘要；不把全國 GeoJSON、PMTiles 或原始檔塞入 Supabase。

| collector | interval | pipeline | 週期 |
| --- | ---: | --- | --- |
| `jp_medical_navii` | 30 days | `jp_medical_navii.py run` | 月度名錄檢查 |
| `jp_medical_idwr` | daily | `pipeline.py --year YYYY` | 每日當年與前一年各最近 4 週；每月 1--3 日全年度重查 |
| `jp_medical_reports` | 30 days | `jp_medical_reports.py` | 低頻 report intake/materialization，加 supplements 保險機構與 e-Stat |

三者 registry/config 預設均為 disabled。啟用前必須由 runtime 明確設定 `ANALYTICS_ROOT`、`PIPELINE_PYTHON` 與各 collector toggle；subprocess 使用 `shell=False`、一小時 timeout。尚未部署、未設定 cron，也沒有 Supabase 表。

三個 source job 各自成功後，才可用額外環境變數 `JP_MEDICAL_BUILD_FRONTEND=true` 讓該次 job 接著執行 analytics 的 `pipelines/world/jp_medical_frontend/build.py`；預設為 false，未改動共享 `config.py`。該 build 讀取 Navii、IDWR、reports 的既有 baseline，故僅在三份 baseline 都已存在時才可啟用；本次本地 baseline 已齊。source 任一步失敗時不會啟動 build；build 失敗時 collector 回報 error，不會回傳 completed/ready。`build.py` 只在完成檔案 hash 驗證後才原子更新其 frontend `current.json`，因此失敗不 promotion。這只是本地 bundle；排程仍 disabled：IDWR 預定每日執行，Navii 與 reports 預定每月檢查；所有 raw/processed artifacts 都留在本地，直到遠端 upload 與 readback 另行驗證。

`python3 tasks/jp_medical_artifacts.py --plan --dataset jp_medical_navii --analytics-root /path/to/analytics` 會列出 resolved raw、processed release、frontend 依賴與預定 key；它是 read-only，沒有 write/upload CLI。collector 僅在 `JP_MEDICAL_ARTIFACTS_ENABLED=true` 時呼叫這個 plan，預設 false。既有 daily archive 只收 JSON，CSV/ZIP/PDF/XLSX 不可宣稱已被它封存。

若要準備審核，由本地準備腳本產生 exact payload manifest，交使用者審核（`dataset` 與每個 raw 的相對 `path`、`sha256`、`bytes`），再加 `--payload-manifest manifest.json --output upload-plan.json`。程式只驗證該列出的檔案並產生固定 content-hash bucket key；不會遞迴挑選其他 raw，也不會連網。`cleanup_eligibility` 僅列出已超過 7 日、每檔 receipt 已驗證且 hash/bytes 一致、又非 current dependency 的候選；沒有 delete 實作。

前端 bundle 使用完整檔案 inventory hash namespace，保留 release 相對路徑；generated current 只能由該 bundle 建立，並遞迴驗證 `current.json`／`public-index.json` 的每個 nested `path`（含 Navii service-details bucket）。H17 僅允許已清洗的 `h17_frontend/shards/*.jsonl`，content type 為 `application/x-ndjson`；catalog、raw、workbook/PDF/ZIP 仍拒絕公開。`raw`、`private`、`unreviewed`、`raw_only`、`..` 一律拒絕。safe pruning 尚未具備逐物件 archive receipt 和 nested current dependency 證明，`prune_verified_raw` 固定回傳 retained/deferred，絕不刪檔。

這份接線沒有執行 upload、deploy、cron 設定或 commit。


`tasks/jp_medical_publish.py:publish_reviewed_payload` 是可注入client的發布函式；只接受exact payload及bucket，先驗完整清單再逐檔upload/readback，最後current。已用純記憶體fake client測試成功、hash mismatch與失敗保留current；沒有建立真client或執行真上傳。歷史版本保存不套用台灣急診30日DB刪除規則，因本批無live DB表。
