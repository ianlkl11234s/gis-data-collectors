# 收集原則

一句話：每條原則都來自一次事故或量測，照著做可以避開已經踩過的坑。

- 一行一條，編號固定（類別字首＋流水號），引用時寫「principles W-04」。新增時接在該類最後，**不重排、不重用已刪除的編號**（刪除時保留編號並標「已撤銷」）。
- 「出處」是事故長文、調查或決策。外部 repo 只寫 repo 名＋路徑；gis-platform、taipei-gis-analytics、.gis-agent-system 是 private repo。
- 保留期天數不寫在這裡，見 [data-lifecycle.md](data-lifecycle.md)「保留期去哪查」。

類別：**W** 寫入正確性｜**I** 冪等｜**L** DB 負載｜**R** 保留期與生命週期｜**S** 排程對齊｜**M** 量測｜**H** 健康判讀｜**D** 部署與來源

## W 寫入正確性

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| W-01 | 寫進 DB 的時間一律帶時區（`datetime.now(TAIPEI_TZ)`），不用 naive datetime | DB session 是 UTC，naive 台灣時間會被當成 UTC，全部時間戳偏移 +8 小時；2026-04 發現後從 S3 回補 30 天 | [pitfall 2026-04-07](../../.claude/pitfalls/2026-04-07-timezone-bug.md)；`collectors/base.py` |
| W-02 | SQL／cron 裡的「今天」寫 `(now() AT TIME ZONE 'Asia/Taipei')::date`，不寫 `current_date` | `current_date` 是 UTC 日，台北 00:00–08:00 永遠不會 refresh「今天」；2026-06-17 七個 dataset 早上全空 | gis-platform `migrations/208…`；mini-taiwan-pulse `.claude/memory/PRINCIPLES.md` |
| W-03 | 回補台灣某一天的分區資料前，先建「前一個 UTC 日」的分區 | 分區命名用台北日、邊界用 UTC，差 8 小時，不先建會 `no partition of relation … found for row` | gis-platform `.claude/pitfalls/2026-04-07-mv-refresh-and-partitions.md` |
| W-04 | 只想去重時，用**不指定目標**的 `ON CONFLICT DO NOTHING` | 表有多個 unique index 時，指定其中一個 key，撞到另一個會讓整批 INSERT 失敗（lightning 2026-07-02 起連續寫入失敗） | `storage/supabase_writer.py` 註解；mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-03 |
| W-05 | 唯一約束不能包含「永遠是 NULL」的欄位 | `NULL <> NULL`，ON CONFLICT 永遠不觸發；公衛週報 17.4 萬列對 1.47 萬個唯一鍵，數字灌水 13 倍 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-21/22 |
| W-06 | 寫入層的內容防護（例如拒寫西里爾亂碼）只對 allowlist 內的 collector 生效；壞列剔除、整批都壞才 raise、不進 buffer | 國際新聞標題、AIS 船名、USGS 地名、衛星名稱都可能合法含西里爾字母，一體適用會讓它們停擺 | `storage/supabase_writer.py` `MOJIBAKE_GUARD_COLLECTORS`；[調查 2026-10-05 GD-2](../investigations/2026-10-05-gd2-mojibake-storage-guard.md) |
| W-07 | 解析 HTTP 內容用原始 bytes（`r.content`），不要讓 requests 猜編碼；壞資料回上游重抓，不要反解 | 上游沒給 charset 時 chardet 會猜成西里爾碼頁；NCDR 16,815 筆中壞 95 筆 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-21/22 |
| W-08 | 「現值表」sticky 寫入：失敗時不清空已知的有效值，搭配 TTL 判定失效 | yt resolver 失敗就清空，0.3% 成功率下每天只有 5 分鐘有資料 | mini-taiwan-pulse `.claude/memory/PRINCIPLES.md` §Collector sticky 寫入原則 |
| W-09 | 完整快照型 collector 失敗時回傳 `_collector_error` 寫 ledger，不用空清單假裝成功 | 空清單會被當成「這輪成功、沒資料」，失敗被藏起來 | `collectors/base.py`；[code-audit-evidence](../investigations/code-audit-evidence.md) |
| W-10 | 改解析邏輯＝改資料契約：回填和部署排在一起，先確認寫入端不再產生壞資料再修資料 | 線上舊版 collector 會把修好的資料蓋回去（共機 `raw_text`）；NCDR 第一次修完又冒 17 筆 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-01/02、§2026-08-21/22 |
| W-11 | 以資料內的來源時間判斷新舊，不信 API 的更新時間或 HTTP 200 | 上游可能凍結仍回 200（JMA 舊警報端點）；TDX road_events `LastUpdateTime` 71% 是假刷新 | .gis-agent-system `decisions/0021…`；taipei-gis-analytics `docs/api-platforms/tdx/gotchas.md` |
| W-12 | 缺值寫 NULL，不寫 0（休市字串、-99、缺幀、`COALESCE(…,0)` 都算） | 0 是一個觀測值，會讓「沒資料」變成「數值為零」 | .gis-agent-system `decisions/0021…`；mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-10-03 |
| W-13 | 較舊的批次不能覆蓋較新的值；會這樣的寫入不進 buffer（`NO_BUFFER_COLLECTORS`） | buffer 晚於新值重放，會把上游的回溯修正蓋回舊值（env_jp_wbgt），或讓同一變動寫兩筆（jma_warnings） | `storage/supabase_writer.py` `NO_BUFFER_COLLECTORS`；`storage/marine_write.py` |
| W-14 | (0,0) 座標在寫入前濾掉 | TDX 偶爾回 (0,0)，國道曾有 435 筆；濾法 `abs(lat)>0.01 and abs(lon)>0.01` | taipei-gis-analytics `docs/api-platforms/tdx/gotchas.md` |

## I 冪等

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| I-01 | 每張 history 表要有自然鍵，寫入要冪等（重跑同一批不會多出列） | waste 同城市輪詢時間窗重疊，臺南重複率 59.9%，軌跡點約 2.5 倍 | gis-platform `docs/research/2026-10-16-disk-io-round3.md` §4 |
| I-02 | 要加唯一索引時，**先部署冪等的 collector，再建唯一索引** | 順序反了 collector 會撞唯一鍵失敗 | gis-platform `docs/research/2026-10-16-disk-io-round3.md` §6 |
| I-03 | 「寫成功才標記」的批次處理要另外記 attempt | 沒有成功結果的項目下一輪又被撈出來，永遠重試（waste_match 每天固定 80 筆） | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-05-09；gis-platform `migrations/075…` |
| I-04 | 新來源和既有來源疑似重疊時，用座標 100 m 配對判定，不信編號 | >90% 重複停一邊、<30% 互補兩邊留（iot_wra 地下水 95% 重複，已停子端點） | mini-taiwan-pulse `.claude/memory/PRINCIPLES.md` §Collector 重複度檢核 |
| I-05 | 對外部服務逐批處理（LLM 標註等）時，單批失敗只跳過該批、下一輪重送；全部批次失敗才讓整輪失敗。批內維持原子，一則不合法整批跳過 | 全有全無在積壓時幾乎每輪都碰到一批失敗：news_events 積壓 28 批、約 18% 批次失敗，加值後還卡 9 小時，且每輪重送全部積壓 | [pitfall 2026-10-21](../../.claude/pitfalls/2026-10-21-news-llm-all-or-nothing.md) |

## L DB 負載

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| L-01 | 沒變就不寫：用內容 hash、epoch 或 `IS DISTINCT FROM` 擋掉無變化的 UPDATE | 海象 35 小時 1,140 萬次更新、實際新增 4.6 萬；TLE 每輪 95% 沒變；站表每 10 分鐘全量 UPSERT 只有 `updated_at` 在變 | [調查 2026-10-12 marine](../investigations/2026-10-12-marine-write-stall.md)；gis-platform `docs/research/2026-10-16-disk-io-round3.md` §3 |
| L-02 | 當心跳或清理依據的欄位不能套 L-01；套用後 `updated_at` 的意思會變成「最後異動」 | `road_events_current.collected_at` 同時是 3 天清理與 freshness 監控的依據，不更新就打壞兩者；看新鮮度要改看 collector 心跳 | gis-platform `docs/research/2026-10-16-disk-io-round3.md` §1、§6 |
| L-03 | 懷疑表膨脹先量 HOT 比例；HOT 看的是「被索引的欄位」有沒有變 | `ship_current` 的 geom 索引讓 HOT 只有 3.7–4.6%，刪索引＋fillfactor 後 97.8%；09-04 治本沒改一行 collector | [pitfall 2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md)；round3 §4 |
| L-04 | 時序表主鍵以時間欄開頭 | 以測站開頭時每輪新資料散在約 1,900 個索引頁，`iot_wra_measurements` PK 一天讀 5.7 GB（mig 456 改正） | gis-platform `docs/research/2026-10-16-disk-io-round3.md` §3；`migrations/456…` |
| L-05 | 索引是寫入稅，沒用的要刪；但刪前要有三個證據：函式定義、各 repo 程式碼、EXPLAIN | 統計重啟就歸零，`idx_scan=0` 只代表最近幾天（見 M-02）；有些索引被 anon view 或 RPC 用到 | gis-platform `docs/research/2026-10-12-disk-io-round2.md` §2、§7 |
| L-06 | 全量重算改增量：水位表＋晚到批退回全量＋夜間全量基準＋上線前用 oracle 逐列比對 | ship trails 243 秒→3 秒、bus 122 秒→2.5 秒 | gis-platform round2 §4–§5；`migrations/402…`、`442…`、`443…`、`459…` |
| L-07 | 高頻聚合三鐵則：昨天的資料一天算一次；單次執行超過排程間隔 1/3 就降頻或改增量；同頻 job 錯開分鐘 | 2026-07-23 refresh cron 自我重疊，單條卡 2 小時 15 分，形成死亡螺旋 | [pitfall 2026-07-23](../../.claude/pitfalls/2026-07-23-disk-io-cron-spiral.md)；.gis-agent-system `decisions/0009…`；gis-platform `migrations/302…` |
| L-08 | 監控不能變成負載：時間條件寫在 WHERE 並落在分區鍵；`time_column` 要有對應索引；預算「頻率×單次成本」 | `COUNT(*) FILTER` 不做 partition pruning；health_snapshot 曾單次 29 分鐘／12 GB、09-04 又佔讀取大宗 | [pitfall 2026-07-23](../../.claude/pitfalls/2026-07-23-disk-io-cron-spiral.md)、[2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) |
| L-09 | 走 Supavisor 6543 不下任何 session 級設定；statement_timeout 在每個交易內 `SET LOCAL` | pooler 丟掉 startup options、session SET 殘留在共用 backend，曾讓 collector 隨機唯讀失敗 | [pitfall 2026-07-24](../../.claude/pitfalls/2026-07-24-supavisor-session-set-poisoning.md)；`storage/supabase_writer.py` `_txn()` |
| L-10 | 寫入走連線池＋借用逾時＋斷路器；借不到連線就進 buffer，不排隊等 | 單連線＋RLock 時，一條 wedge 的連線讓所有 collector 排隊 3 小時（2026-06-26） | `storage/db.py`、`storage/supabase_writer.py` 檔頭 |
| L-11 | 寫入交易要短、要有 `statement_timeout` 與 `lock_timeout`；站表和觀測表分開交易 | 海象一筆交易跑 2 小時並持有站表列鎖，後續 6 輪排隊、佔滿連線池 | [調查 2026-10-12 marine](../investigations/2026-10-12-marine-write-stall.md) |
| L-12 | DB 是收資料、重算、供網站讀的共用資源：事故時先切斷最大的來源，再查細節 | 一個來源吃 IO，其他變慢→重算重疊→清理延後，會正回饋；海象卡死時所有 cron 慢 10–50 倍 | [調查 2026-10-12 marine](../investigations/2026-10-12-marine-write-stall.md)；gis-wiki `inbox/2026-10-15-gis-data-engineering-blind-spots.md` |
| L-13 | 重負載操作（VACUUM FULL、大量回填、重建索引）避開台灣 10:00–20:00，先設 `lock_timeout` | 餐期是網站讀取尖峰；拿不到鎖就放棄，不要排隊卡住寫入 | [pitfall 2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) |

## R 保留期與生命週期

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| R-01 | 保留期的唯一正本是 `metadata.retention_policies`；新表 INSERT 一列，不改清理函式 | 舊做法是改函式內的硬編碼清單，220–222 各自覆寫時把表踢掉，4 張表寫入失敗 | gis-platform `migrations/282_retention_registry_and_coverage.sql` 檔頭 |
| R-02 | 保留天數下限＝前端或 RPC 實際回看的窗口，再加 margin | 設太短會弄壞現有卡片（急診 14 天卡要設 30 天） | gis-platform `migrations/385…` |
| R-03 | 刻意永久保留要明寫 `retention_days = NULL` | 沒登記和「決定永久」分不出來；217 註解寫 7 天其實從未實作 | gis-platform `migrations/309…`、`352…`、`412…` |
| R-04 | 影像類先雙寫 R2，清理條件加 `image_key IS NOT NULL` | 沒有副本的影像刪了救不回 | gis-platform `migrations/282…`、`388…` |
| R-05 | 刪本地 raw 之前，先把遠端歸檔全量讀回驗證並寫原子 receipt | 只看「上傳成功」不能證明遠端內容完整 | `tasks/archive.py`；[lifecycle-hardening-20260918](../lifecycle-hardening-20260918.md) |
| R-06 | S3 不設 expiration；每個新 collector 補一條 `archive-deep-270d:{name}` 規則 | 冷層是最後的副本；`put-bucket-lifecycle-configuration` 是整份覆寫，漏補就停在 GLACIER_IR | [AWS_INVENTORY.md](../AWS_INVENTORY.md) §2 |
| R-07 | 冷層一律每日打包成一個 tar，不存大量小物件 | 物件數比容量貴：Deep Archive 按 PUT 計費、IA／GIR 未滿 128 KB 以 128 KB 計 | .gis-agent-system `decisions/0021…` |
| R-08 | 依賴滾動視窗的資料「今天不存，就永久少一天」，凍結匯出要有防呆並監控 | trails 只在 DB 留幾天，漏跑不會自動補；dates matview 會謊報可用天數 | mini-taiwan-pulse `.claude/memory/PLAYBOOKS.md` PB-35；`scripts/export_daily_trails.py` |
| R-09 | DELETE 不會把磁碟還給 OS；要縮檔得 VACUUM FULL（或 pg_repack） | 純 VACUUM 只讓空間可重用，表大小不變 | [pitfall 2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) |
| R-10 | `check_retention_coverage()` 回報 0 缺口不等於全部有保留期，新表上線要自己確認已登記 | 2026-09-01 稽核找到 7 張共約 4.1 GB 沒登記卻回報 0；擴大範圍後仍有盲區 | gis-platform `migrations/385…`、`352…` |

## S 排程對齊

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| S-01 | 衍生資料的排程要對齊資料到達時間；collector 落地的分鐘會隨部署漂移，排程不要依賴固定相位 | 衛星變軌表 :15 重算、資料 :27 才到，固定晚 1 小時 48 分；部署後落地分鐘由 :27 變 :58 | gis-platform `docs/research/2026-10-16-disk-io-round3.md` §5 |
| S-02 | 會和 DB cron 撞期的 collector 用 `fixed_minutes` 固定在空檔分鐘 | 海象每 15 分一輪撞上 :14、:15 的 refresh，一起推高 IO | `main.py` `_schedule_collector`；`config.py` `CWA_MARINE_OBSERVATION_MINUTES` |
| S-03 | 同頻 collector 首輪與排程加隨機偏移 | 全部撞同一秒會造成連線池尖峰（曾 borrow timeout） | `main.py` `run_collectors` |

## M 量測

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| M-01 | 量 Disk IO 用 `pg_stat_io` 依 `backend_type` 加總；`pg_stat_statements` 只用來排序語句 | statements 看不到 autovacuum；2026-10-12 實測 autovacuum 佔讀取 58% | gis-platform `docs/research/2026-10-12-disk-io-round2.md` §1 口徑說明 |
| M-02 | 統計在 DB 重啟時歸零：窗口起點用 `pg_postmaster_start_time()`；「零呼叫／idx_scan=0」只代表重啟後那幾天 | 正式庫常被平台重啟；`track_functions=none`，拿不到函式層級呼叫數 | gis-platform round2 §2、§7 |
| M-03 | 不同口徑的數字不能互相比較或相減 | 9 月的 330→178 GB/天是 statements 口徑，10 月的 392→215 GB/天是 pg_stat_io 口徑 | gis-platform round2 §1 |
| M-04 | 事故期間量到的數字不能拿來排優先順序 | 衛星變軌表 446 秒是 IO 飽和時量的，正常 16 秒；前端讀取實測只有約 0.7 GB/天，先前憑程式碼猜它是主因是錯的 | gis-platform `docs/research/2026-10-16-disk-io-round3.md` §1 |
| M-05 | 各項改善的推估不能相加當總量，要套用後同口徑、夠長的窗口複量 | 各項互相影響（減寫入也減 autovacuum） | [pitfall 2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md)；round3 §3 |
| M-06 | 收到 Disk IO 告警先 dump 統計快照 | 平台可能隨時重啟，統計一歸零證據就沒了 | [pitfall 2026-09-04](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) |
| M-07 | `QueryCanceled`／statement timeout 不等於斷線，先查 `pg_stat_activity`，不要自動重啟 | 那是壅塞的症狀；重啟救不了 DB 端的壅塞，還可能讓巡檢誤報 | [pitfall 2026-07-23](../../.claude/pitfalls/2026-07-23-disk-io-cron-spiral.md)；.gis-agent-system `decisions/0009…` §4 |

## H 健康判讀

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| H-01 | 有心跳不等於健康：分開看「有沒有跑」「有沒有解析成功」「有沒有新資料」 | 心跳在零筆時不更新、寫入失敗也可能算成功，DB 告警只看全域最大值（見 [operations.md](operations.md)「三個盲點」） | [調查 2026-10-05](../investigations/2026-10-05-collector-health.md)；.gis-agent-system `decisions/0018…` |
| H-02 | 「0 筆」常是合法結果；判準依可靠度：ledger 表 > 第二資料源 > 第三方訊號反證 | 無雷雨、無共機、無地震都會是 0；落雷曾斷 28 天、A1 停 6 週都沒告警 | 本 repo `.claude/pitfalls/2026-08-07-silent-upstream-outage.md` |
| H-03 | 判斷部署生效看「只有新版才會產生的內容特徵」；判斷 collector 在跑看 DB 實際資料，不看 config 預設 | UPSERT 的 UPDATE 分支不一定更新 `updated_at`；自動回填的清冊把 19 個正在寫的 collector 標成停用 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-03、§2026-08-03 |
| H-04 | 多個來源共用一張表時，要能分來源看新鮮度 | 一個來源壞掉會被其他來源的新資料蓋過去（lightning 三源、internet_health 三源） | 盤點 2026-10-15（本手冊建立時發現，見 [operations.md](operations.md)「已知監控缺口」） |
| H-05 | `collect()` 失敗也要寫失敗心跳；失敗訊息帶原因類別（例外類別＋HTTP 狀態碼，不帶內文） | 心跳原本只由 `write()` 回報，`collect()` raise 時狀態表永遠是 0 錯誤：news_events 停 4 天沒被發現，錯誤原因又被 `from None` 吞掉 | `collectors/base.py::run()`；[pitfall 2026-10-21](../../.claude/pitfalls/2026-10-21-news-llm-all-or-nothing.md) |

## D 部署與來源

| ID | 原則 | 為什麼 | 出處 |
|---|---|---|---|
| D-01 | `required_env` 要在 `collectors/registry.py` 和 `config.py` 兩邊都宣告；部署後看啟動 log 的 ⚠ 行 | 只宣告一邊會被 silent skip，不報錯 | [pitfall 2026-07-26](../../.claude/pitfalls/2026-07-26-required-env-silent-skip.md) |
| D-02 | env 改完要 restart；程式改完要 redeploy；`*_ENABLED=true` 卻沒資料先查 image 有沒有裝相依套件 | Zeabur 從 GitHub 建 image，restart 不會拿到新程式；缺套件時 lazy import 靜默失敗 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-02；`.claude/memory/PRINCIPLES.md` §Zeabur 部署 |
| D-03 | ConnectTimeout（不是 403／404）先懷疑來源封鎖雲端 IP；確認後改跑 VM | 多個政府 API 只放台灣 ISP；CelesTrak 封了 Zeabur 出口 IP | [pitfall 2026-04-14](../../.claude/pitfalls/2026-04-14-celestrak-ip-blocked.md)；[EXTERNAL_COLLECTORS.md](../EXTERNAL_COLLECTORS.md) |
| D-04 | VM 上的 collector 是手動部署，git push 不會生效；部署前先 diff VM 現行檔並備份 | 曾補好程式 commit 後正式環境仍沒資料 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-26/27；round3 §4 |
| D-05 | 祕密不進 log、例外訊息、fixture、commit 與 CLI 輸出；API key 放 params 時改寫例外訊息 | `raise_for_status()` 的訊息含完整 URL；Zeabur CLI 會用表格印出祕密值 | `AGENTS.md`；`collectors/moenv_common.py`；[operations.md](operations.md)「祕密處理」 |
| D-06 | 新的 live／reference／spatial 表要開 RLS，anon 與 authenticated 兩個角色都授權 | 只授權 anon 讓登入會員靜默看到 0 筆，持續 3 週 | mini-taiwan-pulse `.claude/pitfalls/2026-07-26-live-rls-anon-only.md` |
| D-07 | 401 和 429 分開判讀；401 時先本地重放 token 請求看 error body | TDX secret 被重置時 retry storm 產生的 429 蓋住了真正的 401，拖了 15.4 小時 | [pitfall 2026-08-30](../../.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md) |
| D-08 | 改遠端登入設定（sshd、防火牆）要先留退路：保留既有連線、排自動還原計時器、用新連線驗證通過才取消 | VM 只有一把登記的金鑰，鎖在外面只能從 HiCloud 主控台救；2026-10-16 改成只允許金鑰登入時照此執行 | [operations.md](operations.md)「SSH 只允許金鑰登入」 |
