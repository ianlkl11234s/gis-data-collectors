# 迭代歷程：為什麼長成現在這樣

一句話：這個 repo 從 2025-12 的「單一 Zeabur 服務＋S3 歸檔」長成 100 個 collector 的收集層，**幾乎每一個現在的設計都是某次事故的修正**——讀這份是為了在想改回去之前，先知道當初為什麼這樣做。

- 期間：2025-12-15 – 2026-10-16，約 500 個 commit。PR 流程從 2026-06-13 才開始（#2），之前都直接推 main，所以早期只能引用日期與 commit。
- 到目前為止**沒有刪除過任何 collector 檔案**，停用一律用 `*_ENABLED` 開關與 `cross_layer_map.yaml` 的 `deployment: disabled`。
- PR 連結：`#N` 指本 repo（ianlkl11234s/gis-data-collectors）；gis-platform、analytics、.gis-agent-system 是 private，只寫路徑。
- 原則文字的正本與編號在 [principles.md](principles.md)；事故長文索引在 [pitfalls.md](pitfalls.md)。

---

## 時間線

| 階段 | 期間 | 做了什麼 | 為什麼（觸發） | 留下的教訓 | 關鍵 PR／文件 |
|---|---|---|---|---|---|
| P1 起步：Zeabur 單體＋S3 歸檔 | 2025-12-15 – 2026-03-06 | 單一 Python 服務掛 Volume，先收 YouBike、天氣、台鐵，再加 AIS、FR24、公車、國道、地震；本地 JSON 每日打包 tar.gz 上 S3；所有 collector 加 `_ENABLED` 開關 | 每筆 S3 PUT 太貴、太多小物件 | 物件數比容量貴，要打包；每個 collector 都要能獨立啟停 | 12-26 S3 歸檔、02-28 本地＋tar.gz、03-03 開關（皆無 PR） |
| P2 搬進 Supabase | 03-16 – 04-09 | Telegram 通知；`SupabaseWriter` 把即時資料寫進 DB；修 +8h 時區；改用 RPC 預聚合並錯開 cron | 前端要即時查詢；Micro 規格第一次資源爆表 | 寫 DB 的時間一律 tz-aware；聚合要預先算、cron 要錯開 | `.claude/pitfalls/2026-04-07-timezone-bug.md` |
| P3 規模擴張 | 04-10 – 05-15 | 公車擴到全台 22 縣市；空品、水文系列；scheduler 改 ThreadPool＋skip-if-running；TDX 全域節流；`main.py` 收編成 registry；公車夜間暫停；垃圾車 GPS＋map-matching | collector 從十幾個變三十幾個，樣板碼與 TDX 配額撐不住 | 集中登記（registry）；共用金鑰要全域節流；「寫成功才標記」要另記 attempt | `.claude/pitfalls/2026-04-14-celestrak-ip-blocked.md`、`2026-04-24-air-quality-partition-broken.md`、`docs/BUS_NIGHT_PAUSE.md` |
| P4 主題擴張＋VM 分流＋監控 | 06-02 – 06-21 | 急診、台電、淹水、北市水利、新聞、共機、CDC 等；ship_ais、waste 改跑 HiCloud VM；一天做完監控 Stage 1–5（9 段）；開始走 PR＋CI；Supabase→S3 備份三機器人 | 政府來源擋雲端 IP；collector 多到靠人眼看不完 | 被擋就換出口（VM），主 repo 保留完整實作；監控要跨層對照 | #2–#5、#8、#12、#13；[`docs/EXTERNAL_COLLECTORS.md`](../EXTERNAL_COLLECTORS.md)、[`docs/MONITORING.md`](../MONITORING.md) |
| P5 韌性強化 | 06-25 – 07-10 | DB 寫入 timeout、斷路器、watchdog、連線池；statement_timeout 改 `SET LOCAL`；VM 端 buffer；全球氣候 6 個 collector；CMEMS 18.7 GB 事故 | 06-25 單一連線＋RLock 卡住，所有 collector 排隊 3 小時 | 一條死連線不能拖垮全體：連線池＋借不到就進 buffer；外部下載一定要設範圍 | #19、#20、#21、#25、#28、#29、#30 |
| P6 Disk IO 螺旋與 live 遷移 | 07-16 – 07-29 | cron 自我重疊造成死亡螺旋，之後做 green-first 監控；`realtime.*` 全面改名 `live.*`；Supavisor session 汙染事故；地震即時化 | 高頻聚合單次執行超過間隔 | 高頻聚合三鐵則；走 6543 禁止 session 級 SET；required_env 兩邊都要宣告 | #36–#40；`.claude/pitfalls/2026-07-23-disk-io-cron-spiral.md`、`2026-07-24-supavisor-session-set-poisoning.md`、`2026-07-26-required-env-silent-skip.md` |
| P7 船舶、海事、網路擴張 | 08-02 – 08-31 | 食品價格、PLA 航跡向量化、落雷雙源、動保；修 NCDR 亂碼；AISStream、GFW、海象（含 ISOHE VM）；TDX 憑證 401；網路健康（RIPE） | 主題擴張；NCDR 中文變亂碼；TDX 密鑰重置 | 編碼要用 `r.content`，壞資料回上游重抓；401 和 429 是兩回事 | #41–#68；`.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md` |
| P8 global_events＋第二次 IO 事故＋生命週期 | 09-01 – 09-29 | GDELT＋LLM 事件抽取；09-04 IO 膨脹螺旋；archive 改「讀回驗證後才刪本地」；磁碟壓力納入健康；GFW safe publish、CMEMS OOM；review 改由 Codex | 09-04 DB 被強制重啟 3 次 | `*_current` 全量 UPSERT 會膨脹；先量 HOT 比例再決定改哪；刪本地前要遠端讀回驗證 | #82–#84、#89、#91–#115；`.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md` |
| P9 環境監測＋健康調查＋防護層 | 10-01 – 10-09 | 新表必開 RLS；環境第二波 4 個 collector；collector 健康調查揭露心跳盲點；亂碼 allowlist；OSRM token 改從環境變數讀 | Supabase `rls_disabled_in_public` 告警；乾旱燈號 100 多天沒寫入沒人發現；token 曾寫死在 PUBLIC repo | 有心跳不等於健康；寫入層防護只能 allowlist；PUBLIC repo 不放祕密 | #122–#130；`docs/investigations/2026-10-05-*` |
| P10 日本 JMA 擴張 | 10-09 – 10-15 | AMeDAS、警報、地震、格網圖磚（ADR-0021 分層留存）、季節、落雷、WBGT、海況、發布幀上 R2 | 擴充到日本資料 | 大量網格資料要分層留存（熱／溫／冷），S3 規則要逐 collector 補 | #131–#151；`docs/JMA_*.md` |
| P11 Disk IO 第二、三輪：collector 端減寫 | 10-12 – 10-16 | marine 寫入停滯修復；TLE、測站 metadata、垃圾車 GPS 改「沒變不寫」；gis-platform 做增量 refresh、刪索引、熱冷分離、主鍵改時間開頭；心跳成功時清掉舊錯誤 | 10-06～10-12 DB 又重啟 5 次，讀取約 392 GB/天 | 分清「內容」與「心跳」；先部署冪等寫入再建唯一索引；量測要知道工具盲點 | #136–#149；見下方「Disk IO 三輪」 |

P10 與 P11 時間重疊，以主題分段。

---

## Disk IO 三輪（collector 最常被問「為什麼不能這樣寫」的來源）

| 輪 | 時間 | 結果 | 對 collector 的影響 | 文件 |
|---|---|---|---|---|
| 第一次事故 | 2026-09-04 | DB 被強制重啟 3 次；兩支查詢佔 76% 讀取，`*_current` 全量 UPSERT 膨脹 | 監控改 TTL 快取、foursquare_poi 移出即時健康清冊；當時判斷 HOT 可行，暫緩「沒變不寫」 | `.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md`（含後續兩輪段落） |
| 第二輪 | 2026-10-12 | 讀取約 392 → 215 GB/天（`pg_stat_io` 口徑），67 小時無重啟；其中 autovacuum 約佔 58% | marine 寫入停滯修復（conditional upsert、只寫最新窗口內有變動的列） | gis-platform `docs/research/2026-10-12-disk-io-round2.md`；`docs/investigations/2026-10-12-marine-write-stall.md` |
| 第三輪 | 2026-10-15 – 10-16 | 10 個 PR（gis-platform #156–#162、本 repo #146–#148），推估再降到約 140–160 GB/天（一週後同口徑複量，約 10-23） | TLE epoch 沒變就不寫（#146）；iot_wra／uswg 測站 metadata 沒變就不 UPDATE（#147）；垃圾車 GPS 改 `ON CONFLICT DO NOTHING`（#148，配合 mig 458 唯一索引）；ship_current 熱冷分離（mig 460，清 7 天未出現的船）；waste 去重（mig 458，臺南重複約 60%） | gis-platform `docs/research/2026-10-16-disk-io-round3.md` |
| 收尾修正 | 2026-10-16 | `report_collector_heartbeat` 成功時清掉舊的 `last_error`（gis-platform mig 465，PR #168） | 已恢復的 collector 不再顯示舊錯誤（例：移民署一直掛著 09-28 VM 磁碟滿的訊息）；停用的 collector 保留最後一筆錯誤 | gis-platform `migrations/465_heartbeat_clear_last_error_on_success.sql` 檔頭 |

注意：第一次事故報告的「330→178 GB/天」用 `pg_stat_statements` 量，和第二、三輪的 `pg_stat_io` 數字**不能直接比較**（量測口徑見 [operations.md](operations.md)）。

---

## 事故轉折表

原則欄寫的是原則文字；正式編號以 [principles.md](principles.md) 為準。

| 日期 | 事故 | 根因 | 修正 | 留下的原則 |
|---|---|---|---|---|
| 2026-04-07 | Supabase 資料時間 +8 小時 | naive datetime 被當成 UTC | 改用 `datetime.now(TAIPEI_TZ)`，從 S3 回補 30 天 | 寫 DB 的時間一律 tz-aware |
| 2026-04-09 | Micro 規格 IO／CPU 爆表 | 前端每次即時聚合大表；cron 擠在同一分鐘 | RPC 預聚合（trails_daily 等）、cron 錯開 | 聚合預先算，同頻 job 錯開分鐘 |
| 2026-04-14 | 衛星資料抓不到 | CelesTrak 封鎖 Zeabur IP | 改用 Space-Track | ConnectTimeout 先懷疑 IP 被封鎖 |
| 2026-04-24 – 27 | air_quality／微型感測器寫入連續失敗（找不到分區） | 新分區表沒掛進預建分區的 cron 清單；「寫入時自動建分區」的 trigger 設計本身不可行 | gis-platform mig 059／060 | 新分區表要同時登記到分區管理與保留期；不要靠 INSERT trigger 建分區 |
| 約 2026-04（07-26 記錄） | USWG collector 部署後完全沒跑 | `required_env` 只列在 registry，`config.py` 沒用 `os.getenv` 宣告，被 silent skip | `config.py` 補宣告 | `required_env` 要在 registry 和 config 兩邊都宣告 |
| 2026-05-09 | 垃圾車 map-matching 無限重試 | 「寫成功才標記」，NoMatch 永遠不算成功 | 另建 attempt 紀錄表 | 批次要另外記 attempt |
| 2026-06-25／26 | DB 卡住，所有 collector 停擺約 3 小時 | 單一連線＋RLock，一條死連線讓全體排隊 | timeout、斷路器、watchdog（#19、#20），連線池（#21） | 借不到連線就進 buffer，不能讓一條連線拖垮全體 |
| 2026-07-02 | CMEMS 單檔 18.7 GB | subset 沒帶時間範圍 | #25 | 外部下載一定要帶範圍 |
| 2026-07-03 | statement_timeout 在 pooler 不生效 | Supavisor transaction pool 不保留 session 設定 | 改交易內 `SET LOCAL`（#29） | 走 6543 只用 `SET LOCAL` |
| 2026-07-07 | DB 長到 52 GB | 新分區表沒加進清理函式的硬編碼清單（mig 220–222） | gis-platform mig 282：`metadata.retention_policies` 註冊表＋覆蓋率檢查 | 保留期只在註冊表登記一列，不改函式 |
| 2026-07-23 | Disk IO cron 死亡螺旋 | 高頻聚合單次執行超過間隔，自我重疊 | 降頻、增量、green-first 監控（#36–#38） | 高頻聚合三鐵則（ADR-0009） |
| 2026-07-24 | collector 唯讀錯誤大爆發 | 在 6543 下 session 級 SET，殘留在共用 backend | 全面移除 session 級 SET（#39） | 走 6543 禁止 session 級 SET |
| 2026-08-07 | 台電落雷上游靜默斷供 | 上游回 200 但只有標題列 | 降頻並加恢復提醒；改用 CWA 為替代源 | 「0 筆」要用 ledger、第二資料源或第三方訊號判斷 |
| 2026-08-22 | NCDR CAP 中文亂碼 | `r.text` 被 chardet 猜錯編碼 | 改用 `r.content`；寫入前擋亂碼（#54、#55） | 壞資料回上游重抓，不要反解 |
| 2026-08-30 | TDX 全面 401，即時資料中斷約 15 小時（永久缺口） | TDX 平台在維護窗重置了共用金鑰的 client secret；401 引發 retry storm，表面看起來像 429 | 換新 secret、更新環境變數並重啟；告警收斂（#62）；現在 `TDXSession` 遇 401 會換 token 重試一次 | 401 是憑證問題、429 是節流問題，同時出現先查認證 |
| 2026-09-04 | Disk IO 膨脹螺旋（第二次大事故） | `*_current` 全量 UPSERT＋監控全掃，清單長大撞上快路徑前提 | 降載、壓實、gis-platform 治本（#82–#84） | 監控不能變成負載；懷疑膨脹先量 HOT 比例 |
| 2026-09-18 – 23 | 本地 raw 刪除前沒驗證 | archive 上傳後直接刪本地 | 讀回驗證＋原子 receipt 後才刪（#92） | 刪本地前先把遠端全量讀回驗證 |
| 2026-10-05 | 乾旱燈號 100 多天沒寫入沒人發現 | 告警只看全域 MAX(last_success_at)；寫入失敗仍算成功；零筆不寫心跳 | 健康調查（#126）；亂碼 allowlist（#127）；OSRM token 改環境變數（#128） | 有跑、解析成功、有新資料三件事分開看 |
| 2026-10-12 | marine 寫入卡死數天 | 每輪對整個 48 小時窗口做 UPSERT，鎖與 IO 互相拖累 | conditional upsert、只寫最新窗口有變動的列、timeout（#136、#138、#139） | 「沒變也寫」是 IO 大戶 |
| 2026-10-15 – 16 | Disk IO 第三輪 | 沒變也寫、主鍵順序不對、熱表無清理、重複列 | 見上方「Disk IO 三輪」 | 先部署冪等寫入，再建唯一索引；心跳欄位不能套「沒變不寫」 |

事故 2026-08-07 的長文目前在 mini-taiwan-pulse `.claude/pitfalls/2026-08-07-silent-upstream-outage.md`。

---

## 怎麼補這份文件

每次事故或大改版：時間線加一行或延長階段，事故轉折表加一列並回連 pitfall 與 PR。規則見 [maintenance.md](maintenance.md)。
