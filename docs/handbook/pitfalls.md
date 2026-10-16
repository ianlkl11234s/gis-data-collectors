# 事故與踩雷索引

一句話：每次出事都有一份長文，這裡只放「日期｜一句教訓｜連結」，要細節點進去看。

- 本 repo 的事故長文放在 [`.claude/pitfalls/`](../../.claude/pitfalls/)，檔名 `YYYY-MM-DD-<主題>.md`。新增長文時，在下表加一行；若產生新原則，在 [principles.md](principles.md) 加一行並回連。
- 外部 repo 的事故只連結不搬。gis-platform、.gis-agent-system 是 private repo，只寫路徑。
- 「原則」欄是 [principles.md](principles.md) 的編號。

## 本 repo（`.claude/pitfalls/`）

| 日期 | 一句教訓 | 原則 | 連結 |
|---|---|---|---|
| 2026-04-07 | naive datetime 在 UTC session 被當成 UTC，所有時間戳 +8 小時；S3 冷備份讓我們能全量回補 | W-01 | [2026-04-07-timezone-bug](../../.claude/pitfalls/2026-04-07-timezone-bug.md) |
| 2026-04-14 | ConnectTimeout（SYN 沒回應）＝出口 IP 被封；資料源盡量選原始供應者（CelesTrak → Space-Track） | D-03 | [2026-04-14-celestrak-ip-blocked](../../.claude/pitfalls/2026-04-14-celestrak-ip-blocked.md) |
| 2026-04-24 | 新分區表沒掛進分區管理，7 天後分區用完；partitioned table 的 BEFORE INSERT trigger 在子分區觸發，當不了防線（保留期現已改由註冊表管理） | R-01 | [2026-04-24-air-quality-partition-broken](../../.claude/pitfalls/2026-04-24-air-quality-partition-broken.md) |
| 2026-07-23 | refresh cron 每次重算昨天的不變資料，執行時間超過間隔後自我重疊成死亡螺旋；監控 RPC 全掃大表也是負載 | L-07、L-08、M-07 | [2026-07-23-disk-io-cron-spiral](../../.claude/pitfalls/2026-07-23-disk-io-cron-spiral.md) |
| 2026-07-24 | 在 Supavisor transaction pool 下 session 級 SET 會殘留在共用 backend，讓別的 client 隨機唯讀失敗 | L-09 | [2026-07-24-supavisor-session-set-poisoning](../../.claude/pitfalls/2026-07-24-supavisor-session-set-poisoning.md) |
| 2026-07-26 | `required_env` 只在 registry 宣告、沒在 config.py 宣告，collector 被 silent skip（事故約 2026-04） | D-01 | [2026-07-26-required-env-silent-skip](../../.claude/pitfalls/2026-07-26-required-env-silent-skip.md) |
| 2026-08-07 | 上游靜默斷供：程序成功、HTTP 200、沒有例外，產出永遠是空的；三種判準與 S3 檔案大小時間軸 | H-02 | [2026-08-07-silent-upstream-outage](../../.claude/pitfalls/2026-08-07-silent-upstream-outage.md) |
| 2026-08-30 | TDX 重置共用 secret，16 支 collector 401 達 15.4 小時；retry storm 的 429 蓋住了 401，告警洗版讓人當雜訊 | D-07 | [2026-08-30-tdx-secret-reset-401](../../.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md) |
| 2026-09-04 | Disk IO 膨脹螺旋（第一輪）：先量 HOT 比例再決定要不要改程式；分項改善不能相加；平台重啟會清空統計 | L-03、M-05、M-06、R-09 | [2026-09-04-disk-io-bloat-spiral](../../.claude/pitfalls/2026-09-04-disk-io-bloat-spiral.md) |

## 本 repo 的調查（`docs/investigations/`）

時點性證據，結論已回寫手冊。

| 日期 | 一句教訓 | 原則 | 連結 |
|---|---|---|---|
| 2026-10-05 | 心跳和實際寫入脫鉤：零筆不寫心跳、DB 告警只看全域最大值，乾旱燈號停寫約 143 天沒告警 | H-01 | [2026-10-05-collector-health](../investigations/2026-10-05-collector-health.md) |
| 2026-10-05 | 亂碼防護只能用 allowlist；壞列剔除、整批壞才 raise、buffer 重放壞檔進 quarantine | W-06 | [2026-10-05-gd2-mojibake-storage-guard](../investigations/2026-10-05-gd2-mojibake-storage-guard.md) |
| 2026-10-12 | 海象每輪重寫 rolling 48h、交易無 timeout 又持有站表列鎖，佔滿連線池；處置是先終止連線並停用該 collector | L-01、L-11、L-12、S-02 | [2026-10-12-marine-write-stall](../investigations/2026-10-12-marine-write-stall.md) |

## 外部 repo

| 日期 | 一句教訓 | 原則 | 位置 |
|---|---|---|---|
| 2026-04-07 | 分區命名用台北日、邊界用 UTC；回補要先建前一個 UTC 日的分區 | W-03 | gis-platform `.claude/pitfalls/2026-04-07-mv-refresh-and-partitions.md` |
| 2026-04-09 | DB 整台掛掉：一口氣加太多高頻 cron＋被遺忘的 MV refresh，Disk IO 與連線池爆表 | L-07 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-04-09 |
| 2026-04-10 | refresh 的 ORDER BY 沒有對應索引＝全表 sort，matview OOM | L-06 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-04-10 |
| 2026-05-09 | 寫成功才標記的批次會無限重試（waste_match）；換機房前要在新機上 curl 目標 API | I-03、D-03 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-05-09 |
| 2026-07-02 | 容器缺套件讓 collector 靜默失效；CMEMS subset 沒帶時間範圍，單檔 18.7 GB | D-02 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-02 |
| 2026-07-03 | 多 unique index 的表指定單一 conflict key 讓整批失敗；pooler 丟掉 startup 的 statement_timeout | W-04、L-09 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-07-03 |
| 2026-07-23 | 高頻聚合三鐵則、監控查詢必須是索引探測、QueryCanceled 不是斷線 | L-07、L-08、M-07 | .gis-agent-system `decisions/0009-supabase-object-placement-and-cron-discipline.md` |
| 2026-07-24 | realtime → live schema 全量搬遷；collector 表放 `live`、RPC 放 `public` | — | .gis-agent-system `decisions/0010-live-schema-migration.md` |
| 2026-07-26 | live 表 policy 只給 anon，登入會員靜默看到 0 筆 | D-06 | mini-taiwan-pulse `.claude/pitfalls/2026-07-26-live-rls-anon-only.md` |
| 2026-08-21/22 | NCDR 亂碼來自 chardet 猜錯編碼；公衛週報唯一約束含恆 NULL 欄位，數字灌水 13 倍 | W-05、W-07、W-10 | mini-taiwan-pulse `.claude/memory/INCIDENTS.md` §2026-08-21/22 |
| 2026-10-12 | Disk IO 第二輪：pg_stat_io 口徑、刪重複索引、trails 全量改增量 | M-01、L-05、L-06 | gis-platform `docs/research/2026-10-12-disk-io-round2.md` |
| 2026-10-16 | Disk IO 第三輪：沒變不寫與其反例、主鍵時間開頭、熱冷分離、先 collector 後唯一索引 | L-01、L-02、L-04、I-02、S-01 | gis-platform `docs/research/2026-10-16-disk-io-round3.md` |
| 2026-10-15 | 三輪 Disk IO 回頭看的 15 個資料工程盲點（心智模型，草稿） | — | gis-wiki `inbox/2026-10-15-gis-data-engineering-blind-spots.md` |
