# 2026-09-04：Disk IO 膨脹螺旋——`*_current` 表全量 UPSERT + 監控全掃

> 上一起同類事故：[`2026-07-23-disk-io-cron-spiral.md`](./2026-07-23-disk-io-cron-spiral.md)（cron 重疊螺旋）。
> 本次**不是** cron 重疊，是**膨脹螺旋**——同樣會自我強化，但機制不同。

## 症狀

- Supabase 寄「Disk IO Budget 即將耗盡」通知
- **6 天內被平台強制重啟三次**：08-30 14:30、09-02 11:54、09-04 04:53（UTC），間隔 3 天 → 2 天，正在縮短
- 與 7/23 不同：cron **全部 succeeded**、無卡住的 backend、無自我重疊。表面看不出異常
- cron 每日執行數 13 天內從 903 漲到 1229（+36%）

## 根因（三層疊加，收斂到同一張表）

### 第一層：兩支查詢佔 76% 磁碟讀取

重啟前 1.7 天窗口（在 stats 被重啟清空前一刻抓到的快照）：

| 查詢 | calls | 讀取 | 單次 | 每日 |
|---|---|---|---|---|
| `public.health_snapshot()` | 109 | 193.7 GB | 1.77 GB / 26 s | ~114 GB |
| `live.compute_pressure_index()` | 164 | 154.6 GB | 0.94 GB / 7 s | ~91 GB |

### 第二層：清單長大撞上快路徑前提

`health_snapshot` **函式本身零漂移**（prod 定義與 mig 306 逐字一致）。變的是餵給它的清冊：
`config/realtime_tables.yaml` 從 7/23 修復當時 **72 張長到 105 張**（+46%）。

其中 **8 張表既非分區表、freshness 欄位又無 leading-column 索引** → 每次呼叫對每張做兩次全表掃描。
其中 7 張在 7/23 的清冊裡**就已存在且當時就沒索引**——舊坑隨資料長大才爆量。

`compute_pressure_index` 則對 `live.road_events_current` 做**兩次 Seq Scan**（30 分窗 + 24h GROUP BY）。

### 第三層（最根本）：`*_current` 表的 UPSERT 膨脹

`storage/supabase_writer.py` 有一行註解：

```python
# 3. current 全部 UPSERT（即使欄位未變也 noop，無副作用）
```

**這個技術認知是錯的。** PostgreSQL 的 `ON CONFLICT DO UPDATE` 即使新舊值完全相同，
仍會寫新版本 tuple 並把舊的標為 dead——是完整的 MVCC 更新，**不是 no-op、有副作用**。

每 5 分鐘對全部 ~13,000 列 UPSERT ≈ **374 萬 dead tuple/天**。
諷刺的是程式碼**上一段就已算好 `prev_content` 比對**（用於決定 history 表寫不寫），卻沒用在 current 的 UPSERT 上。

### 完整螺旋

```
全量 UPSERT（誤以為 no-op）→ 每輪產生全表列數的 dead tuple
  → autovacuum 追不上（IO 飢餓時被餓死）→ 表膨脹 20~86 倍
  → 監控／計算函式全掃這些表（缺索引）→ 掃的是膨脹後的體積
  → Disk IO 爆掉 → autovacuum 更餓死 ⟲
```

`road_events_current` **明明有最激進的 autovacuum 設定**（mig 305 的 scale=0.0/threshold=500）卻仍膨脹 23 倍
→ autovacuum 根本沒跑成功。`cost_delay=2ms / cost_limit=200 / 3 個 worker 服務 105 張表`，
單次 vacuum 遠超 5 分鐘的 churn 間隔——**套 ADR-0009 鐵則 2b，autovacuum 自己就是螺旋的一環**。

## 鑑識方法（可複用）

```sql
-- 膨脹掃描：純 catalog、零 IO。每列 bytes 異常大 = 疑似膨脹
select n.nspname||'.'||c.relname, c.reltuples::bigint,
       pg_size_pretty(c.relpages::bigint*8192),
       round((c.relpages::bigint*8192)/nullif(c.reltuples,0))::bigint bytes_per_row
from pg_class c join pg_namespace n on n.oid=c.relnamespace
where c.relkind='r' and c.relpages>1000 and c.reltuples>0
  and (c.relpages::bigint*8192)/nullif(c.reltuples,0) > 3000
order by c.relpages desc;

-- 找沒有 leading-column 索引的 freshness 欄位（health_snapshot 全掃來源）
-- 對照 config/realtime_tables.yaml 的 time_column 與 pg_index 的第一欄

-- 重啟歷史（stats 被清空也查得到，cron.job_run_details 是普通表）
select start_time, jobid, status, left(return_message,60)
from cron.job_run_details where status<>'succeeded'
order by start_time desc limit 20;   -- 'server restarted' / 'job startup timeout'
```

⚠️ `pg_stat_statements` / `pg_stat_user_tables` 在平台重啟後**全部歸零**。
本次是在重啟前一刻剛好抓到快照才保住證據。收到 Disk IO 告警要**第一時間 dump pg_stat_statements**。

## 修復（全部已執行並驗證）

### 第 1 級：降載（PR #82 + DB cron）

| 改動 | 效果 |
|---|---|
| `cron.alter_job(62, '23 * * * *')`（原 `*/15`）| 96 → 24 次/天，省 ~68 GB/天。前端讀 `get_pressure_index_now()` 快取列，零 UX 損失 |
| `foursquare_poi` 移出即時清冊 | 30 天才更新一次卻每輪全掃 105 MB |
| `marine_observation_readings` 改看 `observed_at` | 原設 `collected_at` 無索引；索引全在 `observed_at`。實測兩欄落差僅 2.3 秒 |
| `query_realtime_health` 加表級 TTL 快取 | `daily_report` 一次執行內 7 個呼叫點查同一份資料 → 7 次變 1 次 |

### 第 2 級：壓實膨脹表（實測數字）

| 表 | 前 | 後 | 倍數 | 耗時 |
|---|---|---|---|---|
| `live.road_events_current` | 433 MB | **19 MB** | 23× | 3.95 s |
| `live.freeway_sections_current` | 108 MB | **1.3 MB** | 86× | 0.70 s |
| `live.road_sections_current` | 49 MB | **2.4 MB** | 20× | 0.54 s |

合計 **590 MB → 23 MB**。三張都是秒級完成，鎖表時間遠短於預期。

⚠️ 普通 `VACUUM` 在此**無效**——它只標記空間可重用，檔案仍是 433 MB，Seq Scan 照讀。
必須 `VACUUM FULL` 或 `pg_repack`（後者本專案未安裝，`pg_available_extensions` 有但 `installed_version` 為空）。

執行時務必 `set lock_timeout='10s'`——`VACUUM FULL` 等鎖期間會把後續查詢全部排隊阻塞，
拿不到鎖就放棄比排隊安全。

## 預防守則

1. **`ON CONFLICT DO UPDATE` 不是 no-op**。值沒變也會產生 dead tuple。
   高頻全量 UPSERT 的 `*_current` 表要嘛只更新真正變動的列，要嘛接受 churn 並確認 autovacuum 跟得上。
2. **膨脹的自檢指標是「每列 bytes」**，不是表大小。`ship_current` 19 萬列只佔 34 MB（0.18 KB/列）是健康的，
   `freeway_sections_current` 680 列佔 108 MB（166 KB/列）是病態的。純 catalog 查詢、零 IO，該進週巡檢。
3. **新表進 `realtime_tables.yaml` 必須檢查 `time_column` 有沒有 leading-column 索引**。
   這次 `marine_observation_readings` 就是設定看 `collected_at`、索引卻全建在 `observed_at`（欄位錯配）。
   mig 306 沒有防回歸守門——新表加入不會自動被納入 pruning 快路徑，這是結構性破口。
4. **監控自己不能成為負載源**（ADR-0009 鐵則 3，本次二度失守）。
   `health_snapshot` 是 7/23 才修好的（29 分 → 8 秒），一年不到又退化回 26 秒。
   修好之後要有守門機制，否則清單一長就回到原點。
5. **同一份資料不要在一次執行內重算多次**。`daily_report` 的 7 個 section 各自呼叫同一支 RPC，
   單次 1.8 GB × 6 次純浪費。跨 section 共用的查詢結果要提到最外層算一次。
6. **autovacuum 也適用「執行時間 > 間隔 1/3 就追不上」**（ADR-0009 鐵則 2b）。
   per-table 設定調得再激進，只要單次 vacuum 掃描量 > churn 間隔內能處理的量，就是無效設定。

## 誠實紀錄

- **調查污染**：子代理在煞車訊息送達前，對 `road_events_current` / `rain_gauge_readings` 各跑了一次
  `count(*)` 全掃（合計 0.49 GB），加重了進行中的事故。事故期間的調查應**只用 catalog 查詢**
  （`pg_class.relpages` 估大小，不用 `count(*)`、不用 `EXPLAIN ANALYZE`）。
  附帶收穫：那次 428 MB 的實測正好驗證了 relpages 估的 433 MB。
- **估計值會漂移**：`reltuples` 在 autoanalyze 前後從 13,069 → 29,562 → 壓實後 13,522。
  用它算膨脹倍數會得到 21× / 10× / 23× 三種答案。**下結論前要註明是估計值**
  （同 7/23 的 `n_live_tup` 教訓）。
- **子代理的建議需要覆核**：兩路子代理都建議「先補 `collected_at` 索引」。但 collector 每輪 bump 全部列的
  `collected_at`，任何時刻所有列都落在 24h 窗內 → **索引選擇性為零**，只救得了 `max()` 探針，
  `count`/`GROUP BY` 照樣全掃。**縮 heap 才是兩個都修**。實測證明這個判斷正確（壓實後直接縮 23 倍）。
- **VACUUM FULL 比預期便宜太多**：原本規劃「挑離峰、鎖表數十秒」，實測三張表全部秒級完成（最大那張 3.95 秒）。
  下次遇到同類膨脹不需要為了排時段而拖延。
